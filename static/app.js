// app.js (LIVE)
document.addEventListener('DOMContentLoaded', initLiveApp);

let map;
let markerClusterGroup;
let shipMarkers = {};
let overlayMarkers = {};
let selectedShips = [];

let collisionsData = [];
let collisionMarkers = [];

let vectorLength = 15;  // suwak min: 1, max:120
let cpaFilter = 0.5;    // suwak min:0,  max:0.5
let tcpaFilter = 10;    // suwak min:1,  max:10

let shipsInterval = null;
let collisionsInterval = null;

function initLiveApp() {
  // Utworzenie mapy z common.js
  map = initSharedMap('map');

  // Dodanie MarkerClusterGroup
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // Obsługa suwaków
  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(true);
  });
  document.getElementById('cpaFilter').addEventListener('input', e => {
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisions();
  });
  document.getElementById('tcpaFilter').addEventListener('input', e => {
    tcpaFilter = parseFloat(e.target.value) || 10;
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    fetchCollisions();
  });
  document.getElementById('clearSelectedShips').addEventListener('click', clearSelectedShips);

  // Pierwsze pobranie statków/kolizji
  fetchShips();
  fetchCollisions();

  // Inicjalizacja intervali
  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

// ------------------ Pobieranie statków ------------------
function fetchShips() {
  fetch('/ships')
    .then(r => r.json())
    .then(data => updateShips(data))
    .catch(err => console.error("Błąd /ships:", err));
}

function updateShips(shipsArray) {
  const currentMmsiSet = new Set(shipsArray.map(s => s.mmsi));

  // usuń nieaktualne
  for (let mmsi in shipMarkers) {
    if (!currentMmsiSet.has(parseInt(mmsi))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      if (overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(line => map.removeLayer(line));
        delete overlayMarkers[mmsi];
      }
    }
  }

  shipsArray.forEach(ship => {
    let mmsi = ship.mmsi;
    let marker = shipMarkers[mmsi];
    let isSelected = selectedShips.includes(mmsi);

    // Ikona z common.js
    let icon = createShipIcon(ship, isSelected);

    // tooltip
    const now = Date.now();
    const upd = new Date(ship.timestamp).getTime();
    const diffSec = Math.floor((now - upd)/1000);
    const diffMin = Math.floor(diffSec/60);
    const diffS = diffSec%60;
    const diffStr = `${diffMin}m ${diffS}s ago`;

    let html = `
      <b>${ship.ship_name||'Unknown'}</b><br>
      MMSI: ${mmsi}<br>
      SOG: ${ship.sog||0} kn, COG: ${ship.cog||0}°<br>
      Len: ${ship.ship_length||'N/A'}<br>
      Updated: ${diffStr}
    `;

    if (!marker) {
      marker = L.marker([ship.latitude, ship.longitude], { icon })
        .on('click', () => selectShip(mmsi));
      marker.bindTooltip(html, { direction:'top', sticky:true });
      marker.shipData = ship;

      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(html);
      marker.shipData = ship;
    }
  });

  updateSelectedShipsInfo(false);
}

// ------------------ Pobieranie kolizji ------------------
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(r => r.json())
    .then(data => {
      collisionsData = data || [];
      updateCollisionsList();
    })
    .catch(err => console.error("Błąd /collisions:", err));
}

function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if (!collisionsData || collisionsData.length===0) {
    let div = document.createElement('div');
    div.classList.add('collision-item');
    div.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(div);
    return;
  }

  // Filtrowanie tylko tych, które jeszcze się nie rozwiązały
  // cpa <=0.5, tcpa >=0, np. c.timestamp>Now jeżeli chcemy TYLKO przyszłe
  let nowTime = Date.now();
  let filtered = collisionsData.filter(c => {
    if (c.tcpa < 0) return false;
    if (c.cpa > 0.5) return false;
    // jeśli chcesz odrzucić te, których timestamp < now (czyli "już w przeszłości"):
    // let collTime = new Date(c.timestamp).getTime();
    // if (collTime < nowTime) return false;
    return true;
  });

  if (filtered.length === 0) {
    let noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML='<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(noItem);
    return;
  }

  // Usuwanie duplikatów A-B/B-A
  let pairsMap = {};
  filtered.forEach(c => {
    let a = Math.min(c.mmsi_a,c.mmsi_b);
    let b = Math.max(c.mmsi_a,c.mmsi_b);
    let key = `${a}_${b}`;
    if(!pairsMap[key]) {
      pairsMap[key]=c;
    } else {
      // wybieraj np. najmniejsze cpa
      if(c.cpa < pairsMap[key].cpa){
        pairsMap[key] = c;
      } 
    }
  });
  let finalCollisions = Object.values(pairsMap);

  if(finalCollisions.length===0){
    let ndiv=document.createElement('div');
    ndiv.classList.add('collision-item');
    ndiv.innerHTML='<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(ndiv);
    return;
  }

  // Tworzenie itemów i markerów
  finalCollisions.forEach(c => {
    let aName = c.ship1_name || c.mmsi_a;
    let bName = c.ship2_name || c.mmsi_b;
    let cpaVal = c.cpa.toFixed(2);
    let tcpaVal= c.tcpa.toFixed(2);
    let dtStr='';
    if(c.timestamp){
      dtStr = new Date(c.timestamp).toLocaleTimeString('en-GB');
    }

    let div = document.createElement('div');
    div.classList.add('collision-item');
    div.innerHTML=`
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <strong>${aName} – ${bName}</strong><br>
          CPA: ${cpaVal} nm, TCPA: ${tcpaVal} min
          ${dtStr ? '@ '+dtStr : ''}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    collisionList.appendChild(div);

    div.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(c);
    });

    // Ikonka na mapie
    const latC=(c.latitude_a + c.latitude_b)/2;
    const lonC=(c.longitude_a + c.longitude_b)/2;
    let colIcon = L.divIcon({
      className:'',
      html: `
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <path d="M0,-7 7,7 -7,7 Z"
                fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="4" text-anchor="middle"
                font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize:[24,24],
      iconAnchor:[12,12]
    });
    let tip=`Możliwa kolizja: ${aName} & ${bName}, w ~${tcpaVal} min`;
    let colMarker=L.marker([latC,lonC],{icon:colIcon})
      .bindTooltip(tip,{direction:'top',sticky:true})
      .on('click',()=>zoomToCollision(c));
    colMarker.addTo(map);
    collisionMarkers.push(colMarker);
  });
}

// zoom do kolizji
function zoomToCollision(c){
  let bounds=L.latLngBounds([
    [c.latitude_a,c.longitude_a],
    [c.latitude_b,c.longitude_b]
  ]);
  map.fitBounds(bounds,{padding:[20,20]});
  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

// ------------------- Zaznaczanie statków -------------------
function selectShip(mmsi){
  if(!selectedShips.includes(mmsi)){
    if(selectedShips.length>=2){
      selectedShips.shift();
    }
    selectedShips.push(mmsi);
    updateSelectedShipsInfo(true);
  }
}
function clearSelectedShips(){
  selectedShips=[];
  for(let m in overlayMarkers){
    overlayMarkers[m].forEach(line=>map.removeLayer(line));
  }
  overlayMarkers={};
  updateSelectedShipsInfo(false);
}

// ------------------- Panel “Selected Ships” -------------------
function updateSelectedShipsInfo(selectionChanged){
  const container=document.getElementById('selected-ships-info');
  container.innerHTML='';
  document.getElementById('pair-info').innerHTML='';

  if(selectedShips.length===0){
    reloadAllShipIcons();
    return;
  }

  let shipsData=[];
  selectedShips.forEach(mmsi=>{
    if(shipMarkers[mmsi]?.shipData){
      shipsData.push(shipMarkers[mmsi].shipData);
    }
  });

  shipsData.forEach(sd=>{
    let d=document.createElement('div');
    d.innerHTML=`
      <b>${sd.ship_name||'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog||0} kn, COG: ${sd.cog||0}°<br>
      Len: ${sd.ship_length||'N/A'}
    `;
    container.appendChild(d);
  });

  // usuń wektory
  for(let m in overlayMarkers){
    overlayMarkers[m].forEach(l=>map.removeLayer(l));
  }
  overlayMarkers={};

  // narysuj wektory
  selectedShips.forEach(m=>drawVector(m));
  reloadAllShipIcons();

  // jeśli 2 statki => cpa/tcpa
  if(selectedShips.length===2){
    let sorted=[...selectedShips].sort((a,b)=>a-b);
    fetch(`/calculate_cpa_tcpa?mmsi_a=${sorted[0]}&mmsi_b=${sorted[1]}`)
      .then(r=>r.json())
      .then(data=>{
        if(data.error){
          document.getElementById('pair-info').innerHTML=`<b>CPA/TCPA:</b> N/A (${data.error})`;
        } else {
          let cpaVal=(data.cpa>=9999)?'n/a':data.cpa.toFixed(2);
          let tcpaVal=(data.tcpa<0)?'n/a':data.tcpa.toFixed(2);
          document.getElementById('pair-info').innerHTML=`
            <b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min
          `;
        }
      })
      .catch(err=>{
        console.error("Błąd /calculate_cpa_tcpa:",err);
        document.getElementById('pair-info').innerHTML='<b>CPA/TCPA:</b> N/A';
      });
  }
}

// ------------------- Rysowanie wektorów -------------------
function drawVector(mmsi){
  let mk=shipMarkers[mmsi];
  if(!mk||!mk.shipData)return;
  let sd=mk.shipData;
  if(!sd.sog||!sd.cog)return;

  let lat=sd.latitude;
  let lon=sd.longitude;
  let sog=sd.sog;
  let cogDeg=sd.cog;

  let distNm=sog*(vectorLength/60);
  let cogRad=(cogDeg*Math.PI)/180;
  let deltaLat=(distNm/60)*Math.cos(cogRad);
  let deltaLon=(distNm/60)*Math.sin(cogRad)/Math.cos(lat*Math.PI/180);

  let endLat=lat+deltaLat;
  let endLon=lon+deltaLon;

  let line=L.polyline([[lat,lon],[endLat,endLon]],{
    color:'blue',dashArray:'4,4'
  });
  line.addTo(map);

  if(!overlayMarkers[mmsi]) overlayMarkers[mmsi]=[];
  overlayMarkers[mmsi].push(line);
}

// ------------------- Odświeżanie ikon statków -------------------
function reloadAllShipIcons(){
  for(let m in shipMarkers){
    let mk=shipMarkers[m];
    if(!mk.shipData) continue;
    let isSelected=selectedShips.includes(parseInt(m));
    // Ponownie stwórz ikonę z common.js
    let newIcon=createShipIcon(mk.shipData, isSelected);
    mk.setIcon(newIcon);
  }
}