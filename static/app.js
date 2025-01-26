// ==========================
// app.js (moduł LIVE)
// ==========================
document.addEventListener('DOMContentLoaded', initLiveApp);

let map;
let markerClusterGroup;
let shipMarkers = {};
let overlayMarkers = {};
let selectedShips = [];
let collisionsData = [];
let collisionMarkers = [];

let vectorLength = 15;
let cpaFilter = 0.5;
let tcpaFilter = 10;

let shipsInterval = null;
let collisionsInterval = null;

function initLiveApp() {
  // 1) Mapa
  map = initSharedMap('map'); // z common.js

  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  map.on('zoomend', () => reloadAllShipIcons());

  // 2) UI
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

  // 3) Start fetch
  fetchShips();
  fetchCollisions();

  shipsInterval = setInterval(fetchShips, 30000);
  collisionsInterval = setInterval(fetchCollisions, 30000);
}

//-------------------------------------------
// A) /ships
//-------------------------------------------
function fetchShips() {
  fetch('/ships')
    .then(res => {
      if (!res.ok) throw new Error(`HTTP ${res.status} - ${res.statusText}`);
      const ctype = res.headers.get('content-type') || '';
      if (!ctype.includes('application/json')) {
        throw new Error(`Oczekiwano JSON, otrzymano: ${ctype}`);
      }
      return res.json();
    })
    .then(data => updateShips(data))
    .catch(err => console.error("Błąd /ships:", err));
}

function updateShips(shipsArray) {
  const currentSet = new Set(shipsArray.map(s => s.mmsi));

  for (let m in shipMarkers) {
    if (!currentSet.has(parseInt(m))) {
      markerClusterGroup.removeLayer(shipMarkers[m]);
      delete shipMarkers[m];
      if (overlayMarkers[m]) {
        overlayMarkers[m].forEach(ln => map.removeLayer(ln));
        delete overlayMarkers[m];
      }
    }
  }

  shipsArray.forEach(ship => {
    const mmsi = ship.mmsi;
    const isSelected = selectedShips.includes(mmsi);

    // UŻYWAMY createShipIcon z common.js
    const icon = createShipIcon(ship, isSelected, map.getZoom());

    const now = Date.now();
    const updTs = ship.timestamp ? new Date(ship.timestamp).getTime() : 0;
    const diffSec = Math.floor((now - updTs) / 1000);
    const mm = Math.floor(diffSec / 60);
    const ss = diffSec % 60;
    const diffStr = `${mm}m ${ss}s ago`;

    let dimsLen = null;
    if (ship.dim_a && ship.dim_b) {
      dimsLen = (parseFloat(ship.dim_a) + parseFloat(ship.dim_b)).toFixed(1);
    }

    const ttHTML = `
      <b>${ship.ship_name || 'Unknown'}</b><br>
      MMSI: ${mmsi}<br>
      SOG: ${ship.sog || 0} kn, COG: ${ship.cog || 0}°<br>
      Length: ${dimsLen || '??'}<br>
      Updated: ${diffStr}
    `;

    let marker = shipMarkers[mmsi];
    if (!marker) {
      marker = L.marker([ship.latitude, ship.longitude], { icon })
        .on('click', () => selectShip(mmsi));
      marker.bindTooltip(ttHTML, { direction:'top', sticky:true });
      marker.shipData = ship;

      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(ttHTML);
      marker.shipData = ship;
    }
  });

  updateSelectedShipsInfo(false);
}

//-------------------------------------------
// B) /collisions
//-------------------------------------------
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(res => {
      if (!res.ok) throw new Error(`HTTP ${res.status} - ${res.statusText}`);
      const ctype = res.headers.get('content-type') || '';
      if (!ctype.includes('application/json')) {
        throw new Error(`Oczekiwano JSON, otrzymano: ${ctype}`);
      }
      return res.json();
    })
    .then(data => {
      collisionsData = data || [];
      updateCollisionsList();
    })
    .catch(err => console.error("Błąd /collisions:", err));
}

function updateCollisionsList() {
  const collList = document.getElementById('collision-list');
  collList.innerHTML = '';
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if (!collisionsData || collisionsData.length === 0) {
    let noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>Brak bieżących kolizji</i>';
    collList.appendChild(noItem);
    return;
  }

  const nowMs = Date.now();
  let filtered = collisionsData.filter(c => {
    if (c.tcpa<0) return false;
    if (c.timestamp) {
      let collTime = new Date(c.timestamp).getTime();
      let futureMs = collTime + c.tcpa * 60000;
      if (futureMs < nowMs) return false;
    }
    return true;
  });

  if (filtered.length===0) {
    let d = document.createElement('div');
    d.classList.add('collision-item');
    d.innerHTML = '<i>Brak bieżących kolizji</i>';
    collList.appendChild(d);
    return;
  }

  let pairsMap = {};
  filtered.forEach(c => {
    let a = Math.min(c.mmsi_a,c.mmsi_b);
    let b = Math.max(c.mmsi_a,c.mmsi_b);
    let key = `${a}_${b}`;
    if (!pairsMap[key]) {
      pairsMap[key] = c;
    } else {
      let oldT = new Date(pairsMap[key].timestamp).getTime();
      let newT = new Date(c.timestamp).getTime();
      if (newT > oldT) {
        pairsMap[key] = c;
      }
    }
  });
  const finalCollisions = Object.values(pairsMap);

  if (finalCollisions.length===0) {
    let d2 = document.createElement('div');
    d2.classList.add('collision-item');
    d2.innerHTML = '<i>Brak bieżących kolizji</i>';
    collList.appendChild(d2);
    return;
  }

  finalCollisions.forEach(c => {
    let shipA = `Ship ${c.mmsi_a}`;
    let shipB = `Ship ${c.mmsi_b}`;
    let cpaStr = c.cpa.toFixed(2);
    let tcpaStr= c.tcpa.toFixed(2);

    // splitted circle z common.js
    const splittedHTML = getCollisionSplitCircle(c.mmsi_a, c.mmsi_b, 0, 0, shipMarkers);

    let timeStr = c.timestamp
      ? new Date(c.timestamp).toLocaleTimeString('pl-PL',{hour12:false})
      : '';

    let item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedHTML}
          <strong>${shipA} – ${shipB}</strong><br>
          CPA: ${cpaStr} nm, TCPA: ${tcpaStr} min
          ${timeStr?'@ '+timeStr:''}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    collList.appendChild(item);

    item.querySelector('.zoom-button').addEventListener('click',()=>{
      zoomToCollision(c);
    });

    let latC = (c.latitude_a + c.latitude_b)/2;
    let lonC = (c.longitude_a + c.longitude_b)/2;

    const collisionIcon = L.divIcon({
      className:'',
      html:`
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

    let tip=`Kolizja: ${shipA} & ${shipB}, ~${tcpaStr} min`;
    let marker = L.marker([latC, lonC], { icon: collisionIcon })
      .bindTooltip(tip, {direction:'top',sticky:true})
      .on('click',()=>zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

function zoomToCollision(c){
  let bounds = L.latLngBounds([
    [c.latitude_a,c.longitude_a],
    [c.latitude_b,c.longitude_b]
  ]);
  map.fitBounds(bounds,{padding:[15,15],maxZoom:13});

  if(c.tcpa && c.tcpa>0 && c.tcpa<9999){
    vectorLength=Math.round(c.tcpa);
    document.getElementById('vectorLengthSlider').value=vectorLength;
    document.getElementById('vectorLengthValue').textContent=vectorLength;
  }
  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

//-------------------------------------------
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
    overlayMarkers[m].forEach(ln=>map.removeLayer(ln));
  }
  overlayMarkers={};
  updateSelectedShipsInfo(false);
}

function updateSelectedShipsInfo(selectionChanged){
  const panel=document.getElementById('selected-ships-info');
  panel.innerHTML='';
  document.getElementById('pair-info').innerHTML='';

  if(selectedShips.length===0){
    reloadAllShipIcons();
    return;
  }
  let sData=[];
  selectedShips.forEach(m=>{
    if(shipMarkers[m]?.shipData){
      sData.push(shipMarkers[m].shipData);
    }
  });
  sData.forEach(sd=>{
    let dimsLen=null;
    if(sd.dim_a&&sd.dim_b){
      dimsLen=(parseFloat(sd.dim_a)+parseFloat(sd.dim_b)).toFixed(1);
    }
    const div=document.createElement('div');
    div.innerHTML=`
      <b>${sd.ship_name||'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog||0} kn, COG: ${sd.cog||0}°<br>
      Length: ${dimsLen||'N/A'}
    `;
    panel.appendChild(div);
  });

  for(let m in overlayMarkers){
    overlayMarkers[m].forEach(ln=>map.removeLayer(ln));
  }
  overlayMarkers={};

  selectedShips.forEach(m=>drawVector(m));
  reloadAllShipIcons();

  if(selectedShips.length===2){
    const [mA,mB]=selectedShips;
    let posA=shipMarkers[mA]?.shipData;
    let posB=shipMarkers[mB]?.shipData;
    let distNm=null;
    if(posA&&posB){
      distNm=computeDistanceNm(posA.latitude,posA.longitude,posB.latitude,posB.longitude);
    }
    const sorted=[mA,mB].sort((a,b)=>a-b);
    const url=`/calculate_cpa_tcpa?mmsi_a=${sorted[0]}&mmsi_b=${sorted[1]}`;
    fetch(url)
      .then(r=>r.json())
      .then(data=>{
        if(data.error){
          document.getElementById('pair-info').innerHTML=`
            ${distNm!==null?`<b>Distance:</b> ${distNm.toFixed(2)} nm<br>`:''}
            <b>CPA/TCPA:</b> N/A (${data.error})
          `;
        }else{
          let cpaVal=(data.cpa>=9999)?'n/a':data.cpa.toFixed(2);
          let tcpaVal=(data.tcpa<0)?'n/a':data.tcpa.toFixed(2);
          document.getElementById('pair-info').innerHTML=`
            ${distNm!==null?`<b>Distance:</b> ${distNm.toFixed(2)} nm<br>`:''}
            <b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min
          `;
        }
      })
      .catch(err=>{
        console.error("Błąd /calculate_cpa_tcpa:",err);
        document.getElementById('pair-info').innerHTML=`
          ${distNm!==null?`<b>Distance:</b> ${distNm.toFixed(2)} nm<br>`:''}
          <b>CPA/TCPA:</b> N/A
        `;
      });
  }
}

//-------------------------------------------
function computeDistanceNm(lat1,lon1,lat2,lon2){
  const R_nm=3440.065;
  const rad=Math.PI/180;
  const dLat=(lat2-lat1)*rad;
  const dLon=(lon2-lon1)*rad;
  const a=Math.sin(dLat/2)**2+
          Math.cos(lat1*rad)*Math.cos(lat2*rad)*Math.sin(dLon/2)**2;
  const c=2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a));
  return R_nm*c;
}

function drawVector(mmsi){
  let mk=shipMarkers[mmsi];
  if(!mk?.shipData) return;
  let sd=mk.shipData;
  if(!sd.sog||!sd.cog) return;

  let lat=sd.latitude;
  let lon=sd.longitude;
  let sog=sd.sog;
  let cogDeg=sd.cog;

  let distNm=sog*(vectorLength/60);
  let cogRad=(cogDeg*Math.PI)/180;

  let dLat=(distNm/60)*Math.cos(cogRad);
  let dLon=(distNm/60)*Math.sin(cogRad)/Math.cos(lat*Math.PI/180);

  let endLat=lat+dLat;
  let endLon=lon+dLon;

  let line=L.polyline([[lat,lon],[endLat,endLon]],{
    color:'blue',
    dashArray:'4,4'
  });
  line.addTo(map);
  if(!overlayMarkers[mmsi]) overlayMarkers[mmsi]=[];
  overlayMarkers[mmsi].push(line);
}

function reloadAllShipIcons(){
  let currentZoom=map.getZoom();
  for(let m in shipMarkers){
    let mk=shipMarkers[m];
    if(!mk.shipData) continue;
    let isSelected=selectedShips.includes(parseInt(m));
    // UŻYWAMY createShipIcon (z common.js)
    let newIcon=createShipIcon(mk.shipData,isSelected,currentZoom);
    mk.setIcon(newIcon);
  }
}