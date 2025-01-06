// ==========================
// app.js  (Moduł LIVE)
// ==========================
let map;
let markerClusterGroup;
let shipMarkers = {};      // klucz = mmsi
let overlayMarkers = {};   // klucz = mmsi -> tablica L.polyline (wektory)
let selectedShips = [];

let collisionsData = [];
let collisionMarkersMap = {}; // klucz = collision_id -> L.marker
let vectorLength = 15;        // domyślnie 15 min
let cpaFilter = 0.5;
let tcpaFilter = 10;

// Timers
let shipsInterval = null;
let collisionsInterval = null;

// Pamiętamy ostatnią parę, by unikać zbędnych fetch
let lastSelectedPair = null;
let lastCpaTcpa = null;

function initMap() {
  map = L.map('map', {
    center: [50, 0],
    zoom: 5
  });

  // Warstwa OSM
  const osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom:18
  });
  osmLayer.addTo(map);

  // Warstwa nawigacyjna (OpenSeaMap)
  let openSeaMap = L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png', {
     maxZoom:18,
     opacity:0.7
   });
  // openSeaMap.addTo(map);

  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius:1 });
  map.addLayer(markerClusterGroup);

  // Suwaki i przyciski
  document.getElementById('vectorLengthSlider').addEventListener('input', e=>{
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(false);
  });

  // Zakładamy, że w HTML ustawiłeś min=0, max=0.5, step=0.01 dla cpa
  document.getElementById('cpaFilter').addEventListener('input', e=>{
    cpaFilter = parseFloat(e.target.value)||0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisions();
  });
  // Zakładamy, że w HTML min=0, max=10 dla tcpa
  document.getElementById('tcpaFilter').addEventListener('input', e=>{
    tcpaFilter = parseFloat(e.target.value)||10;
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    fetchCollisions();
  });

  document.getElementById('clearSelectedShips').addEventListener('click', ()=>{
    clearSelectedShips();
  });

  // Pierwsze pobranie
  fetchShips();
  fetchCollisions();

  // Ustaw interwały
  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

// Pobranie statków
function fetchShips() {
  fetch('/ships')
    .then(r=>r.json())
    .then(data=>{
      updateShips(data);
    })
    .catch(err=>console.error("Error fetching /ships:", err));
}

// Pobranie kolizji
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(r=>r.json())
    .then(data=>{
      collisionsData = data;
      updateCollisionsList();
    })
    .catch(err=>console.error("Error fetching /collisions:", err));
}

// ---------------------------
//  Aktualizacja statków
// ---------------------------
function updateShips(shipsArray) {
  let currentMmsiSet = new Set(shipsArray.map(s=>s.mmsi));

  // Usuń statki, których nie ma
  for(let mmsi in shipMarkers) {
    if(!currentMmsiSet.has(parseInt(mmsi))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      if(overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(line => map.removeLayer(line));
        delete overlayMarkers[mmsi];
      }
    }
  }

  // Dodaj/aktualizuj
  shipsArray.forEach(ship=>{
    const mmsi = ship.mmsi;
    const length = ship.ship_length||null;
    const fillColor = getShipColor(length);
    const rotation = ship.cog||0;
    const width=16, height=24;

    let highlightRect='';
    if(selectedShips.includes(mmsi)) {
      highlightRect=`
        <rect x="-10" y="-10" width="20" height="20"
              fill="none" stroke="black" stroke-width="3"
              stroke-dasharray="5,5"/>
      `;
    }

    // Statek = trójkąt
    const shipSvg=`
      <polygon points="0,-8 6,8 -6,8"
               fill="${fillColor}" stroke="black" stroke-width="1"/>
    `;

    const icon=L.divIcon({
      className:'',
      html:`<svg width="${width}" height="${height}" viewBox="-8 -8 16 16"
                 style="transform:rotate(${rotation}deg)">
              ${highlightRect}
              ${shipSvg}
            </svg>`,
      iconSize:[width,height],
      iconAnchor:[width/2,height/2]
    });

    // Tooltip – czas od aktualizacji
    let now=Date.now();
    let updatedAt=new Date(ship.timestamp).getTime();
    let diffSec = Math.round((now - updatedAt)/1000);
    let diffMin = Math.floor(diffSec/60);
    let diffS   = diffSec % 60;
    let diffStr = `${diffMin}m ${diffS}s ago`;

    let tooltip=`
      <b>${ship.ship_name||'Unknown'}</b><br>
      MMSI: ${mmsi}<br>
      SOG: ${ship.sog||0} kn, COG: ${ship.cog||0}°<br>
      Length: ${length||'N/A'}<br>
      Updated: ${diffStr}
    `;

    let marker=shipMarkers[mmsi];
    if(!marker) {
      // nowy
      marker=L.marker([ship.latitude, ship.longitude], {icon:icon})
        .on('click', ()=>selectShip(mmsi));
      marker.bindTooltip(tooltip,{direction:'top',sticky:true});
      shipMarkers[mmsi]=marker;
      markerClusterGroup.addLayer(marker);
    } else {
      // aktualizacja
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(tooltip);
    }
    marker.shipData = ship;
  });

  // Odśwież info
  updateSelectedShipsInfo(false);
}

function getShipColor(length){
  if(length===null) return 'grey';
  if(length<50) return 'green';
  if(length<150) return 'yellow';
  if(length<250) return 'orange';
  return 'red';
}

// ---------------------------
//  Lista kolizji (prawy panel)
// ---------------------------
function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML='';

  // Usuwamy stare markery
  for(let cid in collisionMarkersMap) {
    map.removeLayer(collisionMarkersMap[cid]);
  }
  collisionMarkersMap={};

  if(!collisionsData || collisionsData.length===0) {
    let noItem=document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML='<i style="padding:8px;">No collisions found</i>';
    collisionList.appendChild(noItem);
    return;
  }

  // Eliminacja duplikatów A-B i B-A
  let unique = {};
  collisionsData.forEach(col=>{
    // Tworzymy klucz np. minimalny mmsi
    let pair = [col.mmsi_a, col.mmsi_b].sort((a,b)=>a-b);
    let key = `${pair[0]}_${pair[1]}_${col.timestamp||''}`;
    if(!unique[key]) {
      unique[key] = col;
    } else {
      // if chcemy, to decydujemy, który col jest lepszy
    }
  });
  let filteredCollisions = Object.values(unique);

  filteredCollisions.forEach(c=>{
    const item=document.createElement('div');
    item.classList.add('collision-item');

    let shipA = c.ship1_name || c.mmsi_a;
    let shipB = c.ship2_name || c.mmsi_b;
    let cpaVal = c.cpa.toFixed(2);
    let tcpaVal = c.tcpa.toFixed(2);

    // splitted circle? Potrzebne ship1_length, ship2_length
    // let splitted = createSplittedCircle(getShipColor(...), ...);

    item.innerHTML=`
      <b>${shipA} - ${shipB}</b><br>
      CPA: ${cpaVal} nm, TCPA: ${tcpaVal} min
      <button class="zoom-button">🔍</button>
    `;
    item.querySelector('.zoom-button').addEventListener('click', ()=>{
      zoomToCollision(c);
    });
    collisionList.appendChild(item);

    // Ikona kolizji – wieksza i tooltip
    let latC=(c.latitude_a + c.latitude_b)/2;
    let lonC=(c.longitude_a + c.longitude_b)/2;
    let collisionIcon = L.divIcon({
      className:'',
      html:`
        <svg width="30" height="30" viewBox="-15 -15 30 30">
          <circle cx="0" cy="0" r="12" fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="5" text-anchor="middle" font-size="12"
                fill="red" font-weight="bold">!</text>
        </svg>
      `,
      iconSize:[30,30],
      iconAnchor:[15,15]
    });
    let marker=L.marker([latC, lonC], {icon:collisionIcon});
    marker.bindTooltip(`
      Potential collision:
      <b>${shipA}</b> & <b>${shipB}</b><br>
      CPA: ${cpaVal} nm, TCPA: ${tcpaVal} min
    `, {direction:'top',sticky:true});
    marker.on('click', ()=>zoomToCollision(c));
    marker.addTo(map);
    if(c.collision_id) {
      collisionMarkersMap[c.collision_id] = marker;
    } else {
      collisionMarkersMap[key] = marker;
    }
  });
}

// (Przykład splitted circle)
function createSplittedCircle(colorA, colorB){
  return `
    <svg width="16" height="16" viewBox="0 0 16 16"
         style="vertical-align:middle; margin-right:6px;">
      <path d="M8,8 m-8,0 a8,8 0 0,1 16,0 z" fill="${colorA}"/>
      <path d="M8,8 m8,0 a8,8 0 0,1 -16,0 z" fill="${colorB}"/>
    </svg>
  `;
}

// Zoom
function zoomToCollision(c) {
  let bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, {padding:[50,50]});

  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

// ---------------------------
// Wybor statków (lewy panel)
// ---------------------------
function selectShip(mmsi) {
  if(!selectedShips.includes(mmsi)) {
    if(selectedShips.length>=2) {
      selectedShips.shift();
    }
    selectedShips.push(mmsi);
    updateSelectedShipsInfo(true);
  }
}

function clearSelectedShips() {
  selectedShips=[];
  lastSelectedPair=null;
  lastCpaTcpa=null;
  for(let m in overlayMarkers) {
    overlayMarkers[m].forEach(o=>map.removeLayer(o));
  }
  overlayMarkers={};
  updateSelectedShipsInfo(false);
}

function updateSelectedShipsInfo(selectionChanged) {
  const container=document.getElementById('selected-ships-info');
  container.innerHTML='';
  document.getElementById('pair-info').innerHTML='';

  if(selectedShips.length===0) {
    reloadAllShipIcons();
    return;
  }

  let shipsData=[];
  selectedShips.forEach(m=>{
    if(shipMarkers[m]?.shipData) {
      shipsData.push(shipMarkers[m].shipData);
    }
  });

  // Wypisz info
  shipsData.forEach(sd=>{
    const div=document.createElement('div');
    div.innerHTML=`
      <b>${sd.ship_name||'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog||0} kn, COG: ${sd.cog||0}°<br>
      Length: ${sd.ship_length||'N/A'}
    `;
    container.appendChild(div);
  });

  // Rysuj wektory
  for(let m in overlayMarkers) {
    overlayMarkers[m].forEach(v=>map.removeLayer(v));
  }
  overlayMarkers={};

  selectedShips.forEach(m=>drawVector(m));
  reloadAllShipIcons();

  // jesli 2 statki -> cpa/tcpa
  if(selectedShips.length===2) {
    let sortedPair=[selectedShips[0],selectedShips[1]].sort((a,b)=>a-b);
    if(selectionChanged ||
       !lastSelectedPair ||
       JSON.stringify(lastSelectedPair)!=JSON.stringify(sortedPair)) {
      lastSelectedPair=sortedPair;
      lastCpaTcpa=null;
      fetch(`/calculate_cpa_tcpa?mmsi_a=${sortedPair[0]}&mmsi_b=${sortedPair[1]}`)
        .then(r=>r.json())
        .then(data=>{
          if(data.error) {
            document.getElementById('pair-info').innerHTML=
              `<b>CPA/TCPA:</b> n/a (${data.error})`;
            lastCpaTcpa={cpa:null, tcpa:null};
          } else {
            // warunek cpa>=9999 lub tcpa<0 => n/a
            if(data.cpa>=9999 || data.tcpa<0) {
              document.getElementById('pair-info').innerHTML=
                `<b>CPA/TCPA:</b> n/a (ships diverging)`;
              lastCpaTcpa={cpa:9999, tcpa:-1};
            } else if(data.cpa>10 || data.tcpa>600) {
              // arbitralnie, powyzej 10 nm czy 600 min => n/a
              document.getElementById('pair-info').innerHTML=
                `<b>CPA/TCPA:</b> n/a (too large)`;
              lastCpaTcpa={cpa:data.cpa, tcpa:data.tcpa};
            } else {
              lastCpaTcpa=data;
              document.getElementById('pair-info').innerHTML=`
                <b>CPA/TCPA:</b> ${data.cpa.toFixed(2)} nm /
                ${data.tcpa.toFixed(2)} min
              `;
            }
          }
        })
        .catch(err=>{
          console.error("Error /calculate_cpa_tcpa:",err);
          document.getElementById('pair-info').innerHTML= `<b>CPA/TCPA:</b> n/a`;
        });
    } else if(lastCpaTcpa) {
      // jesli mamy zapamietane
      if(lastCpaTcpa.cpa>=9999 || lastCpaTcpa.tcpa<0) {
        document.getElementById('pair-info').innerHTML= `<b>CPA/TCPA:</b> n/a (diverging)`;
      } else if(lastCpaTcpa.cpa>10 || lastCpaTcpa.tcpa>600) {
        document.getElementById('pair-info').innerHTML= `<b>CPA/TCPA:</b> n/a (too large)`;
      } else {
        document.getElementById('pair-info').innerHTML=`
          <b>CPA/TCPA:</b> ${lastCpaTcpa.cpa.toFixed(2)} nm /
          ${lastCpaTcpa.tcpa.toFixed(2)} min
        `;
      }
    }
  }
}

function reloadAllShipIcons() {
  for(let mmsi in shipMarkers) {
    let marker=shipMarkers[mmsi];
    let sd=marker.shipData;
    let length=sd.ship_length||null;
    let fillColor=getShipColor(length);
    let rotation=sd.cog||0;
    let width=16, height=24;

    let highlightRect='';
    if(selectedShips.includes(parseInt(mmsi))) {
      highlightRect=`
        <rect x="-10" y="-10" width="20" height="20"
              fill="none" stroke="black" stroke-width="3"
              stroke-dasharray="5,5"/>
      `;
    }

    const shipSvg=`
      <polygon points="0,-8 6,8 -6,8"
               fill="${fillColor}" stroke="black" stroke-width="1"/>
    `;
    const icon=L.divIcon({
      className:'',
      html:`<svg width="${width}" height="${height}" viewBox="-8 -8 16 16"
                 style="transform:rotate(${rotation}deg)">
              ${highlightRect}
              ${shipSvg}
            </svg>`,
      iconSize:[width,height],
      iconAnchor:[width/2,height/2]
    });
    marker.setIcon(icon);
  }
}

// Rysowanie wektora
function drawVector(mmsi) {
  let marker=shipMarkers[mmsi];
  if(!marker) return;
  let sd=marker.shipData;
  if(!sd.sog || !sd.cog) return;

  let lat=sd.latitude;
  let lon=sd.longitude;
  let sog=sd.sog; // nm/h
  let cogDeg=sd.cog;

  let distanceNm=sog*(vectorLength/60.0);
  let cogRad=(cogDeg*Math.PI)/180;

  let deltaLat=(distanceNm/60)*Math.cos(cogRad);
  let deltaLon=(distanceNm/60)*Math.sin(cogRad)/Math.cos(lat*Math.PI/180);

  let endLat=lat+deltaLat;
  let endLon=lon+deltaLon;

  let line=L.polyline([[lat,lon],[endLat,endLon]], {
    color:'blue',
    dashArray:'4,4'
  });
  line.addTo(map);

  if(!overlayMarkers[mmsi]) overlayMarkers[mmsi]=[];
  overlayMarkers[mmsi].push(line);
}

// Start
document.addEventListener('DOMContentLoaded', initMap);