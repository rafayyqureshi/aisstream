// ==========================
// app.js  (Moduł LIVE)
// ==========================
let map;
let markerClusterGroup;
let shipMarkers = {};      // klucz = mmsi
let overlayMarkers = {};   // klucz = mmsi, tablica polylines
let selectedShips = [];

let collisionsData = [];
let collisionMarkers = [];

let vectorLength = 15;  // minuty do przodu (rysowanie wektora)
let cpaFilter = 0.5;
let tcpaFilter = 10;

// Interval IDs
let shipsInterval = null;
let collisionsInterval = null;

let lastSelectedPair = null;
let lastCpaTcpa = null;

function initMap() {
  map = L.map('map', {
    center: [50, 0],
    zoom: 5
  });

  // Warstwa OSM
  const osmLayer = L.tileLayer(
    'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    { maxZoom:18 }
  );
  osmLayer.addTo(map);

  // (opcjonalnie) warstwa nawigacyjna
  // let openSeaMap = L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png', {
  //   maxZoom: 18, opacity: 0.7
  // });
  // openSeaMap.addTo(map);

  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius:1 });
  map.addLayer(markerClusterGroup);

  // UI eventy
  document.getElementById('vectorLengthSlider').addEventListener('input', e=>{
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(false);
  });
  document.getElementById('cpaFilter').addEventListener('input', e=>{
    cpaFilter = parseFloat(e.target.value)||0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisions();
  });
  document.getElementById('tcpaFilter').addEventListener('input', e=>{
    tcpaFilter = parseFloat(e.target.value)||10;
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    fetchCollisions();
  });
  document.getElementById('clearSelectedShips').addEventListener('click', ()=>{
    clearSelectedShips();
  });

  // Pierwszy fetch
  fetchShips();
  fetchCollisions();

  // Cyklik
  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

function fetchShips() {
  fetch('/ships')
    .then(r=>r.json())
    .then(data=>updateShips(data))
    .catch(err=>console.error("Error /ships:", err));
}

function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(r=>r.json())
    .then(data=>{
      collisionsData = data;
      updateCollisionsList();
    })
    .catch(err=>console.error("Error /collisions:", err));
}

// ---------------------------
// Uaktualnianie statków
// ---------------------------
function updateShips(shipsArray) {
  let currentMmsiSet = new Set(shipsArray.map(s=>s.mmsi));

  // Usuwamy te statki, które zniknęły
  for (let mmsi in shipMarkers) {
    if(!currentMmsiSet.has(parseInt(mmsi))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      if(overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(o => map.removeLayer(o));
        delete overlayMarkers[mmsi];
      }
    }
  }

  // Dodaj / aktualizuj statki
  shipsArray.forEach(ship => {
    const mmsi = ship.mmsi;
    const length = ship.ship_length || null;
    const fillColor = getShipColor(length);
    const rotation = ship.cog || 0;

    const width = 16, height = 24;

    // highlight
    let highlightRect = '';
    if(selectedShips.includes(mmsi)) {
      highlightRect = `
        <rect x="-10" y="-10" width="20" height="20"
              fill="none" stroke="black" stroke-width="3"
              stroke-dasharray="5,5"/>
      `;
    }

    // Statek w formie trójkąta
    const shipSvg=`
      <polygon points="0,-8 6,8 -6,8"
               fill="${fillColor}" stroke="black" stroke-width="1"/>
    `;
    const icon = L.divIcon({
      className: '',
      html: `<svg width="${width}" height="${height}"
                  viewBox="-8 -8 16 16"
                  style="transform:rotate(${rotation}deg)">
               ${highlightRect}
               ${shipSvg}
             </svg>`,
      iconSize: [width, height],
      iconAnchor: [width/2, height/2]
    });

    // Tooltip
    const now = Date.now();
    const updatedAt = new Date(ship.timestamp).getTime();
    const diffSec = Math.round((now - updatedAt)/1000);
    const diffMin = Math.floor(diffSec/60);
    const diffS = diffSec % 60;
    const diffStr = `${diffMin}m ${diffS}s ago`;

    const tooltip = `
      <b>${ship.ship_name || 'Unknown'}</b><br>
      MMSI: ${mmsi}<br>
      SOG: ${ship.sog||0} kn, COG: ${ship.cog||0}°<br>
      Length: ${length||'N/A'}<br>
      Updated: ${diffStr}
    `;

    let marker = shipMarkers[mmsi];
    if(!marker) {
      // nowy statek
      marker = L.marker([ship.latitude, ship.longitude], {icon})
        .on('click', ()=>selectShip(mmsi));
      marker.bindTooltip(tooltip, {direction:'top', sticky:true});
      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      // aktualizacja
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(tooltip);
    }
    marker.shipData = ship;
  });

  updateSelectedShipsInfo(false);
}

function getShipColor(length) {
  if(length===null) return 'grey';
  if(length<50) return 'green';
  if(length<150) return 'yellow';
  if(length<250) return 'orange';
  return 'red';
}

// ---------------------------
// Kolizje (prawy panel)
// ---------------------------
function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];

  if(collisionsData.length===0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i style="padding:8px;">No collisions found</i>';
    collisionList.appendChild(noItem);
    return;
  }

  // usuwanie duplikatów np. według collision_id
  let uniqueMap = {};
  collisionsData.forEach(c => {
    if(!c.collision_id) {
      // awaryjnie utwórz key
      c.collision_id = `${c.mmsi_a}_${c.mmsi_b}_${c.timestamp||''}`;
    }
    if(!uniqueMap[c.collision_id]) {
      uniqueMap[c.collision_id] = c;
    } else {
      // ewentualnie sprawdzaj cpa i timestamp
      // tu prosto ignorujemy duplikat
    }
  });
  let filteredCollisions = Object.values(uniqueMap);

  filteredCollisions.forEach(c => {
    const item = document.createElement('div');
    item.classList.add('collision-item');

    let shipA = c.ship1_name || c.mmsi_a;
    let shipB = c.ship2_name || c.mmsi_b;
    let cpa = c.cpa.toFixed(2);
    let tcpa = c.tcpa.toFixed(2);

    // Rysujemy splitted circle, jeśli chcesz też kolory od length:
    // W tym zapytaniu collisions nie ma ship_length –
    // Musiałbyś je dołączyć (JOIN).
    // Zakładamy, że to jest w c np. c.ship1_length / c.ship2_length.
    // let colorA = getShipColor(c.ship1_length);
    // let colorB = getShipColor(c.ship2_length);
    // let splittedCircle = createSplittedCircle(colorA, colorB);

    item.innerHTML = `
      <b>${shipA} - ${shipB}</b><br>
      CPA: ${cpa} nm, TCPA: ${tcpa} min
      <button class="zoom-button">🔍</button>
    `;
    item.querySelector('.zoom-button').addEventListener('click', ()=>{
      zoomToCollision(c);
    });
    collisionList.appendChild(item);

    // marker kolizji
    const latC = (c.latitude_a + c.latitude_b)/2;
    const lonC = (c.longitude_a + c.longitude_b)/2;
    const collisionIcon = L.divIcon({
      className:'',
      html:`<svg width="20" height="20" viewBox="-10 -10 20 20">
              <path d="M0,-6 6,6 -6,6 Z"
                    fill="yellow" stroke="red" stroke-width="2"/>
              <text x="0" y="3" text-anchor="middle" font-size="8"
                    fill="red">!</text>
            </svg>`,
      iconSize:[20,20],
      iconAnchor:[10,10]
    });

    let marker = L.marker([latC, lonC], {icon: collisionIcon})
      .on('click', () => zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

function createSplittedCircle(colorA, colorB){
  return `
    <svg width="16" height="16" viewBox="0 0 16 16" style="vertical-align:middle;margin-right:6px;">
      <!-- lewa połówka -->
      <path d="M8,8 m-8,0 a8,8 0 0,1 16,0 z" fill="${colorA}"/>
      <!-- prawa połówka -->
      <path d="M8,8 m8,0 a8,8 0 0,1 -16,0 z" fill="${colorB}"/>
    </svg>
  `;
}

function zoomToCollision(c) {
  const bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, {padding: [50,50]});

  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
  // ewentualnie:
  // vectorLength = Math.max(vectorLength, Math.ceil(c.tcpa));
  // updateSelectedShipsInfo(true);
}

// ---------------------------
// Obsługa zaznaczonych statków
// ---------------------------
function selectShip(mmsi) {
  if(!selectedShips.includes(mmsi)) {
    if(selectedShips.length >= 2) {
      selectedShips.shift();
    }
    selectedShips.push(mmsi);
    updateSelectedShipsInfo(true);
  }
}

function clearSelectedShips() {
  selectedShips = [];
  lastSelectedPair = null;
  lastCpaTcpa = null;
  for(let m in overlayMarkers) {
    overlayMarkers[m].forEach(o => map.removeLayer(o));
  }
  overlayMarkers = {};
  updateSelectedShipsInfo(false);
}

function updateSelectedShipsInfo(selectionChanged) {
  const container = document.getElementById('selected-ships-info');
  container.innerHTML = '';
  document.getElementById('pair-info').innerHTML = '';

  if(selectedShips.length === 0) {
    reloadAllShipIcons();
    return;
  }

  let shipsData = [];
  selectedShips.forEach(mmsi => {
    if(shipMarkers[mmsi]?.shipData) {
      shipsData.push(shipMarkers[mmsi].shipData);
    }
  });

  shipsData.forEach(sd => {
    const div = document.createElement('div');
    div.innerHTML = `
      <b>${sd.ship_name||'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog||0} kn, COG: ${sd.cog||0}°<br>
      Length: ${sd.ship_length||'N/A'}
    `;
    container.appendChild(div);
  });

  // Rysuj wektory
  for(let m in overlayMarkers) {
    overlayMarkers[m].forEach(o => map.removeLayer(o));
  }
  overlayMarkers = {};

  selectedShips.forEach(m => drawVector(m));
  reloadAllShipIcons();

  // Jeśli mamy 2 statki, pobierz cpa/tcpa
  if(selectedShips.length === 2) {
    let sortedPair = [selectedShips[0], selectedShips[1]].sort((a,b)=>a-b);
    if(selectionChanged || !lastSelectedPair ||
      JSON.stringify(lastSelectedPair) != JSON.stringify(sortedPair)) {
      lastSelectedPair = sortedPair;
      lastCpaTcpa = null;
      fetch(`/calculate_cpa_tcpa?mmsi_a=${sortedPair[0]}&mmsi_b=${sortedPair[1]}`)
        .then(r=>r.json())
        .then(data=>{
          if(data.error) {
            document.getElementById('pair-info').innerHTML =
              `<b>CPA/TCPA:</b> N/A ( ${data.error} )`;
            lastCpaTcpa = { cpa:null, tcpa:null };
          } else {
            lastCpaTcpa = data;
            document.getElementById('pair-info').innerHTML = `
              <b>CPA/TCPA:</b> ${data.cpa.toFixed(2)} nm / ${data.tcpa.toFixed(2)} min
            `;
          }
        })
        .catch(err=>{
          console.error("Error /calculate_cpa_tcpa:", err);
          document.getElementById('pair-info').innerHTML =
            '<b>CPA/TCPA:</b> N/A';
        });
    } else if(lastCpaTcpa) {
      // odśwież cpa/tcpa z lastCpaTcpa
      if(!lastCpaTcpa.cpa || !lastCpaTcpa.tcpa) {
        document.getElementById('pair-info').innerHTML =
          '<b>CPA/TCPA:</b> N/A';
      } else {
        document.getElementById('pair-info').innerHTML = `
          <b>CPA/TCPA:</b> ${lastCpaTcpa.cpa.toFixed(2)} nm /
          ${lastCpaTcpa.tcpa.toFixed(2)} min
        `;
      }
    }
  }
}

function reloadAllShipIcons() {
  for(let m in shipMarkers) {
    let marker = shipMarkers[m];
    let sd = marker.shipData;
    let length = sd.ship_length||null;
    let fillColor = getShipColor(length);
    let rotation = sd.cog||0;
    let width=16, height=24;
    let highlightRect='';

    if(selectedShips.includes(parseInt(m))) {
      highlightRect = `
        <rect x="-10" y="-10" width="20" height="20"
              fill="none" stroke="black" stroke-width="3"
              stroke-dasharray="5,5"/>
      `;
    }

    const shipSvg = `
      <polygon points="0,-8 6,8 -6,8"
               fill="${fillColor}" stroke="black" stroke-width="1"/>
    `;
    const icon = L.divIcon({
      className:'',
      html:`<svg width="${width}" height="${height}"
                 viewBox="-8 -8 16 16"
                 style="transform:rotate(${rotation}deg)">
              ${highlightRect}
              ${shipSvg}
            </svg>`,
      iconSize:[width, height],
      iconAnchor:[width/2, height/2]
    });
    marker.setIcon(icon);
  }
}

// Rysowanie wektora
function drawVector(mmsi) {
  let marker = shipMarkers[mmsi];
  if(!marker) return;
  let sd = marker.shipData;
  if(!sd.sog || !sd.cog) return;

  let lat = sd.latitude;
  let lon = sd.longitude;
  let sog = sd.sog; // nm/h
  let cogDeg = sd.cog;

  let distanceNm = sog * (vectorLength / 60.0);
  let cogRad = (cogDeg * Math.PI)/180;

  let deltaLat = (distanceNm/60)*Math.cos(cogRad);
  let deltaLon = (distanceNm/60)*Math.sin(cogRad) / Math.cos(lat*Math.PI/180);

  let endLat = lat + deltaLat;
  let endLon = lon + deltaLon;

  let line = L.polyline([[lat, lon],[endLat, endLon]], {
    color:'blue',
    dashArray:'4,4'
  });
  line.addTo(map);

  if(!overlayMarkers[mmsi]) overlayMarkers[mmsi] = [];
  overlayMarkers[mmsi].push(line);
}

// start
document.addEventListener('DOMContentLoaded', initMap);