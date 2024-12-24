// ==========================
// app.js  (Moduł LIVE)
// ==========================
let map;
let markerClusterGroup;
let shipMarkers = {};      // klucz = mmsi, wartość = L.marker
let overlayMarkers = {};   // klucz = mmsi, wartość = tablica polylines wektorów
let selectedShips = [];

let collisionsData = [];
let collisionMarkers = [];

let vectorLength = 15;   // do rysowania wektorów
let cpaFilter = 0.5;
let tcpaFilter = 10;

// Interval IDs
let shipsInterval = null;
let collisionsInterval = null;

function initMap() {
  map = L.map('map', {
    center: [50, 0],  // dowolne koordy startowe
    zoom: 5
  });

  // Podstawowe warstwy
  const osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18
  });
  osmLayer.addTo(map);

  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // UI eventy
  document.getElementById('vectorLengthSlider').addEventListener('input', (e)=>{
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(false);
  });
  document.getElementById('cpaFilter').addEventListener('input', (e)=>{
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisions();  // odśwież listę kolizji
  });
  document.getElementById('tcpaFilter').addEventListener('input', (e)=>{
    tcpaFilter = parseFloat(e.target.value) || 10;
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    fetchCollisions();
  });
  document.getElementById('clearSelectedShips').addEventListener('click', ()=>{
    clearSelectedShips();
  });

  // uruchamiamy początkowe fetch
  fetchShips();
  fetchCollisions();

  // odśwież co 60s
  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

function fetchShips() {
  fetch('/ships')
    .then(res => res.json())
    .then(data => {
      updateShips(data);
    })
    .catch(err => console.error("Error fetching /ships:", err));
}

function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(res => res.json())
    .then(data => {
      collisionsData = data;
      updateCollisionsList();
    })
    .catch(err => console.error("Error fetching /collisions:", err));
}

// -------------------------------------------------------------------
// Uaktualnianie statków
// -------------------------------------------------------------------
function updateShips(shipsArray) {
  // Zbiór mmsi wszystkich statków z /ships
  let mmsiSet = new Set(shipsArray.map(s=>s.mmsi));

  // usuwamy te, których już nie ma
  for (let mmsi in shipMarkers) {
    if(!mmsiSet.has(parseInt(mmsi))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      if(overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(h => map.removeLayer(h));
        delete overlayMarkers[mmsi];
      }
    }
  }

  shipsArray.forEach(ship => {
    const mmsi = ship.mmsi;
    const length = ship.ship_length || null;
    const fillColor = getShipColor(length);
    const scale = 1.0; // ewentualnie skalujesz ikony wg length
    const rotation = ship.cog || 0;

    const width = 16*scale;
    const height = 24*scale;
    const svgShape = `<polygon points="0,-8 6,8 -6,8" fill="${fillColor}" stroke="black" stroke-width="1"/>`;

    // ewentualny highlight
    let highlightRect = '';
    if(selectedShips.includes(mmsi)) {
      highlightRect = `<rect x="-10" y="-10" width="20" height="20" fill="none" stroke="black" stroke-width="3" stroke-dasharray="5,5"/>`;
    }

    const icon = L.divIcon({
      className: '',
      html: `<svg width="${width}" height="${height}" viewBox="-8 -8 16 16" style="transform:rotate(${rotation}deg)">
               ${highlightRect}
               ${svgShape}
             </svg>`,
      iconSize: [width, height],
      iconAnchor: [width/2, height/2]
    });

    // tooltip text
    const now = Date.now();
    const updatedAt = new Date(ship.timestamp).getTime();
    const diffSec = Math.round((now - updatedAt)/1000);
    const diffMin = Math.floor(diffSec/60);
    const diffS   = diffSec % 60;
    const diffStr = `${diffMin}m ${diffS}s ago`;
    const tooltipHTML = `
      <b>${ship.ship_name || 'Unknown'}</b><br>
      MMSI: ${mmsi}<br>
      SOG: ${ship.sog||0} kn, COG: ${ship.cog||0}°<br>
      Length: ${length||'N/A'}<br>
      Updated: ${diffStr}
    `;

    let marker = shipMarkers[mmsi];
    if(!marker) {
      // nowy statek
      marker = L.marker([ship.latitude, ship.longitude], {icon: icon})
        .on('click', ()=> selectShip(mmsi));
      marker.bindTooltip(tooltipHTML, {direction:'top', sticky:true});
      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      // aktualizacja istniejącego
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(tooltipHTML);
    }
    marker.shipData = ship; // przechowujemy sobie dane
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

// -------------------------------------------------------------------
// Kolizje w bocznym panelu
// -------------------------------------------------------------------
function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  // usuń stare markery
  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers = [];

  if(collisionsData.length===0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = `<div style="padding:8px;font-style:italic;">No collisions found</div>`;
    collisionList.appendChild(noItem);
    return;
  }

  collisionsData.forEach(col => {
    const item = document.createElement('div');
    item.classList.add('collision-item');
    const shipA = col.ship1_name || col.mmsi_a;
    const shipB = col.ship2_name || col.mmsi_b;

    item.innerHTML = `
      <b>${shipA} - ${shipB}</b><br>
      CPA: ${col.cpa.toFixed(2)} nm, TCPA: ${col.tcpa.toFixed(2)} min
      <button class="zoom-button">🔍</button>
    `;
    item.querySelector('.zoom-button').addEventListener('click', ()=>{
      zoomToCollision(col);
    });
    collisionList.appendChild(item);

    // Rysuj marker
    const collisionLat = (col.latitude_a + col.latitude_b)/2;
    const collisionLon = (col.longitude_a + col.longitude_b)/2;
    const collisionIcon = L.divIcon({
      className:'',
      html:`<svg width="20" height="20" viewBox="-10 -10 20 20">
             <polygon points="0,-6 6,6 -6,6" fill="yellow" stroke="red" stroke-width="2"/>
             <text x="0" y="3" text-anchor="middle" font-size="8" fill="red">!</text>
            </svg>`,
      iconSize:[20,20],
      iconAnchor:[10,10]
    });
    let marker = L.marker([collisionLat, collisionLon], {icon:collisionIcon})
      .on('click', ()=>zoomToCollision(col));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

function zoomToCollision(c) {
  const bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, {padding: [50,50]});

  // ewentualnie zaznacz statki
  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);

  // możesz np. ustawić vectorLength = Math.ceil(c.tcpa)...
  // i updateSelectedShipsInfo(true)
}

// -------------------------------------------------------------------
// Obsługa zaznaczonych statków
// -------------------------------------------------------------------
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
  for(let m in overlayMarkers) {
    overlayMarkers[m].forEach(o=>map.removeLayer(o));
  }
  overlayMarkers={};
  updateSelectedShipsInfo(false);
}

function updateSelectedShipsInfo(selectionChanged) {
  const container = document.getElementById('selected-ships-info');
  container.innerHTML = '';

  if(selectedShips.length===0) {
    reloadAllShipIcons();
    return;
  }

  // Wyświetl info o statkach
  selectedShips.forEach(mmsi=>{
    let marker = shipMarkers[mmsi];
    if(marker && marker.shipData) {
      let sd = marker.shipData;
      const div = document.createElement('div');
      div.innerHTML=`
        <b>${sd.ship_name||'Unknown'}</b><br>
        MMSI:${sd.mmsi}<br>
        SOG:${sd.sog||'N/A'} kn, COG:${sd.cog||'N/A'}<br>
        Length:${sd.ship_length||'N/A'}
      `;
      container.appendChild(div);
    }
  });

  // rysuj wektory
  for(let m in overlayMarkers) {
    overlayMarkers[m].forEach(o=>map.removeLayer(o));
  }
  overlayMarkers={};

  selectedShips.forEach(mmsi=>{
    drawVector(mmsi);
  });

  reloadAllShipIcons(); // odśwież ikony (dodaje highlightRect)
}

function reloadAllShipIcons() {
  for (let mmsi in shipMarkers) {
    let marker = shipMarkers[mmsi];
    let sd = marker.shipData;
    let length = sd.ship_length || null;
    let fillColor = getShipColor(length);
    let scale=1.0;
    let rotation = sd.cog||0;

    const width=16*scale;
    const height=24*scale;
    let highlightRect='';
    if(selectedShips.includes(parseInt(mmsi))) {
      highlightRect=`<rect x="-10" y="-10" width="20" height="20" fill="none" stroke="black" stroke-width="3" stroke-dasharray="5,5"/>`;
    }
    const svgShape=`<polygon points="0,-8 6,8 -6,8" fill="${fillColor}" stroke="black" stroke-width="1"/>`;

    const icon = L.divIcon({
      className:'',
      html:`<svg width="${width}" height="${height}" viewBox="-8 -8 16 16" style="transform:rotate(${rotation}deg)">
              ${highlightRect}
              ${svgShape}
            </svg>`,
      iconSize:[width,height],
      iconAnchor:[width/2,height/2]
    });
    marker.setIcon(icon);
  }
}

// rysujemy wektor
function drawVector(mmsi) {
  const marker = shipMarkers[mmsi];
  if(!marker) return;
  let sd = marker.shipData;
  if(!sd.sog || !sd.cog) return;

  let lat = sd.latitude;
  let lon = sd.longitude;
  let sog = sd.sog; // nm/h
  let cogDeg = sd.cog;

  // wyliczamy kilkuminutowy wektor
  let distanceNm = sog*(vectorLength/60.0); // sog (nm/h) * (vectorLength min / 60)
  let cogRad = cogDeg*(Math.PI/180);

  let deltaLat = (distanceNm/60)*Math.cos(cogRad); 
  let deltaLon = (distanceNm/60)*Math.sin(cogRad)/Math.cos(lat*Math.PI/180);

  let endLat = lat+deltaLat;
  let endLon = lon+deltaLon;

  let line = L.polyline([[lat,lon],[endLat,endLon]], {color:'blue', dashArray:'4,4'});
  line.addTo(map);
  if(!overlayMarkers[mmsi]) overlayMarkers[mmsi]=[];
  overlayMarkers[mmsi].push(line);
}

// Start
document.addEventListener('DOMContentLoaded', initMap);