/* ---------------------------------------------------------
   app.js - moduł LIVE
   --------------------------------------------------------- */

// Zmienne globalne do obsługi mapy i danych
let map;
let markerClusterGroup;
let shipMarkers = {};    // klucz = mmsi
let overlayMarkers = {}; // klucz = mmsi, tablica polylines (wektor prędkości)
let selectedShips = [];

let collisionsData = [];
let collisionMarkers = [];

let vectorLength = 15;  // (min) do rysowania wektora prędkości
let cpaFilter = 0.5;
let tcpaFilter = 10;

// Timery auto-odświeżenia
let shipsInterval = null;
let collisionsInterval = null;

// Ostatnio wybrana para statków (aby uniknąć zbędnych fetch co klik)
let lastSelectedPair = null;
let lastCpaTcpa = null;

/* ---------------------------------------------------------
   initMap() - główna inicjalizacja mapy i eventów
--------------------------------------------------------- */
function initMap() {
  // 1) Tworzymy mapę Leaflet
  map = L.map('map', {
    center: [50, 0],
    zoom: 5
  });

  // 2) Warstwa bazowa OSM
  const osmLayer = L.tileLayer(
    'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    { maxZoom:18 }
  );
  osmLayer.addTo(map);

  // 2a) (Opcjonalnie) Warstwa nawigacyjna z OpenSeaMap:
  // let openSeaMap = L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png', {
  //   maxZoom:18, opacity:0.7
  // });
  // openSeaMap.addTo(map);

  // 3) MarkerCluster do grupowania statków
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius:1 });
  map.addLayer(markerClusterGroup);

  // 4) UI eventy
  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(false); // odśwież wektory
  });

  document.getElementById('cpaFilter').addEventListener('input', e => {
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(1);
    fetchCollisions(); 
  });

  document.getElementById('tcpaFilter').addEventListener('input', e => {
    tcpaFilter = parseFloat(e.target.value) || 10;
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(0);
    fetchCollisions();
  });

  document.getElementById('clearSelectedShips').addEventListener('click', () => {
    clearSelectedShips();
  });

  // 5) Pierwsze pobranie danych
  fetchShips();
  fetchCollisions();

  // 6) Odśwież co minutę
  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

/* ---------------------------------------------------------
   fetchShips() - pobiera listę statków /ships
--------------------------------------------------------- */
function fetchShips() {
  fetch('/ships')
    .then(resp => resp.json())
    .then(data => updateShips(data))
    .catch(err => console.error('Error fetching ships:', err));
}

/* ---------------------------------------------------------
   fetchCollisions() - pobiera kolizje /collisions
   z parametrami cpaFilter i tcpaFilter
--------------------------------------------------------- */
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(resp => resp.json())
    .then(data => {
      collisionsData = data || [];
      updateCollisionsList();
    })
    .catch(err => console.error('Error fetching collisions:', err));
}

/* ---------------------------------------------------------
   updateShips(shipsArray) - aktualizuje statki na mapie
--------------------------------------------------------- */
function updateShips(shipsArray) {
  // 1) Zbiór MMSI z nowego fetch
  const currentMmsiSet = new Set(shipsArray.map(s => s.mmsi));

  // 2) Usuwamy statki, których nie ma
  for (let mmsi in shipMarkers) {
    if(!currentMmsiSet.has(parseInt(mmsi))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      // usuń wektory
      if(overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(o => map.removeLayer(o));
        delete overlayMarkers[mmsi];
      }
    }
  }

  // 3) Dodaj lub aktualizuj statki
  shipsArray.forEach(ship => {
    const mmsi = ship.mmsi;
    const length = ship.ship_length || null;
    const fillColor = getShipColor(length);
    const rotation = ship.cog || 0;

    const iconWidth = 16, iconHeight = 24;
    let highlightRect = '';
    if(selectedShips.includes(mmsi)) {
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
      className: '',
      html: `<svg width="${iconWidth}" height="${iconHeight}"
                  viewBox="-8 -8 16 16"
                  style="transform:rotate(${rotation}deg)">
               ${highlightRect}
               ${shipSvg}
             </svg>`,
      iconSize: [iconWidth, iconHeight],
      iconAnchor: [iconWidth/2, iconHeight/2]
    });

    // Tooltip
    const now = Date.now();
    const updatedAt = new Date(ship.timestamp).getTime();
    const diffSec = Math.round((now - updatedAt)/1000);
    const diffMin = Math.floor(diffSec/60);
    const diffS   = diffSec % 60;
    const diffStr = `${diffMin}m ${diffS}s ago`;

    let tooltip = `
      <b>${ship.ship_name || 'Unknown'}</b><br/>
      MMSI: ${mmsi}<br/>
      SOG: ${ship.sog||0} kn, COG: ${ship.cog||0}°<br/>
      Length: ${length||'N/A'}<br/>
      Updated: ${diffStr}
    `;

    let marker = shipMarkers[mmsi];
    if(!marker) {
      // nowy statek
      marker = L.marker([ship.latitude, ship.longitude], { icon })
        .on('click', () => selectShip(mmsi));
      marker.bindTooltip(tooltip, { direction:'top', sticky:true });
      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      // aktualizacja
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(tooltip);
    }
    marker.shipData = ship; // zapamiętujemy
  });

  updateSelectedShipsInfo(false);
}

/* ---------------------------------------------------------
   getShipColor(length) - zwraca kolor w zależności od długości
--------------------------------------------------------- */
function getShipColor(length) {
  if(length === null) return 'gray';
  if(length < 50)   return 'green';
  if(length < 150)  return 'yellow';
  if(length < 250)  return 'orange';
  return 'red';
}

/* ---------------------------------------------------------
   updateCollisionsList() - rysuje kolizje w panelu i na mapie
--------------------------------------------------------- */
function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  // Usuwamy stare ikony
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if(!collisionsData || collisionsData.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = `<i style="padding:8px;">No collisions found</i>`;
    collisionList.appendChild(noItem);
    return;
  }

  // Usuwanie duplikatów (np. wg collision_id)
  let uniqueCollisions = {};
  collisionsData.forEach(c => {
    const colId = c.collision_id || `${c.mmsi_a}_${c.mmsi_b}_${c.timestamp||''}`;
    // weź np. minimalne cpa lub najnowszy timestamp itp.
    // tutaj bierzemy cpa minimalne
    if(!uniqueCollisions[colId]) {
      uniqueCollisions[colId] = c;
    } else {
      // Porównywanie cpa, timestamp itp. - do decyzji
      let existing = uniqueCollisions[colId];
      if(c.cpa < existing.cpa) {
        uniqueCollisions[colId] = c; // bierzemy mniejszą cpa
      }
    }
  });
  let finalCollisions = Object.values(uniqueCollisions);

  // Tworzymy listę
  finalCollisions.forEach(col => {
    const item = document.createElement('div');
    item.classList.add('collision-item');

    // statki, cpa, itp.
    let shipAName = col.ship1_name || col.mmsi_a;
    let shipBName = col.ship2_name || col.mmsi_b;
    let cpaStr = col.cpa.toFixed(2);
    let tcpaStr = col.tcpa.toFixed(2);
    let timeStr = col.timestamp ? new Date(col.timestamp).toLocaleTimeString('en-GB') : '---';

    // Splitted circle
    let colorA = getShipColor(col.ship_length_a || null);
    let colorB = getShipColor(col.ship_length_b || null);
    let splittedCircle = createSplittedCircle(colorA, colorB);

    item.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedCircle}
          <strong>${shipAName} - ${shipBName}</strong><br/>
          CPA: ${cpaStr} nm, TCPA: ${tcpaStr} min (@ ${timeStr})
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;

    // Obsługa klik
    item.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(col);
    });
    collisionList.appendChild(item);

    // Rysujemy marker kolizji
    let latC = (col.latitude_a + col.latitude_b)/2;
    let lonC = (col.longitude_a + col.longitude_b)/2;
    let collisionIcon = L.divIcon({
      className: '',
      html: `
        <svg width="20" height="20" viewBox="-10 -10 20 20">
          <polygon points="0,-6 6,6 -6,6"
                   fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="3" text-anchor="middle" font-size="8"
                fill="red">!</text>
        </svg>
      `,
      iconSize: [20,20],
      iconAnchor: [10,10]
    });
    let marker = L.marker([latC, lonC], { icon: collisionIcon })
      .on('click', () => zoomToCollision(col));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

/* ---------------------------------------------------------
   createSplittedCircle(colorA, colorB)
   Zwraca SVG <span> z lewą i prawą połówką w innych kolorach
--------------------------------------------------------- */
function createSplittedCircle(colorA, colorB) {
  return `
    <svg width="16" height="16" viewBox="0 0 16 16"
         style="vertical-align:middle; margin-right:6px;">
      <!-- lewa połówka koła -->
      <path d="M8,8 m-8,0 a8,8 0 0,1 16,0 z" fill="${colorA}"/>
      <!-- prawa połówka koła -->
      <path d="M8,8 m8,0 a8,8 0 0,1 -16,0 z" fill="${colorB}"/>
    </svg>
  `;
}

/* ---------------------------------------------------------
   zoomToCollision(c)
   Ustawiamy widok mapy na kolizję
--------------------------------------------------------- */
function zoomToCollision(col) {
  let bounds = L.latLngBounds([
    [col.latitude_a, col.longitude_a],
    [col.latitude_b, col.longitude_b]
  ]);
  map.fitBounds(bounds, { padding:[50,50] });

  // Możesz zaznaczyć te dwa statki
  clearSelectedShips();
  selectShip(col.mmsi_a);
  selectShip(col.mmsi_b);
}

/* ---------------------------------------------------------
   Wybór statku
--------------------------------------------------------- */
function selectShip(mmsi) {
  if(!selectedShips.includes(mmsi)) {
    if(selectedShips.length >= 2) {
      // usuwamy najstarszy
      selectedShips.shift();
    }
    selectedShips.push(mmsi);
    updateSelectedShipsInfo(true);
  }
}

/* ---------------------------------------------------------
   Czyszczenie wybranych statków
--------------------------------------------------------- */
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

/* ---------------------------------------------------------
   updateSelectedShipsInfo(selectionChanged)
   - panel z wybranymi statkami (po lewej)
   - rysowanie wektorów
   - obliczanie cpa/tcpa pary
--------------------------------------------------------- */
function updateSelectedShipsInfo(selectionChanged) {
  const container = document.getElementById('selected-ships-info');
  container.innerHTML = '';
  document.getElementById('pair-info').innerHTML = '';

  if(selectedShips.length === 0) {
    reloadAllShipIcons();
    return;
  }

  // pobieramy dane
  let shipsData = [];
  selectedShips.forEach(mmsi => {
    if(shipMarkers[mmsi]?.shipData) {
      shipsData.push(shipMarkers[mmsi].shipData);
    }
  });

  // wyświetlamy w panelu
  shipsData.forEach(sd => {
    let div = document.createElement('div');
    div.innerHTML = `
      <b>${sd.ship_name||'Unknown'}</b><br/>
      MMSI: ${sd.mmsi}<br/>
      SOG: ${sd.sog||0} kn, COG: ${sd.cog||0}°<br/>
      Length: ${sd.ship_length||'N/A'}
    `;
    container.appendChild(div);
  });

  // rysujemy wektory (kasujemy stare)
  for(let m in overlayMarkers) {
    overlayMarkers[m].forEach(o => map.removeLayer(o));
  }
  overlayMarkers = {};

  selectedShips.forEach(m => drawVector(m));
  reloadAllShipIcons();

  // Jeżeli mamy 2 statki, oblicz cpa/tcpa
  if(selectedShips.length === 2) {
    let pairSorted = [selectedShips[0], selectedShips[1]].sort((a,b) => a - b);
    if(selectionChanged || !lastSelectedPair 
       || JSON.stringify(lastSelectedPair) !== JSON.stringify(pairSorted)) {
      lastSelectedPair = pairSorted;
      lastCpaTcpa = null;
      fetch(`/calculate_cpa_tcpa?mmsi_a=${pairSorted[0]}&mmsi_b=${pairSorted[1]}`)
        .then(r => r.json())
        .then(data => {
          if(data.error) {
            document.getElementById('pair-info').innerHTML = 
              `<b>CPA/TCPA:</b> N/A (${data.error})`;
            lastCpaTcpa = { cpa: null, tcpa: null };
          } else {
            lastCpaTcpa = data;
            let cpaVal = (data.cpa >= 9999) ? 'n/a' : data.cpa.toFixed(2);
            let tcpaVal = (data.tcpa < 0) ? 'n/a' : data.tcpa.toFixed(2);
            // np. jeżeli cpa>10 -> 'n/a' itd.
            // jeżeli tcpa<0 -> "ships parted"
            let out = `<b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min`;
            document.getElementById('pair-info').innerHTML = out;
          }
        })
        .catch(err => {
          console.error("Error /calculate_cpa_tcpa:", err);
          document.getElementById('pair-info').innerHTML = '<b>CPA/TCPA:</b> N/A';
        });
    } else if(lastCpaTcpa) {
      // odśwież z lastCpaTcpa
      let cpaVal = (lastCpaTcpa.cpa >= 9999) ? 'n/a' : lastCpaTcpa.cpa.toFixed(2);
      let tcpaVal = (lastCpaTcpa.tcpa < 0) ? 'n/a' : lastCpaTcpa.tcpa.toFixed(2);
      document.getElementById('pair-info').innerHTML =
        `<b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min`;
    }
  }
}

/* ---------------------------------------------------------
   reloadAllShipIcons() - odświeża ikony statków
--------------------------------------------------------- */
function reloadAllShipIcons() {
  for(let m in shipMarkers) {
    let marker = shipMarkers[m];
    let sd = marker.shipData;
    let length = sd.ship_length||null;
    let fillColor = getShipColor(length);
    let rotation = sd.cog||0;

    let width = 16, height = 24;
    let highlightRect = '';
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
    marker.setIcon(icon);
  }
}

/* ---------------------------------------------------------
   drawVector(mmsi) - rysuje wektor prędkości
   (sog nm/h na vectorLength minut)
--------------------------------------------------------- */
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

  // uproszczona kalkulacja
  let deltaLat = (distanceNm / 60) * Math.cos(cogRad);
  let deltaLon = (distanceNm / 60) * Math.sin(cogRad) / Math.cos(lat * Math.PI/180);

  let endLat = lat + deltaLat;
  let endLon = lon + deltaLon;

  let line = L.polyline([[lat, lon], [endLat, endLon]], {
    color: 'blue',
    dashArray: '4,4'
  });
  line.addTo(map);

  if(!overlayMarkers[mmsi]) overlayMarkers[mmsi] = [];
  overlayMarkers[mmsi].push(line);
}

// -------------------------------------
// Start
// -------------------------------------
document.addEventListener('DOMContentLoaded', initMap);