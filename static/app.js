// ==========================
// app.js  (Moduł LIVE)
// ==========================
let map;
let markerClusterGroup;

// Mapa mmsi -> L.marker (pozycje statków).
let shipMarkers = {};

// Mapa mmsi -> tablica polylines (wektory).
let overlayMarkers = {};

// Lista mmsi wybranych przez klik.
let selectedShips = [];

// Pełna lista kolizji (z /collisions).
let collisionsData = [];

// Mapa collisionKey -> L.marker (ikona kolizji).
// collisionKey np. "mmsiA_mmsiB_timestamp" (posortowane A<B).
let collisionMarkersMap = {};

// Parametry
let vectorLength = 15;  // rysowany wektor w minutach
let cpaFilter = 0.5;    // suwak CPA
let tcpaFilter = 10;    // suwak TCPA

// Timery
let shipsInterval = null;
let collisionsInterval = null;

// Dla /calculate_cpa_tcpa
let lastSelectedPair = null;
let lastCpaTcpa = null;

function initMap() {
  // Inicjalizacja mapy
  map = L.map('map', {
    center: [50, 0],
    zoom: 5
  });

  // --- Warstwa bazowa OSM ---
  const osmLayer = L.tileLayer(
    'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    { maxZoom: 18 }
  );
  osmLayer.addTo(map);

  // --- Warstwa nawigacyjna (OpenSeaMap) ---
  const openSeaMap = L.tileLayer(
    'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
    { maxZoom: 18, opacity: 0.9 }
  );
  openSeaMap.addTo(map);

  // MarkerCluster
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // Obsługa suwaków
  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(false);
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
  document.getElementById('clearSelectedShips').addEventListener('click', () => {
    clearSelectedShips();
  });

  // Startowy fetch
  fetchShips();
  fetchCollisions();

  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

/** Pobiera statki z /ships */
function fetchShips() {
  fetch('/ships')
    .then(r => r.json())
    .then(data => updateShips(data))
    .catch(err => console.error('Error /ships:', err));
}

/** Pobiera kolizje z /collisions */
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(r => r.json())
    .then(data => {
      collisionsData = data || [];
      updateCollisionsList();
    })
    .catch(err => console.error('Error /collisions:', err));
}

/** Uaktualnia statki na mapie. */
function updateShips(shipsArray) {
  const currentMmsiSet = new Set(shipsArray.map(s => s.mmsi));

  // Usuwamy statki, których już nie ma
  for (const mmsi in shipMarkers) {
    if (!currentMmsiSet.has(parseInt(mmsi))) {
      // usuń z mapy
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      // usuń wektory
      if (overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(poly => map.removeLayer(poly));
        delete overlayMarkers[mmsi];
      }
    }
  }

  // Dodaj / update
  shipsArray.forEach(ship => {
    const { mmsi, latitude, longitude, sog, cog, timestamp, ship_name, ship_length } = ship;
    const fillColor = getShipColor(ship_length);
    const rotation = cog || 0;
    const w = 16, h = 24;

    let highlightRect = '';
    if (selectedShips.includes(mmsi)) {
      highlightRect = `<rect x="-10" y="-10" width="20" height="20"
                             fill="none" stroke="black" stroke-width="3"
                             stroke-dasharray="5,5"/>`;
    }

    const shipSvg = `<polygon points="0,-8 6,8 -6,8"
                             fill="${fillColor}" stroke="black" stroke-width="1"/>`;
    const iconHtml = `
      <svg width="${w}" height="${h}" viewBox="-8 -8 16 16"
           style="transform:rotate(${rotation}deg)">
        ${highlightRect}
        ${shipSvg}
      </svg>
    `;
    const icon = L.divIcon({
      className: '',
      html: iconHtml,
      iconSize: [w, h],
      iconAnchor: [w/2, h/2]
    });

    // Tooltip
    const now = Date.now();
    const updatedAt = new Date(timestamp).getTime();
    const diffSec = Math.round((now - updatedAt) / 1000);
    const diffMin = Math.floor(diffSec / 60);
    const diffS = diffSec % 60;
    const diffStr = `${diffMin}m ${diffS}s ago`;

    const tooltipHtml = `
      <b>${ship_name || 'Unknown'}</b><br>
      MMSI: ${mmsi}<br>
      SOG: ${sog||0} kn, COG: ${cog||0}°<br>
      Length: ${ship_length||'N/A'}<br>
      Updated: ${diffStr}
    `;

    let marker = shipMarkers[mmsi];
    if (!marker) {
      marker = L.marker([latitude, longitude], { icon })
        .on('click', () => selectShip(mmsi));
      marker.bindTooltip(tooltipHtml, { direction: 'top', sticky: true });
      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      // update
      marker.setLatLng([latitude, longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(tooltipHtml);
    }
    marker.shipData = ship;
  });

  updateSelectedShipsInfo(false);
}

/** Zwraca kolor w zależności od długości statku. */
function getShipColor(length) {
  if (length == null) return 'grey';
  if (length < 50) return 'green';
  if (length < 150) return 'yellow';
  if (length < 250) return 'orange';
  return 'red';
}

/** Uaktualnia listę kolizji w panelu i na mapie. */
function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  // Usuwamy stare markery kolizji
  for (const key in collisionMarkersMap) {
    map.removeLayer(collisionMarkersMap[key]);
  }
  collisionMarkersMap = {};

  if (!collisionsData || collisionsData.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = `<i style="padding:8px;">No collisions found</i>`;
    collisionList.appendChild(noItem);
    return;
  }

  // Filtrowanie "nieaktualnych" kolizji,
  // np. c.cpa>cpaFilter, c.tcpa>tcpaFilter albo c.tcpa<0 => pomijamy.
  let validCollisions = collisionsData.filter(c => {
    if (!c.timestamp) return false; // brak timestamp
    if (c.cpa > cpaFilter) return false;
    if (c.tcpa > tcpaFilter) return false;
    if (c.tcpa < 0) return false; // statki się rozminęły
    return true;
  });

  // Eliminacja duplikatów (dla tej samej pary/timestamp):
  const uniqueMap = {};
  validCollisions.forEach(col => {
    const pair = [col.mmsi_a, col.mmsi_b].sort((a,b)=>a-b);
    const key = `${pair[0]}_${pair[1]}_${col.timestamp}`;
    const existing = uniqueMap[key];
    if (!existing) {
      uniqueMap[key] = col;
    } else {
      // Weźmy nowszy
      if (col.timestamp > existing.timestamp) {
        uniqueMap[key] = col;
      }
    }
  });
  const filteredCollisions = Object.values(uniqueMap);

  if (filteredCollisions.length === 0) {
    const noCol = document.createElement('div');
    noCol.classList.add('collision-item');
    noCol.innerHTML = `<i style="padding:8px;">No collisions found after filtering</i>`;
    collisionList.appendChild(noCol);
    return;
  }

  // Tworzymy elementy w panelu i markery na mapie
  filteredCollisions.forEach(c => {
    const item = document.createElement('div');
    item.classList.add('collision-item');

    // Kolory do splitted circle (jeśli c zawiera ship1_length, ship2_length)
    // Dla uproszczenia sprawdzimy, czy w obiekcie mamy te pola:
    let circleHtml = '';
    if (typeof c.ship1_length === 'number' && typeof c.ship2_length === 'number') {
      const colorA = getShipColor(c.ship1_length);
      const colorB = getShipColor(c.ship2_length);
      circleHtml = createSplittedCircle(colorA, colorB);
    }

    const shipA = c.ship1_name || c.mmsi_a;
    const shipB = c.ship2_name || c.mmsi_b;
    const cpaVal = c.cpa.toFixed(2);
    const tcpaVal = c.tcpa.toFixed(2);

    item.innerHTML = `
      <div style="display:flex;align-items:center;">
        ${circleHtml}
        <div>
          <b>${shipA} - ${shipB}</b><br>
          CPA: ${cpaVal} nm, TCPA: ${tcpaVal} min
        </div>
        <button class="zoom-button" style="margin-left:auto;">🔍</button>
      </div>
    `;
    item.querySelector('.zoom-button').addEventListener('click', () => zoomToCollision(c));
    collisionList.appendChild(item);

    // Rysujemy ikonę kolizji
    const latC = (c.latitude_a + c.latitude_b)/2;
    const lonC = (c.longitude_a + c.longitude_b)/2;

    const collisionIcon = L.divIcon({
      className: '',
      html: `
        <svg width="30" height="30" viewBox="-15 -15 30 30">
          <circle cx="0" cy="0" r="12" fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="5" text-anchor="middle"
                font-size="12" fill="red" font-weight="bold">!</text>
        </svg>
      `,
      iconSize: [30,30],
      iconAnchor: [15,15]
    });

    const marker = L.marker([latC, lonC], { icon: collisionIcon });
    const tooltip = `
      Potential collision<br>
      <b>${shipA}</b> & <b>${shipB}</b><br>
      CPA: ${cpaVal} nm, TCPA: ${tcpaVal} min
    `;
    marker.bindTooltip(tooltip, { direction:'top', sticky:true });
    marker.on('click', () => zoomToCollision(c));
    marker.addTo(map);

    const collisionKey = `${latC}_${lonC}_${c.timestamp}`;
    collisionMarkersMap[collisionKey] = marker;
  });
}

/** Rysuje splitted circle (pół/pół) dla statków. */
function createSplittedCircle(colorA, colorB) {
  return `
    <svg width="20" height="20" viewBox="0 0 20 20" style="margin-right:6px;">
      <!-- lewa połówka -->
      <path d="M10,10 m-10,0 a10,10 0 0,1 20,0 z" fill="${colorA}"/>
      <!-- prawa połówka -->
      <path d="M10,10 m10,0 a10,10 0 0,1 -20,0 z" fill="${colorB}"/>
    </svg>
  `;
}

/** Zoom do kolizji (pasuje mapę). */
function zoomToCollision(c) {
  const bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, { padding:[50,50] });

  // Możemy też zaznaczyć statki
  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

/** Zaznaczenie statku. */
function selectShip(mmsi) {
  if (!selectedShips.includes(mmsi)) {
    if (selectedShips.length >= 2) {
      selectedShips.shift();
    }
    selectedShips.push(mmsi);
    updateSelectedShipsInfo(true);
  }
}

/** Czyścimy zaznaczenie. */
function clearSelectedShips() {
  selectedShips = [];
  lastSelectedPair = null;
  lastCpaTcpa = null;
  for (const m in overlayMarkers) {
    overlayMarkers[m].forEach(line => map.removeLayer(line));
  }
  overlayMarkers = {};
  updateSelectedShipsInfo(false);
}

/** Uaktualnia panel lewy i wyświetla cpa/tcpa. */
function updateSelectedShipsInfo(selectionChanged) {
  const infoDiv = document.getElementById('selected-ships-info');
  infoDiv.innerHTML = '';
  const pairDiv = document.getElementById('pair-info');
  pairDiv.innerHTML = '';

  if (selectedShips.length === 0) {
    reloadAllShipIcons();
    return;
  }

  // Wyświetlamy info o wybranych
  let shipsData = [];
  selectedShips.forEach(m => {
    if (shipMarkers[m]?.shipData) {
      shipsData.push(shipMarkers[m].shipData);
    }
  });

  shipsData.forEach(sd => {
    const d = document.createElement('div');
    d.innerHTML = `
      <b>${sd.ship_name || 'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog||0} kn, COG: ${sd.cog||0}°<br>
      Length: ${sd.ship_length || 'N/A'}
    `;
    infoDiv.appendChild(d);
  });

  // Rysuj wektory
  for (const m in overlayMarkers) {
    overlayMarkers[m].forEach(line => map.removeLayer(line));
  }
  overlayMarkers = {};

  selectedShips.forEach(m => drawVector(m));
  reloadAllShipIcons();

  // Jak mamy 2 statki, liczymy /calculate_cpa_tcpa
  if (selectedShips.length === 2) {
    const sortedPair = [selectedShips[0], selectedShips[1]].sort((a,b)=>a-b);
    if (selectionChanged || !lastSelectedPair ||
        JSON.stringify(lastSelectedPair) !== JSON.stringify(sortedPair)) {
      lastSelectedPair = sortedPair;
      lastCpaTcpa = null;
      fetch(`/calculate_cpa_tcpa?mmsi_a=${sortedPair[0]}&mmsi_b=${sortedPair[1]}`)
        .then(r => r.json())
        .then(data => {
          if (data.error) {
            pairDiv.innerHTML = `<b>CPA/TCPA:</b> n/a (${data.error})`;
            lastCpaTcpa = { cpa:null, tcpa:null };
          } else {
            // interpretacja
            if (data.cpa>=9999 || data.tcpa<0) {
              pairDiv.innerHTML = `<b>CPA/TCPA:</b> n/a (diverging)`;
              lastCpaTcpa = data;
            } else if (data.cpa>10 || data.tcpa>600) {
              pairDiv.innerHTML = `<b>CPA/TCPA:</b> n/a (too large)`;
              lastCpaTcpa = data;
            } else {
              lastCpaTcpa = data;
              pairDiv.innerHTML = `
                <b>CPA/TCPA:</b> ${data.cpa.toFixed(2)} nm /
                ${data.tcpa.toFixed(2)} min
              `;
            }
          }
        })
        .catch(err => {
          console.error('Error /calculate_cpa_tcpa:', err);
          pairDiv.innerHTML = `<b>CPA/TCPA:</b> n/a`;
        });
    } else if (lastCpaTcpa) {
      // Odtwarzamy zapamiętane
      if (lastCpaTcpa.cpa>=9999 || lastCpaTcpa.tcpa<0) {
        pairDiv.innerHTML = `<b>CPA/TCPA:</b> n/a (diverging)`;
      } else if (lastCpaTcpa.cpa>10 || lastCpaTcpa.tcpa>600) {
        pairDiv.innerHTML = `<b>CPA/TCPA:</b> n/a (too large)`;
      } else {
        pairDiv.innerHTML = `
          <b>CPA/TCPA:</b> ${lastCpaTcpa.cpa.toFixed(2)} nm /
          ${lastCpaTcpa.tcpa.toFixed(2)} min
        `;
      }
    }
  }
}

/** Przeładowuje ikony statków z highlight. */
function reloadAllShipIcons() {
  for (const mmsi in shipMarkers) {
    const marker = shipMarkers[mmsi];
    const sd = marker.shipData || {};
    const fillColor = getShipColor(sd.ship_length);
    const rotation = sd.cog||0;
    const w=16, h=24;

    let highlightRect='';
    if (selectedShips.includes(parseInt(mmsi))) {
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
    const iconHtml=`
      <svg width="${w}" height="${h}" viewBox="-8 -8 16 16"
           style="transform:rotate(${rotation}deg)">
        ${highlightRect}
        ${shipSvg}
      </svg>
    `;
    const icon = L.divIcon({
      className:'',
      html: iconHtml,
      iconSize:[w,h],
      iconAnchor:[w/2,h/2]
    });
    marker.setIcon(icon);
  }
}

/** Rysuje wektor (kilkuminutowy) statku. */
function drawVector(mmsi) {
  const marker = shipMarkers[mmsi];
  if (!marker) return;
  const sd = marker.shipData;
  if (!sd.sog || !sd.cog) return;

  const lat = sd.latitude;
  const lon = sd.longitude;
  const sog = sd.sog; // nm/h
  const cogDeg = sd.cog;
  const distanceNm = sog * (vectorLength / 60.0);
  const cogRad = cogDeg * Math.PI / 180;

  const deltaLat = (distanceNm/60)*Math.cos(cogRad);
  const deltaLon = (distanceNm/60)*Math.sin(cogRad)/(Math.cos(lat*Math.PI/180));

  const endLat = lat + deltaLat;
  const endLon = lon + deltaLon;

  const line = L.polyline([[lat, lon],[endLat, endLon]], {
    color:'blue', dashArray:'4,4'
  });
  line.addTo(map);

  if (!overlayMarkers[mmsi]) overlayMarkers[mmsi] = [];
  overlayMarkers[mmsi].push(line);
}

// Start
document.addEventListener('DOMContentLoaded', initMap);