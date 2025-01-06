// ==========================
// app.js  (Moduł LIVE)
// ==========================

// -----------------------------------------
// Zmienne globalne
// -----------------------------------------
let map;
let markerClusterGroup;

// Mapa: mmsi -> L.marker (pozycje statków).
let shipMarkers = {};

// Mapa: mmsi -> tablica L.polyline (wektory).
let overlayMarkers = {};

// Lista mmsi wybranych statków (klik).
let selectedShips = [];

// Mapa kolizji: klucz = "mmsiA_mmsiB" => obiekt najnowszej kolizji
let activeCollisionsMap = {};

// Mapa kolizji -> L.marker (ikona kolizji na mapie),
// klucz zbieżny z activeCollisionsMap, np. "mmsiA_mmsiB".
let collisionMarkersMap = {};

// Parametry
let vectorLength = 15;   // wektor ruchu w min
let cpaFilter = 0.5;     // suwak
let tcpaFilter = 10;     // suwak

// Dla /calculate_cpa_tcpa (podgląd pary statków)
let lastSelectedPair = null;
let lastCpaTcpa = null;

// Timery
let shipsInterval = null;
let collisionsInterval = null;


// -----------------------------------------
// Inicjalizacja mapy
// -----------------------------------------
function initMap() {
  map = L.map('map', {
    center: [50, 0],
    zoom: 5
  });

  // Warstwa bazowa OSM
  const osmLayer = L.tileLayer(
    'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    { maxZoom: 18 }
  );
  osmLayer.addTo(map);

  // Warstwa nawigacyjna (OpenSeaMap)
  const openSeaMap = L.tileLayer(
    'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
    { maxZoom: 18, opacity: 0.9 }
  );
  openSeaMap.addTo(map);

  // MarkerCluster
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // Obsługa suwaków i przycisków
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

  // Pierwsze pobrania
  fetchShips();
  fetchCollisions();

  // Odświeżanie co 60s
  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}


// -----------------------------------------
// 1) Pobieranie i wyświetlanie statków
// -----------------------------------------
function fetchShips() {
  fetch('/ships')
    .then(r => r.json())
    .then(data => updateShips(data))
    .catch(err => console.error('Error /ships:', err));
}

function updateShips(shipsArray) {
  const currentMmsiSet = new Set(shipsArray.map(s => s.mmsi));

  // Usuwamy statki, których już nie ma
  for (const mmsi in shipMarkers) {
    if (!currentMmsiSet.has(parseInt(mmsi))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      if (overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(line => map.removeLayer(line));
        delete overlayMarkers[mmsi];
      }
    }
  }

  // Dodaj / aktualizuj
  shipsArray.forEach(ship => {
    const { mmsi, latitude, longitude, sog, cog, timestamp,
            ship_name, ship_length } = ship;
    const fillColor = getShipColor(ship_length);
    const rotation = cog || 0;
    const w = 16, h = 24;

    // highlight ?
    let highlightRect = '';
    if (selectedShips.includes(mmsi)) {
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
      marker.setLatLng([latitude, longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(tooltipHtml);
    }
    marker.shipData = ship;
  });

  // Odśwież wektory dla zaznaczonych statków
  updateSelectedShipsInfo(false);
}

function getShipColor(len) {
  if (len == null) return 'grey';
  if (len < 50) return 'green';
  if (len < 150) return 'yellow';
  if (len < 250) return 'orange';
  return 'red';
}


// -----------------------------------------
// 2) Pobieranie i wyświetlanie kolizji
// -----------------------------------------
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(r => r.json())
    .then(fetchedCollisions => {
      if (!Array.isArray(fetchedCollisions)) {
        fetchedCollisions = [];
      }
      // Budujemy nową mapę kolizji
      const newMap = {};

      fetchedCollisions.forEach(c => {
        // Odrzucamy to, co wykracza poza filtry:
        if (!c.timestamp) return;
        if (c.tcpa < 0) return;    // statki się rozminęły
        if (c.cpa > cpaFilter) return;
        if (c.tcpa > tcpaFilter) return;

        // klucz pary (sort mmsi)
        const pair = [c.mmsi_a, c.mmsi_b].sort((a,b) => a-b);
        const key = `${pair[0]}_${pair[1]}`;

        // Jeżeli w newMap coś jest, wybieramy nowsze
        const existing = newMap[key];
        if (!existing) {
          newMap[key] = c;
        } else {
          // porównaj timestamp
          const t1 = new Date(c.timestamp).getTime();
          const t2 = new Date(existing.timestamp).getTime();
          if (t1 > t2) {
            newMap[key] = c;
          }
        }
      });

      // Przypisujemy do globalnego
      activeCollisionsMap = newMap;

      // Render
      renderCollisions();
    })
    .catch(err => console.error('Error /collisions:', err));
}

function renderCollisions() {
  // Czyścimy panel
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  // Usuń stare markery z mapy
  for (const key in collisionMarkersMap) {
    map.removeLayer(collisionMarkersMap[key]);
  }
  collisionMarkersMap = {};

  const keys = Object.keys(activeCollisionsMap);
  if (keys.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = `<i style="padding:8px;">No collisions found</i>`;
    collisionList.appendChild(noItem);
    return;
  }

  // Tworzymy itemy w panelu i markery
  keys.forEach(key => {
    const c = activeCollisionsMap[key];
    if (!c) return;

    // splitted circle?
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

    // Tworzymy item w liście
    const item = document.createElement('div');
    item.classList.add('collision-item');
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

    // Rysujemy ikonę na mapie
    const latC = (c.latitude_a + c.latitude_b) / 2;
    const lonC = (c.longitude_a + c.longitude_b) / 2;

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
    marker.bindTooltip(`
      Potential collision<br>
      <b>${shipA}</b> & <b>${shipB}</b><br>
      CPA: ${cpaVal} nm, TCPA: ${tcpaVal} min
    `, { direction:'top', sticky:true });
    marker.on('click', () => zoomToCollision(c));
    marker.addTo(map);

    collisionMarkersMap[key] = marker;
  });
}

/** Rysuje splitted circle (połówki w różnych kolorach). */
function createSplittedCircle(colorA, colorB) {
  return `
    <svg width="20" height="20" viewBox="0 0 20 20" style="margin-right:6px;">
      <!-- lewa połowa -->
      <path d="M10,10 m-10,0 a10,10 0 0,1 20,0 z" fill="${colorA}"/>
      <!-- prawa połowa -->
      <path d="M10,10 m10,0 a10,10 0 0,1 -20,0 z" fill="${colorB}"/>
    </svg>
  `;
}

function zoomToCollision(c) {
  const bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, { padding:[50,50] });

  // Ewentualne zaznaczenie statków
  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}


// -----------------------------------------
// 3) Obsługa zaznaczonych statków
// -----------------------------------------
function selectShip(mmsi) {
  if (!selectedShips.includes(mmsi)) {
    if (selectedShips.length >= 2) {
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
  // usuń wektory
  for (const m in overlayMarkers) {
    overlayMarkers[m].forEach(line => map.removeLayer(line));
  }
  overlayMarkers = {};
  updateSelectedShipsInfo(false);
}

/** Uaktualnia info w panelu po lewej + CPA/TCPA. */
function updateSelectedShipsInfo(selectionChanged) {
  const leftPanel = document.getElementById('selected-ships-info');
  leftPanel.innerHTML = '';
  const pairInfo = document.getElementById('pair-info');
  pairInfo.innerHTML = '';

  if (selectedShips.length === 0) {
    reloadAllShipIcons();
    return;
  }

  // Zbieramy dane
  const shipsData = [];
  selectedShips.forEach(m => {
    if (shipMarkers[m]?.shipData) {
      shipsData.push(shipMarkers[m].shipData);
    }
  });

  // Wyświetl info
  shipsData.forEach(sd => {
    const div = document.createElement('div');
    div.innerHTML = `
      <b>${sd.ship_name || 'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog||0} kn, COG: ${sd.cog||0}°<br>
      Length: ${sd.ship_length||'N/A'}
    `;
    leftPanel.appendChild(div);
  });

  // Rysuj wektory
  for (const m in overlayMarkers) {
    overlayMarkers[m].forEach(line => map.removeLayer(line));
  }
  overlayMarkers = {};
  selectedShips.forEach(m => drawVector(m));
  reloadAllShipIcons();

  // Jeżeli 2 statki => oblicz /calculate_cpa_tcpa
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
            pairInfo.innerHTML = `<b>CPA/TCPA:</b> n/a (${data.error})`;
            lastCpaTcpa = { cpa: null, tcpa: null };
          } else {
            // interpretacja
            if (data.cpa >= 9999 || data.tcpa < 0) {
              pairInfo.innerHTML = `<b>CPA/TCPA:</b> n/a (diverging)`;
              lastCpaTcpa = data;
            } else if (data.cpa > 10 || data.tcpa > 600) {
              pairInfo.innerHTML = `<b>CPA/TCPA:</b> n/a (too large)`;
              lastCpaTcpa = data;
            } else {
              lastCpaTcpa = data;
              pairInfo.innerHTML = `
                <b>CPA/TCPA:</b> ${data.cpa.toFixed(2)} nm /
                ${data.tcpa.toFixed(2)} min
              `;
            }
          }
        })
        .catch(err => {
          console.error('Error /calculate_cpa_tcpa:', err);
          pairInfo.innerHTML = `<b>CPA/TCPA:</b> n/a`;
        });
    } else if (lastCpaTcpa) {
      // Odtwarzamy zapamiętane
      if (lastCpaTcpa.cpa >= 9999 || lastCpaTcpa.tcpa < 0) {
        pairInfo.innerHTML = `<b>CPA/TCPA:</b> n/a (diverging)`;
      } else if (lastCpaTcpa.cpa > 10 || lastCpaTcpa.tcpa > 600) {
        pairInfo.innerHTML = `<b>CPA/TCPA:</b> n/a (too large)`;
      } else {
        pairInfo.innerHTML = `
          <b>CPA/TCPA:</b> ${lastCpaTcpa.cpa.toFixed(2)} nm /
          ${lastCpaTcpa.tcpa.toFixed(2)} min
        `;
      }
    }
  }
}

/** Odtwarza ikony statków z highlight. */
function reloadAllShipIcons() {
  for (const mmsi in shipMarkers) {
    const marker = shipMarkers[mmsi];
    const sd = marker.shipData || {};
    const fillColor = getShipColor(sd.ship_length);
    const rotation = sd.cog||0;
    const w=16, h=24;

    let highlightRect = '';
    if (selectedShips.includes(parseInt(mmsi))) {
      highlightRect = `
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
      className: '',
      html: iconHtml,
      iconSize: [w,h],
      iconAnchor:[w/2,h/2]
    });
    marker.setIcon(icon);
  }
}

/** Rysuje wektor statku. */
function drawVector(mmsi) {
  const marker = shipMarkers[mmsi];
  if (!marker) return;
  const sd = marker.shipData;
  if (!sd.sog || !sd.cog) return;

  const lat = sd.latitude;
  const lon = sd.longitude;
  const sog = sd.sog;  // nm/h
  const cogDeg = sd.cog;
  const distanceNm = sog * (vectorLength / 60.0);
  const cogRad = cogDeg * Math.PI / 180;

  const deltaLat = (distanceNm/60)*Math.cos(cogRad);
  const deltaLon = (distanceNm/60)*Math.sin(cogRad)/(Math.cos(lat*Math.PI/180));

  const endLat = lat + deltaLat;
  const endLon = lon + deltaLon;

  const line = L.polyline([[lat, lon], [endLat, endLon]],
                          { color:'blue', dashArray:'4,4' });
  line.addTo(map);

  if (!overlayMarkers[mmsi]) overlayMarkers[mmsi] = [];
  overlayMarkers[mmsi].push(line);
}


// -----------------------------------------
// Start
// -----------------------------------------
document.addEventListener('DOMContentLoaded', initMap);