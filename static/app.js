// ==========================
// app.js  (Moduł LIVE)
// ==========================
let map;
let markerClusterGroup;

/** Mapa mmsi -> L.marker (pozycje statków). */
let shipMarkers = {};

/** Mapa mmsi -> L.polyline[] (wektory ruchu). */
let overlayMarkers = {};

/** Zaznaczone statki przez klik. */
let selectedShips = [];

/** Kolizje pobrane z /collisions (najczęściej 1 wpis na parę). */
let collisionsData = [];

/** Mapa collision_id (albo pary) -> L.marker (ikona kolizji). */
let collisionMarkersMap = {};

/** Domyślna długość wektora (min) i filtry cpa/tcpa. */
let vectorLength = 15;  // w minutach
let cpaFilter = 0.5;    // mil morskich
let tcpaFilter = 10;    // minut

/** Timery do fetch co 60 sek. */
let shipsInterval = null;
let collisionsInterval = null;

/** Pamiętamy ostatnio wybraną parę (aby unikać zbędnych fetch /calculate_cpa_tcpa). */
let lastSelectedPair = null;
let lastCpaTcpa = null;

function initMap() {
  map = L.map('map', {
    center: [50, 0],
    zoom: 5
  });

  // --- Warstwa OSM ---
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
  // Aktywujemy warstwę:
  openSeaMap.addTo(map);

  // MarkerClusterGroup do obsługi klastrowania statków
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // --- Obsługa suwaków i przycisków ---
  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(false);
  });

  // Zakładamy, że w HTML min=0, max=0.5, step=0.01
  document.getElementById('cpaFilter').addEventListener('input', e => {
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisions();
  });

  // Zakładamy, że w HTML min=0, max=10
  document.getElementById('tcpaFilter').addEventListener('input', e => {
    tcpaFilter = parseFloat(e.target.value) || 10;
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    fetchCollisions();
  });

  document.getElementById('clearSelectedShips').addEventListener('click', () => {
    clearSelectedShips();
  });

  // Pierwszy fetch
  fetchShips();
  fetchCollisions();

  // Odświeżanie co 60 sek
  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

/** Pobiera listę statków z /ships. */
function fetchShips() {
  fetch('/ships')
    .then(res => res.json())
    .then(data => {
      updateShips(data);
    })
    .catch(err => console.error('Error fetching /ships:', err));
}

/** Pobiera listę kolizji z /collisions z parametrami cpa/tcpa. */
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(res => res.json())
    .then(data => {
      collisionsData = data || [];
      updateCollisionsList();
    })
    .catch(err => console.error('Error fetching /collisions:', err));
}

/** Aktualizuje widok statków na mapie. */
function updateShips(shipsArray) {
  // Zbiór MMSI z najnowszego fetch
  const currentMmsiSet = new Set(shipsArray.map(s => s.mmsi));

  // Usuwamy z mapy statki, których już nie ma
  for (const mmsi in shipMarkers) {
    if (!currentMmsiSet.has(parseInt(mmsi))) {
      // Usuń marker statku
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];

      // Usuń wektory
      if (overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(line => map.removeLayer(line));
        delete overlayMarkers[mmsi];
      }
    }
  }

  // Dodaj/aktualizuj statki
  shipsArray.forEach(ship => {
    const { mmsi, latitude, longitude, sog, cog, timestamp, ship_name, ship_length } = ship;
    const fillColor = getShipColor(ship_length);
    const rotation = cog || 0;
    const iconSize = [16, 24];

    // Zaznaczenie?
    let highlightRect = '';
    if (selectedShips.includes(mmsi)) {
      highlightRect = `
        <rect x="-10" y="-10" width="20" height="20"
              fill="none" stroke="black" stroke-width="3"
              stroke-dasharray="5,5"/>
      `;
    }

    // Ikona statku = trójkąt
    const shipSvg = `
      <polygon points="0,-8 6,8 -6,8"
               fill="${fillColor}" stroke="black" stroke-width="1"/>
    `;
    const iconHtml = `
      <svg width="16" height="24" viewBox="-8 -8 16 16"
           style="transform:rotate(${rotation}deg)">
        ${highlightRect}
        ${shipSvg}
      </svg>
    `;
    const icon = L.divIcon({
      className: '',
      html: iconHtml,
      iconSize,
      iconAnchor: [iconSize[0]/2, iconSize[1]/2]
    });

    // Tooltip – czas od aktualizacji
    const now = Date.now();
    const updatedAt = new Date(timestamp).getTime();
    const diffSec = Math.round((now - updatedAt) / 1000);
    const diffMin = Math.floor(diffSec / 60);
    const diffS = diffSec % 60;
    const diffStr = `${diffMin}m ${diffS}s ago`;

    const tooltipHtml = `
      <b>${ship_name || 'Unknown'}</b><br>
      MMSI: ${mmsi}<br>
      SOG: ${sog || 0} kn, COG: ${cog || 0}°<br>
      Length: ${ship_length || 'N/A'}<br>
      Updated: ${diffStr}
    `;

    let marker = shipMarkers[mmsi];
    if (!marker) {
      // Nowy statek
      marker = L.marker([latitude, longitude], { icon })
        .on('click', () => selectShip(mmsi));
      marker.bindTooltip(tooltipHtml, { direction: 'top', sticky: true });
      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      // Aktualizacja
      marker.setLatLng([latitude, longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(tooltipHtml);
    }

    marker.shipData = ship;
  });

  // Odśwież info o wybranych statkach
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

/** Uaktualnia listę kolizji (prawy panel) i rysuje ikonę kolizji. */
function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  // Usuwamy stare ikony kolizji
  for (const cid in collisionMarkersMap) {
    map.removeLayer(collisionMarkersMap[cid]);
  }
  collisionMarkersMap = {};

  if (!collisionsData || collisionsData.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i style="padding:8px;">No collisions found</i>';
    collisionList.appendChild(noItem);
    return;
  }

  // Eliminujemy duplikaty (np. B-A vs A-B). 
  // Zakładamy, że (mmsi_a, mmsi_b, timestamp) jest unikatowe.
  // A jeśli chcesz 1 parę (bez rozróżniania timestamp), to sięgnij do backendu.
  const uniqueMap = {};
  collisionsData.forEach(col => {
    const pair = [col.mmsi_a, col.mmsi_b].sort((a, b) => a - b);
    // dopuszczamy też timestamp w kluczu, aby 'nadpisać' stare
    const key = `${pair[0]}_${pair[1]}_${col.timestamp || ''}`;

    // jeżeli mamy już coś w uniqueMap[key],
    // to decydujemy, który timestamp nowszy:
    if (!uniqueMap[key]) {
      uniqueMap[key] = col;
    } else {
      // porównaj, weź nowszy:
      const existing = uniqueMap[key];
      if (col.timestamp > existing.timestamp) {
        uniqueMap[key] = col;
      }
    }
  });

  // Rezultat
  const filteredCollisions = Object.values(uniqueMap);

  filteredCollisions.forEach(c => {
    const item = document.createElement('div');
    item.classList.add('collision-item');

    // Splitted circle, jeśli chcesz:
    // let colorA = getShipColor(c.ship1_length);
    // let colorB = getShipColor(c.ship2_length);
    // let splitted = createSplittedCircle(colorA, colorB);

    const shipA = c.ship1_name || c.mmsi_a;
    const shipB = c.ship2_name || c.mmsi_b;
    const cpaVal = c.cpa.toFixed(2);
    const tcpaVal = c.tcpa.toFixed(2);

    item.innerHTML = `
      <b>${shipA} - ${shipB}</b><br>
      CPA: ${cpaVal} nm, TCPA: ${tcpaVal} min
      <button class="zoom-button">🔍</button>
    `;
    item.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(c);
    });
    collisionList.appendChild(item);

    // Ikona kolizji – duże kółko z '!'
    const latC = (c.latitude_a + c.latitude_b) / 2;
    const lonC = (c.longitude_a + c.longitude_b) / 2;

    const collisionIcon = L.divIcon({
      className: '',
      html: `
        <svg width="30" height="30" viewBox="-15 -15 30 30">
          <circle cx="0" cy="0" r="12" fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="5" text-anchor="middle" font-size="12"
                fill="red" font-weight="bold">!</text>
        </svg>
      `,
      iconSize: [30, 30],
      iconAnchor: [15, 15]
    });

    const marker = L.marker([latC, lonC], { icon: collisionIcon });
    const tooltip = `
      Potential collision between
      <b>${shipA}</b> &amp; <b>${shipB}</b><br>
      CPA: ${cpaVal} nm, TCPA: ${tcpaVal} min
    `;
    marker.bindTooltip(tooltip, { direction: 'top', sticky: true });
    marker.on('click', () => zoomToCollision(c));
    marker.addTo(map);

    const collisionId = c.collision_id || `${latC}_${lonC}`;
    collisionMarkersMap[collisionId] = marker;
  });
}

/** (Przykład) – splitted circle, jeśli mamy ship1_length i ship2_length. */
function createSplittedCircle(colorA, colorB) {
  return `
    <svg width="16" height="16" viewBox="0 0 16 16"
         style="vertical-align:middle;margin-right:6px;">
      <path d="M8,8 m-8,0 a8,8 0 0,1 16,0 z" fill="${colorA}"/>
      <path d="M8,8 m8,0 a8,8 0 0,1 -16,0 z" fill="${colorB}"/>
    </svg>
  `;
}

/** Zoom do kolizji (dopasowujemy widok). */
function zoomToCollision(c) {
  const bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, { padding: [50, 50] });

  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

/** Dodaje lub usuwa mmsi z tablicy selectedShips. */
function selectShip(mmsi) {
  if (!selectedShips.includes(mmsi)) {
    if (selectedShips.length >= 2) {
      selectedShips.shift();  // by docelowo mieć max 2
    }
    selectedShips.push(mmsi);
    updateSelectedShipsInfo(true);
  }
}

/** Czyści listę zaznaczonych statków. */
function clearSelectedShips() {
  selectedShips = [];
  lastSelectedPair = null;
  lastCpaTcpa = null;

  // Usuwamy wektory
  for (const m in overlayMarkers) {
    overlayMarkers[m].forEach(poly => map.removeLayer(poly));
  }
  overlayMarkers = {};

  updateSelectedShipsInfo(false);
}

/** Aktualizuje panel lewy z info o zaznaczonych statkach, liczy cpa/tcpa. */
function updateSelectedShipsInfo(selectionChanged) {
  const container = document.getElementById('selected-ships-info');
  container.innerHTML = '';

  const pairInfoElem = document.getElementById('pair-info');
  pairInfoElem.innerHTML = '';

  if (selectedShips.length === 0) {
    reloadAllShipIcons();
    return;
  }

  // Wyświetl info
  let shipsData = [];
  selectedShips.forEach(m => {
    if (shipMarkers[m]?.shipData) {
      shipsData.push(shipMarkers[m].shipData);
    }
  });

  shipsData.forEach(sd => {
    const div = document.createElement('div');
    div.innerHTML = `
      <b>${sd.ship_name || 'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog || 0} kn, COG: ${sd.cog || 0}°<br>
      Length: ${sd.ship_length || 'N/A'}
    `;
    container.appendChild(div);
  });

  // Rysowanie wektorów
  for (const m in overlayMarkers) {
    overlayMarkers[m].forEach(v => map.removeLayer(v));
  }
  overlayMarkers = {};

  selectedShips.forEach(m => drawVector(m));
  reloadAllShipIcons();

  // Jeśli mamy 2 statki, pobierz cpa/tcpa
  if (selectedShips.length === 2) {
    const sortedPair = [selectedShips[0], selectedShips[1]].sort((a, b) => a - b);

    if (selectionChanged || !lastSelectedPair ||
        JSON.stringify(lastSelectedPair) !== JSON.stringify(sortedPair)) {
      lastSelectedPair = sortedPair;
      lastCpaTcpa = null;
      // pobierz cpa/tcpa
      fetch(`/calculate_cpa_tcpa?mmsi_a=${sortedPair[0]}&mmsi_b=${sortedPair[1]}`)
        .then(r => r.json())
        .then(data => {
          if (data.error) {
            pairInfoElem.innerHTML = `<b>CPA/TCPA:</b> n/a (${data.error})`;
            lastCpaTcpa = { cpa: null, tcpa: null };
          } else {
            // interpretacja
            if (data.cpa >= 9999 || data.tcpa < 0) {
              pairInfoElem.innerHTML = `<b>CPA/TCPA:</b> n/a (ships diverging)`;
              lastCpaTcpa = { cpa: 9999, tcpa: -1 };
            } else if (data.cpa > 10 || data.tcpa > 600) {
              // arbitrary limit
              pairInfoElem.innerHTML = `<b>CPA/TCPA:</b> n/a (too large)`;
              lastCpaTcpa = data;
            } else {
              lastCpaTcpa = data;
              pairInfoElem.innerHTML = `
                <b>CPA/TCPA:</b> ${data.cpa.toFixed(2)} nm /
                ${data.tcpa.toFixed(2)} min
              `;
            }
          }
        })
        .catch(err => {
          console.error('Error /calculate_cpa_tcpa:', err);
          pairInfoElem.innerHTML = `<b>CPA/TCPA:</b> n/a`;
        });
    } else if (lastCpaTcpa) {
      // Mamy zapamiętane cpa/tcpa
      if (lastCpaTcpa.cpa >= 9999 || lastCpaTcpa.tcpa < 0) {
        pairInfoElem.innerHTML = `<b>CPA/TCPA:</b> n/a (diverging)`;
      } else if (lastCpaTcpa.cpa > 10 || lastCpaTcpa.tcpa > 600) {
        pairInfoElem.innerHTML = `<b>CPA/TCPA:</b> n/a (too large)`;
      } else {
        pairInfoElem.innerHTML = `
          <b>CPA/TCPA:</b> ${lastCpaTcpa.cpa.toFixed(2)} nm /
          ${lastCpaTcpa.tcpa.toFixed(2)} min
        `;
      }
    }
  }
}

/** Odtwarza ikony statków z ewentualnym highlight. */
function reloadAllShipIcons() {
  for (const mmsi in shipMarkers) {
    const marker = shipMarkers[mmsi];
    const sd = marker.shipData || {};
    const length = sd.ship_length || null;
    const fillColor = getShipColor(length);
    const rotation = sd.cog || 0;
    const width = 16, height = 24;

    let highlightRect = '';
    if (selectedShips.includes(parseInt(mmsi))) {
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
      <svg width="${width}" height="${height}" viewBox="-8 -8 16 16"
           style="transform:rotate(${rotation}deg)">
        ${highlightRect}
        ${shipSvg}
      </svg>
    `;
    const icon = L.divIcon({
      className: '',
      html: iconHtml,
      iconSize: [width, height],
      iconAnchor: [width/2, height/2]
    });
    marker.setIcon(icon);
  }
}

/** Rysuje wektor ruchu statku. */
function drawVector(mmsi) {
  const marker = shipMarkers[mmsi];
  if (!marker) return;
  const sd = marker.shipData;
  if (!sd.sog || !sd.cog) return;

  const lat = sd.latitude;
  const lon = sd.longitude;
  const sog = sd.sog;   // nm/h
  const cogDeg = sd.cog;
  const distanceNm = sog * (vectorLength / 60.0);
  const cogRad = (cogDeg * Math.PI) / 180;

  const deltaLat = (distanceNm / 60) * Math.cos(cogRad);
  const deltaLon = (distanceNm / 60) * Math.sin(cogRad) / Math.cos(lat * Math.PI / 180);

  const endLat = lat + deltaLat;
  const endLon = lon + deltaLon;

  const line = L.polyline([[lat, lon], [endLat, endLon]], {
    color: 'blue',
    dashArray: '4,4'
  });
  line.addTo(map);

  if (!overlayMarkers[mmsi]) {
    overlayMarkers[mmsi] = [];
  }
  overlayMarkers[mmsi].push(line);
}

// Gdy DOM gotowy:
document.addEventListener('DOMContentLoaded', initMap);