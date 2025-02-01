// ==========================
// app.js (moduł LIVE) – finalna wersja + dodanie X-API-Key, spinnerów oraz debouncingu dla CPA/TCPA
// ==========================

// ---------------
// 1) Zdarzenia startowe
// ---------------
document.addEventListener('DOMContentLoaded', () => {
  initLiveApp().catch(err => console.error("Błąd initLiveApp:", err));
});

// ---------------
// 2) Zmienne globalne
// ---------------
const API_KEY = "Ais-mon";  // Klucz API (jawnie wpisany do celów testowych)

let map;
let markerClusterGroup;
let shipMarkers = {};         // klucz: mmsi -> L.marker
let shipPolygonLayers = {};   // klucz: mmsi -> L.polygon
let overlayVectors = {};      // klucz: mmsi -> [L.Polyline]

let collisionMarkers = [];
let collisionsData = [];
let selectedShips = [];       // wybrane statki (max 2)

let shipsInterval = null;
let collisionsInterval = null;

// Globalny timeout dla debouncingu CPA/TCPA
let cpaUpdateTimeout = null;

// Parametry i filtry
let vectorLength = 15;   // minuty (dla rysowania wektora prędkości)
let cpaFilter = 0.5;     // [0..0.5] – parametr slidera
let tcpaFilter = 10;     // [1..10] – parametr slidera

// ---------------
// Funkcje pomocnicze – Spinner
// ---------------
function showSpinner(id) {
  const spinner = document.getElementById(id);
  if (spinner) {
    spinner.style.display = 'block';
  }
}
function hideSpinner(id) {
  const spinner = document.getElementById(id);
  if (spinner) {
    spinner.style.display = 'none';
  }
}

/**
 * buildShipTooltip(ship):
 *   Wyświetla podstawowe info: nazwa, SOG, COG, HDG, LEN (m),
 *   oraz czas od ostatniej aktualizacji (updated).
 */
function buildShipTooltip(ship) {
  const sogVal = (ship.sog || 0).toFixed(1);
  const cogVal = (ship.cog || 0).toFixed(1);
  const hdgVal = (ship.heading != null) ? ship.heading.toFixed(1) : cogVal;
  
  let lengthStr = 'N/A';
  if (ship.dim_a && ship.dim_b) {
    const lenMeters = parseFloat(ship.dim_a) + parseFloat(ship.dim_b);
    lengthStr = lenMeters.toFixed(1);
  }
  
  let updatedAgo = '';
  if (ship.timestamp) {
    updatedAgo = computeTimeAgo(ship.timestamp);
  }
  
  return `
    <div>
      <b>${ship.ship_name || 'Unknown'}</b><br/>
      SOG: ${sogVal} kn | COG: ${cogVal}°<br/>
      HDG: ${hdgVal}°, LEN: ${lengthStr} m<br/>
      Updated: ${updatedAgo}
    </div>
  `;
}

/**
 * computeTimeAgo(timestamp):
 *   Format "Xs ago" (poniżej 60s) lub "Xm Ys ago" (1 min i więcej).
 */
function computeTimeAgo(timestamp) {
  if (!timestamp) return '';
  const now = Date.now();
  const then = new Date(timestamp).getTime();
  let diffSec = Math.floor((now - then) / 1000);
  if (diffSec < 0) diffSec = 0;
  
  return diffSec < 60 ? `${diffSec}s ago` : `${Math.floor(diffSec / 60)}m ${diffSec % 60}s ago`;
}

// ---------------
// 3) Funkcja główna – inicjalizacja aplikacji
// ---------------
async function initLiveApp() {
  map = initSharedMap('map');
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);
  
  map.on('zoomend', () => { fetchShips(); });
  
  // Suwak wektora prędkości
  const vectorSlider = document.getElementById('vectorLengthSlider');
  vectorSlider.addEventListener('input', e => {
    vectorLength = parseInt(e.target.value, 10) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(true);
  });
  
  // Filtry kolizji
  const cpaSlider = document.getElementById('cpaFilter');
  cpaSlider.addEventListener('input', e => {
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisions();
  });
  
  const tcpaSlider = document.getElementById('tcpaFilter');
  tcpaSlider.addEventListener('input', e => {
    tcpaFilter = parseFloat(e.target.value) || 10;
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    fetchCollisions();
  });
  
  // Przycisk czyszczący zaznaczone statki
  document.getElementById('clearSelectedShips').addEventListener('click', clearSelectedShips);
  
  // Pierwsze pobrania
  await fetchShips();
  await fetchCollisions();
  
  // Ustawienie interwałów
  shipsInterval = setInterval(fetchShips, 10000);
  collisionsInterval = setInterval(fetchCollisions, 5000);
}

// ---------------
// 4) Pobieranie i wyświetlanie statków
// ---------------
async function fetchShips() {
  showSpinner('ships-spinner');
  try {
    const res = await fetch('/ships', { headers: { 'X-API-Key': API_KEY } });
    if (!res.ok) throw new Error(`HTTP ${res.status} - ${res.statusText}`);
    const data = await res.json();
    updateShips(data);
  } catch (err) {
    console.error("Błąd /ships:", err);
  } finally {
    hideSpinner('ships-spinner');
  }
}

function updateShips(shipsArray) {
  const currentSet = new Set(shipsArray.map(s => s.mmsi));
  // Usuwamy statki, których nie ma w nowym zestawie
  for (const mmsi in shipMarkers) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
    }
  }
  for (const mmsi in shipPolygonLayers) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      map.removeLayer(shipPolygonLayers[mmsi]);
      delete shipPolygonLayers[mmsi];
    }
  }
  for (const mmsi in overlayVectors) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
      delete overlayVectors[mmsi];
    }
  }
  
  const zoomLevel = map.getZoom();
  shipsArray.forEach(ship => {
    const { mmsi, latitude, longitude } = ship;
    const isSelected = selectedShips.includes(mmsi);
    const tooltipContent = buildShipTooltip(ship);
    
    if (zoomLevel < 14) {
      if (shipPolygonLayers[mmsi]) {
        map.removeLayer(shipPolygonLayers[mmsi]);
        delete shipPolygonLayers[mmsi];
      }
      let marker = shipMarkers[mmsi];
      if (!marker) {
        const icon = createShipIcon(ship, isSelected, zoomLevel);
        marker = L.marker([latitude, longitude], { icon })
          .on('click', () => selectShip(mmsi));
        marker.shipData = ship;
        marker.bindTooltip(tooltipContent, { direction: 'top', offset: [0, -5] });
        shipMarkers[mmsi] = marker;
        markerClusterGroup.addLayer(marker);
      } else {
        marker.setLatLng([latitude, longitude]);
        marker.setIcon(createShipIcon(ship, isSelected, zoomLevel));
        marker.shipData = ship;
        marker.bindTooltip(tooltipContent, { direction: 'top', offset: [0, -5] });
      }
    } else {
      if (shipMarkers[mmsi]) {
        markerClusterGroup.removeLayer(shipMarkers[mmsi]);
        delete shipMarkers[mmsi];
      }
      if (shipPolygonLayers[mmsi]) {
        map.removeLayer(shipPolygonLayers[mmsi]);
        delete shipPolygonLayers[mmsi];
      }
      const poly = createShipPolygon(ship);
      if (poly) {
        poly.on('click', () => selectShip(mmsi));
        poly.addTo(map);
        poly.shipData = ship;
        poly.bindTooltip(tooltipContent, { direction: 'top', sticky: true });
        shipPolygonLayers[mmsi] = poly;
      }
    }
  });
  
  updateSelectedShipsInfo(false);
}

// ---------------
// 5) Obsługa kolizji
// ---------------
async function fetchCollisions() {
  showSpinner('collisions-spinner');
  try {
    const url = `/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`;
    const res = await fetch(url, { headers: { 'X-API-Key': API_KEY } });
    if (!res.ok) throw new Error(`HTTP ${res.status} – ${res.statusText}`);
    collisionsData = await res.json() || [];
    updateCollisionsList();
  } catch (err) {
    console.error("Błąd /collisions:", err);
  } finally {
    hideSpinner('collisions-spinner');
  }
}

function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';
  
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];
  
  if (!collisionsData || collisionsData.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(noItem);
    return;
  }
  
  // Grupowanie par: nowsze nadpisują starsze
  const pairsMap = {};
  collisionsData.forEach(c => {
    const a = Math.min(c.mmsi_a, c.mmsi_b);
    const b = Math.max(c.mmsi_a, c.mmsi_b);
    const key = `${a}_${b}`;
    if (!pairsMap[key]) {
      pairsMap[key] = c;
    } else {
      const oldT = new Date(pairsMap[key].timestamp).getTime();
      const newT = new Date(c.timestamp).getTime();
      if (newT > oldT) {
        pairsMap[key] = c;
      }
    }
  });
  
  let finalColls = Object.values(pairsMap);
  finalColls.sort((a, b) => a.tcpa - b.tcpa);
  
  if (finalColls.length === 0) {
    const d = document.createElement('div');
    d.classList.add('collision-item');
    d.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(d);
    return;
  }
  
  finalColls.forEach(c => {
    const splittedHTML = getCollisionSplitCircle(c.mmsi_a, c.mmsi_b, 0, 0, shipMarkers);
    const cpaStr = c.cpa.toFixed(2);
    const tcpaStr = c.tcpa.toFixed(2);
    const updatedStr = c.timestamp ? computeTimeAgo(c.timestamp) : '';
    const shipAName = c.ship1_name || String(c.mmsi_a);
    const shipBName = c.ship2_name || String(c.mmsi_b);
    
    const item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <strong>${shipAName}</strong>
          ${splittedHTML}
          <strong>${shipBName}</strong><br/>
          CPA: ${cpaStr} nm, TCPA: ${tcpaStr} min, updated: ${updatedStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    collisionList.appendChild(item);
    
    const latC = (c.latitude_a + c.latitude_b) / 2;
    const lonC = (c.longitude_a + c.longitude_b) / 2;
    const collisionIcon = L.divIcon({
      className: '',
      html: `
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <path d="M0,-7 7,7 -7,7 Z" fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="4" text-anchor="middle" font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize: [24, 24],
      iconAnchor: [12, 12]
    });
    const mark = L.marker([latC, lonC], { icon: collisionIcon })
      .bindTooltip(`Kolizja: ${shipAName} & ${shipBName}`, { direction: 'top', sticky: true })
      .on('click', () => zoomToCollision(c));
    mark.addTo(map);
    collisionMarkers.push(mark);
    
    const zoomBtn = item.querySelector('.zoom-button');
    zoomBtn.addEventListener('click', () => {
      zoomToCollision(c);
      let newVectorLen = Math.round(c.tcpa);
      if (newVectorLen < 1) newVectorLen = 1;
      document.getElementById('vectorLengthSlider').value = newVectorLen;
      document.getElementById('vectorLengthValue').textContent = newVectorLen;
      vectorLength = newVectorLen;
      updateSelectedShipsInfo(true);
    });
  });
}

function zoomToCollision(c) {
  const bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, { padding: [15, 15], maxZoom: 13 });
  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

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
  for (const mmsi in overlayVectors) {
    overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
  }
  overlayVectors = {};
  updateSelectedShipsInfo(false);
}


function updateSelectedShipsInfo(selectionChanged) {
  const panel = document.getElementById('selected-ships-info');
  const pairInfo = document.getElementById('pair-info');
  panel.innerHTML = '';
  pairInfo.innerHTML = '';
  
  // Jeśli żaden statek nie jest zaznaczony
  if (selectedShips.length === 0) {
    panel.innerHTML = "<i>Select up to two ships to calculate CPA/TCPA.</i>";
    return;
  }
  
  // Jeśli tylko jeden statek jest zaznaczony – wyświetl dane bez obliczeń
  if (selectedShips.length === 1) {
    const sData = selectedShips.map(mmsi => {
      if (shipMarkers[mmsi]?.shipData) return shipMarkers[mmsi].shipData;
      else if (shipPolygonLayers[mmsi]?.shipData) return shipPolygonLayers[mmsi].shipData;
      return null;
    }).filter(Boolean);
    sData.forEach(sd => {
      const sogVal = (sd.sog || 0).toFixed(1);
      const cogVal = (sd.cog || 0).toFixed(1);
      const hdgVal = (sd.heading != null) ? sd.heading.toFixed(1) : cogVal;
      let lengthStr = 'N/A';
      if (sd.dim_a && sd.dim_b) {
        const lenMeters = parseFloat(sd.dim_a) + parseFloat(sd.dim_b);
        lengthStr = lenMeters.toFixed(1);
      }
      let updatedAgo = sd.timestamp ? computeTimeAgo(sd.timestamp) : '';
      const infoDiv = document.createElement('div');
      infoDiv.innerHTML = `
        <b>${sd.ship_name || 'Unknown'}</b><br>
        MMSI: ${sd.mmsi}<br>
        SOG: ${sogVal} kn, COG: ${cogVal}°<br>
        HDG: ${hdgVal}°, LEN: ${lengthStr} m<br>
        Updated: ${updatedAgo}
      `;
      panel.appendChild(infoDiv);
      drawVector(sd.mmsi, sd);
    });
    return;
  }
  
  // Dla dokładnie dwóch statków – ustaw debouncing dla fetch CPA/TCPA
  if (selectedShips.length === 2) {
    // Jeśli timeout już istnieje, wyczyść go
    if (cpaUpdateTimeout) {
      clearTimeout(cpaUpdateTimeout);
    }
    cpaUpdateTimeout = setTimeout(() => {
      const [mA, mB] = selectedShips.sort((a, b) => a - b);
      const url = `/calculate_cpa_tcpa?mmsi_a=${mA}&mmsi_b=${mB}`;
      fetch(url, { headers: { 'X-API-Key': API_KEY } })
        .then(r => r.json())
        .then(data => {
          if (data.error) {
            pairInfo.innerHTML = `<b>CPA/TCPA:</b> N/A (${data.error})`;
          } else {
            const cpaVal = (data.cpa >= 9999) ? 'n/a' : data.cpa.toFixed(2);
            const tcpaVal = (data.tcpa < 0) ? 'n/a' : data.tcpa.toFixed(2);
            pairInfo.innerHTML = `<b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min`;
          }
        })
        .catch(err => {
          console.error("Błąd /calculate_cpa_tcpa:", err);
          pairInfo.innerHTML = `<b>CPA/TCPA:</b> N/A`;
        });
    }, 30000); // fetch wywołany po 30 sekundach
  }
  
  // Renderowanie danych statków (dla wszystkich zaznaczonych)
  const sData = selectedShips.map(mmsi => {
    if (shipMarkers[mmsi]?.shipData) return shipMarkers[mmsi].shipData;
    else if (shipPolygonLayers[mmsi]?.shipData) return shipPolygonLayers[mmsi].shipData;
    return null;
  }).filter(Boolean);
  
  // Czyszczenie starych wektorów
  for (const mmsi in overlayVectors) {
    overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
  }
  overlayVectors = {};
  
  sData.forEach(sd => {
    const sogVal = (sd.sog || 0).toFixed(1);
    const cogVal = (sd.cog || 0).toFixed(1);
    const hdgVal = (sd.heading != null) ? sd.heading.toFixed(1) : cogVal;
    let lengthStr = 'N/A';
    if (sd.dim_a && sd.dim_b) {
      const lenMeters = parseFloat(sd.dim_a) + parseFloat(sd.dim_b);
      lengthStr = lenMeters.toFixed(1);
    }
    let updatedAgo = sd.timestamp ? computeTimeAgo(sd.timestamp) : '';
    const infoDiv = document.createElement('div');
    infoDiv.innerHTML = `
      <b>${sd.ship_name || 'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sogVal} kn, COG: ${cogVal}°<br>
      HDG: ${hdgVal}°, LEN: ${lengthStr} m<br>
      Updated: ${updatedAgo}
    `;
    panel.appendChild(infoDiv);
    drawVector(sd.mmsi, sd);
  });
}

// ---------------
// 8) Rysowanie wektora prędkości
// ---------------
function drawVector(mmsi, sd) {
  if (!sd.sog || !sd.cog) return;
  const { latitude: lat, longitude: lon, sog: sogKn, cog: cogDeg } = sd;
  const distNm = sogKn * (vectorLength / 60);
  const cogRad = (cogDeg * Math.PI) / 180;
  const endLat = lat + (distNm / 60) * Math.cos(cogRad);
  let lonScale = Math.cos(lat * Math.PI / 180);
  if (lonScale < 1e-6) lonScale = 1e-6;
  const endLon = lon + ((distNm / 60) * Math.sin(cogRad) / lonScale);
  const line = L.polyline([[lat, lon], [endLat, endLon]], { color: 'blue', dashArray: '4,4' });
  line.addTo(map);
  if (!overlayVectors[mmsi]) { overlayVectors[mmsi] = []; }
  overlayVectors[mmsi].push(line);
}

// ---------------
// 9) computeDistanceNm (pomocniczo, dystans w nm)
// ---------------
function computeDistanceNm(lat1, lon1, lat2, lon2) {
  const R_NM = 3440.065;
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2)**2 + Math.cos(lat1*rad)*Math.cos(lat2*rad)*Math.sin(dLon / 2)**2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R_NM * c;
}