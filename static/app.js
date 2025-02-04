// ==========================
// app.js (LIVE) – ulepszona wersja
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
const API_KEY = "Ais-mon";  // Klucz API testowy

let map;
let markerClusterGroup;
let shipMarkers = {};         // mmsi -> L.marker
let shipPolygonLayers = {};   // mmsi -> L.polygon
let overlayVectors = {};      // mmsi -> [L.Polyline]

let collisionMarkers = [];
let collisionsData = [];
let selectedShips = [];       // max 2

// Interwały
let shipsInterval = null;
let collisionsInterval = null;

// Parametry suwaków / filtrów
let vectorLength = 15; // minuty do rysowania wektora
let cpaFilter = 0.5;
let tcpaFilter = 10;

// Debounce do suwaka
let vectorSliderDebounce = null;

// ---------------
// Spinner (ikona/tekst)
// ---------------
function showSpinner(id) {
  const spinner = document.getElementById(id);
  if (spinner) spinner.style.display = 'block';
}
function hideSpinner(id) {
  const spinner = document.getElementById(id);
  if (spinner) spinner.style.display = 'none';
}

// ---------------
// Funkcje pomocnicze – tooltip, time ago
// ---------------
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

function computeTimeAgo(timestamp) {
  if (!timestamp) return '';
  const now = Date.now();
  const then = new Date(timestamp).getTime();
  let diffSec = Math.floor((now - then) / 1000);
  if (diffSec < 0) diffSec = 0;
  if (diffSec < 60) {
    return `${diffSec}s ago`;
  } else {
    const mm = Math.floor(diffSec / 60);
    const ss = diffSec % 60;
    return `${mm}m ${ss}s ago`;
  }
}

// ---------------
// 3) Funkcja główna – inicjalizacja
// ---------------
async function initLiveApp() {
  map = initSharedMap('map');
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // Odśwież statki przy zoomie
  map.on('zoomend', () => fetchShips());

  // Suwak wektora – z debounce
  const vectorSlider = document.getElementById('vectorLengthSlider');
  vectorSlider.addEventListener('input', e => {
    const val = parseInt(e.target.value, 10) || 15;
    document.getElementById('vectorLengthValue').textContent = val;
    if (vectorSliderDebounce) clearTimeout(vectorSliderDebounce);
    vectorSliderDebounce = setTimeout(() => {
      vectorLength = val;
      updateSelectedShipsInfo(true);
    }, 300);
  });

  // Suwaki cpa / tcpa
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

  // Przycisk czyszczący
  document.getElementById('clearSelectedShips').addEventListener('click', clearSelectedShips);

  // Pierwsze pobranie
  await fetchShips();
  await fetchCollisions();

  // Interwały rzadziej
  shipsInterval = setInterval(fetchShips, 15000);       // co 15s
  collisionsInterval = setInterval(fetchCollisions, 20000); // co 20s
}

// ---------------
// 4) Pobieranie + wyświetlanie statków
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

  // Usuwamy statki, poligony i wektory, których już nie ma
  for (const mmsi in shipMarkers) {
    if (!currentSet.has(+mmsi)) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
    }
  }
  for (const mmsi in shipPolygonLayers) {
    if (!currentSet.has(+mmsi)) {
      map.removeLayer(shipPolygonLayers[mmsi]);
      delete shipPolygonLayers[mmsi];
    }
  }
  for (const mmsi in overlayVectors) {
    if (!currentSet.has(+mmsi)) {
      overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
      delete overlayVectors[mmsi];
    }
  }

  const zoomLevel = map.getZoom();
  shipsArray.forEach(ship => {
    const { mmsi, latitude, longitude } = ship;
    const isSelected = selectedShips.includes(mmsi);
    const tooltipContent = buildShipTooltip(ship);

    // Gdyby istniał jakiś stary wektor – usuwamy
    if (overlayVectors[mmsi]) {
      overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
      delete overlayVectors[mmsi];
    }

    const hasLength = (ship.dim_a && ship.dim_b);

if (!hasLength) {
  // Nieznana długość – zawsze rysuj marker
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
} else if (zoomLevel < 14) {
  // Przy zoomie mniejszym niż 14 – rysujemy marker
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
  // Przy zoomie >= 14 i gdy znana jest długość – rysujemy poligon
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

  // Rysujemy ewentualne wektory (panel)
  updateSelectedShipsInfo(true);
}

// ---------------
// 5) Pobieranie + wyświetlanie kolizji
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

  // Usuwamy stare markery
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if (!collisionsData || collisionsData.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>No collisions currently.</i>';
    collisionList.appendChild(noItem);
    return;
  }

  // Zapobiegamy usunięciu wszystkich kolizji: 
  // mechanizm pairsMap => weź najnowszy rekord
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

  // finalColls = unikalne pary, najnowsze
  let finalColls = Object.values(pairsMap);
  // Sort po tcpa
  finalColls.sort((a, b) => a.tcpa - b.tcpa);

  if (finalColls.length === 0) {
    const d = document.createElement('div');
    d.classList.add('collision-item');
    d.innerHTML = '<i>No collisions currently.</i>';
    collisionList.appendChild(d);
    return;
  }

  finalColls.forEach(c => {
    // splitted circle
    const splittedHTML = getCollisionSplitCircle(c.mmsi_a, c.mmsi_b, 0, 0, shipMarkers);
    const cpaStr = c.cpa.toFixed(2);
    const tcpaStr = c.tcpa.toFixed(2);
    const updatedStr = c.timestamp ? computeTimeAgo(c.timestamp) : '';
    // Jeżeli collisions ma nazwy -> c.ship_name_a, c.ship_name_b;
    // lub fallback -> c.mmsi_a, c.mmsi_b
    const shipAName = c.ship_name_a || String(c.mmsi_a);
    const shipBName = c.ship_name_b || String(c.mmsi_b);

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

    // Rysowanie markera kolizyjnego
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
      .bindTooltip(`Collision: ${shipAName} & ${shipBName}`, { direction: 'top', sticky: true })
      .on('click', () => zoomToCollision(c));
    mark.addTo(map);
    collisionMarkers.push(mark);

    // Zoom button
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

// ---------------
// Wybor statku
// ---------------
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

// ---------------
// 6) Panel lewy – wyświetlanie i lokalne liczenie CPA/TCPA
// ---------------

// Lokalna implementacja obliczeń, analogiczna do cpa_utils:
// (Można przenieść do common.js, jeśli chcesz)
function toXY(lat, lon) {
  // Konwersja współrzędnych geograficznych na przybliżone współrzędne kartezjańskie (NM)
  const x = lon * 60 * Math.cos(lat * Math.PI / 180);
  const y = lat * 60;
  return [x, y];
}

function cogToVector(cog, sog) {
  // Przekształcenie kursu (stopnie) i prędkości (kn) w wektor prędkości (NM/h)
  const rad = (cog * Math.PI) / 180;
  const vx = sog * Math.sin(rad);
  const vy = sog * Math.cos(rad);
  return [vx, vy];
}

function computeLocalCPATCPA(shipA, shipB) {
  // Obliczenia na podstawie przybliżenia "flat earth" – wszystkie jednostki są w NM lub NM/h.
  const [xA, yA] = toXY(shipA.latitude, shipA.longitude);
  const [xB, yB] = toXY(shipB.latitude, shipB.longitude);
  const dx = xA - xB;
  const dy = yA - yB;

  const [vxA, vyA] = cogToVector(shipA.cog, shipA.sog);
  const [vxB, vyB] = cogToVector(shipB.cog, shipB.sog);
  const dvx = vxA - vxB;
  const dvy = vyA - vyB;

  const v2 = dvx * dvx + dvy * dvy;
  let tcpaHours = 0;
  if (v2 !== 0) {
    // tcpa w godzinach – jednostki: [NM * (NM/h)] / [(NM/h)²] = h
    tcpaHours = - (dx * dvx + dy * dvy) / v2;
    if (tcpaHours < 0) {
      tcpaHours = 0;
    }
  }
  // Przeliczamy tcpa z godzin na minuty
  const tcpaMinutes = tcpaHours * 60;

  // Obliczamy pozycje w momencie CPA
  const xA_cpa = xA + vxA * tcpaHours;
  const yA_cpa = yA + vyA * tcpaHours;
  const xB_cpa = xB + vxB * tcpaHours;
  const yB_cpa = yB + vyB * tcpaHours;

  // CPA – dystans między statkami w punkcie najbliższego podejścia (NM)
  const cpa = Math.sqrt((xA_cpa - xB_cpa) ** 2 + (yA_cpa - yB_cpa) ** 2);

  // Zwracamy CPA w NM oraz TCPA w minutach
  return [cpa, tcpaMinutes];
}

function haversineNM(lat1, lon1, lat2, lon2) {
  // Oblicza dystans w milach morskich przy użyciu wzoru Haversine
  const R_NM = 3440.065;
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2) ** 2 +
            Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R_NM * c;
}

// Główna funkcja
function updateSelectedShipsInfo(forceRedraw) {
  const headerDiv = document.getElementById('left-panel-header');
  const ship1Div = document.getElementById('selected-ship-1');
  const ship2Div = document.getElementById('selected-ship-2');
  const calcDiv = document.getElementById('calculated-info');

  // Górna sekcja
  headerDiv.innerHTML = `
    <h2>Selected Ships
      <button id="clearSelectedShips">Clear</button>
    </h2>
  `;
  document.getElementById('clearSelectedShips').addEventListener('click', clearSelectedShips);

  ship1Div.innerHTML = '';
  ship2Div.innerHTML = '';
  calcDiv.innerHTML = '';

  if (selectedShips.length === 0) {
    ship1Div.innerHTML = "<i>No ship selected.</i>";
    document.getElementById('vector-length-container').style.display = 'none';
    return;
  }

  // Helper
  function getShipData(mmsi) {
    if (shipMarkers[mmsi]?.shipData) return shipMarkers[mmsi].shipData;
    if (shipPolygonLayers[mmsi]?.shipData) return shipPolygonLayers[mmsi].shipData;
    return null;
  }

  // Rysowanie info
  const firstData = getShipData(selectedShips[0]);
  if (firstData) {
    ship1Div.innerHTML = renderShipInfo(firstData);
    drawVector(firstData.mmsi, firstData);
  } else {
    ship1Div.innerHTML = "<i>No data for first ship.</i>";
  }
  if (selectedShips.length === 2) {
    const secondData = getShipData(selectedShips[1]);
    if (secondData) {
      ship2Div.innerHTML = renderShipInfo(secondData);
      drawVector(secondData.mmsi, secondData);
    } else {
      ship2Div.innerHTML = "<i>No data for second ship.</i>";
    }
    document.getElementById('vector-length-container').style.display = 'block';

    // Liczymy lok. cpa/tcpa, dystans
    if (firstData && secondData) {
      const [cpaVal, tcpaVal] = computeLocalCPATCPA(firstData, secondData);
      const distVal = haversineNM(
        firstData.latitude, firstData.longitude,
        secondData.latitude, secondData.longitude
      );
      calcDiv.innerHTML = `
        <b>Distance:</b> ${distVal.toFixed(2)} nm<br>
        <b>CPA/TCPA:</b> ${cpaVal.toFixed(2)} nm / ${tcpaVal.toFixed(2)} min
      `;
    } else {
      calcDiv.innerHTML = "<i>Cannot compute CPA/TCPA - missing data.</i>";
    }
  } else {
    // Tylko 1 statek
    document.getElementById('vector-length-container').style.display = 'none';
  }

  function renderShipInfo(s) {
    const sogVal = (s.sog || 0).toFixed(1);
    const cogVal = (s.cog || 0).toFixed(1);
    const hdgVal = (s.heading != null) ? s.heading.toFixed(1) : cogVal;
    let lengthStr = 'N/A';
    if (s.dim_a && s.dim_b) {
      const lenMeters = parseFloat(s.dim_a) + parseFloat(s.dim_b);
      lengthStr = lenMeters.toFixed(1);
    }
    let updatedAgo = s.timestamp ? computeTimeAgo(s.timestamp) : '';
    return `
      <div>
        <b>${s.ship_name || 'Unknown'}</b><br>
        MMSI: ${s.mmsi}<br>
        SOG: ${sogVal} kn, COG: ${cogVal}°<br>
        HDG: ${hdgVal}°, LEN: ${lengthStr} m<br>
        Updated: ${updatedAgo}
      </div>
    `;
  }
}

// ---------------
// Rysowanie wektora
// ---------------
function drawVector(mmsi, sd) {
  if (!sd || !sd.sog || !sd.cog) return;

  // Usuwamy stare
  if (overlayVectors[mmsi]) {
    overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
    overlayVectors[mmsi] = [];
  }

  const { latitude: lat, longitude: lon, sog: sogKn, cog: cogDeg } = sd;
  const distNm = sogKn * (vectorLength / 60);
  const rad = (cogDeg * Math.PI) / 180;
  const endLat = lat + (distNm / 60) * Math.cos(rad);
  let lonScale = Math.cos(lat * Math.PI / 180);
  if (lonScale < 1e-6) lonScale = 1e-6;
  const endLon = lon + ((distNm / 60) * Math.sin(rad) / lonScale);

  const line = L.polyline([[lat, lon], [endLat, endLon]], {
    color: 'blue',
    dashArray: '4,4'
  });
  line.addTo(map);

  if (!overlayVectors[mmsi]) overlayVectors[mmsi] = [];
  overlayVectors[mmsi].push(line);
}

// ---------------
// computeDistanceNm
// ---------------
function computeDistanceNm(lat1, lon1, lat2, lon2) {
  const R_NM = 3440.065;
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2)**2
          + Math.cos(lat1*rad)*Math.cos(lat2*rad)*Math.sin(dLon / 2)**2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R_NM * c;
}