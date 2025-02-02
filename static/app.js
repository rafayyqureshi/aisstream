// ==========================
// app.js (LIVE) – wersja z natychmiastowym fetch CPA/TCPA, 
//                poprawionym wyświetlaniem wektorów i slidera
// ==========================

// 1) Zdarzenia startowe
document.addEventListener('DOMContentLoaded', () => {
  initLiveApp().catch(err => console.error("Błąd initLiveApp:", err));
});

// 2) Zmienne globalne
const API_KEY = "Ais-mon";  // Klucz API (na potrzeby testów)

let map;
let markerClusterGroup;
let shipMarkers = {};         // mmsi -> L.marker
let shipPolygonLayers = {};   // mmsi -> L.polygon
let overlayVectors = {};      // mmsi -> [L.Polyline]

let collisionMarkers = [];
let collisionsData = [];
let selectedShips = [];       // max 2 statki

let shipsInterval = null;
let collisionsInterval = null;

// Parametry suwaków / filtrów
let vectorLength = 15; // minuty (do rysowania wektorów)
let cpaFilter = 0.5;
let tcpaFilter = 10;

// ---------------
// Spinner – mała ikona/tekst w rogu
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
// Funkcje pomocnicze – tooltip, „time ago”
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
// Funkcja główna – inicjalizacja
// ---------------
async function initLiveApp() {
  map = initSharedMap('map'); // Funkcja z common.js
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // Odśwież statki przy zmianie zoomu
  map.on('zoomend', () => {
    fetchShips();
  });

  // Obsługa suwaka wektorów
  const vectorSlider = document.getElementById('vectorLengthSlider');
  vectorSlider.addEventListener('input', e => {
    vectorLength = parseInt(e.target.value, 10) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    // Gdy zmieniamy długość wektorów, przerysuj je dla zaznaczonych statków
    updateSelectedShipsInfo(true);
  });

  // Obsługa suwaków cpa i tcpa
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

  // Pierwsze pobrania
  await fetchShips();
  await fetchCollisions();

  // Interwały automatycznego odświeżania
  shipsInterval = setInterval(fetchShips, 10000);
  collisionsInterval = setInterval(fetchCollisions, 5000);
}

// ---------------
// 4) Pobieranie + wyświetlanie statków
// ---------------
async function fetchShips() {
  showSpinner('ships-spinner');
  try {
    const res = await fetch('/ships', { headers: { 'X-API-Key': API_KEY } });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} - ${res.statusText}`);
    }
    const data = await res.json();
    updateShips(data);
  } catch (err) {
    console.error("Błąd /ships:", err);
  } finally {
    hideSpinner('ships-spinner');
  }
}

function updateShips(shipsArray) {
  // Mmsi, które przyszły w nowym zbiorze
  const currentSet = new Set(shipsArray.map(s => s.mmsi));

  // Usuwamy statki, których już nie ma
  for (const mmsi in shipMarkers) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      // Usuń stary marker
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
    }
  }
  // Usuwamy poligony
  for (const mmsi in shipPolygonLayers) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      map.removeLayer(shipPolygonLayers[mmsi]);
      delete shipPolygonLayers[mmsi];
    }
  }
  // Usuwamy wektory
  for (const mmsi in overlayVectors) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
      delete overlayVectors[mmsi];
    }
  }

  // Aktualny zoom
  const zoomLevel = map.getZoom();

  shipsArray.forEach(ship => {
    const { mmsi, latitude, longitude } = ship;
    const isSelected = selectedShips.includes(mmsi);
    const tooltipContent = buildShipTooltip(ship);

    // Zanim narysujemy nowy wektor (dla wybranych statków), usuń stary wektor
    if (overlayVectors[mmsi]) {
      overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
      delete overlayVectors[mmsi];
    }

    if (zoomLevel < 14) {
      // -> rysujemy marker
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
        // Aktualizujemy pozycję i icon
        marker.setLatLng([latitude, longitude]);
        marker.setIcon(createShipIcon(ship, isSelected, zoomLevel));
        marker.shipData = ship;
        marker.bindTooltip(tooltipContent, { direction: 'top', offset: [0, -5] });
      }
    } else {
      // -> rysujemy poligon
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

  // Po aktualizacji statków – rysujemy ewentualne wektory tylko dla wybranych (update panelu)
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

  // Usuń stare markery kolizyjne
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if (!collisionsData || collisionsData.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>No collisions currently.</i>';
    collisionList.appendChild(noItem);
    return;
  }

  // Grupujemy nowsze kolizje dla tych samych par
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
    d.innerHTML = '<i>No collisions currently.</i>';
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

    // Marker kolizyjny
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

// Zoom do kolizji
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
    // Zawsze max 2 statki
    if (selectedShips.length >= 2) {
      selectedShips.shift();
    }
    selectedShips.push(mmsi);
    updateSelectedShipsInfo(true);
  }
}

// Czyszczenie zaznaczenia
function clearSelectedShips() {
  selectedShips = [];
  // Usuwamy stare wektory
  for (const mmsi in overlayVectors) {
    overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
  }
  overlayVectors = {};
  updateSelectedShipsInfo(false);
}

// ---------------
// Panel lewy – natychmiastowe obliczanie CPA/TCPA
// ---------------
function updateSelectedShipsInfo(selectionChanged = false) {
  const headerDiv = document.getElementById('left-panel-header');
  const ship1Div = document.getElementById('selected-ship-1');
  const ship2Div = document.getElementById('selected-ship-2');
  const calcDiv = document.getElementById('calculated-info');

  // Nagłówek
  headerDiv.innerHTML = `
    <h2>
      Selected Ships
      <button id="clearSelectedShips">Clear</button>
    </h2>
  `;
  document.getElementById('clearSelectedShips').addEventListener('click', clearSelectedShips);

  // Czyścimy
  ship1Div.innerHTML = '';
  ship2Div.innerHTML = '';
  calcDiv.innerHTML = '';

  // Brak statków -> info
  if (selectedShips.length === 0) {
    ship1Div.innerHTML = "<i>No ship selected.</i>";
    // Ukryj slider wektora
    document.getElementById('vector-length-container').style.display = 'none';
    return;
  }

  // Helper – pobranie danych statku
  function getShipData(mmsi) {
    if (shipMarkers[mmsi]?.shipData) return shipMarkers[mmsi].shipData;
    if (shipPolygonLayers[mmsi]?.shipData) return shipPolygonLayers[mmsi].shipData;
    return null;
  }

  // Render info 1
  const data1 = getShipData(selectedShips[0]);
  if (data1) {
    ship1Div.innerHTML = renderShipInfo(data1);
    // Rysujemy wektor
    drawVector(data1.mmsi, data1);
  }

  // Jeżeli dwa statki
  if (selectedShips.length === 2) {
    const data2 = getShipData(selectedShips[1]);
    if (data2) {
      ship2Div.innerHTML = renderShipInfo(data2);
      drawVector(data2.mmsi, data2);
    }
    // Pokaż slider wektora
    document.getElementById('vector-length-container').style.display = 'block';

    // Natychmiastowy fetch CPA/TCPA
    // (Za każdym zaznaczeniem lub aktualizacją)
    const [mA, mB] = selectedShips.slice().sort((a, b) => a - b);
    const url = `/calculate_cpa_tcpa?mmsi_a=${mA}&mmsi_b=${mB}`;
    fetch(url, { headers: { 'X-API-Key': API_KEY } })
      .then(r => r.json())
      .then(data => {
        let distanceStr = '';
        const d1 = getShipData(mA);
        const d2 = getShipData(mB);
        if (d1 && d2) {
          const dist = computeDistanceNm(d1.latitude, d1.longitude, d2.latitude, d2.longitude);
          distanceStr = `<b>Distance:</b> ${dist.toFixed(2)} nm<br>`;
        }
        if (data.error) {
          calcDiv.innerHTML = `${distanceStr}<b>CPA/TCPA:</b> N/A (${data.error})`;
        } else {
          const cpaVal = (data.cpa >= 9999) ? 'n/a' : data.cpa.toFixed(2);
          const tcpaVal = (data.tcpa < 0) ? 'n/a' : data.tcpa.toFixed(2);
          calcDiv.innerHTML = `${distanceStr}<b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min`;
        }
      })
      .catch(err => {
        console.error("Error /calculate_cpa_tcpa:", err);
        calcDiv.innerHTML = `<b>CPA/TCPA:</b> N/A`;
      });
  } else {
    // Tylko jeden statek -> ukrywamy slider, brak obliczeń
    document.getElementById('vector-length-container').style.display = 'none';
  }

  // Pomocnicza funkcja do renderowania
  function renderShipInfo(ship) {
    const sogVal = (ship.sog || 0).toFixed(1);
    const cogVal = (ship.cog || 0).toFixed(1);
    const hdgVal = (ship.heading != null) ? ship.heading.toFixed(1) : cogVal;
    let lengthStr = 'N/A';
    if (ship.dim_a && ship.dim_b) {
      const lenMeters = parseFloat(ship.dim_a) + parseFloat(ship.dim_b);
      lengthStr = lenMeters.toFixed(1);
    }
    let updatedAgo = ship.timestamp ? computeTimeAgo(ship.timestamp) : '';
    return `
      <div>
        <b>${ship.ship_name || 'Unknown'}</b><br>
        MMSI: ${ship.mmsi}<br>
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
  if (!sd?.sog || !sd?.cog) return;

  // Usuń stare wektory (asekuracyjnie, w razie braku usunięcia w updateShips)
  if (overlayVectors[mmsi]) {
    overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
    overlayVectors[mmsi] = [];
  }

  const { latitude: lat, longitude: lon, sog: sogKn, cog: cogDeg } = sd;
  const distNm = sogKn * (vectorLength / 60);
  const cogRad = (cogDeg * Math.PI) / 180;
  const endLat = lat + (distNm / 60) * Math.cos(cogRad);
  let lonScale = Math.cos(lat * Math.PI / 180);
  if (lonScale < 1e-6) lonScale = 1e-6;
  const endLon = lon + ((distNm / 60) * Math.sin(cogRad) / lonScale);

  const line = L.polyline([[lat, lon], [endLat, endLon]], {
    color: 'blue',
    dashArray: '4,4'
  });
  line.addTo(map);

  if (!overlayVectors[mmsi]) {
    overlayVectors[mmsi] = [];
  }
  overlayVectors[mmsi].push(line);
}

// ---------------
// Dystans w NM
// ---------------
function computeDistanceNm(lat1, lon1, lat2, lon2) {
  const R_NM = 3440.065;
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2)**2 +
            Math.cos(lat1*rad)*Math.cos(lat2*rad)*
            Math.sin(dLon / 2)**2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R_NM * c;
}