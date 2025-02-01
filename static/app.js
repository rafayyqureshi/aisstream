// ==========================
// app.js (LIVE) – finalna wersja z debouncingiem fetch CPA/TCPA oraz spinnerem
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
const API_KEY = "Ais-mon";  // Klucz API (do celów testowych)

let map;
let markerClusterGroup;
let shipMarkers = {};         // mmsi -> L.marker
let shipPolygonLayers = {};   // mmsi -> L.polygon
let overlayVectors = {};      // mmsi -> [L.Polyline]

let collisionMarkers = [];
let collisionsData = [];
let selectedShips = [];       // lista mmsi wybranych statków (max 2)

let shipsInterval = null;
let collisionsInterval = null;

// Timeout dla debouncingu fetch CPA/TCPA
let cpaUpdateTimeout = null;

// Parametry i filtry
let vectorLength = 15;   // minuty (dla rysowania wektora)
let cpaFilter = 0.5;     // slider CPA (Nm)
let tcpaFilter = 10;     // slider TCPA (min)

// ---------------
// Funkcje pomocnicze – Spinner
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
// Funkcja główna – inicjalizacja aplikacji
// ---------------
async function initLiveApp() {
  map = initSharedMap('map');
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);
  
  map.on('zoomend', () => fetchShips());

  document.getElementById('clearSelectedShips').addEventListener('click', clearSelectedShips);

  // Suwak wektora prędkości
  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value, 10) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo();
  });

  // Filtry kolizji
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

  // Pierwsze pobrania
  await fetchShips();
  await fetchCollisions();

  shipsInterval = setInterval(fetchShips, 10000);
  collisionsInterval = setInterval(fetchCollisions, 5000);
}

// ---------------
// Pobieranie i wyświetlanie statków
// ---------------
async function fetchShips() {
  showSpinner('ships-spinner');
  try {
    const res = await fetch('/ships', { headers: { 'X-API-Key': API_KEY } });
    if (!res.ok) throw new Error(`HTTP ${res.status} - ${res.statusText}`);
    updateShips(await res.json());
  } catch (err) {
    console.error("Błąd /ships:", err);
  } finally {
    hideSpinner('ships-spinner');
  }
}

function updateShips(shipsArray) {
  const currentSet = new Set(shipsArray.map(s => s.mmsi));

  Object.keys(shipMarkers).forEach(mmsi => {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
    }
  });

  shipsArray.forEach(ship => {
    const { mmsi, latitude, longitude } = ship;
    const isSelected = selectedShips.includes(mmsi);

    if (!shipMarkers[mmsi]) {
      shipMarkers[mmsi] = L.marker([latitude, longitude])
        .on('click', () => selectShip(mmsi));
      markerClusterGroup.addLayer(shipMarkers[mmsi]);
    } else {
      shipMarkers[mmsi].setLatLng([latitude, longitude]);
    }
  });

  updateSelectedShipsInfo();
}

// ---------------
// Pobieranie i wyświetlanie kolizji
// ---------------
async function fetchCollisions() {
  showSpinner('collisions-spinner');
  try {
    const url = `/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`;
    const res = await fetch(url, { headers: { 'X-API-Key': API_KEY } });
    if (!res.ok) throw new Error(`HTTP ${res.status} – ${res.statusText}`);
    collisionsData = await res.json();
  } catch (err) {
    console.error("Błąd /collisions:", err);
  } finally {
    hideSpinner('collisions-spinner');
  }
}

// ---------------
// Aktualizacja panelu informacji
// ---------------
function updateSelectedShipsInfo() {
  const ship1Div = document.getElementById('selected-ship-1');
  const ship2Div = document.getElementById('selected-ship-2');
  const calcDiv = document.getElementById('calculated-info');

  ship1Div.innerHTML = ship2Div.innerHTML = calcDiv.innerHTML = '';

  if (selectedShips.length === 0) {
    ship1Div.innerHTML = "<i>No ship selected.</i>";
    return;
  }

  function getShipData(mmsi) {
    return shipMarkers[mmsi]?.shipData || null;
  }

  if (selectedShips.length >= 1) {
    const data1 = getShipData(selectedShips[0]);
    if (data1) ship1Div.innerHTML = renderShipInfo(data1);
  }

  if (selectedShips.length >= 2) {
    const data2 = getShipData(selectedShips[1]);
    if (data2) ship2Div.innerHTML = renderShipInfo(data2);

    if (cpaUpdateTimeout) clearTimeout(cpaUpdateTimeout);
    cpaUpdateTimeout = setTimeout(fetchCPA_TCPA, 3000);
  }
}

function fetchCPA_TCPA() {
  const calcDiv = document.getElementById('calculated-info');
  if (selectedShips.length < 2) return;

  const [mA, mB] = selectedShips.sort((a, b) => a - b);
  const url = `/calculate_cpa_tcpa?mmsi_a=${mA}&mmsi_b=${mB}`;

  fetch(url, { headers: { 'X-API-Key': API_KEY } })
    .then(res => res.json())
    .then(data => {
      calcDiv.innerHTML = data.error ? `<b>CPA/TCPA:</b> N/A (${data.error})` 
        : `<b>CPA:</b> ${data.cpa.toFixed(2)} nm, <b>TCPA:</b> ${data.tcpa.toFixed(2)} min`;
    })
    .catch(err => {
      console.error("Błąd CPA/TCPA:", err);
      calcDiv.innerHTML = `<b>CPA/TCPA:</b> Błąd pobierania`;
    });
}

// ---------------
// Wybór i czyszczenie statków
// ---------------
function selectShip(mmsi) {
  if (!selectedShips.includes(mmsi)) {
    if (selectedShips.length >= 2) selectedShips.shift();
    selectedShips.push(mmsi);
    updateSelectedShipsInfo();
  }
}

function clearSelectedShips() {
  selectedShips = [];
  updateSelectedShipsInfo();
}