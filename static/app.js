// ==========================
// app.js (moduł LIVE) – nowa poprawiona wersja
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
let map;
let markerClusterGroup;
let shipMarkers = {};         // klucz: mmsi -> L.marker
let shipPolygonLayers = {};   // klucz: mmsi -> L.polygon
let overlayVectors = {};      // klucz: mmsi -> [L.Polyline wektorów prędkości]

let collisionMarkers = [];
let collisionsData = [];
let selectedShips = [];       // wybrane statki (max 2)

// Interwały
let shipsInterval = null;
let collisionsInterval = null;

// Parametry i filtry
let vectorLength = 15;   // minuty (dla rysowania wektora prędkości)
let cpaFilter = 0.5;     // [0..0.5] param w sliderze
let tcpaFilter = 10;     // [1..10] param w sliderze

// ---------------
// Funkcje pomocnicze
// ---------------
/**
 * buildShipTooltip(ship):
 *   Tworzy zawartość HTML tooltipu
 *   z podstawowymi informacjami: nazwa, COG, SOG, HDG, dł.
 */
function buildShipTooltip(ship) {
  const sogVal = (ship.sog || 0).toFixed(1);
  const cogVal = (ship.cog || 0).toFixed(1);
  const hdgVal = (ship.heading != null) ? ship.heading.toFixed(1) : cogVal;
  let lengthStr = 'N/A';
  if (ship.dim_a && ship.dim_b) {
    lengthStr = (parseFloat(ship.dim_a) + parseFloat(ship.dim_b)).toFixed(1);
  }

  return `
    <div>
      <b>${ship.ship_name || 'Unknown'}</b><br/>
      SOG: ${sogVal} kn | COG: ${cogVal}° | HDG: ${hdgVal}°<br/>
      Len: ${lengthStr} m
    </div>
  `;
}

/**
 * computeTimeAgo(timestamp):
 *   Zwraca krótki opis ile czasu minęło od zdarzenia (np. "12s ago", "2min ago")
 */
function computeTimeAgo(timestamp) {
  if (!timestamp) return '';
  const now = Date.now();
  const then = new Date(timestamp).getTime();
  let diffSec = Math.floor((now - then) / 1000);
  if (diffSec < 0) diffSec = 0;
  if (diffSec < 60) {
    return `${diffSec}s ago`;
  }
  const diffMin = Math.floor(diffSec / 60);
  return `${diffMin}m ago`;
}

// ---------------
// 3) Funkcja główna – inicjalizacja aplikacji
// ---------------
async function initLiveApp() {
  // A) Tworzymy mapę (funkcja z common.js)
  map = initSharedMap('map');

  // B) Warstwa klastrująca do "małych" ikon
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // C) Obsługa zdarzenia zoomend:
  map.on('zoomend', () => {
    fetchShips();  // Wymuszone natychmiastowe odświeżenie
  });

  // D) Obsługa suwaka wektora prędkości (vectorLengthSlider)
  const vectorSlider = document.getElementById('vectorLengthSlider');
  vectorSlider.addEventListener('input', e => {
    vectorLength = parseInt(e.target.value, 10) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(true); // Odśwież wektory prędkości
  });

  // E) Filtry kolizji
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

  // F) Przycisk czyszczący zaznaczone statki
  document.getElementById('clearSelectedShips')
          .addEventListener('click', clearSelectedShips);

  // G) Pierwszy fetch
  await fetchShips();
  await fetchCollisions();

  // H) Cykliczne odświeżanie co 30s
  shipsInterval = setInterval(fetchShips, 30000);
  collisionsInterval = setInterval(fetchCollisions, 30000);
}

// ---------------
// 4) Pobieranie i wyświetlanie statków
// ---------------
async function fetchShips() {
  try {
    const res = await fetch('/ships');
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} - ${res.statusText}`);
    }
    const data = await res.json();
    updateShips(data);
  } catch (err) {
    console.error("Błąd /ships:", err);
  }
}

function updateShips(shipsArray) {
  // A) Zbiór bieżących MMSI
  const currentSet = new Set(shipsArray.map(s => s.mmsi));

  // B) Usuwamy statki spoza nowego zestawu
  // 1) Markery
  for (const mmsi in shipMarkers) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
    }
  }
  // 2) Polygony
  for (const mmsi in shipPolygonLayers) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      map.removeLayer(shipPolygonLayers[mmsi]);
      delete shipPolygonLayers[mmsi];
    }
  }
  // 3) Wektory
  for (const mmsi in overlayVectors) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
      delete overlayVectors[mmsi];
    }
  }

  // C) Dodajemy / aktualizujemy
  const zoomLevel = map.getZoom();
  shipsArray.forEach(ship => {
    const { mmsi, latitude, longitude } = ship;
    const isSelected = selectedShips.includes(mmsi);

    // --- Funkcja do tworzenia tooltipu ---
    const tooltipContent = buildShipTooltip(ship);

    if (zoomLevel < 14) {
      // => Marker
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
        // Nowy tooltip
        marker.bindTooltip(tooltipContent, { direction: 'top', offset: [0, -5] });
        shipMarkers[mmsi] = marker;
        markerClusterGroup.addLayer(marker);
      } else {
        marker.setLatLng([latitude, longitude]);
        marker.setIcon(createShipIcon(ship, isSelected, zoomLevel));
        marker.shipData = ship;
        // Aktualizacja tooltipu
        marker.bindTooltip(tooltipContent, { direction: 'top', offset: [0, -5] });
      }

    } else {
      // => Polygon
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
        // Tooltip
        poly.bindTooltip(tooltipContent, { direction: 'top', sticky: true });
        shipPolygonLayers[mmsi] = poly;
      }
    }
  });

  // D) Odśwież "selected"
  updateSelectedShipsInfo(false);
}

// ---------------
// 5) Obsługa kolizji
// ---------------
async function fetchCollisions() {
  try {
    const url = `/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`;
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} – ${res.statusText}`);
    }
    collisionsData = await res.json() || [];
    updateCollisionsList();
  } catch (err) {
    console.error("Błąd /collisions:", err);
  }
}

function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  // Usuwamy stare markery kolizyjne
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if (!collisionsData || collisionsData.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(noItem);
    return;
  }

  // Mechanizm - pary A-B (nowsze nadpisują starsze)
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

  // -- Sortujemy wg rosnącego tcpa --
  finalColls.sort((a, b) => a.tcpa - b.tcpa);

  if (finalColls.length === 0) {
    const d = document.createElement('div');
    d.classList.add('collision-item');
    d.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(d);
    return;
  }

  finalColls.forEach(c => {
    // splitted circle => 
    const splittedHTML = getCollisionSplitCircle(c.mmsi_a, c.mmsi_b, 0, 0, shipMarkers);
    const cpaStr = c.cpa.toFixed(2);
    const tcpaStr = c.tcpa.toFixed(2);

    // Zakładamy, że backend zwraca c.ship1_name i c.ship2_name 
    // lub fallback do c.mmsi_a i c.mmsi_b
    const shipAName = c.ship1_name || String(c.mmsi_a);
    const shipBName = c.ship2_name || String(c.mmsi_b);

    // Czas "last updated"
    const updatedStr = computeTimeAgo(c.timestamp);

    // Wyświetlenie
    const item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <strong>${shipAName}</strong>
          ${splittedHTML}
          <strong>${shipBName}</strong><br/>
          CPA: ${cpaStr} nm, TCPA: ${tcpaStr} min, <span style="color:gray;">${updatedStr}</span>
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    collisionList.appendChild(item);

    // Marker kolizji na mapie
    const latC = (c.latitude_a + c.latitude_b) / 2;
    const lonC = (c.longitude_a + c.longitude_b) / 2;
    const collisionIcon = L.divIcon({
      className: '',
      html: `
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <path d="M0,-7 7,7 -7,7 Z"
                fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="4" text-anchor="middle"
                font-size="8" fill="red">!</text>
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

    // Obsługa kliknięcia "zoom"
    const zoomBtn = item.querySelector('.zoom-button');
    zoomBtn.addEventListener('click', () => {
      zoomToCollision(c);
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
// 6) Zaznaczanie statków
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
  // Wektory
  for (const mmsi in overlayVectors) {
    overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
  }
  overlayVectors = {};
  updateSelectedShipsInfo(false);
}

// ---------------
// 7) Aktualizacja informacji o zaznaczonych statkach
// ---------------
function updateSelectedShipsInfo(selectionChanged) {
  const panel = document.getElementById('selected-ships-info');
  const pairInfo = document.getElementById('pair-info');

  panel.innerHTML = '';
  pairInfo.innerHTML = '';

  if (selectedShips.length === 0) {
    return;
  }

  // Lista danych z markerów / polygonów
  const sData = selectedShips.map(mmsi => {
    if (shipMarkers[mmsi]?.shipData) {
      return shipMarkers[mmsi].shipData;
    } else if (shipPolygonLayers[mmsi]?.shipData) {
      return shipPolygonLayers[mmsi].shipData;
    }
    return null;
  }).filter(Boolean);

  // Czyścimy stare wektory
  for (const mmsi in overlayVectors) {
    overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
  }
  overlayVectors = {};

  sData.forEach(sd => {
    const approxLen = (sd.dim_a && sd.dim_b)
      ? (parseFloat(sd.dim_a) + parseFloat(sd.dim_b)).toFixed(1)
      : 'N/A';
    const hdgVal = (sd.heading !== undefined && sd.heading !== null)
      ? sd.heading
      : (sd.cog || 0);

    const infoDiv = document.createElement('div');
    infoDiv.innerHTML = `
      <b>${sd.ship_name || 'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${(sd.sog || 0).toFixed(1)} kn, 
      COG: ${(sd.cog || 0).toFixed(1)}°<br>
      HDG: ${hdgVal.toFixed(1)}°<br>
      Len: ${approxLen}
    `;
    panel.appendChild(infoDiv);

    // Rysuj wektor
    drawVector(sd.mmsi, sd);
  });

  // Jeśli 2 statki -> oblicz CPA/TCPA
  if (selectedShips.length === 2) {
    const [mA, mB] = selectedShips;
    const posA = sData.find(s => s?.mmsi === mA);
    const posB = sData.find(s => s?.mmsi === mB);

    let distNm = null;
    if (posA && posB) {
      distNm = computeDistanceNm(
        posA.latitude, posA.longitude,
        posB.latitude, posB.longitude
      );
    }
    const sorted = [mA, mB].sort((x, y) => x - y);
    const url = `/calculate_cpa_tcpa?mmsi_a=${sorted[0]}&mmsi_b=${sorted[1]}`;

    fetch(url)
      .then(r => r.json())
      .then(data => {
        if (data.error) {
          pairInfo.innerHTML = `
            ${distNm !== null ? `<b>Distance:</b> ${distNm.toFixed(2)} nm<br>` : ''}
            <b>CPA/TCPA:</b> N/A (${data.error})
          `;
        } else {
          const cpaVal = (data.cpa >= 9999) ? 'n/a' : data.cpa.toFixed(2);
          const tcpaVal = (data.tcpa < 0) ? 'n/a' : data.tcpa.toFixed(2);
          pairInfo.innerHTML = `
            ${distNm !== null ? `<b>Distance:</b> ${distNm.toFixed(2)} nm<br>` : ''}
            <b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min
          `;
        }
      })
      .catch(err => {
        console.error("Błąd /calculate_cpa_tcpa:", err);
        pairInfo.innerHTML = `
          ${distNm !== null ? `<b>Distance:</b> ${distNm.toFixed(2)} nm<br>` : ''}
          <b>CPA/TCPA:</b> N/A
        `;
      });
  }
}

// ---------------
// 8) Rysowanie wektora prędkości
// ---------------
function drawVector(mmsi, sd) {
  if (!sd.sog || !sd.cog) return;

  const { latitude: lat, longitude: lon, sog: sogKn, cog: cogDeg } = sd;
  const distNm = sogKn * (vectorLength / 60);
  const cogRad = (cogDeg * Math.PI) / 180;

  // 1° szer. geogr. ~ 60 nm
  let endLat = lat + (distNm / 60) * Math.cos(cogRad);
  let lonScale = Math.cos(lat * Math.PI / 180);
  if (lonScale < 1e-6) lonScale = 1e-6;
  let endLon = lon + ((distNm / 60) * Math.sin(cogRad) / lonScale);

  const line = L.polyline([[lat, lon],[endLat, endLon]], {
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
// 9) Funkcja pomocnicza do obliczania dystansu (nm)
// ---------------
function computeDistanceNm(lat1, lon1, lat2, lon2) {
  const R_NM = 3440.065;
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2)**2
          + Math.cos(lat1*rad)*Math.cos(lat2*rad)*Math.sin(dLon / 2)**2;
  const c = 2*Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R_NM*c;
}