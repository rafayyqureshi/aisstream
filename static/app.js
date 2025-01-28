// ==========================
// app.js (moduł LIVE) – poprawiona wersja
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
// 3) Funkcja główna – inicjalizacja aplikacji
// ---------------
async function initLiveApp() {
  // A) Tworzymy mapę (funkcja z common.js)
  map = initSharedMap('map');

  // B) Warstwa klastrująca do "małych" ikon
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // C) Obsługa zdarzenia zoomend:
  //    Po zmianie zoomu odświeżamy dane, aby natychmiast przełączyć
  //    poligony <-> ikonki (zamiast czekać na interwał 30s).
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
  // A) Zbiór bieżących (aktualnie otrzymanych) MMSI
  const currentSet = new Set(shipsArray.map(s => s.mmsi));

  // B) Usuwamy z mapy i struktur statki, których nie ma w nowym zestawie
  //    1) Markery
  for (const mmsi in shipMarkers) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
    }
  }
  //    2) Polygony
  for (const mmsi in shipPolygonLayers) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      map.removeLayer(shipPolygonLayers[mmsi]);
      delete shipPolygonLayers[mmsi];
    }
  }
  //    3) Wektory
  for (const mmsi in overlayVectors) {
    if (!currentSet.has(parseInt(mmsi, 10))) {
      overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
      delete overlayVectors[mmsi];
    }
  }

  // C) Dodajemy / aktualizujemy obiekty
  const zoomLevel = map.getZoom();
  shipsArray.forEach(ship => {
    const { mmsi, latitude, longitude } = ship;
    const isSelected = selectedShips.includes(mmsi);

    if (zoomLevel < 14) {
      // -> Rysujemy marker (ikonka)
      // Usunięcie polygonu, jeśli istniał
      if (shipPolygonLayers[mmsi]) {
        map.removeLayer(shipPolygonLayers[mmsi]);
        delete shipPolygonLayers[mmsi];
      }
      // Tworzymy lub aktualizujemy marker
      let marker = shipMarkers[mmsi];
      if (!marker) {
        const icon = createShipIcon(ship, isSelected, zoomLevel);
        marker = L.marker([latitude, longitude], { icon })
          .on('click', () => selectShip(mmsi));
        marker.shipData = ship;
        shipMarkers[mmsi] = marker;
        markerClusterGroup.addLayer(marker);
      } else {
        marker.setLatLng([latitude, longitude]);
        marker.setIcon(createShipIcon(ship, isSelected, zoomLevel));
        marker.shipData = ship;
      }

    } else {
      // -> Rysujemy georeferencyjny polygon
      // Usuwamy marker, jeśli istniał
      if (shipMarkers[mmsi]) {
        markerClusterGroup.removeLayer(shipMarkers[mmsi]);
        delete shipMarkers[mmsi];
      }
      // Usuwamy stary polygon
      if (shipPolygonLayers[mmsi]) {
        map.removeLayer(shipPolygonLayers[mmsi]);
        delete shipPolygonLayers[mmsi];
      }
      // Tworzymy nowy polygon (jeśli mamy wymiary)
      const poly = createShipPolygon(ship);
      if (poly) {
        poly.on('click', () => selectShip(mmsi));
        poly.addTo(map);
        poly.shipData = ship;
        shipPolygonLayers[mmsi] = poly;
      }
    }
  });

  // D) Odśwież zaznaczone statki
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

  // Czyścimy poprzednie markery kolizyjne z mapy
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  // Jeśli brak kolizji
  if (!collisionsData || collisionsData.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(noItem);
    return;
  }

  // Mechanizm - pary A-B (bierzemy najnowsze zdarzenie)
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
  const finalColls = Object.values(pairsMap);

  if (finalColls.length === 0) {
    const d = document.createElement('div');
    d.classList.add('collision-item');
    d.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(d);
    return;
  }

  // Tworzymy elementy listy i markery kolizyjne
  finalColls.forEach(c => {
    const splittedHTML = getCollisionSplitCircle(c.mmsi_a, c.mmsi_b, 0, 0, shipMarkers);
    const timeStr = c.timestamp
      ? new Date(c.timestamp).toLocaleTimeString('pl-PL', { hour12: false })
      : '';
    const cpaStr = c.cpa.toFixed(2);
    const tcpaStr = c.tcpa.toFixed(2);

    // Element w liście kolizji
    const item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedHTML}
          <strong>Ships ${c.mmsi_a} – ${c.mmsi_b}</strong><br>
          CPA: ${cpaStr} nm, TCPA: ${tcpaStr} min ${timeStr ? '@ ' + timeStr : ''}
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
      .bindTooltip(`Kolizja: ${c.mmsi_a} & ${c.mmsi_b}`, { direction: 'top', sticky: true })
      .on('click', () => zoomToCollision(c));
    mark.addTo(map);
    collisionMarkers.push(mark);

    // Obsługa kliknięcia w przycisk „zoom”
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
  // Usuwamy wektory prędkości z mapy
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

  // Jeśli żaden statek nie jest zaznaczony
  if (selectedShips.length === 0) {
    return;
  }

  // Budujemy listę danych o zaznaczonych statkach
  const sData = selectedShips.map(mmsi => {
    if (shipMarkers[mmsi]?.shipData) {
      return shipMarkers[mmsi].shipData;
    } else if (shipPolygonLayers[mmsi]?.shipData) {
      return shipPolygonLayers[mmsi].shipData;
    }
    return null;
  }).filter(Boolean);

  // Czyścimy stare wektory prędkości
  for (const mmsi in overlayVectors) {
    overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
  }
  overlayVectors = {};

  // Wyświetlanie informacji i rysowanie wektorów
  sData.forEach(sd => {
    const approxLen = (sd.dim_a && sd.dim_b)
      ? (parseFloat(sd.dim_a) + parseFloat(sd.dim_b)).toFixed(1)
      : 'N/A';

    // Kąt rzeczywisty (heading) lub COG
    const hdgVal = (sd.heading !== undefined && sd.heading !== null)
      ? sd.heading
      : (sd.cog || 0);

    // Tworzymy blok HTML z danymi
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

    // Rysowanie wektora prędkości
    drawVector(sd.mmsi, sd);
  });

  // Jeżeli wybrano dokładnie 2 statki – oblicz i wyświetl CPA/TCPA
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
  // Sprawdzamy, czy mamy dane o prędkości
  if (!sd.sog || !sd.cog) return;

  const { latitude: lat, longitude: lon, sog: sogKn, cog: cogDeg } = sd;
  const distNm = sogKn * (vectorLength / 60);  // miles w danym czasie
  const cogRad = (cogDeg * Math.PI) / 180;

  // 1° szerokości geograficznej ≈ 60 nm
  // 1° długości geograficznej ≈ 60 nm * cos(lat)
  const endLat = lat + (distNm / 60) * Math.cos(cogRad);
  let lonScale = Math.cos(lat * Math.PI / 180);
  if (lonScale < 1e-6) lonScale = 1e-6;
  const endLon = lon + ((distNm / 60) * Math.sin(cogRad) / lonScale);

  // Rysujemy linię
  const line = L.polyline(
    [
      [lat, lon],
      [endLat, endLon]
    ],
    {
      color: 'blue',
      dashArray: '4,4'
    }
  );
  line.addTo(map);

  // Zapis w strukturze overlayVectors
  if (!overlayVectors[mmsi]) {
    overlayVectors[mmsi] = [];
  }
  overlayVectors[mmsi].push(line);
}

// ---------------
// 9) Funkcja pomocnicza do obliczania dystansu (nm)
// ---------------
function computeDistanceNm(lat1, lon1, lat2, lon2) {
  const R_NM = 3440.065; // promień Ziemi w milach morskich
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R_NM * c;
}