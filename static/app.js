// ==========================
// app.js (moduł LIVE)
// ==========================

document.addEventListener('DOMContentLoaded', initLiveApp);

let map;
let markerClusterGroup;
let shipMarkers = {};       // klucz: mmsi -> L.marker
let overlayMarkers = {};    // klucz: mmsi -> [list of polylines]
let selectedShips = [];

// Tablica kolizji i markerów kolizyjnych
let collisionsData = [];
let collisionMarkers = [];

// Parametry sterujące z suwaków
let vectorLength = 15;   // w minutach
let cpaFilter = 0.5;     // [0..0.5]
let tcpaFilter = 10;     // [1..10]

// Odświeżanie
let shipsInterval = null;
let collisionsInterval = null;

/**
 * Funkcja inicjalizująca moduł LIVE (wywoływana po DOMContentLoaded).
 */
function initLiveApp() {
  // 1) Tworzymy mapę z pliku common.js
  map = initSharedMap('map');

  // 2) Dodajemy warstwę klastrującą statki
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // 3) Obsługa UI (suwaki i przyciski)
  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    // Odświeżamy (m.in. wektory prędkości) i ewentualnie CPA/TCPA
    updateSelectedShipsInfo(true);
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
  document.getElementById('clearSelectedShips').addEventListener('click', clearSelectedShips);

  // 4) Pierwsze pobranie statków i kolizji
  fetchShips();
  fetchCollisions();

  // 5) Odświeżamy co 60 sekund (lub wg potrzeb)
  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

// ----------------------------------
// 1) Obsługa statków
// ----------------------------------
function fetchShips() {
  fetch('/ships')
    .then(res => res.json())
    .then(data => updateShips(data))
    .catch(err => console.error("Błąd /ships:", err));
}

function updateShips(shipsArray) {
  // Zbiór MMSI z nowego fetchu
  const currentSet = new Set(shipsArray.map(s => s.mmsi));

  // Usuwamy statki, których już nie ma
  for (let mmsi in shipMarkers) {
    if (!currentSet.has(parseInt(mmsi))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      if (overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(ln => map.removeLayer(ln));
        delete overlayMarkers[mmsi];
      }
    }
  }

  // Dodajemy / aktualizujemy statki
  shipsArray.forEach(ship => {
    const mmsi = ship.mmsi;
    const isSelected = selectedShips.includes(mmsi);

    // Ikona statku z pliku common.js
    const icon = createShipIcon(ship, isSelected);

    // Tooltip
    const now = Date.now();
    const updTs = ship.timestamp ? new Date(ship.timestamp).getTime() : 0;
    const diffSec = Math.floor((now - updTs)/1000);
    const mm = Math.floor(diffSec/60);
    const ss = diffSec % 60;
    const diffStr = `${mm}m ${ss}s ago`;

    const ttHTML = `
      <b>${ship.ship_name || 'Unknown'}</b><br>
      MMSI: ${mmsi}<br>
      SOG: ${ship.sog || 0} kn, COG: ${ship.cog || 0}°<br>
      Len: ${ship.ship_length || 'N/A'}<br>
      Updated: ${diffStr}
    `;

    let marker = shipMarkers[mmsi];
    if (!marker) {
      // Nowy statek
      marker = L.marker([ship.latitude, ship.longitude], { icon })
        .on('click', () => selectShip(mmsi));
      marker.bindTooltip(ttHTML, { direction:'top', sticky:true });
      marker.shipData = ship;

      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      // Aktualizacja
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(ttHTML);
      marker.shipData = ship;
    }
  });

  // Odświeżamy zaznaczenia i wektory
  updateSelectedShipsInfo(false);
}

// ----------------------------------
// 2) Obsługa kolizji
// ----------------------------------
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(res => res.json())
    .then(data => {
      collisionsData = data || [];
      updateCollisionsList();
    })
    .catch(err => console.error("Błąd /collisions:", err));
}

/**
 * updateCollisionsList():
 *  - Filtrowanie i grupowanie kolizji (tcpa >= 0, timestamp + tcpa > now, itp.)
 *  - Wyświetlanie w panelu z splitted circle
 *  - Rysowanie markerów kolizyjnych na mapie
 */
function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  // Usuwamy stare kolizyjne markery
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if (!collisionsData || collisionsData.length === 0) {
    let noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(noItem);
    return;
  }

  const nowMs = Date.now();

  // 1) Filtrowanie (np. statki minęły się lub kolizja w przeszłości)
  let filtered = collisionsData.filter(c => {
    if (c.tcpa < 0) return false;
    if (c.timestamp) {
      let collTime = new Date(c.timestamp).getTime();
      let futureMs = collTime + (c.tcpa*60000);
      if (futureMs < nowMs) {
        return false;
      }
    }
    return true;
  });

  if (filtered.length === 0) {
    let d = document.createElement('div');
    d.classList.add('collision-item');
    d.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(d);
    return;
  }

  // 2) Grupowanie wg pary (mmsi_a, mmsi_b)
  let pairsMap = {};
  filtered.forEach(c => {
    let a = Math.min(c.mmsi_a, c.mmsi_b);
    let b = Math.max(c.mmsi_a, c.mmsi_b);
    let key = `${a}_${b}`;

    if (!pairsMap[key]) {
      pairsMap[key] = c;
    } else {
      // Bierzemy nowszy timestamp
      let oldT = new Date(pairsMap[key].timestamp).getTime();
      let newT = new Date(c.timestamp).getTime();
      if (newT > oldT) {
        pairsMap[key] = c;
      }
    }
  });
  const finalCollisions = Object.values(pairsMap);

  if (finalCollisions.length === 0) {
    let d2 = document.createElement('div');
    d2.classList.add('collision-item');
    d2.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(d2);
    return;
  }

  // 3) Wyświetlanie
  finalCollisions.forEach(c => {
    let shipA = c.ship1_name || c.mmsi_a;
    let shipB = c.ship2_name || c.mmsi_b;
    let cpaStr = c.cpa.toFixed(2);
    let tcpaStr= c.tcpa.toFixed(2);

    // Pozyskujemy splitted circle z common.js
    // Wykorzystujemy getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, shipMarkers)
    const splittedHTML = getCollisionSplitCircle(
      c.mmsi_a,
      c.mmsi_b,
      c.ship_length_a,
      c.ship_length_b,
      shipMarkers // dzięki temu możemy sprawdzić aktualne length
    );

    let timeStr = '';
    if (c.timestamp) {
      let dt = new Date(c.timestamp);
      timeStr = dt.toLocaleTimeString('pl-PL', { hour12:false });
    }

    const item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedHTML}
          <strong>${shipA} – ${shipB}</strong><br>
          CPA: ${cpaStr} nm, TCPA: ${tcpaStr} min
          ${timeStr ? '@ ' + timeStr : ''}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    collisionList.appendChild(item);

    // Klik -> zoom
    item.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(c);
    });

    // Marker kolizyjny
    let latC = (c.latitude_a + c.latitude_b)/2;
    let lonC = (c.longitude_a + c.longitude_b)/2;

    const collisionIcon = L.divIcon({
      className:'',
      html: `
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <path d="M0,-7 7,7 -7,7 Z"
                fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="4" text-anchor="middle"
                font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize:[24,24],
      iconAnchor:[12,12]
    });

    const tip = `Możliwa kolizja: ${shipA} & ${shipB}, ~${tcpaStr} min`;
    const marker = L.marker([latC, lonC], { icon: collisionIcon })
      .bindTooltip(tip, { direction:'top', sticky:true })
      .on('click', () => zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

// ------------------ zoomToCollision ------------------
function zoomToCollision(c) {
  let bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);

  // 1) Mniejszy padding i/lub maxZoom
  map.fitBounds(bounds, {
    padding: [15, 15],
    maxZoom: 13  // przykładowo
  });

  // 2) Jeśli c.tcpa > 0, ustawiamy vectorLength
  if (c.tcpa && c.tcpa > 0 && c.tcpa < 9999) {
    vectorLength = Math.round(c.tcpa);
    // wywołujemy update, aby wektory się zaktualizowały
    updateSelectedShipsInfo(true);
  }

  // 3) Reszta bez zmian
  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

// ------------------ Zaznaczanie statków ------------------
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
  for (let m in overlayMarkers) {
    overlayMarkers[m].forEach(ln => map.removeLayer(ln));
  }
  overlayMarkers = {};
  updateSelectedShipsInfo(false);
}

// ------------------ Panel "Selected Ships" ------------------
function updateSelectedShipsInfo(selectionChanged) {
  const panel = document.getElementById('selected-ships-info');
  panel.innerHTML = '';
  document.getElementById('pair-info').innerHTML = '';

  if (selectedShips.length === 0) {
    reloadAllShipIcons();
    return;
  }

  // Zbieramy dane statków
  let sData = [];
  selectedShips.forEach(mmsi => {
    if (shipMarkers[mmsi]?.shipData) {
      sData.push(shipMarkers[mmsi].shipData);
    }
  });

  // Prezentacja w panelu
  sData.forEach(sd => {
    const div = document.createElement('div');
    div.innerHTML = `
      <b>${sd.ship_name || 'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog||0} kn, COG: ${sd.cog||0}°<br>
      Len: ${sd.ship_length||'N/A'}
    `;
    panel.appendChild(div);
  });

  // Wektory prędkości (usuwamy stare)
  for (let m in overlayMarkers) {
    overlayMarkers[m].forEach(ln => map.removeLayer(ln));
  }
  overlayMarkers = {};

  selectedShips.forEach(m => drawVector(m));
  reloadAllShipIcons();

  // Jeżeli zaznaczone 2 – pobieramy cpa/tcpa
  if (selectedShips.length === 2) {
    const sorted = [...selectedShips].sort((a,b)=>a-b);
    const url = `/calculate_cpa_tcpa?mmsi_a=${sorted[0]}&mmsi_b=${sorted[1]}`;
    fetch(url)
      .then(r => r.json())
      .then(data => {
        if (data.error) {
          document.getElementById('pair-info').innerHTML =
            `<b>CPA/TCPA:</b> N/A (${data.error})`;
        } else {
          let cpaVal = (data.cpa >= 9999) ? 'n/a' : data.cpa.toFixed(2);
          let tcpaVal= (data.tcpa < 0) ? 'n/a' : data.tcpa.toFixed(2);
          document.getElementById('pair-info').innerHTML = `
            <b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min
          `;
        }
      })
      .catch(err => {
        console.error("Błąd /calculate_cpa_tcpa:", err);
        document.getElementById('pair-info').innerHTML =
          '<b>CPA/TCPA:</b> N/A';
      });
  }
}

// ------------------ drawVector: wektor prędkości ------------------
function drawVector(mmsi) {
  let mk = shipMarkers[mmsi];
  if (!mk?.shipData) return;
  let sd = mk.shipData;
  if (!sd.sog || !sd.cog) return;

  let lat = sd.latitude;
  let lon = sd.longitude;
  let sog = sd.sog;  // nm/h
  let cogDeg = sd.cog;

  let distNm = sog * (vectorLength / 60);
  let cogRad = (cogDeg * Math.PI)/180;
  let dLat = (distNm / 60) * Math.cos(cogRad);
  let dLon = (distNm / 60) * Math.sin(cogRad) / Math.cos(lat*Math.PI/180);

  let endLat = lat + dLat;
  let endLon = lon + dLon;

  let line = L.polyline([[lat, lon],[endLat, endLon]], {
    color:'blue',
    dashArray:'4,4'
  });
  line.addTo(map);
  if (!overlayMarkers[mmsi]) overlayMarkers[mmsi] = [];
  overlayMarkers[mmsi].push(line);
}

// ------------------ reloadAllShipIcons ------------------
function reloadAllShipIcons() {
  for (let m in shipMarkers) {
    let mk = shipMarkers[m];
    if (!mk.shipData) continue;
    let isSelected = selectedShips.includes(parseInt(m));
    let newIcon = createShipIcon(mk.shipData, isSelected);
    mk.setIcon(newIcon);
  }
}