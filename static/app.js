// ==========================
// app.js (moduł LIVE)
// ==========================

document.addEventListener('DOMContentLoaded', initLiveApp);

let map;
let markerClusterGroup;
let shipMarkers = {};       // klucz: mmsi -> L.marker
let overlayMarkers = {};    // klucz: mmsi -> [list of polylines]
let selectedShips = [];

// Tablica kolizji i kolizyjnych markerów
let collisionsData = [];
let collisionMarkers = [];

// Parametry z suwaków
let vectorLength = 15;    // w minutach
let cpaFilter = 0.5;      // [0..0.5]
let tcpaFilter = 10;      // [1..10]

// Interval (odświeżanie)
let shipsInterval = null;
let collisionsInterval = null;

/**
 * Funkcja inicjalizująca moduł LIVE (wywoływane po DOMContentLoaded).
 */
function initLiveApp() {
  // 1) Inicjalizujemy mapę z common.js
  map = initSharedMap('map');

  // 2) Warstwa klastrująca
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // 3) Obsługa UI
  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    // Odświeżamy wektory i ewentualnie CPA/TCPA dla wybranych statków
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

  // 5) Odświeżaj co 60s (lub inny interwał)
  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

// ----------------------------------
// 1) Pobranie i aktualizacja statków
// ----------------------------------
function fetchShips() {
  fetch('/ships')
    .then(res => res.json())
    .then(data => updateShips(data))
    .catch(err => console.error("Błąd /ships:", err));
}

function updateShips(shipsArray) {
  // Zbiór MMSI, które mamy w nowym fetchu
  const currentSet = new Set(shipsArray.map(s => s.mmsi));

  // Usunięcie starych statków
  for (let mmsi in shipMarkers) {
    if (!currentSet.has(parseInt(mmsi))) {
      // Usuwamy marker i ewentualne linie wektorów
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      if (overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(ln => map.removeLayer(ln));
        delete overlayMarkers[mmsi];
      }
    }
  }

  // Dodanie / aktualizacja statków z bieżącego pobrania
  shipsArray.forEach(ship => {
    const mmsi = ship.mmsi;
    const isSelected = selectedShips.includes(mmsi);

    // Ikona z common.js
    const icon = createShipIcon(ship, isSelected);

    // Tekst do tooltipa
    const now = Date.now();
    const updTs = ship.timestamp ? new Date(ship.timestamp).getTime() : 0;
    const diffSec = Math.floor((now - updTs) / 1000);
    const mm = Math.floor(diffSec / 60);
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
      marker.bindTooltip(ttHTML, { direction: 'top', sticky: true });
      marker.shipData = ship;

      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      // Aktualizacja istniejącego
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(ttHTML);
      marker.shipData = ship;
    }
  });

  // Odświeżamy ewentualne zaznaczenia i wektory
  updateSelectedShipsInfo(false);
}

// ----------------------------------
// 2) Pobranie i aktualizacja kolizji
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
 *  - Odfiltrowujemy kolizje nieaktualne (tcpa < 0, itp.).
 *  - Dla każdej pary statków bierzemy najświeższy wiersz (max timestamp).
 *  - Wyświetlamy w panelu + dodajemy markery na mapie.
 *  - Dodajemy splitted circle (z common.js) na liście kolizji.
 */
function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  // Usuwamy stare markery kolizyjne z mapy
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if (!collisionsData || collisionsData.length === 0) {
    const noDiv = document.createElement('div');
    noDiv.classList.add('collision-item');
    noDiv.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(noDiv);
    return;
  }

  const nowMs = Date.now();

  // 1) Odfiltrowanie kolizji, które już się skończyły
  //    (tcpa <0 lub timestamp + tcpa*60k < now).
  let filtered = collisionsData.filter(c => {
    if (c.tcpa < 0) return false;  // statki się rozminęły
    if (c.timestamp) {
      let collTime = new Date(c.timestamp).getTime();
      let futureMs = collTime + (c.tcpa * 60000); 
      if (futureMs < nowMs) {
        // kolizja powinna nastąpić w przeszłości => pomijamy
        return false;
      }
    }
    return true;
  });

  if (filtered.length === 0) {
    const d = document.createElement('div');
    d.classList.add('collision-item');
    d.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(d);
    return;
  }

  // 2) Grupowanie wg pary
  let pairMap = {};
  filtered.forEach(c => {
    let a = Math.min(c.mmsi_a, c.mmsi_b);
    let b = Math.max(c.mmsi_a, c.mmsi_b);
    let key = `${a}_${b}`;
    if (!pairMap[key]) {
      pairMap[key] = c;
    } else {
      // Wybieramy nowszy (większy timestamp)
      let oldT = new Date(pairMap[key].timestamp).getTime();
      let newT = new Date(c.timestamp).getTime();
      if (newT > oldT) {
        pairMap[key] = c;
      }
    }
  });
  const finalCollisions = Object.values(pairMap);

  if (finalCollisions.length === 0) {
    const d2 = document.createElement('div');
    d2.classList.add('collision-item');
    d2.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(d2);
    return;
  }

  // 3) Wyświetlenie w panelu + markery
  finalCollisions.forEach(c => {
    let shipA = c.ship1_name || c.mmsi_a;
    let shipB = c.ship2_name || c.mmsi_b;
    let cpaStr = c.cpa.toFixed(2);
    let tcpaStr= c.tcpa.toFixed(2);

    // Spróbujmy zapewnić, że mamy liczby
    const la = (typeof c.ship_length_a === 'number')
                 ? c.ship_length_a
                 : parseFloat(c.ship_length_a) || 0;
    const lb = (typeof c.ship_length_b === 'number')
                 ? c.ship_length_b
                 : parseFloat(c.ship_length_b) || 0;

    // Kolory z getShipColor
    const colorA = getShipColor(la);
    const colorB = getShipColor(lb);
    // splitted circle
    const splittedCircle = createSplittedCircle(colorA, colorB);

    let timeStr = '';
    if (c.timestamp) {
      let d = new Date(c.timestamp);
      // Godzina wg pl-PL, 24h
      timeStr = d.toLocaleTimeString('pl-PL', { hour12:false });
    }

    const item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedCircle}
          <strong>${shipA} – ${shipB}</strong><br>
          CPA: ${cpaStr} nm, TCPA: ${tcpaStr} min
          ${ timeStr ? '@ ' + timeStr : '' }
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    collisionList.appendChild(item);

    // Zoom
    item.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(c);
    });

    // Marker kolizji
    let latC = (c.latitude_a + c.latitude_b) / 2;
    let lonC = (c.longitude_a + c.longitude_b) / 2;

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

    const tip = `Możliwa kolizja: ${shipA} & ${shipB}, w ok. ${tcpaStr} min`;
    const marker = L.marker([latC, lonC], { icon: collisionIcon })
      .bindTooltip(tip, { direction:'top', sticky:true })
      .on('click', () => zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

// ------------------ Zoom do kolizji ------------------
function zoomToCollision(c) {
  let bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, { padding: [20,20] });
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

// ------------------ Panel “Selected Ships” ------------------
function updateSelectedShipsInfo(selectionChanged) {
  const panel = document.getElementById('selected-ships-info');
  panel.innerHTML = '';
  document.getElementById('pair-info').innerHTML = '';

  if (selectedShips.length === 0) {
    reloadAllShipIcons();
    return;
  }

  // Zbieramy shipData
  let sData = [];
  selectedShips.forEach(mmsi => {
    if (shipMarkers[mmsi]?.shipData) {
      sData.push(shipMarkers[mmsi].shipData);
    }
  });

  // Wyświetlamy info
  sData.forEach(sd => {
    let div = document.createElement('div');
    div.innerHTML = `
      <b>${sd.ship_name || 'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog || 0} kn, COG: ${sd.cog || 0}°<br>
      Len: ${sd.ship_length || 'N/A'}
    `;
    panel.appendChild(div);
  });

  // Wektory
  for (let m in overlayMarkers) {
    overlayMarkers[m].forEach(ln => map.removeLayer(ln));
  }
  overlayMarkers = {};

  selectedShips.forEach(m => drawVector(m));
  reloadAllShipIcons();

  // Gdy 2 statki – pobieramy cpa/tcpa
  if (selectedShips.length === 2) {
    let sorted = [...selectedShips].sort((a,b) => a-b);
    let url = `/calculate_cpa_tcpa?mmsi_a=${sorted[0]}&mmsi_b=${sorted[1]}`;
    fetch(url)
      .then(r => r.json())
      .then(data => {
        if (data.error) {
          document.getElementById('pair-info').innerHTML =
            `<b>CPA/TCPA:</b> N/A (${data.error})`;
        } else {
          let cpaVal = (data.cpa >= 9999) ? 'n/a' : data.cpa.toFixed(2);
          let tcpaVal = (data.tcpa < 0) ? 'n/a' : data.tcpa.toFixed(2);
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

// ------------------ Rysowanie wektora prędkości ------------------
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
  let cogRad = (cogDeg * Math.PI) / 180;
  let dLat = (distNm / 60) * Math.cos(cogRad);
  let dLon = (distNm / 60) * Math.sin(cogRad) / Math.cos(lat * Math.PI / 180);

  let endLat = lat + dLat;
  let endLon = lon + dLon;

  let line = L.polyline([[lat, lon], [endLat, endLon]], {
    color: 'blue',
    dashArray: '4,4'
  });
  line.addTo(map);
  if (!overlayMarkers[mmsi]) overlayMarkers[mmsi] = [];
  overlayMarkers[mmsi].push(line);
}

// ------------------ Odświeżenie ikon statków ------------------
function reloadAllShipIcons() {
  for (let m in shipMarkers) {
    let mk = shipMarkers[m];
    if (!mk.shipData) continue;
    let isSelected = selectedShips.includes(parseInt(m));
    let newIcon = createShipIcon(mk.shipData, isSelected);
    mk.setIcon(newIcon);
  }
}