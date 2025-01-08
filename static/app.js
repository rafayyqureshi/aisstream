//
// app.js – Moduł LIVE (z poprawkami)
//

let map;                        // obiekt Leaflet mapy
let markerClusterGroup;         // grupa klastrująca statki
let shipMarkers = {};           // { mmsi: L.marker }
let overlayMarkers = {};        // { mmsi: [L.polyline, ...] } – wektory
let selectedShips = [];

let collisionsData = [];        // dane o kolizjach z /collisions
let collisionMarkers = [];      // markery kolizji na mapie

let vectorLength = 15;          // minuty do przodu (wektor statku)
let cpaFilter = 0.5;            // suwak – max CPA
let tcpaFilter = 10;            // suwak – max TCPA

let shipsInterval = null;
let collisionsInterval = null;

/**
 * Inicjalizacja aplikacji LIVE.
 * Wywoływana np. po załadowaniu DOM (DOMContentLoaded).
 */
function initLiveApp() {
  // 1) Tworzymy mapę przez initSharedMap z common.js
  map = initSharedMap('map'); // zakładam, że w HTML jest <div id="map"></div>

  // 2) Dodajemy MarkerClusterGroup
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // 3) Obsługa suwaków / buttonów
  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(true); // odśwież wektory i ewentualne cpa/tcpa
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

  // 4) Pierwsze pobranie statków/kolizji
  fetchShips();
  fetchCollisions();

  // 5) Ustaw intervale (np. co minutę)
  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

// --------------------------------------
// 1) Funkcje pobierające STATKI
// --------------------------------------
function fetchShips() {
  fetch('/ships')
    .then(r => r.json())
    .then(data => updateShips(data))
    .catch(err => console.error("Błąd /ships:", err));
}

function updateShips(shipsArray) {
  // mmsi bieżących statków
  const currentMmsiSet = new Set(shipsArray.map(s => s.mmsi));

  // Usunięcie tych, których już nie ma
  for (let mmsi in shipMarkers) {
    if (!currentMmsiSet.has(parseInt(mmsi))) {
      // usuń z markerClusterGroup
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      // usuń wektory
      if (overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(line => map.removeLayer(line));
        delete overlayMarkers[mmsi];
      }
    }
  }

  // Dodaj/aktualizuj
  shipsArray.forEach(ship => {
    let mmsi = ship.mmsi;
    let marker = shipMarkers[mmsi];
    let isSelected = selectedShips.includes(mmsi);

    // Tworzymy ikonę statku przez createShipIcon z common.js
    let icon = createShipIcon(ship, isSelected);

    // tooltip
    const now = Date.now();
    const updAt = new Date(ship.timestamp).getTime();
    const diffSec = Math.floor((now - updAt)/1000);
    const diffMin = Math.floor(diffSec/60);
    const diffS = diffSec % 60;
    const diffStr = `${diffMin}m ${diffS}s ago`;

    let tooltipHTML = `
      <b>${ship.ship_name||'Unknown'}</b><br>
      MMSI: ${mmsi}<br>
      SOG: ${ship.sog||0} kn, COG: ${ship.cog||0}°<br>
      Length: ${ship.ship_length||'N/A'}<br>
      Updated: ${diffStr}
    `;

    if (!marker) {
      // Nowy marker
      marker = L.marker([ship.latitude, ship.longitude], { icon })
        .on('click', () => selectShip(mmsi));
      marker.bindTooltip(tooltipHTML, { direction:'top', sticky:true });
      marker.shipData = ship;

      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      // Aktualizacja
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(tooltipHTML);
      marker.shipData = ship;
    }
  });

  // Odśwież info
  updateSelectedShipsInfo(false);
}

// --------------------------------------
// 2) Funkcje pobierające i wyświetlające KOLIZJE
// --------------------------------------
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(r => r.json())
    .then(data => {
      collisionsData = data || [];
      updateCollisionsList();
    })
    .catch(err => console.error("Błąd /collisions:", err));
}

function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  // Usuwamy poprzednie markery kolizji
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if (!collisionsData || collisionsData.length===0) {
    let div = document.createElement('div');
    div.classList.add('collision-item');
    div.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(div);
    return;
  }

  // Filtrowanie kolizji, by NIE pokazywać już rozwiązanych:
  // (tcpa < 0 => statki się rozminęły; cpa > 0.5 => zbyt duży dystans)
  // Ewentualnie można też pominąć te, których timestamp < now (już w przeszłości).
  let nowTime = Date.now();
  let filtered = collisionsData.filter(c => {
    if (c.tcpa < 0) return false;
    if (c.cpa > 0.5) return false;
    // jeśli chcesz pominąć te, które w momencie c.timestamp < aktualny czas
    // (co oznacza, że kolizja już wystąpiła?)
    // let collTime = new Date(c.timestamp).getTime();
    // if (collTime < nowTime) return false;
    return true;
  });

  if (filtered.length===0) {
    let nodiv = document.createElement('div');
    nodiv.classList.add('collision-item');
    nodiv.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(nodiv);
    return;
  }

  // Usunięcie duplikatów (A-B vs B-A)
  let colMap = {};
  filtered.forEach(c => {
    let a = Math.min(c.mmsi_a, c.mmsi_b);
    let b = Math.max(c.mmsi_a, c.mmsi_b);
    let key = `${a}_${b}`;

    if (!colMap[key]) {
      colMap[key] = c;
    } else {
      // Wybierz np. minimalne cpa lub najnowszy timestamp
      if (c.cpa < colMap[key].cpa) {
        colMap[key] = c;
      } else if (c.cpa===colMap[key].cpa) {
        let oldT = new Date(colMap[key].timestamp).getTime();
        let newT = new Date(c.timestamp).getTime();
        if (newT>oldT) {
          colMap[key] = c;
        }
      }
    }
  });

  let finalCollisions = Object.values(colMap);
  if (finalCollisions.length===0) {
    let noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML='<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(noItem);
    return;
  }

  // Wyświetlmy finalCollisions
  finalCollisions.forEach(c => {
    let shipA = c.ship1_name || c.mmsi_a;
    let shipB = c.ship2_name || c.mmsi_b;
    let cpaVal = c.cpa.toFixed(2);
    let tcpaVal = c.tcpa.toFixed(2);
    let dtStr = '';
    if (c.timestamp) {
      let dt = new Date(c.timestamp);
      dtStr = dt.toLocaleTimeString('en-GB');
    }

    // Budujemy item
    let item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML=`
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <strong>${shipA} – ${shipB}</strong><br>
          CPA: ${cpaVal} nm, TCPA: ${tcpaVal} min
          ${dtStr ? '@ '+dtStr : ''}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    collisionList.appendChild(item);

    // Zoom-button
    item.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(c);
    });

    // Marker na mapie
    const latC = (c.latitude_a + c.latitude_b)/2;
    const lonC = (c.longitude_a + c.longitude_b)/2;
    let markerIcon = L.divIcon({
      className:'',
      html: `
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <path d="M0,-7 7,7 -7,7 Z"
                fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="4" text-anchor="middle" font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize:[24,24],
      iconAnchor:[12,12]
    });
    let tipText = `Możliwa kolizja: ${shipA} & ${shipB}, w ~${tcpaVal} min`;
    let cMarker = L.marker([latC, lonC], { icon: markerIcon })
      .bindTooltip(tipText, { direction:'top', sticky:true })
      .on('click', ()=>zoomToCollision(c));
    cMarker.addTo(map);
    collisionMarkers.push(cMarker);
  });
}

// --------------------------------------
// Zoom do kolizji
// --------------------------------------
function zoomToCollision(c) {
  let bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, { padding:[20,20] });

  // Zaznacz statki
  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

// --------------------------------------
// 3) Zaznaczanie statków
// --------------------------------------
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
    overlayMarkers[m].forEach(line => map.removeLayer(line));
  }
  overlayMarkers = {};
  updateSelectedShipsInfo(false);
}

// --------------------------------------
// 4) Panel Selected Ships
// --------------------------------------
function updateSelectedShipsInfo(selectionChanged) {
  const container = document.getElementById('selected-ships-info');
  container.innerHTML = '';
  document.getElementById('pair-info').innerHTML = '';

  if (selectedShips.length === 0) {
    reloadAllShipIcons();
    return;
  }

  // Dane statków
  let shipsData = [];
  selectedShips.forEach(mmsi => {
    let mk = shipMarkers[mmsi];
    if (mk && mk.shipData) {
      shipsData.push(mk.shipData);
    }
  });

  // Wypisz w panelu
  shipsData.forEach(sd => {
    let div = document.createElement('div');
    div.innerHTML=`
      <b>${sd.ship_name||'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog||0} kn, COG: ${sd.cog||0}°<br>
      Length: ${sd.ship_length||'N/A'}
    `;
    container.appendChild(div);
  });

  // Wyczyszczenie starych wektorów
  for (let m in overlayMarkers) {
    overlayMarkers[m].forEach(line => map.removeLayer(line));
  }
  overlayMarkers = {};

  // Rysowanie nowych
  selectedShips.forEach(m => drawVector(m));
  reloadAllShipIcons();

  // Jeśli 2 statki – oblicz cpa/tcpa
  if (selectedShips.length === 2) {
    let sorted = [...selectedShips].sort((a,b)=>a-b);
    fetch(`/calculate_cpa_tcpa?mmsi_a=${sorted[0]}&mmsi_b=${sorted[1]}`)
      .then(r => r.json())
      .then(data => {
        if (data.error) {
          document.getElementById('pair-info').innerHTML =
            `<b>CPA/TCPA:</b> N/A (${data.error})`;
        } else {
          let cpaVal = (data.cpa>=9999)? 'n/a': data.cpa.toFixed(2);
          let tcpaVal = (data.tcpa<0)? 'n/a': data.tcpa.toFixed(2);
          document.getElementById('pair-info').innerHTML = `
            <b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min
          `;
        }
      })
      .catch(err => {
        console.error("Błąd /calculate_cpa_tcpa:", err);
        document.getElementById('pair-info').innerHTML='<b>CPA/TCPA:</b> N/A';
      });
  }
}

// --------------------------------------
// Rysowanie wektora
// --------------------------------------
function drawVector(mmsi) {
  let mk = shipMarkers[mmsi];
  if (!mk || !mk.shipData) return;
  let sd = mk.shipData;
  if (!sd.sog || !sd.cog) return;

  let lat = sd.latitude;
  let lon = sd.longitude;
  let sog = sd.sog;  
  let cogDeg = sd.cog;

  let distNm = sog * (vectorLength / 60.0);
  let cogRad = (cogDeg * Math.PI)/180;
  let deltaLat = (distNm/60) * Math.cos(cogRad);
  let deltaLon = (distNm/60) * Math.sin(cogRad) / Math.cos(lat*Math.PI/180);

  let endLat = lat + deltaLat;
  let endLon = lon + deltaLon;

  let line = L.polyline([[lat, lon], [endLat, endLon]], {
    color: 'blue',
    dashArray: '4,4'
  });
  line.addTo(map);

  if (!overlayMarkers[mmsi]) overlayMarkers[mmsi] = [];
  overlayMarkers[mmsi].push(line);
}

// --------------------------------------
// Przeładowanie ikon statków
// --------------------------------------
function reloadAllShipIcons() {
  for (let m in shipMarkers) {
    let mk = shipMarkers[m];
    if (!mk.shipData) continue;
    let isSelected = selectedShips.includes(parseInt(m));
    let newIcon = createShipIcon(mk.shipData, isSelected);
    mk.setIcon(newIcon);
  }
}

// --------------------------------------
// Koniec pliku
// --------------------------------------

// Upewnij się, że w HTML jest np. onload="initLiveApp()" 
// lub document.addEventListener('DOMContentLoaded', initLiveApp);