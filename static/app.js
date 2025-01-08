// app.js (LIVE)

document.addEventListener('DOMContentLoaded', initLiveApp);

let map;
let markerClusterGroup;
let shipMarkers = {};       // klucz = mmsi
let overlayMarkers = {};    // klucz = mmsi
let selectedShips = [];

let collisionsData = [];
let collisionMarkers = [];

let vectorLength = 15;      // suwak [1..120]
let cpaFilter = 0.5;        // suwak [0..0.5]
let tcpaFilter = 10;        // suwak [1..10]

let shipsInterval = null;
let collisionsInterval = null;

function initLiveApp() {
  // 1) Inicjalizacja mapy
  map = initSharedMap('map');

  // 2) Dodanie warstwy klastrującej statki
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // 3) Obsługa suwaków / przycisków
  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
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

  // 5) Odświeżaj co 60s
  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

// ------------------ 1) Pobranie / aktualizacja statków ------------------
function fetchShips() {
  fetch('/ships')
    .then(r => r.json())
    .then(data => updateShips(data))
    .catch(err => console.error("Błąd /ships:", err));
}

function updateShips(shipsArray) {
  // Zbiór MMSI, które istnieją w nowym fetchu
  const currentMmsiSet = new Set(shipsArray.map(s => s.mmsi));

  // Usuń statki, które zniknęły
  for (let mmsi in shipMarkers) {
    if (!currentMmsiSet.has(parseInt(mmsi))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      if (overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(poly => map.removeLayer(poly));
        delete overlayMarkers[mmsi];
      }
    }
  }

  // Dodaj / uaktualnij te, które mamy w fetchu
  shipsArray.forEach(ship => {
    const mmsi = ship.mmsi;
    const isSelected = selectedShips.includes(mmsi);

    // Ikona z common.js
    const icon = createShipIcon(ship, isSelected);

    // Tooltip
    const now = Date.now();
    const updTimestamp = (ship.timestamp) ? new Date(ship.timestamp).getTime() : 0;
    const diffSec = Math.floor((now - updTimestamp)/1000);
    const diffMin = Math.floor(diffSec/60);
    const diffS   = diffSec%60;
    const diffStr = `${diffMin}m ${diffS}s ago`;

    const tooltipHTML = `
      <b>${ship.ship_name||'Unknown'}</b><br>
      MMSI: ${mmsi}<br>
      SOG: ${ship.sog||0} kn, COG: ${ship.cog||0}°<br>
      Len: ${ship.ship_length||'N/A'}<br>
      Updated: ${diffStr}
    `;

    let marker = shipMarkers[mmsi];
    if (!marker) {
      // Nowy statek
      marker = L.marker([ship.latitude, ship.longitude], { icon })
        .on('click', () => selectShip(mmsi));
      marker.bindTooltip(tooltipHTML, { direction:'top', sticky:true });
      marker.shipData = ship;

      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      // Aktualizacja istniejącego
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(tooltipHTML);
      marker.shipData = ship;
    }
  });

  updateSelectedShipsInfo(false);
}

// ------------------ 2) Pobranie / aktualizacja kolizji ------------------
function fetchCollisions() {
  // Kolizje z backendu (filtr cpa/tcpa), ale dodatkowo odfiltrowujemy tu również "przeszłe".
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

  // Usuwamy stare markery
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if (!collisionsData || collisionsData.length === 0) {
    const div = document.createElement('div');
    div.classList.add('collision-item');
    div.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(div);
    return;
  }

  // Filtracja "nadchodzących" kolizji. 
  // Warunek: c.tcpa >= 0 => jeszcze nie minęły. 
  // (Opcjonalnie: c.timestamp > now => kolizja jeszcze w przyszłości)
  const nowTime = Date.now();
  let filtered = collisionsData.filter(c => {
    // warunki z /collisions?max_cpa=..., ok; sprawdzamy tcpa >=0
    if (c.tcpa < 0) return false;

    // jeśli c.timestamp jest momentem zbliżenia, to sprawdź, czy to jeszcze przed nami
    // (opcjonalne, zależy od definicji)
    // let collTime = new Date(c.timestamp).getTime();
    // if (collTime <= nowTime) return false;

    return true;
  });

  if (filtered.length === 0) {
    const divNo = document.createElement('div');
    divNo.classList.add('collision-item');
    divNo.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(divNo);
    return;
  }

  // Usunięcie duplikatów A-B i B-A: 
  const pairsMap = {};
  filtered.forEach(c => {
    let a = Math.min(c.mmsi_a, c.mmsi_b);
    let b = Math.max(c.mmsi_a, c.mmsi_b);
    let key = `${a}_${b}`;
    // Wybieramy np. mniejszą cpa
    if(!pairsMap[key]){
      pairsMap[key] = c;
    } else {
      if(c.cpa < pairsMap[key].cpa){
        pairsMap[key] = c;
      }
    }
  });

  const finalCollisions = Object.values(pairsMap);
  if (finalCollisions.length === 0) {
    const divN = document.createElement('div');
    divN.classList.add('collision-item');
    divN.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(divN);
    return;
  }

  // Rysowanie listy i markerów
  finalCollisions.forEach(c => {
    let aName = c.ship1_name || c.mmsi_a;
    let bName = c.ship2_name || c.mmsi_b;
    let cpaStr = c.cpa.toFixed(2);
    let tcpaStr = c.tcpa.toFixed(2);

    // Data/godz. kolizji (opcjonalnie)
    let timeStr = '';
    if (c.timestamp) {
      timeStr = new Date(c.timestamp).toLocaleTimeString('pl-PL', { hour12:false });
    }

    const item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <strong>${aName} – ${bName}</strong><br>
          CPA: ${cpaStr} nm, TCPA: ${tcpaStr} min
          ${ timeStr ? '@ '+timeStr : '' }
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    collisionList.appendChild(item);

    item.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(c);
    });

    // Marker na mapie
    const latMid = (c.latitude_a + c.latitude_b)/2;
    const lonMid = (c.longitude_a + c.longitude_b)/2;
    const colIcon = L.divIcon({
      className:'',
      html: `
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <path d="M0,-7 7,7 -7,7 Z"
                fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="4" text-anchor="middle"
                font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize: [24,24],
      iconAnchor: [12,12]
    });
    const tooltipTxt = `Możliwa kolizja: ${aName} & ${bName}, w ~${tcpaStr} min`;
    let colMarker = L.marker([latMid, lonMid], { icon: colIcon })
      .bindTooltip(tooltipTxt, { direction:'top', sticky:true })
      .on('click', () => zoomToCollision(c));
    colMarker.addTo(map);
    collisionMarkers.push(colMarker);
  });
}

// ------------------ Zoom do kolizji ------------------
function zoomToCollision(c) {
  let bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, { padding:[20,20] });
  clearSelectedShips(); // reset zaznaczeń
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

// ------------------ Zaznaczanie statków ------------------
function selectShip(mmsi) {
  if(!selectedShips.includes(mmsi)) {
    // max 2
    if(selectedShips.length >= 2) {
      selectedShips.shift();
    }
    selectedShips.push(mmsi);
    updateSelectedShipsInfo(true);
  }
}
function clearSelectedShips() {
  selectedShips = [];
  for(let m in overlayMarkers) {
    overlayMarkers[m].forEach(ln => map.removeLayer(ln));
  }
  overlayMarkers = {};
  updateSelectedShipsInfo(false);
}

// ------------------ Panel “Selected Ships” ------------------
function updateSelectedShipsInfo(selectionChanged) {
  const container = document.getElementById('selected-ships-info');
  container.innerHTML = '';
  document.getElementById('pair-info').innerHTML = '';

  if(selectedShips.length === 0) {
    reloadAllShipIcons();
    return;
  }

  // Zbieramy dane
  let shipsData = [];
  selectedShips.forEach(mmsi => {
    if(shipMarkers[mmsi]?.shipData) {
      shipsData.push(shipMarkers[mmsi].shipData);
    }
  });

  // Wyświetlamy
  shipsData.forEach(sd => {
    const div = document.createElement('div');
    div.innerHTML = `
      <b>${sd.ship_name || 'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog||0} kn, COG: ${sd.cog||0}°<br>
      Len: ${sd.ship_length||'N/A'}
    `;
    container.appendChild(div);
  });

  // Usuwamy wektory i rysujemy na nowo
  for(let m in overlayMarkers) {
    overlayMarkers[m].forEach(ln => map.removeLayer(ln));
  }
  overlayMarkers = {};

  selectedShips.forEach(m => drawVector(m));
  reloadAllShipIcons();

  if(selectedShips.length === 2) {
    let sorted = [...selectedShips].sort((a,b)=>a-b);
    const url = `/calculate_cpa_tcpa?mmsi_a=${sorted[0]}&mmsi_b=${sorted[1]}`;
    fetch(url)
      .then(r => r.json())
      .then(data => {
        if(data.error) {
          document.getElementById('pair-info').innerHTML = 
            `<b>CPA/TCPA:</b> N/A (${data.error})`;
        } else {
          let cpaVal = (data.cpa>=9999) ? 'n/a' : data.cpa.toFixed(2);
          let tcpaVal= (data.tcpa<0) ? 'n/a' : data.tcpa.toFixed(2);
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

// ------------------ Rysowanie wektorów ------------------
function drawVector(mmsi) {
  let mk = shipMarkers[mmsi];
  if(!mk || !mk.shipData) return;
  let sd = mk.shipData;
  if(!sd.sog||!sd.cog) return;

  let lat = sd.latitude;
  let lon = sd.longitude;
  let sog = sd.sog;
  let cogDeg = sd.cog;

  let distanceNm = sog*(vectorLength/60);
  let cogRad = (cogDeg*Math.PI)/180;
  let deltaLat = (distanceNm/60)*Math.cos(cogRad);
  let deltaLon = (distanceNm/60)*Math.sin(cogRad)/Math.cos(lat*Math.PI/180);

  let endLat = lat+deltaLat;
  let endLon = lon+deltaLon;

  let line = L.polyline([[lat,lon],[endLat,endLon]],
                        { color:'blue', dashArray:'4,4' });
  line.addTo(map);

  if(!overlayMarkers[mmsi]) overlayMarkers[mmsi] = [];
  overlayMarkers[mmsi].push(line);
}

// ------------------ Przeładowanie ikon statków ------------------
function reloadAllShipIcons() {
  for(let m in shipMarkers) {
    let mk = shipMarkers[m];
    if(!mk.shipData) continue;
    let isSelected = selectedShips.includes(parseInt(m));
    let newIcon = createShipIcon(mk.shipData, isSelected);
    mk.setIcon(newIcon);
  }
}