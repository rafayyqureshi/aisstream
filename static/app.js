let map;
let shipMarkers = {};
let collisionMarkers = L.markerClusterGroup({ maxClusterRadius: 40 }); // Klasteruj kolizje
let shipsData = [];
let collisionsData = [];
let selectedShips = []; // Tablica dla 2 wybranych statków
let vectorLayer;
let vectorTime = 7; // domyślny czas wektora w minutach

// Filtry kolizji
let cpaFilter = 0.5; // domyślnie maks. 0.5 Nm
let tcpaFilter = 15; // domyślnie maks. 15 min
let cpaMinFilter = 0.2; // minimalne CPA do filtra
let tcpaMinFilter = 1;  // minimalne tcpa

function initMap() {
  map = L.map('map', {
    center: [50.5, 1.0],
    zoom: 9
  });

  // OSM Layer
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
  }).addTo(map);

  // OpenSeaMap Overlay
  L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png', {
    maxZoom: 18,
    opacity: 0.7
  }).addTo(map);

  vectorLayer = L.layerGroup().addTo(map);
  map.addLayer(collisionMarkers);

  fetchAndUpdateData();

  setInterval(fetchAndUpdateData, 5000); // odśwież co 5s

  createUI();
}

function createUI() {
  // Panel lewy
  const leftPanel = L.control({ position: 'topleft' });
  leftPanel.onAdd = function (map) {
    let div = L.DomUtil.create('div', 'info');
    div.innerHTML = `
      <div>
        <h3>Vector time: <span id="vectorTimeDisplay">${vectorTime} min</span></h3>
        <input type="range" min="1" max="120" value="${vectorTime}" id="vectorSlider">
        <button id="clearSelection">Clear</button>
        <div id="selectedPair"></div>
      </div>
    `;
    L.DomEvent.disableClickPropagation(div);
    return div;
  };
  leftPanel.addTo(map);

  document.getElementById('vectorSlider').addEventListener('input', (e) => {
    vectorTime = parseInt(e.target.value);
    document.getElementById('vectorTimeDisplay').innerText = vectorTime + ' min';
    updateSelectedVectors();
  });

  document.getElementById('clearSelection').addEventListener('click', () => {
    selectedShips = [];
    updateSelectedVectors();
  });

  // Panel prawy
  const rightPanel = L.control({ position: 'topright' });
  rightPanel.onAdd = function (map) {
    let div = L.DomUtil.create('div', 'info');
    div.style.width = '300px';
    div.style.background = 'white';
    div.style.padding = '10px';

    div.innerHTML = `
      <h2>Collisions</h2>
      <p>CPA filter: <span id="cpaDisplay">${cpaFilter}</span>Nm<br>
      <input type="range" min="0.2" max="0.5" step="0.01" value="${cpaFilter}" id="cpaSlider"></p>
      <p>TCPA filter: <span id="tcpaDisplay">${tcpaFilter}</span>min<br>
      <input type="range" min="1" max="15" step="1" value="${tcpaFilter}" id="tcpaSlider"></p>
      <div id="collision-list"></div>
    `;
    L.DomEvent.disableClickPropagation(div);
    return div;
  };
  rightPanel.addTo(map);

  document.getElementById('cpaSlider').addEventListener('input', (e) => {
    cpaFilter = parseFloat(e.target.value);
    document.getElementById('cpaDisplay').innerText = cpaFilter;
    updateCollisionList();
  });

  document.getElementById('tcpaSlider').addEventListener('input', (e) => {
    tcpaFilter = parseInt(e.target.value);
    document.getElementById('tcpaDisplay').innerText = tcpaFilter;
    updateCollisionList();
  });
}

async function fetchAndUpdateData() {
  try {
    let shipsRes = await fetch('/ships');
    if (!shipsRes.ok) throw new Error('Error ships: ' + shipsRes.statusText);
    let shipsJson = await shipsRes.json();
    shipsData = shipsJson;

    let collisionsRes = await fetch('/collisions');
    if (!collisionsRes.ok) throw new Error('Error collisions: ' + collisionsRes.statusText);
    let collisionsJson = await collisionsRes.json();
    collisionsData = collisionsJson;

    updateShips();
    updateCollisions();

  } catch (err) {
    console.error(err);
  }
}

function updateShips() {
  // usuń stare markery
  for (let m in shipMarkers) {
    map.removeLayer(shipMarkers[m]);
  }
  shipMarkers = {};

  shipsData.forEach((ship) => {
    let lat = ship.latitude;
    let lon = ship.longitude;
    let cog = ship.cog || 0;
    let name = ship.ship_name || ship.mmsi;
    let length = ship.ship_length;

    // Wyznaczenie stylu wg długości statku:
    let fillColor = 'none';
    let scale = 1;
    if (!length) {
      fillColor = 'none';
      scale = 1;
    } else if (length < 50) {
      fillColor = 'green';
      scale = 0.8;
    } else if (length < 150) {
      fillColor = 'yellow';
      scale = 1.2;
    } else if (length < 250) {
      fillColor = 'orange';
      scale = 1.4;
    } else {
      fillColor = 'red';
      scale = 1.6;
    }

    let shipIcon = L.divIcon({
      className: '',
      html: `<svg width="${20 * scale}" height="${20 * scale}" viewBox="-10 -10 20 20" style="transform: rotate(${cog}deg);">
        <polygon points="0,-10 5,10 0,5 -5,10" fill="${fillColor}" stroke="blue" stroke-width="1" />
      </svg>`,
      iconAnchor: [0, 0]
    });

    let marker = L.marker([lat, lon], { icon: shipIcon });
    marker.addTo(map);

    let lastUpdate = new Date(ship.timestamp);
    let now = new Date();
    let diffSec = Math.floor((now - lastUpdate)/1000);

    let tip = `
      <strong>${name}</strong><br>
      MMSI: ${ship.mmsi}<br>
      SOG: ${ship.sog || 'N/A'}<br>
      COG: ${ship.cog || 'N/A'}<br>
      Length: ${ship.ship_length || 'N/A'}<br>
      Last update: ${diffSec} seconds ago
    `;
    marker.bindTooltip(tip);

    marker.on('click', () => {
      selectShip(ship);
    });

    shipMarkers[ship.mmsi] = marker;
  });

  updateSelectedVectors();
}

function updateCollisions() {
  updateCollisionList();
}

function updateCollisionList() {
  let div = document.getElementById('collision-list');
  div.innerHTML = '';

  // Filtruj kolizje wg suwaków
  let filtered = collisionsData.filter(c => c.cpa <= cpaFilter && c.cpa >= cpaMinFilter && c.tcpa <= tcpaFilter && c.tcpa >= tcpaMinFilter);

  // Sortuj wg cpa
  filtered.sort((a,b) => a.cpa - b.cpa);

  filtered.forEach(col => {
    let shipA = col.ship1_name || col.ship1_mmsi;
    let shipB = col.ship2_name || col.ship2_mmsi;
    let item = document.createElement('div');
    item.style.borderBottom = '1px solid #ccc';
    item.style.padding = '5px';
    let text = `
      <strong>${shipA}</strong> - <strong>${shipB}</strong><br>
      CPA: ${col.cpa.toFixed(2)} nm, TCPA: ${col.tcpa.toFixed(0)} min
    `;
    item.innerHTML = text;
    div.appendChild(item);

    // Dodaj marker kolizji
    // symbol kolizji np. czerwone kółko z liczbą kolizji w cluster
    // Tutaj bez liczb, bo cluster plugin je pokaże
    let lat = (col.latitude_a + col.latitude_b)/2;
    let lon = (col.longitude_a + col.longitude_b)/2;

    let collisionIcon = L.divIcon({
      html: '<svg width="20" height="20"><circle cx="10" cy="10" r="8" fill="red" /></svg>',
      iconAnchor: [10,10]
    });

    let colMarker = L.marker([lat,lon], { icon: collisionIcon });
    collisionMarkers.addLayer(colMarker);
  });
}

function selectShip(ship) {
  // Dodaj statek do selectedShips, max 2
  let found = selectedShips.find(s => s.mmsi === ship.mmsi);
  if (found) {
    // Odznacz statek
    selectedShips = selectedShips.filter(s => s.mmsi !== ship.mmsi);
  } else {
    if (selectedShips.length < 2) {
      selectedShips.push(ship);
    } else {
      // Jeżeli mamy już 2, to najpierw odznacz pierwszego
      selectedShips.shift();
      selectedShips.push(ship);
    }
  }
  updateSelectedVectors();
}

function updateSelectedVectors() {
  vectorLayer.clearLayers();

  // Wyświetl kwadrat zaznaczenia i wektory dla zaznaczonych statków
  selectedShips.forEach(ship => {
    let lat = ship.latitude;
    let lon = ship.longitude;

    // Kwadrat zaznaczenia jako icon
    let scale = 1;
    let length = ship.ship_length;
    if (!length) scale = 1;
    else if (length < 50) scale = 0.8;
    else if (length < 150) scale = 1.2;
    else if (length < 250) scale = 1.4;
    else scale = 1.6;

    let size = 30 * scale; // kwadrat nieco większy niż statek
    let selectionIcon = L.divIcon({
      html: `<svg width="${size}" height="${size}" viewBox="0 0 20 20">
        <rect x="1" y="1" width="18" height="18" fill="none" stroke="orange" stroke-dasharray="4" stroke-width="2"/>
      </svg>`,
      iconAnchor: [size/2, size/2]
    });

    let selectionMarker = L.marker([lat, lon], { icon: selectionIcon });
    vectorLayer.addLayer(selectionMarker);

    // wektor ruchu
    let sog_ms = ship.sog * 0.51444; // knots to m/s
    let distance = sog_ms * 60 * vectorTime / 1852; // w nm jest sog, w knots 1 knot = 1 nm/h
    // w sumie wystarczy sam kierunek i odległość w nm; 1 nm ~1852m
    // Przesuniecie konca wektora
    let bearing = ship.cog || 0;
    let R = 6371e3; // Ziemia w metrach
    let dist_m = distance * 1852; // nm to m

    let latRad = lat * Math.PI/180;
    let lonRad = lon * Math.PI/180;
    let brgRad = bearing * Math.PI/180;

    let lat2 = Math.asin( Math.sin(latRad)*Math.cos(dist_m/R) +
      Math.cos(latRad)*Math.sin(dist_m/R)*Math.cos(brgRad));
    let lon2 = lonRad + Math.atan2(Math.sin(brgRad)*Math.sin(dist_m/R)*Math.cos(latRad),
                                   Math.cos(dist_m/R)-Math.sin(latRad)*Math.sin(lat2));

    lat2 = lat2*180/Math.PI;
    lon2 = lon2*180/Math.PI;

    let poly = L.polyline([[lat, lon], [lat2, lon2]], {color:'blue',dashArray:'',weight:2});
    vectorLayer.addLayer(poly);
  });

  // Wyświetl informacje o parze statków wybranych
  let selectedDiv = document.getElementById('selectedPair');
  selectedDiv.innerHTML = '';
  if (selectedShips.length === 2) {
    let A = selectedShips[0];
    let B = selectedShips[1];
    // Oblicz CPA/TCPA lokalnie jeśli potrzeba lub już obliczone
    // Zakładamy że obliczenia na backendzie, tutaj tylko wyświetlamy
    // Możemy wykorzystać collisionsData i znaleźć pasującą parę
    let pairCollision = collisionsData.find(c =>
      (c.mmsi_a === A.mmsi && c.mmsi_b === B.mmsi) || 
      (c.mmsi_b === A.mmsi && c.mmsi_a === B.mmsi)
    );

    if (pairCollision && pairCollision.tcpa > 0) {
      selectedDiv.innerHTML = `
        <p>Ship A: ${A.ship_name || A.mmsi}<br>
        Ship B: ${B.ship_name || B.mmsi}<br>
        CPA:${pairCollision.cpa.toFixed(1)} nm, TCPA:${pairCollision.tcpa.toFixed(0)} min<br>
        Potential collision</p>
      `;
    } else {
      selectedDiv.innerHTML = `
        <p>Ship A: ${A.ship_name || A.mmsi}<br>
        Ship B: ${B.ship_name || B.mmsi}<br>
        No collision data or TCPA=0</p>
      `;
    }
  }
}

initMap();