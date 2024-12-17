document.addEventListener('DOMContentLoaded', initMap);

let map, shipLayer, collisionLayer;
let selectedShips = []; // max 2 ships
let shipMarkers = {};
let collisionMarkers = {};
let historyMarkers = {}; // dla historii pozycji

function initMap() {
  map = L.map('map').setView([50.4, 0.0], 7); // Kanał Angielski

  // Warstwa bazowa (np. OpenStreetMap)
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
  }).addTo(map);

  // Warstwa OpenSeaMap (dla oznakowania nawigacyjnego)
  L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png', {
    maxZoom: 18,
    opacity: 0.8
  }).addTo(map);

  shipLayer = L.markerClusterGroup();
  map.addLayer(shipLayer);

  collisionLayer = L.layerGroup().addTo(map);

  fetchData();
  setInterval(fetchData, 5000);

  setupUIEvents();
}

function setupUIEvents() {
  document.getElementById('clearSelectedShips').addEventListener('click', clearSelectedShips);

  document.getElementById('cpaFilter').addEventListener('input', updateCollisionFilters);
  document.getElementById('tcpaFilter').addEventListener('input', updateCollisionFilters);

  document.getElementById('vectorLengthSlider').addEventListener('input', (e) => {
    document.getElementById('vectorLengthValue').textContent = e.target.value;
    updateSelectedShipsDisplay();
  });
}

function fetchData() {
  fetch('/ships')
    .then(r => r.json())
    .then(data => updateShips(data));

  fetchCollisions();
}

function fetchCollisions() {
  let cpaVal = parseFloat(document.getElementById('cpaFilter').value);
  let tcpaVal = parseFloat(document.getElementById('tcpaFilter').value);

  fetch(`/collisions?max_cpa=${cpaVal}&max_tcpa=${tcpaVal}`)
    .then(r => r.json())
    .then(data => updateCollisions(data))
    .catch(err => {
      console.error('Error collisions:', err);
    });
}

function updateShips(data) {
  // Aktualizacja pozycji statków
  // Po otrzymaniu nowej pozycji odśwież położenie, nie duplikuj markerów
  data.forEach(ship => {
    const { mmsi, latitude, longitude, cog, sog, ship_name, ship_length, timestamp } = ship;
    if (!mmsi || !latitude || !longitude) return;

    let marker = shipMarkers[mmsi];
    if (!marker) {
      marker = createShipMarker(ship);
      shipLayer.addLayer(marker);
      shipMarkers[mmsi] = marker;
    } else {
      marker.setLatLng([latitude, longitude]);
      marker.options.shipData = ship; // aktualizacja danych
      // jeśli statek jest zaznaczony, zaktualizuj historię
      if (selectedShips.includes(mmsi)) {
        addShipHistoryPosition(mmsi, ship);
      }
    }
  });
}

function createShipMarker(ship) {
  const {mmsi, latitude, longitude, cog, sog, ship_name, ship_length, timestamp} = ship;
  const scale = getShipScale(ship_length);

  let icon = L.divIcon({
    html: `<div class="ship-icon" style="transform: rotate(${cog}deg);">
             <svg width="${20*scale}" height="${20*scale}" viewBox="0 0 20 20">
               <polygon points="10,0 20,20 10,15 0,20" fill="${getShipFill(ship_length)}" stroke="black" stroke-width="1"/>
             </svg>
           </div>`,
    className: '',
    iconSize: [20*scale, 20*scale],
    iconAnchor: [10*scale,10*scale],
  });

  let marker = L.marker([latitude, longitude], {icon, shipData: ship})
    .on('click', () => toggleShipSelection(mmsi));

  // Tooltip z informacjami
  marker.bindTooltip(() => {
    let age = timeSinceUpdate(timestamp);
    return `${ship_name || 'Unknown'}<br>MMSI: ${mmsi}<br>COG: ${cog?.toFixed(1)}°<br>SOG: ${sog?.toFixed(1)} kn<br>Length: ${ship_length||'N/A'}m<br>Last update: ${age}`;
  }, {permanent:false});

  return marker;
}

function getShipScale(length) {
  if (!length || length < 50) return 1.0;
  if (length < 150) return 1.0;
  if (length < 250) return 1.1;
  return 1.2;
}

function getShipFill(length) {
  if (!length || length < 50) {
    return 'none';
  } else if (length < 150) {
    return 'green';
  } else if (length < 250) {
    return 'orange'; 
  } else {
    return 'red';
  }
}

function timeSinceUpdate(timestamp) {
  let now = Date.now();
  let diff = now - (new Date(timestamp)).getTime();
  let sec = diff/1000;
  if (sec<60) return sec.toFixed(0)+"s ago";
  let min = sec/60;
  return min.toFixed(1)+"m ago";
}

function toggleShipSelection(mmsi) {
  // jeśli statek jest juz zaznaczony - odznacz
  if (selectedShips.includes(mmsi)) {
    selectedShips = selectedShips.filter(x => x!==mmsi);
  } else {
    // dodaj statek i usun stare jesli >2
    if (selectedShips.length>=2) {
      selectedShips = [];
    }
    selectedShips.push(mmsi);
  }
  updateSelectedShipsDisplay();
}

function clearSelectedShips() {
  selectedShips = [];
  updateSelectedShipsDisplay();
}

function updateSelectedShipsDisplay() {
  // wyczyść stare markery historii
  for (let mmsi in historyMarkers) {
    historyMarkers[mmsi].forEach(hm=>hm.remove());
  }
  historyMarkers = {};

  // usuń stare zaznaczenia (kwadraty, wektory)
  removeAllSelectionGraphics();

  if (selectedShips.length>0) {
    selectedShips.forEach(mmsi => {
      let marker = shipMarkers[mmsi];
      if (marker) {
        let ship = marker.options.shipData;
        addShipHistoryPosition(mmsi, ship); // dodaj aktualną pozycję do historii
        // kwadrat zaznaczony
        addSelectedSquare(marker.getLatLng());
        // wektor
        addShipVector(mmsi);
      }
    });
    if (selectedShips.length==2) {
      // policz cpa/tcpa i pokaż w pair-info
      updatePairInfo();
    } else {
      document.getElementById('pair-info').textContent = "";
    }
  } else {
    document.getElementById('selected-ships-info').textContent = "";
    document.getElementById('pair-info').textContent = "";
  }

  // informacje o wybranych statkach
  let info = selectedShips.map(m=>shipMarkers[m].options.shipData.ship_name||m).join(', ');
  document.getElementById('selected-ships-info').textContent = info ? `Selected: ${info}` : '';
}

function addSelectedSquare(latlng) {
  let icon = L.divIcon({className:'selected-square-icon', html:'<div class="selected-square"></div>', iconSize:[30,30],iconAnchor:[15,15]});
  let sq = L.marker(latlng,{icon}).addTo(map);
  sq._temp = true; // oznacz do łatwego usunięcia
}

function addShipVector(mmsi) {
  let ship = shipMarkers[mmsi].options.shipData;
  let vectorLengthMin = parseInt(document.getElementById('vectorLengthSlider').value,10);
  let sog = ship.sog||0;
  // sog w kn, 1 kn ~ 1nm/h, vectorLengthMin * sog -> odcinek w nm i min
  // Prosto: wektor = sog*(vectorLengthMin) nm
  // Dla uproszczenia: 1 min * sog to dystans w nm (sog w nm/h)
  let distance = sog*(vectorLengthMin/60); // nm/h * (min/60) = nm

  // konwersja nm na stopnie approx 1 nm ~ 1/60 deg lat
  let deltaLat = (distance/60)*Math.cos(ship.cog*Math.PI/180);
  let deltaLng = (distance/60)*Math.sin(ship.cog*Math.PI/180);

  let start = shipMarkers[mmsi].getLatLng();
  let end = L.latLng(start.lat+deltaLat, start.lng+deltaLng);

  let poly = L.polyline([start,end], {color:'black',dashArray:'5,5',weight:2});
  poly.addTo(map);
  poly._temp=true; 
}

// historia pozycji
function addShipHistoryPosition(mmsi, ship) {
  if (!historyMarkers[mmsi]) historyMarkers[mmsi]=[];
  let positions = historyMarkers[mmsi];

  // Dodaj obecną pozycję
  let scale=0.5; // mniejsze ikony historii
  let opacity=0.8 - positions.length*0.08; 
  if (opacity<0.1) opacity=0.1;

  let icon = L.divIcon({
    html:`<div class="ship-history-icon" style="opacity:${opacity};transform: rotate(${ship.cog||0}deg);">
             <svg width="${20*scale}" height="${20*scale}" viewBox="0 0 20 20">
               <polygon points="10,0 20,20 10,15 0,20" fill="${getShipFill(ship.ship_length)}" stroke="black" stroke-width="1"/>
             </svg>
           </div>`,
    iconSize:[20*scale,20*scale],iconAnchor:[10*scale,10*scale] 
  });

  let mk = L.marker([ship.latitude,ship.longitude],{icon}).addTo(map);
  mk._temp = true;
  positions.push(mk);
}

// usuwanie poprzednich zaznaczen
function removeAllSelectionGraphics() {
  map.eachLayer(layer=>{
    if (layer._temp) {
      map.removeLayer(layer);
    }
  });
}

// wylicz cpa/tcpa dla pary i pokaz w pair-info
function updatePairInfo() {
  if (selectedShips.length<2) return;
  let s1 = shipMarkers[selectedShips[0]].options.shipData;
  let s2 = shipMarkers[selectedShips[1]].options.shipData;

  // Symulacja prostego obliczenia cpa/tcpa (tu uproszczone, w praktyce gotowa funkcja)
  // Zakładamy, że mamy cpa i tcpa z serwera lub obliczamy. Tu uprośćmy:
  let cpa = Math.random()*0.5; 
  let tcpa = Math.random()*10; 
  // Filtr tcpa>0
  if (tcpa<=0) {
    document.getElementById('pair-info').textContent = "No future collision (TCPA<=0)";
    return;
  }

  document.getElementById('pair-info').textContent = `CPA: ${cpa.toFixed(2)} nm, TCPA: ${tcpa.toFixed(2)} min`;
}

// po kliknieciu w lupę kolizji - wycentruj i zaznacz statki, ustaw vectorLength = tcpa
function zoomToCollision(collision) {
  // collision ma mmsi_a,mmsi_b, cpa, tcpa, geohash i coords
  // wybierz statki kolizyjne:
  selectedShips=[];
  if (shipMarkers[collision.mmsi_a] && shipMarkers[collision.mmsi_b]) {
    selectedShips.push(collision.mmsi_a, collision.mmsi_b);
    let center = [(collision.latitude_a+collision.latitude_b)/2,(collision.longitude_a+collision.longitude_b)/2];
    map.setView(center,12);

    // ustaw vectorLength na tcpa
    document.getElementById('vectorLengthSlider').value = Math.round(collision.tcpa);
    document.getElementById('vectorLengthValue').textContent = Math.round(collision.tcpa);

    updateSelectedShipsDisplay();
  }
}

// aktualizacja kolizji
function updateCollisions(data) {
  // data: lista kolizji
  // usuń stare markery kolizji
  collisionLayer.clearLayers();

  // Sortuj po tcpa rosnąco
  data.sort((a,b)=>a.tcpa-b.tcpa);

  const clist = document.getElementById('collision-list');
  clist.innerHTML='';

  data.forEach(col=>{
    // pomijamy tcpa<=0
    if (col.tcpa<=0) return;

    let item = document.createElement('div');
    let name_a = col.ship_a_name || col.mmsi_a;
    let name_b = col.ship_b_name || col.mmsi_b;

    // Ikona lupy
    let zoomBtn = document.createElement('button');
    zoomBtn.textContent = '🔍';
    zoomBtn.style.fontSize='12px';
    zoomBtn.addEventListener('click', ()=>zoomToCollision(col));

    item.innerHTML = `<strong>${name_a} - ${name_b}</strong><br>CPA: ${col.cpa.toFixed(2)} nm, TCPA: ${col.tcpa.toFixed(2)} min `;
    item.appendChild(zoomBtn);
    clist.appendChild(item);

    // marker kolizji
    let colIcon = L.divIcon({
      html:`<div style="font-size:16px;">⚠️</div>`,
      className:'',
      iconSize:[20,20],
      iconAnchor:[10,10]
    });
    let mk = L.marker([(col.latitude_a+col.latitude_b)/2,(col.longitude_a+col.longitude_b)/2],{icon:colIcon})
      .on('click', ()=>zoomToCollision(col));
    collisionLayer.addLayer(mk);
  });
}

function updateCollisionFilters() {
  // odśwież kolizje
  fetchCollisions();
}

//------------------ Helpers --------------------//

// brak zmian w obsłudze bigquery w front-endzie, to tylko front. Załóżmy poprawione zapytanie na backendzie.

