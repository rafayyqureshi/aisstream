let map;
let shipMarkers = {}; // key: mmsi, value: marker
let collisionMarkers = {};
let selectedShips = []; // Lista zaznaczonych statków (max 2)
let vectorLength = 15; // min domyślnie
let riskFilters = ['High','Medium','Low'];
let collisionsData = [];

// Inicjalizacja mapy
function initMap() {
  map = L.map('map').setView([52.237049, 21.017532], 6);

  // Warstwa OpenSeaMap
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18
  }).addTo(map);

  // Warstwa openseamap znaków morskich
  L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png', {
    maxZoom: 18,
    opacity: 0.7
  }).addTo(map);

  fetchAndUpdateData();
  setInterval(fetchAndUpdateData, 5000); // odśwież co 5s

  // Obsluga suwaka wektora
  const vectorSlider = document.getElementById('vectorLengthSlider');
  const vectorValue = document.getElementById('vectorLengthValue');
  vectorSlider.addEventListener('input', () => {
    vectorLength = parseInt(vectorSlider.value);
    vectorValue.textContent = vectorLength;
    updateSelectedShipsInfo();
  });
}

// Pobierz dane i odśwież mapę, panele
function fetchAndUpdateData() {
  // Pobierz statki
  fetch('/ships')
    .then(r => r.json())
    .then(data => {
      updateShips(data);
    })
    .catch(err => console.error("Error ships:", err));

  // Pobierz kolizje
  fetch('/collisions')
    .then(r => r.json())
    .then(data => {
      collisionsData = data;
      updateCollisionsList();
    })
    .catch(err => console.error("Error collisions:", err));
}

// Aktualizacja statków
function updateShips(data) {
  // data: [{mmsi, ship_name, latitude, longitude, sog, cog, ship_length, timestamp}]

  // Aktualizuj pozycje statków
  // klucz: mmsi
  let seenMmsi = new Set();

  data.forEach(ship => {
    const mmsi = ship.mmsi;
    seenMmsi.add(mmsi);

    if (!shipMarkers[mmsi]) {
      // Tworzymy nowy marker
      const icon = L.divIcon({
        className: '',
        html: `<div style="transform: rotate(${ship.cog}deg);">
                 <svg width="20" height="20" viewBox="0 0 20 20">
                   <polygon points="10,0 15,20 10,15 5,20" fill="#0000ff" stroke="#000" stroke-width="1"/>
                 </svg>
               </div>`,
        iconSize: [20,20],
        iconAnchor: [10,10]
      });

      let marker = L.marker([ship.latitude, ship.longitude], {icon: icon}).addTo(map);
      marker.mmsi = mmsi;
      marker.shipData = ship;
      marker.on('click', () => toggleSelectShip(ship));
      shipMarkers[mmsi] = marker;
    } else {
      // Aktualizuj pozycję i kierunek
      const marker = shipMarkers[mmsi];
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.shipData = ship;
      const el = marker._icon;
      if (el) {
        el.innerHTML = `<div style="transform: rotate(${ship.cog}deg);">
                          <svg width="20" height="20" viewBox="0 0 20 20">
                            <polygon points="10,0 15,20 10,15 5,20" fill="#0000ff" stroke="#000" stroke-width="1"/>
                          </svg>
                        </div>`;
      }
    }
  });

  // Usun markery statków, których nie ma w nowych danych
  for (let mmsi in shipMarkers) {
    if (!seenMmsi.has(parseInt(mmsi))) {
      map.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
    }
  }

  updateSelectedShipsInfo();
}

// Kolizje
function updateCollisionsList() {
  const list = document.getElementById('collision-list');
  list.innerHTML = '';

  const filtered = collisionsData.filter(c => riskFilters.includes(c.risk_category));

  // Sort wg CPA rosnąco
  filtered.sort((a,b) => a.cpa - b.cpa);

  filtered.forEach(col => {
    const item = document.createElement('div');
    item.classList.add('collision-item', col.risk_category.toLowerCase());
    item.innerHTML = `
      <div><strong>${col.ship1_name || col.ship1_mmsi} - ${col.ship2_name || col.ship2_mmsi}</strong><br>
      CPA: ${col.cpa.toFixed(2)} nm, TCPA: ${col.tcpa.toFixed(2)} min
      <button class="zoom-button">🔍</button>
      </div>
    `;
    item.querySelector('.zoom-button').addEventListener('click', () => {
      // Zoom do miejsca kolizji
      map.setView([(col.latitude_a + col.latitude_b)/2, (col.longitude_a + col.longitude_b)/2], 12);
    });
    list.appendChild(item);
  });
}

function filterCollisions() {
  const checkboxes = document.querySelectorAll('#risk-filters input[type="checkbox"]');
  riskFilters = [];
  checkboxes.forEach(cb => { if(cb.checked) riskFilters.push(cb.value); });
  updateCollisionsList();
}

// Zaznaczanie statków
function toggleSelectShip(ship) {
  const idx = selectedShips.findIndex(s => s.mmsi === ship.mmsi);
  if (idx >= 0) {
    // Odznacz
    selectedShips.splice(idx,1);
  } else {
    if (selectedShips.length < 2) {
      selectedShips.push(ship);
    } else {
      // juz 2 zaznaczone - zastąp pierwszy lub ignoruj
      selectedShips[0] = ship;
    }
  }
  updateSelectedShipsInfo();
}

function updateSelectedShipsInfo() {
  const container = document.getElementById('selected-ships-info');
  container.innerHTML = '';
  selectedShips.forEach(ship => {
    const div = document.createElement('div');
    div.classList.add('ship-info');
    div.innerHTML = `
      <strong>${ship.ship_name || "MMSI:"+ship.mmsi}</strong>
      MMSI: ${ship.mmsi}<br>
      SOG: ${ship.sog || 'N/A'}<br>
      COG: ${ship.cog || 'N/A'}<br>
      Długość: ${ship.ship_length || 'N/A'}<br>
      Wektor: ${vectorLength} min
    `;
    container.appendChild(div);
  });
  drawSelectionBoxes();
}

function drawSelectionBoxes() {
  // Idea: Przy 1-2 zaznaczonych statkach rysujemy kwadraty. 
  // Możemy zamiast kwadratów w CSS użyć osobnych markerów lub poligonów.
  // Skasujmy stare kwadraty/polilinie jeśli były.

  // Prostota: Nie dodajemy osobnych markerów kwadratu, tylko np. obrys statku w ikonie
  // lub osobny warstwowy poligon.
  // Tutaj dla uproszczenia – pomijamy skomplikowany kod kotwiczenia kwadratu w tym momencie,
  // bo brak szczegółów co do stylu.
  // Zakładamy tylko poprawki estetyczne i brak nakładania wielu symboli.
  // Można np. narysować niewielki kwadrat markerem obok statku.

  // Najprostsze: usuńmy stare markery kwadratów (jeśli były).
  // zakładamy że w poprzednich próbach kwadraty były markerami - nie tworzymy ich teraz.
  // Można ewentualnie zmienić obramowanie ikony statku jeśli jest zaznaczony.

  // Zróbmy tak: jeśli statek jest zaznaczony, zmodyfikujmy jego iconę (np. dodaj obramowanie)
  for (let mmsi in shipMarkers) {
    const marker = shipMarkers[mmsi];
    const selected = selectedShips.some(s => s.mmsi == mmsi);
    const ship = marker.shipData;
    marker._icon.innerHTML = `<div style="position:relative;transform:rotate(${ship.cog}deg);">
      <svg width="20" height="20" viewBox="0 0 20 20">
        <polygon points="10,0 15,20 10,15 5,20" fill="#0000ff" stroke="${selected?'red':'#000'}" stroke-width="1"/>
      </svg>
    </div>`;
  }
}

document.addEventListener('DOMContentLoaded', initMap);