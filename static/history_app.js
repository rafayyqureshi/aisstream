// ======================
// history_app.js
// (Moduł obsługujący prezentację historycznych kolizji)
// ======================

// Zmienne globalne
let map;                 // Obiekt mapy Leaflet
let collisionMarkers = [];  // Markery kolizji na mapie

// Sterowanie dniem (z "slider" lub przyciskami)
let currentDay = 0;      
const minDay = -7;
const maxDay = 0;

// Filtr CPA (suwak)
let cpaFilter = 0.5;  

// Animacja
let isPlaying = false;
let animationData = [];   // Pełna tablica klatek
let animationIndex = 0;   
let animationInterval = null;

let inSituationView = false;  // Czy jesteśmy w trybie "podglądu kolizji"
let shipMarkersOnMap = [];    // Markery statków w danej klatce animacji

/**
 * Inicjalizacja mapy i interfejsu.
 */
function initMap() {
  // Zakładamy, że w common.js mamy np. initSharedMap
  // Zamiast L.map(...) ->:
  map = initSharedMap("map"); 

  updateDayLabel();
  setupUI();
  fetchCollisionsData();

  // Kliknięcie na pustej mapie = wyjście z widoku kolizji
  map.on('click', () => {
    if (inSituationView) {
      exitSituationView();
    }
  });
}

/**
 * setupUI() – konfiguracja przycisków i suwaków
 */
function setupUI() {
  // Nawigacja po dniach
  document.getElementById('prevDay').addEventListener('click', () => {
    if (currentDay > minDay) {
      currentDay--;
      updateDayLabel();
      fetchCollisionsData();
    }
  });
  document.getElementById('nextDay').addEventListener('click', () => {
    if (currentDay < maxDay) {
      currentDay++;
      updateDayLabel();
      fetchCollisionsData();
    }
  });

  // Sterowanie animacją
  document.getElementById('playPause').addEventListener('click', () => {
    if (isPlaying) stopAnimation();
    else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click', () => stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', () => stepAnimation(-1));

  // Filtr CPA
  document.getElementById('cpaFilter').addEventListener('input', (e) => {
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisionsData();
  });
}

/**
 * updateDayLabel() – wyświetla aktualnie wybrany dzień
 */
function updateDayLabel() {
  const now = new Date();
  let targetDate = new Date(now);
  targetDate.setDate(now.getDate() + currentDay);

  const dateStr = targetDate.toISOString().slice(0, 10);
  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;

  document.getElementById('prevDay').disabled = (currentDay <= minDay);
  document.getElementById('nextDay').disabled = (currentDay >= maxDay);
}

/**
 * fetchCollisionsData() – pobiera kolizje z /history_collisions
 * i przekazuje do displayCollisions().
 */
function fetchCollisionsData() {
  clearCollisions();

  fetch(`/history_collisions?day=${currentDay}&max_cpa=${cpaFilter}`)
    .then(r => r.json())
    .then(data => displayCollisions(data))
    .catch(err => console.error("Error fetching collisions:", err));
}

/**
 * displayCollisions(collisions) – wyświetla listę kolizji w panelu
 * i rysuje markery na mapie.
 */
function displayCollisions(collisions) {
  const listContainer = document.getElementById('collision-list');
  listContainer.innerHTML = '';

  if (!collisions || collisions.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = `<div style="padding:10px;font-style:italic;">
      No collisions for this day.
    </div>`;
    listContainer.appendChild(noItem);
    return;
  }

  // Ewentualne usuwanie duplikatów
  let uniqueMap = {};
  collisions.forEach(c => {
    if (!c.collision_id) {
      // awaryjne generowanie
      c.collision_id = `${c.mmsi_a}_${c.mmsi_b}_${c.timestamp || ''}`;
    }
    if (!uniqueMap[c.collision_id]) {
      uniqueMap[c.collision_id] = c;
    }
  });

  let finalCollisions = Object.values(uniqueMap);

  // Tworzenie elementów listy i markerów
  finalCollisions.forEach(c => {
    const item = document.createElement('div');
    item.classList.add('collision-item');

    // Domyślne nazwy
    let shipA = c.ship1_name || c.mmsi_a;
    let shipB = c.ship2_name || c.mmsi_b;

    // Czas
    let timeStr = 'unknown';
    if (c.timestamp) {
      timeStr = new Date(c.timestamp).toLocaleTimeString('en-GB');
    }

    // Wpis w panelu
    item.innerHTML = `
      <div class="collision-header">
        <strong>${shipA} - ${shipB}</strong><br>
        CPA: ${Number(c.cpa).toFixed(2)} nm @ ${timeStr}
        <button class="zoom-button">🔍</button>
      </div>
    `;
    listContainer.appendChild(item);

    // Obsługa kliknięcia (zoom + wczytanie animacji)
    item.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(c);
    });

    // Marker kolizyjny
    let latC = (c.latitude_a + c.latitude_b) / 2;
    let lonC = (c.longitude_a + c.longitude_b) / 2;

    // Ikona – można użyć np. splitted circle lub prostą ikonkę
    // Ale tu na szybko:
    const collisionIcon = L.divIcon({
      className: '',
      html: `
        <svg width="20" height="20" viewBox="-10 -10 20 20">
          <polygon points="0,-6 6,6 -6,6"
                   fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="3" text-anchor="middle"
                font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize: [20, 20],
      iconAnchor: [10, 10]
    });

    const marker = L.marker([latC, lonC], { icon: collisionIcon })
      .on('click', () => zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

/**
 * zoomToCollision(c) – przybliża mapę do danej kolizji
 * i wczytuje animację z /history_data?collision_id=...
 */
function zoomToCollision(c) {
  let bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, { padding: [40, 40] });

  loadCollisionData(c.collision_id);
}

/**
 * loadCollisionData(collision_id) – pobieramy dane animacji (klatek)
 * i rozpoczynamy prezentację.
 */
function loadCollisionData(collision_id) {
  fetch(`/history_data?collision_id=${collision_id}`)
    .then(r => r.json())
    .then(data => {
      animationData = data || [];
      animationIndex = 0;
      stopAnimation(); // na wszelki wypadek
      inSituationView = true;

      // Pokaż panele
      document.getElementById('left-panel').style.display = 'block';
      document.getElementById('bottom-center-bar').style.display = 'block';

      updateMapFrame();

      // Jeżeli chcemy dopasować do np. klatki 9
      // (jeśli jest wystarczająco klatek)
      if (animationData.length >= 10 && animationData[9].shipPositions?.length >= 1) {
        let boundingPoints = [];
        animationData[9].shipPositions.forEach(sp => {
          boundingPoints.push([sp.lat, sp.lon]);
        });
        if (boundingPoints.length > 0) {
          let b = L.latLngBounds(boundingPoints);
          map.fitBounds(b, { padding: [20, 20] });
        }
      }
    })
    .catch(err => console.error("Error /history_data:", err));
}

/**
 * startAnimation() – uruchamiamy animację
 */
function startAnimation() {
  if (!animationData || animationData.length === 0) return;
  isPlaying = true;
  document.getElementById('playPause').textContent = 'Pause';
  animationInterval = setInterval(() => stepAnimation(1), 1000);
}

/**
 * stopAnimation() – zatrzymujemy animację
 */
function stopAnimation() {
  isPlaying = false;
  document.getElementById('playPause').textContent = 'Play';
  if (animationInterval) {
    clearInterval(animationInterval);
    animationInterval = null;
  }
}

/**
 * stepAnimation(step) – zmiana klatki o 'step'
 */
function stepAnimation(step) {
  animationIndex += step;
  if (animationIndex < 0) animationIndex = 0;
  if (animationIndex >= animationData.length) {
    animationIndex = animationData.length - 1;
  }
  updateMapFrame();
}

/**
 * updateMapFrame() – wyświetla aktualną klatkę animacji
 */
function updateMapFrame() {
  const frameIndicator = document.getElementById('frameIndicator');
  frameIndicator.textContent = `${animationIndex + 1}/${animationData.length}`;

  // Usuwamy stare markery
  shipMarkersOnMap.forEach(m => map.removeLayer(m));
  shipMarkersOnMap = [];

  if (!animationData || animationData.length === 0) return;

  let frame = animationData[animationIndex];
  let ships = frame.shipPositions || [];

  // Rysujemy markery statków
  ships.forEach(s => {
    // createShipIcon() lub splitted circle z common.js
    let icon = createShipIcon(s, false);  
    // np.: createShipIcon(s, false) — jeśli jest odpowiednia sygnatura w common.js

    let marker = L.marker([s.lat, s.lon], { icon });
    // Tooltip z info
    let nm = s.name || s.mmsi;
    let toolTip = `
      <b>${nm}</b><br>
      COG: ${Math.round(s.cog)}°, SOG: ${s.sog.toFixed(1)} kn<br>
      Len: ${s.ship_length || 'Unknown'}
    `;
    marker.bindTooltip(toolTip, { direction: 'top', sticky: true });
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  // Możemy też wyświetlać info w panelu po lewej
  const leftPanel = document.getElementById('selected-ships-info');
  leftPanel.innerHTML = '';
  const pairInfo = document.getElementById('pair-info');
  pairInfo.innerHTML = '';

  // Jeśli jest dokładnie 2 statki, obliczamy cpa/tcpa w JS:
  if (ships.length === 2) {
    let sA = ships[0];
    let sB = ships[1];
    let { cpa, tcpa } = compute_cpa_tcpa_js(sA, sB);

    let tObj = new Date(frame.time);
    let hh = tObj.getHours().toString().padStart(2, '0');
    let mm = tObj.getMinutes().toString().padStart(2, '0');
    let ss = tObj.getSeconds().toString().padStart(2, '0');
    let timeStr = `${hh}:${mm}:${ss}`;

    // Przykładowa logika
    if (animationIndex > 6) {
      pairInfo.innerHTML = `
        Time: ${timeStr}<br>
        Distance now: ${cpa.toFixed(2)} nm (ships are moving apart)
      `;
    } else {
      pairInfo.innerHTML = `
        Time: ${timeStr}<br>
        CPA: ${cpa.toFixed(2)} nm, TCPA: ${tcpa.toFixed(2)} min
      `;
    }

    // Info w lewym panelu
    leftPanel.innerHTML = `
      <b>${sA.name || sA.mmsi}</b><br>
      SOG:${sA.sog.toFixed(1)} kn, COG:${Math.round(sA.cog)}°, L:${sA.ship_length||'N/A'}<br><br>
      <b>${sB.name || sB.mmsi}</b><br>
      SOG:${sB.sog.toFixed(1)} kn, COG:${Math.round(sB.cog)}°, L:${sB.ship_length||'N/A'}
    `;
  }
}

/**
 * compute_cpa_tcpa_js(a, b) – uproszczone obliczenia CPA/TCPA w JS
 * (opcjonalnie, można skopiować z modułu live)
 */
function compute_cpa_tcpa_js(a, b) {
  // Zwraca np. { cpa: ..., tcpa: ... }
  // Tu placeholder:
  return { cpa: 0.35, tcpa: 4.5 };
}

/**
 * exitSituationView() – powrót do trybu „listy kolizji”
 */
function exitSituationView() {
  inSituationView = false;
  document.getElementById('left-panel').style.display = 'none';
  document.getElementById('bottom-center-bar').style.display = 'none';
  stopAnimation();

  // Usunięcie markerów statków
  shipMarkersOnMap.forEach(m => map.removeLayer(m));
  shipMarkersOnMap = [];

  // Czyścimy animację
  animationData = [];
  animationIndex = 0;
}

/**
 * clearCollisions() – usuwa markery kolizyjne z mapy
 */
function clearCollisions() {
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];
}

// Start
document.addEventListener('DOMContentLoaded', initMap);