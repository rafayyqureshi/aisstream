// ======================
// history_app.js (Moduł obsługujący prezentację historycznych kolizji)
// ======================

// Zmienne globalne
let map;                     // Obiekt mapy (Leaflet)
let collisionMarkers = [];   // Markery kolizji na mapie

// Zakres dni (np. -7 .. 0)
let currentDay = 0;
const minDay = -7;
const maxDay = 0;

// Filtr CPA
let cpaFilter = 0.5;   

// Animacja
let isPlaying = false;
let animationData = [];  
let animationIndex = 0;  
let animationInterval = null;

let inSituationView = false;  
let shipMarkersOnMap = [];  

/**
 * Inicjalizacja mapy i interfejsu po załadowaniu DOM.
 */
function initHistoryApp() {
  // 1) Inicjalizacja mapy (z common.js)
  map = initSharedMap("map"); // Musi istnieć <div id="map"> w HTML

  // 2) Ustawiamy interfejs
  setupUI();
  updateDayLabel();

  // 3) Pobieramy kolizje dla wybranego dnia
  fetchCollisionsData();

  // Klik w pusty obszar mapy -> wyjście z widoku sytuacji
  map.on('click', () => {
    if (inSituationView) {
      exitSituationView();
    }
  });
}

/**
 * setupUI – podpina eventy do przycisków/suwaków.
 */
function setupUI() {
  // Nawigacja po dniach
  const prevBtn = document.getElementById('prevDay');
  if (prevBtn) {
    prevBtn.addEventListener('click', () => {
      if (currentDay > minDay) {
        currentDay--;
        updateDayLabel();
        fetchCollisionsData();
      }
    });
  }
  const nextBtn = document.getElementById('nextDay');
  if (nextBtn) {
    nextBtn.addEventListener('click', () => {
      if (currentDay < maxDay) {
        currentDay++;
        updateDayLabel();
        fetchCollisionsData();
      }
    });
  }

  // Suwak cpaFilter
  const cpaSlider = document.getElementById('cpaFilter');
  if (cpaSlider) {
    cpaSlider.addEventListener('input', (e) => {
      cpaFilter = parseFloat(e.target.value) || 0.5;
      const cpaValEl = document.getElementById('cpaValue');
      if (cpaValEl) {
        cpaValEl.textContent = cpaFilter.toFixed(2);
      }
      fetchCollisionsData();
    });
  }

  // Sterowanie animacją
  const playPauseBtn = document.getElementById('playPause');
  if (playPauseBtn) {
    playPauseBtn.addEventListener('click', () => {
      if (isPlaying) stopAnimation();
      else startAnimation();
    });
  }

  const stepFBtn = document.getElementById('stepForward');
  if (stepFBtn) {
    stepFBtn.addEventListener('click', () => stepAnimation(1));
  }

  const stepBBtn = document.getElementById('stepBack');
  if (stepBBtn) {
    stepBBtn.addEventListener('click', () => stepAnimation(-1));
  }
}

/**
 * updateDayLabel – wyświetla aktualnie wybrany dzień w UI
 */
function updateDayLabel() {
  const now = new Date();
  let targetDate = new Date(now);
  targetDate.setDate(now.getDate() + currentDay);
  let dateStr = targetDate.toISOString().slice(0, 10);

  const dayLabel = document.getElementById('currentDayLabel');
  if (dayLabel) {
    dayLabel.textContent = `Date: ${dateStr}`;
  }
  
  // Dezaktywuj przyciski?
  if (document.getElementById('prevDay')) {
    document.getElementById('prevDay').disabled = (currentDay <= minDay);
  }
  if (document.getElementById('nextDay')) {
    document.getElementById('nextDay').disabled = (currentDay >= maxDay);
  }
}

/**
 * fetchCollisionsData – pobiera listę kolizji z /history_collisions
 */
function fetchCollisionsData() {
  clearCollisions(); // usuwa stare markery

  // Endpoint, np. /history_collisions?day=...&max_cpa=...
  fetch(`/history_collisions?day=${currentDay}&max_cpa=${cpaFilter}`)
    .then(response => {
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      return response.json();
    })
    .then(data => {
      displayCollisions(data);
    })
    .catch(err => {
      console.error("Error fetching collisions:", err);
      displayCollisions([]); // w razie błędu – pusta lista
    });
}

/**
 * displayCollisions(collisions) – wyświetla listę kolizji i rysuje markery
 */
function displayCollisions(collisions) {
  const listEl = document.getElementById('collision-list');
  if (!listEl) return;

  listEl.innerHTML = '';

  if (!collisions || collisions.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = `<div style="padding:10px;font-style:italic;">
      No collisions for this day.
    </div>`;
    listEl.appendChild(noItem);
    return;
  }

  // Ewentualna deduplikacja
  let uniqueMap = {};
  collisions.forEach(c => {
    if (!c.collision_id) {
      c.collision_id = `${c.mmsi_a}_${c.mmsi_b}_${c.timestamp || ''}`;
    }
    if (!uniqueMap[c.collision_id]) {
      uniqueMap[c.collision_id] = c;
    }
  });
  let finalCollisions = Object.values(uniqueMap);

  finalCollisions.forEach(c => {
    let shipA = c.ship1_name || c.mmsi_a;
    let shipB = c.ship2_name || c.mmsi_b;
    let cpaVal = c.cpa.toFixed(2);
    let timeStr = (c.timestamp)
      ? new Date(c.timestamp).toLocaleTimeString('en-GB')
      : '???';

    const item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <strong>${shipA} - ${shipB}</strong><br>
          CPA: ${cpaVal} nm @ ${timeStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    listEl.appendChild(item);

    // Obsługa kliknięcia
    item.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(c);
    });

    // Rysuj marker
    let latC = (c.latitude_a + c.latitude_b) / 2;
    let lonC = (c.longitude_a + c.longitude_b) / 2;

    const collisionIcon = L.divIcon({
      className: '',
      html: `
        <svg width="18" height="18" viewBox="-9 -9 18 18">
          <polygon points="0,-6 6,6 -6,6"
                   fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="3" text-anchor="middle"
                font-size="7" fill="red">!</text>
        </svg>
      `,
      iconSize: [18, 18],
      iconAnchor: [9, 9]
    });

    const marker = L.marker([latC, lonC], { icon: collisionIcon })
      .on('click', () => zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

/**
 * zoomToCollision(c) – przybliż do kolizji i pobierz klatki animacji
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
 * loadCollisionData(collision_id) – pobieramy dane animacji
 */
function loadCollisionData(collision_id) {
  // Endpoint np. /history_data?collision_id=...
  fetch(`/history_data?collision_id=${collision_id}`)
    .then(res => {
      if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
      }
      return res.json();
    })
    .then(data => {
      animationData = data || [];
      animationIndex = 0;
      stopAnimation(); 
      inSituationView = true;

      // Pokaż panele animacji
      document.getElementById('left-panel').style.display = 'block';
      document.getElementById('bottom-center-bar').style.display = 'block';

      updateMapFrame();

      // Ewentualnie dopasowanie do klatki nr 9
      if (animationData.length >= 10 && animationData[9].shipPositions?.length > 0) {
        let latlngs = [];
        animationData[9].shipPositions.forEach(s => {
          latlngs.push([s.lat, s.lon]);
        });
        if (latlngs.length > 0) {
          let b = L.latLngBounds(latlngs);
          map.fitBounds(b, { padding: [20, 20] });
        }
      }
    })
    .catch(err => {
      console.error("Error loadCollisionData:", err);
    });
}

/**
 * startAnimation() – start
 */
function startAnimation() {
  if (!animationData || animationData.length === 0) return;
  isPlaying = true;
  document.getElementById('playPause').textContent = 'Pause';
  animationInterval = setInterval(() => stepAnimation(1), 1000);
}

/**
 * stopAnimation() – stop
 */
function stopAnimation() {
  isPlaying = false;
  const btn = document.getElementById('playPause');
  if (btn) {
    btn.textContent = 'Play';
  }
  if (animationInterval) {
    clearInterval(animationInterval);
    animationInterval = null;
  }
}

/**
 * stepAnimation(step) – przesunięcie o klatkę
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
  // Indykator
  const frameIndicator = document.getElementById('frameIndicator');
  if (frameIndicator) {
    frameIndicator.textContent = `${animationIndex + 1}/${animationData.length}`;
  }

  // Usuń stare markery
  shipMarkersOnMap.forEach(m => map.removeLayer(m));
  shipMarkersOnMap = [];

  if (!animationData || animationData.length === 0) return;
  let frame = animationData[animationIndex];
  let ships = frame.shipPositions || [];

  ships.forEach(s => {
    // Ikona statku – z common.js
    let icon = createShipIcon(s, false);
    let marker = L.marker([s.lat, s.lon], { icon });

    // tooltip
    let nm = s.name || s.mmsi;
    let tt = `
      <b>${nm}</b><br>
      COG:${Math.round(s.cog)}°, SOG:${s.sog.toFixed(1)} kn<br>
      Len:${s.ship_length || 'Unknown'}
    `;
    marker.bindTooltip(tt, { direction: 'top', sticky: true });
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  // Ewentualne info panel
  const leftPanel = document.getElementById('selected-ships-info');
  const pairInfo = document.getElementById('pair-info');
  if (leftPanel) leftPanel.innerHTML = '';
  if (pairInfo) pairInfo.innerHTML = '';

  if (ships.length === 2) {
    let sA = ships[0];
    let sB = ships[1];
    let { cpa, tcpa } = compute_cpa_tcpa_js(sA, sB);

    let tObj = new Date(frame.time);
    let hh = tObj.getHours().toString().padStart(2, '0');
    let mm = tObj.getMinutes().toString().padStart(2, '0');
    let ss = tObj.getSeconds().toString().padStart(2, '0');
    let timeStr = `${hh}:${mm}:${ss}`;

    if (pairInfo) {
      pairInfo.innerHTML = `
        Time: ${timeStr}<br>
        CPA: ${cpa.toFixed(2)} nm, TCPA: ${tcpa.toFixed(2)} min
      `;
    }

    if (leftPanel) {
      leftPanel.innerHTML = `
        <b>${sA.name || sA.mmsi}</b><br>
        SOG:${sA.sog.toFixed(1)} kn, COG:${Math.round(sA.cog)}°, L:${sA.ship_length||'N/A'}<br><br>
        <b>${sB.name || sB.mmsi}</b><br>
        SOG:${sB.sog.toFixed(1)} kn, COG:${Math.round(sB.cog)}°, L:${sB.ship_length||'N/A'}
      `;
    }
  }
}

/**
 * compute_cpa_tcpa_js(a,b) – uproszczone obliczenia w JS
 */
function compute_cpa_tcpa_js(a, b) {
  // Prosta „mockowa” logika
  return { cpa: 0.3, tcpa: 5.0 };
}

/**
 * exitSituationView – wyjście z trybu podglądu animacji
 */
function exitSituationView() {
  inSituationView = false;
  document.getElementById('left-panel').style.display = 'none';
  document.getElementById('bottom-center-bar').style.display = 'none';
  stopAnimation();

  shipMarkersOnMap.forEach(m => map.removeLayer(m));
  shipMarkersOnMap = [];
  animationData = [];
  animationIndex = 0;
}

/**
 * clearCollisions() – usuwa kolizyjne markery z mapy
 */
function clearCollisions() {
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];
}

/**
 * Główny start
 */
document.addEventListener('DOMContentLoaded', initHistoryApp);