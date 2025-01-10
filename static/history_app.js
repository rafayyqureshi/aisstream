// ==========================
// history_app.js
// ==========================

let map;
let scenarioMarkers = [];       // ikony scenariuszy na mapie
let currentScenarios = [];      // wszystkie wczytane scenariusze (z wielu plików)
let scenarioGroups = {};        // np. hour -> [scenarios]
let selectedScenario = null;    // aktualnie wybrany scenariusz
let animationData = null;       // trzymamy klatki frames
let animationIndex = 0;
let animationInterval = null;
let isPlaying = false;

let shipMarkersOnMap = [];      // statki w aktualnej klatce
let inSituationView = false;    // czy jesteśmy w trybie odtwarzania

// Parametry filtra cpa (opcjonalnie)
let cpaFilter = 0.5;

// Day offset (jeśli mamy przyciski nextDay/prevDay)
let currentDay = 0;
const minDay = -7;
const maxDay = 0;

// Główna inicjalizacja
function initHistoryApp() {
  // Tworzymy mapę
  map = initSharedMap('map');

  // UI
  setupDayUI();
  setupBottomUI();

  // Start
  fetchFileListAndLoadScenarios();
  
  // Możemy ewentualnie nasłuchiwać klik w mapę => exitSituationView
  map.on('click', () => {
    if (inSituationView) {
      exitSituationView();
    }
  });
}

// -------------------------
// 1) Obsługa day offset
// -------------------------
function setupDayUI() {
  document.getElementById('prevDay').addEventListener('click', () => {
    if (currentDay > minDay) {
      currentDay--;
      updateDayLabel();
      clearAllScenarios();
      fetchFileListAndLoadScenarios();
    }
  });
  document.getElementById('nextDay').addEventListener('click', () => {
    if (currentDay < maxDay) {
      currentDay++;
      updateDayLabel();
      clearAllScenarios();
      fetchFileListAndLoadScenarios();
    }
  });
  updateDayLabel();
}
function updateDayLabel() {
  const now = new Date();
  let d = new Date(now);
  d.setDate(now.getDate() + currentDay);
  const dateStr = d.toISOString().slice(0, 10);
  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
}

// -------------------------
// 2) Fetch plików GCS + parse
// -------------------------
function fetchFileListAndLoadScenarios() {
  const dayOffsetParam = currentDay; // lub np. param “days=7”, w zależności od Twojej implementacji
  // np. /history_filelist?days=7, ale można też rozbić to na inny param
  const url = `/history_filelist?days=${7 + (currentDay>=0 ? currentDay : 0)}`;
  // (powyższy 7 to przykładowy – dostosuj jak chcesz)

  fetch(url)
    .then(res => {
      if (!res.ok) {
        throw new Error(`HTTP status ${res.status} - ${res.statusText}`);
      }
      return res.json();
    })
    .then(data => {
      const files = data.files || [];
      if (files.length === 0) {
        console.log('Brak plików w GCS dla day=', currentDay);
        updateCollisionListUI();
        return;
      }
      // Wczytujemy każdy plik po kolei
      return loadAllScenarioFiles(files);
    })
    .catch(err => {
      console.error("Błąd fetchFileList:", err);
    });
}

function loadAllScenarioFiles(fileList) {
  // Oczyszczamy
  currentScenarios = [];
  scenarioGroups = {};
  scenarioMarkers.forEach(m => map.removeLayer(m));
  scenarioMarkers = [];

  // Ładujemy pliki sekwencyjnie (dla uproszczenia Promise chain)
  const promises = fileList.map(f => {
    const fname = f.name;
    return fetch(`/history_file?file=${encodeURIComponent(fname)}`)
      .then(r => {
        if (!r.ok) {
          throw new Error(`HTTP status ${r.status} - ${r.statusText}`);
        }
        return r.json();
      })
      .then(jsonData => {
        // Plik ma: { "scenarios": [ ... ] }
        if (!jsonData.scenarios) {
          console.warn(`Plik ${fname} nie zawiera "scenarios". Pomijam...`);
          return;
        }
        jsonData.scenarios.forEach(sc => {
          // Dla uproszczenia doklejamy 'fileName' i 'timeCreated'?
          sc.fileName = fname;
          currentScenarios.push(sc);
        });
      })
      .catch(err => {
        console.error("Błąd loadOneFile:", err);
      });
  });

  // Gdy wszystkie pliki wczytane
  return Promise.all(promises)
    .then(() => {
      console.log(`Załadowano wszystkie pliki. currentScenarios.length:`, currentScenarios.length);
      groupScenariosByHour();
      updateCollisionListUI();
      drawScenarioMarkers();
    });
}

// 3) Grupowanie po “godzinach” – np. wyciągamy z nazwy pliku "YYYYmmddHH"
function groupScenariosByHour() {
  scenarioGroups = {};  // np. key = "2025010914" (2025-01-09 14)
  currentScenarios.forEach(sc => {
    // scenario_id np. "scenario_123456789_111_222"
    // plik np. "multiship_20250109143512.json"
    const fname = sc.fileName || "";
    // wycinamy datę/godzinę z nazwy pliku
    // Przykład: collisions_20250109143512.json => hourKey = "2025010914"
    const match = fname.match(/(\d{4}\d{2}\d{2}\d{2})\d{2}\d{2}\.json$/);
    let hourKey = "unknown";
    if (match) {
      hourKey = match[1]; // e.g. "2025010914"
    }

    if (!scenarioGroups[hourKey]) {
      scenarioGroups[hourKey] = [];
    }
    scenarioGroups[hourKey].push(sc);
  });
}

// -------------------------
// 4) Generowanie listy w prawym panelu
// -------------------------
function updateCollisionListUI() {
  const listDiv = document.getElementById('collision-list');
  listDiv.innerHTML = '';

  if (Object.keys(scenarioGroups).length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>No collision scenarios found</i>';
    listDiv.appendChild(noItem);
    return;
  }

  // Sortuj klucze
  const hourKeys = Object.keys(scenarioGroups).sort();
  hourKeys.forEach(hourKey => {
    // Tworzymy dropDown
    const hourBlock = document.createElement('div');
    hourBlock.classList.add('hour-block');

    const hourTitle = document.createElement('div');
    hourTitle.classList.add('hour-title');
    hourTitle.textContent = `Hour: ${hourKey}`;
    hourBlock.appendChild(hourTitle);

    // Lista scenariuszy
    const scList = scenarioGroups[hourKey];
    scList.forEach(sc => {
      const item = document.createElement('div');
      item.classList.add('collision-item');
      const scID = sc.scenario_id;

      // Jak opisać? Np. “scenario_id” + liczbę statków + liczbę frames
      const shipsCount = (sc.ships_involved || []).length;
      const framesCount = (sc.frames || []).length;

      item.innerHTML = `
        <strong>${scID}</strong><br>
        Ships: ${shipsCount}, Frames: ${framesCount}
        <button class="zoom-button">🔍</button>
      `;

      // Po kliknięciu w “lupę” => zoom do scenariusza / wczytaj animację
      item.querySelector('.zoom-button').addEventListener('click', () => {
        onSelectScenario(sc);
      });

      hourBlock.appendChild(item);
    });

    listDiv.appendChild(hourBlock);
  });
}

// 5) Rysujemy jedną ikonę na mapie dla scenariusza
function drawScenarioMarkers() {
  // Usuwamy stare
  scenarioMarkers.forEach(m => map.removeLayer(m));
  scenarioMarkers = [];

  Object.keys(scenarioGroups).forEach(hourKey => {
    const scList = scenarioGroups[hourKey];
    scList.forEach(sc => {
      // Ustalmy “pozycję” scenariusza -> np. bierzemy average (latitude, longitude)
      // z collisions_in_scenario[0] - w nowym potoku mamy “collisions_in_scenario”?
      // Albo bierzemy 1. klatkę frames[0], average statków.

      let latSum = 0, lonSum = 0, count = 0;
      if (sc.frames && sc.frames.length > 0) {
        // Weźmy 1. klatkę
        const firstFrame = sc.frames[0];
        const ships = firstFrame.shipPositions || [];
        ships.forEach(s => {
          latSum += s.lat;
          lonSum += s.lon;
          count++;
        });
      }
      if (count === 0) {
        // fallback
        return;
      }
      let latC = latSum / count;
      let lonC = lonSum / count;

      // Marker
      const iconHTML = `
        <svg width="20" height="20" viewBox="-10 -10 20 20">
          <circle cx="0" cy="0" r="8" fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="3" text-anchor="middle" font-size="8" fill="red">S</text>
        </svg>
      `;
      const scenarioIcon = L.divIcon({
        className: '',
        html: iconHTML,
        iconSize: [20,20],
        iconAnchor: [10,10]
      });

      const marker = L.marker([latC, lonC], { icon: scenarioIcon })
        .bindTooltip(`Scenario: ${sc.scenario_id}`, {direction:'top'})
        .on('click', () => {
          onSelectScenario(sc);
        });
      marker.addTo(map);
      scenarioMarkers.push(marker);
    });
  });
}

// Gdy wybieramy scenariusz z listy lub z mapy
function onSelectScenario(scenario) {
  selectedScenario = scenario;
  // Zoom do bounding box (wszystkie statki we wszystkich frames?), lub wystarczy 1. klatka
  if (!scenario.frames || scenario.frames.length===0) {
    console.warn("Scenario has no frames");
    return;
  }

  const firstFrame = scenario.frames[0];
  const ships = firstFrame.shipPositions || [];
  if (ships.length === 0) {
    console.warn("No ships in first frame");
    return;
  }

  let latLngs = ships.map(s => [s.lat, s.lon]);
  let bounds = L.latLngBounds(latLngs);
  map.fitBounds(bounds, {padding:[30,30], maxZoom:13});

  // Otwieramy panel animacji
  loadScenarioAnimation(scenario);
}

// ---------------------------
// 6) Animacja scenariusza
// ---------------------------
function loadScenarioAnimation(scenario) {
  animationData = scenario.frames || [];
  animationIndex = 0;
  stopAnimation();
  inSituationView = true;

  // Pokaż panele
  document.getElementById('left-panel').style.display = 'block';
  document.getElementById('bottom-center-bar').style.display = 'block';

  updateMapFrame();
}

function startAnimation() {
  if (!animationData || animationData.length===0) return;
  isPlaying = true;
  document.getElementById('playPause').textContent = 'Pause';
  animationInterval = setInterval(() => stepAnimation(1), 1000);
}
function stopAnimation() {
  isPlaying = false;
  document.getElementById('playPause').textContent = 'Play';
  if (animationInterval) clearInterval(animationInterval);
  animationInterval = null;
}
function stepAnimation(step) {
  animationIndex += step;
  if (animationIndex < 0) animationIndex = 0;
  if (animationIndex >= animationData.length) animationIndex = animationData.length-1;
  updateMapFrame();
}
function updateMapFrame() {
  const frameIndicator = document.getElementById('frameIndicator');
  frameIndicator.textContent = `${animationIndex+1}/${animationData.length}`;

  // czyścimy stare
  shipMarkersOnMap.forEach(m => map.removeLayer(m));
  shipMarkersOnMap = [];

  if (!animationData || animationData.length===0) return;
  let frame = animationData[animationIndex];
  let ships = frame.shipPositions || [];

  ships.forEach(s => {
    let marker = L.marker([s.lat, s.lon], {
      icon: createShipIcon(s, false) // z common.js
    });
    const nm = s.name || s.mmsi;
    const tooltip = `
      <b>${nm}</b><br>
      COG:${Math.round(s.cog)}°, SOG:${s.sog.toFixed(1)} kn<br>
      Len:${s.ship_length || 'Unknown'}
    `;
    marker.bindTooltip(tooltip, { direction:'top', sticky:true });
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  // Można tu też wypełnić panel “selected-ships-info” => 
  const leftPanel = document.getElementById('selected-ships-info');
  leftPanel.innerHTML = '';
  const pairInfo = document.getElementById('pair-info');
  pairInfo.innerHTML = '';

  // Ponieważ w scenario może być n statków, wyświetlamy np. spis:
  let html = `<b>Frame time:</b> ${frame.time}<br>`;
  ships.forEach(s => {
    html += `
      <div>
        <b>${s.name || s.mmsi}</b> 
        [COG:${Math.round(s.cog)}, SOG:${s.sog.toFixed(1)} kn, L:${s.ship_length||'?'}]
      </div>
    `;
  });
  leftPanel.innerHTML = html;
}

function exitSituationView() {
  inSituationView = false;
  document.getElementById('left-panel').style.display = 'none';
  document.getElementById('bottom-center-bar').style.display = 'none';
  stopAnimation();
  shipMarkersOnMap.forEach(m => map.removeLayer(m));
  shipMarkersOnMap = [];
  animationData = null;
  animationIndex = 0;
}

function clearAllScenarios() {
  // czyścimy listę i marker i panel
  currentScenarios = [];
  scenarioGroups = {};
  scenarioMarkers.forEach(m => map.removeLayer(m));
  scenarioMarkers = [];
  document.getElementById('collision-list').innerHTML = '';
}

function setupBottomUI() {
  document.getElementById('playPause').addEventListener('click', () => {
    if (isPlaying) stopAnimation();
    else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click', () => stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', () => stepAnimation(-1));
}

// ---------------
// init
// ---------------
document.addEventListener('DOMContentLoaded', initHistoryApp);