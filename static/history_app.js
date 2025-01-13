// ==========================
// history_app.js
// ==========================
let map;

// Umbrella scenario => _parent:true
// Sub-scenarios => _parent:false
let umbrellasMap = {};    // key: scenario_id -> umbrella object
let subScenariosMap = {}; // key: scenario_id -> array of sub-scenarios

let scenarioMarkers = []; // markers for each sub-scenario (A–B)
let inSituationView = false;
let isPlaying = false;
let animationData = null;
let animationIndex = 0;
let animationInterval = null;
let shipMarkersOnMap = [];
let selectedScenario = null;

// Day offset
let currentDay = 0;
const minDay = -7;
const maxDay = 0;

function initHistoryApp() {
  // 1) Init map
  map = initSharedMap('map'); // z pliku common.js

  // 2) Setup UI
  setupDayUI();
  setupBottomUI();

  // 3) Load pliki .json z GCS
  fetchFileListAndLoadScenarios();
}

// ---------------------------------------
// A) Day offset
// ---------------------------------------
function setupDayUI() {
  document.getElementById('prevDay').addEventListener('click', () => {
    if (currentDay > minDay) {
      currentDay--;
      updateDayLabel();
      clearAll();
      fetchFileListAndLoadScenarios();
    }
  });
  document.getElementById('nextDay').addEventListener('click', () => {
    if (currentDay < maxDay) {
      currentDay++;
      updateDayLabel();
      clearAll();
      fetchFileListAndLoadScenarios();
    }
  });
  updateDayLabel();
}

function updateDayLabel() {
  const now = new Date();
  let d = new Date(now);
  d.setDate(d.getDate() + currentDay);
  const dateStr = d.toISOString().slice(0, 10);
  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
}

// ---------------------------------------
// B) Pobranie listy plików z GCS + parse
// ---------------------------------------
function fetchFileListAndLoadScenarios() {
  const url = `/history_filelist?days=${7 + currentDay}`;

  fetch(url)
    .then(res => {
      if (!res.ok) throw new Error(`HTTP ${res.status} - ${res.statusText}`);
      return res.json();
    })
    .then(data => {
      const files = data.files || [];
      if (files.length === 0) {
        console.log("No GCS files found for day offset=", currentDay);
        updateCollisionList();
        return;
      }
      return loadAllScenarioFiles(files);
    })
    .catch(err => console.error("fetchFileList error:", err));
}

function loadAllScenarioFiles(fileList) {
  // Wstępne czyszczenie
  umbrellasMap = {};
  subScenariosMap = {};
  scenarioMarkers.forEach(m => map.removeLayer(m));
  scenarioMarkers = [];

  const promises = fileList.map(f => {
    const fname = f.name;
    return fetch(`/history_file?file=${encodeURIComponent(fname)}`)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status} - ${r.statusText}`);
        return r.json();
      })
      .then(jsonData => {
        const arr = jsonData.scenarios || [];
        arr.forEach(obj => {
          if (obj._parent) {
            // Umbrella
            umbrellasMap[obj.scenario_id] = obj;
          } else {
            // Sub-scenario
            const sid = obj.scenario_id;
            if (!subScenariosMap[sid]) subScenariosMap[sid] = [];
            subScenariosMap[sid].push(obj);
          }
        });
      })
      .catch(err => console.error("loadOneFile error:", err));
  });

  return Promise.all(promises)
    .then(() => {
      console.log(
        "Umbrellas:", Object.keys(umbrellasMap).length,
        "Sub-scenarios:", Object.keys(subScenariosMap).length
      );
      updateCollisionList();
      drawScenarioMarkers();
    });
}

// ---------------------------------------
// C) Wyświetlanie listy scenariuszy w panelu
// ---------------------------------------
function updateCollisionList() {
  const listDiv = document.getElementById('collision-list');
  listDiv.innerHTML = '';

  let allIDs = new Set([
    ...Object.keys(umbrellasMap),
    ...Object.keys(subScenariosMap)
  ]);
  if (allIDs.size === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>No collision scenarios found</i>';
    listDiv.appendChild(noItem);
    return;
  }

  const sortedIDs = Array.from(allIDs).sort();

  sortedIDs.forEach(sid => {
    const umb = umbrellasMap[sid];         // ewentualnie undefined
    const subs = subScenariosMap[sid] || [];

    let headerText = `Scenario: ${sid} (subs=${subs.length})`;
    if (umb) {
      const scCount = umb.collisions_count || subs.length;
      const shipsCount = umb.ships_involved ? umb.ships_involved.length : 0;
      headerText = `Umbrella ${sid} [ships:${shipsCount}, collisions:${scCount}]`;
    }

    const blockDiv = document.createElement('div');
    blockDiv.classList.add('umbrella-block');

    const headerDiv = document.createElement('div');
    headerDiv.classList.add('umbrella-header');
    headerDiv.textContent = headerText;

    // expand/collapse
    const expandBtn = document.createElement('button');
    expandBtn.textContent = '▼';
    headerDiv.appendChild(expandBtn);
    blockDiv.appendChild(headerDiv);

    const subListDiv = document.createElement('div');
    subListDiv.style.display = 'none';

    expandBtn.addEventListener('click', () => {
      if (subListDiv.style.display === 'none') {
        subListDiv.style.display = 'block';
        expandBtn.textContent = '▲';
      } else {
        subListDiv.style.display = 'none';
        expandBtn.textContent = '▼';
      }
    });

    // Sub-scenariusze (A–B)
    subs.forEach(sc => {
      const item = document.createElement('div');
      item.classList.add('collision-item');
      const framesCount = sc.frames?.length || 0;
      const scTitle = sc.title || sc.collision_id || sid;

      item.innerHTML = `
        <strong>${scTitle}</strong><br>
        Frames: ${framesCount}
        <button class="zoom-button">🔍</button>
      `;
      item.querySelector('.zoom-button').addEventListener('click', () => {
        onSelectScenario(sc);
      });

      subListDiv.appendChild(item);
    });

    blockDiv.appendChild(subListDiv);
    listDiv.appendChild(blockDiv);
  });
}

// ---------------------------------------
// D) Rysowanie sub-scenariuszy (markery na mapie)
// ---------------------------------------
function drawScenarioMarkers() {
  scenarioMarkers.forEach(m => map.removeLayer(m));
  scenarioMarkers = [];

  for (let sid in subScenariosMap) {
    const subs = subScenariosMap[sid];
    subs.forEach(sub => {
      const frames = sub.frames || [];
      if (frames.length === 0) return;

      let latC = sub.icon_lat;
      let lonC = sub.icon_lon;

      // fallback => 1st frame
      if (latC == null || lonC == null) {
        const ships0 = frames[0].shipPositions || [];
        if (ships0.length > 0) {
          let sumLat = 0, sumLon = 0, cnt = 0;
          ships0.forEach(s => {
            sumLat += s.lat;
            sumLon += s.lon;
            cnt++;
          });
          if (cnt > 0) {
            latC = sumLat / cnt;
            lonC = sumLon / cnt;
          }
        }
      }
      if (latC == null || lonC == null) return;

      // Ikona
      const iconHTML = `
        <svg width="20" height="20" viewBox="-10 -10 20 20">
          <circle cx="0" cy="0" r="8" fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="3" text-anchor="middle" font-size="8" fill="red">C</text>
        </svg>
      `;
      const scenarioIcon = L.divIcon({
        className: '',
        html: iconHTML,
        iconSize: [20, 20],
        iconAnchor: [10, 10]
      });

      const title = sub.title || sub.collision_id;
      const marker = L.marker([latC, lonC], { icon: scenarioIcon })
        .bindTooltip(title, { direction: 'top' })
        .on('click', () => onSelectScenario(sub));
      marker.addTo(map);
      scenarioMarkers.push(marker);
    });
  }
}

// ---------------------------------------
// E) Wybór sub-scenariusza => animacja
// ---------------------------------------
function onSelectScenario(subScenario) {
  selectedScenario = subScenario;
  const frames = subScenario.frames || [];
  if (frames.length === 0) {
    console.warn("Scenario has no frames");
    return;
  }

  // Zoom to first frame
  const ships0 = frames[0].shipPositions || [];
  if (ships0.length > 0) {
    const latLngs = ships0.map(s => [s.lat, s.lon]);
    const b = L.latLngBounds(latLngs);
    map.fitBounds(b, { padding: [30, 30], maxZoom: 13 });
  }

  loadScenarioAnimation(subScenario);
}

function loadScenarioAnimation(subScenario) {
  animationData = subScenario.frames || [];
  animationIndex = 0;
  stopAnimation();
  inSituationView = true;

  document.getElementById('left-panel').style.display = 'block';
  document.getElementById('bottom-center-bar').style.display = 'block';

  updateMapFrame();
}

// ---------------------------------------
// F) Animacja
// ---------------------------------------
function startAnimation() {
  if (!animationData || animationData.length === 0) return;
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
  if (animationIndex >= animationData.length) animationIndex = animationData.length - 1;
  updateMapFrame();
}

function updateMapFrame() {
  const frameIndicator = document.getElementById('frameIndicator');
  frameIndicator.textContent = `${animationIndex + 1}/${animationData.length}`;

  // Usuwamy stare markery
  shipMarkersOnMap.forEach(m => map.removeLayer(m));
  shipMarkersOnMap = [];

  if (!animationData || animationData.length === 0) return;
  const frame = animationData[animationIndex];
  const ships = frame.shipPositions || [];

  // Rysujemy wszystkie statki
  ships.forEach(s => {
    const mk = L.marker([s.lat, s.lon], {
      icon: createShipIcon(s, false) // z common.js
    });
    const nm = s.name || s.mmsi;
    const tt = `
      <b>${nm}</b><br>
      COG: ${Math.round(s.cog)}°, SOG: ${s.sog.toFixed(1)} kn<br>
      L: ${s.ship_length || "??"}
    `;
    mk.bindTooltip(tt, { direction: 'top', sticky: true });
    mk.addTo(map);
    shipMarkersOnMap.push(mk);
  });

  // Panel
  const leftPanel = document.getElementById('selected-ships-info');
  leftPanel.innerHTML = '';
  const pairInfo = document.getElementById('pair-info');
  pairInfo.innerHTML = '';

  let html = `<b>Frame time:</b> ${frame.time}<br>`;
  if (frame.focus_dist !== undefined) {
    html += `<b>Focus Dist:</b> ${frame.focus_dist.toFixed(3)} nm<br>`;
  }
  if (frame.delta_minutes !== undefined) {
    html += `<b>Time to min approach:</b> ${frame.delta_minutes} min<br>`;
  }
  html += `<hr/>`;

  // Podświetlamy "focus" A–B
  if (selectedScenario && selectedScenario.focus_mmsi) {
    const [mA, mB] = selectedScenario.focus_mmsi;
    let posA = null, posB = null;
    ships.forEach(ss => {
      if (ss.mmsi === mA) posA = ss;
      if (ss.mmsi === mB) posB = ss;
    });
    if (posA && posB) {
      html += `<b>Focus ships:</b><br>
        ${posA.name} [COG:${Math.round(posA.cog)}, SOG:${posA.sog.toFixed(1)}]<br>
        ${posB.name} [COG:${Math.round(posB.cog)}, SOG:${posB.sog.toFixed(1)}]<br>
      <hr/>`;
    }
  }

  // Wypisujemy wszystkie statki w tej klatce
  ships.forEach(s => {
    const nm = s.name || s.mmsi;
    html += `
      <div>
        <b>${nm}</b>
        [COG:${Math.round(s.cog)}, SOG:${s.sog.toFixed(1)} kn, L:${s.ship_length||"?"}]
      </div>
    `;
  });
  leftPanel.innerHTML = html;
}

// ---------------------------------------
// G) exitSituationView
// ---------------------------------------
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

// ---------------------------------------
// cleanup
// ---------------------------------------
function clearAll() {
  umbrellasMap = {};
  subScenariosMap = {};
  scenarioMarkers.forEach(m => map.removeLayer(m));
  scenarioMarkers = [];
  document.getElementById('collision-list').innerHTML = '';
}

// ---------------------------------------
// bottom UI (animacja)
// ---------------------------------------
function setupBottomUI() {
  document.getElementById('playPause').addEventListener('click', () => {
    if (isPlaying) stopAnimation();
    else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click', () => stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', () => stepAnimation(-1));
  document.getElementById('closePlayback').addEventListener('click', () => {
    exitSituationView();
  });
}

// Start
document.addEventListener('DOMContentLoaded', initHistoryApp);