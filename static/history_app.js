// ==========================
// history_app.js
// ==========================
let map;

// Mapa “umbrella” -> obiekt = { _parent:true, scenario_id, ships_involved, collisions_count, ... }
// Mapa sub-scenariuszy -> array obiektów = { _parent:false, collision_id, frames, icon_lat, icon_lon, ... }
let umbrellasMap = {};    // klucz: scenario_id => obiekt umbrella
let subScenariosMap = {}; // klucz: scenario_id => array sub-scenariuszy

let scenarioMarkers = []; // ikony sub-scenariuszy na mapie
let inSituationView = false;
let isPlaying = false;
let animationData = null; 
let animationIndex = 0;
let animationInterval = null;
let shipMarkersOnMap = [];
let selectedScenario = null;

// Obsługa dni wstecz
let currentDay = 0;
const minDay = -7;
const maxDay = 0;

function initHistoryApp() {
  // 1) Inicjalizacja mapy (common.js)
  map = initSharedMap('map');

  // 2) Obsługa UI (dni, animacja)
  setupDayUI();
  setupBottomUI();

  // 3) Start
  fetchFileListAndLoadScenarios();
}

// ---------------------------------------
// A) Obsługa day offset
// ---------------------------------------
function setupDayUI() {
  document.getElementById('prevDay').addEventListener('click', () => {
    if(currentDay > minDay) {
      currentDay--;
      updateDayLabel();
      clearAll();
      fetchFileListAndLoadScenarios();
    }
  });
  document.getElementById('nextDay').addEventListener('click', () => {
    if(currentDay < maxDay) {
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
  const d = new Date(now);
  d.setDate(d.getDate() + currentDay);
  const dateStr = d.toISOString().slice(0, 10);
  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
}

// ---------------------------------------
// B) Pobieranie listy plików GCS + wczytywanie
// ---------------------------------------
function fetchFileListAndLoadScenarios() {
  const url = `/history_filelist?days=${7 + currentDay}`;

  fetch(url)
    .then(res => {
      if(!res.ok) throw new Error(`HTTP ${res.status} - ${res.statusText}`);
      return res.json();
    })
    .then(data => {
      const files = data.files || [];
      if (files.length === 0) {
        console.log("Brak plików GCS (day offset =", currentDay, ")");
        updateCollisionList();
        return;
      }
      return loadAllScenarioFiles(files);
    })
    .catch(err => console.error("Błąd fetchFileList:", err));
}

function loadAllScenarioFiles(fileList) {
  // Czyścimy
  umbrellasMap = {};
  subScenariosMap = {};
  scenarioMarkers.forEach(m => map.removeLayer(m));
  scenarioMarkers = [];

  // Sekwencyjnie wczytujemy
  const promises = fileList.map(f => {
    const fname = f.name;
    return fetch(`/history_file?file=${encodeURIComponent(fname)}`)
      .then(r => {
        if(!r.ok) throw new Error(`HTTP ${r.status} - ${r.statusText}`);
        return r.json();
      })
      .then(jsonData => {
        const arr = jsonData.scenarios || [];
        arr.forEach(obj => {
          // Rozróżniamy: umbrella (_parent == true) vs sub-scenario (_parent == false)
          if(obj._parent) {
            // Zapamiętujemy "umbrella" w umbrellasMap
            umbrellasMap[obj.scenario_id] = obj;
          } else {
            // Sub-scenario
            let sid = obj.scenario_id;
            if(!subScenariosMap[sid]) subScenariosMap[sid] = [];
            subScenariosMap[sid].push(obj);
          }
        });
      })
      .catch(err => console.error("Błąd loadOneFile:", err));
  });

  return Promise.all(promises)
    .then(() => {
      console.log("Umbrellas:", Object.keys(umbrellasMap).length,
                  "Sub-scenarios:", Object.keys(subScenariosMap).length);
      updateCollisionList();
      drawScenarioMarkers();
    });
}

// ---------------------------------------
// C) Budowa listy kolizji w panelu (umbrella -> sub-scenarios)
// ---------------------------------------
function updateCollisionList() {
  const listDiv = document.getElementById('collision-list');
  listDiv.innerHTML = '';

  // Zbierz *wszystkie* scenario_id z umbrellas + sub-scenarios
  let allIDs = new Set([...Object.keys(umbrellasMap), ...Object.keys(subScenariosMap)]);
  if(allIDs.size === 0) {
    let noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>No collision scenarios found</i>';
    listDiv.appendChild(noItem);
    return;
  }
  const sortedIDs = Array.from(allIDs).sort();

  // Tworzymy węzły
  sortedIDs.forEach(sid => {
    const umbrella = umbrellasMap[sid];   // undefined jeśli brak
    const subs = subScenariosMap[sid] || [];

    // Umbrella header
    let headerText = `Scenario: ${sid}, (subs:${subs.length})`;
    if(umbrella) {
      const shipsCount = umbrella.ships_involved?.length || 0;
      let cCount = umbrella.collisions_count || subs.length;
      headerText = `Umbrella ${sid} [ships:${shipsCount}, collisions:${cCount}]`;
    }

    const blockDiv = document.createElement('div');
    blockDiv.classList.add('umbrella-block');

    // Nagłówek umbrella
    const headerDiv = document.createElement('div');
    headerDiv.classList.add('umbrella-header');
    headerDiv.textContent = headerText;

    const expandBtn = document.createElement('button');
    expandBtn.textContent = '▼';
    headerDiv.appendChild(expandBtn);

    blockDiv.appendChild(headerDiv);

    // Podlista sub-scenariuszy
    const subListDiv = document.createElement('div');
    subListDiv.style.display = 'none';

    expandBtn.addEventListener('click', () => {
      if(subListDiv.style.display === 'none') {
        subListDiv.style.display = 'block';
        expandBtn.textContent = '▲';
      } else {
        subListDiv.style.display = 'none';
        expandBtn.textContent = '▼';
      }
    });

    // Wypełniamy sub-scenariusze
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
// D) Rysowanie ikon sub-scenariuszy
// ---------------------------------------
function drawScenarioMarkers() {
  scenarioMarkers.forEach(m => map.removeLayer(m));
  scenarioMarkers = [];

  // subScenariosMap[sid] => array
  for(let sid in subScenariosMap) {
    let arr = subScenariosMap[sid];
    arr.forEach(sub => {
      const frames = sub.frames || [];
      if(frames.length === 0) return;

      // Pozycja minimalnego rozminięcia
      let latC = sub.icon_lat, lonC = sub.icon_lon;

      // fallback => 1. klatka
      if(latC == null || lonC == null) {
        const ships0 = frames[0].shipPositions||[];
        if(ships0.length>0){
          let sumLat=0, sumLon=0, count=0;
          ships0.forEach(s=>{
            sumLat+=s.lat; sumLon+=s.lon; count++;
          });
          if(count>0){
            latC=sumLat/count; lonC=sumLon/count;
          }
        }
      }
      if(latC == null || lonC == null) return;

      // Ikona
      const iconHTML=`
        <svg width="20" height="20" viewBox="-10 -10 20 20">
          <circle cx="0" cy="0" r="8" fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="3" text-anchor="middle" font-size="8" fill="red">C</text>
        </svg>
      `;
      const scenarioIcon=L.divIcon({
        className:'',
        html:iconHTML,
        iconSize:[20,20],
        iconAnchor:[10,10]
      });
      const marker = L.marker([latC, lonC], {icon:scenarioIcon})
        .bindTooltip(sub.title||sub.collision_id, {direction:'top'})
        .on('click', ()=> onSelectScenario(sub));
      marker.addTo(map);
      scenarioMarkers.push(marker);
    });
  }
}

// ---------------------------------------
// E) Otwieranie sub-scenariusza -> animacja
// ---------------------------------------
function onSelectScenario(subScenario){
  selectedScenario=subScenario;
  const frames= subScenario.frames||[];
  if(frames.length===0){
    console.warn("Scenario has no frames");
    return;
  }

  // Zoom do 1 klatki
  const ships0= frames[0].shipPositions||[];
  if(ships0.length>0){
    const latLngs= ships0.map(s=>[s.lat,s.lon]);
    const bounds= L.latLngBounds(latLngs);
    map.fitBounds(bounds,{padding:[30,30],maxZoom:13});
  }

  loadScenarioAnimation(subScenario);
}

function loadScenarioAnimation(subScenario){
  animationData=subScenario.frames||[];
  animationIndex=0;
  stopAnimation();
  inSituationView=true;

  document.getElementById('left-panel').style.display='block';
  document.getElementById('bottom-center-bar').style.display='block';

  updateMapFrame();
}

// ---------------------------------------
// F) Animacja
// ---------------------------------------
function startAnimation(){
  if(!animationData||animationData.length===0)return;
  isPlaying=true;
  document.getElementById('playPause').textContent='Pause';
  animationInterval=setInterval(()=> stepAnimation(1),1000);
}
function stopAnimation(){
  isPlaying=false;
  document.getElementById('playPause').textContent='Play';
  if(animationInterval) clearInterval(animationInterval);
  animationInterval=null;
}
function stepAnimation(step){
  animationIndex+=step;
  if(animationIndex<0) animationIndex=0;
  if(animationIndex>=animationData.length) animationIndex=animationData.length-1;
  updateMapFrame();
}

function updateMapFrame(){
  const frameIndicator=document.getElementById('frameIndicator');
  frameIndicator.textContent=`${animationIndex+1}/${animationData.length}`;

  // Czyścimy poprzednie
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];

  if(!animationData||animationData.length===0)return;
  const frame= animationData[animationIndex];
  const ships= frame.shipPositions||[];

  // 1) Rysowanie statków
  ships.forEach(s=>{
    const nm= s.name|| s.mmsi;
    const mk= L.marker([s.lat, s.lon], {
      icon: createShipIcon(s,false) // z common.js
    });
    const tooltipHtml= `
      <b>${nm}</b><br>
      COG: ${Math.round(s.cog)}°, SOG: ${s.sog.toFixed(1)} kn<br>
      L: ${s.ship_length||"??"}
    `;
    mk.bindTooltip(tooltipHtml, {direction:'top', sticky:true});
    mk.addTo(map);
    shipMarkersOnMap.push(mk);
  });

  // 2) Wypełniamy panel (po lewej)
  const leftPanel=document.getElementById('selected-ships-info');
  leftPanel.innerHTML='';
  const pairInfo=document.getElementById('pair-info');
  pairInfo.innerHTML='';

  let html=`<b>Frame time:</b> ${frame.time}<br>`;
  if(frame.focus_dist!==undefined) {
    html+=`<b>Focus Dist:</b> ${frame.focus_dist.toFixed(3)} nm<br>`;
  }
  if(frame.delta_minutes!==undefined) {
    html+=`<b>Time to min approach:</b> ${frame.delta_minutes} min<br>`;
  }
  html+=`<hr/>`;

  // Ewentualnie wyróżnienie pary
  if(selectedScenario && selectedScenario.focus_mmsi) {
    const [mA,mB]= selectedScenario.focus_mmsi;
    let posA=null, posB=null;
    ships.forEach(s=>{
      if(s.mmsi===mA) posA=s;
      if(s.mmsi===mB) posB=s;
    });
    if(posA && posB){
      html+=`<div><b>Focus Ships:</b><br>
        ${posA.name} [COG:${Math.round(posA.cog)}, SOG:${posA.sog.toFixed(1)}]<br>
        ${posB.name} [COG:${Math.round(posB.cog)}, SOG:${posB.sog.toFixed(1)}]<br>
      </div><hr/>`;
    }
  }

  // Lista wszystkich statków
  ships.forEach(s=>{
    const nm= s.name||s.mmsi;
    html+= `
      <div>
        <b>${nm}</b> 
        [COG:${Math.round(s.cog)}, SOG:${s.sog.toFixed(1)} kn, L:${s.ship_length||"?"}]
      </div>
    `;
  });

  leftPanel.innerHTML= html;
}

// ---------------------------------------
// G) Zakończenie odtwarzania
// ---------------------------------------
function exitSituationView(){
  inSituationView=false;
  document.getElementById('left-panel').style.display='none';
  document.getElementById('bottom-center-bar').style.display='none';
  stopAnimation();
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];
  animationData=null;
  animationIndex=0;
}

// ---------------------------------------
// Porządki
// ---------------------------------------
function clearAll(){
  umbrellasMap={};
  subScenariosMap={};
  scenarioMarkers.forEach(m=>map.removeLayer(m));
  scenarioMarkers=[];
  document.getElementById('collision-list').innerHTML='';
}

// ---------------------------------------
// bottom panel (animacja)
// ---------------------------------------
function setupBottomUI(){
  document.getElementById('playPause').addEventListener('click',()=>{
    if(isPlaying) stopAnimation();
    else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click',()=> stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click',()=> stepAnimation(-1));
  document.getElementById('closePlayback').addEventListener('click', ()=>{
    exitSituationView();
  });
}

// Uruchamiamy
document.addEventListener('DOMContentLoaded', initHistoryApp);