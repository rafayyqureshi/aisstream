// ==========================
// history_app.js
// ==========================

let map;
let scenarioMarkers = [];    // Ikony na mapie (sub-scenariusze)
let currentScenarios = [];   // list[a, b, c]
let scenarioGroups = {};     // klucze = np. "20250109_14"
let selectedScenario = null; // aktualnie wybrany sub-scenariusz
let animationData = null;
let animationIndex = 0;
let animationInterval = null;
let isPlaying = false;

let shipMarkersOnMap = [];
let inSituationView = false;

// Dni
let currentDay = 0;
const minDay = -7;
const maxDay = 0;

function initHistoryApp() {
  // 1) Mapa
  map = initSharedMap('map');

  // 2) UI
  setupDayUI();
  setupBottomUI();

  // 3) Start
  fetchFileListAndLoadScenarios();

  // Klik w mapę => ewentualnie wychodzimy z animacji
  map.on('click', ()=>{
    if(inSituationView) exitSituationView();
  });
}

// -------------------------
// Obsługa day offset
// -------------------------
function setupDayUI(){
  document.getElementById('prevDay').addEventListener('click', ()=>{
    if(currentDay>minDay){
      currentDay--;
      updateDayLabel();
      clearAllScenarios();
      fetchFileListAndLoadScenarios();
    }
  });
  document.getElementById('nextDay').addEventListener('click', ()=>{
    if(currentDay<maxDay){
      currentDay++;
      updateDayLabel();
      clearAllScenarios();
      fetchFileListAndLoadScenarios();
    }
  });
  updateDayLabel();
}

function updateDayLabel(){
  const now = new Date();
  const d = new Date(now);
  d.setDate(d.getDate()+currentDay);
  const dateStr = d.toISOString().slice(0,10);
  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
}

// -------------------------
// 1) Pobieranie listy plików z GCS
// -------------------------
function fetchFileListAndLoadScenarios(){
  const url = `/history_filelist?days=${7+currentDay}`;

  fetch(url)
    .then(res=>{
      if(!res.ok) throw new Error(`HTTP ${res.status} - ${res.statusText}`);
      return res.json();
    })
    .then(data=>{
      const files = data.files||[];
      if(files.length===0){
        console.log(`Brak plików GCS dla dayOffset=${currentDay}`);
        updateCollisionListUI();
        return;
      }
      return loadAllScenarioFiles(files);
    })
    .catch(err=>{
      console.error("Błąd fetchFileList:", err);
    });
}

function loadAllScenarioFiles(fileList){
  currentScenarios=[];
  scenarioGroups={};
  scenarioMarkers.forEach(m=>map.removeLayer(m));
  scenarioMarkers=[];

  const promises = fileList.map(f=>{
    const fname = f.name;
    return fetch(`/history_file?file=${encodeURIComponent(fname)}`)
      .then(r=>{
        if(!r.ok) throw new Error(`HTTP ${r.status} - ${r.statusText}`);
        return r.json();
      })
      .then(jsonData=>{
        if(!jsonData.scenarios){
          console.warn(`Plik ${fname} nie zawiera "scenarios". Pomijam.`);
          return;
        }
        jsonData.scenarios.forEach(sc=>{
          sc.fileName = fname;
          currentScenarios.push(sc);
        });
      })
      .catch(err=>{
        console.error("Błąd loadOneFile:", err);
      });
  });

  return Promise.all(promises)
    .then(()=>{
      console.log("Załadowano pliki GCS. Liczba sub-scenariuszy:", currentScenarios.length);
      groupScenariosByHour();
      updateCollisionListUI();
      drawScenarioMarkers();
    });
}

function groupScenariosByHour(){
  scenarioGroups={};
  currentScenarios.forEach(sc=>{
    const fname = sc.fileName||"";
    // np. multiship_20250109_14.json => klucz "20250109_14"
    const match = fname.match(/(\d{8}_\d{2})\.json$/);
    let hourKey="unknown";
    if(match){
      hourKey=match[1];
    }
    if(!scenarioGroups[hourKey]){
      scenarioGroups[hourKey]=[];
    }
    scenarioGroups[hourKey].push(sc);
  });
}

// -------------------------
// 2) Wyświetlanie listy w panelu
// -------------------------
function updateCollisionListUI(){
  const listDiv = document.getElementById('collision-list');
  listDiv.innerHTML='';

  const hourKeys = Object.keys(scenarioGroups).sort();
  if(hourKeys.length===0){
    let noItem=document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML='<i>No collision scenarios found</i>';
    listDiv.appendChild(noItem);
    return;
  }

  hourKeys.forEach(hk=>{
    const hourBlock = document.createElement('div');
    hourBlock.classList.add('hour-block');

    const hourTitle = document.createElement('div');
    hourTitle.classList.add('hour-title');
    hourTitle.textContent=`Hour: ${hk}`;
    hourBlock.appendChild(hourTitle);

    scenarioGroups[hk].forEach(sc=>{
      const item=document.createElement('div');
      item.classList.add('collision-item');

      const scTitle = sc.title || sc.collision_id || sc.scenario_id || "Unknown scenario";
      const framesCount = (sc.frames||[]).length;

      item.innerHTML=`
        <strong>${scTitle}</strong><br>
        Frames: ${framesCount}
        <button class="zoom-button">🔍</button>
      `;
      item.querySelector('.zoom-button').addEventListener('click', ()=>{
        onSelectScenario(sc);
      });

      hourBlock.appendChild(item);
    });

    listDiv.appendChild(hourBlock);
  });
}

// -------------------------
// 3) Rysowanie ikon scenariuszy
// -------------------------
function drawScenarioMarkers(){
  scenarioMarkers.forEach(m=>map.removeLayer(m));
  scenarioMarkers=[];

  Object.keys(scenarioGroups).forEach(hk=>{
    scenarioGroups[hk].forEach(sc=>{
      const frames = sc.frames||[];
      if(frames.length===0)return;
      const ships0 = frames[0].shipPositions||[];
      if(ships0.length===0)return;

      let latSum=0, lonSum=0, count=0;
      ships0.forEach(s=>{
        latSum+=s.lat; lonSum+=s.lon; count++;
      });
      if(count===0)return;
      const latC = latSum/count, lonC=lonSum/count;

      // Ikona
      const iconHTML=`
        <svg width="20" height="20" viewBox="-10 -10 20 20">
          <circle cx="0" cy="0" r="8" fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="3" text-anchor="middle" font-size="8" fill="red">H</text>
        </svg>
      `;
      const scenarioIcon = L.divIcon({
        className:'',
        html: iconHTML,
        iconSize:[20,20],
        iconAnchor:[10,10]
      });

      const title = sc.title || sc.collision_id || sc.scenario_id;
      const marker=L.marker([latC, lonC], {icon:scenarioIcon})
        .bindTooltip(`${title}`, {direction:'top'})
        .on('click', ()=>onSelectScenario(sc));

      marker.addTo(map);
      scenarioMarkers.push(marker);
    });
  });
}

// -------------------------
// 4) Wybór scenariusza
// -------------------------
function onSelectScenario(scenario){
  selectedScenario=scenario;
  const frames= scenario.frames||[];
  if(frames.length===0){
    console.warn("Scenario has no frames");
    return;
  }

  const ships0= frames[0].shipPositions||[];
  if(ships0.length===0)return;
  let latLngs= ships0.map(s=>[s.lat, s.lon]);
  let bounds= L.latLngBounds(latLngs);
  map.fitBounds(bounds, {padding:[30,30], maxZoom:13});

  loadScenarioAnimation(scenario);
}

function loadScenarioAnimation(scenario){
  animationData= scenario.frames||[];
  animationIndex=0;
  stopAnimation();
  inSituationView=true;

  document.getElementById('left-panel').style.display='block';
  document.getElementById('bottom-center-bar').style.display='block';

  updateMapFrame();
}

// -------------------------
// 5) Animacja
// -------------------------
function startAnimation(){
  if(!animationData||animationData.length===0)return;
  isPlaying=true;
  document.getElementById('playPause').textContent='Pause';
  animationInterval=setInterval(()=>stepAnimation(1), 1000);
}
function stopAnimation(){
  isPlaying=false;
  document.getElementById('playPause').textContent='Play';
  if(animationInterval)clearInterval(animationInterval);
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

  // czyścimy stare
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];

  if(!animationData||animationData.length===0)return;
  const frame = animationData[animationIndex];
  const ships = frame.shipPositions||[];

  ships.forEach(s=>{
    let marker=L.marker([s.lat, s.lon],{
      icon:createShipIcon(s,false) // z common.js
    });
    let nm=s.name||s.mmsi;
    let tt=`
      <b>${nm}</b><br>
      COG:${Math.round(s.cog)}°, SOG:${s.sog.toFixed(1)} kn<br>
      L:${s.ship_length||'?'}
    `;
    marker.bindTooltip(tt, {direction:'top', sticky:true});
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  // Panel
  const leftPanel=document.getElementById('selected-ships-info');
  leftPanel.innerHTML='';
  const pairInfo=document.getElementById('pair-info');
  pairInfo.innerHTML='';

  let html=`
    <b>Frame time:</b> ${frame.time}<br>
  `;
  if(frame.focus_dist!==undefined){
    html+=`<b>Focus dist:</b> ${frame.focus_dist?.toFixed(2)} nm<br>`;
  }
  if(frame.delta_minutes!==undefined){
    html+=`<b>Time to min approach:</b> ${frame.delta_minutes} min<br>`;
  }
  html+=`<hr/>`;

  ships.forEach(s=>{
    let nm=s.name||s.mmsi;
    html+=`
      <div>
        <b>${nm}</b> 
        [COG:${Math.round(s.cog)}, SOG:${s.sog.toFixed(1)} kn, L:${s.ship_length||'?'}]
      </div>
    `;
  });

  leftPanel.innerHTML=html;
}

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

function clearAllScenarios(){
  currentScenarios=[];
  scenarioGroups={};
  scenarioMarkers.forEach(m=>map.removeLayer(m));
  scenarioMarkers=[];
  document.getElementById('collision-list').innerHTML='';
}

// -------------------------
// UI (dolny panel animacji)
// -------------------------
function setupBottomUI(){
  document.getElementById('playPause').addEventListener('click', ()=>{
    if(isPlaying) stopAnimation();
    else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click', ()=>stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', ()=>stepAnimation(-1));
}

// Uruchamiamy
document.addEventListener('DOMContentLoaded', initHistoryApp);