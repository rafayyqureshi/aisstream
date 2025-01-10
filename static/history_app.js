document.addEventListener('DOMContentLoaded', initHistoryApp);

let map;
let allCollisions = []; // przechowujemy kolizje z wielu plików GCS
let collisionMarkers = [];

let currentDay = 0;
const minDay = -7;
const maxDay = 0;
let cpaFilter = 0.5;

// animacja
let isPlaying = false;
let animationData = [];
let animationIndex = 0;
let animationInterval = null;
let inSituationView = false;
let shipMarkersOnMap = [];

function initHistoryApp() {
  map = initSharedMap('map'); // z common.js

  setupUI();
  fetchFileListAndLoadCollisions();
}

// ----- UI -----
function setupUI() {
  document.getElementById('prevDay').addEventListener('click', () => {
    if (currentDay > minDay) {
      currentDay--;
      updateDayLabel();
      filterAndDisplayCollisions();
    }
  });
  document.getElementById('nextDay').addEventListener('click', () => {
    if (currentDay < maxDay) {
      currentDay++;
      updateDayLabel();
      filterAndDisplayCollisions();
    }
  });

  const cpaSlider = document.getElementById('cpaFilter');
  cpaSlider.addEventListener('input', e => {
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    filterAndDisplayCollisions();
  });

  // animacja
  document.getElementById('playPause').addEventListener('click', () => {
    if (isPlaying) stopAnimation(); 
    else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click', () => {
    stepAnimation(1);
  });
  document.getElementById('stepBack').addEventListener('click', () => {
    stepAnimation(-1);
  });

  updateDayLabel();
}

function updateDayLabel() {
  const now = new Date();
  let targetDate = new Date(now);
  targetDate.setDate(now.getDate() + currentDay);
  const dateStr = targetDate.toISOString().slice(0, 10);

  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
  document.getElementById('prevDay').disabled = (currentDay <= minDay);
  document.getElementById('nextDay').disabled = (currentDay >= maxDay);
}

// ----- POBIERANIE LISTY Z GCS -----
function fetchFileListAndLoadCollisions() {
  fetch('/history_filelist')
    .then(res => {
      if (!res.ok) throw new Error(`HTTP status ${res.status} - ${res.statusText}`);
      return res.json();
    })
    .then(json => {
      if (!json.files) {
        throw new Error("No 'files' in /history_filelist response");
      }
      loadAllFiles(json.files);
    })
    .catch(err => {
      console.error("Błąd fetchFileList:", err);
      showErrorInCollisionList(`Error fetching file list: ${err.message}`);
    });
}

/** 
 * Wypisuje błąd w #collision-list 
 */
function showErrorInCollisionList(msg) {
  const clist = document.getElementById('collision-list');
  clist.innerHTML = `<div class="collision-item" style="color:red;">
    <b>Error:</b> ${msg}
  </div>`;
}

/**
 * Ładujemy wszystkie pliki z listy w łańcuchu Promise,
 * a następnie filtrujemy i wyświetlamy.
 */
function loadAllFiles(fileArr) {
  if (!fileArr.length) {
    showErrorInCollisionList("No files found (history is empty).");
    return;
  }
  let chain = Promise.resolve();
  fileArr.forEach(fileObj => {
    chain = chain.then(() => loadOneFile(fileObj.name));
  });
  chain.then(() => {
    console.log("Załadowano wszystkie pliki. allCollisions.length:", allCollisions.length);
    filterAndDisplayCollisions();
  });
}

function loadOneFile(fileName) {
  const url = `/history_file?file=${encodeURIComponent(fileName)}`;
  return fetch(url)
    .then(res => {
      if (!res.ok) {
        throw new Error(`HTTP status ${res.status} for file ${fileName}`);
      }
      return res.json();
    })
    .then(jsonData => {
      if (!jsonData.collisions) {
        console.warn("Brak .collisions w pliku:", fileName, jsonData);
        return; // pomijamy
      }
      allCollisions.push(...jsonData.collisions);
    })
    .catch(err => {
      console.error("Błąd loadOneFile:", fileName, err);
      // nie przerywamy
    });
}

// ---- FILTR I PREZENTACJA NA MAPIE ----
function filterAndDisplayCollisions() {
  clearCollisions();

  if (!allCollisions || !allCollisions.length) {
    const clist = document.getElementById('collision-list');
    clist.innerHTML = `<div class="collision-item"><i>No collisions loaded</i></div>`;
    return;
  }

  // Filtr day
  const now = new Date();
  let targetDate = new Date(now);
  targetDate.setDate(now.getDate() + currentDay);
  let dateStr = targetDate.toISOString().slice(0, 10);

  // Filtr cpa
  let finalList = allCollisions.filter(c => {
    // c.timestamp to moment kolizji
    if (!c.timestamp) return false;
    let dayOfCollision = c.timestamp.slice(0, 10);
    if (dayOfCollision !== dateStr) return false;
    // cpa
    let cpaVal = c.cpa || 99;
    return cpaVal <= cpaFilter;
  });

  displayCollisions(finalList);
}

function clearCollisions() {
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];
  let clist = document.getElementById('collision-list');
  if (clist) clist.innerHTML = '';
}

/**
 * Wyświetla listę kolizji w panelu i rysuje marker (tylko 1) na mapie
 */
function displayCollisions(collisions) {
  const clist = document.getElementById('collision-list');
  if (!collisions.length) {
    clist.innerHTML = `<div class="collision-item"><i>No collisions for selected filters</i></div>`;
    return;
  }

  // aby nie tworzyć duplikatów, używamy collision_id
  let usedIds = new Set();

  collisions.forEach(c => {
    if (!c.collision_id) return;
    if (usedIds.has(c.collision_id)) return;
    usedIds.add(c.collision_id);

    // nazwy statków
    let shipA = c.mmsi_a;
    let shipB = c.mmsi_b;
    // jeśli pipeline history zapisał nazwy statków w innym polu, to tu można je wstawić
    // tu jest minimalny fallback:
    shipA = c.ship1_name || `MMSI:${c.mmsi_a}`;
    shipB = c.ship2_name || `MMSI:${c.mmsi_b}`;

    let distNm = (c.cpa || 0).toFixed(2);
    let dtStr = c.timestamp ? new Date(c.timestamp).toLocaleTimeString('en-GB') : '---';

    // Tworzymy item
    let div = document.createElement('div');
    div.classList.add('collision-item');
    div.innerHTML = `
      <div class="collision-header" style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <strong>${shipA} - ${shipB}</strong><br>
          Min Dist: ${distNm} nm @ ${dtStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;

    clist.appendChild(div);

    div.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(c);
    });

    // Marker
    let latC = ((c.latitude_a||0) + (c.latitude_b||0))/2;
    let lonC = ((c.longitude_a||0) + (c.longitude_b||0))/2;
    const collisionIcon = L.divIcon({
      className: '',
      html: `
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <circle cx="0" cy="0" r="8" fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="3" text-anchor="middle" font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize: [24,24],
      iconAnchor: [12,12]
    });
    let tip = `Collision: ${shipA}&${shipB}\nDist: ${distNm} nm @ ${dtStr}`;
    let marker = L.marker([latC, lonC], {icon: collisionIcon})
      .bindTooltip(tip.replace(/\n/g, "<br>"), {sticky:true})
      .on('click', () => zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

/**
 * Po kliknięciu w lupę / marker -> wczytujemy plik JSON z klatkami i animujemy
 */
function zoomToCollision(c) {
  // minimalne zbliżenie
  let latC = ((c.latitude_a||0) + (c.latitude_b||0))/2;
  let lonC = ((c.longitude_a||0) + (c.longitude_b||0))/2;
  map.setView([latC, lonC], 10);

  document.getElementById('left-panel').style.display='block';
  document.getElementById('bottom-center-bar').style.display='block';

  stopAnimation();
  animationData=[];
  animationIndex=0;

  let url=`/history_file?file=${encodeURIComponent(c.collision_file||'')}`;
  // ALE: pipeline history często generuje "collisions_yyyymmddHHMMSS.json" 
  // c.collision_file mogłoby być kluczem do pliku. 
  // Ewentualnie c.collision_id -> kluczem w aggregatorze. 
  // Twój pipeline w pliku:
  // {
  //   collisions: [{ collision_id, frames:[...]}]
  // }
  // w `c` może być brak 'collision_file'.
  if(!c.collision_file) {
    console.warn("Brak collision_file w obiekcie kolizji. Nie można wczytać animacji.");
    return;
  }

  fetch(url)
    .then(r=>{
      if(!r.ok) throw new Error(`HTTP ${r.status} - cannot load collision frames`);
      return r.json();
    })
    .then(jsonData=>{
      if(!jsonData.collisions){
        console.warn("Brak .collisions w pliku animacji");
        return;
      }
      // Znajdź w pliku collisions[] nasz collision_id
      let theOne = jsonData.collisions.find(x=> x.collision_id===c.collision_id);
      if(!theOne){
        console.warn("Nie znaleziono collision_id w pliku");
        return;
      }
      if(!theOne.frames){
        console.warn("Brak frames");
        return;
      }
      inSituationView=true;
      animationData=theOne.frames;
      animationIndex=0;
      updateMapFrame();
    })
    .catch(err=>{
      console.error("Błąd zoomToCollision fetch:", err);
    });
}

// ---- ANIMACJA ----
function startAnimation(){
  if(!animationData||!animationData.length)return;
  isPlaying=true;
  document.getElementById('playPause').textContent='Pause';
  animationInterval=setInterval(()=>stepAnimation(1),1000);
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

/**
 * Uaktualnia klatkę
 */
function updateMapFrame(){
  let frameIndicator=document.getElementById('frameIndicator');
  frameIndicator.textContent=`${animationIndex+1}/${animationData.length}`;
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];

  if(!animationData||!animationData.length)return;
  let frame=animationData[animationIndex];
  let ships=frame.shipPositions||[];

  ships.forEach(s=>{
    let fakeShipData={
      cog:s.cog||0,
      ship_length:s.ship_length||0
    };
    let icon=createShipIcon(fakeShipData,false);
    let marker=L.marker([s.lat,s.lon],{icon});
    let nm=s.name||`MMSI:${s.mmsi}`;
    let sogV=(s.sog||0).toFixed(1);
    let cogDeg=Math.round(s.cog||0);
    let tip=`
      <b>${nm}</b><br>
      SOG:${sogV} kn, COG:${cogDeg}°<br>
      Len:${s.ship_length||0}
    `;
    marker.bindTooltip(tip,{sticky:true});
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });
}