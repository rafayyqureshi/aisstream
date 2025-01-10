document.addEventListener('DOMContentLoaded', initHistoryApp);

/**
 * Zmienne globalne modułu history.
 */
let map;
let allCollisions = [];   // przechowuje kolizje z wielu plików GCS
let collisionMarkers = [];

let currentDay = 0;  // offset dnia
const minDay = -7;
const maxDay = 0;
let cpaFilter = 0.5;

// Animacja
let isPlaying = false;
let animationData = [];
let animationIndex = 0;
let animationInterval = null;
let inSituationView = false;
let shipMarkersOnMap = [];

/**
 * Funkcja inicjalizacyjna wywoływana po DOMContentLoaded.
 */
function initHistoryApp() {
  // 1) Mapa z common.js
  map = initSharedMap('map');

  // 2) Setup UI
  setupUI();

  // 3) Pobierz listę plików z GCS i wczytaj kolizje
  fetchFileListAndLoadCollisions();
}

/**
 * Ustawienia interfejsu: day +/- i cpaFilter, animacja.
 */
function setupUI() {
  // Day -
  document.getElementById('prevDay').addEventListener('click', ()=>{
    if (currentDay > minDay) {
      currentDay--;
      updateDayLabel();
      filterAndDisplayCollisions();
    }
  });
  // Day +
  document.getElementById('nextDay').addEventListener('click', ()=>{
    if (currentDay < maxDay) {
      currentDay++;
      updateDayLabel();
      filterAndDisplayCollisions();
    }
  });

  // cpaFilter slider
  const cpaSlider = document.getElementById('cpaFilter');
  cpaSlider.addEventListener('input', e => {
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    filterAndDisplayCollisions();
  });

  // Animacja (play/pause)
  document.getElementById('playPause').addEventListener('click', ()=>{
    if (isPlaying) stopAnimation(); else startAnimation();
  });
  // Step +/- 
  document.getElementById('stepForward').addEventListener('click', ()=> stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', ()=> stepAnimation(-1));

  // Ustaw label
  updateDayLabel();
}

/**
 * Ustaw etykietę z currentDay, dezaktywuj przyciski w razie min/max.
 */
function updateDayLabel() {
  const now = new Date();
  let targetDate = new Date(now);
  targetDate.setDate(now.getDate() + currentDay);
  const dateStr = targetDate.toISOString().slice(0,10);

  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
  document.getElementById('prevDay').disabled = (currentDay <= minDay);
  document.getElementById('nextDay').disabled = (currentDay >= maxDay);
}

/**
 * Pobieramy listę plików GCS z endpointu, np. /history_filelist
 */
function fetchFileListAndLoadCollisions() {
  fetch('/history_filelist')
    .then(r=>r.json())
    .then(data => {
      if (!data.files) {
        console.warn("Brak 'files' w /history_filelist");
        return;
      }
      // Ładujemy wszystkie pliki
      loadAllFiles(data.files);
    })
    .catch(err => console.error("Błąd fetchFileList:", err));
}

/**
 * Sekwencyjne ładowanie wszystkich plików JSON i łączenie kolizji do allCollisions.
 */
function loadAllFiles(fileList) {
  let promise = Promise.resolve();
  fileList.forEach(fileName => {
    promise = promise.then(()=> loadOneFile(fileName));
  });
  promise.then(()=>{
    console.log("Wczytano wszystkie pliki GCS. Liczba kolizji:", allCollisions.length);
    // Po wszystkim
    filterAndDisplayCollisions();
  });
}

function loadOneFile(fileName) {
  // /history_data?file=someFileName
  const url = `/history_data?file=${encodeURIComponent(fileName)}`;
  return fetch(url)
    .then(r=>r.json())
    .then(jsonData => {
      // Zakładamy, że jsonData: { collisions: [ {...}, {...} ] }
      if (!jsonData.collisions) {
        console.warn("Brak collisions w pliku:", fileName);
        return;
      }
      allCollisions.push(...jsonData.collisions);
    })
    .catch(err => console.error("Błąd loadOneFile:", fileName, err));
}

/**
 * Filtrowanie i wyświetlanie kolizji w panelu i na mapie.
 */
function filterAndDisplayCollisions() {
  clearCollisions();

  if (!allCollisions || allCollisions.length===0) {
    // Nic do wyświetlenia
    const list = document.getElementById('collision-list');
    list.innerHTML = '<div class="collision-item"><i>No collisions loaded</i></div>';
    return;
  }

  // Filtr wg. day offset (data kolizji) i cpa
  const now = new Date();
  let targetDate = new Date(now);
  targetDate.setDate(now.getDate() + currentDay);
  let targetStr = targetDate.toISOString().slice(0,10);

  let filtered = allCollisions.filter(c => {
    if (!c.timestamp) return false;
    let dtStr = c.timestamp.slice(0,10);
    if (dtStr !== targetStr) return false;

    if (c.cpa > cpaFilter) return false;
    return true;
  });

  displayCollisions(filtered);
}

/**
 * Czyści kolizyjne markery z mapy i listę w panelu.
 */
function clearCollisions(){
  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];
  const listElem = document.getElementById('collision-list');
  if (listElem) listElem.innerHTML='';
}

/**
 * Rysuje kolizje w panelu i stawia JEDEN marker na mapie dla collision_id.
 */
function displayCollisions(collisions) {
  const listElem = document.getElementById('collision-list');

  if (!collisions || collisions.length===0) {
    let noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML='<i>No collisions for this day</i>';
    listElem.appendChild(noItem);
    return;
  }

  let usedIds = new Set();
  collisions.forEach(c => {
    if (!c.collision_id) return;
    if (usedIds.has(c.collision_id)) return;
    usedIds.add(c.collision_id);

    // Nazwy
    let shipA = c.ship1_name || `MMSI:${c.mmsi_a}`;
    let shipB = c.ship2_name || `MMSI:${c.mmsi_b}`;
    let distNm = (c.cpa||0).toFixed(2);
    let timeStr = '';
    if (c.timestamp) {
      let d = new Date(c.timestamp);
      timeStr = d.toLocaleTimeString('en-GB');
    }

    // splitted circle (przy history nie mamy dynamicznych markerów, fallback do c.ship_length_a/b?)
    let splittedHTML = getCollisionSplitCircle(
      c.mmsi_a, c.mmsi_b,
      c.ship_length_a||50,
      c.ship_length_b||50,
      null
    );

    // Element w panelu
    let item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML=`
      <div class="collision-header" style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedHTML}
          <strong>${shipA} - ${shipB}</strong><br>
          Dist: ${distNm} nm @ ${timeStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    listElem.appendChild(item);

    // Klik w lupę => zoom i wczytanie frames
    item.querySelector('.zoom-button').addEventListener('click',()=>{
      zoomToCollision(c);
    });

    // Marker kolizyjny
    let latC = (c.latitude_a + c.latitude_b)/2.0;
    let lonC = (c.longitude_a + c.longitude_b)/2.0;

    let collisionIcon = L.divIcon({
      className:'',
      html: `
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <circle cx="0" cy="0" r="8" fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="3" text-anchor="middle" font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize:[24,24],
      iconAnchor:[12,12]
    });

    let tip=`Collision: ${shipA} & ${shipB}\nDist:${distNm} nm\n@ ${timeStr}`;
    let marker=L.marker([latC, lonC], {icon:collisionIcon})
      .bindTooltip(tip.replace(/\n/g,"<br>"), {sticky:true})
      .on('click',()=>zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

/**
 * Kliknięcie w lupę/ikonę => przejście do kolizji + wczytanie animacji
 */
function zoomToCollision(c) {
  // Ustawiamy widok
  let latC = (c.latitude_a + c.latitude_b)/2;
  let lonC = (c.longitude_a + c.longitude_b)/2;
  map.setView([latC, lonC], 10);

  // Pokazujemy panele
  document.getElementById('left-panel').style.display='block';
  document.getElementById('bottom-center-bar').style.display='block';

  stopAnimation();
  animationData=[];
  animationIndex=0;

  // Zakładamy, że c.collision_id -> plik frames
  // lub /history_data?collision_id=...
  let url = `/history_data?collision_id=${encodeURIComponent(c.collision_id)}`;
  fetch(url)
    .then(r=>r.json())
    .then(json=>{
      // Zakładamy, że json ma frames: [...]
      if (!json.frames) {
        console.warn("Brak frames w pliku animacji:", json);
        return;
      }
      inSituationView=true;
      animationData=json.frames;
      animationIndex=0;
      updateMapFrame();
    })
    .catch(err=>{
      console.error("Błąd zoomToCollision fetch:", err);
    });
}

/**
 * start/stop animacji
 */
function startAnimation(){
  if (!animationData || animationData.length===0) return;
  isPlaying=true;
  document.getElementById('playPause').textContent='Pause';
  animationInterval=setInterval(()=>stepAnimation(1),1000);
}
function stopAnimation(){
  isPlaying=false;
  document.getElementById('playPause').textContent='Play';
  if (animationInterval) clearInterval(animationInterval);
  animationInterval=null;
}
function stepAnimation(step){
  animationIndex+=step;
  if (animationIndex<0) animationIndex=0;
  if (animationIndex>=animationData.length) animationIndex=animationData.length-1;
  updateMapFrame();
}

/**
 * Rysowanie klatki animacji
 */
function updateMapFrame(){
  const frameIndicator=document.getElementById('frameIndicator');
  frameIndicator.textContent=`${animationIndex+1}/${animationData.length}`;

  // Usuwamy poprzednie statki
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];

  if (!animationData||animationData.length===0) return;
  let frame=animationData[animationIndex];
  let ships=frame.shipPositions||[];

  ships.forEach(s=>{
    let sd={
      cog: s.cog||0,
      ship_length: s.ship_length||0
    };
    let icon=createShipIcon(sd,false);
    let marker=L.marker([s.lat,s.lon],{icon});
    let nm=s.name||`MMSI:${s.mmsi}`;
    let sogVal=(s.sog||0).toFixed(1);
    let rDeg=Math.round(s.cog||0);
    let tip=`
      <b>${nm}</b><br>
      SOG:${sogVal} kn, COG:${rDeg}°<br>
      Len:${s.ship_length||0}
    `;
    marker.bindTooltip(tip,{sticky:true});
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  // Lewy panel
  const leftPanel=document.getElementById('selected-ships-info');
  leftPanel.innerHTML='';
  const pairInfo=document.getElementById('pair-info');
  pairInfo.innerHTML='';

  // Przykład: wyświetl dane 2 statków:
  if (ships.length>=2){
    let sA=ships[0];
    let sB=ships[1];
    let nameA=sA.name||`MMSI:${sA.mmsi}`;
    let nameB=sB.name||`MMSI:${sB.mmsi}`;
    leftPanel.innerHTML=`
      <b>${nameA}</b><br>
      SOG:${(sA.sog||0).toFixed(1)} kn, COG:${Math.round(sA.cog||0)}°, L:${sA.ship_length||'N/A'}<br><br>
      <b>${nameB}</b><br>
      SOG:${(sB.sog||0).toFixed(1)} kn, COG:${Math.round(sB.cog||0)}°, L:${sB.ship_length||'N/A'}
    `;
  }
}