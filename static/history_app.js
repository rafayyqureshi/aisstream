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
  // 1) Inicjalizujemy mapę (z pliku common.js)
  map = initSharedMap('map');

  // 2) Ustawiamy UI
  setupUI();

  // 3) Pobieramy listę plików z GCS i wczytujemy kolizje
  fetchFileListAndLoadCollisions();
}

/**
 * Inicjalizacja sliderów, przycisków, itp.
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

  // cpaFilter
  const cpaSlider = document.getElementById('cpaFilter');
  cpaSlider.addEventListener('input', e => {
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    filterAndDisplayCollisions();
  });

  // Animacja
  document.getElementById('playPause').addEventListener('click', ()=>{
    if (isPlaying) stopAnimation(); 
    else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click', ()=>{
    stepAnimation(1);
  });
  document.getElementById('stepBack').addEventListener('click', ()=>{
    stepAnimation(-1);
  });

  updateDayLabel();
}

/**
 * Aktualizuje label z wybranym dniem i dezaktywuje przyciski min/max.
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
 * Pobieramy listę plików GCS z np. /history_filelist
 */
function fetchFileListAndLoadCollisions() {
  fetch('/history_filelist')
    .then(res => {
      if (!res.ok) {
        // Może być 500 czy inny kod
        throw new Error(`HTTP status ${res.status} - ${res.statusText}`);
      }
      return res.text(); // Spróbujemy odczytać jako tekst
    })
    .then(txt => {
      let data;
      try {
        data = JSON.parse(txt);
      } catch (parseErr) {
        console.error("Otrzymano niepoprawny JSON (być może HTML z błędem?).", parseErr);
        return;
      }
      if (!data.files) {
        console.warn("Brak 'files' w odpowiedzi /history_filelist:", data);
        return;
      }
      // Ładujemy wszystkie pliki
      loadAllFiles(data.files);
    })
    .catch(err => {
      console.error("Błąd fetchFileList:", err);
    });
}

/**
 * Sekwencyjnie ładujemy pliki z GCS i łączymy kolizje do allCollisions
 */
function loadAllFiles(fileList) {
  let chain = Promise.resolve();
  fileList.forEach(fileName => {
    chain = chain.then(() => loadOneFile(fileName));
  });
  chain.then(()=>{
    console.log("Wczytano wszystkie pliki GCS. Liczba kolizji:", allCollisions.length);
    filterAndDisplayCollisions();
  });
}

function loadOneFile(fileName) {
  const url = `/history_data?file=${encodeURIComponent(fileName)}`;
  return fetch(url)
    .then(res => {
      if (!res.ok) {
        throw new Error(`HTTP status ${res.status} loading ${fileName}`);
      }
      return res.text();
    })
    .then(txt => {
      let jsonData;
      try {
        jsonData = JSON.parse(txt);
      } catch (err) {
        console.error(`Niepoprawny JSON w pliku: ${fileName}`, err);
        return;
      }
      if (!jsonData.collisions) {
        console.warn(`Brak 'collisions' w pliku: ${fileName}`, jsonData);
        return;
      }
      allCollisions.push(...jsonData.collisions);
    })
    .catch(err => {
      console.error("Błąd loadOneFile:", fileName, err);
    });
}

/**
 * Filtr wg day i cpa, potem wyświetlenie
 */
function filterAndDisplayCollisions() {
  clearCollisions();

  if (!allCollisions || allCollisions.length===0) {
    const listElem = document.getElementById('collision-list');
    listElem.innerHTML = '<div class="collision-item"><i>No collisions loaded or no data</i></div>';
    return;
  }

  // Filtr day
  const now = new Date();
  let targetDate = new Date(now);
  targetDate.setDate(now.getDate() + currentDay);
  let targetStr = targetDate.toISOString().slice(0,10);

  // Filtr cpa
  let filtered = allCollisions.filter(c => {
    if (!c.timestamp) return false;
    let dtStr = c.timestamp.slice(0,10);
    if (dtStr !== targetStr) return false;

    if ((c.cpa||99) > cpaFilter) return false;
    return true;
  });

  displayCollisions(filtered);
}

/**
 * Czyści listę i markery
 */
function clearCollisions() {
  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];
  const listElem = document.getElementById('collision-list');
  if (listElem) listElem.innerHTML='';
}

/**
 * Wyświetla kolizje w panelu i JEDEN marker na mapie (po collision_id).
 */
function displayCollisions(collisions) {
  const listElem = document.getElementById('collision-list');
  if (!collisions || collisions.length===0) {
    let noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML='<i>No collisions for selected day/cpa</i>';
    listElem.appendChild(noItem);
    return;
  }

  let usedIds = new Set();
  collisions.forEach(c => {
    if (!c.collision_id) return;
    if (usedIds.has(c.collision_id)) return;
    usedIds.add(c.collision_id);

    // Nazwy statków (lub fallback do mmsi)
    let shipA = c.ship1_name || `MMSI:${c.mmsi_a}`;
    let shipB = c.ship2_name || `MMSI:${c.mmsi_b}`;
    let distNm = (c.cpa||0).toFixed(2);
    let timeStr = '';
    if (c.timestamp) {
      let d = new Date(c.timestamp);
      timeStr = d.toLocaleTimeString('en-GB');
    }

    // Tworzymy item w panelu
    let item = document.createElement('div');
    item.classList.add('collision-item');

    item.innerHTML=`
      <div class="collision-header" style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <strong>${shipA} - ${shipB}</strong><br>
          Min Dist: ${distNm} nm @ ${timeStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    listElem.appendChild(item);

    // Obsługa kliknięcia -> zoom i wczytanie animacji
    item.querySelector('.zoom-button').addEventListener('click', ()=>{
      zoomToCollision(c);
    });

    // Marker kolizyjny
    let latC = ((c.latitude_a ||0) + (c.latitude_b||0))/2;
    let lonC = ((c.longitude_a||0) + (c.longitude_b||0))/2;

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

    let tip=`Collision: ${shipA} & ${shipB}\nDist:${distNm} nm @ ${timeStr}`;
    let marker = L.marker([latC, lonC], { icon:collisionIcon })
      .bindTooltip(tip.replace(/\n/g,"<br>"), {sticky:true})
      .on('click', ()=> zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

/**
 * Przeniesienie widoku do kolizji i pobranie animacji
 */
function zoomToCollision(c) {
  let latC = ((c.latitude_a||0) + (c.latitude_b||0))/2;
  let lonC = ((c.longitude_a||0) + (c.longitude_b||0))/2;
  map.setView([latC, lonC], 10);

  document.getElementById('left-panel').style.display='block';
  document.getElementById('bottom-center-bar').style.display='block';

  stopAnimation();
  animationData=[];
  animationIndex=0;

  // c.collision_id => /history_data?collision_id=...
  let url = `/history_data?collision_id=${encodeURIComponent(c.collision_id)}`;
  fetch(url)
    .then(r => {
      if (!r.ok) {
        throw new Error(`HTTP error: ${r.status}`);
      }
      return r.text();
    })
    .then(txt => {
      let json;
      try {
        json = JSON.parse(txt);
      } catch (err) {
        console.error("Niepoprawny JSON frames:", err);
        return;
      }
      if (!json.frames) {
        console.warn("Brak frames w:", json);
        return;
      }
      inSituationView = true;
      animationData = json.frames;
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
 * Wyświetlenie klatki animacji
 */
function updateMapFrame(){
  const frameIndicator = document.getElementById('frameIndicator');
  frameIndicator.textContent = `${animationIndex+1}/${animationData.length}`;

  // czyścimy poprzednie
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];

  if(!animationData || animationData.length===0) return;
  let frame = animationData[animationIndex];
  let ships = frame.shipPositions||[];

  ships.forEach(s=>{
    let sd={
      cog: s.cog||0,
      ship_length: s.ship_length||0
    };
    let icon = createShipIcon(sd,false);
    let marker = L.marker([s.lat, s.lon], {icon});
    let nm = s.name || `MMSI:${s.mmsi}`;
    let sogVal = (s.sog||0).toFixed(1);
    let rDeg = Math.round(s.cog||0);
    let tip=`
      <b>${nm}</b><br>
      SOG:${sogVal} kn, COG:${rDeg}°<br>
      Len:${s.ship_length||0}
    `;
    marker.bindTooltip(tip, {sticky:true});
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  // Panel info
  const leftPanel = document.getElementById('selected-ships-info');
  leftPanel.innerHTML='';
  const pairInfo = document.getElementById('pair-info');
  pairInfo.innerHTML='';

  // Dla prostoty 2 statki (lub n)
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