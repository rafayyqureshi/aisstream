// history_app.js
// Moduł “History”: wyświetlanie historycznych sytuacji kolizyjnych,
// z możliwością animacji i klatkowego przeglądu.

document.addEventListener('DOMContentLoaded', initHistoryApp);

/** Zmienne globalne dla modułu history */
let map;
let collisionMarkers = [];        // Ikony kolizyjne na mapie
let currentDay = 0;               // Przesunięcie daty (np. day=0 -> dzisiaj, day=-1 -> wczoraj)
const minDay = -7;
const maxDay = 0;

let distFilter = 0.5;            // Filtr minimalnego dystansu (zamiast cpaFilter)
let isPlaying = false;           // Czy trwa animacja
let animationData = [];          // Tablica z klatkami
let animationIndex = 0;
let animationInterval = null;

let inSituationView = false;     // Czy jesteśmy w trybie odtwarzania sytuacji
let shipMarkersOnMap = [];       // Markerki statków w animacji

/** 
 * Inicjalizacja modułu history (wywoływana po DOMContentLoaded).
 */
function initHistoryApp() {
  // 1) Tworzymy mapę (z common.js)
  map = initSharedMap("map");

  // 2) Obsługa UI (przyciski, suwak filtrujący distance)
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

  document.getElementById('playPause').addEventListener('click', () => {
    if(isPlaying) stopAnimation(); else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click', ()=> stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', ()=> stepAnimation(-1));

  // Filtr minimalnego dystansu
  document.getElementById('cpaFilter').addEventListener('input', (e) => {
    distFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = distFilter.toFixed(2);
    fetchCollisionsData();
  });

  // Wyjście z trybu animacji po kliknięciu w mapę (opcjonalne)
  map.on('click', () => {
    if(inSituationView) exitSituationView();
  });

  // 3) Ustawiamy początkową datę i pobieramy kolizje
  updateDayLabel();
  fetchCollisionsData();
}

/**
 * updateDayLabel() – wyświetla aktualnie wybrany dzień w #currentDayLabel
 */
function updateDayLabel() {
  const now = new Date();
  let target = new Date(now);
  target.setDate(now.getDate() + currentDay);
  const dateStr = target.toISOString().slice(0,10);

  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
  document.getElementById('prevDay').disabled = (currentDay<=minDay);
  document.getElementById('nextDay').disabled = (currentDay>=maxDay);
}

/**
 * Pobiera listę kolizji historycznych z /history_collisions
 */
function fetchCollisionsData() {
  clearCollisions();
  // Zakładam endpoint: /history_collisions?day=...&max_dist=...
  // lub w Twoim kodzie: ?max_cpa=... – dostosuj do faktycznego backendu
  fetch(`/history_collisions?day=${currentDay}&max_cpa=${distFilter}`)
    .then(r => r.json())
    .then(data => displayCollisions(data))
    .catch(err => console.error("Error fetching collisions:", err));
}

/**
 * Usuwa poprzednie markery kolizyjne z mapy
 */
function clearCollisions() {
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];
}

/**
 * Wyświetla listę kolizji w #collision-list, tworzy markery na mapie.
 */
function displayCollisions(collisions) {
  const list = document.getElementById('collision-list');
  list.innerHTML = '';

  if(!collisions || collisions.length===0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = `<div style="padding:10px; font-style:italic;">
      No collisions for this day.</div>`;
    list.appendChild(noItem);
    return;
  }

  // Ewentualnie usuwanie duplikatów (collision_id)
  let uniqueMap = {};
  collisions.forEach(c => {
    let cid = c.collision_id || `${c.mmsi_a}_${c.mmsi_b}_${c.timestamp||''}`;
    if(!uniqueMap[cid]) uniqueMap[cid] = c;
    // w przeciwnym razie decyduj, czy wolisz nowszy/ starszy
  });
  let finalCollisions = Object.values(uniqueMap);

  finalCollisions.forEach(c => {
    // Nazwy statków, fallback do mmsi
    let shipA = c.ship1_name || `#${c.mmsi_a}`;
    let shipB = c.ship2_name || `#${c.mmsi_b}`;
    // Minimalny dystans (np. c.min_dist)
    let minDist = c.min_dist ? c.min_dist.toFixed(2) : (c.cpa||0).toFixed(2);
    
    // Rozmiary statków -> splitted circle
    // (common.js: getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, markers) 
    //  lub prosto createSplittedCircle(getShipColor(...), getShipColor(...))
    let splittedHTML = createSplittedCircle(
      getShipColor(c.ship_length_a||0),
      getShipColor(c.ship_length_b||0)
    );

    // Czas minimalnego zbliżenia
    let timeStr = '???';
    if(c.timestamp) {
      let d = new Date(c.timestamp);
      timeStr = d.toLocaleTimeString('en-GB');
    }

    // Tworzymy element w liście
    const item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML = `
      <div class="collision-header" style="display:flex;justify-content:space-between;align-items:center;">
        <div style="flex:1;">
          ${splittedHTML}
          <strong>${shipA} – ${shipB}</strong><br>
          Distance: ${minDist} nm @ ${timeStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    list.appendChild(item);

    // Klik w lupkę -> zoom do kolizji + odtworzenie
    item.querySelector('.zoom-button').addEventListener('click', ()=>{
      zoomToCollision(c);
    });

    // Marker na mapie w miejscu minimalnego dystansu
    // Zakładam c.min_lat, c.min_lon. Jeśli nie masz, to uśrednij:
    let latC = (c.latitude_a + c.latitude_b)/2;    // np. (min_lat ? c.min_lat : ...)
    let lonC = (c.longitude_a + c.longitude_b)/2;

    const collisionIcon = L.divIcon({
      className:'',
      html: `
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <path d="M0,-7 7,7 -7,7 Z" 
                fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="4" text-anchor="middle" font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize:[24,24],
      iconAnchor:[12,12]
    });
    const tipHTML = `
      <b>Collision:</b> ${shipA} &amp; ${shipB}<br>
      Dist: ${minDist} nm<br>
      Time: ${timeStr}
    `;
    let marker = L.marker([latC, lonC], { icon: collisionIcon })
      .bindTooltip(tipHTML, { direction:'top', sticky:true })
      .on('click', ()=> zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

/**
 * Po kliknięciu w kolizję – zoom + wczytanie animacji
 */
function zoomToCollision(c) {
  // Zakładam c.min_lat/lon lub c.latitude_a,b
  let latC = (c.latitude_a + c.latitude_b)/2;
  let lonC = (c.longitude_a + c.longitude_b)/2;
  let bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, {padding:[20,20]});

  // Wczytanie pliku /history_data?collision_id=..., odtworzenie animacji
  loadCollisionData(c.collision_id);
}

/**
 * loadCollisionData(collision_id):
 *  pobiera klatki z serwera i przygotowuje do animacji
 */
function loadCollisionData(collision_id) {
  fetch(`/history_data?collision_id=${collision_id}`)
    .then(r => r.json())
    .then(data => {
      animationData = data || [];
      animationIndex=0;
      stopAnimation();
      inSituationView=true;

      // Pokazujemy panele
      document.getElementById('left-panel').style.display='block';
      document.getElementById('bottom-center-bar').style.display='block';

      updateMapFrame();

      // Dopasowanie mapy do pierwszej klatki (opcjonalnie)
      if(animationData.length>0) {
        let firstShips = animationData[0].shipPositions||[];
        if(firstShips.length>0) {
          let latLngs = firstShips.map(s=>[s.lat, s.lon]);
          let b = L.latLngBounds(latLngs);
          map.fitBounds(b, {padding:[20,20]});
        }
      }
    })
    .catch(err => console.error("Error loading collision data:", err));
}

/**
 * start/stop animacji i klatkowanie
 */
function startAnimation() {
  if(!animationData || animationData.length===0) return;
  isPlaying=true;
  document.getElementById('playPause').textContent='Pause';
  animationInterval = setInterval(()=> stepAnimation(1), 1000);
}
function stopAnimation() {
  isPlaying=false;
  document.getElementById('playPause').textContent='Play';
  if(animationInterval) clearInterval(animationInterval);
  animationInterval=null;
}
function stepAnimation(step){
  animationIndex += step;
  if(animationIndex<0) animationIndex=0;
  if(animationIndex>=animationData.length) animationIndex=animationData.length-1;
  updateMapFrame();
}

/**
 * updateMapFrame – wyświetla statki dla danej klatki
 */
function updateMapFrame(){
  const frameIndicator = document.getElementById('frameIndicator');
  frameIndicator.textContent = `${animationIndex+1}/${animationData.length}`;

  // Usuwanie starych statków z mapy
  shipMarkersOnMap.forEach(m => map.removeLayer(m));
  shipMarkersOnMap=[];

  if(!animationData || animationData.length===0) return;
  let frame = animationData[animationIndex];
  let ships = frame.shipPositions||[];

  ships.forEach(s => {
    // createShipIcon z common.js
    let marker = L.marker([s.lat, s.lon], {
      icon: createShipIcon(s, false)
    });
    let nm = s.name||s.mmsi;
    let tip=`
      <b>${nm}</b><br>
      COG:${Math.round(s.cog||0)}°, SOG:${(s.sog||0).toFixed(1)} kn<br>
      Len:${s.ship_length||'Unknown'}
    `;
    marker.bindTooltip(tip,{direction:'top',sticky:true});
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  // Lewy panel
  let leftPanel=document.getElementById('selected-ships-info');
  leftPanel.innerHTML='';
  let pairInfo=document.getElementById('pair-info');
  pairInfo.innerHTML='';

  if(ships.length>=2){
    let sA=ships[0], sB=ships[1];
    let { cpa, tcpa } = compute_cpa_tcpa_js(sA,sB); // Jeśli chcesz w JS
    let tObj=new Date(frame.time||Date.now());
    let hh = tObj.getHours().toString().padStart(2,'0');
    let mm = tObj.getMinutes().toString().padStart(2,'0');
    let ss = tObj.getSeconds().toString().padStart(2,'0');
    let timeStr=`${hh}:${mm}:${ss}`;

    pairInfo.innerHTML=`
      Time: ${timeStr}<br>
      Dist now: ${cpa.toFixed(2)} nm, 
      TCPA: ${tcpa.toFixed(2)} min
    `;
    leftPanel.innerHTML=`
      <b>${sA.name||sA.mmsi}</b><br>
      SOG:${(sA.sog||0).toFixed(1)} kn, COG:${Math.round(sA.cog||0)}°, L:${sA.ship_length||'N/A'}<br><br>
      <b>${sB.name||sB.mmsi}</b><br>
      SOG:${(sB.sog||0).toFixed(1)} kn, COG:${Math.round(sB.cog||0)}°, L:${sB.ship_length||'N/A'}
    `;
  }
}

/**
 * compute_cpa_tcpa_js – prosta lokalna implementacja, 
 *  lub pusta, jeśli niepotrzebna.
 */
function compute_cpa_tcpa_js(a,b) {
  // np. mock:
  return { cpa: 0.25, tcpa: 4.7 };
}

/**
 * exitSituationView – wyjście z trybu animacji
 */
function exitSituationView(){
  inSituationView=false;
  document.getElementById('left-panel').style.display='none';
  document.getElementById('bottom-center-bar').style.display='none';
  stopAnimation();
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];
  animationData=[];
  animationIndex=0;
}