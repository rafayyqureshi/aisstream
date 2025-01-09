document.addEventListener('DOMContentLoaded', initHistoryApp);

// Mapa i warstwa klastrująca (opcjonalnie)
let map;
let collisionMarkers = [];

// Parametry
let currentDay = 0;      // suwak / przyciski do zmiany dni wstecz
const minDay = -7;
const maxDay = 0;

// Filtry
let cpaFilter = 0.5;     // [0..0.5 nm]
let isPlaying = false;
let animationData = [];  // dane pobrane do animacji
let animationIndex = 0;
let animationInterval = null;

let inSituationView = false;  // Czy jesteśmy w trybie animowania jednej kolizji?
let shipMarkersOnMap = [];    // Markerki statków w klatce

/**
 * Funkcja inicjalizująca moduł history.
 */
function initHistoryApp() {
  // 1) Inicjujemy mapę z common.js
  map = initSharedMap('map');

  // 2) UI
  setupUI();

  // 3) Pierwsze pobranie kolizji (dzisiaj)
  updateDayLabel();
  fetchCollisionsData();

  // Ewentualnie można dodać odświeżanie
  // setInterval(fetchCollisionsData, 5 * 60 * 1000);
}

function setupUI() {
  // Przycisk wstecz
  document.getElementById('prevDay').addEventListener('click', ()=>{
    if (currentDay > minDay) {
      currentDay--;
      updateDayLabel();
      fetchCollisionsData();
    }
  });

  // Przycisk dalej
  document.getElementById('nextDay').addEventListener('click', ()=>{
    if (currentDay < maxDay) {
      currentDay++;
      updateDayLabel();
      fetchCollisionsData();
    }
  });

  // Play/Pause
  document.getElementById('playPause').addEventListener('click', ()=>{
    if (isPlaying) stopAnimation();
    else startAnimation();
  });

  // Step forward/back
  document.getElementById('stepForward').addEventListener('click', ()=> stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', ()=> stepAnimation(-1));

  // Filtr CPA
  const cpaSlider = document.getElementById('cpaFilter');
  cpaSlider.addEventListener('input', e => {
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisionsData();
  });
}

/**
 * Zmiana etykiety z aktualnym dniem
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
 * Pobranie listy kolizji z back-endu (np. /history_collisions?day=..., cpa=...).
 */
function fetchCollisionsData() {
  // Wyczyść stare markery i listę
  clearCollisions();

  const url = `/history_collisions?day=${currentDay}&max_cpa=${cpaFilter}`;
  fetch(url)
    .then(r=>r.json())
    .then(data => {
      displayCollisions(data);
    })
    .catch(err => {
      console.error("Błąd fetchCollisionsData:", err);
    });
}

/**
 * Wyświetlenie kolizji w panelu i narysowanie ikon kolizji na mapie.
 */
function displayCollisions(collisions) {
  const list = document.getElementById('collision-list');
  list.innerHTML = '';

  if (!collisions || collisions.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = `<div style="padding:10px;font-style:italic;">No collisions for this day.</div>`;
    list.appendChild(noItem);
    return;
  }

  // Eliminacja duplikatów, wybór minimalnego dystansu, itp.
  // lub bierzemy wprost co daje backend (np. backend to obrobi).
  // Załóżmy, że backend już zrobił grupowanie i ma ship1_name, ship2_name, minimal_distance, timestamp, collision_id...

  // Rysowanie
  let usedCollisionIds = new Set();

  collisions.forEach(c => {
    // Sprawdzamy, czy mamy unikalny collision_id
    if (usedCollisionIds.has(c.collision_id)) {
      return; // pomijamy, już narysowaliśmy
    }
    usedCollisionIds.add(c.collision_id);

    // Nazwy statków
    let shipA = c.ship1_name || `MMSI:${c.mmsi_a}`;
    let shipB = c.ship2_name || `MMSI:${c.mmsi_b}`;

    // Minimal distance (nazywasz to "faktycznym dystansem")
    let distNm = c.min_distance ? c.min_distance.toFixed(2) : c.cpa.toFixed(2); 
    // -> ewentualnie c.min_distance vs c.cpa

    let timeStr = '';
    if (c.timestamp) {
      let dt = new Date(c.timestamp);
      timeStr = dt.toLocaleTimeString('en-GB');
    }

    // split circle
    const splittedHTML = getCollisionSplitCircle(
      c.mmsi_a,
      c.mmsi_b,
      c.ship_length_a,
      c.ship_length_b,
      null // tu można wstawić null lub obiekt markerów, jeśli chcemy nadpisywać
    );

    // Tworzymy item na liście
    const item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML = `
      <div class="collision-header" style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedHTML}
          <strong>${shipA} - ${shipB}</strong><br>
          Min dist: ${distNm} nm @ ${timeStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    list.appendChild(item);

    // Obsługa kliknięcia lupy
    item.querySelector('.zoom-button').addEventListener('click', ()=>{
      zoomToCollision(c);
    });

    // Ikona na mapie – jedna na kolizję:
    let latC = c.latitude_collision || ((c.latitude_a + c.latitude_b)/2);
    let lonC = c.longitude_collision || ((c.longitude_a + c.longitude_b)/2);

    // np. sygnał ostrzegawczy
    const collisionIcon = L.divIcon({
      className: '',
      html: `
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <circle cx="0" cy="0" r="8" fill="yellow" stroke="red" stroke-width="2"></circle>
          <text x="0" y="3" text-anchor="middle" font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize:[24,24],
      iconAnchor:[12,12]
    });

    let tip = `Collision: ${shipA} & ${shipB}\nDist: ${distNm} nm\n${timeStr}`;
    let marker = L.marker([latC, lonC], {icon: collisionIcon})
      .on('click', ()=>zoomToCollision(c))
      .bindTooltip(tip.replace(/\n/g,"<br>"), {sticky:true});
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

/**
 * Klik w lupę lub w ikonę kolizji – wczytujemy animację.
 */
function zoomToCollision(c) {
  // 1) Centrujemy mapę
  let latC = c.latitude_collision || ((c.latitude_a + c.latitude_b)/2);
  let lonC = c.longitude_collision || ((c.longitude_a + c.longitude_b)/2);
  map.setView([latC, lonC], 9);  // np. zoom=9

  // 2) Wczytujemy dane animacji (JSON) z back-endu
  // - zakładamy endpoint /history_data?collision_id=...
  let url = `/history_data?collision_id=${encodeURIComponent(c.collision_id)}`;
  fetch(url)
    .then(r => r.json())
    .then(bigJson => {
      // bigJson może zawierać "collisions": [ {...}, {...} ] 
      //  lub pojedyńczy collision_obj. 
      //  Musisz dopasować do formatu, który zapisał pipeline

      let collisionObj = null;
      if (bigJson.collisions) {
        // Szukamy collision_id
        collisionObj = bigJson.collisions.find(col => col.collision_id===c.collision_id);
      } else {
        // Może to już pojedyńczy obiekt
        collisionObj = bigJson;
      }
      if (!collisionObj) {
        console.warn("Nie znaleziono collision_id:", c.collision_id);
        return;
      }

      // Mamy frames do animacji
      inSituationView = true;
      document.getElementById('left-panel').style.display='block';
      document.getElementById('bottom-center-bar').style.display='block';

      animationData = collisionObj.frames || [];
      animationIndex=0;
      stopAnimation();
      updateMapFrame(); // pokaż klatkę 0
    })
    .catch(err=>{
      console.error("Błąd wczytywania animacji kolizji:", err);
    });
}

/**
 * Czyścimy stare markery kolizyjne
 */
function clearCollisions() {
  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers = [];
}

/**
 * Animacja: start
 */
function startAnimation(){
  if (!animationData || animationData.length===0) return;
  isPlaying=true;
  document.getElementById('playPause').textContent = 'Pause';
  animationInterval = setInterval(()=>stepAnimation(1), 1000);
}

/**
 * Animacja: stop
 */
function stopAnimation(){
  isPlaying=false;
  document.getElementById('playPause').textContent = 'Play';
  if (animationInterval) clearInterval(animationInterval);
  animationInterval=null;
}

/**
 * Krok animacji
 */
function stepAnimation(step){
  animationIndex += step;
  if (animationIndex<0) animationIndex=0;
  if (animationIndex>=animationData.length) animationIndex=animationData.length-1;
  updateMapFrame();
}

/**
 * Rysujemy klatkę
 */
function updateMapFrame(){
  const frameIndicator = document.getElementById('frameIndicator');
  frameIndicator.textContent = `${animationIndex+1}/${animationData.length}`;

  // Usunięcie starych statków
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];

  if (animationData.length===0) return;
  let frame = animationData[animationIndex];
  let ships = frame.shipPositions || [];

  // Rysuj każdy statek
  ships.forEach(s => {
    let icon = createShipIcon({
      cog: s.cog,
      ship_length: s.ship_length
    }, false);

    let mk = L.marker([s.lat, s.lon], { icon });
    let tip = `
      <b>${s.name||s.mmsi}</b><br>
      SOG: ${Number(s.sog).toFixed(1)} kn, COG: ${Math.round(s.cog)}°<br>
      Len: ${s.ship_length||0}
    `;
    mk.bindTooltip(tip, {direction:'top', sticky:true});
    mk.addTo(map);
    shipMarkersOnMap.push(mk);
  });

  // Panel info
  let leftPanel = document.getElementById('selected-ships-info');
  leftPanel.innerHTML = '';
  let pairInfo = document.getElementById('pair-info');
  pairInfo.innerHTML = '';

  if (ships.length>=2){
    // Możesz tu obliczać doraźnie cpa/tcpa itp. w JS
    // ale to zależy od potrzeb
  }
}