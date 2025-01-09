document.addEventListener('DOMContentLoaded', initHistoryApp);

// ------------------------------------------
// Zmienne globalne modułu history
// ------------------------------------------
let map;
let collisionMarkers = [];

// Parametry do przesunięcia o dni
let currentDay = 0; 
const minDay = -7;
const maxDay = 0;

// Filtr odległości
let cpaFilter = 0.5;

// Kontrola animacji
let isPlaying = false;
let animationData = [];   // klatki z /history_data
let animationIndex = 0;
let animationInterval = null;

let inSituationView = false; // czy jesteśmy w trakcie oglądania konkretnej animacji
let shipMarkersOnMap = [];   // aktualne markery statków w klatce

/**
 * Główna inicjalizacja modułu history,
 * wywoływana po załadowaniu DOM.
 */
function initHistoryApp() {
  // 1) Tworzymy mapę z pliku common.js
  map = initSharedMap('map');

  // 2) Ustawiamy eventy UI
  setupUI();

  // 3) Pierwszy fetch kolizji (aktualny dzień = 0)
  updateDayLabel();
  fetchCollisionsData();

  // (opcjonalnie) setInterval(fetchCollisionsData, 5*60*1000);
}

/**
 * Ustawienia UI: przyciski, suwaki itp.
 */
function setupUI() {
  // Dzień “-”
  document.getElementById('prevDay').addEventListener('click', ()=>{
    if (currentDay > minDay) {
      currentDay--;
      updateDayLabel();
      fetchCollisionsData();
    }
  });

  // Dzień “+”
  document.getElementById('nextDay').addEventListener('click', ()=>{
    if (currentDay < maxDay) {
      currentDay++;
      updateDayLabel();
      fetchCollisionsData();
    }
  });

  // Suwak cpaFilter
  const cpaSlider = document.getElementById('cpaFilter');
  cpaSlider.addEventListener('input', e => {
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisionsData();
  });

  // Animacja: play/pause
  document.getElementById('playPause').addEventListener('click', ()=>{
    if (isPlaying) stopAnimation();
    else startAnimation();
  });

  // Step forward/back
  document.getElementById('stepForward').addEventListener('click', ()=> stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', ()=> stepAnimation(-1));
}

/**
 * Ustaw aktualną etykietę dnia (nadpisuje #currentDayLabel),
 * dezaktywuje przyciski jeśli min/max.
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
 * Pobieramy listę kolizji z backendu (np. /history_collisions?day=..., &max_cpa=...).
 */
function fetchCollisionsData() {
  // Czyścimy starą listę i markery
  clearCollisions();

  const url = `/history_collisions?day=${currentDay}&max_cpa=${cpaFilter}`;
  fetch(url)
    .then(resp => resp.json())
    .then(data => {
      displayCollisions(data);
    })
    .catch(err => console.error("Błąd fetchCollisionsData:", err));
}

/**
 * Czyści stare markery kolizyjne z mapy
 */
function clearCollisions(){
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];
}

/**
 * Wyświetlamy kolizje w panelu i rysujemy 1 marker kolizyjny na mapie.
 * W polach c mamy m.in. ship1_name, ship2_name, min_distance, cpa, timestamp, collision_id, itp.
 */
function displayCollisions(collisions) {
  const listElem = document.getElementById('collision-list');
  listElem.innerHTML = '';

  if (!collisions || collisions.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = `<div style="padding:10px;font-style:italic;">No collisions for this day.</div>`;
    listElem.appendChild(noItem);
    return;
  }

  // Dla bezpieczeństwa unikamy powtórek collision_id
  let usedIds = new Set();

  collisions.forEach(c => {
    if (!c.collision_id) return; // pomijamy rekord bez collision_id
    if (usedIds.has(c.collision_id)) return;
    usedIds.add(c.collision_id);

    // 1) Nazwy statków
    let shipA = c.ship1_name || `MMSI:${c.mmsi_a}`;
    let shipB = c.ship2_name || `MMSI:${c.mmsi_b}`;

    // 2) Minimalny dystans
    let distNm = (c.min_distance !== undefined)
      ? c.min_distance.toFixed(2)
      : (c.cpa !== undefined ? c.cpa.toFixed(2) : "N/A");

    // 3) Czas
    let timeStr = '';
    if (c.timestamp) {
      let dt = new Date(c.timestamp);
      timeStr = dt.toLocaleTimeString('en-GB'); // np. 13:27:45
    }

    // 4) splitted circle
    let splittedHTML = getCollisionSplitCircle(
      c.mmsi_a, c.mmsi_b,
      c.ship_length_a, c.ship_length_b,
      null  // w history raczej nie mamy shipMarkers live, więc null
    );

    // 5) Tworzymy item w panelu
    const collisionItem = document.createElement('div');
    collisionItem.classList.add('collision-item');
    collisionItem.innerHTML = `
      <div class="collision-header" style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedHTML}
          <strong>${shipA} - ${shipB}</strong><br>
          Min dist: ${distNm} nm @ ${timeStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    listElem.appendChild(collisionItem);

    // Przycisk lupy => zoom
    collisionItem.querySelector('.zoom-button').addEventListener('click', ()=>{
      zoomToCollision(c);
    });

    // 6) Ikona kolizyjna
    let latC = c.latitude_collision
      || ((c.latitude_a + c.latitude_b) / 2.0);
    let lonC = c.longitude_collision
      || ((c.longitude_a + c.longitude_b) / 2.0);

    let collisionIcon = L.divIcon({
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

    let tip = `Collision: ${shipA} & ${shipB}\nMin dist: ${distNm} nm\n@ ${timeStr}`;
    let marker = L.marker([latC, lonC], { icon: collisionIcon })
      .bindTooltip(tip.replace(/\n/g,"<br>"), {sticky:true})
      .on('click', ()=> zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

/**
 * Kliknięcie na lupę/ikonę kolizji – wczytujemy animację, pokazujemy panele
 */
function zoomToCollision(c) {
  // 1) Ustawiamy mapę na właściwy zoom
  let latC = c.latitude_collision
    || ((c.latitude_a + c.latitude_b)/2);
  let lonC = c.longitude_collision
    || ((c.longitude_a + c.longitude_b)/2);
  map.setView([latC, lonC], 10);

  // 2) Pokaż panele (left i bottom)
  document.getElementById('left-panel').style.display='block';
  document.getElementById('bottom-center-bar').style.display='block';

  // 3) wczytaj dane z /history_data
  let url = `/history_data?collision_id=${encodeURIComponent(c.collision_id)}`;
  fetch(url)
    .then(r=>r.json())
    .then(bigJson => {
      // Zależy od formatu pliku. Może jest { collisions: [ { collision_id, frames: [...] } ] }
      // Albo już jest jednym obiektem collision.
      let foundObj = null;
      if (bigJson.collisions) {
        // Szukamy w collisions
        foundObj = bigJson.collisions.find(x => x.collision_id === c.collision_id);
      } else if (bigJson.collision_id) {
        // Być może to już jeden obiekt
        foundObj = bigJson;
      }

      if (!foundObj) {
        console.warn("Nie znaleziono collision_id w bigJson:", c.collision_id);
        return;
      }

      // Ustawiamy dane animacji
      inSituationView = true;
      stopAnimation(); // reset
      animationData = foundObj.frames || [];
      animationIndex = 0;

      // start klatki 0
      updateMapFrame();
    })
    .catch(err=>{
      console.error("Błąd /history_data fetch:", err);
    });
}

/**
 * Start animacji
 */
function startAnimation(){
  if (!animationData || animationData.length===0) return;
  isPlaying = true;
  document.getElementById('playPause').textContent='Pause';
  animationInterval = setInterval(()=> stepAnimation(1), 1000);
}

/**
 * Stop animacji
 */
function stopAnimation(){
  isPlaying = false;
  document.getElementById('playPause').textContent='Play';
  if (animationInterval) clearInterval(animationInterval);
  animationInterval=null;
}

/**
 * Krok animacji
 */
function stepAnimation(step) {
  animationIndex += step;
  if (animationIndex < 0) animationIndex=0;
  if (animationIndex >= animationData.length) animationIndex = animationData.length-1;
  updateMapFrame();
}

/**
 * Rysowanie klatki animacji
 */
function updateMapFrame(){
  const frameIndicator = document.getElementById('frameIndicator');
  frameIndicator.textContent = `${animationIndex+1}/${animationData.length}`;

  // Usuwamy poprzednie markery statków
  shipMarkersOnMap.forEach(m => map.removeLayer(m));
  shipMarkersOnMap = [];

  if (!animationData || animationData.length===0) return;
  let frame = animationData[animationIndex];
  let ships = frame.shipPositions || [];

  // Rysujemy statki
  ships.forEach(s=>{
    // build "shipData" stylizowane do createShipIcon
    let sd = {
      cog: s.cog || 0,
      ship_length: s.ship_length || 0
      // tu można dodać sog, name, cokolwiek - createShipIcon 
      // i tak używa tylko cog i ship_length
    };

    let icon = createShipIcon(sd, false);
    let marker = L.marker([s.lat, s.lon], { icon });

    // tooltip
    let sogVal = s.sog !== undefined ? Number(s.sog).toFixed(1) : 'N/A';
    let rotation = Math.round(s.cog||0);
    let nm = s.name || `MMSI:${s.mmsi}`;
    let tip = `
      <b>${nm}</b><br>
      SOG: ${sogVal} kn, COG: ${rotation}°<br>
      Len: ${s.ship_length||0}
    `;
    marker.bindTooltip(tip, {direction:'top', sticky:true});

    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  // Lewy panel – np. wyświetlamy nazwy statków z tej klatki
  const leftPanel = document.getElementById('selected-ships-info');
  leftPanel.innerHTML='';
  const pairInfo = document.getElementById('pair-info');
  pairInfo.innerHTML='';

  if (ships.length>=2) {
    // Wyświetlmy dwie nazwy statków:
    let sA = ships[0];
    let sB = ships[1];
    let nameA = sA.name || `MMSI:${sA.mmsi}`;
    let nameB = sB.name || `MMSI:${sB.mmsi}`;

    leftPanel.innerHTML=`
      <b>${nameA}</b><br>
      SOG:${(sA.sog||0).toFixed(1)} kn, COG:${Math.round(sA.cog||0)}°, L:${sA.ship_length||'N/A'}<br><br>
      <b>${nameB}</b><br>
      SOG:${(sB.sog||0).toFixed(1)} kn, COG:${Math.round(sB.cog||0)}°, L:${sB.ship_length||'N/A'}
    `;
    // (opcjonalnie) oblicz w JS cpa/tcpa pomiędzy sA, sB – wg. twych metod
  }
}