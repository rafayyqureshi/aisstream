// history_app.js
// Moduł “History”: prezentacja historycznych kolizji, wraz z animacją i trybem klatkowym.

document.addEventListener('DOMContentLoaded', initHistoryApp);

let map;
let collisionMarkers = [];  // Jedna ikona kolizji na mapie
let currentDay = 0;
const minDay = -7;
const maxDay = 0;

// Filtr dystansu (zamiast cpaFilter)
let distFilter = 0.5;

// Animacja
let isPlaying = false;
let animationData = [];
let animationIndex = 0;
let animationInterval = null;

let inSituationView = false;  // Czy jesteśmy w trybie szczegółowej animacji
let shipMarkersOnMap = [];

/**
 * Inicjalizacja modułu history (po załadowaniu DOM).
 */
function initHistoryApp() {
  // 1) Tworzymy mapę (z common.js)
  map = initSharedMap("map");

  // 2) Ustawienia UI
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
  document.getElementById('cpaFilter').addEventListener('input', e => {
    distFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = distFilter.toFixed(2);
    fetchCollisionsData();
  });

  document.getElementById('playPause').addEventListener('click', () => {
    if (isPlaying) stopAnimation(); else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click', () => stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', () => stepAnimation(-1));

  // Klik w mapę -> wyjście z animacji (opcjonalnie)
  map.on('click', () => {
    if (inSituationView) {
      exitSituationView();
    }
  });

  // 3) Pobierz kolizje
  updateDayLabel();
  fetchCollisionsData();
}

/**
 * updateDayLabel(): wyświetlamy wybrany dzień w #currentDayLabel
 */
function updateDayLabel() {
  const now = new Date();
  let target = new Date(now);
  target.setDate(now.getDate() + currentDay);
  const dateStr = target.toISOString().slice(0,10);

  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
  document.getElementById('prevDay').disabled = (currentDay <= minDay);
  document.getElementById('nextDay').disabled = (currentDay >= maxDay);
}

/**
 * Pobiera listę kolizji z /history_collisions?day=...&max_cpa=...
 */
function fetchCollisionsData() {
  clearCollisionMarkers();

  let url = `/history_collisions?day=${currentDay}&max_cpa=${distFilter}`;
  fetch(url)
    .then(r => r.json())
    .then(data => displayCollisions(data))
    .catch(err => console.error("Error /history_collisions:", err));
}

/**
 * clearCollisionMarkers(): usuwa poprzednie markery kolizyjne
 */
function clearCollisionMarkers() {
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];
}

/**
 * Wyświetla listę kolizji i tworzy JEDEN marker na kolizję.
 */
function displayCollisions(collisions) {
  const list = document.getElementById('collision-list');
  list.innerHTML = '';

  if (!collisions || collisions.length === 0) {
    let noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i style="padding:10px; font-style:italic;">No collisions for this day.</i>';
    list.appendChild(noItem);
    return;
  }

  // 1) Eliminacja duplikatów (jedna kolizja => collision_id)
  let uniqueMap = {};
  collisions.forEach(c => {
    let cid = c.collision_id || `${c.mmsi_a}_${c.mmsi_b}_${c.timestamp||''}`;
    if (!uniqueMap[cid]) {
      uniqueMap[cid] = c;
    } else {
      // Jeżeli chcesz brać nowszy timestamp
      let oldT = new Date(uniqueMap[cid].timestamp).getTime();
      let newT = new Date(c.timestamp).getTime();
      if (newT > oldT) {
        uniqueMap[cid] = c;
      }
    }
  });
  let finalCollisions = Object.values(uniqueMap);
  if(finalCollisions.length === 0) {
    let noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>No collisions for this day.</i>';
    list.appendChild(noItem);
    return;
  }

  // 2) Rysowanie listy i markerów
  finalCollisions.forEach(c => {
    // Nazwy statków (fallback do mmsi)
    let shipA = c.ship1_name || `#${c.mmsi_a}`;
    let shipB = c.ship2_name || `#${c.mmsi_b}`;

    // Zamiast cpa – minimalny dystans (np. c.cpa)
    let distStr = c.cpa ? c.cpa.toFixed(2) : '???';

    // Splitted circle
    let splittedHTML = createSplittedCircle(
      getShipColor(c.ship_length_a||0),
      getShipColor(c.ship_length_b||0)
    );

    // Czas
    let timeStr = '???';
    if (c.timestamp) {
      let dt = new Date(c.timestamp);
      timeStr = dt.toLocaleTimeString('en-GB');
    }

    // Tworzymy entry w liście
    let item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedHTML}
          <strong>${shipA} – ${shipB}</strong><br>
          Distance: ${distStr} nm @ ${timeStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    list.appendChild(item);

    // Event – klik w lupę
    item.querySelector('.zoom-button')
      .addEventListener('click', ()=> zoomToCollision(c));

    // Marker – jedna ikona
    let latC = (c.latitude_a + c.latitude_b)/2;
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

    let tipText = `
      <b>Collision</b><br>
      ${shipA} &amp; ${shipB}<br>
      Dist: ${distStr} nm<br>
      Time: ${timeStr}
    `;

    let marker = L.marker([latC, lonC], { icon: collisionIcon })
      .bindTooltip(tipText, {direction:'top',sticky:true})
      .on('click', ()=> zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

/**
 * zoomToCollision(c) – dopasowuje mapę i wywołuje loadCollisionData
 */
function zoomToCollision(c) {
  let bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, { padding:[20,20] });

  loadCollisionData(c.collision_id);
}

/**
 * loadCollisionData(collision_id):
 *  - pobiera klatki animacji z /history_data
 *  - pokazuje panele
 *  - rysuje klatki
 */
function loadCollisionData(collision_id) {
  fetch(`/history_data?collision_id=${collision_id}`)
    .then(r => r.json())
    .then(data => {
      animationData = data || [];
      animationIndex = 0;
      stopAnimation();
      inSituationView = true;

      // Odsłaniamy panele
      document.getElementById('left-panel').style.display='block';
      document.getElementById('bottom-center-bar').style.display='block';

      updateMapFrame();

      // Dopasowanie do statków 1. klatki (opcjonalnie)
      if (animationData.length>0) {
        let ships = animationData[0].shipPositions||[];
        if (ships.length>0) {
          let latlngs = ships.map(s=>[s.lat,s.lon]);
          let b = L.latLngBounds(latlngs);
          map.fitBounds(b, {padding:[20,20]});
        }
      }
    })
    .catch(err => console.error("Error loading collision data:", err));
}

/** start/stop animacji */
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
 * updateMapFrame(): rysuje statki klatki animationData[animationIndex]
 */
function updateMapFrame() {
  let frameIndicator=document.getElementById('frameIndicator');
  frameIndicator.textContent=`${animationIndex+1}/${animationData.length}`;

  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];

  if(!animationData || animationData.length===0) return;
  let frame=animationData[animationIndex];
  let ships=frame.shipPositions||[];

  ships.forEach(s => {
    let marker = L.marker([s.lat, s.lon], {
      icon: createShipIcon(s, false)
    });
    let nm = s.ship_name || `#${s.mmsi}`;
    let tip=`
      <b>${nm}</b><br>
      COG:${Math.round(s.cog||0)}°, 
      SOG:${(s.sog||0).toFixed(1)} kn<br>
      Len:${s.ship_length||'N/A'}
    `;
    marker.bindTooltip(tip,{direction:'top',sticky:true});
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  // Lewy panel = info
  let leftPanel=document.getElementById('selected-ships-info');
  let pairInfo=document.getElementById('pair-info');
  leftPanel.innerHTML='';
  pairInfo.innerHTML='';

  if(ships.length>=2) {
    let sA=ships[0];
    let sB=ships[1];
    // Oblicz np. dystans w JS (opcjonalnie)
    let dist=0.2, tcpa=4.0; // stub
    let tObj=new Date(frame.time||Date.now());
    let hh=tObj.getHours().toString().padStart(2,'0');
    let mm=tObj.getMinutes().toString().padStart(2,'0');
    let ss=tObj.getSeconds().toString().padStart(2,'0');
    let timeStr=`${hh}:${mm}:${ss}`;

    pairInfo.innerHTML=`
      Time: ${timeStr}<br>
      Dist now: ${dist.toFixed(2)} nm, 
      TCPA: ${tcpa.toFixed(2)} min
    `;
    leftPanel.innerHTML=`
      <b>${sA.ship_name||sA.mmsi}</b><br>
      SOG:${(sA.sog||0).toFixed(1)} kn, COG:${Math.round(sA.cog||0)}°, 
      Len:${sA.ship_length||'N/A'}<br><br>
      <b>${sB.ship_name||sB.mmsi}</b><br>
      SOG:${(sB.sog||0).toFixed(1)} kn, COG:${Math.round(sB.cog||0)}°, 
      Len:${sB.ship_length||'N/A'}
    `;
  }
}

/** Wyjście z trybu animacji */
function exitSituationView() {
  inSituationView=false;
  document.getElementById('left-panel').style.display='none';
  document.getElementById('bottom-center-bar').style.display='none';
  stopAnimation();
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];
  animationData=[];
  animationIndex=0;
}