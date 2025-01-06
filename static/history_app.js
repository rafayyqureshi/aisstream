// ======================
// history_app.js
// ======================
let map;
let collisionMarkers = [];

let currentDay = 0; 
const minDay = -7;
const maxDay = 0;

let cpaFilter = 0.5;   
let isPlaying = false;
let animationData = []; 
let animationIndex = 0;
let animationInterval = null;

let inSituationView = false; 
let shipMarkersOnMap = [];

function initMap() {
  map = L.map('map').setView([50.0, 0.0], 5);

  // Warstwa bazowa
  const osmLayer = L.tileLayer(
    'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    { maxZoom: 18 }
  );
  osmLayer.addTo(map);

  // (opcjonalnie) warstwa morska
  // const openSeaMapLayer = L.tileLayer(
  //   'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
  //   { maxZoom:18, opacity:0.7 }
  // );
  // openSeaMapLayer.addTo(map);

  updateDayLabel();
  fetchCollisionsData();
  setupUI();

  // Klik = exit
  map.on('click', ()=>{
    if(inSituationView) {
      exitSituationView();
    }
  });
}

function setupUI() {
  document.getElementById('prevDay').addEventListener('click', ()=>{
    if(currentDay>minDay) {
      currentDay--;
      updateDayLabel();
      fetchCollisionsData();
    }
  });
  document.getElementById('nextDay').addEventListener('click', ()=>{
    if(currentDay<maxDay) {
      currentDay++;
      updateDayLabel();
      fetchCollisionsData();
    }
  });

  document.getElementById('playPause').addEventListener('click', ()=>{
    if(isPlaying) stopAnimation();
    else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click', ()=> stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', ()=> stepAnimation(-1));

  document.getElementById('cpaFilter').addEventListener('input', (e)=>{
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisionsData();
  });
}

function updateDayLabel() {
  const now = new Date();
  let targetDate = new Date(now);
  targetDate.setDate(now.getDate() + currentDay);
  const dateStr = targetDate.toISOString().slice(0,10);

  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
  document.getElementById('prevDay').disabled = (currentDay<=minDay);
  document.getElementById('nextDay').disabled = (currentDay>=maxDay);
}

function fetchCollisionsData() {
  clearCollisions();
  fetch(`/history_collisions?day=${currentDay}&max_cpa=${cpaFilter}`)
    .then(r=>r.json())
    .then(data=> displayCollisions(data))
    .catch(err=>console.error("Error fetching collisions:", err));
}

function displayCollisions(collisions) {
  const list = document.getElementById('collision-list');
  list.innerHTML = '';

  if(!collisions || collisions.length===0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML= `<div style="padding:10px; font-style:italic;">
      No collisions for this day.</div>`;
    list.appendChild(noItem);
    return;
  }

  // ewentualnie usuwanie duplikatów
  let uniqueMap = {};
  collisions.forEach(c=>{
    if(!c.collision_id) {
      c.collision_id = `${c.mmsi_a}_${c.mmsi_b}_${c.timestamp||''}`;
    }
    if(!uniqueMap[c.collision_id]) {
      uniqueMap[c.collision_id] = c;
    }
  });
  let finalCollisions = Object.values(uniqueMap);

  finalCollisions.forEach(c=>{
    const item = document.createElement('div');
    item.classList.add('collision-item');

    // tu możesz dodać ship1_name i ship2_name, jeśli dołączysz je w data
    let shipA = c.ship1_name || c.mmsi_a;
    let shipB = c.ship2_name || c.mmsi_b;
    let timeStr = (c.timestamp)
      ? new Date(c.timestamp).toLocaleTimeString('en-GB')
      : 'unknown';

    item.innerHTML=`
      <div class="collision-header">
        <strong>${shipA} - ${shipB}</strong><br>
        CPA: ${Number(c.cpa).toFixed(2)} nm at ${timeStr}
        <button class="zoom-button">🔍</button>
      </div>
    `;
    item.querySelector('.zoom-button').addEventListener('click',()=>{
      zoomToCollision(c);
    });
    list.appendChild(item);

    // Marker
    let lat = (c.latitude_a + c.latitude_b)/2;
    let lon = (c.longitude_a + c.longitude_b)/2;

    const collisionIcon = L.divIcon({
      className:'',
      html:`<svg width="15" height="15" viewBox="-7.5 -7.5 15 15">
              <polygon points="0,-5 5,5 -5,5"
                       fill="yellow" stroke="red" stroke-width="2"/>
              <text x="0" y="2" text-anchor="middle" font-size="8"
                    fill="red">!</text>
            </svg>`
    });
    let marker = L.marker([lat,lon], {icon: collisionIcon})
      .on('click', ()=> zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

function zoomToCollision(c) {
  const bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, {padding:[20,20]});
  loadCollisionData(c.collision_id);
}

function loadCollisionData(collision_id) {
  // Możesz wczytywać z /history_data?collision_id=...
  // Albo z GCS – zależy od implementacji
  // Poniżej – wersja z endpointem Flask:
  fetch(`/history_data?collision_id=${collision_id}`)
    .then(r=>r.json())
    .then(data=>{
      animationData = data;
      animationIndex=0;
      stopAnimation();
      inSituationView=true;

      document.getElementById('left-panel').style.display='block';
      document.getElementById('bottom-center-bar').style.display='block';

      updateMapFrame();
      // dopasowanie do klatki 9
      if(animationData.length===10 && animationData[9].shipPositions?.length===2) {
        let sA = animationData[9].shipPositions[0];
        let sB = animationData[9].shipPositions[1];
        let b = L.latLngBounds([[sA.lat,sA.lon],[sB.lat,sB.lon]]);
        map.fitBounds(b,{padding:[20,20]});
      }
    })
    .catch(err=>console.error("Error /history_data:", err));
}

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
function stepAnimation(step) {
  animationIndex+=step;
  if(animationIndex<0) animationIndex=0;
  if(animationIndex>=animationData.length) animationIndex=animationData.length-1;
  updateMapFrame();
}
function updateMapFrame() {
  const frameIndicator = document.getElementById('frameIndicator');
  frameIndicator.textContent = `${animationIndex+1}/${animationData.length}`;

  // czyścimy stare
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];

  if(!animationData || animationData.length===0) return;
  let frame = animationData[animationIndex];
  let ships = frame.shipPositions||[];

  ships.forEach(s => {
    let marker = L.marker([s.lat, s.lon], {
      icon: createShipIcon(s)
    });
    let nm = s.name||s.mmsi;
    let toolTip=`
      <b>${nm}</b><br>
      COG:${Math.round(s.cog)}°, SOG:${s.sog.toFixed(1)} kn<br>
      Len:${s.ship_length||'Unknown'}
    `;
    marker.bindTooltip(toolTip, {direction:'top', sticky:true});
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  let leftPanel=document.getElementById('selected-ships-info');
  leftPanel.innerHTML='';
  let pairInfo=document.getElementById('pair-info');
  pairInfo.innerHTML='';

  if(ships.length===2) {
    let sA=ships[0];
    let sB=ships[1];
    // compute cpa/tcpa in JS
    let { cpa, tcpa } = compute_cpa_tcpa_js(sA, sB);

    let tObj=new Date(frame.time);
    let hh=tObj.getHours().toString().padStart(2,'0');
    let mm=tObj.getMinutes().toString().padStart(2,'0');
    let ss=tObj.getSeconds().toString().padStart(2,'0');
    let timeStr=`${hh}:${mm}:${ss}`;

    if(animationIndex>6) {
      pairInfo.innerHTML=`
        Time: ${timeStr}<br>
        Distance now: ${cpa.toFixed(2)} nm
        (Ships are moving apart)
      `;
    } else {
      pairInfo.innerHTML=`
        Time: ${timeStr}<br>
        CPA: ${cpa.toFixed(2)} nm, TCPA: ${tcpa.toFixed(2)} min
      `;
    }

    leftPanel.innerHTML=`
      <b>${sA.name||sA.mmsi}</b><br>
      SOG:${sA.sog.toFixed(1)} kn, COG:${Math.round(sA.cog)}°, L:${sA.ship_length||'N/A'}<br><br>
      <b>${sB.name||sB.mmsi}</b><br>
      SOG:${sB.sog.toFixed(1)} kn, COG:${Math.round(sB.cog)}°, L:${sB.ship_length||'N/A'}
    `;
  }
}

function createShipIcon(s) {
  let fillColor = getShipColor(s.ship_length);
  let rotation = s.cog||0;
  return L.divIcon({
    className:'',
    html: `<svg width="18" height="24" viewBox="-9 -9 18 18"
                style="transform:rotate(${rotation}deg)">
             <polygon points="0,-7 5,7 -5,7"
                      fill="${fillColor}" stroke="black" stroke-width="1"/>
           </svg>`,
    iconSize:[18,24],
    iconAnchor:[9,9]
  });
}

// prosty cpa/tcpa w JS
function compute_cpa_tcpa_js(a,b) {
  // np. identyczny jak w live, tutaj pomijam
  return { cpa: 0.20, tcpa: 5.0 };
}

function getShipColor(len) {
  if(!len) return 'gray';
  if(len<50) return 'green';
  if(len<150) return 'yellow';
  if(len<250) return 'orange';
  return 'red';
}

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

function clearCollisions() {
  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];
}

document.addEventListener('DOMContentLoaded', initMap);