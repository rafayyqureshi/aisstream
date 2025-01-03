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
  // Base OSM layer
  const osmLayer = L.tileLayer(
    'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    { maxZoom:18 }
  );
  osmLayer.addTo(map);

  updateDayLabel();
  fetchCollisionsData();
  setupUI();

  map.on('click', ()=>{
    if(inSituationView) {
      exitSituationView();
    }
  });
}

function setupUI() {
  // Day controls
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

  // Anim controls
  document.getElementById('playPause').addEventListener('click', ()=>{
    if(isPlaying) stopAnimation(); 
    else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click', ()=> stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', ()=> stepAnimation(-1));

  // Filter cpa
  document.getElementById('cpaFilter').addEventListener('input', (e)=>{
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisionsData();
  });
}

function updateDayLabel() {
  const now = new Date();
  let d = new Date(now);
  d.setDate(now.getDate() + currentDay);
  const dateStr = d.toISOString().slice(0,10);

  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
  document.getElementById('prevDay').disabled = (currentDay<=minDay);
  document.getElementById('nextDay').disabled = (currentDay>=maxDay);
}

function fetchCollisionsData() {
  clearCollisions();
  // e.g. /history_collisions?day=...&max_cpa=...
  fetch(`/history_collisions?day=${currentDay}&max_cpa=${cpaFilter}`)
    .then(r=>r.json())
    .then(data=>{
      displayCollisions(data);
    })
    .catch(err=>console.error("Error fetching collisions:", err));
}

function displayCollisions(collisions) {
  const list = document.getElementById('collision-list');
  list.innerHTML='';

  if(!collisions || collisions.length===0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML='<div style="padding:10px;font-style:italic;">No collisions for this day.</div>';
    list.appendChild(noItem);
    return;
  }

  collisions.forEach(c=>{
    const item = document.createElement('div');
    item.classList.add('collision-item');

    let shipA = c.ship1_name || c.mmsi_a;
    let shipB = c.ship2_name || c.mmsi_b;
    let timeStr = c.timestamp ? new Date(c.timestamp).toLocaleTimeString('en-GB') : '???';

    // Kolory statków (opcjonalnie)
    // let colorA = ...
    // let colorB = ...

    // Twój splitted circle
    // let splittedCircle = createSplittedCircle(colorA, colorB);

    item.innerHTML=`
      <div class="collision-header" style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <strong>${shipA} - ${shipB}</strong><br>
          CPA: ${Number(c.cpa).toFixed(2)} nm @ ${timeStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    // splitted circle i inne elementy w razie potrzeby

    item.querySelector('.zoom-button').addEventListener('click', ()=>{
      zoomToCollision(c);
    });
    list.appendChild(item);

    // Marker
    let lat = (c.latitude_a + c.latitude_b)/2;
    let lon = (c.longitude_a + c.longitude_b)/2;

    const collisionIcon = L.divIcon({
      className: '',
      html: `
        <svg width="15" height="15" viewBox="-7.5 -7.5 15 15">
          <polygon points="0,-5 5,5 -5,5" fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="2" text-anchor="middle" font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize:[15,15],
      iconAnchor:[7.5,7.5]
    });

    let marker = L.marker([lat, lon], {icon: collisionIcon})
      .on('click', ()=>zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

// ewentualnie splitted circle:
function createSplittedCircle(colorA, colorB){
  return `
    <svg width="16" height="16" viewBox="0 0 16 16" style="vertical-align:middle;margin-right:6px;">
      <path d="M8,8 m-8,0 a8,8 0 0,1 16,0 z" fill="${colorA}"/>
      <path d="M8,8 m8,0 a8,8 0 0,1 -16,0 z" fill="${colorB}"/>
    </svg>
  `;
}

function zoomToCollision(c){
  let bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, {padding:[20,20]});

  // Wczytanie danych 10 klatek z GCS lub z /history_data
  loadCollisionData(c.collision_id);
}

// Przykład wczytania z GCS:
function loadCollisionData(collision_id){
  let url = `https://storage.googleapis.com/your_bucket/history_collisions_batch/${collision_id}.json`;

  fetch(url)
    .then(r=>{
      if(!r.ok) throw new Error("HTTP error " + r.status);
      return r.text();
    })
    .then(text=>{
      animationData = JSON.parse(text); 
      animationIndex=0;
      stopAnimation();
      inSituationView=true;

      document.getElementById('left-panel').style.display='block';
      document.getElementById('bottom-center-bar').style.display='block';

      updateMapFrame();

      // ewentualne dopasowanie mapy do klatki 10
      if(animationData.length===10 && animationData[9].shipPositions.length===2){
        let sA=animationData[9].shipPositions[0];
        let sB=animationData[9].shipPositions[1];
        let b = L.latLngBounds([[sA.lat,sA.lon],[sB.lat,sB.lon]]);
        map.fitBounds(b,{padding:[20,20]});
      }
    })
    .catch(err=>{
      console.error("Error loading collision data from GCS:", err);
    });
}

// Animacja
function startAnimation(){
  if(!animationData || animationData.length===0) return;
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
  animationIndex += step;
  if(animationIndex<0) animationIndex=0;
  if(animationIndex>=animationData.length) animationIndex=animationData.length-1;
  updateMapFrame();
}
function updateMapFrame(){
  const frameIndicator=document.getElementById('frameIndicator');
  frameIndicator.textContent=`${animationIndex+1}/${animationData.length}`;

  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];

  if(!animationData || animationData.length===0) return;
  let frame=animationData[animationIndex];
  let ships=frame.shipPositions||[];

  ships.forEach(s=>{
    let marker = L.marker([s.lat,s.lon],{
      icon:createShipIcon(s)
    });
    let nm = s.name||s.mmsi;
    let toolTip=`
      <b>${nm}</b><br>
      COG:${Math.round(s.cog)}°, SOG:${s.sog.toFixed(1)}kn<br>
      Len:${s.ship_length||'Unknown'}
    `;
    marker.bindTooltip(toolTip,{direction:'top',sticky:true});
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });
  
  // W lewym panelu
  let leftPanel=document.getElementById('selected-ships-info');
  leftPanel.innerHTML='';
  let pairInfo=document.getElementById('pair-info');
  pairInfo.innerHTML='';

  if(ships.length===2){
    let sA=ships[0];
    let sB=ships[1];
    let {cpa, tcpa}=compute_cpa_tcpa_js(sA, sB);

    // Czas klatki
    let tObj=new Date(frame.time);
    let hh=tObj.getHours().toString().padStart(2,'0');
    let mm=tObj.getMinutes().toString().padStart(2,'0');
    let ss=tObj.getSeconds().toString().padStart(2,'0');
    let timeStr=`${hh}:${mm}:${ss}`;

    if(animationIndex>6){
      pairInfo.innerHTML=`
        Time: ${timeStr}<br>
        Dist now: ${cpa.toFixed(2)} nm
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
      SOG:${sA.sog.toFixed(1)} kn, COG:${Math.round(sA.cog)}°, L:${sA.ship_length||'?'}<br><br>
      <b>${sB.name||sB.mmsi}</b><br>
      SOG:${sB.sog.toFixed(1)} kn, COG:${Math.round(sB.cog)}°, L:${sB.ship_length||'?'}
    `;
  }
}

// Pomocnicza – oblicz cpa/tcpa w JS, podobnie jak w "live"
function compute_cpa_tcpa_js(a,b){
  // ...
  // identycznie jak w poprzednich wersjach
  return {cpa:0.2, tcpa:5.0}; // stub
}

function createShipIcon(s){
  let fillColor = getShipColor(s.ship_length);
  let rotation = s.cog||0;
  return L.divIcon({
    className:'',
    html:`
      <svg width="18" height="24" viewBox="-9 -9 18 18" style="transform:rotate(${rotation}deg)">
        <polygon points="0,-7 5,7 -5,7"
                 fill="${fillColor}" stroke="black" stroke-width="1"/>
      </svg>
    `,
    iconSize:[18,24],
    iconAnchor:[9,9]
  });
}
function getShipColor(len){
  if(!len) return 'gray';
  if(len<50) return 'green';
  if(len<150) return 'yellow';
  if(len<250) return 'orange';
  return 'red';
}

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

function clearCollisions(){
  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];
}

document.addEventListener('DOMContentLoaded', initMap);