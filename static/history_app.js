let map;
let collisionMarkers = [];
let selectedShips = [];
let currentDay = 0; // 0=today, -1=yesterday, etc.
let cpaFilter = 0.5;
let tcpaFilter = 10;
let playbackSpeed = 1;
let isPlaying = false;

// Data for animation
let currentCollisionId = null;
let animationData = []; // {time, shipPositions:[{mmsi,lat,lon,sog,cog}]}
let animationIndex = 0;
let animationInterval = null;

function initMap() {
  map = L.map('map').setView([50.0, 0.0], 6);
  const osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom:18});
  osmLayer.addTo(map);

  fetchCollisionsData();
  setupUI();
}

function setupUI() {
  document.getElementById('prevDay').addEventListener('click', ()=>{
    currentDay -= 1;
    updateDayLabel();
    fetchCollisionsData();
  });
  document.getElementById('nextDay').addEventListener('click', ()=>{
    currentDay += 1;
    updateDayLabel();
    fetchCollisionsData();
  });

  document.getElementById('cpaFilter').addEventListener('input', (e)=>{
    cpaFilter = parseFloat(e.target.value);
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisionsData();
  });
  document.getElementById('tcpaFilter').addEventListener('input', (e)=>{
    tcpaFilter = parseFloat(e.target.value);
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    fetchCollisionsData();
  });

  document.getElementById('playbackSpeed').addEventListener('change',(e)=>{
    playbackSpeed = parseInt(e.target.value);
    if(isPlaying) {
      stopAnimation();
      startAnimation();
    }
  });

  document.getElementById('playPause').addEventListener('click', ()=>{
    if(isPlaying) {
      stopAnimation();
    } else {
      startAnimation();
    }
  });
  document.getElementById('stepForward').addEventListener('click', ()=>{
    stepAnimation(1);
  });
  document.getElementById('stepBack').addEventListener('click', ()=>{
    stepAnimation(-1);
  });
}

function updateDayLabel() {
  let label = "Day: "+currentDay;
  if(currentDay===0) label+=" (Today)";
  document.getElementById('currentDayLabel').textContent = label;
}

function fetchCollisionsData() {
  clearCollisions();
  fetch(`/history_collisions?day=${currentDay}&max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(r=>r.json())
    .then(data=>{
      displayCollisions(data);
    })
    .catch(err=>console.error("Error fetching history collisions:", err));
}

function displayCollisions(collisions) {
  const list = document.getElementById('collision-list');
  list.innerHTML = '';
  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];

  if(collisions.length===0) {
    const noColl = document.createElement('div');
    noColl.classList.add('collision-item');
    noColl.innerHTML = `<div style="padding:10px; font-style:italic;">No collisions detected for this day.</div>`;
    list.appendChild(noColl);
    return;
  }

  collisions.forEach(c=>{
    const item = document.createElement('div');
    item.classList.add('collision-item');
    const shipA = c.ship1_name || c.mmsi_a;
    const shipB = c.ship2_name || c.mmsi_b;
    item.innerHTML=`
      <div class="collision-header">
        <strong>${shipA} - ${shipB}</strong><br>
        CPA: ${c.cpa.toFixed(2)} nm, TCPA: ${c.tcpa.toFixed(2)} min
        <button class="zoom-button">🔍</button>
      </div>
    `;

    item.querySelector('.zoom-button').addEventListener('click', ()=>{
      zoomToCollision(c);
    });

    list.appendChild(item);

    const collisionLat = (c.latitude_a+c.latitude_b)/2;
    const collisionLon = (c.longitude_a+c.longitude_b)/2;

    const collisionIcon = L.divIcon({
      className:'',
      html:`<svg width="15" height="15" viewBox="-7.5 -7.5 15 15">
        <polygon points="0,-5 5,5 -5,5" fill="yellow" stroke="red" stroke-width="2"/>
        <text x="0" y="2" text-anchor="middle" font-size="8" font-weight="bold" fill="red">!</text>
      </svg>`,
      iconSize:[15,15],
      iconAnchor:[7.5,7.5]
    });

    const marker = L.marker([collisionLat, collisionLon], {icon: collisionIcon});
    marker.on('click',()=>{ zoomToCollision(c); });
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

function zoomToCollision(c) {
  const bounds = L.latLngBounds([[c.latitude_a,c.longitude_a],[c.latitude_b,c.longitude_b]]);
  map.fitBounds(bounds,{padding:[50,50]});
  loadCollisionData(c.collision_id);
}

function loadCollisionData(collision_id) {
  currentCollisionId = collision_id;
  fetch(`/history_data?collision_id=${collision_id}`)
    .then(r=>r.json())
    .then(data=>{
      animationData = data;
      animationIndex = 0;
      stopAnimation();
      updateMapFrame();
    })
    .catch(err=>console.error("Error fetching collision data:", err));
}

function updateMapFrame() {
  // Here we update ship positions on map and panel
  // This is simplified placeholder
  // animationData[animationIndex].shipPositions = [{mmsi,lat,lon,sog,cog}]
  
  let frame = animationData[animationIndex];
  // Update ships and panels...
}

function startAnimation() {
  if(animationData.length===0) return;
  isPlaying = true;
  document.getElementById('playPause').textContent="Pause";

  animationInterval = setInterval(()=>{
    stepAnimation(1);
  }, 1000/(playbackSpeed)); // simplistic approach
}

function stopAnimation() {
  isPlaying = false;
  document.getElementById('playPause').textContent="Play";
  if(animationInterval) clearInterval(animationInterval);
  animationInterval=null;
}

function stepAnimation(step) {
  animationIndex += step;
  if(animationIndex<0) animationIndex=0;
  if(animationIndex>=animationData.length) animationIndex=animationData.length-1;
  updateMapFrame();
}

function clearCollisions() {
  const list = document.getElementById('collision-list');
  list.innerHTML='';
  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];
}

document.addEventListener('DOMContentLoaded', initMap);