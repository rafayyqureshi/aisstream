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
  const osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom:18});
  osmLayer.addTo(map);

  updateDayLabel();
  fetchCollisionsData();

  setupUI();

  // Klik na mapę = wyjście z podglądu sytuacji
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
    cpaFilter = parseFloat(e.target.value);
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisionsData();
  });
}

function updateDayLabel() {
  const now = new Date();
  let dayDate = new Date(now);
  dayDate.setDate(now.getDate() + currentDay);
  const dateStr = dayDate.toISOString().slice(0,10);
  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
  document.getElementById('prevDay').disabled = (currentDay<=minDay);
  document.getElementById('nextDay').disabled = (currentDay>=maxDay);
}

function fetchCollisionsData() {
  clearCollisions();
  fetch(`/history_collisions?day=${currentDay}&max_cpa=${cpaFilter}`)
    .then(r=>r.json())
    .then(data=>{
      displayCollisions(data);
    })
    .catch(err=>console.error("Error fetching collisions:", err));
}

function displayCollisions(collisions) {
  const list = document.getElementById('collision-list');
  list.innerHTML = '';

  if(collisions.length===0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML= `<div style="padding:10px; font-style:italic;">No collisions for this day.</div>`;
    list.appendChild(noItem);
    return;
  }

  collisions.forEach(c=>{
    const item = document.createElement('div');
    item.classList.add('collision-item');
    const shipA = c.ship1_name || c.mmsi_a;
    const shipB = c.ship2_name || c.mmsi_b;
    let timeStr = c.timestamp ? new Date(c.timestamp).toLocaleTimeString('en-GB'): 'unknown';

    item.innerHTML=`
      <div class="collision-header">
        <strong>${shipA} - ${shipB}</strong><br>
        CPA: ${c.cpa.toFixed(2)} nm at ${timeStr}
        <button class="zoom-button">🔍</button>
      </div>
    `;
    item.querySelector('.zoom-button').addEventListener('click',()=>{
      zoomToCollision(c);
    });
    list.appendChild(item);

    // Rysuj marker
    let lat = (c.latitude_a + c.latitude_b)/2;
    let lon = (c.longitude_a + c.longitude_b)/2;

    const icon = L.divIcon({
      className:'',
      html:`<svg width="15" height="15" viewBox="-7.5 -7.5 15 15">
              <polygon points="0,-5 5,5 -5,5" fill="yellow" stroke="red" stroke-width="2"/>
              <text x="0" y="2" text-anchor="middle" font-size="8" fill="red">!</text>
            </svg>`
    });
    let marker = L.marker([lat, lon], {icon:icon})
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
  map.fitBounds(bounds,{padding:[20,20]});
  loadCollisionData(c.collision_id, c);
}

function loadCollisionData(collision_id, collisionData) {
  fetch(`/history_data?collision_id=${collision_id}`)
    .then(r=>r.json())
    .then(data=>{
      animationData = data; // tablica 10 klatek
      animationIndex = 0;
      stopAnimation();
      inSituationView = true;

      document.getElementById('left-panel').style.display='block';
      document.getElementById('bottom-center-bar').style.display='block';

      updateMapFrame();

      // ewentualny fit w klatce 10
      if(animationData.length===10 && animationData[9].shipPositions.length===2){
        let sA = animationData[9].shipPositions[0];
        let sB = animationData[9].shipPositions[1];
        const bounds = L.latLngBounds([[sA.lat,sA.lon],[sB.lat,sB.lon]]);
        map.fitBounds(bounds,{padding:[20,20]});
      }
    })
    .catch(err => console.error("Error fetching collision data:", err));
}

function startAnimation() {
  if(animationData.length===0) return;
  isPlaying = true;
  document.getElementById('playPause').textContent='Pause';
  animationInterval = setInterval(()=> stepAnimation(1), 1000);
}

function stopAnimation() {
  isPlaying = false;
  document.getElementById('playPause').textContent='Play';
  if(animationInterval) clearInterval(animationInterval);
  animationInterval=null;
}

function stepAnimation(step) {
  animationIndex += step;
  if(animationIndex<0) animationIndex=0;
  if(animationIndex>=animationData.length) animationIndex=animationData.length-1;
  updateMapFrame();
}

function updateMapFrame() {
  const frameIndicator = document.getElementById('frameIndicator');
  frameIndicator.textContent = `${animationIndex+1}/10`;

  // czyścimy stare markery
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];

  if(animationData.length===0) return;
  let frame = animationData[animationIndex];
  let ships = frame.shipPositions || [];

  ships.forEach(s=>{
    // rysujemy “statek”
    const icon = createShipIcon(s);
    let marker = L.marker([s.lat, s.lon], {icon:icon});
    let tooltipHTML = `${s.name||s.mmsi}<br>COG:${Math.round(s.cog)}° SOG:${s.sog.toFixed(1)} kn<br>Len:${s.ship_length||'Unknown'}`;
    marker.bindTooltip(tooltipHTML,{direction:'top',sticky:true});
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  // panel info
  let container = document.getElementById('selected-ships-info');
  container.innerHTML='';

  if(ships.length===2) {
    let sA=ships[0];
    let sB=ships[1];
    // ewentualnie oblicz cpa/tcpa
    let nowTime = new Date(frame.time);
    let hh=nowTime.getHours().toString().padStart(2,'0');
    let mm=nowTime.getMinutes().toString().padStart(2,'0');
    let ss=nowTime.getSeconds().toString().padStart(2,'0');
    let timeStr=`${hh}:${mm}:${ss}`;

    // decyduj, czy "vessels are moving apart"
    if(animationIndex>6) {
      document.getElementById('pair-info').innerHTML = `
        Time: ${timeStr}<br>Vessels are moving apart
      `;
    } else {
      // w Twoim kodzie jest compute_cpa_tcpa_js
      let {cpa, tcpa} = compute_cpa_tcpa_js(sA, sB);
      document.getElementById('pair-info').innerHTML=`
        Time: ${timeStr}<br>CPA: ${cpa.toFixed(2)}, TCPA: ${tcpa.toFixed(2)}
      `;
    }
    container.innerHTML=`
      <b>${sA.name||sA.mmsi}</b><br>
      COG:${Math.round(sA.cog)}°, SOG:${sA.sog.toFixed(1)} kn<br><br>
      <b>${sB.name||sB.mmsi}</b><br>
      COG:${Math.round(sB.cog)}°, SOG:${sB.sog.toFixed(1)} kn
    `;
  } else {
    document.getElementById('pair-info').innerHTML='';
  }
}

function createShipIcon(s) {
  let fillColor='yellow';
  if(s.ship_length!==null) {
    if(s.ship_length<50) fillColor='green';
    else if(s.ship_length<150) fillColor='yellow';
    else if(s.ship_length<250) fillColor='orange';
    else fillColor='red';
  }
  let rotation = s.cog||0;
  let icon = L.divIcon({
    className:'',
    html: `<svg width="18" height="24" viewBox="-9 -9 18 18" style="transform:rotate(${rotation}deg)">
             <polygon points="0,-7 5,7 -5,7" fill="${fillColor}" stroke="black" stroke-width="1"/>
           </svg>`,
    iconSize:[18,24],
    iconAnchor:[9,9]
  });
  return icon;
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

function compute_cpa_tcpa_js(a, b) {
  // prosty JS obliczanie cpa/tcpa
  // (możesz tu wstawić to samo co w "live")
  return {cpa:0.2, tcpa:5.0};
}

document.addEventListener('DOMContentLoaded', initMap);