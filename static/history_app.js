// ======================
// history_app.js
// ======================
let map;
let collisionMarkers = [];

let currentDay = 0; 
const minDay = -7;
const maxDay = 0;

let cpaFilter = 0.5;   // filtry
let isPlaying = false;
let animationData = [];  // tablica 10 klatek
let animationIndex = 0;
let animationInterval = null;

let inSituationView = false;  // czy jesteśmy w podglądzie kolizji
let shipMarkersOnMap = [];

function initMap() {
  // Inicjalizacja mapy
  map = L.map('map').setView([50.0, 0.0], 5);

  // Warstwa bazowa OSM
  const osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom:18
  });
  osmLayer.addTo(map);

  // Ewentualnie warstwa morska
  // const seaMapLayer = L.tileLayer(
  //   'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
  //   { maxZoom:18, opacity:0.7 }
  // );
  // seaMapLayer.addTo(map);

  updateDayLabel();
  fetchCollisionsData();
  setupUI();

  // Klik w mapę => wyjście z podglądu (jeśli w nim jesteśmy)
  map.on('click', ()=>{
    if(inSituationView) {
      exitSituationView();
    }
  });
}

function setupUI() {
  // Przełączanie dni
  document.getElementById('prevDay').addEventListener('click', ()=>{
    if(currentDay > minDay) {
      currentDay--;
      updateDayLabel();
      fetchCollisionsData();
    }
  });
  document.getElementById('nextDay').addEventListener('click', ()=>{
    if(currentDay < maxDay) {
      currentDay++;
      updateDayLabel();
      fetchCollisionsData();
    }
  });

  // Animacja
  document.getElementById('playPause').addEventListener('click', ()=>{
    if(isPlaying) stopAnimation();
    else startAnimation();
  });
  document.getElementById('stepForward').addEventListener('click', ()=> stepAnimation(1));
  document.getElementById('stepBack').addEventListener('click', ()=> stepAnimation(-1));

  // Filtr cpa
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
  document.getElementById('prevDay').disabled = (currentDay <= minDay);
  document.getElementById('nextDay').disabled = (currentDay >= maxDay);
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

  if(collisions.length === 0) {
    const noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = `<div style="padding:10px; font-style:italic;">No collisions for this day.</div>`;
    list.appendChild(noItem);
    return;
  }

  collisions.forEach(c=>{
    const item = document.createElement('div');
    item.classList.add('collision-item');

    let shipA = c.ship1_name || c.mmsi_a;
    let shipB = c.ship2_name || c.mmsi_b;
    let timeStr = (c.timestamp)
      ? new Date(c.timestamp).toLocaleTimeString('en-GB')
      : 'unknown';

    // Półkola (kolor zależny od ship_length)
    let colorA = getShipColor(c.ship1_length);
    let colorB = getShipColor(c.ship2_length);
    let splittedCircle = createSplittedCircle(colorA, colorB);

    item.innerHTML = `
      <div class="collision-header" style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedCircle}
          <strong>${shipA} - ${shipB}</strong><br>
          CPA: ${c.cpa.toFixed(2)} nm @ ${timeStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    item.querySelector('.zoom-button').addEventListener('click', ()=>{
      zoomToCollision(c);
    });
    list.appendChild(item);

    // Marker kolizji na mapie
    let lat = (c.latitude_a + c.latitude_b)/2;
    let lon = (c.longitude_a + c.longitude_b)/2;
    const collisionIcon = L.divIcon({
      className: '',
      html: createCollisionIcon(colorA, colorB, c.cpa, timeStr),
      iconSize: [20,20],
      iconAnchor: [10,10]
    });
    let marker = L.marker([lat, lon], {icon: collisionIcon})
      .on('click', ()=> zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

function createSplittedCircle(colorA, colorB) {
  // Tworzy mały okrąg podzielony pionowo
  return `
  <svg width="16" height="16" viewBox="0 0 16 16" style="vertical-align:middle; margin-right:6px;">
    <!-- lewa połówka -->
    <path d="M8,8 m-8,0 a8,8 0 0,1 16,0 z" fill="${colorA}"/>
    <!-- prawa połówka -->
    <path d="M8,8 m8,0 a8,8 0 0,1 -16,0 z" fill="${colorB}"/>
  </svg>
  `;
}

function createCollisionIcon(colorA, colorB, cpaVal, timeStr) {
  // Ikona kolizji na mapie (również splitted circle + '!' w środku)
  return `
  <svg width="20" height="20" viewBox="0 0 20 20">
    <!-- lewa połówka -->
    <path d="M10,10 m-10,0 a10,10 0 0,1 20,0 z" fill="${colorA}"/>
    <!-- prawa połówka -->
    <path d="M10,10 m10,0 a10,10 0 0,1 -20,0 z" fill="${colorB}"/>

    <text x="10" y="12" text-anchor="middle" font-size="8" fill="red" font-weight="bold">!</text>
  </svg>
  `;
}

function getShipColor(lenVal) {
  if(!lenVal) return 'gray';
  if(lenVal < 50) return 'green';
  if(lenVal < 150) return 'yellow';
  if(lenVal < 250) return 'orange';
  return 'red';
}

function zoomToCollision(c) {
  const bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, {padding:[20,20]});
  loadCollisionData(c.collision_id, c);
}

// Ładujemy dane 10 klatek z /history_data
function loadCollisionData(collision_id, collisionData) {
  fetch(`/history_data?collision_id=${collision_id}`)
    .then(r=>r.json())
    .then(data=>{
      animationData = data;
      animationIndex = 0;
      stopAnimation();
      inSituationView = true;

      document.getElementById('left-panel').style.display = 'block';
      document.getElementById('bottom-center-bar').style.display = 'block';

      updateMapFrame();

      // Fit do ostatniej klatki
      if(animationData.length===10 && animationData[9].shipPositions.length===2){
        let sA = animationData[9].shipPositions[0];
        let sB = animationData[9].shipPositions[1];
        const bounds = L.latLngBounds([[sA.lat,sA.lon],[sB.lat,sB.lon]]);
        map.fitBounds(bounds, {padding:[20,20]});
      }
    })
    .catch(err=>console.error("Error fetching collision data:", err));
}

// Animacja
function startAnimation() {
  if(animationData.length===0) return;
  isPlaying = true;
  document.getElementById('playPause').textContent = 'Pause';
  animationInterval = setInterval(()=> stepAnimation(1), 1000);
}

function stopAnimation() {
  isPlaying = false;
  document.getElementById('playPause').textContent = 'Play';
  if(animationInterval) clearInterval(animationInterval);
  animationInterval = null;
}

function stepAnimation(step) {
  animationIndex += step;
  if(animationIndex<0) animationIndex = 0;
  if(animationIndex>=animationData.length) animationIndex=animationData.length-1;
  updateMapFrame();
}

function updateMapFrame() {
  const frameIndicator = document.getElementById('frameIndicator');
  frameIndicator.textContent = `${animationIndex+1}/10`;

  // Czyścimy poprzednie statki z mapy
  shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  shipMarkersOnMap=[];

  if(animationData.length===0) return;
  let frame = animationData[animationIndex];
  let ships = frame.shipPositions || [];

  ships.forEach(s=>{
    let marker = L.marker([s.lat, s.lon], {
      icon: createShipIcon(s)
    });
    let nm = s.name || s.mmsi;
    let toolTip = `
      <b>${nm}</b><br>
      COG: ${Math.round(s.cog)}°, SOG: ${s.sog.toFixed(1)} kn<br>
      Length: ${s.ship_length||'Unknown'}
    `;
    marker.bindTooltip(toolTip,{direction:'top',sticky:true});
    marker.addTo(map);
    shipMarkersOnMap.push(marker);
  });

  // Lewy panel: info
  let leftPanel = document.getElementById('selected-ships-info');
  leftPanel.innerHTML = '';

  let pairInfo = document.getElementById('pair-info');
  pairInfo.innerHTML = '';

  if(ships.length===2) {
    let sA = ships[0];
    let sB = ships[1];

    let {cpa, tcpa} = compute_cpa_tcpa_js(sA, sB);

    // Czas klatki
    let tObj = new Date(frame.time);
    let hh = tObj.getHours().toString().padStart(2,'0');
    let mm = tObj.getMinutes().toString().padStart(2,'0');
    let ss = tObj.getSeconds().toString().padStart(2,'0');
    let timeStr = `${hh}:${mm}:${ss}`;

    // Gdy animationIndex > 6 => statki się oddalają
    if(animationIndex>6) {
      pairInfo.innerHTML=`
        Time: ${timeStr}<br>
        Distance now: ${cpa.toFixed(2)} nm <br>
        (Ships are moving apart)
      `;
    } else {
      pairInfo.innerHTML=`
        Time: ${timeStr}<br>
        CPA: ${cpa.toFixed(2)} nm,
        TCPA: ${tcpa.toFixed(2)} min
      `;
    }

    leftPanel.innerHTML=`
      <b>${sA.name||sA.mmsi}</b><br>
      SOG: ${sA.sog.toFixed(1)} kn, COG: ${Math.round(sA.cog)}°, Len: ${sA.ship_length||'N/A'}<br><br>
      <b>${sB.name||sB.mmsi}</b><br>
      SOG: ${sB.sog.toFixed(1)} kn, COG: ${Math.round(sB.cog)}°, Len: ${sB.ship_length||'N/A'}
    `;
  }
}

function createShipIcon(s) {
  // Kolor zależny od długości
  let fillColor = getShipColor(s.ship_length);
  let rotation = s.cog || 0;
  return L.divIcon({
    className:'',
    html: `
      <svg width="18" height="24" viewBox="-9 -9 18 18"
           style="transform:rotate(${rotation}deg)">
        <polygon points="0,-7 5,7 -5,7"
                 fill="${fillColor}" stroke="black" stroke-width="1"/>
      </svg>
    `,
    iconSize:[18,24],
    iconAnchor:[9,9]
  });
}

function getShipColor(lenVal) {
  if(!lenVal) return 'gray';
  if(lenVal < 50) return 'green';
  if(lenVal < 150) return 'yellow';
  if(lenVal < 250) return 'orange';
  return 'red';
}

// Przykładowe cpa/tcpa w JS
function compute_cpa_tcpa_js(a, b) {
  if(!a.ship_length || !b.ship_length) return { cpa:9999, tcpa:-1 };
  if(a.ship_length<50 || b.ship_length<50) return { cpa:9999, tcpa:-1 };

  let latRef=(a.lat+b.lat)/2;
  let scaleLat=111000;
  let scaleLon=111000*Math.cos(latRef*Math.PI/180);

  function toXY(lat,lon){
    return [lon*scaleLon, lat*scaleLat];
  }
  let [xA,yA]=toXY(a.lat,a.lon);
  let [xB,yB]=toXY(b.lat,b.lon);

  let sogA=a.sog||0;
  let sogB=b.sog||0;
  function cogToVec(cogDeg, sogNmH){
    let rad=(cogDeg||0)*Math.PI/180;
    return [
      sogNmH*Math.sin(rad),
      sogNmH*Math.cos(rad)
    ];
  }
  let [vxA,vyA]=cogToVec(a.cog,sogA);
  let [vxB,vyB]=cogToVec(b.cog,sogB);

  let dx=xA-xB;
  let dy=yA-yB;
  let dvx=vxA-vxB;
  let dvy=vyA-vyB;

  let speedScale=1852/60; // nm/h -> m/min
  let dvx_mpm=dvx*speedScale;
  let dvy_mpm=dvy*speedScale;

  let VV_m=dvx_mpm**2 + dvy_mpm**2;
  let PV_m=dx*dvx_mpm + dy*dvy_mpm;
  let tcpa=0.0;
  if(VV_m===0) {
    tcpa=0.0;
  } else {
    tcpa= -PV_m/VV_m;
  }
  if(tcpa<0) return { cpa:9999, tcpa:-1 };

  let vxA_mpm=vxA*speedScale;
  let vyA_mpm=vyA*speedScale;
  let vxB_mpm=vxB*speedScale;
  let vyB_mpm=vyB*speedScale;

  let xA2=xA+vxA_mpm*tcpa;
  let yA2=yA+vyA_mpm*tcpa;
  let xB2=xB+vxB_mpm*tcpa;
  let yB2=yB+vyB_mpm*tcpa;

  let dist=Math.sqrt((xA2-xB2)**2+(yA2-yB2)**2);
  let distNm=dist/1852;

  return { cpa: distNm, tcpa: tcpa };
}

function exitSituationView() {
  inSituationView = false;
  document.getElementById('left-panel').style.display = 'none';
  document.getElementById('bottom-center-bar').style.display = 'none';
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