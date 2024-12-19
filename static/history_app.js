let map;
let collisionMarkers = [];
let selectedShips = [];
let currentDay = 0; 
const minDay = -7;
const maxDay = 0;

let cpaFilter = 0.5;
let isPlaying = false;
let animationData = [];
let animationIndex = 0;
let animationInterval = null;
let currentCollisionId = null;
let currentCollisionInfo = null;
let shipMarkersOnMap = [];
let inSituationView = false;

let shipA_length = null;
let shipB_length = null;

function initMap() {
  map = L.map('map').setView([50.0, 0.0], 6);
  const osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom:18});
  osmLayer.addTo(map);

  updateDayLabel();
  fetchCollisionsData();
  setupUI();

  map.on('click',()=>{
    if(inSituationView) {
      exitSituationView();
    }
  });
}

function setupUI() {
  document.getElementById('prevDay').addEventListener('click', ()=>{
    if(currentDay>minDay) {
      currentDay -= 1;
      updateDayLabel();
      fetchCollisionsData();
    }
  });
  document.getElementById('nextDay').addEventListener('click', ()=>{
    if(currentDay<maxDay) {
      currentDay += 1;
      updateDayLabel();
      fetchCollisionsData();
    }
  });

  document.getElementById('cpaFilter').addEventListener('input', (e)=>{
    cpaFilter = parseFloat(e.target.value);
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisionsData();
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

  updateDayButtons();
}

function updateDayLabel() {
  const now = new Date(); 
  const realDate = new Date(now);
  realDate.setDate(now.getDate() + currentDay);
  const dateStr = realDate.toISOString().slice(0,10); 
  document.getElementById('currentDayLabel').textContent = `Date: ${dateStr}`;
  updateDayButtons();
}

function updateDayButtons() {
  const prevBtn = document.getElementById('prevDay');
  const nextBtn = document.getElementById('nextDay');
  prevBtn.disabled = (currentDay<=minDay);
  nextBtn.disabled = (currentDay>=maxDay);

  prevBtn.style.opacity=prevBtn.disabled?0.5:1.0;
  nextBtn.style.opacity=nextBtn.disabled?0.5:1.0;
}

function fetchCollisionsData() {
  clearCollisions();
  fetch(`/history_collisions?day=${currentDay}&max_cpa=${cpaFilter}`)
    .then(r=>r.json())
    .then(data=>{
      displayCollisions(data);
    })
    .catch(err=>console.error("Error fetching history collisions:", err));
}

function getShipColor(length){
  if(length===null) return 'gray';
  if(length<50) return 'green';
  if(length<150) return 'yellow';
  if(length<250) return 'orange';
  return 'red';
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

    let timeStr = c.timestamp?new Date(c.timestamp).toLocaleTimeString('en-GB'):'unknown';

    let colorA = getShipColor(c.ship1_length);
    let colorB = getShipColor(c.ship2_length);
    let sizeIcon = `
      <svg width="20" height="20" viewBox="-10 -10 20 20" style="margin-right:5px; vertical-align:middle;">
        <path d="M0,-10 A10,10 0 0,1 10,0 L0,0 Z" fill="${colorB}"/>
        <path d="M0,-10 A10,10 0 0,0 -10,0 L0,0 Z" fill="${colorA}"/>
        <path d="M0,0 A10,10 0 0,1 -10,0 L0,0 Z" fill="${colorA}"/>
        <path d="M0,0 A10,10 0 0,0 10,0 L0,0 Z" fill="${colorB}"/>
      </svg>`;

    item.innerHTML=`
      <div class="collision-header">
        <div>${sizeIcon}<strong>${shipA} - ${shipB}</strong><br>
        CPA: ${c.cpa.toFixed(2)} nm at ${timeStr}</div>
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
    let tooltipContent = `${shipA} - ${shipB}<br>CPA: ${c.cpa.toFixed(2)} nm at ${timeStr}`;
    marker.bindTooltip(tooltipContent,{direction:'top',sticky:true});
    marker.on('click',()=>{ zoomToCollision(c); });
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

function zoomToCollision(c) {
  const bounds = L.latLngBounds([[c.latitude_a,c.longitude_a],[c.latitude_b,c.longitude_b]]);
  // bigger zoom = less padding, previously was [60,60], now [20,20]
  map.fitBounds(bounds,{padding:[20,20]});
  loadCollisionData(c.collision_id, c);
}

function loadCollisionData(collision_id, collisionData) {
  currentCollisionId = collision_id;
  fetch(`/history_data?collision_id=${collision_id}`)
    .then(r=>r.json())
    .then(data=>{
      animationData = data;
      animationIndex = 0;
      stopAnimation();
      currentCollisionInfo = collisionData; 
      inSituationView = true;
      shipA_length = currentCollisionInfo.ship1_length;
      shipB_length = currentCollisionInfo.ship2_length;

      document.getElementById('left-panel').style.display='block';
      document.getElementById('bottom-center-bar').style.display='block';
      updateMapFrame();

      // Fit do klatki nr 10 (index 9)
      if(animationData.length===10 && animationData[9].shipPositions.length===2) {
        let sA=animationData[9].shipPositions[0];
        let sB=animationData[9].shipPositions[1];
        const bounds = L.latLngBounds([[sA.lat,sA.lon],[sB.lat,sB.lon]]);
        map.fitBounds(bounds,{padding:[20,20]});
      }

    })
    .catch(err=>{
      console.error("Error fetching collision data:", err);
    });
}

function startAnimation() {
  if(animationData.length===0) return;
  isPlaying = true;
  document.getElementById('playPause').textContent="Pause";

  // 1 klatka/sek
  animationInterval = setInterval(()=>{
    stepAnimation(1);
  }, 1000);
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

function updateMapFrame() {
  if(animationData.length===0) return;
  let frame = animationData[animationIndex];

  document.getElementById('frameIndicator').textContent = `${animationIndex+1}/10`;

  if(shipMarkersOnMap) {
    shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  }
  shipMarkersOnMap=[];

  let ships = frame.shipPositions;
  if(ships.length===2) {
    let shipAName = currentCollisionInfo.ship1_name || currentCollisionInfo.mmsi_a;
    let shipBName = currentCollisionInfo.ship2_name || currentCollisionInfo.mmsi_b;

    function fmtCOG(c){return Math.round(c);}
    function fmtSOG(s){return s.toFixed(1);}

    let A_len = shipA_length;
    let B_len = shipB_length;

    // Skalowanie długości
    function lengthScale(len){
      if(len===null || len<20) return 0.7;
      let val=(len/250)+0.5; 
      if(val<0.7) val=0.7; 
      if(val>1.5) val=1.5;
      return val;
    }

    let shipA = {latitude: ships[0].lat, longitude: ships[0].lon, sog:ships[0].sog||0, cog:ships[0].cog||0, ship_length:A_len};
    let shipB = {latitude: ships[1].lat, longitude: ships[1].lon, sog:ships[1].sog||0, cog:ships[1].cog||0, ship_length:B_len};

    let {cpa, tcpa} = compute_cpa_tcpa_js(shipA,shipB);

    ships.forEach(s=>{
      let isA = (s.mmsi===currentCollisionInfo.mmsi_a);
      let length = isA?A_len:B_len;
      let fillColor = getShipColor(length);
      let scale = lengthScale(length);

      let marker = L.marker([s.lat,s.lon], {icon:createShipIcon(s,fillColor,scale)});
      let name = isA?shipAName:shipBName;
      let cogTxt = fmtCOG(s.cog||0);
      let sogTxt = fmtSOG(s.sog||0);
      let lenTxt = length===null?'Unknown':length+'m';
      let tt = `${name}<br>COG:${cogTxt}°, SOG:${sogTxt} kn<br>Length:${lenTxt}`;
      marker.bindTooltip(tt,{direction:'top',sticky:true});
      marker.addTo(map);
      shipMarkersOnMap.push(marker);
    });

    const nowTime = frame.time;
    let timeObj = new Date(nowTime);
    let hh=timeObj.getHours().toString().padStart(2,'0');
    let mm=timeObj.getMinutes().toString().padStart(2,'0');
    let ss=timeObj.getSeconds().toString().padStart(2,'0');
    let timeStr=`${hh}:${mm}:${ss}`;

    let container = document.getElementById('selected-ships-info');
    container.innerHTML=`
      <b>${shipAName}</b><br>
      SOG:${fmtSOG(shipA.sog)} kn, COG:${fmtCOG(shipA.cog)}°<br><br>
      <b>${shipBName}</b><br>
      SOG:${fmtSOG(shipB.sog)} kn, COG:${fmtCOG(shipB.cog)}°
    `;

    let pairInfo = document.getElementById('pair-info');
    if(animationIndex>6) {
      // English message: "Vessels are moving apart"
      pairInfo.innerHTML=`
        Time: ${timeStr}<br>
        Vessels are moving apart
      `;
    } else {
      pairInfo.innerHTML=`
        Time: ${timeStr}<br>
        CPA: ${cpa.toFixed(2)} nm, TCPA: ${tcpa.toFixed(2)} min
      `;
    }

  } else {
    let container = document.getElementById('selected-ships-info');
    container.innerHTML='No data for both ships.';
    document.getElementById('pair-info').innerHTML='';
  }
}

function exitSituationView() {
  inSituationView=false;
  document.getElementById('left-panel').style.display='none';
  document.getElementById('bottom-center-bar').style.display='none';
  stopAnimation();
  if(shipMarkersOnMap) {
    shipMarkersOnMap.forEach(m=>map.removeLayer(m));
  }
  shipMarkersOnMap=[];
  animationData=[];
  animationIndex=0;
  currentCollisionInfo=null;
  shipA_length=null;
  shipB_length=null;
}

function createShipIcon(shipData, fillColor='yellow', scale=1.0) {
  let rotation=shipData.cog||0;
  let baseW=12, baseH=18;
  let w=baseW*scale, h=baseH*scale;
  const shape = `<polygon points="0,-7.5 5,7.5 -5,7.5" fill="${fillColor}" stroke="#000" stroke-width="1"/>`;
  // Adjust viewBox accordingly
  let icon = L.divIcon({
    className:'',
    html:`<svg width="${w}" height="${h}" viewBox="-5 -7.5 10 15" style="transform:rotate(${rotation}deg);">${shape}</svg>`,
    iconSize:[w,h],
    iconAnchor:[w/2,h/2]
  });
  return icon;
}

function compute_cpa_tcpa_js(a, b) {
  if (a.ship_length===null || b.ship_length===null) return {cpa:9999,tcpa:-1};
  if (a.ship_length<50 || b.ship_length<50) return {cpa:9999,tcpa:-1};

  let latRef=(a.latitude+b.latitude)/2;
  let scaleLat=111000;
  let scaleLon=111000*Math.cos(latRef*Math.PI/180);

  function toXY(lat,lon){ return [lon*scaleLon, lat*scaleLat]; }

  let [xA,yA]=toXY(a.latitude,a.longitude);
  let [xB,yB]=toXY(b.latitude,b.longitude);

  let sogA=a.sog||0; let sogB=b.sog||0;
  function cogToVector(cogDeg, sogNmH){
    let cogRad = (cogDeg||0)*Math.PI/180;
    let vx=sogNmH*Math.sin(cogRad);
    let vy=sogNmH*Math.cos(cogRad);
    return [vx,vy];
  }

  let [vxA,vyA]=cogToVector(a.cog||0,sogA);
  let [vxB,vyB]=cogToVector(b.cog||0,sogB);

  let dx=xA-xB;
  let dy=yA-yB;
  let dvx=vxA-vxB;
  let dvy=vyA-vyB;

  let speedScale=1852/60;
  let dvx_mpm=dvx*speedScale;
  let dvy_mpm=dvy*speedScale;

  let VV_m=dvx_mpm**2 + dvy_mpm**2;
  let PV_m=dx*dvx_mpm+dy*dvy_mpm;

  let tcpa=0.0;
  if(VV_m===0) {
    tcpa=0.0;
  } else {
    tcpa= -PV_m/VV_m;
  }

  if(tcpa<0) return {cpa:9999, tcpa:-1};

  let vxA_mpm = vxA*speedScale;
  let vyA_mpm = vyA*speedScale;
  let vxB_mpm = vxB*speedScale;
  let vyB_mpm = vyB*speedScale;

  let xA2=xA+vxA_mpm*tcpa;
  let yA2=yA+vyA_mpm*tcpa;
  let xB2=xB+vxB_mpm*tcpa;
  let yB2=yB+vyB_mpm*tcpa;

  let dist=Math.sqrt((xA2-xB2)**2+(yA2-yB2)**2);
  let distNm=dist/1852;
  return {cpa:distNm, tcpa:tcpa};
}

function clearCollisions() {
  const list = document.getElementById('collision-list');
  list.innerHTML='';
  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];
}

document.addEventListener('DOMContentLoaded', initMap);