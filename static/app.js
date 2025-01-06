// ==========================
// app.js (Moduł LIVE) – z splitted circle i filtr duplikatów
// ==========================
let map;
let markerClusterGroup;
let shipMarkers = {};    // klucz = mmsi
let overlayMarkers = {}; // klucz = mmsi
let selectedShips = [];

let collisionsData = [];
let collisionMarkers = [];

let vectorLength = 15;  // domyślnie 15 min
let cpaFilter = 0.5;
let tcpaFilter = 10;

// Timers
let shipsInterval = null;
let collisionsInterval = null;

function initMap() {
  map = L.map('map', {
    center: [50, 0],
    zoom: 5
  });

  // Warstwa OSM
  const osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{
    maxZoom:18
  });
  osmLayer.addTo(map);

  // Warstwa nawigacyjna (OpenSeaMap)
  const openSeaMap = L.tileLayer(
    'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
    { maxZoom: 18, opacity: 0.8 }
  );
  openSeaMap.addTo(map);

  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // UI
  document.getElementById('vectorLengthSlider').addEventListener('input', e=>{
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(false);
  });
  document.getElementById('cpaFilter').addEventListener('input', e=>{
    cpaFilter = parseFloat(e.target.value)||0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisions();
  });
  document.getElementById('tcpaFilter').addEventListener('input', e=>{
    tcpaFilter = parseFloat(e.target.value)||10;
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    fetchCollisions();
  });
  document.getElementById('clearSelectedShips').addEventListener('click', ()=>{
    clearSelectedShips();
  });

  // Start
  fetchShips();
  fetchCollisions();

  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

// 1) Fetcowanie statków
function fetchShips() {
  fetch(`/ships`)
    .then(r=>r.json())
    .then(data=>updateShips(data))
    .catch(err=>console.error("Error /ships:", err));
}

// 2) Fetcowanie kolizji
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(r=>r.json())
    .then(data=>{
      collisionsData = data;
      updateCollisionsList();
    })
    .catch(err=>console.error("Error /collisions:", err));
}

// 3) Update ships
function updateShips(shipsArray) {
  let currentMmsiSet = new Set(shipsArray.map(s=>s.mmsi));

  // usuwamy stare
  for(let mmsi in shipMarkers) {
    if(!currentMmsiSet.has(parseInt(mmsi))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      if(overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(o=>map.removeLayer(o));
        delete overlayMarkers[mmsi];
      }
    }
  }

  shipsArray.forEach(ship=>{
    const mmsi = ship.mmsi;
    const length = ship.ship_length||null;
    const color = getShipColor(length);
    const rotation = ship.cog||0;
    const width=16, height=24;

    let highlightRect='';
    if(selectedShips.includes(mmsi)){
      highlightRect=`
        <rect x="-10" y="-10" width="20" height="20"
              fill="none" stroke="black" stroke-width="3"
              stroke-dasharray="5,5"/>
      `;
    }
    const shipSvg=`
      <polygon points="0,-8 6,8 -6,8"
               fill="${color}" stroke="black" stroke-width="1"/>
    `;
    const icon = L.divIcon({
      className:'',
      html:`<svg width="${width}" height="${height}"
                 viewBox="-8 -8 16 16"
                 style="transform:rotate(${rotation}deg)">
              ${highlightRect}
              ${shipSvg}
            </svg>`,
      iconSize:[width,height],
      iconAnchor:[width/2,height/2]
    });

    let marker=shipMarkers[mmsi];
    // tooltip
    const now=Date.now();
    const updatedAt=new Date(ship.timestamp).getTime();
    const diffSec=Math.floor((now-updatedAt)/1000);
    const diffMin=Math.floor(diffSec/60);
    const diffS = diffSec%60;
    const diffStr=`${diffMin}m ${diffS}s ago`;

    const tooltipHTML=`
      <b>${ship.ship_name||'Unknown'}</b><br>
      MMSI:${mmsi}<br>
      SOG:${ship.sog||0} kn, COG:${ship.cog||0}°<br>
      Length:${length||'N/A'}<br>
      Updated:${diffStr}
    `;

    if(!marker) {
      marker=L.marker([ship.latitude, ship.longitude],{icon})
        .on('click', ()=>selectShip(mmsi));
      marker.bindTooltip(tooltipHTML,{direction:'top',sticky:true});
      shipMarkers[mmsi]=marker;
      markerClusterGroup.addLayer(marker);
    } else {
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(tooltipHTML);
    }
    marker.shipData=ship;
  });

  updateSelectedShipsInfo(false);
}

// get color by length
function getShipColor(length){
  if(length===null) return 'grey';
  if(length<50) return 'green';
  if(length<150) return 'yellow';
  if(length<250) return 'orange';
  return 'red';
}

// 4) Update collisions list
function updateCollisionsList() {
  const collisionList=document.getElementById('collision-list');
  collisionList.innerHTML='';

  // usuwamy stare markery
  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];

  if(!collisionsData || collisionsData.length===0) {
    let div=document.createElement('div');
    div.classList.add('collision-item');
    div.innerHTML='<i>No collisions found</i>';
    collisionList.appendChild(div);
    return;
  }

  // usuwanie duplikatów
  let colMap={};
  collisionsData.forEach(c=>{
    // klucz: (mmsi_a, mmsi_b) + np. zaokrąglenie do 1 min w timestamp
    let tObj=c.timestamp ? new Date(c.timestamp):null;
    let tMinKey='';
    if(tObj){
      let mins=Math.floor(tObj.getTime()/(60*1000)); // min-based
      tMinKey=`${mins}`;
    }
    // ustalamy parę w rosnącej kolejności
    let a= Math.min(c.mmsi_a,c.mmsi_b);
    let b= Math.max(c.mmsi_a,c.mmsi_b);
    let key=`${a}_${b}_${tMinKey}`;

    // zapamiętujemy "najświeższe"
    if(!colMap[key]){
      colMap[key]=c;
    } else {
      // ewentualnie porównywać c.timestamp
      // tu ignorujemy
    }
  });
  let finalCollisions=Object.values(colMap);

  finalCollisions.forEach(c=>{
    const shipA=c.ship1_name||c.mmsi_a;
    const shipB=c.ship2_name||c.mmsi_b;
    const cpa=c.cpa.toFixed(2);
    const tcpa=c.tcpa.toFixed(2);
    const item=document.createElement('div');
    item.classList.add('collision-item');

    // splitted circle
    let la=c.ship_length_a||0;
    let lb=c.ship_length_b||0;
    let colorA=getShipColor(la);
    let colorB=getShipColor(lb);
    let splittedCircle=createSplittedCircle(colorA,colorB);

    // tooltip
    let timeStr='';
    if(c.timestamp){
      let dt=new Date(c.timestamp);
      timeStr=dt.toLocaleTimeString('en-GB');
    }

    item.innerHTML=`
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedCircle}
          <strong>${shipA} - ${shipB}</strong><br>
          CPA:${cpa} nm, TCPA:${tcpa} min @ ${timeStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    item.querySelector('.zoom-button').addEventListener('click',()=>{
      zoomToCollision(c);
    });
    collisionList.appendChild(item);

    // marker
    let latC=(c.latitude_a + c.latitude_b)/2;
    let lonC=(c.longitude_a + c.longitude_b)/2;

    // większa ikona
    const collisionIcon=L.divIcon({
      className:'',
      html:`
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <path d="M0,-7 7,7 -7,7 Z"
                fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="4" text-anchor="middle" font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize:[24,24],
      iconAnchor:[12,12]
    });
    let marker=L.marker([latC,lonC],{icon:collisionIcon})
      .on('click',()=>zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

function createSplittedCircle(colorA,colorB){
  return `
  <svg width="16" height="16" viewBox="0 0 16 16" style="vertical-align:middle;margin-right:6px;">
    <path d="M8,8 m-8,0 a8,8 0 0,1 16,0 z" fill="${colorA}"/>
    <path d="M8,8 m8,0 a8,8 0 0,1 -16,0 z" fill="${colorB}"/>
  </svg>
  `;
}

function zoomToCollision(c){
  let bounds=L.latLngBounds([
    [c.latitude_a,c.longitude_a],
    [c.latitude_b,c.longitude_b]
  ]);
  map.fitBounds(bounds,{padding:[30,30]});
  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

// -------------------------------------------------------------------
// Obsługa zaznaczonych statków
// -------------------------------------------------------------------
function selectShip(mmsi) {
  if(!selectedShips.includes(mmsi)) {
    if(selectedShips.length>=2){
      selectedShips.shift();
    }
    selectedShips.push(mmsi);
    updateSelectedShipsInfo(true);
  }
}

function clearSelectedShips(){
  selectedShips=[];
  for(let m in overlayMarkers){
    overlayMarkers[m].forEach(o=>map.removeLayer(o));
  }
  overlayMarkers={};
  updateSelectedShipsInfo(false);
}

function updateSelectedShipsInfo(selectionChanged){
  const container=document.getElementById('selected-ships-info');
  container.innerHTML='';
  document.getElementById('pair-info').innerHTML='';

  if(selectedShips.length===0){
    reloadAllShipIcons();
    return;
  }

  let shipsData=[];
  selectedShips.forEach(mmsi=>{
    if(shipMarkers[mmsi]?.shipData){
      shipsData.push(shipMarkers[mmsi].shipData);
    }
  });

  shipsData.forEach(sd=>{
    const div=document.createElement('div');
    div.innerHTML=`
      <b>${sd.ship_name||'Unknown'}</b><br>
      MMSI:${sd.mmsi}<br>
      SOG:${sd.sog||0} kn, COG:${sd.cog||0}°<br>
      Length:${sd.ship_length||'N/A'}
    `;
    container.appendChild(div);
  });

  for(let m in overlayMarkers){
    overlayMarkers[m].forEach(o=>map.removeLayer(o));
  }
  overlayMarkers={};

  selectedShips.forEach(m=>drawVector(m));
  reloadAllShipIcons();

  if(selectedShips.length===2){
    let mA=selectedShips[0];
    let mB=selectedShips[1];
    let sortedPair=[mA,mB].sort((a,b)=>a-b);
    // pobierz CPA/TCPA
    fetch(`/calculate_cpa_tcpa?mmsi_a=${sortedPair[0]}&mmsi_b=${sortedPair[1]}`)
      .then(r=>r.json())
      .then(data=>{
        if(data.error){
          document.getElementById('pair-info').innerHTML=`<b>CPA/TCPA:</b> N/A`;
        } else {
          let cpa= data.cpa>=9999?'n/a': data.cpa.toFixed(2);
          let tcpa=data.tcpa<0?'n/a': data.tcpa.toFixed(2);
          document.getElementById('pair-info').innerHTML=`
            <b>CPA/TCPA:</b> ${cpa} nm / ${tcpa} min
          `;
        }
      })
      .catch(err=>{
        console.error(err);
        document.getElementById('pair-info').innerHTML=`<b>CPA/TCPA:</b> N/A`;
      });
  }
}

function reloadAllShipIcons(){
  for(let m in shipMarkers){
    let marker=shipMarkers[m];
    let sd=marker.shipData;
    let fillColor=getShipColor(sd.ship_length);
    let rotation=sd.cog||0;
    let width=16,height=24;
    let highlightRect='';
    if(selectedShips.includes(parseInt(m))){
      highlightRect=`
        <rect x="-10" y="-10" width="20" height="20"
              fill="none" stroke="black" stroke-width="3"
              stroke-dasharray="5,5"/>
      `;
    }
    const shipSvg=`
      <polygon points="0,-8 6,8 -6,8"
               fill="${fillColor}" stroke="black" stroke-width="1"/>
    `;
    const icon=L.divIcon({
      className:'',
      html:`<svg width="${width}" height="${height}"
                 viewBox="-8 -8 16 16"
                 style="transform:rotate(${rotation}deg)">
              ${highlightRect}
              ${shipSvg}
            </svg>`,
      iconSize:[width,height],
      iconAnchor:[width/2,height/2]
    });
    marker.setIcon(icon);
  }
}

function drawVector(mmsi){
  let marker=shipMarkers[mmsi];
  if(!marker)return;
  let sd=marker.shipData;
  if(!sd.sog||!sd.cog)return;

  let lat=sd.latitude;
  let lon=sd.longitude;
  let sog=sd.sog;
  let cogDeg=sd.cog;

  let distanceNm=sog*(vectorLength/60.0);
  let cogRad=(cogDeg*Math.PI)/180;
  let deltaLat=(distanceNm/60)*Math.cos(cogRad);
  let deltaLon=(distanceNm/60)*Math.sin(cogRad)/Math.cos(lat*Math.PI/180);
  let endLat=lat+deltaLat;
  let endLon=lon+deltaLon;

  let line=L.polyline([[lat,lon],[endLat,endLon]],{color:'blue',dashArray:'4,4'});
  line.addTo(map);
  if(!overlayMarkers[mmsi]) overlayMarkers[mmsi]=[];
  overlayMarkers[mmsi].push(line);
}

document.addEventListener('DOMContentLoaded', initMap);