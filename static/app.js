// ==========================
// app.js (Moduł LIVE) – z filtrowaniem przeszłych kolizji
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

  // Warstwa OSM (podstawowa)
  const osmLayer = L.tileLayer(
    'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    { maxZoom:18 }
  );
  osmLayer.addTo(map);

  // Warstwa nawigacyjna (OpenSeaMap)
  const openSeaMap = L.tileLayer(
    'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
    { maxZoom: 18, opacity: 0.8 }
  );
  openSeaMap.addTo(map);

  // MarkerCluster
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // UI eventy
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

  // Start fetch
  fetchShips();
  fetchCollisions();

  shipsInterval = setInterval(fetchShips, 60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

// =============== Fetcowanie statków ===============
function fetchShips() {
  fetch(`/ships`)
    .then(r=>r.json())
    .then(data=>updateShips(data))
    .catch(err=>console.error("Error /ships:", err));
}

// =============== Fetcowanie kolizji ===============
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(r=>r.json())
    .then(data=>{
      collisionsData = data;
      updateCollisionsList();
    })
    .catch(err=>console.error("Error /collisions:", err));
}

// =============== Uaktualnianie statków ===============
function updateShips(shipsArray) {
  // Zbiór MMSI z fetch
  let currentMmsiSet = new Set(shipsArray.map(s=>s.mmsi));

  // Usuwamy statki, których już nie ma
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

  // Dodaj/aktualizuj
  shipsArray.forEach(ship=>{
    const mmsi = ship.mmsi;
    const length = ship.ship_length || null;
    const color = getShipColor(length);
    const rotation = ship.cog || 0;
    const width=16, height=24;

    // highlight?
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

    let marker = shipMarkers[mmsi];
    // tooltip
    const now=Date.now();
    const updatedAt=new Date(ship.timestamp).getTime();
    const diffSec=Math.floor((now - updatedAt)/1000);
    const diffMin=Math.floor(diffSec/60);
    const diffS=diffSec%60;
    const diffStr=`${diffMin}m ${diffS}s ago`;

    const tooltipHTML=`
      <b>${ship.ship_name||'Unknown'}</b><br>
      MMSI:${mmsi}<br>
      SOG:${ship.sog||0} kn, COG:${ship.cog||0}°<br>
      Length:${length||'N/A'}<br>
      Updated:${diffStr}
    `;

    if(!marker){
      marker=L.marker([ship.latitude, ship.longitude], {icon})
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

// =============== Kolory statków wg długości ===============
function getShipColor(length){
  if(length===null) return 'grey';
  if(length<50) return 'green';
  if(length<150) return 'yellow';
  if(length<250) return 'orange';
  return 'red';
}

// =============== Uaktualnianie listy kolizji ===============
function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML='';

  // Usuwamy stare kolizyjne markery
  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];

  if(!collisionsData || collisionsData.length===0){
    let div=document.createElement('div');
    div.classList.add('collision-item');
    div.innerHTML='<i>No collisions found</i>';
    collisionList.appendChild(div);
    return;
  }

  // Filtrowanie starych kolizji (np. tcpa < 0) lub sprzed np. 2 minut
  let nowMs=Date.now();
  let filtered= collisionsData.filter(c=>{
    // Mamy c.timestamp: jeżeli stary, to skip
    if(!c.timestamp) return false;
    let t=new Date(c.timestamp).getTime();
    // np. odrzucamy, jeżeli t < now - 2min
    if(t < nowMs - 2*60*1000) return false;
    // odrzucamy jeżeli tcpa < 0
    if(c.tcpa<0) return false;
    // Mamy c.cpa i c.tcpa – jeżeli c.cpa>0.5 lub c.tcpa>10 – i tak by nie przeszło
    // ale to i tak jest wyfiltrowane na backendzie parametrami.
    return true;
  });

  // Redukowanie duplikatów – np. klucz (mmsi_a,mmsi_b)
  let colMap={};
  filtered.forEach(c=>{
    // ustalamy klucz dla pary
    let a=Math.min(c.mmsi_a,c.mmsi_b);
    let b=Math.max(c.mmsi_a,c.mmsi_b);

    // opcjonalnie można round-ować timestamp do 1 min
    let timeKey = Math.floor(new Date(c.timestamp).getTime()/(60*1000));
    let key = `${a}_${b}_${timeKey}`;
    if(!colMap[key]){
      colMap[key]=c;
    } else {
      // ewentualnie decyduj który bardziej aktualny
      // tu ignorujemy
    }
  });
  let finalCollisions= Object.values(colMap);

  if(finalCollisions.length===0){
    let div=document.createElement('div');
    div.classList.add('collision-item');
    div.innerHTML='<i>No current collisions</i>';
    collisionList.appendChild(div);
    return;
  }

  finalCollisions.forEach(c=>{
    const shipA=c.ship1_name||c.mmsi_a;
    const shipB=c.ship2_name||c.mmsi_b;
    const cpa=c.cpa.toFixed(2);
    const tcpa=c.tcpa.toFixed(2);

    let item=document.createElement('div');
    item.classList.add('collision-item');

    // splitted circle
    // UWAGA: w /collisions parametrach musisz mieć ship_length_a,b
    // Tu zakładamy, że c zawiera te wartości
    let la=c.ship_length_a||0;
    let lb=c.ship_length_b||0;
    let colorA=getShipColor(la);
    let colorB=getShipColor(lb);
    let splittedCircle=createSplittedCircle(colorA,colorB);

    let timeStr='';
    if(c.timestamp){
      let dt=new Date(c.timestamp);
      timeStr = dt.toLocaleTimeString('en-GB');
    }

    item.innerHTML=`
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedCircle}
          <strong>${shipA} - ${shipB}</strong><br>
          CPA: ${cpa} nm, TCPA: ${tcpa} min @ ${timeStr}
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

    const collisionIcon=L.divIcon({
      className:'',
      html:`
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <path d="M0,-7 7,7 -7,7 Z"
                fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="4" text-anchor="middle" font-size="8"
                fill="red">!</text>
        </svg>
      `,
      iconSize:[24,24],
      iconAnchor:[12,12]
    });
    let marker = L.marker([latC,lonC], {icon:collisionIcon})
      .on('click',()=>zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

// Tworzenie splitted circle
function createSplittedCircle(colorA,colorB){
  return `
  <svg width="16" height="16" viewBox="0 0 16 16" style="vertical-align:middle;margin-right:6px;">
    <path d="M8,8 m-8,0 a8,8 0 0,1 16,0 z" fill="${colorA}"/>
    <path d="M8,8 m8,0 a8,8 0 0,1 -16,0 z" fill="${colorB}"/>
  </svg>
  `;
}

// Zoom do kolizji
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
// Obsługa statków zaznaczonych
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

  // rysowanie wektora
  for(let m in overlayMarkers){
    overlayMarkers[m].forEach(o=>map.removeLayer(o));
  }
  overlayMarkers={};

  selectedShips.forEach(m=>drawVector(m));
  reloadAllShipIcons();

  // jeżeli mamy 2 statki, pobierz cpa/tcpa
  if(selectedShips.length===2){
    let [mA,mB]=[selectedShips[0],selectedShips[1]];
    let sorted=[mA,mB].sort((a,b)=>a-b);
    fetch(`/calculate_cpa_tcpa?mmsi_a=${sorted[0]}&mmsi_b=${sorted[1]}`)
      .then(r=>r.json())
      .then(data=>{
        if(data.error){
          document.getElementById('pair-info').innerHTML=`
            <b>CPA/TCPA:</b> n/a
          `;
        } else {
          let cpaVal=(data.cpa>=9999)? 'n/a': data.cpa.toFixed(2);
          let tcpaVal=(data.tcpa<0)? 'n/a': data.tcpa.toFixed(2);
          document.getElementById('pair-info').innerHTML=`
            <b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min
          `;
        }
      })
      .catch(err=>{
        console.error(err);
        document.getElementById('pair-info').innerHTML='<b>CPA/TCPA:</b> n/a';
      });
  }
}

function reloadAllShipIcons(){
  for(let m in shipMarkers){
    let marker=shipMarkers[m];
    let sd=marker.shipData;
    let color=getShipColor(sd.ship_length);
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
               fill="${color}" stroke="black" stroke-width="1"/>
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

  let line=L.polyline([[lat,lon],[endLat,endLon]],
                      {color:'blue',dashArray:'4,4'});
  line.addTo(map);
  if(!overlayMarkers[mmsi]) overlayMarkers[mmsi]=[];
  overlayMarkers[mmsi].push(line);
}

// Start
document.addEventListener('DOMContentLoaded', initMap);