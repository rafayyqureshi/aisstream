let map;
let shipMarkers={};
let collisionsData=[];
let selectedShips=[];
let shipHistoryMarkers=[];
let cpaFilterVal=0.5;
let tcpaFilterVal=10;
let vectorLength=15;
let collisionTimer=null;
let shipsTimer=null;
let collisionMarkers=[]; // ADDED - przechowujemy markery kolizji

function getShipIcon(ship) {
  let fillColor='white';
  let scale=1.0;
  if(!ship.ship_length){
    fillColor='white';
    scale=1.0;
  } else if(ship.ship_length<50){
    fillColor='green';
    scale=0.9;
  } else if(ship.ship_length<150){
    fillColor='yellow';
    scale=1.1;
  } else if(ship.ship_length<250){
    fillColor='orange';
    scale=1.2;
  } else {
    fillColor='red';
    scale=1.3;
  }
  let rotation=ship.cog||0;
  return L.divIcon({
    className:'ship-icon',
    html:`<div style="transform: rotate(${rotation}deg);">
      <svg width="${30*scale}" height="${30*scale}" viewBox="0 0 20 20">
        <polygon points="10,0 20,20 10,15 0,20" fill="${fillColor}" stroke="black" stroke-width="1"/>
      </svg>
    </div>`,
    iconSize:[30*scale,30*scale],
    iconAnchor:[10*scale,10*scale]
  });
}

function getSelectedShipBoxIcon(){
  return L.divIcon({
    className:'selection-icon',
    html:`<div style="width:40px;height:40px;border:2px dashed black;position:absolute;left:-20px;top:-20px;"></div>`,
    iconSize:[40,40],
    iconAnchor:[20,20]
  });
}

// ADDED: Ikona kolizji - czerwony trójkąt z wykrzyknikiem
function getCollisionIcon(){
  return L.divIcon({
    className:'collision-icon',
    html:`<div style="width:30px;height:30px;position:absolute;left:-15px;top:-15px;">
          <svg width="30" height="30" viewBox="0 0 20 20">
            <polygon points="10,0 20,20 0,20" fill="red" stroke="black" stroke-width="1"/>
            <text x="10" y="15" font-size="10" text-anchor="middle" fill="white" font-weight="bold">!</text>
          </svg>
          </div>`,
    iconSize:[30,30],
    iconAnchor:[15,15]
  });
}

function initMap(){
  map = L.map('map').setView([50.6,0.0],7);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{
    maxZoom:19,
    attribution:'&copy; OpenStreetMap contributors'
  }).addTo(map);

  document.getElementById('vectorLengthSlider').addEventListener('input',e=>{
    vectorLength=parseInt(e.target.value);
    document.getElementById('vectorLengthValue').textContent=e.target.value;
    updateSelectedShipsInfo();
  });

  document.getElementById('cpaFilter').addEventListener('input',e=>{
    cpaFilterVal=parseFloat(e.target.value);
    document.getElementById('cpaValue').textContent=cpaFilterVal.toFixed(2);
    updateCollisionsList();
  });

  document.getElementById('tcpaFilter').addEventListener('input',e=>{
    tcpaFilterVal=parseFloat(e.target.value);
    document.getElementById('tcpaValue').textContent=tcpaFilterVal.toFixed(2);
    updateCollisionsList();
  });

  document.getElementById('clearSelectedShips').addEventListener('click',()=>{
    selectedShips=[];
    clearShipHistoryMarkers();
    updateSelectedShipsInfo();
  });

  fetchAndUpdateShips();
  shipsTimer=setInterval(fetchAndUpdateShips,5000);
  fetchAndUpdateCollisions();
  collisionTimer=setInterval(fetchAndUpdateCollisions,5000);
}

function fetchAndUpdateShips(){
  fetch('/ships')
    .then(r=>r.json())
    .then(data=>{
      updateShipsOnMap(data);
    })
    .catch(e=>console.error(e));
}

function fetchAndUpdateCollisions(){
  fetch('/collisions')
    .then(r=>r.json())
    .then(data=>{
      collisionsData=data; 
      updateCollisionsList();
    })
    .catch(e=>console.error(e));
}

function updateShipsOnMap(ships){
  let currentMMSIs=ships.map(s=>s.mmsi);
  for(let mmsi in shipMarkers){
    if(!currentMMSIs.includes(parseInt(mmsi))){
      map.removeLayer(shipMarkers[mmsi].marker);
      if(shipMarkers[mmsi].vectorLine) map.removeLayer(shipMarkers[mmsi].vectorLine);
      if(shipMarkers[mmsi].selectionBox) map.removeLayer(shipMarkers[mmsi].selectionBox);
      delete shipMarkers[mmsi];
    }
  }

  ships.forEach(ship=>{
    if(!shipMarkers[ship.mmsi]){
      shipMarkers[ship.mmsi]={};
      let marker=L.marker([ship.latitude,ship.longitude],{icon:getShipIcon(ship)})
        .on('click',()=>toggleSelectShip(ship))
        .addTo(map);
      shipMarkers[ship.mmsi].marker=marker;
    } else {
      shipMarkers[ship.mmsi].marker.setLatLng([ship.latitude,ship.longitude]);
      shipMarkers[ship.mmsi].marker.setIcon(getShipIcon(ship));
    }
    let diff=humanTimeDiff(ship.timestamp);
    let nm=ship.ship_name||'Unknown';
    let sogtxt=ship.sog?ship.sog.toFixed(1)+' kn':'N/A';
    let cogtxt=ship.cog?(ship.cog.toFixed(1)+'°'):'N/A';
    let lentxt=ship.ship_length?(ship.ship_length+' m'):'N/A';
    let ttip=`${nm}\nMMSI: ${ship.mmsi}\nSOG: ${sogtxt}\nCOG: ${cogtxt}\nLength: ${lentxt}\nLast update: ${diff} ago`;
    shipMarkers[ship.mmsi].marker.bindTooltip(ttip,{permanent:false});

    if(selectedShips.find(s=>s.mmsi===ship.mmsi)){
      updateShipVector(ship);
      updateShipSelectionBox(ship);
    } else {
      removeShipVector(ship.mmsi);
      removeShipSelectionBox(ship.mmsi);
    }
  });
}

function toggleSelectShip(ship){
  let idx=selectedShips.findIndex(s=>s.mmsi===ship.mmsi);
  if(idx>=0){
    selectedShips.splice(idx,1);
    clearShipHistoryMarkers();
  } else {
    if(selectedShips.length===2){
      selectedShips=[];
      clearShipHistoryMarkers();
    }
    selectedShips.push(ship);
    fetchShipHistory(ship.mmsi);
    if(selectedShips.length===2){
      fetchShipHistory(selectedShips[0].mmsi);
      fetchShipHistory(selectedShips[1].mmsi);
    }
  }
  updateSelectedShipsInfo();
}

function fetchShipHistory(mmsi){
  // TODO: implement if needed
  clearShipHistoryMarkers();
}

function clearShipHistoryMarkers(){
  shipHistoryMarkers.forEach(m=>map.removeLayer(m));
  shipHistoryMarkers=[];
}

function updateSelectedShipsInfo(){
  const cont=document.getElementById('selected-ships-info');
  cont.innerHTML='';
  const pairInfo=document.getElementById('pair-info');
  pairInfo.innerHTML='';

  if(selectedShips.length===0)return; 

  selectedShips.forEach(s=>{
    let sogtxt=s.sog? s.sog.toFixed(1)+' kn':'N/A';
    let cogtxt=s.cog?(s.cog.toFixed(1)+'°'):'N/A';
    let lentxt=s.ship_length?(s.ship_length+' m'):'N/A';
    let nm=s.ship_name||'Unknown';
    const div=document.createElement('div');
    div.style.marginBottom='10px';
    div.innerHTML=`<strong>${nm}</strong><br>
    MMSI: ${s.mmsi}<br>
    SOG: ${sogtxt}<br>
    COG: ${cogtxt}<br>
    Length: ${lentxt}`;
    cont.appendChild(div);
  });
  
  if(selectedShips.length===2){
    let c=computeCPA(selectedShips[0],selectedShips[1]);
    if(c){
      pairInfo.textContent=`CPA/TPCA for selected pair: CPA: ${c.cpa.toFixed(2)} nm, TCPA: ${c.tcpa.toFixed(2)} min`;
    }
  }
}

function computeCPA(shipA, shipB){
  if(!shipA.sog||!shipA.cog||!shipB.sog||!shipB.cog)return null;
  let latA=shipA.latitude;let lonA=shipA.longitude;
  let latB=shipB.latitude;let lonB=shipB.longitude;
  let va=shipA.sog*0.51444;let vb=shipB.sog*0.51444;
  let ca=deg2rad(shipA.cog);let cb=deg2rad(shipB.cog);
  let vxA=va*Math.sin(ca);let vyA=va*Math.cos(ca);
  let vxB=vb*Math.sin(cb);let vyB=vb*Math.cos(cb);

  let latRef=50.0;
  let scaleLat=111000; 
  let scaleLon=111000*Math.cos(deg2rad(latRef));
  let xA=(lonA*scaleLon);let yA=(latA*scaleLat);
  let xB=(lonB*scaleLon);let yB=(latB*scaleLat);
  let dx=xA-xB;let dy=yA-yB;
  let dvx=vxA-vxB;let dvy=vyA-vyB;
  let VV=dvx*dvx+dvy*dvy;
  if(VV===0){
    let dist=Math.sqrt(dx*dx+dy*dy);
    let distNm=dist/1852;
    return {cpa:distNm,tcpa:0};
  }
  let PV=dx*dvx+dy*dvy;
  let tcpa=-PV/VV;
  if(tcpa<0) tcpa=0;
  let xA2=xA+vxA*tcpa;let yA2=yA+vyA*tcpa;
  let xB2=xB+vxB*tcpa;let yB2=yB+vyB*tcpa;
  let dist=Math.sqrt((xA2 - xB2)**2+(yA2 - yB2)**2);
  let distNm=dist/1852;
  let tcpaMin=tcpa/60;
  if(tcpaMin===0)return null;
  return {cpa:distNm, tcpa:tcpaMin};
}

function deg2rad(deg){return deg*Math.PI/180;}

function humanTimeDiff(ts){
  let now=Date.now();
  let diff=(now-(new Date(ts)).getTime())/1000;
  if(diff<60) return diff.toFixed(0)+'s';
  let min=diff/60;
  return min.toFixed(0)+' min';
}

function removeShipVector(mmsi){
  if(shipMarkers[mmsi]?.vectorLine){
    map.removeLayer(shipMarkers[mmsi].vectorLine);
    shipMarkers[mmsi].vectorLine=null;
  }
}

function removeShipSelectionBox(mmsi){
  if(shipMarkers[mmsi]?.selectionBox){
    map.removeLayer(shipMarkers[mmsi].selectionBox);
    shipMarkers[mmsi].selectionBox=null;
  }
}

function updateShipVector(ship){
  removeShipVector(ship.mmsi);
  if(!ship.sog||!ship.cog)return;
  let va=ship.sog*0.51444; 
  let ca=deg2rad(ship.cog);
  let vx=va*Math.sin(ca);
  let vy=va*Math.cos(ca);
  let latRef=50.0;
  let scaleLat=111000; 
  let scaleLon=111000*Math.cos(deg2rad(latRef));
  let x=ship.longitude*scaleLon;let y=ship.latitude*scaleLat;
  let t=vectorLength*60; 
  let x2=x+vx*t;let y2=y+vy*t;
  let lat2=y2/scaleLat;let lon2=x2/scaleLon;
  let line=L.polyline([[ship.latitude,ship.longitude],[lat2,lon2]],{
    color:'black',weight:2,opacity:1,dashArray:'4'
  }).addTo(map);
  shipMarkers[ship.mmsi].vectorLine=line;
}

function updateShipSelectionBox(ship){
  removeShipSelectionBox(ship.mmsi);
  let boxMarker=L.marker([ship.latitude,ship.longitude],{icon:getSelectedShipBoxIcon()}).addTo(map);
  shipMarkers[ship.mmsi].selectionBox=boxMarker;
}

function updateCollisionsList(){
  const list=document.getElementById('collision-list');
  list.innerHTML='';
  collisionMarkers.forEach(m=>map.removeLayer(m)); // remove old markers
  collisionMarkers=[];

  let uniqueKeys=new Set();
  let filtered=collisionsData.filter(c=>{
    // odrzucamy kolizje z tcpa=0
    if(c.tcpa<=0)return false;
    if(c.mmsi_a===c.mmsi_b)return false;
    if(c.cpa>cpaFilterVal)return false;
    if(c.tcpa>tcpaFilterVal)return false;
    let key=[c.mmsi_a,c.mmsi_b].sort().join('-');
    if(uniqueKeys.has(key))return false;
    uniqueKeys.add(key);
    return true;
  });

  filtered.sort((a,b)=>a.tcpa-b.tcpa);

  filtered.forEach(col=>{
    let shipA=col.ship1_name||('Unknown');
    let shipB=col.ship2_name||('Unknown');
    const item=document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML=`
      <div><strong>${shipA} - ${shipB}</strong><br>
      CPA: ${col.cpa.toFixed(2)} nm, TCPA: ${col.tcpa.toFixed(2)} min
      <button class="zoom-button">🔍</button></div>
    `;
    item.querySelector('.zoom-button').addEventListener('click',()=>{
      handleCollisionAction(col);
    });

    // ADDED: Dodajemy marker kolizji na mapie
    let collisionMarker=L.marker([(col.latitude_a+col.latitude_b)/2,(col.longitude_a+col.longitude_b)/2],{icon:getCollisionIcon()})
      .on('click',()=>{
        handleCollisionAction(col);
      }).addTo(map);
    collisionMarkers.push(collisionMarker);

    list.appendChild(item);
  });
}

// ADDED: funkcja obsługi kolizji (po kliknięciu lupy lub ikony kolizji)
function handleCollisionAction(col){
  map.setView([(col.latitude_a+col.latitude_b)/2,(col.longitude_a+col.longitude_b)/2],12);
  fetch('/ships')
    .then(r=>r.json())
    .then(data=>{
      let sA=data.find(s=>s.mmsi===col.mmsi_a);
      let sB=data.find(s=>s.mmsi===col.mmsi_b);
      selectedShips=[];
      clearShipHistoryMarkers();
      if(sA) selectedShips.push(sA);
      if(sB) selectedShips.push(sB);
      // Ustaw vectorLength = TCPA z kolizji (zaokrąglij jeśli trzeba)
      let newVectorLength=Math.floor(col.tcpa);
      if(newVectorLength<1)newVectorLength=1;
      if(newVectorLength>120)newVectorLength=120;
      vectorLength=newVectorLength;
      document.getElementById('vectorLengthSlider').value=vectorLength;
      document.getElementById('vectorLengthValue').textContent=vectorLength;
      if(sA)fetchShipHistory(sA.mmsi);
      if(sB)fetchShipHistory(sB.mmsi);
      updateSelectedShipsInfo();
    });
}

document.addEventListener('DOMContentLoaded',initMap);