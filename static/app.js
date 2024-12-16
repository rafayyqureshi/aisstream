let map;
let ships = {}; 
let selectedShips = [];
let collisionsData = [];
let collisionMarkers = [];
let collisionLayerGroup;
let shipVectors = {};
let vectorTime = 30; // domyślnie 30
let selectedShipBoxMarker = null;
let lastShipsData = {};

function initMap() {
  map = L.map('map',{
    center: [50.5, 0],
    zoom: 6
  });

  // Bazowa warstwa
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{
    maxZoom:18
  }).addTo(map);

  // Warstwa oznakowania morskiego
  L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',{
    maxZoom:18
  }).addTo(map);

  collisionLayerGroup = L.markerClusterGroup({maxClusterRadius:80});
  map.addLayer(collisionLayerGroup);

  fetchAndUpdateData();
  setInterval(fetchAndUpdateData,5000);
}

function fetchAndUpdateData() {
  fetch('/ships')
    .then(r=>r.json())
    .then(d=>updateShips(d))
    .catch(e=>console.error('Error ships:',e));

  fetch('/collisions')
    .then(r=>r.json())
    .then(d=>{
      collisionsData=d;
      updateCollisions(d);
    })
    .catch(e=>console.error('Error collisions:',e));
}

function updateShips(shipsData) {
  let now=Date.now();
  let currentMmsi={};
  shipsData.forEach(s=>{
    currentMmsi[s.mmsi]=true;
    if(!ships[s.mmsi]){
      let icon=getShipIcon(s);
      let marker=L.marker([s.latitude,s.longitude],{icon:icon})
        .on('click',()=>toggleSelectShip(s.mmsi))
        .addTo(map);
      marker.bindTooltip(getShipTooltip(s));
      ships[s.mmsi]={marker:marker,data:s};
    }else{
      let icon=getShipIcon(s);
      ships[s.mmsi].marker.setLatLng([s.latitude,s.longitude]);
      ships[s.mmsi].marker.setIcon(icon);
      ships[s.mmsi].marker.setTooltipContent(getShipTooltip(s));
      ships[s.mmsi].data=s;
    }
    lastShipsData[s.mmsi]=now;
  });

  // usuwamy statki niewidoczne od 2 min
  for (let m in ships) {
    if(!currentMmsi[m]){
      if(lastShipsData[m] && now - lastShipsData[m]>120000){
        map.removeLayer(ships[m].marker);
        delete ships[m];
      }
    }
  }

  updateLeftPanel();
  updateShipVectors();
  updateSelectedShipBox();
}

function updateCollisions(collData) {
  collisionLayerGroup.clearLayers();
  collData.forEach(c=>{
    let lat=(c.latitude_a+c.latitude_b)/2;
    let lon=(c.longitude_a+c.longitude_b)/2;
    let marker=L.marker([lat,lon],{icon:getCollisionIcon()});
    collisionLayerGroup.addLayer(marker);
  });
  updateRightPanel();
}

function updateLeftPanel() {
  let panel=document.getElementById('left-panel-content');
  panel.innerHTML=`
    <label>Vector time: <span id="vectorTimeLabel">${vectorTime}</span> min</label><br>
    <input type="range" id="vectorTimeSlider" min="1" max="120" value="${vectorTime}">
  `;

  if(selectedShips.length===0){
    panel.innerHTML+=`<p>No ships selected.</p>`;
  } else if(selectedShips.length===1){
    let s=ships[selectedShips[0]].data;
    panel.innerHTML+=`<p>Selected: ${s.ship_name||s.mmsi}</p>`;
  } else if(selectedShips.length===2){
    let s1=ships[selectedShips[0]].data;
    let s2=ships[selectedShips[1]].data;
    let {cpa,tcpa,info}=computeCPAandTCPA(s1,s2);
    panel.innerHTML+=`<p>Ship A: ${s1.ship_name||s1.mmsi}<br>Ship B: ${s2.ship_name||s2.mmsi}<br>CPA:${cpa.toFixed(1)} nm, TCPA:${Math.round(tcpa)} min<br>${info}</p>`;
  }

  let slider=document.getElementById('vectorTimeSlider');
  slider.addEventListener('input',()=>{
    vectorTime=parseInt(slider.value);
    document.getElementById('vectorTimeLabel').innerText=vectorTime;
    updateShipVectors();
    updateSelectedShipBox();
  });
}

function updateRightPanel() {
  let list=document.getElementById('collision-list');
  list.innerHTML='';

  let sorted=collisionsData.slice().sort((a,b)=>a.cpa-b.cpa);
  sorted.forEach(c=>{
    let shipAName=c.ship_a_name||c.mmsi_a;
    let shipBName=c.ship_b_name||c.mmsi_b;
    let info='';
    if(c.tcpa<0) info='Ships separating';
    else info='Potential collision';
    let div=document.createElement('div');
    div.innerHTML=`
      <p><strong>${shipAName}</strong> & <strong>${shipBName}</strong><br>
      CPA:${c.cpa.toFixed(1)} nm, TCPA:${Math.round(c.tcpa)} min - ${info}</p>
    `;
    list.appendChild(div);
  });
}

// Komputacja CPA/TCPA:
function computeCPAandTCPA(a,b) {
  let sogAmin=a.sog/60;
  let sogBmin=b.sog/60;

  function deg2rad(d){return d*Math.PI/180;}
  let latRef=(a.latitude+b.latitude)/2;
  let scale_lat=60;
  let scale_lon=60*Math.cos(deg2rad(latRef));

  function toXY(lat,lon){return [lon*scale_lon,lat*scale_lat];}

  let [xA,yA]=toXY(a.latitude,a.longitude);
  let [xB,yB]=toXY(b.latitude,b.longitude);

  let cogA=deg2rad(a.cog);
  let cogB=deg2rad(b.cog);
  let vxA=sogAmin*Math.sin(cogA);
  let vyA=sogAmin*Math.cos(cogA);
  let vxB=sogBmin*Math.sin(cogB);
  let vyB=sogBmin*Math.cos(cogB);

  let dx=xA-xB;let dy=yA-yB;
  let dvx=vxA-vxB;let dvy=vyA-vyB;

  let VV=dvx*dvx+dvy*dvy;
  if(VV===0){
    let dist=Math.sqrt(dx*dx+dy*dy);
    return {cpa:dist,tcpa:0,info:'Same speed'};
  }
  let PV=dx*dvx+dy*dvy;
  let tcpa=-PV/VV;
  if(tcpa<0)tcpa=0;
  let xA2=xA+vxA*tcpa;
  let yA2=yA+vyA*tcpa;
  let xB2=xB+vxB*tcpa;
  let yB2=yB+vyB*tcpa;
  let dist=Math.sqrt((xA2-xB2)*(xA2-xB2)+(yA2-yB2)*(yA2-yB2));

  let info='';
  if(tcpa===0 && PV>0) info='Ships separating';
  else info='Potential collision';

  return {cpa:dist,tcpa:tcpa,info:info};
}

function toggleSelectShip(mmsi) {
  let idx=selectedShips.indexOf(mmsi);
  if(idx>-1){
    selectedShips.splice(idx,1);
  } else {
    if(selectedShips.length===2) {
      selectedShips.shift();
    }
    selectedShips.push(mmsi);
  }
  updateShipVectors();
  updateSelectedShipBox();
  updateLeftPanel();
}

function updateShipVectors() {
  for(let k in shipVectors){
    map.removeLayer(shipVectors[k]);
  }
  shipVectors={};
  selectedShips.forEach(m=>{
    let s=ships[m].data;
    if(s && s.sog && s.cog && s.sog>0){
      let end=computeVectorEnd(s, vectorTime);
      let poly=L.polyline([[s.latitude,s.longitude],[end.lat,end.lon]], {color:'blue',dashArray:'5,5'});
      poly.addTo(map);
      shipVectors[m]=poly;
    }
  });
}

function computeVectorEnd(ship,minutes){
  let sogMin=ship.sog/60;
  let dist=sogMin*minutes;
  let cog=ship.cog*Math.PI/180;
  let latRef=ship.latitude;
  let scale_lat=60;
  let scale_lon=60*Math.cos(latRef*Math.PI/180);

  function toXY(lat,lon){return [lon*scale_lon,lat*scale_lat];}
  function toLatLon(x,y){return [y/scale_lat,x/scale_lon];}

  let [x,y]=toXY(ship.latitude,ship.longitude);
  let dx=dist*Math.sin(cog);
  let dy=dist*Math.cos(cog);
  let x2=x+dx,y2=y+dy;
  let [lat2,lon2]=toLatLon(x2,y2);
  return {lat:lat2,lon:lon2};
}

function updateSelectedShipBox(){
  if(selectedShipBoxMarker){
    map.removeLayer(selectedShipBoxMarker);
    selectedShipBoxMarker=null;
  }
  if(selectedShips.length>0){
    let m=selectedShips[selectedShips.length-1];
    let s=ships[m].data;
    // Ikona kwadratu większego niż symbol statku
    let icon=L.divIcon({
      className:'',
      iconSize:[30,30],
      iconAnchor:[15,15],
      html:`<div style="width:30px;height:30px;border:2px dashed red;box-sizing:border-box;"></div>`
    });
    selectedShipBoxMarker=L.marker([s.latitude,s.longitude],{icon:icon,zIndexOffset:1000});
    selectedShipBoxMarker.addTo(map);
  }
}

function getShipTooltip(s) {
  let lastUpdate=new Date(s.timestamp);
  let now=Date.now();
  let diffSec=Math.floor((now-lastUpdate.getTime())/1000);
  let diffText=diffSec<60?`${diffSec}s ago`:`${Math.floor(diffSec/60)}min ago`;
  return `${s.ship_name||s.mmsi}<br>MMSI:${s.mmsi}<br>COG:${s.cog}<br>SOG:${s.sog}<br>Length:${s.ship_length||'N/A'}<br>Last update: ${diffText}`;
}

function getShipIcon(s){
  let length=s.ship_length;
  let fillColor='none';
  let scale=1.0;
  if(length===null){fillColor='none';scale=1.0;}
  else if(length<50){fillColor='green';scale=0.8;}
  else if(length<150){fillColor='yellow';scale=1.0;}
  else if(length<250){fillColor='orange';scale=1.2;}
  else {fillColor='red';scale=1.4;}

  let rotation=s.cog||0;
  let size=20*scale;

  // Symbol statku: polygon z apexem do góry, środek w (10,10)
  let icon=L.divIcon({
    className:'',
    iconSize:[size,size],
    iconAnchor:[size/2,size/2], // kotwiczymy środek
    html:`<svg width="${size}" height="${size}" viewBox="0 0 20 20" style="transform:rotate(${rotation}deg);">
<polygon points="10,0 20,20 10,15 0,20" fill="${fillColor}" stroke="black" stroke-width="1"/>
</svg>`
  });
  return icon;
}

function getCollisionIcon(){
  let icon=L.divIcon({
    className:'',
    html:`<div style="width:40px;height:40px;background:#f00;border-radius:50%;"></div>`,
    iconSize:[40,40],
    iconAnchor:[20,20]
  });
  return icon;
}

document.addEventListener('DOMContentLoaded', initMap);