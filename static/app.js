let map;
let shipMarkers = {}; // {mmsi:{marker,boxMarker,vectorLine}}
let selectedShips = [];
let vectorLength = 15;
let collisionsData = [];
let cpaFilterVal = 0.5;
let tcpaFilterVal = 10;

function initMap() {
  map = L.map('map').setView([52.237049,21.017532],6);
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:18}).addTo(map);
  L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',{maxZoom:18,opacity:0.7}).addTo(map);

  document.getElementById('vectorLengthSlider').addEventListener('input',()=>{
    vectorLength = parseInt(document.getElementById('vectorLengthSlider').value);
    document.getElementById('vectorLengthValue').textContent=vectorLength;
    updateSelectedShipsInfo();
  });

  document.getElementById('cpaFilter').addEventListener('input',()=>{
    cpaFilterVal = parseFloat(document.getElementById('cpaFilter').value);
    document.getElementById('cpaValue').textContent = cpaFilterVal.toFixed(2);
    updateCollisionsList();
  });

  document.getElementById('tcpaFilter').addEventListener('input',()=>{
    tcpaFilterVal = parseInt(document.getElementById('tcpaFilter').value);
    document.getElementById('tcpaValue').textContent = tcpaFilterVal;
    updateCollisionsList();
  });

  fetchAndUpdateData();
  setInterval(fetchAndUpdateData,5000);
}

function fetchAndUpdateData() {
  fetch('/ships')
    .then(r=>r.json())
    .then(data=>updateShips(data))
    .catch(e=>console.error("Error ships:",e));

  fetch('/collisions')
    .then(r=>r.json())
    .then(data=>{ collisionsData=data; updateCollisionsList();})
    .catch(e=>console.error("Error collisions:",e));
}

function getShipStyle(ship) {
  let length=ship.ship_length;
  let fillColor='none';
  let scale=1.0;
  if(length) {
    if(length<50){fillColor='green';scale=0.9;}
    else if(length<=150){fillColor='yellow';scale=1.1;}
    else if(length<=250){fillColor='orange';scale=1.2;}
    else {fillColor='red';scale=1.3;}
  }
  return {fillColor,scale};
}

function humanTimeDiff(ts) {
  if(!ts)return '';
  let now=Date.now();
  let t=new Date(ts).getTime();
  let diffSec=(now - t)/1000;
  if(diffSec<60) return diffSec.toFixed(0)+'s ago';
  else {
    let diffMin=diffSec/60;
    return diffMin.toFixed(1)+'m ago';
  }
}

function updateShips(data) {
  let seen=new Set();
  data.forEach(ship=>{
    seen.add(ship.mmsi);
    let {fillColor,scale}=getShipStyle(ship);
    const icon=createShipIcon(ship.cog,fillColor,scale);
    if(!shipMarkers[ship.mmsi]) {
      let marker=L.marker([ship.latitude,ship.longitude],{icon}).addTo(map);
      marker.shipData=ship;
      marker.on('click',()=>toggleSelectShip(ship));
      let tooltipContent=`${ship.ship_name||("MMSI:"+ship.mmsi)}<br>SOG:${ship.sog||'N/A'}kn<br>COG:${ship.cog||'N/A'}°<br>${ship.ship_length?'Len:'+ship.ship_length+'m<br>':''}${humanTimeDiff(ship.timestamp)}`;
      marker.bindTooltip(tooltipContent,{permanent:false,className:'ship-tooltip'});
      shipMarkers[ship.mmsi]={marker:marker,boxMarker:null,vectorLine:null};
    } else {
      const obj=shipMarkers[ship.mmsi];
      obj.marker.setLatLng([ship.latitude,ship.longitude]);
      obj.marker.shipData=ship;
      obj.marker.setIcon(icon);
      let tooltipContent=`${ship.ship_name||("MMSI:"+ship.mmsi)}<br>SOG:${ship.sog||'N/A'}kn<br>COG:${ship.cog||'N/A'}°<br>${ship.ship_length?'Len:'+ship.ship_length+'m<br>':''}${humanTimeDiff(ship.timestamp)}`;
      obj.marker.bindTooltip(tooltipContent,{permanent:false,className:'ship-tooltip'});
    }
  });

  for(let mmsi in shipMarkers) {
    if(!seen.has(parseInt(mmsi))) {
      let obj=shipMarkers[mmsi];
      if(obj.boxMarker) map.removeLayer(obj.boxMarker);
      if(obj.vectorLine) map.removeLayer(obj.vectorLine);
      map.removeLayer(obj.marker);
      delete shipMarkers[mmsi];
    }
  }

  updateSelectedShipsInfo();
}

function createShipIcon(cog,fillColor,scale) {
  let w=20*scale,h=20*scale;
  return L.divIcon({
    className:'',
    html:`<div style="transform:rotate(${cog}deg);width:${w}px;height:${h}px;">
      <svg width="${w}" height="${h}" viewBox="0 0 20 20">
        <polygon points="10,0 15,20 10,15 5,20"
          fill="${fillColor}" stroke="#000" stroke-width="1"/>
      </svg>
    </div>`,
    iconSize:[w,h],
    iconAnchor:[w/2,h/2]
  });
}

function createBoxIcon(scale){
  // Slightly bigger than ship icon: if ship ~20x20, box ~30x30:
  let size=30*scale;
  return L.divIcon({
    className:'',
    html:`<div style="width:${size}px;height:${size}px;border:2px dashed black;"></div>`,
    iconSize:[size,size],
    iconAnchor:[size/2,size/2]
  });
}

function toggleSelectShip(ship) {
  const idx=selectedShips.findIndex(s=>s.mmsi===ship.mmsi);
  if(idx>=0) {
    selectedShips.splice(idx,1);
  } else {
    if(selectedShips.length<2) selectedShips.push(ship);
    else {
      selectedShips[0]=selectedShips[1];
      selectedShips[1]=ship;
    }
  }
  updateSelectedShipsInfo();
}

function updateSelectedShipsInfo() {
  const container=document.getElementById('selected-ships-info');
  container.innerHTML='';
  selectedShips.forEach(ship=>{
    let div=document.createElement('div');
    div.classList.add('ship-info');
    div.innerHTML=`
      <strong>${ship.ship_name||("MMSI:"+ship.mmsi)}</strong><br>
      MMSI: ${ship.mmsi}<br>
      SOG: ${ship.sog||'N/A'} kn<br>
      COG: ${ship.cog||'N/A'}°<br>
      ${ship.ship_length?'Length: '+ship.ship_length+' m':''}
    `;
    container.appendChild(div);
  });

  if(selectedShips.length===2) {
    const cpaData=computeCPA(selectedShips[0],selectedShips[1]);
    if(cpaData) {
      let div=document.createElement('div');
      div.classList.add('ship-info');
      div.innerHTML=`
        <strong>CPA/TCPA for selected pair:</strong><br>
        CPA: ${cpaData.cpa.toFixed(2)} nm<br>
        TCPA: ${cpaData.tcpa.toFixed(2)} min
      `;
      container.appendChild(div);
    }
  }

  drawSelectionHighlights();
}

function drawSelectionHighlights() {
  for(let mmsi in shipMarkers) {
    let obj=shipMarkers[mmsi];
    if(obj.boxMarker){map.removeLayer(obj.boxMarker);obj.boxMarker=null;}
    if(obj.vectorLine){map.removeLayer(obj.vectorLine);obj.vectorLine=null;}
  }

  selectedShips.forEach(ship=>{
    const m=shipMarkers[ship.mmsi];
    if(!m)return;
    const pos=m.marker.getLatLng();

    // create box marker icon
    let {scale}=getShipStyle(ship);
    let boxIcon=createBoxIcon(scale);
    m.boxMarker=L.marker(pos,{icon:boxIcon,interactive:false}).addTo(map);

    // Vector line: black dashed line
    const cpaData=computeCPAData(ship,pos);
    if(cpaData){
      let lineOpts={color:'black',weight:2,dashArray:'5,5'};
      m.vectorLine=L.polyline([pos,[cpaData.endLat,cpaData.endLng]],lineOpts).addTo(map);
    }
  });
}

function computeCPAData(ship,pos) {
  if(!ship.sog||!ship.cog) return null;
  let distanceNm=ship.sog*(vectorLength/60);
  let distanceDeg=distanceNm*(1/60);
  let cogRad=ship.cog*(Math.PI/180);

  // Approx: latDist = distanceDeg * cos, lngDist = distanceDeg * sin
  // This is approximate, but good enough for short distances
  let endLat=pos.lat + distanceDeg*Math.cos(cogRad);
  let endLng=pos.lng + distanceDeg*Math.sin(cogRad);
  return {endLat,endLng};
}

function computeCPA(shipA, shipB) {
  function toXY(lat,lon) {
    let latRef=50;
    const scaleLat=111000,scaleLon=111000*Math.cos(latRef*Math.PI/180);
    let x=lon*scaleLon,y=lat*scaleLat;
    return [x,y];
  }

  let A=toXY(shipA.latitude,shipA.longitude);
  let B=toXY(shipB.latitude,shipB.longitude);
  let sogA=shipA.sog?shipA.sog*0.51444:0;
  let sogB=shipB.sog?shipB.sog*0.51444:0;
  let cogA=(shipA.cog||0)*Math.PI/180;
  let cogB=(shipB.cog||0)*Math.PI/180;
  let vxA=sogA*Math.sin(cogA), vyA=sogA*Math.cos(cogA);
  let vxB=sogB*Math.sin(cogB), vyB=sogB*Math.cos(cogB);

  let dx=A[0]-B[0],dy=A[1]-B[1];
  let dvx=vxA-vxB,dvy=vyA-vyB;
  let VV=dvx*dvx+dvy*dvy;
  let PV=dx*dvx+dy*dvy;
  let tcpa=0;
  if(VV!==0) tcpa=-PV/VV;
  if(tcpa<0) tcpa=0;
  let xA2=A[0]+vxA*tcpa,yA2=A[1]+vyA*tcpa;
  let xB2=B[0]+vxB*tcpa,yB2=B[1]+vyB*tcpa;
  let dist=Math.sqrt((xA2-xB2)**2+(yA2-yB2)**2);
  let distNm=dist/1852;
  let tcpaMin=tcpa/60;
  return {cpa:distNm,tcpa:tcpaMin};
}

function updateCollisionsList() {
  const list=document.getElementById('collision-list');
  list.innerHTML='';

  let filtered=collisionsData.filter(c=>{
    if(c.cpa>cpaFilterVal)return false;
    if(c.tcpa>tcpaFilterVal)return false;
    return true;
  });

  // sort by TCPA ascending
  filtered.sort((a,b)=>a.tcpa - b.tcpa);

  filtered.forEach(col=>{
    const item=document.createElement('div');
    item.classList.add('collision-item');
    let shipA=col.ship1_name||('Unknown');
    let shipB=col.ship2_name||('Unknown');
    item.innerHTML=`
      <div><strong>${shipA} - ${shipB}</strong><br>
      CPA: ${col.cpa.toFixed(2)} nm, TCPA: ${col.tcpa.toFixed(2)} min
      <button class="zoom-button">🔍</button>
      </div>
    `;
    item.querySelector('.zoom-button').addEventListener('click',()=>{
      map.setView([(col.latitude_a+col.latitude_b)/2,(col.longitude_a+col.longitude_b)/2],12);
      // After zoom, select these ships
      fetch('/ships')
        .then(r=>r.json())
        .then(data=>{
          let sA=data.find(s=>s.mmsi===col.mmsi_a);
          let sB=data.find(s=>s.mmsi===col.mmsi_b);
          selectedShips=[];
          if(sA) selectedShips.push(sA);
          if(sB) selectedShips.push(sB);
          updateSelectedShipsInfo();
        });
    });
    list.appendChild(item);
  });
}

document.addEventListener('DOMContentLoaded', initMap);