let map;
let shipMarkers = {}; // key: mmsi, value: {marker,box,vectorLine}
let selectedShips = []; // max 2 statki
let vectorLength = 15; 
let riskFilters = ['High','Medium','Low'];
let collisionsData = [];
let cpaFilterVal = 0.5;
let tcpaFilterVal = 10;

// init map
function initMap() {
  map = L.map('map').setView([52.237049, 21.017532], 6);

  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom:18}).addTo(map);
  L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',{maxZoom:18,opacity:0.7}).addTo(map);

  document.getElementById('vectorLengthSlider').addEventListener('input', () => {
    vectorLength = parseInt(document.getElementById('vectorLengthSlider').value);
    document.getElementById('vectorLengthValue').textContent = vectorLength;
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
  setInterval(fetchAndUpdateData, 5000);
}

function fetchAndUpdateData() {
  fetch('/ships')
    .then(r=>r.json())
    .then(data => updateShips(data))
    .catch(e=>console.error("Error ships:",e));

  fetch('/collisions')
    .then(r=>r.json())
    .then(data => { collisionsData=data; updateCollisionsList();})
    .catch(e=>console.error("Error collisions:",e));
}

// Kolor i skala w zależności od długości
function getShipStyle(ship) {
  let length = ship.ship_length;
  let fillColor = 'none';
  let scale = 1.0;
  if (length) {
    if (length<50) { fillColor='green'; scale=0.9; }
    else if (length<=150) { fillColor='yellow'; scale=1.1; }
    else if (length<=250) { fillColor='orange'; scale=1.2; }
    else { fillColor='red'; scale=1.3; }
  }

  return {fillColor,scale};
}

function updateShips(data) {
  let seen = new Set();
  data.forEach(ship=>{
    seen.add(ship.mmsi);
    let {fillColor,scale} = getShipStyle(ship);

    if(!shipMarkers[ship.mmsi]) {
      const icon = createShipIcon(ship.cog, fillColor, scale, selectedShips.some(s=>s.mmsi==ship.mmsi));
      let marker = L.marker([ship.latitude, ship.longitude], {icon}).addTo(map);
      marker.shipData=ship;

      marker.on('click',()=>toggleSelectShip(ship));
      shipMarkers[ship.mmsi]={marker:marker,box:null,vectorLine:null};
    } else {
      const m = shipMarkers[ship.mmsi].marker;
      m.setLatLng([ship.latitude, ship.longitude]);
      m.shipData = ship;
      const selected = selectedShips.some(s=>s.mmsi==ship.mmsi);
      const icon = createShipIcon(ship.cog, fillColor, scale, selected);
      m.setIcon(icon);
    }
  });

  for(let mmsi in shipMarkers) {
    if(!seen.has(parseInt(mmsi))) {
      // remove
      const obj = shipMarkers[mmsi];
      if(obj.box) map.removeLayer(obj.box);
      if(obj.vectorLine) map.removeLayer(obj.vectorLine);
      map.removeLayer(obj.marker);
      delete shipMarkers[mmsi];
    }
  }

  updateSelectedShipsInfo();
}

function createShipIcon(cog,fillColor,scale,selected) {
  // skalujemy polygon w SVG
  // trójkąt w środku, transform rotate(cog deg)
  const strokeColor = selected?'red':'#000';
  let w=20*scale,h=20*scale;
  return L.divIcon({
    className:'',
    html:`<div style="transform:rotate(${cog}deg);width:${w}px;height:${h}px;">
      <svg width="${w}" height="${h}" viewBox="0 0 20 20">
        <polygon points="10,0 15,20 10,15 5,20" fill="${fillColor}" stroke="${strokeColor}" stroke-width="1"/>
      </svg>
    </div>`,
    iconSize:[w,h],
    iconAnchor:[w/2,h/2]
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
  const container = document.getElementById('selected-ships-info');
  container.innerHTML='';
  selectedShips.forEach(ship=>{
    let div=document.createElement('div');
    div.classList.add('ship-info');
    div.innerHTML=`
      <strong>${ship.ship_name||("MMSI:"+ship.mmsi)}</strong>
      MMSI: ${ship.mmsi}<br>
      SOG: ${ship.sog||'N/A'}<br>
      COG: ${ship.cog||'N/A'}<br>
      Długość: ${ship.ship_length||'N/A'}<br>
      Wektor: ${vectorLength} min
    `;
    container.appendChild(div);
  });

  // Jeśli 2 statki zaznaczone, licz cpa/tcpa i pokaż:
  if(selectedShips.length===2) {
    const cpaData = computeCPA(selectedShips[0], selectedShips[1]);
    if(cpaData) {
      let div = document.createElement('div');
      div.classList.add('ship-info');
      div.innerHTML=`
        <strong>CPA/TCPA dla pary:</strong>
        CPA: ${cpaData.cpa.toFixed(2)} nm<br>
        TCPA: ${cpaData.tcpa.toFixed(2)} min
      `;
      container.appendChild(div);
    }
  }

  drawSelectionHighlights();
}

function drawSelectionHighlights() {
  // Usuń stare boxy i vectorLine
  for(let mmsi in shipMarkers) {
    let obj=shipMarkers[mmsi];
    if(obj.box){map.removeLayer(obj.box);obj.box=null;}
    if(obj.vectorLine){map.removeLayer(obj.vectorLine);obj.vectorLine=null;}
  }

  // Dodaj box i vector dla zaznaczonych
  selectedShips.forEach(ship=>{
    const m = shipMarkers[ship.mmsi];
    if(!m) return;
    const pos = m.marker.getLatLng();

    // Kwadrat większy niż symbol: np. 30x30px w koord mapy – trudne, bo Leaflet w pixelach offset.
    // Zrobimy mały kwadrat geograficzny ~0.0005deg w każdą stronę:
    let delta = 0.0008; 
    let boxBounds = [[pos.lat-delta,pos.lng-delta],[pos.lat+delta,pos.lng+delta]];
    m.box = L.rectangle(boxBounds, {color:'red',weight:1,fill:false}).addTo(map);

    // Wektor: sog(kn), vectorLength(min)-> odległość w nm = sog*(vectorLength/60)
    // 1nm ~ 1/60 deg lat. 
    let distanceNm = ship.sog*(vectorLength/60);
    let distanceDeg = distanceNm*(1/60);
    // Kierunek (cog deg): w rad
    let cogRad = ship.cog*(Math.PI/180);
    let endLat = pos.lat + distanceDeg * Math.cos(cogRad);
    let endLng = pos.lng + distanceDeg * Math.sin(cogRad);

    m.vectorLine = L.polyline([pos,[endLat,endLng]],{color:'blue',weight:2}).addTo(map);
  });
}

function computeCPA(shipA, shipB) {
  // Prostą implementację CPA/TCPA:
  // Dane: lat, lon, sog(kn), cog(deg)
  // Rzutujemy na płaszczyznę: 
  // uproszczony model na małej odległości:
  function toXY(lat,lon) {
    // approx: lat scale ~111 km/deg, cos(lat)*111 km/deg for lon
    let latRef=50; 
    const scaleLat=111000, scaleLon=111000*Math.cos(latRef*Math.PI/180);
    let x=lon*scaleLon;
    let y=lat*scaleLat;
    return [x,y];
  }

  let A=toXY(shipA.latitude,shipA.longitude);
  let B=toXY(shipB.latitude,shipB.longitude);

  let sogA=shipA.sog*0.51444; // m/s
  let sogB=shipB.sog*0.51444;
  let cogA=shipA.cog*(Math.PI/180);
  let cogB=shipB.cog*(Math.PI/180);
  let vxA=sogA*Math.sin(cogA), vyA=sogA*Math.cos(cogA);
  let vxB=sogB*Math.sin(cogB), vyB=sogB*Math.cos(cogB);

  let dx=A[0]-B[0];
  let dy=A[1]-B[1];
  let dvx=vxA-vxB;
  let dvy=vyA-vyB;
  let VV=dvx*dvx+dvy*dvy;
  let PV=dx*dvx+dy*dvy;
  let tcpa=0;
  if(VV!==0) {
    tcpa=-PV/VV;
  }
  if(tcpa<0) tcpa=0; // minimal in future
  // po tcpa positions:
  let xA2=A[0]+vxA*tcpa, yA2=A[1]+vyA*tcpa;
  let xB2=B[0]+vxB*tcpa, yB2=B[0]+vyB*tcpa; 
  // oops correction: last line B[0]+??? Should be B[0]+vxB*tcpa
  xB2=B[0]+vxB*tcpa; yB2=B[1]+vyB*tcpa;
  let dist=Math.sqrt((xA2-xB2)**2+(yA2-yB2)**2);
  // dist in meters -> nm = dist/1852
  let distNm=dist/1852;
  let tcpaMin=tcpa/60; // tcpa in s, divide by 60 for minutes
  return {cpa:distNm, tcpa:tcpaMin};
}

function updateCollisionsList() {
  const list=document.getElementById('collision-list');
  list.innerHTML='';

  let filtered = collisionsData.filter(c=>{
    if(!riskFilters.includes(c.risk_category))return false;
    if(c.cpa>cpaFilterVal)return false;
    if(c.tcpa>tcpaFilterVal)return false;
    return true;
  });

  filtered.sort((a,b)=>a.cpa-b.cpa);

  filtered.forEach(col=>{
    const item=document.createElement('div');
    item.classList.add('collision-item', col.risk_category.toLowerCase());
    item.innerHTML=`
      <div><strong>${col.ship1_name||col.ship1_mmsi} - ${col.ship2_name||col.ship2_mmsi}</strong><br>
      CPA: ${col.cpa.toFixed(2)} nm, TCPA: ${col.tcpa.toFixed(2)} min
      <button class="zoom-button">🔍</button>
      </div>
    `;
    item.querySelector('.zoom-button').addEventListener('click',()=>{
      map.setView([(col.latitude_a+col.latitude_b)/2,(col.longitude_a+col.longitude_b)/2],12);
    });
    list.appendChild(item);
  });
}

function filterCollisions() {
  const checkboxes = document.querySelectorAll('#risk-filters input[type="checkbox"]');
  riskFilters=[];
  checkboxes.forEach(cb=>{if(cb.checked)riskFilters.push(cb.value);});
  updateCollisionsList();
}

document.addEventListener('DOMContentLoaded', initMap);