let map;
let shipMarkers = {};
let collisionsData = [];
let selectedShips = [];
let historyMarkers = {};
let vectorLength = 15; // in minutes
let cpaFilter = 0.5;
let tcpaFilter = 10;

function initMap() {
  map = L.map('map').setView([50.0, 0.0], 6);

  // Podkład
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18
  }).addTo(map);

  console.log("Map initialized");
  fetchAndUpdateData();
  setInterval(fetchAndUpdateData, 60000);

  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value);
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo();
  });
  document.getElementById('cpaFilter').addEventListener('input', e=>{
    cpaFilter = parseFloat(e.target.value);
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    updateCollisionsList();
  });
  document.getElementById('tcpaFilter').addEventListener('input', e=>{
    tcpaFilter = parseFloat(e.target.value);
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    updateCollisionsList();
  });

  document.getElementById('clearSelectedShips').addEventListener('click', ()=>{
    clearSelectedShips();
  });
}

function fetchAndUpdateData() {
  fetch('/ships')
    .then(res=>res.json())
    .then(data=>{
      console.log("Ships data:", data);
      updateShips(data);
    })
    .catch(err=>console.error("Error ships:", err));

  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(res=>res.json())
    .then(data=> {
      console.log("Collisions data:", data);
      collisionsData = data;
      updateCollisionsList();
    })
    .catch(err=>console.error("Error collisions:", err));
}

function updateShips(data) {
  let currentMmsiSet = new Set(data.map(d=>d.mmsi));

  for (let m in shipMarkers) {
    if(!currentMmsiSet.has(parseInt(m))) {
      map.removeLayer(shipMarkers[m]);
      delete shipMarkers[m];
      if (historyMarkers[m]) {
        historyMarkers[m].forEach(hm=>map.removeLayer(hm));
        delete historyMarkers[m];
      }
    }
  }

  data.forEach(ship=>{
    const length = ship.ship_length;
    let fillColor='lightgray'; // zawsze coś w środku, żeby było widać
    let scale=1.0;
    let strokeColor='#000000';
    if (length == null) {
      fillColor='lightgray'; scale=1.0;
    } else if (length<50) {fillColor='green'; scale=0.9;}
    else if (length<150) {fillColor='yellow'; scale=1.0;}
    else if (length<250) {fillColor='orange'; scale=1.1;}
    else {fillColor='red'; scale=1.2;}

    const rotation = ship.cog||0;
    const shipIcon = L.divIcon({
      className:'',
      html:`<svg width="${20*scale}" height="${20*scale}" viewBox="-10 -10 20 20" style="transform:rotate(${rotation}deg);">
        <polygon points="0,-10 5,10 0,5 -5,10" fill="${fillColor}" stroke="${strokeColor}" stroke-width="1"/>
      </svg>`,
      iconSize:[20*scale,20*scale],
      iconAnchor:[10*scale,10*scale]
    });

    let marker = shipMarkers[ship.mmsi];
    if(!marker) {
      marker = L.marker([ship.latitude, ship.longitude], {icon: shipIcon});
      marker.addTo(map);
      marker.shipData = ship;
      marker.on('click', ()=>selectShip(ship.mmsi));
      marker.on('mouseover', ()=>{
        const now=Date.now();
        const updatedAt=new Date(ship.timestamp).getTime();
        const diffSec=Math.round((now-updatedAt)/1000);
        const diffStr= diffSec<60?`${diffSec}s ago`:`${Math.floor(diffSec/60)}m ago`;
        const content=`
          <b>${ship.ship_name}</b><br>
          MMSI: ${ship.mmsi}<br>
          SOG: ${ship.sog||'N/A'} kn<br>
          COG: ${ship.cog||'N/A'} deg<br>
          Length: ${ship.ship_length||'N/A'}<br>
          Updated: ${diffStr}
        `;
        L.popup()
         .setLatLng([ship.latitude, ship.longitude])
         .setContent(content)
         .openOn(map);
      });
      marker.on('mouseout', ()=>map.closePopup());
      shipMarkers[ship.mmsi]=marker;
    } else {
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(shipIcon);
      marker.shipData=ship;
    }
  });

  updateSelectedShipsInfo();
}

function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  let filtered = collisionsData.filter(c=>c.cpa<=cpaFilter && c.tcpa<=tcpaFilter);
  filtered.sort((a,b)=>a.tcpa - b.tcpa);

  filtered.forEach(c=>{
    const item = document.createElement('div');
    item.classList.add('collision-item');
    const shipA = c.ship1_name || c.mmsi_a;
    const shipB = c.ship2_name || c.mmsi_b;
    item.innerHTML = `
      <div class="collision-header">
        <strong>${shipA} - ${shipB}</strong><br>
        CPA: ${c.cpa.toFixed(2)} nm, TCPA: ${c.tcpa.toFixed(2)} min
        <button class="zoom-button">🔍</button>
      </div>
    `;
    item.querySelector('.zoom-button').addEventListener('click', ()=>{
      const lat = (c.latitude_a+c.latitude_b)/2;
      const lon = (c.longitude_a+c.longitude_b)/2;
      map.setView([lat,lon],10);

      clearSelectedShips();
      selectShip(c.mmsi_a);
      selectShip(c.mmsi_b);

      vectorLength=Math.ceil(c.tcpa);
      if(vectorLength<1) vectorLength=1;
      document.getElementById('vectorLengthSlider').value=vectorLength;
      document.getElementById('vectorLengthValue').textContent=vectorLength;
      updateSelectedShipsInfo();
    });

    collisionList.appendChild(item);
  });
}

function selectShip(mmsi) {
  if(selectedShips.length>=2) {
    clearSelectedShips();
  }
  if(!selectedShips.includes(mmsi)) {
    selectedShips.push(mmsi);
    updateSelectedShipsInfo();
  }
}

function clearSelectedShips() {
  selectedShips=[];
  updateSelectedShipsInfo();
}

function updateSelectedShipsInfo() {
  const container = document.getElementById('selected-ships-info');
  container.innerHTML = '';
  for(let m in historyMarkers) {
    historyMarkers[m].forEach(h=>map.removeLayer(h));
  }
  historyMarkers={};

  if(selectedShips.length===0) {
    document.getElementById('pair-info').innerHTML='';
    return;
  }

  let selectedData = selectedShips.map(m=>shipMarkers[m]?shipMarkers[m].shipData:null).filter(d=>d);
  selectedData.forEach(sd=>{
    const div = document.createElement('div');
    div.innerHTML=`
    <b>${sd.ship_name}</b><br>
    MMSI:${sd.mmsi}<br>
    SOG:${sd.sog||'N/A'}<br>
    COG:${sd.cog||'N/A'}<br>
    Length:${sd.ship_length||'N/A'}
    `;
    container.appendChild(div);
  });

  if(selectedShips.length===2) {
    // CPA/TCPA placeholder
    document.getElementById('pair-info').innerHTML='<b>CPA/TCPA:</b> dummy 0.1nm / 2.5min';
  } else {
    document.getElementById('pair-info').innerHTML='';
  }

  selectedShips.forEach(mmsi=>{
    showHistory(mmsi);
  });
}

function showHistory(mmsi) {
  fetch('/ships')
    .then(r=>r.json())
    .then(data=>{
      let shipPos=data.filter(d=>d.mmsi===mmsi);
      shipPos.sort((a,b)=>(new Date(b.timestamp))-(new Date(a.timestamp)));
      shipPos=shipPos.slice(0,10);

      historyMarkers[mmsi]=[];
      const now=Date.now();
      shipPos.forEach((pos,i)=>{
        let opacity=1.0-(i*0.1);
        if(opacity<0.1) opacity=0.1;
        const rotation=pos.cog||0;
        let fillColor='lightgray',strokeColor='#000',scale=1.0;
        const length=pos.ship_length;
        if(length==null){fillColor='lightgray';scale=1.0;}
        else if(length<50){fillColor='green';scale=0.9;}
        else if(length<150){fillColor='yellow';scale=1.0;}
        else if(length<250){fillColor='orange';scale=1.1;}
        else{fillColor='red';scale=1.2;}

        const icon = L.divIcon({
          className:'',
          html:`<svg width="${20*scale}" height="${20*scale}" viewBox="-10 -10 20 20" style="transform:rotate(${rotation}deg);opacity:${opacity}">
            <polygon points="0,-10 5,10 0,5 -5,10" fill="${fillColor}" stroke="${strokeColor}" stroke-width="1"/>
          </svg>`,
          iconSize:[20*scale,20*scale],
          iconAnchor:[10*scale,10*scale]
        });

        const marker=L.marker([pos.latitude,pos.longitude],{icon});
        marker.addTo(map);
        historyMarkers[mmsi].push(marker);

        const updatedAt=new Date(pos.timestamp).getTime();
        const diffSec=Math.round((now-updatedAt)/1000);
        const diffStr=diffSec<60?`${diffSec}s ago`:`${Math.floor(diffSec/60)}m ago`;

        marker.on('mouseover',()=>{
          L.popup()
           .setLatLng([pos.latitude,pos.longitude])
           .setContent(`${pos.ship_name}<br>History pos: ${diffStr}`)
           .openOn(map);
        });
        marker.on('mouseout',()=>map.closePopup());
      });
    })
    .catch(err=>console.error("Error history:",err));
}

document.addEventListener('DOMContentLoaded', initMap);