let map;
let shipMarkers = {};
let collisionsData = [];
let selectedShips = [];
let historyMarkers = {};
let vectorLength = 15; // in minutes
let cpaFilter = 0.5;
let tcpaFilter = 10;
let markerClusterGroup; 
let collisionMarkers = [];

// Cache CPA/TCPA
let lastSelectedPair = null;
let lastCpaTcpa = null; // {cpa:..., tcpa:...} albo null

function initMap() {
  map = L.map('map').setView([50.0, 0.0], 6);

  const osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18
  });

  const openSeaMapLayer = L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: 'Map data © OpenSeaMap contributors'
  });

  osmLayer.addTo(map);
  openSeaMapLayer.addTo(map);

  const baseMaps = {
    "OpenStreetMap": osmLayer,
  };
  const overlayMaps = {
    "OpenSeaMap": openSeaMapLayer
  };
  L.control.layers(baseMaps, overlayMaps, {collapsed:false}).addTo(map);

  markerClusterGroup = L.markerClusterGroup({
    maxClusterRadius:1, // ekstremalnie rzadkie klastrowanie
    removeOutsideVisibleBounds: true
  });
  map.addLayer(markerClusterGroup);

  fetchAndUpdateData();
  setInterval(fetchAndUpdateData, 60000);

  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value);
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    // Zmieniamy długość wektora ale nie zmieniamy zaznaczenia statków.
    // Nie będziemy ponownie pobierać cpa/tcpa jeśli para się nie zmieniła.
    updateSelectedShipsInfo(false); 
  });
  document.getElementById('cpaFilter').addEventListener('input', e=>{
    cpaFilter = parseFloat(e.target.value);
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchAndUpdateData(); 
  });
  document.getElementById('tcpaFilter').addEventListener('input', e=>{
    tcpaFilter = parseFloat(e.target.value);
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    fetchAndUpdateData();
  });

  document.getElementById('clearSelectedShips').addEventListener('click', ()=>{
    clearSelectedShips();
  });
}

function fetchAndUpdateData() {
  fetch(`/ships`)
    .then(res=>res.json())
    .then(data=>{
      updateShips(data);
    })
    .catch(err=>console.error("Error ships:", err));

  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(res=>res.json())
    .then(data=> {
      collisionsData = data;
      updateCollisionsList();
    })
    .catch(err=>console.error("Error collisions:", err));
}

function updateShips(data) {
  let currentMmsiSet = new Set(data.map(d=>d.mmsi));

  for (let m in shipMarkers) {
    if(!currentMmsiSet.has(parseInt(m))) {
      markerClusterGroup.removeLayer(shipMarkers[m]);
      delete shipMarkers[m];
      if (historyMarkers[m]) {
        historyMarkers[m].forEach(hm=>map.removeLayer(hm));
        delete historyMarkers[m];
      }
    }
  }

  data.forEach(ship=>{
    const length = ship.ship_length;
    let fillColor = 'none';
    let scale = 1.0;
    let strokeColor = '#000000';

    if (length != null) {
      if (length < 50) {fillColor='green'; scale=1.0;}
      else if (length<150) {fillColor='yellow'; scale=1.0;}
      else if (length<250) {fillColor='orange'; scale=1.0;} // zachowajmy 1.0 scale żeby były małe
      else {fillColor='red'; scale=1.0;} 
    } else {
      fillColor='none';
      scale=1.0;
    }

    // Mniejsze symbole: half size
    // Oryginalnie: viewBox="-10 -15 20 30"
    // Teraz: viewBox="-5 -7.5 10 15"
    // Trójkąt: "0,-7.5 5,7.5 -5,7.5"
    // Kwadrat zaznaczenia: wcześniej 40x40, teraz 20x20
    const rotation = ship.cog||0;
    const width = 10*scale; 
    const height = 15*scale; 

    let highlightRect = '';
    if(selectedShips.includes(ship.mmsi)) {
      highlightRect = `<rect x="-10" y="-10" width="20" height="20" fill="none" stroke="black" stroke-width="3" stroke-dasharray="5,5" />`;
    }

    const shape = `<polygon points="0,-7.5 5,7.5 -5,7.5" fill="${fillColor}" stroke="${strokeColor}" stroke-width="1"/>`;

    const shipIcon = L.divIcon({
      className:'',
      html:`<svg width="${width}" height="${height}" viewBox="-5 -7.5 10 15" style="transform:rotate(${rotation}deg);">
        ${highlightRect}
        ${shape}
      </svg>`,
      iconSize:[width,height],
      iconAnchor:[width/2,height/2]
    });

    let marker = shipMarkers[ship.mmsi];
    const now=Date.now();
    const updatedAt=new Date(ship.timestamp).getTime();
    const diffSec=Math.round((now-updatedAt)/1000);
    let diffMin = Math.floor(diffSec/60);
    let diffS = diffSec%60;
    const diffStr= `${diffMin} min ${diffS} s ago`;
    const content=`
      <b>${ship.ship_name}</b><br>
      MMSI: ${ship.mmsi}<br>
      SOG: ${ship.sog||'N/A'} kn<br>
      COG: ${ship.cog||'N/A'} deg<br>
      Length: ${ship.ship_length||'N/A'}<br>
      Updated: ${diffStr}
    `;

    if(!marker) {
      marker = L.marker([ship.latitude, ship.longitude], {icon: shipIcon});
      marker.bindTooltip(content, {direction:'top', sticky:true});
      marker.on('click', ()=>{
        selectShip(ship.mmsi);
      });
      marker.shipData = ship;
      shipMarkers[ship.mmsi]=marker;
      markerClusterGroup.addLayer(marker);
    } else {
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(shipIcon);
      marker.shipData=ship;
      marker.setTooltipContent(content);
    }
  });

  // Po zaktualizowaniu statków, odśwież panel
  updateSelectedShipsInfo(true);
}

function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];

  let filtered = collisionsData.filter(c=>c.tcpa>0);
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
      zoomToCollision(c);
    });

    collisionList.appendChild(item);

    // Marker kolizji
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

    const collisionMarker = L.marker([collisionLat, collisionLon], {icon: collisionIcon});
    collisionMarker.on('click', ()=>{
      zoomToCollision(c);
    });
    collisionMarker.addTo(map);
    collisionMarkers.push(collisionMarker);
  });
}

function zoomToCollision(c) {
  const lat = (c.latitude_a+c.latitude_b)/2;
  const lon = (c.longitude_a+c.longitude_b)/2;
  // Zoom ~14, aby pokazać obszar ~800-900m
  map.setView([lat,lon],14);

  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);

  vectorLength=Math.ceil(c.tcpa);
  if(vectorLength<1) vectorLength=1;
  document.getElementById('vectorLengthSlider').value=vectorLength;
  document.getElementById('vectorLengthValue').textContent=vectorLength;
  updateSelectedShipsInfo(true);
}

function selectShip(mmsi) {
  if(selectedShips.includes(mmsi)) {
    return;
  }
  if(selectedShips.length===2) {
    selectedShips.shift();
  }
  selectedShips.push(mmsi);
  updateSelectedShipsInfo(true);
}

function clearSelectedShips() {
  selectedShips=[];
  lastSelectedPair = null;
  lastCpaTcpa = null;
  updateSelectedShipsInfo(true);
}

function updateSelectedShipsInfo(selectionChanged) {
  // selectionChanged == true, jeśli zmieniła się lista zaznaczonych statków
  // jeśli false, to nie zmieniamy pary statków (np. zmiana vectorLength)
  
  const container = document.getElementById('selected-ships-info');
  container.innerHTML = '';
  document.getElementById('pair-info').innerHTML='';

  for(let m in historyMarkers) {
    historyMarkers[m].forEach(h=>map.removeLayer(h));
  }
  historyMarkers={};

  if(selectedShips.length===0) {
    reloadAllShipIcons();
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

  let currentPair = null;
  if(selectedShips.length===2) {
    // Sortujemy żeby para była deterministyczna
    currentPair = [...selectedShips].sort((a,b)=>a-b);
  } else {
    currentPair = null;
  }

  // Pobierz cpa/tcpa tylko jeśli mamy dwie zaznaczone jednostki
  // i jeśli para się zmieniła (selectionChanged oraz para inna niż lastSelectedPair)
  if(currentPair && (selectionChanged || JSON.stringify(currentPair)!=JSON.stringify(lastSelectedPair))) {
    lastSelectedPair = currentPair;
    lastCpaTcpa = null; 
    const mmsi_a = currentPair[0];
    const mmsi_b = currentPair[1];

    fetch(`/calculate_cpa_tcpa?mmsi_a=${mmsi_a}&mmsi_b=${mmsi_b}`)
      .then(r=>r.json())
      .then(data=>{
        let cpa = data.cpa;
        let tcpa = data.tcpa;
        if(!cpa || !tcpa || cpa>20 || tcpa>180 || cpa<0 || tcpa<0) {
          document.getElementById('pair-info').innerHTML='<b>CPA/TCPA:</b> N/A';
          lastCpaTcpa = {cpa:null, tcpa:null};
        } else {
          document.getElementById('pair-info').innerHTML=`<b>CPA/TCPA:</b> ${cpa.toFixed(2)} nm / ${tcpa.toFixed(2)} min`;
          lastCpaTcpa = {cpa, tcpa};
        }
      })
      .catch(err=>{
        console.error("Error fetching CPA/TCPA:", err);
        document.getElementById('pair-info').innerHTML='<b>CPA/TCPA:</b> N/A';
        lastCpaTcpa = {cpa:null, tcpa:null};
      });
  } else if(currentPair && lastCpaTcpa) {
    // Jeśli para się nie zmieniła, używamy poprzedniego wyniku
    if(!lastCpaTcpa.cpa || !lastCpaTcpa.tcpa) {
      document.getElementById('pair-info').innerHTML='<b>CPA/TCPA:</b> N/A';
    } else {
      document.getElementById('pair-info').innerHTML=`<b>CPA/TCPA:</b> ${lastCpaTcpa.cpa.toFixed(2)} nm / ${lastCpaTcpa.tcpa.toFixed(2)} min`;
    }
  }

  selectedShips.forEach(mmsi=>{
    showHistory(mmsi);
    drawVector(mmsi);
  });

  reloadAllShipIcons();
}

function reloadAllShipIcons() {
  for (let m in shipMarkers) {
    const ship = shipMarkers[m].shipData;
    const length = ship.ship_length;
    let fillColor = 'none';
    let scale = 1.0;
    let strokeColor = '#000000';

    if (length != null) {
      if (length < 50) {fillColor='green'; scale=1.0;}
      else if (length<150) {fillColor='yellow'; scale=1.0;}
      else if (length<250) {fillColor='orange'; scale=1.0;}
      else {fillColor='red'; scale=1.0;}
    } else {
      fillColor='none';scale=1.0;
    }

    const rotation = ship.cog||0;
    const width = 10*scale;
    const height = 15*scale;

    const shape = `<polygon points="0,-7.5 5,7.5 -5,7.5" fill="${fillColor}" stroke="${strokeColor}" stroke-width="1"/>`;

    let highlightRect='';
    if(selectedShips.includes(ship.mmsi)) {
      highlightRect = `<rect x="-10" y="-10" width="20" height="20" fill="none" stroke="black" stroke-width="3" stroke-dasharray="5,5" />`;
    }

    const icon = L.divIcon({
      className:'',
      html:`<svg width="${width}" height="${height}" viewBox="-5 -7.5 10 15" style="transform:rotate(${rotation}deg);">
        ${highlightRect}
        ${shape}
      </svg>`,
      iconSize:[width,height],
      iconAnchor:[width/2,height/2]
    });
    shipMarkers[m].setIcon(icon);
  }
}

function showHistory(mmsi) {
  // Past positions tylko dla zaznaczonych statków.
  if(!selectedShips.includes(mmsi)) return; 

  fetch('/ships')
    .then(r=>r.json())
    .then(data=>{
      let shipPos=data.filter(d=>d.mmsi===mmsi);
      shipPos.sort((a,b)=>(new Date(b.timestamp))-(new Date(a.timestamp)));
      shipPos=shipPos.slice(0,10);

      historyMarkers[mmsi]=[];
      const now=Date.now();
      // i=0 -> najnowsza historia (opacity=1.0), i=9 -> najstarsza (0.1)
      // opacity = 1.0 - i*0.1
      shipPos.forEach((pos,i)=>{
        let opacity=1.0 - i*0.1;
        const rotation=pos.cog||0;

        let fillColor='none',strokeColor='#000',scale=1.0;
        const length=pos.ship_length;
        if(length!=null){
          if(length<50){fillColor='green';scale=1.0;}
          else if(length<150){fillColor='yellow';scale=1.0;}
          else if(length<250){fillColor='orange';scale=1.0;}
          else{fillColor='red';scale=1.0;}
        } else {
          fillColor='none';scale=1.0;
        }

        const width = 10*scale;
        const height = 15*scale;
        const icon = L.divIcon({
          className:'',
          html:`<svg width="${width}" height="${height}" viewBox="-5 -7.5 10 15" style="transform:rotate(${rotation}deg);opacity:${opacity}">
            <polygon points="0,-7.5 5,7.5 -5,7.5" fill="${fillColor}" stroke="${strokeColor}" stroke-width="1"/>
          </svg>`,
          iconSize:[width,height],
          iconAnchor:[width/2,height/2]
        });

        const marker=L.marker([pos.latitude,pos.longitude],{icon});
        marker.addTo(map);
        historyMarkers[mmsi].push(marker);

        const updatedAt=new Date(pos.timestamp).getTime();
        const diffSec=Math.round((now-updatedAt)/1000);
        const diffMin = Math.floor(diffSec/60);
        const diffS = diffSec%60;

        const content = `${pos.ship_name}<br>History pos: ${diffMin} min ${diffS} s ago`;
        marker.bindTooltip(content,{direction:'top',sticky:true});
      });
    })
    .catch(err=>console.error("Error history:",err));
}

function drawVector(mmsi){
  const ship = shipMarkers[mmsi]? shipMarkers[mmsi].shipData : null;
  if(!ship || !ship.sog || !ship.cog) return;

  const distanceNm = ship.sog*(vectorLength/60);
  const lat = ship.latitude;
  const lon = ship.longitude;
  const cogRad = (ship.cog*Math.PI)/180;

  const deltaLat = (distanceNm/60)*Math.cos(cogRad);
  const deltaLon = (distanceNm/60)*Math.sin(cogRad)/Math.cos(lat*Math.PI/180);

  const endLat = lat+deltaLat;
  const endLon = lon+deltaLon;

  const poly = L.polyline([[lat,lon],[endLat,endLon]], {color:'blue',dashArray:'5,5'}).addTo(map);
  if(!historyMarkers[mmsi]) historyMarkers[mmsi]=[];
  historyMarkers[mmsi].push(poly);
}

document.addEventListener('DOMContentLoaded', initMap);