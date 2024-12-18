let map;
let shipMarkers = {};
let collisionsData = [];
let selectedShips = [];
let vectorLength = 15; 
let cpaFilter = 0.5;
let tcpaFilter = 10;
let markerClusterGroup; 
let collisionMarkers = [];
let lastSelectedPair = null;
let lastCpaTcpa = null; 
let overlayMarkers = {};

function initMap() {
  map = L.map('map');

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
    maxClusterRadius:1,
    removeOutsideVisibleBounds: true
  });
  map.addLayer(markerClusterGroup);

  // Ustaw widok na Kanał Angielski
  map.fitBounds([[49.0, -2.0], [51.0, 2.0]]);

  fetchShips();
  fetchCollisions();

  setInterval(fetchShips, 60000);
  setInterval(fetchCollisions, 30000);

  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value);
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(false); 
  });
  document.getElementById('cpaFilter').addEventListener('input', e=>{
    cpaFilter = parseFloat(e.target.value);
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisions();
  });
  document.getElementById('tcpaFilter').addEventListener('input', e=>{
    tcpaFilter = parseFloat(e.target.value);
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    fetchCollisions();
  });

  document.getElementById('clearSelectedShips').addEventListener('click', ()=>{
    clearSelectedShips();
  });

  // Obsługa modala "Info"
  const infoButton = document.getElementById('infoButton');
  const infoModal = document.getElementById('infoModal');
  const modalClose = document.getElementById('modalClose');

  infoButton.addEventListener('click', ()=>{
    infoModal.style.display = 'block';
  });

  modalClose.addEventListener('click', ()=>{
    infoModal.style.display = 'none';
  });

  window.addEventListener('click', (e)=>{
    if(e.target == infoModal) {
      infoModal.style.display = 'none';
    }
  });
}

function fetchShips() {
  fetch(`/ships`)
    .then(res=>res.json())
    .then(data=>{
      updateShips(data);
    })
    .catch(err=>console.error("Error ships:", err));
}

function fetchCollisions() {
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
      if (overlayMarkers[m]) {
        overlayMarkers[m].forEach(h=>map.removeLayer(h));
        delete overlayMarkers[m];
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
      else if (length<250) {fillColor='orange'; scale=1.0;}
      else {fillColor='red'; scale=1.0;}
    } else {
      fillColor='none';
      scale=1.0;
    }

    const width = 12*scale;
    const height = 18*scale;
    const rotation = ship.cog||0;

    let highlightRect = '';
    if(selectedShips.includes(ship.mmsi)) {
      highlightRect = `<rect x="-8" y="-8" width="16" height="16" fill="none" stroke="black" stroke-width="3" stroke-dasharray="5,5" />`;
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

  updateSelectedShipsInfo(true);
}

function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  collisionMarkers.forEach(m=>map.removeLayer(m));
  collisionMarkers=[];

  let filtered = collisionsData.filter(c=>c.tcpa>0);
  filtered.sort((a,b)=>a.tcpa - b.tcpa);

  if(filtered.length===0) {
    const noColl = document.createElement('div');
    noColl.classList.add('collision-item');
    noColl.innerHTML = `<div style="padding:10px; font-style:italic;">Fortunately, I have detected no collisions at the moment. Enjoy the calm seas!</div>`;
    collisionList.appendChild(noColl);
    return;
  }

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
  const bounds = L.latLngBounds([[c.latitude_a,c.longitude_a],[c.latitude_b,c.longitude_b]]);
  map.fitBounds(bounds, {padding:[50,50]});

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

  for (let m in overlayMarkers) {
    overlayMarkers[m].forEach(h=>map.removeLayer(h));
  }
  overlayMarkers={};

  updateSelectedShipsInfo(true);
}

function updateSelectedShipsInfo(selectionChanged) {
  const container = document.getElementById('selected-ships-info');
  container.innerHTML = '';
  document.getElementById('pair-info').innerHTML='';

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
    currentPair = [...selectedShips].sort((a,b)=>a-b);
  } else {
    currentPair = null;
  }

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
        if(!isFinite(cpa) || !isFinite(tcpa) || cpa>20 || tcpa>180 || cpa<0 || tcpa<0) {
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
    if(!lastCpaTcpa.cpa || !lastCpaTcpa.tcpa) {
      document.getElementById('pair-info').innerHTML='<b>CPA/TCPA:</b> N/A';
    } else {
      document.getElementById('pair-info').innerHTML=`<b>CPA/TCPA:</b> ${lastCpaTcpa.cpa.toFixed(2)} nm / ${lastCpaTcpa.tcpa.toFixed(2)} min`;
    }
  }

  for (let m in overlayMarkers) {
    overlayMarkers[m].forEach(h=>map.removeLayer(h));
  }
  overlayMarkers={};

  selectedShips.forEach(mmsi=>{
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
      if (length < 50) {fillColor='green'; scale=0.9;}
      else if (length<150) {fillColor='yellow'; scale=1.0;}
      else if (length<250) {fillColor='orange'; scale=1.1;}
      else {fillColor='red'; scale=1.2;}
    } else {
      fillColor='none';scale=1.0;
    }

    // Nowy, większy viewBox
    // Mamy kwadrat 24x24 jednostki (od -12 do +12 w osi x i y).
    // Statek jest mały (polygon o szerokości 10 i wysokości 15 z poprzednich koordynatów),
    // więc będzie mały w tym dużym boxie, ale to pozwoli na duży kwadrat wokół.
    const rotation = ship.cog || 0;
    const width = 24 * scale;
    const height = 24 * scale;

    // Polygon statku (ten sam co wcześniej):
    // Oryginalne punkty: 0,-7.5  5,7.5  -5,7.5
    // Pozostawiamy je bez zmian - będą małe w środku viewBox, ale to w niczym nie przeszkadza.
    const shape = `<polygon points="0,-7.5 5,7.5 -5,7.5" fill="${fillColor}" stroke="${strokeColor}" stroke-width="1"/>`;

    let highlightRect = '';
    if (selectedShips.includes(ship.mmsi)) {
      // Duży kwadrat obejmujący znacznie większy obszar niż statek,
      // dzięki czemu jest wyraźny, czarny, przerywany i skalowalny.
      // Kwadrat 20x20 w środku viewBox 24x24, co daje spory margines:
      highlightRect = `<rect x="-10" y="-10" width="20" height="20" fill="none" stroke="black" stroke-width="3" stroke-dasharray="5,5" />`;
    }

    const icon = L.divIcon({
      className: '',
      html: `
        <svg width="${width}" height="${height}" viewBox="-12 -12 24 24" style="transform:rotate(${rotation}deg);">
          ${highlightRect}
          ${shape}
        </svg>
      `,
      iconSize: [width, height],
      iconAnchor: [width/2, height/2]
    });

    shipMarkers[m].setIcon(icon);
  }
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
  if(!overlayMarkers[mmsi]) overlayMarkers[mmsi]=[];
  overlayMarkers[mmsi].push(poly);
}

document.addEventListener('DOMContentLoaded', initMap);