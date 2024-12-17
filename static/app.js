document.addEventListener('DOMContentLoaded', initMap);

let map, shipLayer, collisionLayer;
let selectedShips = []; // max 2 ships
let shipMarkers = {};
let collisionMarkers = {};
let historyMarkers = {}; // dla historii pozycji

function initMap() {
  map = L.map('map').setView([50.4, 0.0], 7); // Kanał Angielski

  // Warstwa bazowa (OpenStreetMap)
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
  }).addTo(map);

  // Warstwa OpenSeaMap
  L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png', {
    maxZoom: 18,
    opacity: 0.8
  }).addTo(map);

  // Ustawiamy klastrowanie z restrykcjami:
  shipLayer = L.markerClusterGroup({
    maxClusterRadius: 20,
    minimumClusterSize: 5
  });
  map.addLayer(shipLayer);

  collisionLayer = L.layerGroup().addTo(map);

  fetchData();
  setInterval(fetchData, 5000);

  setupUIEvents();
}

function setupUIEvents() {
  document.getElementById('clearSelectedShips').addEventListener('click', clearSelectedShips);

  document.getElementById('cpaFilter').addEventListener('input', (e)=>{
    document.getElementById('cpaValue').textContent = parseFloat(e.target.value).toFixed(2);
    fetchCollisions();
  });
  document.getElementById('tcpaFilter').addEventListener('input', (e)=>{
    document.getElementById('tcpaValue').textContent = parseFloat(e.target.value).toFixed(1);
    fetchCollisions();
  });

  document.getElementById('vectorLengthSlider').addEventListener('input', (e) => {
    document.getElementById('vectorLengthValue').textContent = e.target.value;
    updateSelectedShipsDisplay();
  });
}

function fetchData() {
  fetch('/ships')
    .then(r => r.json())
    .then(data => updateShips(data))
    .catch(err=>console.error('Error ships:',err));

  fetchCollisions();
}

function fetchCollisions() {
  let cpaVal = parseFloat(document.getElementById('cpaFilter').value);
  let tcpaVal = parseFloat(document.getElementById('tcpaFilter').value);

  fetch(`/collisions?max_cpa=${cpaVal}&max_tcpa=${tcpaVal}`)
    .then(r => r.json())
    .then(data => updateCollisions(data))
    .catch(err => {
      console.error('Error collisions:', err);
      document.getElementById('collision-list').textContent = "No collisions data.";
    });
}

function updateShips(data) {
  data.forEach(ship => {
    const { mmsi, latitude, longitude, cog, sog, ship_name, ship_length, timestamp } = ship;
    if (!mmsi || !latitude || !longitude) return;

    let marker = shipMarkers[mmsi];
    if (!marker) {
      marker = createShipMarker(ship);
      shipLayer.addLayer(marker);
      shipMarkers[mmsi] = marker;
    } else {
      marker.setLatLng([latitude, longitude]);
      marker.options.shipData = ship;
      if (selectedShips.includes(mmsi)) {
        addShipHistoryPosition(mmsi, ship);
      }
    }
  });
}

function createShipMarker(ship) {
  const {mmsi, cog, sog, ship_name, ship_length, timestamp} = ship;
  let scale = getShipScale(ship_length);

  let icon = L.divIcon({
    html: `<div class="ship-icon" style="transform: rotate(${cog||0}deg);">
             <svg width="${20*scale}" height="${20*scale}" viewBox="0 0 20 20">
               <polygon points="10,0 20,20 10,15 0,20" fill="${getShipFill(ship_length)}" stroke="black" stroke-width="1"/>
             </svg>
           </div>`,
    className: '',
    iconSize: [20*scale, 20*scale],
    iconAnchor: [10*scale,10*scale],
  });

  let marker = L.marker([ship.latitude, ship.longitude], {icon, shipData: ship})
    .on('click', () => toggleShipSelection(mmsi));

  marker.bindTooltip(() => {
    let age = timeSinceUpdate(timestamp);
    return `${ship_name || 'Unknown'}<br>MMSI: ${mmsi}<br>COG: ${cog?.toFixed(1)}°<br>SOG: ${sog?.toFixed(1)} kn<br>Length: ${ship.ship_length||'N/A'}m<br>Last update: ${age}`;
  }, {permanent:false});

  return marker;
}

function getShipScale(length) {
  if (!length || length<50) return 1.0;
  if (length<150) return 1.0;
  if (length<250) return 1.1;
  return 1.2;
}

function getShipFill(length) {
  if (!length || length < 50) {
    return 'none';
  } else if (length < 150) {
    return 'green';
  } else if (length < 250) {
    return 'orange'; 
  } else {
    return 'red';
  }
}

function timeSinceUpdate(timestamp) {
  let now = Date.now();
  let diff = now - (new Date(timestamp)).getTime();
  let sec = diff/1000;
  if (sec<60) return sec.toFixed(0)+"s ago";
  let min = sec/60;
  return min.toFixed(1)+"m ago";
}

function toggleShipSelection(mmsi) {
  if (selectedShips.includes(mmsi)) {
    selectedShips = selectedShips.filter(x => x!==mmsi);
  } else {
    if (selectedShips.length>=2) selectedShips=[];
    selectedShips.push(mmsi);
  }
  updateSelectedShipsDisplay();
}

function clearSelectedShips() {
  selectedShips = [];
  updateSelectedShipsDisplay();
}

function updateSelectedShipsDisplay() {
  removeAllSelectionGraphics();

  // czyści historię
  for (let m in historyMarkers) {
    historyMarkers[m].forEach(hm=>map.removeLayer(hm));
  }
  historyMarkers={};

  if (selectedShips.length>0) {
    selectedShips.forEach(mmsi => {
      let marker = shipMarkers[mmsi];
      if (marker) {
        let ship = marker.options.shipData;
        addShipHistoryPosition(mmsi, ship);
        addSelectedSquare(marker.getLatLng());
        addShipVector(mmsi);
      }
    });
    if (selectedShips.length==2) {
      updatePairInfo();
    } else {
      document.getElementById('pair-info').textContent = "";
    }
  } else {
    document.getElementById('pair-info').textContent = "";
  }

  let info = selectedShips.map(m=>shipMarkers[m].options.shipData.ship_name||m).join(', ');
  document.getElementById('selected-ships-info').textContent = info ? `Selected: ${info}` : '';
}

function addSelectedSquare(latlng) {
  let icon = L.divIcon({className:'selected-square-icon', html:'<div class="selected-square"></div>', iconSize:[30,30],iconAnchor:[15,15]});
  let sq = L.marker(latlng,{icon}).addTo(map);
  sq._temp = true;
}

function addShipVector(mmsi) {
  let ship = shipMarkers[mmsi].options.shipData;
  let vectorLengthMin = parseInt(document.getElementById('vectorLengthSlider').value,10);
  let sog = ship.sog||0;
  let distance = sog*(vectorLengthMin/60); 
  let deltaLat = (distance/60)*Math.cos((ship.cog||0)*Math.PI/180);
  let deltaLng = (distance/60)*Math.sin((ship.cog||0)*Math.PI/180);

  let start = shipMarkers[mmsi].getLatLng();
  let end = L.latLng(start.lat+deltaLat, start.lng+deltaLng);

  let poly = L.polyline([start,end], {color:'black',dashArray:'5,5',weight:2});
  poly.addTo(map);
  poly._temp=true; 
}

function addShipHistoryPosition(mmsi, ship) {
  if (!historyMarkers[mmsi]) historyMarkers[mmsi]=[];
  let positions = historyMarkers[mmsi];

  let maxHistory=10;
  if (positions.length>=maxHistory) {
    let oldest=positions.shift();
    if (oldest) map.removeLayer(oldest);
  }

  let index=positions.length; 
  let scale=0.5; 
  let opacity=0.8-(index*0.07); 
  if (opacity<0.1) opacity=0.1;

  let icon = L.divIcon({
    html:`<div class="ship-history-icon" style="opacity:${opacity};transform: rotate(${ship.cog||0}deg);">
             <svg width="${20*scale}" height="${20*scale}" viewBox="0 0 20 20">
               <polygon points="10,0 20,20 10,15 0,20" fill="${getShipFill(ship.ship_length)}" stroke="black" stroke-width="1"/>
             </svg>
           </div>`,
    iconSize:[20*scale,20*scale],iconAnchor:[10*scale,10*scale] 
  });

  let mk = L.marker([ship.latitude,ship.longitude],{icon}).addTo(map);
  mk._temp = true;
  positions.push(mk);
}

function removeAllSelectionGraphics() {
  map.eachLayer(layer=>{
    if (layer._temp) {
      map.removeLayer(layer);
    }
  });
}

function updatePairInfo() {
  if (selectedShips.length<2) return;
  let s1 = shipMarkers[selectedShips[0]].options.shipData;
  let s2 = shipMarkers[selectedShips[1]].options.shipData;

  // Tu zakładamy fikcyjne obliczenia CPA/TCPA lub mamy je z backendu.
  // Jeśli mamy je z backendu - musisz je pobrać tam.
  // Poniżej przykładowe wartości:
  let cpa = 0.1;
  let tcpa = 2.5;
  if (tcpa<=0) {
    document.getElementById('pair-info').textContent = "No future collision (TCPA<=0)";
    return;
  }

  document.getElementById('pair-info').textContent = `CPA: ${cpa.toFixed(2)} nm, TCPA: ${tcpa.toFixed(2)} min`;
}

function zoomToCollision(collision) {
  selectedShips = [];
  if (shipMarkers[collision.mmsi_a] && shipMarkers[collision.mmsi_b]) {
    selectedShips.push(collision.mmsi_a, collision.mmsi_b);
    let center = [(collision.latitude_a+collision.latitude_b)/2,(collision.longitude_a+collision.longitude_b)/2];
    map.setView(center,12);

    document.getElementById('vectorLengthSlider').value = Math.round(collision.tcpa);
    document.getElementById('vectorLengthValue').textContent = Math.round(collision.tcpa);

    updateSelectedShipsDisplay();
  }
}

function updateCollisions(data) {
  collisionLayer.clearLayers();
  const clist = document.getElementById('collision-list');
  clist.innerHTML='';

  if (!data || !Array.isArray(data) || data.length===0) {
    clist.textContent = "No collisions data.";
    return;
  }

  // sort po tcpa rosnąco
  data.sort((a,b)=>a.tcpa-b.tcpa);

  data.forEach(col=>{
    if (col.tcpa<=0) return;
    let item = document.createElement('div');
    let name_a = col.ship_a_name || col.mmsi_a;
    let name_b = col.ship_b_name || col.mmsi_b;

    let zoomBtn = document.createElement('button');
    zoomBtn.textContent = '🔍';
    zoomBtn.style.fontSize='12px';
    zoomBtn.addEventListener('click', ()=>zoomToCollision(col));

    item.innerHTML = `<strong>${name_a} - ${name_b}</strong><br>CPA: ${col.cpa.toFixed(2)} nm, TCPA: ${col.tcpa.toFixed(2)} min `;
    item.appendChild(zoomBtn);
    clist.appendChild(item);

    let colIcon = L.divIcon({
      html:`<div style="font-size:16px;">⚠️</div>`,
      className:'',
      iconSize:[20,20],
      iconAnchor:[10,10]
    });
    let mk = L.marker([(col.latitude_a+col.latitude_b)/2,(col.longitude_a+col.longitude_b)/2],{icon:colIcon})
      .on('click', ()=>zoomToCollision(col));
    collisionLayer.addLayer(mk);
  });
}