//
// app.js (LIVE) – uses common.js for map init & visuals
//

let map;
let markerClusterGroup;

let shipMarkers    = {};  // mmsi -> L.marker
let overlayMarkers = {};  // mmsi -> [L.polyline]
let collisionsData = [];
let collisionMarkers = [];

let selectedShips = [];

let vectorLength = 15; // default
let cpaFilter    = 0.5;
let tcpaFilter   = 10;

// intervals
let shipsInterval      = null;
let collisionsInterval = null;

/**
 * Initialize the live logic.
 */
function initLiveApp() {
  // create map from common
  const { map: sharedMap, markerClusterGroup: clusterGroup } = initSharedMap('map');
  map                 = sharedMap;
  markerClusterGroup  = clusterGroup;

  // UI events
  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(true);
  });
  document.getElementById('cpaFilter').addEventListener('input', e => {
    cpaFilter = parseFloat(e.target.value) || 0.5;
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    fetchCollisions();
  });
  document.getElementById('tcpaFilter').addEventListener('input', e => {
    tcpaFilter = parseFloat(e.target.value) || 10;
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    fetchCollisions();
  });
  document.getElementById('clearSelectedShips').addEventListener('click', () => {
    clearSelectedShips();
  });

  // initial fetch
  fetchShips();
  fetchCollisions();

  // periodic
  shipsInterval      = setInterval(fetchShips,      60000);
  collisionsInterval = setInterval(fetchCollisions, 60000);
}

// -----------------------------
// SHIPS
// -----------------------------
function fetchShips() {
  fetch('/ships')
    .then(res => res.json())
    .then(data => updateShips(data))
    .catch(err => console.error('Error /ships:', err));
}

function updateShips(shipsArray) {
  const nowMs = Date.now();
  const currentMmsi = new Set(shipsArray.map(s => s.mmsi));

  // remove stale
  for (let mmsi in shipMarkers) {
    if (!currentMmsi.has(parseInt(mmsi))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
      if (overlayMarkers[mmsi]) {
        overlayMarkers[mmsi].forEach(line => map.removeLayer(line));
        delete overlayMarkers[mmsi];
      }
    }
  }

  // add/update
  shipsArray.forEach(ship => {
    const mmsi = ship.mmsi;
    let marker = shipMarkers[mmsi];

    // tooltip
    const updatedMs = new Date(ship.timestamp).getTime();
    const diffSec   = Math.floor((nowMs - updatedMs)/1000);
    const mm        = Math.floor(diffSec/60);
    const ss        = diffSec % 60;
    const diffStr   = `${mm}m ${ss}s ago`;

    const tooltipHtml = `
      <b>${ship.ship_name || 'Unknown'}</b><br>
      MMSI: ${mmsi}<br>
      SOG: ${ship.sog||0} kn, COG: ${ship.cog||0}°<br>
      Length: ${ship.ship_length||'N/A'}<br>
      Updated: ${diffStr}
    `;

    const isSelected = selectedShips.includes(mmsi);
    const icon       = createShipIcon(ship, isSelected);

    if (!marker) {
      marker = L.marker([ship.latitude, ship.longitude], { icon })
        .on('click', () => selectShip(mmsi));
      marker.bindTooltip(tooltipHtml, { direction:'top', sticky:true });
      shipMarkers[mmsi] = marker;
      markerClusterGroup.addLayer(marker);
    } else {
      // update
      marker.setLatLng([ship.latitude, ship.longitude]);
      marker.setIcon(icon);
      marker.setTooltipContent(tooltipHtml);
    }
    marker.shipData = ship;
  });

  updateSelectedShipsInfo(false);
}

// -----------------------------
// COLLISIONS
// -----------------------------
function fetchCollisions() {
  fetch(`/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`)
    .then(r => r.json())
    .then(data => {
      collisionsData = data;
      updateCollisionsList();
    })
    .catch(err => console.error("Error /collisions:", err));
}

function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if(!collisionsData || collisionsData.length===0) {
    const div = document.createElement('div');
    div.classList.add('collision-item');
    div.innerHTML='<i>No collisions found</i>';
    collisionList.appendChild(div);
    return;
  }

  // filter out collisions in the past
  const nowMs = Date.now();
  let upcoming = collisionsData.filter(c => {
    if(!c.timestamp) return false;
    let collMs = new Date(c.timestamp).getTime();
    return (collMs >= nowMs);
  });

  if(upcoming.length===0) {
    const div = document.createElement('div');
    div.classList.add('collision-item');
    div.innerHTML='<i>No current collisions</i>';
    collisionList.appendChild(div);
    return;
  }

  // deduplicate
  let colMap = {};
  upcoming.forEach(c => {
    let a = Math.min(c.mmsi_a, c.mmsi_b);
    let b = Math.max(c.mmsi_a, c.mmsi_b);
    let key = `${a}_${b}`;

    if(!colMap[key]) {
      colMap[key] = c;
    } else {
      // pick the one w/ smaller CPA or more recent timestamp
      if(c.cpa < colMap[key].cpa) {
        colMap[key] = c;
      } else if(c.cpa === colMap[key].cpa) {
        let oldT = new Date(colMap[key].timestamp).getTime();
        let newT = new Date(c.timestamp).getTime();
        if(newT>oldT) {
          colMap[key] = c;
        }
      }
    }
  });

  let final = Object.values(colMap);
  if(final.length===0) {
    const div = document.createElement('div');
    div.classList.add('collision-item');
    div.innerHTML='<i>No current collisions</i>';
    collisionList.appendChild(div);
    return;
  }

  final.forEach(c => {
    let shipA = c.ship1_name || c.mmsi_a;
    let shipB = c.ship2_name || c.mmsi_b;
    let cpa   = c.cpa.toFixed(2);
    let tcpa  = c.tcpa.toFixed(2);

    // splitted circle
    let colorA = getShipColor(c.ship_length_a||0);
    let colorB = getShipColor(c.ship_length_b||0);
    let splitted = createSplittedCircle(colorA, colorB);

    let timeStr='';
    if(c.timestamp) {
      let dt=new Date(c.timestamp);
      timeStr=dt.toLocaleTimeString('en-GB');
    }

    const item=document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML=`
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splitted}
          <strong>${shipA} - ${shipB}</strong><br>
          CPA: ${cpa} nm, TCPA: ${tcpa} min @ ${timeStr}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    item.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(c);
    });
    collisionList.appendChild(item);

    // place marker
    let latC=(c.latitude_a + c.latitude_b)/2;
    let lonC=(c.longitude_a + c.longitude_b)/2;
    let colIcon = createCollisionIcon();
    let tip = `Collision between ${shipA} & ${shipB} in ${tcpa} min`;

    let marker = L.marker([latC, lonC], { icon: colIcon })
      .bindTooltip(tip, {direction:'top', sticky:true})
      .on('click', ()=> zoomToCollision(c));
    marker.addTo(map);
    collisionMarkers.push(marker);
  });
}

function zoomToCollision(c) {
  let bounds=L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds,{padding:[30,30]});
  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

// -----------------------------
// SELECTED SHIPS
// -----------------------------
function selectShip(mmsi) {
  if(!selectedShips.includes(mmsi)) {
    if(selectedShips.length>=2) {
      selectedShips.shift();
    }
    selectedShips.push(mmsi);
    updateSelectedShipsInfo(true);
  }
}

function clearSelectedShips() {
  selectedShips=[];
  for(let m in overlayMarkers) {
    overlayMarkers[m].forEach(l => map.removeLayer(l));
  }
  overlayMarkers={};
  updateSelectedShipsInfo(false);
}

// re-draw selected ships / vectors
function updateSelectedShipsInfo(selectionChanged) {
  const container=document.getElementById('selected-ships-info');
  container.innerHTML='';
  document.getElementById('pair-info').innerHTML='';

  if(selectedShips.length===0) {
    reloadAllShipIcons();
    return;
  }

  // gather data
  let shipsData=[];
  selectedShips.forEach(mmsi => {
    if(shipMarkers[mmsi]?.shipData) {
      shipsData.push(shipMarkers[mmsi].shipData);
    }
  });

  // display
  shipsData.forEach(sd => {
    let div=document.createElement('div');
    div.innerHTML=`
      <b>${sd.ship_name||'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${sd.sog||0} kn, COG: ${sd.cog||0}°<br>
      Length: ${sd.ship_length||'N/A'}
    `;
    container.appendChild(div);
  });

  // remove old polylines
  for(let m in overlayMarkers){
    overlayMarkers[m].forEach(line => map.removeLayer(line));
  }
  overlayMarkers={};

  // draw new lines
  selectedShips.forEach(mmsi => {
    let marker=shipMarkers[mmsi];
    if(!marker) return;
    let line=drawVectorLine(map, marker.shipData, vectorLength);
    if(line){
      if(!overlayMarkers[mmsi]) overlayMarkers[mmsi]=[];
      overlayMarkers[mmsi].push(line);
    }
  });

  reloadAllShipIcons();

  // if 2 ships, fetch cpa/tcpa
  if(selectedShips.length===2) {
    let sorted=[...selectedShips].sort((a,b)=>a-b);
    fetch(`/calculate_cpa_tcpa?mmsi_a=${sorted[0]}&mmsi_b=${sorted[1]}`)
      .then(r=>r.json())
      .then(data=>{
        if(data.error) {
          document.getElementById('pair-info').innerHTML=
            `<b>CPA/TCPA:</b> n/a ( ${data.error} )`;
        } else {
          let cpaVal  = (data.cpa>=9999)?'n/a': data.cpa.toFixed(2);
          let tcpaVal = (data.tcpa<0)?'n/a': data.tcpa.toFixed(2);
          document.getElementById('pair-info').innerHTML=`
            <b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min
          `;
        }
      })
      .catch(err=>{
        console.error("Error /calculate_cpa_tcpa:", err);
        document.getElementById('pair-info').innerHTML='<b>CPA/TCPA:</b> n/a';
      });
  }
}

function reloadAllShipIcons() {
  for(let m in shipMarkers){
    let marker=shipMarkers[m];
    let sd=marker.shipData;
    let selected=selectedShips.includes(parseInt(m));
    let icon=createShipIcon(sd, selected);
    marker.setIcon(icon);
  }
}

// DOMContentLoaded
document.addEventListener('DOMContentLoaded', initLiveApp);