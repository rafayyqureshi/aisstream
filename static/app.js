// ==========================
// app.js (moduł LIVE) – przykładowa wersja
// ==========================

document.addEventListener('DOMContentLoaded', initLiveApp);

// ----- Globalne zmienne -----
let map;
let markerClusterGroup;
let shipMarkers = {};         // klucz: mmsi -> L.marker
let shipPolygonLayers = {};   // klucz: mmsi -> L.polygon
let overlayVectors = {};      // klucz: mmsi -> [lista L.Polyline wektorów]

let collisionMarkers = [];
let collisionsData = [];
let selectedShips = [];       // wybrane statki (max 2)

// Parametry
let shipsInterval = null;
let collisionsInterval = null;

let vectorLength = 15;   // minuty (dla rysowania wektora prędkości)
let cpaFilter = 0.5;     // [0..0.5] param w slider
let tcpaFilter = 10;     // [1..10] param w slider

// ---------------------------
// 1) Inicjalizacja
// ---------------------------
function initLiveApp() {
  // A) Tworzymy mapę (z common.js)
  map = initSharedMap('map');

  // B) Warstwa klastrująca do "małych" ikon
  markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
  map.addLayer(markerClusterGroup);

  // C) Zoom => po zmianie zoomu (np. crossing 14) w następnym fetchShips
  //    nastąpi przełączanie marker vs polygon.
  map.on('zoomend', () => {
    // Ewentualnie można wymusić natychmiastowe odświeżenie, np.:
  fetchShips();
  });

  // D) UI: suwak wektora
  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value) || 15;
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo(true); // odśwież wektory
  });

  // E) Filtry kolizji
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

  // F) Clear selected
  document.getElementById('clearSelectedShips').addEventListener('click', clearSelectedShips);

  // G) Pierwszy fetch
  fetchShips();
  fetchCollisions();

  // H) Odśwież co 30s
  shipsInterval = setInterval(fetchShips, 30000);
  collisionsInterval = setInterval(fetchCollisions, 30000);
}

// ---------------------------
// 2) Pobieranie / wyświetlanie statków
// ---------------------------
function fetchShips() {
  fetch('/ships')
    .then(res => {
      if (!res.ok) {
        throw new Error(`HTTP ${res.status} - ${res.statusText}`);
      }
      return res.json();
    })
    .then(data => updateShips(data))
    .catch(err => console.error("Błąd /ships:", err));
}

function updateShips(shipsArray) {
  // A) Zbiór bieżących MMSI
  const currentSet = new Set(shipsArray.map(s => s.mmsi));

  // B) Usuwamy statki, których nie ma w fetchu
  //    1) Markery
  for (let mmsi in shipMarkers) {
    if (!currentSet.has(parseInt(mmsi))) {
      markerClusterGroup.removeLayer(shipMarkers[mmsi]);
      delete shipMarkers[mmsi];
    }
  }
  //    2) Polygony
  for (let mmsi in shipPolygonLayers) {
    if (!currentSet.has(parseInt(mmsi))) {
      map.removeLayer(shipPolygonLayers[mmsi]);
      delete shipPolygonLayers[mmsi];
    }
  }
  //    3) Wektory (overlayVectors)
  for (let mmsi in overlayVectors) {
    if (!currentSet.has(parseInt(mmsi))) {
      // usuń stare linie z mapy
      overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
      delete overlayVectors[mmsi];
    }
  }

  // C) Dodajemy / aktualizujemy
  const z = map.getZoom();
  shipsArray.forEach(ship => {
    const mmsi = ship.mmsi;
    const isSelected = selectedShips.includes(mmsi);

    if (z < 14) {
      // -> marker
      // usuwamy polygon, jeśli istnieje
      if (shipPolygonLayers[mmsi]) {
        map.removeLayer(shipPolygonLayers[mmsi]);
        delete shipPolygonLayers[mmsi];
      }
      // Tworzymy / update marker
      let marker = shipMarkers[mmsi];
      if (!marker) {
        const icon = createShipIcon(ship, isSelected, z);
        marker = L.marker([ship.latitude, ship.longitude], { icon })
          .on('click', () => selectShip(mmsi));
        marker.shipData = ship;
        shipMarkers[mmsi] = marker;
        markerClusterGroup.addLayer(marker);
      } else {
        marker.setLatLng([ship.latitude, ship.longitude]);
        marker.setIcon(createShipIcon(ship, isSelected, z));
        marker.shipData = ship;
      }
    } else {
      // -> polygon
      if (shipMarkers[mmsi]) {
        markerClusterGroup.removeLayer(shipMarkers[mmsi]);
        delete shipMarkers[mmsi];
      }
      // usuwamy stary polygon
      if (shipPolygonLayers[mmsi]) {
        map.removeLayer(shipPolygonLayers[mmsi]);
        delete shipPolygonLayers[mmsi];
      }
      // tworzymy nowy
      let poly = createShipPolygon(ship);
      if (poly) {
        poly.on('click', () => selectShip(mmsi));
        poly.addTo(map);
        poly.shipData = ship;
        shipPolygonLayers[mmsi] = poly;
      }
    }
  });

  // D) Odśwież "selected"
  updateSelectedShipsInfo(false);
}

// ---------------------------
// 3) Obsługa kolizji
// ---------------------------
function fetchCollisions() {
  const url = `/collisions?max_cpa=${cpaFilter}&max_tcpa=${tcpaFilter}`;
  fetch(url)
    .then(res => {
      if (!res.ok) {
        throw new Error(`HTTP ${res.status} – ${res.statusText}`);
      }
      return res.json();
    })
    .then(data => {
      collisionsData = data || [];
      updateCollisionsList();
    })
    .catch(err => console.error("Błąd /collisions:", err));
}

function updateCollisionsList() {
  const collisionList = document.getElementById('collision-list');
  collisionList.innerHTML = '';

  // usuwamy stare
  collisionMarkers.forEach(m => map.removeLayer(m));
  collisionMarkers = [];

  if (!collisionsData || collisionsData.length===0) {
    let noItem = document.createElement('div');
    noItem.classList.add('collision-item');
    noItem.innerHTML = '<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(noItem);
    return;
  }

  // prosty mechanizm: weź najnowsze A-B
  let pairsMap = {};
  collisionsData.forEach(c => {
    let a = Math.min(c.mmsi_a, c.mmsi_b);
    let b = Math.max(c.mmsi_a, c.mmsi_b);
    let key = `${a}_${b}`;
    if (!pairsMap[key]) {
      pairsMap[key] = c;
    } else {
      let oldT = new Date(pairsMap[key].timestamp).getTime();
      let newT = new Date(c.timestamp).getTime();
      if (newT>oldT) {
        pairsMap[key] = c;
      }
    }
  });
  const finalColls = Object.values(pairsMap);
  if (finalColls.length===0) {
    let d = document.createElement('div');
    d.classList.add('collision-item');
    d.innerHTML='<i>Brak bieżących kolizji</i>';
    collisionList.appendChild(d);
    return;
  }

  finalColls.forEach(c => {
    // splitted circle => np. getCollisionSplitCircle
    const splittedHTML = getCollisionSplitCircle(c.mmsi_a, c.mmsi_b, 0, 0, shipMarkers);

    const timeStr = c.timestamp
      ? new Date(c.timestamp).toLocaleTimeString('pl-PL',{hour12:false})
      : '';
    let cpaStr = c.cpa.toFixed(2);
    let tcpaStr= c.tcpa.toFixed(2);

    const item = document.createElement('div');
    item.classList.add('collision-item');
    item.innerHTML=`
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          ${splittedHTML}
          <strong>Ships ${c.mmsi_a} – ${c.mmsi_b}</strong><br>
          CPA: ${cpaStr} nm, TCPA: ${tcpaStr} min ${timeStr?'@ '+timeStr:''}
        </div>
        <button class="zoom-button">🔍</button>
      </div>
    `;
    collisionList.appendChild(item);

    // Marker kolizyjny
    let latC = (c.latitude_a + c.latitude_b)/2;
    let lonC = (c.longitude_a + c.longitude_b)/2;
    const collisionIcon = L.divIcon({
      className:'',
      html:`
      <svg width="24" height="24" viewBox="-12 -12 24 24">
        <path d="M0,-7 7,7 -7,7 Z"
              fill="yellow" stroke="red" stroke-width="2"/>
        <text x="0" y="4" text-anchor="middle"
              font-size="8" fill="red">!</text>
      </svg>
      `,
      iconSize:[24,24],
      iconAnchor:[12,12]
    });
    const mark = L.marker([latC, lonC], { icon: collisionIcon })
      .bindTooltip(`Kolizja: ${c.mmsi_a} & ${c.mmsi_b}`, {direction:'top',sticky:true})
      .on('click', () => zoomToCollision(c));
    mark.addTo(map);
    collisionMarkers.push(mark);

    item.querySelector('.zoom-button').addEventListener('click', () => {
      zoomToCollision(c);
    });
  });
}

function zoomToCollision(c) {
  let bounds = L.latLngBounds([
    [c.latitude_a, c.longitude_a],
    [c.latitude_b, c.longitude_b]
  ]);
  map.fitBounds(bounds, { padding:[15,15], maxZoom:13 });

  clearSelectedShips();
  selectShip(c.mmsi_a);
  selectShip(c.mmsi_b);
}

// ---------------------------
// 4) Zaznaczanie statków
// ---------------------------
function selectShip(mmsi) {
  if (!selectedShips.includes(mmsi)) {
    if (selectedShips.length>=2) {
      selectedShips.shift();
    }
    selectedShips.push(mmsi);
    updateSelectedShipsInfo(true);
  }
}

function clearSelectedShips() {
  selectedShips=[];
  // usuwamy wektory
  for (let mmsi in overlayVectors) {
    overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
  }
  overlayVectors={};
  updateSelectedShipsInfo(false);
}

// ---------------------------
// 5) updateSelectedShipsInfo
// ---------------------------
function updateSelectedShipsInfo(selectionChanged) {
  const panel = document.getElementById('selected-ships-info');
  panel.innerHTML='';
  document.getElementById('pair-info').innerHTML='';

  // Gdy pusto
  if (selectedShips.length===0) {
    return;
  }

  // Tworzymy tablicę sData z shipData (z markerów/polygonów)
  let sData=[];
  selectedShips.forEach(mmsi => {
    if (shipMarkers[mmsi]?.shipData) {
      sData.push(shipMarkers[mmsi].shipData);
    } else if (shipPolygonLayers[mmsi]?.shipData) {
      sData.push(shipPolygonLayers[mmsi].shipData);
    }
  });

  // Rysowanie wektorów => najpierw czyścimy stare
  for (let mmsi in overlayVectors) {
    overlayVectors[mmsi].forEach(ln => map.removeLayer(ln));
  }
  overlayVectors={};

  sData.forEach(sd => {
    // Wyświetlanie w panelu
    let approxLen = null;
    if (sd.dim_a && sd.dim_b) {
      approxLen = (parseFloat(sd.dim_a)+parseFloat(sd.dim_b)).toFixed(1);
    }
    let hdgVal = sd.heading!==undefined && sd.heading!==null 
                 ? sd.heading 
                 : (sd.cog||0);

    const div = document.createElement('div');
    div.innerHTML=`
      <b>${sd.ship_name || 'Unknown'}</b><br>
      MMSI: ${sd.mmsi}<br>
      SOG: ${(sd.sog||0).toFixed(1)} kn, 
      COG: ${(sd.cog||0).toFixed(1)}°<br>
      HDG: ${hdgVal.toFixed(1)}°<br>
      Len: ${approxLen||'N/A'}
    `;
    panel.appendChild(div);

    // Rysujemy vector
    drawVector(sd.mmsi, sd);
  });

  // jeżeli 2 statki => odpalamy /calculate_cpa_tcpa
  if (selectedShips.length===2) {
    const [mA,mB] = selectedShips;
    let posA = sData.find(s => s.mmsi===mA);
    let posB = sData.find(s => s.mmsi===mB);

    let distNm=null;
    if (posA && posB) {
      distNm=computeDistanceNm(posA.latitude,posA.longitude,
                              posB.latitude,posB.longitude);
    }
    const sorted=[mA,mB].sort((a,b)=>a-b);
    const url=`/calculate_cpa_tcpa?mmsi_a=${sorted[0]}&mmsi_b=${sorted[1]}`;
    fetch(url)
      .then(r=>r.json())
      .then(data=>{
        if (data.error) {
          document.getElementById('pair-info').innerHTML=`
            ${distNm!==null?`<b>Distance:</b> ${distNm.toFixed(2)} nm<br>`:''}
            <b>CPA/TCPA:</b> N/A (${data.error})
          `;
        } else {
          let cpaVal=(data.cpa>=9999)? 'n/a': data.cpa.toFixed(2);
          let tcpaVal=(data.tcpa<0)? 'n/a': data.tcpa.toFixed(2);
          document.getElementById('pair-info').innerHTML=`
            ${distNm!==null?`<b>Distance:</b> ${distNm.toFixed(2)} nm<br>`:''}
            <b>CPA/TCPA:</b> ${cpaVal} nm / ${tcpaVal} min
          `;
        }
      })
      .catch(err=>{
        console.error("Błąd /calculate_cpa_tcpa:", err);
        document.getElementById('pair-info').innerHTML=`
          ${distNm!==null?`<b>Distance:</b> ${distNm.toFixed(2)} nm<br>`:''}
          <b>CPA/TCPA:</b> N/A
        `;
      });
  }
}

// ---------------------------
// 6) drawVector(mmsi, shipData)
// ---------------------------
function drawVector(mmsi, sd) {
  // Jeżeli brak sog/cog => rezygnujemy
  if (!sd.sog || !sd.cog) return;
  let lat = sd.latitude;
  let lon = sd.longitude;
  let sogKn = sd.sog; 
  let cogDeg= sd.cog;

  // Przykład: w vectorLength (minutach) statek przepłynie X mil
  let distNm = sogKn*(vectorLength/60); 
  let cogRad = (cogDeg*Math.PI)/180;

  // konwersja lat-lon 
  // 1 stopień lat ~ 60 nm
  // 1 stopień lon ~ 60 nm * cos(lat)
  let endLat = lat + (distNm/60)*Math.cos(cogRad);
  let lonScale = Math.cos(lat*Math.PI/180);
  if (lonScale<1e-6) lonScale=1e-6;
  let endLon = lon + ((distNm/60)*Math.sin(cogRad)/lonScale);

  // Rysuj polylinę
  let line = L.polyline([
      [lat, lon],
      [endLat, endLon]
    ], {
      color:'blue',
      dashArray:'4,4'
    });
  line.addTo(map);

  // zapisz w overlayVectors
  if (!overlayVectors[mmsi]) {
    overlayVectors[mmsi]=[];
  }
  overlayVectors[mmsi].push(line);
}

// ------------- computeDistanceNm -------------
function computeDistanceNm(lat1, lon1, lat2, lon2) {
  const R_nm = 3440.065;
  const rad = Math.PI/180;
  const dLat = (lat2-lat1)*rad;
  const dLon = (lon2-lon1)*rad;
  const a = Math.sin(dLat/2)**2
          + Math.cos(lat1*rad)*Math.cos(lat2*rad)*Math.sin(dLon/2)**2;
  const c = 2*Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  return R_nm*c;
}