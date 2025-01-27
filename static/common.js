//
// common.js
//
// Wspólne funkcje i logika wizualizacji,
// używane przez "live" (app.js) oraz "history" (history_app.js).
//
// Zawiera:
// 1) Funkcję initSharedMap()
// 2) Kolorowanie, skalowanie
// 3) Metry->stopnie konwersje
// 4) Funkcję computeHullPolygonLatLon => georeferencyjny kadłub
// 5) Funkcję createShipIcon => standardowa trójkątna ikona
// 6) Funkcję createShipPolygon => L.polygon(...) kadłuba
// 7) Funkcję createSplittedCircle => pół na pół koło
// 8) Funkcję getCollisionSplitCircle => do kolorowej ikonki kolizji
//

// -----------------------------------------------------------
// 1) Inicjalizacja mapy
// -----------------------------------------------------------
function initSharedMap(mapContainerId) {
  const map = L.map(mapContainerId, {
    center: [50, 0],
    zoom: 5
  });

  // Warstwa bazowa OSM
  const osmLayer = L.tileLayer(
    'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    { maxZoom: 18 }
  );
  osmLayer.addTo(map);

  // (Opcjonalnie) warstwa nawigacyjna OpenSeaMap
  const openSeaLayer = L.tileLayer(
    'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
    { maxZoom: 18, opacity: 0.7 }
  );
  openSeaLayer.addTo(map);

  return map;
}

// -----------------------------------------------------------
// 2) Kolorowanie, skalowanie
// -----------------------------------------------------------
/**
 * getShipColorFromDims(dimA, dimB):
 *   Kolor na podstawie sumy (dimA + dimB) => orientacyjna dł. statku.
 */
function getShipColorFromDims(dimA, dimB) {
  if (!dimA || !dimB) return 'gray';
  const lengthVal = dimA + dimB;
  if (lengthVal < 50)   return 'green';
  if (lengthVal < 150)  return 'yellow';
  if (lengthVal < 250)  return 'orange';
  return 'red';
}

/**
 * getShipColor(len):
 *   Stary fallback, jeśli mamy np. "ship_length" albo inne.
 */
function getShipColor(len) {
  if (len === null || isNaN(len)) return 'gray';
  if (len < 50)   return 'green';
  if (len < 150)  return 'yellow';
  if (len < 250)  return 'orange';
  return 'red';
}

/**
 * getShipScale(color):
 *   Skala rysowania trójkątnej ikony w <svg>.
 */
function getShipScale(color) {
  switch (color) {
    case 'green':  return 0.8;
    case 'yellow': return 1.0;
    case 'orange': return 1.2;
    case 'red':    return 1.4;
    default:       return 1.0;
  }
}

// -----------------------------------------------------------
// 3) Konwersje metry <-> stopnie geograficzne
// -----------------------------------------------------------
function metersToDegLat(meters) {
  // ~111320 m = 1 stopień szerokości geogr.
  return meters / 111320;
}
function metersToDegLon(meters, latDeg) {
  const rad = latDeg * Math.PI / 180;
  const cosLat = Math.cos(rad);
  if (cosLat < 1e-8) return 0;
  return meters / (111320 * cosLat);
}

// -----------------------------------------------------------
// 4) computeHullPolygonLatLon:
//    georeferencyjny kształt kadłuba
// -----------------------------------------------------------
/**
 * computeHullPolygonLatLon(lat0, lon0, headingDeg, a, b, c, d):
 *   Zwraca tablicę [ [lat1,lon1], [lat2,lon2], ... ]
 *   param:
 *    a,b,c,d => wymiary w metrach (anteny->dziob, anteny->rufa, anteny->lewa/prawa burta)
 *    headingDeg => 0 = North, rosnąco cw
 */
function computeHullPolygonLatLon(lat0, lon0, headingDeg, a, b, c, d) {
  if (!a || !b || !c || !d) {
    return [];
  }
  // local coords
  const localPts = [
    { x: -b, y: -c }, // rufa-left
    { x: -b, y:  d }, // rufa-right
    { x:  a - (c + d)/2, y: d },  // dziób-right
    { x:  a, y: 0 },             // wierzchołek dziobu
    { x:  a - (c + d)/2, y: -c } // dziób-left
  ];

  const hdgRad = (headingDeg||0) * Math.PI/180;
  const sinH = Math.sin(hdgRad);
  const cosH = Math.cos(hdgRad);

  const out = [];
  for (let i=0; i<localPts.length; i++) {
    let { x, y } = localPts[i];
    // rotacja
    const xR = x*cosH - y*sinH;
    const yR = x*sinH + y*cosH;

    // konwersja
    const dLat = metersToDegLat(xR);
    const dLon = metersToDegLon(yR, lat0);

    const lat = lat0 + dLat;
    const lon = lon0 + dLon;
    out.push([lat, lon]);
  }
  return out;
}

// -----------------------------------------------------------
// 5) createShipIcon: domyślna ikonka (trójkąt <svg>)
// -----------------------------------------------------------
/**
 * createShipIcon(shipData, isSelected, mapZoom=5):
 *   - jeśli zoom < 14 => narysuj trójkąt
 *   - powyżej => w "app.js" możemy stworzyć georeferencyjny poligon
 *     (tu wewnątrz raczej nie, bo to inna metoda).
 */
function createShipIcon(shipData, isSelected, mapZoom=5) {
  // pobierz wymiary i heading
  const dimA = parseFloat(shipData.dim_a)||0;
  const dimB = parseFloat(shipData.dim_b)||0;
  // do koloru
  const color = getShipColorFromDims(dimA, dimB);
  // skala
  const scaleVal = getShipScale(color);

  // highlight?
  let highlightRect = '';
  if (isSelected) {
    const w = 16*scaleVal;
    highlightRect = `
      <rect x="${-w/2 - 2}" y="${-w/2 - 2}"
            width="${w + 4}" height="${w + 4}"
            fill="none" stroke="black"
            stroke-width="3" stroke-dasharray="8,4" />
    `;
  }

  // prefer heading, fallback cog
  const hdg = (shipData.heading!=null)
               ? parseFloat(shipData.heading)
               : parseFloat(shipData.cog||0);

  // Rysujemy prosty trójkąt <polygon points="0,-8 6,8 -6,8">
  // W docelowej metodzie A poligon georeferencyjny będzie
  // rysowany w innym miejscu (app.js).
  const svgSize = 32;
  const half = svgSize/2;

  // Budujemy svg
  const triHTML = `
    <polygon points="0,-8 6,8 -6,8"
             fill="${color}" stroke="black" stroke-width="1" />
  `;

  const svgHTML = `
    <svg width="${svgSize}" height="${svgSize}" viewBox="0 0 32 32">
      <g transform="translate(16,16) scale(${scaleVal}) rotate(${hdg})">
        ${highlightRect}
        ${triHTML}
      </g>
    </svg>
  `;

  return L.divIcon({
    className: '',
    html: svgHTML,
    iconSize: [svgSize, svgSize],
    iconAnchor: [half, half]
  });
}

// -----------------------------------------------------------
// 6) createShipPolygon: tworzony georeferencyjnie
// -----------------------------------------------------------
/**
 * createShipPolygon(shipData):
 *   Zwraca L.polygon([...]) lub null, jeśli brak wymiarów.
 *   Używane np. w app.js, aby dodać do mapy przy zoom>=14.
 */
function createShipPolygon(shipData) {
  const a = parseFloat(shipData.dim_a)||0;
  const b = parseFloat(shipData.dim_b)||0;
  const c = parseFloat(shipData.dim_c)||0;
  const d = parseFloat(shipData.dim_d)||0;
  if (!a || !b || !c || !d) {
    return null;
  }

  const lat0 = parseFloat(shipData.latitude)||0;
  const lon0 = parseFloat(shipData.longitude)||0;
  const hdg  = (shipData.heading!=null)
                 ? parseFloat(shipData.heading)
                 : parseFloat(shipData.cog||0);

  // georeferencyjne rogi
  const hull = computeHullPolygonLatLon(lat0, lon0, hdg, a, b, c, d);
  if (hull.length < 3) {
    return null;
  }

  // kolor
  const col = getShipColorFromDims(a,b);
  const poly = L.polygon(hull, {
    color: col,
    weight: 2,
    opacity: 0.8,
    fillColor: col,
    fillOpacity: 0.4
  });
  return poly;
}

// -----------------------------------------------------------
// 7) createSplittedCircle
// -----------------------------------------------------------
/**
 * createSplittedCircle(colorA, colorB):
 *   Generuje mały <svg> 16x16, dwie połówki koła.
 */
function createSplittedCircle(colorA, colorB) {
  return `
    <svg width="16" height="16" viewBox="0 0 16 16"
         style="vertical-align:middle; margin-right:6px;">
      <!-- lewa połówka -->
      <path d="M8,0 A8,8 0 0,0 8,16 Z" fill="${colorA}"/>
      <!-- prawa połówka -->
      <path d="M8,16 A8,8 0 0,0 8,0 Z" fill="${colorB}"/>
    </svg>
  `;
}

/**
 * getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, shipMarkers):
 *   Dla pary statków (mmsiA, mmsiB) generujemy splitted circle:
 *     - lewa połówka = colorA
 *     - prawa połówka= colorB
 *   Gdzie color zależy od sumy (dimA + dimB) lub fallbackLen
 */
function getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, shipMarkers) {
  let lenA = parseFloat(fallbackLenA)||0;
  let lenB = parseFloat(fallbackLenB)||0;

  // Szukamy w shipMarkers
  function getLenFromMarker(mmsi) {
    if (!shipMarkers || !shipMarkers[mmsi]?.shipData) return null;
    const sd = shipMarkers[mmsi].shipData;
    const a = parseFloat(sd.dim_a)||0;
    const b = parseFloat(sd.dim_b)||0;
    if (a>0 && b>0) {
      return (a+b);
    }
    return null;
  }
  const L_A = getLenFromMarker(mmsiA);
  if (L_A!==null) lenA = L_A;
  const L_B = getLenFromMarker(mmsiB);
  if (L_B!==null) lenB = L_B;

  const colorA = getShipColor(lenA);
  const colorB = getShipColor(lenB);

  return createSplittedCircle(colorA, colorB);
}

// -----------------------------------------------------------
// 8) Eksport do global scope
// -----------------------------------------------------------
window.initSharedMap           = initSharedMap;
window.getShipColorFromDims    = getShipColorFromDims;
window.getShipColor            = getShipColor;
window.getShipScale            = getShipScale;

window.metersToDegLat          = metersToDegLat;
window.metersToDegLon          = metersToDegLon;
window.computeHullPolygonLatLon= computeHullPolygonLatLon;

window.createShipIcon          = createShipIcon;
window.createShipPolygon       = createShipPolygon;

window.createSplittedCircle    = createSplittedCircle;
window.getCollisionSplitCircle = getCollisionSplitCircle;