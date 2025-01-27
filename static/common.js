//
// common.js
//
// Wspólne funkcje i logika wizualizacji
// używane przez moduły "live" (app.js) i "history" (history_app.js).
// UWAGA: plik ten musi być załadowany PRZED app.js / history_app.js!
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
  const openSeaMapLayer = L.tileLayer(
    'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
    { maxZoom: 18, opacity: 0.7 }
  );
  openSeaMapLayer.addTo(map);

  return map;
}

// -----------------------------------------------------------
// 2) Pomocnicze: kolorowanie i skalowanie
// -----------------------------------------------------------
/**
 * getShipColorFromDims: logika kolorowania na podstawie (dim_a + dim_b).
 * Jeśli a,b nie dostępne => zwraca 'gray'.
 */
function getShipColorFromDims(dimA, dimB) {
  if (!dimA || !dimB) {
    return 'gray';
  }
  const lengthVal = dimA + dimB;
  if (lengthVal < 50)   return 'green';
  if (lengthVal < 150)  return 'yellow';
  if (lengthVal < 250)  return 'orange';
  return 'red';
}

/**
 * getShipColor: fallback dla pojedynczego lengthValue (np. stare dane).
 */
function getShipColor(len) {
  if (len === null || isNaN(len)) return 'gray';
  if (len < 50)   return 'green';
  if (len < 150)  return 'yellow';
  if (len < 250)  return 'orange';
  return 'red';
}

/**
 * getShipScale – skala ikony zależna od koloru.
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
// 3) Rysowanie kadłuba w local coords
// -----------------------------------------------------------
/**
 * buildHullPoints(a,b,c,d):
 *  Zwraca string "x1,y1 x2,y2 ..." do <polygon points="..."> w local coords (px).
 *   - rufa-left  = (-b, -c)
 *   - rufa-right = (-b, +d)
 *   - dziób-left = (a - (c+d)/2, -c)
 *   - wierzchołek dziobu = (a, 0)
 *   - dziób-right= (a - (c+d)/2, +d)
 * Jeśli a,b,c,d są niepełne -> return null
 */
function buildHullPoints(a, b, c, d) {
  if (!a || !b || !c || !d) {
    return null;
  }
  // Local coords, (0,0) = antena
  const pts = [
    [-b, -c],                               // rufa-left
    [-b,  d],                               // rufa-right
    [ a - (c + d)/2,  d],                   // dziób-right
    [ a,  0],                               // wierzchołek dziobu
    [ a - (c + d)/2, -c]                    // dziób-left
  ];

  return pts.map(pt => `${pt[0]},${pt[1]}`).join(' ');
}

// -----------------------------------------------------------
// 4) Tworzenie ikony statku / kadłuba
// -----------------------------------------------------------
/**
 * createShipIcon(shipData, isSelected, mapZoom=5):
 *   - jeśli zoom <14 => rysujemy uproszczony trójkąt
 *   - jeśli zoom >=14 => rysujemy polygon kadłuba
 * Uwaga: do rotacji używamy heading (TrueHeading),
 *        a w razie braku => fallback do cog.
 */
function createShipIcon(shipData, isSelected, mapZoom = 5) {
  // Wymiary (dim_a..d)
  const dimA = parseFloat(shipData.dim_a) || 0;
  const dimB = parseFloat(shipData.dim_b) || 0;
  const dimC = parseFloat(shipData.dim_c) || 0;
  const dimD = parseFloat(shipData.dim_d) || 0;

  // Kolor
  const color = getShipColorFromDims(dimA, dimB);
  const scaleVal = getShipScale(color);

  // highlight?
  let highlightRect = '';
  if (isSelected) {
    const w = 16 * scaleVal;
    highlightRect = `
      <rect x="${-w/2 - 2}" y="${-w/2 - 2}"
            width="${w + 4}" height="${w + 4}"
            fill="none" stroke="black"
            stroke-width="3" stroke-dasharray="8,4" />
    `;
  }

  // Rotation – prefer heading, fallback to cog
  const hdg = (shipData.heading != null) ? shipData.heading : (shipData.cog || 0);

  // Size
  const svgSize = 32;
  const half = svgSize / 2;

  // Tworzymy shape (polygon kadłuba) lub trójkąt
  let shapeHTML = '';
  if (mapZoom >= 14 && dimA>0 && dimB>0 && dimC>0 && dimD>0) {
    // Poligon kadłuba
    const hullStr = buildHullPoints(dimA, dimB, dimC, dimD);
    if (hullStr) {
      shapeHTML = `<polygon points="${hullStr}" fill="${color}" stroke="black" stroke-width="1" />`;
    } else {
      // fallback => trójkąt
      shapeHTML = `<polygon points="0,-8 6,8 -6,8" fill="${color}" stroke="black" stroke-width="1" />`;
    }
  } else {
    // Mniejszy zoom => trójkąt
    shapeHTML = `<polygon points="0,-8 6,8 -6,8" fill="${color}" stroke="black" stroke-width="1" />`;
  }

  const svgHTML = `
    <svg width="${svgSize}" height="${svgSize}" viewBox="0 0 32 32">
      <g transform="translate(16,16) scale(${scaleVal}) rotate(${hdg})">
        ${highlightRect}
        ${shapeHTML}
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
// 5) Splitted circle do kolizji
// -----------------------------------------------------------
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
 *  Tworzy splitted circle do listy kolizji
 */
function getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, shipMarkers) {
  let lenA = parseFloat(fallbackLenA) || 0;
  let lenB = parseFloat(fallbackLenB) || 0;

  function getLenFromMarker(mmsi) {
    if (!shipMarkers || !shipMarkers[mmsi]?.shipData) return null;
    const sd = shipMarkers[mmsi].shipData;
    const a = parseFloat(sd.dim_a) || 0;
    const b = parseFloat(sd.dim_b) || 0;
    if (a>0 && b>0) return (a+b);
    return null;
  }

  const L_A = getLenFromMarker(mmsiA);
  if (L_A !== null) lenA = L_A;
  const L_B = getLenFromMarker(mmsiB);
  if (L_B !== null) lenB = L_B;

  const colorA = getShipColor(lenA);
  const colorB = getShipColor(lenB);

  return createSplittedCircle(colorA, colorB);
}

// -----------------------------------------------------------
// 6) Eksport do global scope
// -----------------------------------------------------------
window.initSharedMap = initSharedMap;
window.createShipIcon = createShipIcon;
window.buildHullPoints = buildHullPoints;

window.getShipColor = getShipColor;
window.getShipColorFromDims = getShipColorFromDims;
window.getShipScale = getShipScale;

window.createSplittedCircle = createSplittedCircle;
window.getCollisionSplitCircle = getCollisionSplitCircle;