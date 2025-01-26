//
// common.js
//
// Zawiera funkcje współdzielone przez moduły "live" i "history".
// ŁADOWANY PRZED app.js / history_app.js!
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

  // Warstwa OpenSeaMap (opcjonalnie)
  const openSeaMapLayer = L.tileLayer(
    'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
    { maxZoom: 18, opacity: 0.7 }
  );
  openSeaMapLayer.addTo(map);

  return map;
}

// -----------------------------------------------------------
// 2) Pomocnicze: kolorowanie i skala
// -----------------------------------------------------------
/**
 * getShipColorFromDims: logika kolorowania na podstawie (dim_a + dim_b).
 * Jeżeli a,b nie są dostępne, zwraca 'gray'.
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
 * getShipColor: (z poprzedniej wersji), fallback – jeżeli w starym kodzie
 * mamy "ship_length" – można użyć tej funkcji. Ale teraz możesz używać
 * getShipColorFromDims zamiast.
 */
function getShipColor(len) {
  if (len === null || isNaN(len)) return 'gray';
  if (len < 50)   return 'green';
  if (len < 150)  return 'yellow';
  if (len < 250)  return 'orange';
  return 'red';
}

/**
 * getShipScale: zależne od koloru (z poprzedniej wersji).
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
// 3) Rysowanie poligonu kadłuba
// -----------------------------------------------------------
/**
 * Tworzymy w local coords 5 punktów:
 *  - rufa-left  = (-b, -c)
 *  - rufa-right = (-b, +d)
 *  - dziób-left = (a - (c+d)/2, -c)
 *  - wierzchołek dziobu = (a, 0)
 *  - dziób-right= (a - (c+d)/2, +d)
 * 
 * Zwraca string "x1,y1 x2,y2 ..." do <polygon points="...">
 */
function buildHullPoints(a, b, c, d) {
  if (!a || !b || !c || !d) {
    return null; // Brak wymiarów
  }
  // Konstruujemy w local coords
  // (0,0) to antena
  const pts = [
    [-b, -c],                                // rufa-left
    [-b, +d],                                // rufa-right
    [a - (c + d)/2, +d],                     // dziób-right
    [a, 0],                                  // wierzchołek dziobu
    [a - (c + d)/2, -c]                      // dziób-left
  ];

  // Transformujemy w prosty 2D *bez* rotacji – bo i tak polygon
  // rotujemy w <svg> -> rotate(cog).
  // Ale jeżeli chcesz inną geometrię, zmodyfikuj tu.

  // Budujemy string
  const strPoints = pts.map(pt => `${pt[0]},${pt[1]}`).join(' ');
  return strPoints;
}

// -----------------------------------------------------------
// 4) createShipIcon - wersja rozbudowana
// -----------------------------------------------------------
/**
 * createShipIcon(shipData, isSelected, mapZoom=5)
 *  - Jeżeli zoom <12 => rysuje stary trójkąt
 *  - Jeżeli zoom >=12 i dim_a,b,c,d => rysuje poligon kadłuba
 */
function createShipIcon(shipData, isSelected, mapZoom=5) {
  // Pobierz wymiary
  const dimA = parseFloat(shipData.dim_a) || 0;
  const dimB = parseFloat(shipData.dim_b) || 0;
  const dimC = parseFloat(shipData.dim_c) || 0;
  const dimD = parseFloat(shipData.dim_d) || 0;

  // Kolor z dims
  const color = getShipColorFromDims(dimA, dimB);

  // Skala (z poprzedniej logiki)
  const scale = getShipScale(color);

  // Ewentualny highlight
  let highlightRect = '';
  if (isSelected) {
    const w = 16 * scale;
    highlightRect = `
      <rect x="${-w/2 - 2}" y="${-w/2 - 2}"
            width="${w + 4}" height="${w + 4}"
            fill="none" stroke="black"
            stroke-width="3" stroke-dasharray="8,4" />
    `;
  }

  // Rotation
  const rotationDeg = shipData.cog || 0;
  const svgSize = 32;
  const half = svgSize/2;

  let shapeHTML = '';
  if (mapZoom >= 12 && dimA>0 && dimB>0 && dimC>0 && dimD>0) {
    // Rysujemy poligon
    const hull = buildHullPoints(dimA, dimB, dimC, dimD);
    if (hull) {
      shapeHTML = `
        <polygon points="${hull}"
                 fill="${color}" stroke="black" stroke-width="1" />
      `;
    } else {
      // w razie braku, fallback do trójkąta
      shapeHTML = `
        <polygon points="0,-8 6,8 -6,8"
                 fill="${color}" stroke="black" stroke-width="1" />
      `;
    }
  } else {
    // Zoom za mały => stary trójkąt
    shapeHTML = `
      <polygon points="0,-8 6,8 -6,8"
               fill="${color}" stroke="black" stroke-width="1" />
    `;
  }

  const svgHTML = `
    <svg width="${svgSize}" height="${svgSize}" viewBox="0 0 32 32">
      <g transform="translate(16,16) scale(${scale}) rotate(${rotationDeg})">
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
// 5) splitted circle - do kolizji
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
 * getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, shipMarkers)
 *  - Jeśli w shipMarkers[mmsi] mamy dim_a, dim_b -> liczymy length i kolor.
 *  - W razie braku => fallbackLen.
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

  const colorA = getShipColor(lenA);   // lub getShipColorFromDims(a,b)
  const colorB = getShipColor(lenB);

  return createSplittedCircle(colorA, colorB);
}

// -----------------------------------------------------------
// 6) Eksporty w global scope
// -----------------------------------------------------------
window.initSharedMap = initSharedMap;
window.createShipIcon = createShipIcon;          // rysuje trójkąt/poligon
window.getShipColor = getShipColor;              // fallback
window.getShipScale = getShipScale;
window.getShipColorFromDims = getShipColorFromDims; // nowa
window.createSplittedCircle = createSplittedCircle;
window.getCollisionSplitCircle = getCollisionSplitCircle;