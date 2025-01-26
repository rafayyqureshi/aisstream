//
// common.js
//
// Wspólne funkcje i logika wizualizacji,
// używane przez "live" (app.js) i w przyszłości przez "history".
//

/**
 * Inicjalizacja mapy Leaflet z warstwami bazowymi OSM i OpenSeaMap.
 */
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

  // Warstwa nawigacyjna (OpenSeaMap) - opcjonalna
  const openSeaMapLayer = L.tileLayer(
    'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
    { maxZoom: 18, opacity: 0.8 }
  );
  openSeaMapLayer.addTo(map);

  return map;
}

/**
 * Konwersja metry -> stopnie (dla szerokości geogr).
 */
function metersToDegLat(meters) {
  // ~111.32 km / stopień
  return meters / 111320;
}

/**
 * Konwersja metry -> stopnie (dla długości geogr), uwzględnia cos(lat)
 */
function metersToDegLon(meters, latDeg) {
  const cosLat = Math.cos(latDeg * Math.PI / 180);
  if (cosLat < 1e-5) {
    return 0;
  }
  return meters / (111320 * cosLat);
}

/**
 * computeShipPolygonLatLon(lat0, lon0, cogDeg, a, b, c, d):
 *  Otrzymuje parametry statku (A, B, C, D).
 *  Zwraca tablicę [ [lat, lon], [lat, lon], ... ] – wierzchołki polygonu.
 *  Kod z Twojego przykładu (można rozbudować).
 */
function computeShipPolygonLatLon(lat0, lon0, cogDeg, a, b, c, d) {
  // Prosty wzorzec local coords (metry)
  // x wzdłuż kadłuba, y poprzeczny
  // ...
  // W razie potrzeby implementacja
  return [];
}

/**
 * getShipColorFromDims(dim_a, dim_b):
 *  Zwraca kolor zależny od sumy A+B.
 */
function getShipColorFromDims(dim_a, dim_b) {
  if (!dim_a || !dim_b) {
    return 'gray';
  }
  const lengthVal = dim_a + dim_b;
  if (lengthVal < 50)   return 'green';
  if (lengthVal < 150)  return 'yellow';
  if (lengthVal < 250)  return 'orange';
  return 'red';
}

/**
 * getShipColorByLength(lengthValue)
 *  Kolor zależny od numeric lengthValue.
 */
function getShipColorByLength(lengthValue) {
  if (lengthValue === null || isNaN(lengthValue)) return 'gray';
  if (lengthValue < 50)   return 'green';
  if (lengthValue < 150)  return 'yellow';
  if (lengthValue < 250)  return 'orange';
  return 'red';
}

/**
 * createSplittedCircle(colorA, colorB):
 *  Generuje okrąg podzielony pionowo 2 kolorami.
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
 * getCollisionSplitCircle(...)
 *  Do wizualizacji kolizji (2 kolory).
 */
function getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, shipMarkers) {
  let lenA = parseFloat(fallbackLenA) || 0;
  let lenB = parseFloat(fallbackLenB) || 0;

  // Jeśli w shipMarkers mamy dim_a, dim_b => sumujemy
  function getLenFromMarker(mmsi) {
    if (!shipMarkers || !shipMarkers[mmsi]?.shipData) return null;
    const sd = shipMarkers[mmsi].shipData;
    const dA = parseFloat(sd.dim_a) || 0;
    const dB = parseFloat(sd.dim_b) || 0;
    if (dA>0 && dB>0) return dA + dB;
    return null;
  }

  const maybeA = getLenFromMarker(mmsiA);
  const maybeB = getLenFromMarker(mmsiB);
  if (maybeA !== null) lenA = maybeA;
  if (maybeB !== null) lenB = maybeB;

  const colorA = getShipColorByLength(lenA);
  const colorB = getShipColorByLength(lenB);

  return createSplittedCircle(colorA, colorB);
}

/**
 * createShipIcon(shipData, isSelected, mapZoom=5):
 *  Główna funkcja zwracająca L.divIcon.
 *  - dla zoom<12 => rysuje trójkąt
 *  - dla zoom>=12 => rysuje prostszy poligon
 *  - kolory wg (dim_a + dim_b)
 *  - highlightRect, jeśli isSelected
 */
function createShipIcon(shipData, isSelected, mapZoom=5) {
  const dimA = parseFloat(shipData.dim_a) || 0;
  const dimB = parseFloat(shipData.dim_b) || 0;
  const rotationDeg = shipData.cog || 0;

  let lengthVal = dimA + dimB;
  let color = getShipColorByLength(lengthVal);

  let highlightRect = '';
  if (isSelected) {
    highlightRect = `
      <rect
        x="-16" y="-16" width="32" height="32"
        fill="none" stroke="black" stroke-width="3"
        stroke-dasharray="5,3"
      />
    `;
  }

  let shapeContent = '';
  if (mapZoom < 12) {
    // prosta ikona – trójkąt
    shapeContent = `
      <polygon points="0,-8 6,8 -6,8"
        fill="${color}" stroke="black" stroke-width="1" />
    `;
  } else {
    // poligon + wierzchołek dziobu
    shapeContent = `
      <polygon points="-8,-8  -8,8  12,8  15,0  12,-8"
        fill="${color}" stroke="black" stroke-width="1" />
    `;
  }

  const svgW = 48, svgH = 48;
  const svgHTML = `
    <svg width="${svgW}" height="${svgH}" viewBox="-24 -24 48 48">
      <g transform="rotate(${rotationDeg})">
        ${highlightRect}
        ${shapeContent}
      </g>
    </svg>
  `;

  return L.divIcon({
    className: '',
    html: svgHTML,
    iconSize: [svgW, svgH],
    iconAnchor: [0, 0]
  });
}

// Eksport w global scope:
window.initSharedMap = initSharedMap;
window.computeShipPolygonLatLon = computeShipPolygonLatLon;
window.getShipColorFromDims = getShipColorFromDims;
window.getShipColorByLength = getShipColorByLength;
window.getCollisionSplitCircle = getCollisionSplitCircle;
window.createShipIcon = createShipIcon;