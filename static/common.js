//
// common.js
//
// Zawiera wspólne funkcje i logikę wizualizacji, 
// używane przez "live" (app.js) i w przyszłości przez "history".
//

/**
 * Inicjalizacja mapy Leaflet z warstwami bazowymi OSM i OpenSeaMap.
 * @param {string} mapContainerId - ID elementu DOM, w którym osadzamy mapę
 * @returns {L.Map} - obiekt mapy
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
 * getShipColor(lengthValue):
 *  Zwraca kolor zależny od długości (green, yellow, orange, red, grey).
 */
function getShipColor(lengthValue) {
  if (lengthValue === null || isNaN(lengthValue)) return 'gray';
  if (lengthValue < 50)   return 'green';
  if (lengthValue < 150)  return 'yellow';
  if (lengthValue < 250)  return 'orange';
  return 'red';
}

/**
 * getShipScaleByColor(color):
 *  Zwraca skalę rysowania ikony statku zależną od koloru
 */
function getShipScaleByColor(color) {
  switch (color) {
    case 'green':  return 0.8;
    case 'yellow': return 1.0;
    case 'orange': return 1.2;
    case 'red':    return 1.4;
    default:       return 1.0; // 'gray'
  }
}

/**
 * createShipIcon(shipData, isSelected):
 *  Tworzy ikonę statku (L.DivIcon) z kolorem zależnym od shipData.ship_length.
 *  Jeśli isSelected=true, dodaje highlight rectangle wokół statku.
 */
function createShipIcon(shipData, isSelected) {
  const lengthVal = parseFloat(shipData.ship_length) || null;  
  const color = getShipColor(lengthVal);
  const scaleFactor = getShipScaleByColor(color);

  const rotationDeg = shipData.cog || 0;
  const basePoints = "0,-8 6,8 -6,8";  // trójkąt

  let highlightRect = '';
  if (isSelected) {
    // bounding box o wielkości ~16x16 (skalowane scaleFactor)
    const halfW = 8 * scaleFactor;
    const rectX = -halfW - 2;
    const rectSize = (halfW * 2) + 4;
    highlightRect = `
      <rect
        x="${rectX}" y="${rectX}"
        width="${rectSize}" height="${rectSize}"
        fill="none" stroke="black" stroke-width="3"
        stroke-dasharray="8,4"
      />
    `;
  }

  const svgW = 32, svgH = 32;
  const halfSvg = 16;
  const svgHTML = `
    <svg width="${svgW}" height="${svgH}" viewBox="0 0 32 32">
      <g transform="translate(16,16) scale(${scaleFactor}) rotate(${rotationDeg})">
        ${highlightRect}
        <polygon
          points="${basePoints}"
          fill="${color}"
          stroke="black"
          stroke-width="1"
        />
      </g>
    </svg>
  `;
  return L.divIcon({
    className: '',
    html: svgHTML,
    iconSize: [svgW, svgH],
    iconAnchor: [halfSvg, halfSvg]
  });
}

/**
 * createSplittedCircle(colorA, colorB):
 *  Generuje kod SVG okręgu podzielonego pionowo na dwie połówki w barwach colorA i colorB.
 *  Używamy do listy kolizji. 
 */
function createSplittedCircle(colorA, colorB) {
  return `
    <svg width="16" height="16" viewBox="0 0 16 16" 
         style="vertical-align:middle; margin-right:6px;">
      <!-- lewa połówka -->
      <path d="M8,8 m0-8 a8,8 0 0,1 0,16 z" fill="${colorA}"/>
      <!-- prawa połówka -->
      <path d="M8,8 m0,8 a8,8 0 0,1 0,-16 z" fill="${colorB}"/>
      </svg>
  `;
}

/**
 * getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, shipMarkers):
 *  Funkcja tworząca splitted circle dla pary statków:
 *   - Najpierw próbuje pobrać faktyczny "ship_length" z shipMarkers[mmsi] (jeśli istnieje).
 *   - W razie braku / undefined fallback do fallbackLenA/B (z collisions).
 *   - Zwraca gotowy HTML splitted circle.
 */
function getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, shipMarkers) {
  let lenA = parseFloat(fallbackLenA) || 0;
  let lenB = parseFloat(fallbackLenB) || 0;

  // Jeśli dany statek istnieje w shipMarkers => bierzemy stamtąd
  if (shipMarkers && shipMarkers[mmsiA] && shipMarkers[mmsiA].shipData) {
    const lA = parseFloat(shipMarkers[mmsiA].shipData.ship_length);
    if (!isNaN(lA)) lenA = lA;
  }
  if (shipMarkers && shipMarkers[mmsiB] && shipMarkers[mmsiB].shipData) {
    const lB = parseFloat(shipMarkers[mmsiB].shipData.ship_length);
    if (!isNaN(lB)) lenB = lB;
  }

  // Kolory
  const colorA = getShipColor(lenA);
  const colorB = getShipColor(lenB);

  // Gotowy splitted circle
  return createSplittedCircle(colorA, colorB);
}

// Eksportujemy w global scope
window.initSharedMap = initSharedMap;
window.createShipIcon = createShipIcon;
window.getCollisionSplitCircle = getCollisionSplitCircle;