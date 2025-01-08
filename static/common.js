//
// common.js
//
// Ten plik zawiera wspólny kod związany z mapą (wizualne aspekty, style, ikony statków itd.)
// używany zarówno przez moduł “live”, jak i “history”.
//

/**
 * initSharedMap(mapContainerId)
 *    Funkcja pomocnicza do inicjalizacji mapy Leaflet
 *    z domyślnymi warstwami (OSM, OpenSeaMap).
 *
 * @param {string} mapContainerId - ID elementu DOM dla mapy
 * @returns {L.Map} - obiekt mapy Leaflet
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

  // Opcjonalna warstwa nawigacyjna (OpenSeaMap)
  const openSeaMapLayer = L.tileLayer(
    'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
    { maxZoom: 18, opacity: 0.8 }
  );
  openSeaMapLayer.addTo(map);

  return map;
}

/**
 * getShipColor(lengthValue)
 *    Zwraca kolor (string) zależny od długości statku.
 */
function getShipColor(lengthValue) {
  if (lengthValue === null) return 'gray';  // brak info
  if (lengthValue < 50)   return 'green';
  if (lengthValue < 150)  return 'yellow';
  if (lengthValue < 250)  return 'orange';
  return 'red';
}

/**
 * getShipScaleByColor(color)
 *    Zwraca współczynnik skalowania ikony zależnie od koloru.
 */
function getShipScaleByColor(color) {
  switch (color) {
    case 'green':  return 0.8;
    case 'yellow': return 1.0;
    case 'orange': return 1.2;
    case 'red':    return 1.4;
    case 'gray':
    default:       return 1.0;
  }
}

/**
 * createShipIcon(shipData, isSelected)
 *    Tworzy ikonę (L.DivIcon) statku z kolorem zależnym od długości
 *    i ewentualnym highlightem, jeśli isSelected=true.
 */
function createShipIcon(shipData, isSelected) {
  const color = getShipColor(shipData.ship_length || null);
  const scaleFactor = getShipScaleByColor(color);

  // Główny kształt (trójkąt “w górę”)
  const rotationDeg = shipData.cog || 0;
  const baseTrianglePoints = "0,-8 6,8 -6,8";

  // Dodatkowe highlight, jeśli zaznaczony
  let highlightRect = '';
  if (isSelected) {
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

  const svgWidth = 32, svgHeight = 32;
  const halfSvg = 16;

  const svgHTML = `
    <svg width="${svgWidth}" height="${svgHeight}" viewBox="0 0 32 32">
      <g transform="translate(16,16) scale(${scaleFactor}) rotate(${rotationDeg})">
        ${highlightRect}
        <polygon
          points="${baseTrianglePoints}"
          fill="${color}" stroke="black" stroke-width="1"
        />
      </g>
    </svg>
  `;

  return L.divIcon({
    className: '',
    html: svgHTML,
    iconSize: [svgWidth, svgHeight],
    iconAnchor: [halfSvg, halfSvg]
  });
}

/**
 * createSplittedCircle(colorA, colorB)
 *    Generuje kod SVG z kółkiem podzielonym na dwie połówki:
 *    lewą w kolorze colorA i prawą w kolorze colorB.
 *    Zwraca HTML (string), który można wstawić do item.innerHTML.
 *
 * @param {string} colorA - kolor lewej połówki
 * @param {string} colorB - kolor prawej połówki
 * @returns {string} - kod SVG
 */
function createSplittedCircle(colorA, colorB) {
  return `
    <svg width="16" height="16" viewBox="0 0 16 16"
         style="vertical-align:middle; margin-right:6px;">
      <!-- lewa połówka -->
      <path d="M8,8 m-8,0 a8,8 0 0,1 16,0 z" fill="${colorA}"/>
      <!-- prawa połówka -->
      <path d="M8,8 m8,0 a8,8 0 0,1 -16,0 z" fill="${colorB}"/>
    </svg>
  `;
}

// Eksportujemy do globalnego obiektu:
window.initSharedMap = initSharedMap;
window.createShipIcon = createShipIcon;
window.getShipColor = getShipColor;
window.createSplittedCircle = createSplittedCircle;