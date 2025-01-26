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

  // Po powiększeniu >12 => rysujemy kadłuby (możesz to obsłużyć w app.js)
  map.on('zoomend', () => {
    // ewentualnie emit event do app.js, by przełączyć z ikony na rysowanie polygon
  });

  return map;
}

/**
 * Kolor zależny od (A+B) => ~długości
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
 * Prosty fallback: getShipColorByLength(lengthValue)
 *  (używane, jeśli mamy sam length)
 */
function getShipColorByLength(lengthValue) {
  if (lengthValue === null || isNaN(lengthValue)) return 'gray';
  if (lengthValue < 50)   return 'green';
  if (lengthValue < 150)  return 'yellow';
  if (lengthValue < 250)  return 'orange';
  return 'red';
}

/**
 * createShipContour(dim_a, dim_b, dim_c, dim_d):
 *  Tworzymy w local coords poligon, np. prosty kształt:
 *   - rufa => -b, 0
 *   - dziób => +a, 0
 *   - lewy bok => 0, -c
 *   - prawy bok => 0, +d
 *  Dodatkowo malujemy mały trójkąt z przodu by zasygnalizować dziób
 *  (opcjonalnie).
 *  Zwraca tablicę punktów w (x,y).
 *  
 * Uwaga: to tylko definicja w local coords, 
 *        w createShipIcon musimy transformować i rysować w <svg>.
 */
function createShipContour(a, b, c, d) {
  // jeżeli cokolwiek null => zwracamy null
  if (!a || !b || !c || !d) return null;

  // definicja kadłuba w local coords (x = wzdłuż statku, y = na boki)
  // antena = (0,0)
  // rufa   = (-b, 0)
  // dziób  = (+a, 0)
  // lewa burta = y = -c
  // prawa burta= y = +d
  // -> prostokąt + mały trójkąt dziobu
  const halfBeamLeft  = -c;
  const halfBeamRight = +d;

  // Bierzemy minimalny poligon:
  // rufa-left  =>  (-b, halfBeamLeft)
  // rufa-right =>  (-b, halfBeamRight)
  // dziób-right=>  ( a, halfBeamRight)
  // dziób-left =>  ( a, halfBeamLeft)
  // -> ewentualnie mały wierzchołek dziobu (a+5, 0)...

  let shape = [
    [-b, halfBeamLeft],
    [-b, halfBeamRight],
    [ a, halfBeamRight],
    [ a, halfBeamLeft],
  ];
  // ewentualnie dodajmy (a+5, 0) by zrobić “trójkąt” dziobu
  shape.push([a+5, 0]);

  return shape;
}

/**
 * createShipIcon(shipData, isSelected):
 *  - Jeżeli zoom <12 => rysujemy prostą ikonkę (trójkąt).
 *  - Jeżeli zoom >=12 => rysujemy kadłub poligon -> shape from A,B,C,D
 *    (na razie uproszczenie, bo nie znamy mapZoom, w app.js 
 *     możemy wstrzyknąć zoom jako parametr).
 * 
 *  Dla przykładu tutaj załóżmy, że zawsze rysujemy PROSTĄ IKONĘ 
 *  jeśli (a,b,c,d) jest null lub nie mamy nic.
 *  W innym wypadku rysujemy polygon w svg.
 */
function createShipIcon(shipData, isSelected, mapZoom=5) {
  // Pobierz dims:
  const dimA = parseFloat(shipData.dim_a) || null;
  const dimB = parseFloat(shipData.dim_b) || null;
  const dimC = parseFloat(shipData.dim_c) || null;
  const dimD = parseFloat(shipData.dim_d) || null;

  // Jeżeli zoom < 12 => rysujemy dotychczasowy trójkąt:
  const useContour = (mapZoom >= 12 && dimA && dimB && dimC && dimD);

  // Rotation:
  const rotationDeg = shipData.cog || 0;

  // Kolor:
  let color = 'gray';
  if (useContour) {
    color = getShipColorFromDims(dimA, dimB);
  } else {
    // fallback na length = a+b
    let fallbackLen = null;
    if (dimA && dimB) fallbackLen = dimA + dimB;
    color = getShipColorByLength(fallbackLen);
  }

  let highlightRect = '';
  if (isSelected) {
    highlightRect = `
      <rect
        x="-16" y="-16"
        width="32" height="32"
        fill="none" stroke="black" stroke-width="3"
        stroke-dasharray="6,3"
      />
    `;
  }

  // Generujemy <path> lub <polygon> do shape
  let shapeContent = '';
  if (useContour) {
    // Tworzymy poligon wg createShipContour
    let shapePoints = createShipContour(dimA, dimB, dimC, dimD);
    if (shapePoints) {
      // budujemy "points" w formacie "x1,y1 x2,y2..."
      const pts = shapePoints.map(pt => `${pt[0]},${pt[1]}`).join(' ');
      shapeContent = `<polygon points="${pts}" fill="${color}" stroke="black" stroke-width="1" />`;
    } else {
      // fallback
      shapeContent = `<polygon points="0,-8 6,8 -6,8" fill="${color}" stroke="black" stroke-width="1" />`;
    }
  } else {
    // prosta ikona trójkąt
    shapeContent = `<polygon points="0,-8 6,8 -6,8" fill="${color}" stroke="black" stroke-width="1" />`;
  }

  // Całe SVG
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
    iconAnchor: [0, 0]  // ewentualnie [24,24] w środku
  });
}

/**
 * createSplittedCircle(colorA, colorB):
 *  Generuje kod SVG okręgu podzielonego pionowo na dwie połówki w barwach colorA i colorB.
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
 *  Tworzy splitted circle dla pary statków, biorąc kolor z getShipColorByLength.
 */
function getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, shipMarkers) {
  let lenA = parseFloat(fallbackLenA) || 0;
  let lenB = parseFloat(fallbackLenB) || 0;

  // Jeśli dany statek istnieje w shipMarkers => bierzemy a+b
  // bo tam mamy dim_a, dim_b => sum
  function getLenFromMarker(mmsi) {
    if (shipMarkers && shipMarkers[mmsi]?.shipData) {
      let sd = shipMarkers[mmsi].shipData;
      let dA = parseFloat(sd.dim_a) || 0;
      let dB = parseFloat(sd.dim_b) || 0;
      if (dA>0 && dB>0) return (dA + dB);
    }
    return null;
  }
  const L_A = getLenFromMarker(mmsiA);
  const L_B = getLenFromMarker(mmsiB);
  if (L_A !== null) lenA = L_A;
  if (L_B !== null) lenB = L_B;

  // Kolory
  const colorA = getShipColorByLength(lenA);
  const colorB = getShipColorByLength(lenB);

  return createSplittedCircle(colorA, colorB);
}

// Eksport do global scope
window.initSharedMap = initSharedMap;
window.createShipIcon = createShipIcon;
window.getCollisionSplitCircle = getCollisionSplitCircle;