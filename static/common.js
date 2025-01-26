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
 * Konwersja: metry -> stopnie szerokości geogr.
 */
function metersToDegLat(meters) {
  // Średnio ~111.32 km => 1°
  return meters / 111320;
}

/**
 * Konwersja: metry -> stopnie długości geogr. (uwzględnia cos(lat))
 */
function metersToDegLon(meters, latDeg) {
  const cosLat = Math.cos(latDeg * Math.PI / 180);
  if (cosLat < 1e-5) {
    return 0; // Zabezpieczenie na wysokich szerokościach
  }
  return meters / (111320 * cosLat);
}

/**
 * computeShipPolygonLatLon(lat0, lon0, cogDeg, a, b, c, d):
 *  Tworzy tablicę [ [lat, lon], ... ] w skali (1:1).
 * 
 *  Założenia:
 *   - antena = (0,0)
 *   - rufa-left  = (-b, -c)
 *   - rufa-right = (-b, +d)
 *   - dziób-left = (a - (c+d)/2, -c)
 *   - wierzchołek dziobu = (a, 0)
 *   - dziób-right= (a - (c+d)/2, +d)
 * 
 *  W efekcie rufa ma szerokość c+d (lewy bok c, prawy d),
 *  wierzchołek dziobu w (a,0),
 *  a punkty burt dziobu "zbiegają się" w okolicy (a,0) tak, by kadłub był
 *  bardziej symetryczny względem anteny.
 *
 *  Następnie obracamy o -COG i przeliczamy na lat-lon z (lat0,lon0).
 */
function computeShipPolygonLatLon(lat0, lon0, cogDeg, a, b, c, d) {
  // Definiujemy 5 wierzchołków w local coords (metry).
  // x - wzdłuż kadłuba (plus do przodu = dziób),
  // y - poprzeczny (plus w prawo).
  const localPoints = [
    { x: -b,              y: -c },   // rufa-left
    { x: -b,              y: +d },   // rufa-right
    { x: a - (c + d)/2,   y: +d },   // dziób-right
    { x: a,               y: 0 },    // wierzchołek dziobu
    { x: a - (c + d)/2,   y: -c }    // dziób-left
  ];

  const cogRad = (cogDeg * Math.PI) / 180;
  let polygonLatLon = [];

  for (let pt of localPoints) {
    // 1) Obrót local coords o -cogRad
    //    Zakładamy cog=0 => statek płynie "na północ"
    const sinθ = Math.sin(-cogRad);
    const cosθ = Math.cos(-cogRad);

    const xRot = pt.x * cosθ - pt.y * sinθ;  // w metrach N-S
    const yRot = pt.x * sinθ + pt.y * cosθ;  // w metrach E-W

    // 2) Konwersja w metrach -> Δlat, Δlon
    const dLat = metersToDegLat(xRot);
    const dLon = metersToDegLon(yRot, lat0);

    const lat = lat0 + dLat;
    const lon = lon0 + dLon;

    polygonLatLon.push([lat, lon]);
  }

  return polygonLatLon;
}

/**
 * getShipColorFromDims(dim_a, dim_b):
 *  Kolor w zależności od sumy (A+B).
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
 * Fallback: getShipColorByLength(lengthValue)
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
 *  Generuje okrąg pionowo podzielony.
 */
function createSplittedCircle(colorA, colorB) {
  return `
    <svg width="16" height="16" viewBox="0 0 16 16" 
         style="vertical-align:middle; margin-right:6px;">
      <path d="M8,0 A8,8 0 0,0 8,16 Z" fill="${colorA}"/>
      <path d="M8,16 A8,8 0 0,0 8,0 Z" fill="${colorB}"/>
    </svg>
  `;
}

/**
 * getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, shipMarkers):
 *  Tworzy splitted circle dla pary statków.
 */
function getCollisionSplitCircle(mmsiA, mmsiB, fallbackLenA, fallbackLenB, shipMarkers) {
  let lenA = parseFloat(fallbackLenA) || 0;
  let lenB = parseFloat(fallbackLenB) || 0;

  function getLenFromMarker(mmsi) {
    if (!shipMarkers || !shipMarkers[mmsi]?.shipData) return null;
    let sd = shipMarkers[mmsi].shipData;
    let dA = parseFloat(sd.dim_a) || 0;
    let dB = parseFloat(sd.dim_b) || 0;
    if (dA>0 && dB>0) return (dA + dB);
    return null;
  }
  let L_A = getLenFromMarker(mmsiA);
  let L_B = getLenFromMarker(mmsiB);
  if (L_A!==null) lenA = L_A;
  if (L_B!==null) lenB = L_B;

  const colorA = getShipColorByLength(lenA);
  const colorB = getShipColorByLength(lenB);

  return createSplittedCircle(colorA, colorB);
}

// Eksport do global scope
window.initSharedMap = initSharedMap;
window.computeShipPolygonLatLon = computeShipPolygonLatLon;
window.getShipColorFromDims = getShipColorFromDims;
window.getShipColorByLength = getShipColorByLength;
window.getCollisionSplitCircle = getCollisionSplitCircle;