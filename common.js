//
// common.js
// Shared map initialization & visual helpers for both "live" and "history" modules.
//

/**
 * Create the shared Leaflet map with base layers, seamarks, etc.
 * @param {string} elementId - The DOM element ID (default: 'map').
 * @returns {Object} { map, markerClusterGroup }
 */
function initSharedMap(elementId = 'map') {
    // 1) Create the main Leaflet map
    const map = L.map(elementId, {
      center: [50, 0],
      zoom: 5
    });
  
    // 2) Base OSM layer
    const osmLayer = L.tileLayer(
      'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
      { maxZoom: 18 }
    );
    osmLayer.addTo(map);
  
    // 3) Nautical seamark layer
    const openSeaMapLayer = L.tileLayer(
      'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
      { maxZoom: 18, opacity: 0.8 }
    );
    openSeaMapLayer.addTo(map);
  
    // 4) MarkerCluster
    const markerClusterGroup = L.markerClusterGroup({ maxClusterRadius: 1 });
    map.addLayer(markerClusterGroup);
  
    return { map, markerClusterGroup };
  }
  
  /**
   * Returns a color string for a given ship length.
   * @param {number|null} len - The ship length.
   * @returns {string} - CSS color or hex code.
   */
  function getShipColor(len) {
    if (len === null) return 'gray';
    if (len < 50)   return 'green';
    if (len < 150)  return 'yellow';
    if (len < 250)  return 'orange';
    return 'red';
  }
  
  /**
   * Creates a splitted circle icon (inline SVG).
   * @param {string} colorA - left half color
   * @param {string} colorB - right half color
   * @returns {string} inline SVG snippet
   */
  function createSplittedCircle(colorA, colorB) {
    return `
      <svg width="16" height="16" viewBox="0 0 16 16"
           style="vertical-align:middle; margin-right:6px;">
        <!-- left half -->
        <path d="M8,8 m-8,0 a8,8 0 0,1 16,0 z" fill="${colorA}"/>
        <!-- right half -->
        <path d="M8,8 m8,0 a8,8 0 0,1 -16,0 z" fill="${colorB}"/>
      </svg>
    `;
  }
  
  /**
   * Creates a collision exclamation icon as a Leaflet DivIcon.
   * @returns {L.DivIcon}
   */
  function createCollisionIcon() {
    return L.divIcon({
      className: '',
      html: `
        <svg width="24" height="24" viewBox="-12 -12 24 24">
          <path d="M0,-7 7,7 -7,7 Z"
                fill="yellow" stroke="red" stroke-width="2"/>
          <text x="0" y="4" text-anchor="middle"
                font-size="8" fill="red">!</text>
        </svg>
      `,
      iconSize: [24, 24],
      iconAnchor: [12, 12]
    });
  }
  
  /**
   * Creates a ship icon with rotation, color, highlight, etc.
   * @param {Object} shipData - AIS data
   * @param {boolean} isSelected - highlight?
   * @returns {L.DivIcon}
   */
  function createShipIcon(shipData, isSelected = false) {
    const len      = shipData.ship_length || null;
    const fillColor= getShipColor(len);
    const rotation = shipData.cog || 0;
  
    const width  = 16;
    const height = 24;
  
    let highlightRect = '';
    if (isSelected) {
      highlightRect = `
        <rect x="-10" y="-10" width="20" height="20"
              fill="none" stroke="black" stroke-width="3"
              stroke-dasharray="8,4"/>
      `;
    }
  
    const shipSvg = `
      <polygon points="0,-8 6,8 -6,8"
               fill="${fillColor}" stroke="black" stroke-width="1"/>
    `;
  
    const iconHtml = `
      <svg width="${width}" height="${height}" viewBox="-8 -8 16 16"
           style="transform:rotate(${rotation}deg)">
        ${highlightRect}
        ${shipSvg}
      </svg>
    `;
  
    return L.divIcon({
      className: '',
      html: iconHtml,
      iconSize: [width, height],
      iconAnchor: [width/2, height/2]
    });
  }
  
  /**
   * Draws a heading vector line from the ship's position for the given minutes.
   * @param {L.Map} map
   * @param {Object} shipData - AIS data (lat, lon, sog, cog)
   * @param {number} vectorLengthMin
   * @returns {L.Polyline|null}
   */
  function drawVectorLine(map, shipData, vectorLengthMin) {
    const lat = shipData.latitude;
    const lon = shipData.longitude;
    const sog = shipData.sog;
    const cog = shipData.cog;
    if (!sog || !cog) return null;
  
    const distanceNm = sog * (vectorLengthMin / 60.0);
    const cogRad     = (cog * Math.PI)/180.0;
  
    // approximate
    const deltaLat = (distanceNm/60) * Math.cos(cogRad);
    const deltaLon = (distanceNm/60) * Math.sin(cogRad) / Math.cos(lat*Math.PI/180);
  
    const endLat = lat + deltaLat;
    const endLon = lon + deltaLon;
  
    const line = L.polyline([[lat, lon], [endLat, endLon]], {
      color: 'blue',
      dashArray: '4,4'
    });
    line.addTo(map);
    return line;
  }
  
  // Expose them as global for usage in app.js
  window.initSharedMap       = initSharedMap;
  window.getShipColor        = getShipColor;
  window.createSplittedCircle= createSplittedCircle;
  window.createCollisionIcon = createCollisionIcon;
  window.createShipIcon      = createShipIcon;
  window.drawVectorLine      = drawVectorLine;