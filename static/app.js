let map;
let shipMarkers = {};
let historyMarkers = {};
let collisionsData = [];
let selectedShips = [];
let vectorLength = 15; // in minutes
let cpaFilter = 0.5;
let tcpaFilter = 10;

let markerCluster; 
let vectorLines = {}; // przechowujemy wektory dla zaznaczonych statków

function initMap() {
  map = L.map('map').setView([50.0, 0.0], 6);

  let osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18
  }).addTo(map);

  let seamarkLayer = L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png', {
    attribution: 'Map data: &copy; OpenStreetMap contributors & OpenSeaMap',
    transparent: true
  }).addTo(map);

  // Marker cluster z bardzo małym promieniem
  markerCluster = L.markerClusterGroup({
    maxClusterRadius: 10
  });
  map.addLayer(markerCluster);

  fetchAndUpdateData();
  setInterval(fetchAndUpdateData, 60000);

  document.getElementById('vectorLengthSlider').addEventListener('input', e => {
    vectorLength = parseInt(e.target.value);
    document.getElementById('vectorLengthValue').textContent = vectorLength;
    updateSelectedShipsInfo();
  });
  document.getElementById('cpaFilter').addEventListener('input', e=>{
    cpaFilter = parseFloat(e.target.value);
    document.getElementById('cpaValue').textContent = cpaFilter.toFixed(2);
    updateCollisionsList();
  });
  document.getElementById('tcpaFilter').addEventListener('input', e=>{
    tcpaFilter = parseFloat(e.target.value);
    document.getElementById('tcpaValue').textContent = tcpaFilter.toFixed(1);
    updateCollisionsList();
  });

  document.getElementById('clearSelectedShips').addEventListener('click', ()=>{
    clearSelectedShips();
  });
}

function fetchAndUpdateData() {
  fetch(`/ships`)
    .then(res=>res.json())
    .then(data=>{
      updateShips(data);
    })
    .catch(err=>console.error("Error ships