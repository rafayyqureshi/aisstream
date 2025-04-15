#!/usr/bin/env python3
import os
import json
import asyncio
import threading
import ssl
import certifi
import math
import time
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, render_template, request
from dotenv import load_dotenv
import websockets
from collections import defaultdict

# Try to import numpy, but don't fail if not available
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    print("NumPy not available, using math module instead")

# ML components would typically be imported here
# For demo purposes, we'll simulate ML functionality
# import tensorflow as tf
# from sklearn.ensemble import RandomForestClassifier

##################################################
# Inicjalizacja Flask
##################################################
app = Flask(__name__, static_folder='static', template_folder='templates')

##################################################
# Wczytanie zmiennych
##################################################
load_dotenv()
API_KEY_REQUIRED = os.getenv("API_KEY", "Ais-mon")
AISSTREAM_TOKEN = os.getenv('AISSTREAM_TOKEN', '8f8affb0553d1cc4361693feb9f93a7a96dc0669')

##################################################
# Global data stores
##################################################
# Store for ships and collision data
SHIPS_DATA = []
COLLISION_DATA = []
# Lock for thread-safe access
data_lock = threading.Lock()
# Ship static data
SHIP_STATIC_DATA = {}
# Historical trajectory data for ML prediction
SHIP_HISTORY = defaultdict(list)  # mmsi -> list of positions
# ML prediction cache
TRAJECTORY_PREDICTIONS = {}  # mmsi -> list of predicted positions
RISK_ASSESSMENT = {}  # collision_id -> risk details

##################################################
# ML Model Configuration
##################################################
# In a real implementation, these would be loaded from saved models
PREDICTION_HORIZON = 30  # minutes
PREDICTION_STEPS = 10  # number of future positions to predict
UNCERTAINTY_LEVELS = [0.1, 0.3, 0.5]  # confidence intervals (68%, 95%, 99%)

##################################################
# AIS Stream Processing Functions
##################################################
def is_valid_coordinate(value):
    return isinstance(value, (int, float)) and -180 <= value <= 180

def process_ship_static_data(message: dict):
    metadata = message.get("MetaData", {})
    mmsi = metadata.get("MMSI")
    if not mmsi:
        return

    static_part = message.get("Message", {}).get("ShipStaticData", {})
    ship_name = static_part.get("ShipName") or static_part.get("Name") or "Unknown"
    dimension = static_part.get("Dimension", {})

    def to_float(val):
        return float(val) if isinstance(val, (int, float)) else None

    with data_lock:
        SHIP_STATIC_DATA[mmsi] = {
            "ship_name": ship_name,
            "dim_a": to_float(dimension.get("A")),
            "dim_b": to_float(dimension.get("B")),
            "dim_c": to_float(dimension.get("C")),
            "dim_d": to_float(dimension.get("D"))
        }

def process_position_report(message: dict):
    global SHIPS_DATA
    position_report = message.get("Message", {}).get("PositionReport", {})
    metadata = message.get("MetaData", {})
    mmsi = metadata.get("MMSI")
    if not position_report or not mmsi:
        return None

    latitude = position_report.get("Latitude")
    longitude = position_report.get("Longitude")
    cog = position_report.get("Cog")
    sog = position_report.get("Sog")
    heading = position_report.get("TrueHeading")
    if heading is not None:
        try:
            heading = float(heading)
        except Exception:
            heading = None

    # If heading is 511, replace with COG
    if heading == 511.0:
        heading = cog

    # Get timestamp from "TimeReceived" in MetaData
    time_received_str = metadata.get("TimeReceived")
    if not time_received_str:
        time_received_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    else:
        if time_received_str.endswith("+00:00"):
            time_received_str = time_received_str.replace("+00:00", "Z")
    
    # Filter: only process if SOG >= 2 and coordinates are valid
    if sog is None or sog < 2:
        return None
    if not is_valid_coordinate(latitude) or not is_valid_coordinate(longitude):
        return None

    # Get static data if available
    with data_lock:
        statics = SHIP_STATIC_DATA.get(mmsi, {})
    
    ship_name = statics.get("ship_name", "Unknown")
    
    # Calculate ship length and width from dimensions
    dim_a = statics.get("dim_a") or 0
    dim_b = statics.get("dim_b") or 0
    dim_c = statics.get("dim_c") or 0
    dim_d = statics.get("dim_d") or 0
    
    length = dim_a + dim_b
    width = dim_c + dim_d
    
    # If dimensions are missing, use defaults based on ship type
    if length <= 0:
        length = 100  # Default length
    if width <= 0:
        width = 20  # Default width

    ship_data = {
        "mmsi": mmsi,
        "ship_name": ship_name,
        "latitude": latitude,
        "longitude": longitude,
        "sog": sog,
        "cog": cog,
        "heading": heading,
        "timestamp": time_received_str,
        "length": length,
        "width": width
    }
    
    # Store position in historical data for trajectory prediction
    with data_lock:
        # Add to history with timestamp
        SHIP_HISTORY[mmsi].append({
            "latitude": latitude,
            "longitude": longitude,
            "sog": sog,
            "cog": cog,
            "heading": heading,
            "timestamp": time_received_str
        })
        
        # Keep only last 60 minutes of data
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
        SHIP_HISTORY[mmsi] = [p for p in SHIP_HISTORY[mmsi] if p["timestamp"] >= cutoff]
    
    return ship_data

def compute_cpa_tcpa(shipA, shipB):
    """Simplified CPA/TCPA calculation between two ships"""
    # This is a simple placeholder - for a real implementation, you would use 
    # more sophisticated calculations from the original app
    import math
    
    # Convert speed and course to vectors
    def to_xy(lat, lon):
        x = lon * 60 * math.cos(math.radians(lat))
        y = lat * 60
        return x, y
        
    def cog_to_vector(cog, sog):
        rad = math.radians(cog)
        vx = sog * math.sin(rad)
        vy = sog * math.cos(rad)
        return vx, vy
    
    xA, yA = to_xy(shipA["latitude"], shipA["longitude"])
    xB, yB = to_xy(shipB["latitude"], shipB["longitude"])
    dx, dy = xA - xB, yA - yB
    
    vxA, vyA = cog_to_vector(shipA["cog"], shipA["sog"])
    vxB, vyB = cog_to_vector(shipB["cog"], shipB["sog"])
    dvx, dvy = vxA - vxB, vyA - vyB
    
    v2 = dvx**2 + dvy**2
    
    if v2 == 0:
        tcpa = 0
    else:
        tcpa = - (dx*dvx + dy*dvy) / v2
        if tcpa < 0:
            tcpa = 0
            
    xA_cpa = xA + vxA * tcpa
    yA_cpa = yA + vyA * tcpa
    xB_cpa = xB + vxB * tcpa
    yB_cpa = yB + vyB * tcpa
    
    cpa = math.sqrt((xA_cpa - xB_cpa)**2 + (yA_cpa - yB_cpa)**2)
    
    return cpa, tcpa

##################################################
# ML Prediction Functions
##################################################
def predict_trajectory(mmsi):
    """
    Predict future trajectory based on historical positions
    In a real implementation, this would use a trained ML model
    Here we'll use a simple physics-based model for demonstration
    """
    with data_lock:
        history = SHIP_HISTORY.get(mmsi, [])
    
    if len(history) < 2:
        return None  # Not enough data for prediction
    
    # Sort by timestamp (newest first)
    history = sorted(history, key=lambda x: x["timestamp"], reverse=True)
    current = history[0]
    
    # Simple physics-based prediction (constant velocity model)
    predictions = []
    lat = current["latitude"]
    lon = current["longitude"]
    sog = current["sog"]
    cog = current["cog"] if current["cog"] is not None else 0
    
    # Convert knots to degrees per minute (very rough approximation)
    # 1 knot ≈ 0.0166 degrees of latitude per hour at the equator
    # Longitude conversion depends on latitude
    lat_speed = sog * 0.0166 / 60  # degrees per minute
    lon_speed = lat_speed / math.cos(math.radians(lat)) if lat != 90 and lat != -90 else 0
    
    # Project movement based on course
    lat_component = lat_speed * math.cos(math.radians(cog))
    lon_component = lon_speed * math.sin(math.radians(cog))
    
    # Generate predictions with uncertainty
    current_time = datetime.fromisoformat(current["timestamp"].replace("Z", "+00:00"))
    
    for step in range(1, PREDICTION_STEPS + 1):
        # Time increment (minutes)
        time_delta = step * (PREDICTION_HORIZON / PREDICTION_STEPS)
        
        # Simple linear projection
        pred_lat = lat + lat_component * time_delta
        pred_lon = lon + lon_component * time_delta
        pred_time = (current_time + timedelta(minutes=time_delta)).isoformat().replace("+00:00", "Z")
        
        # Add some randomness to simulate uncertainty (would be model-based in reality)
        uncertainties = []
        for level in UNCERTAINTY_LEVELS:
            # Uncertainty grows with time
            radius = level * step * sog * 0.02  # nautical miles
            uncertainties.append(radius)
        
        predictions.append({
            "latitude": pred_lat,
            "longitude": pred_lon,
            "timestamp": pred_time,
            "uncertainty": uncertainties,
            "step": step
        })
    
    return predictions

def assess_collision_risk(collision):
    """
    Perform advanced risk assessment for a collision
    In a production system, this would use an ensemble of ML models
    Here we'll use a simple rule-based approach for demonstration
    """
    ship_a = collision["ship_a"]
    ship_b = collision["ship_b"]
    cpa = collision["cpa"]
    tcpa = collision["tcpa"]
    
    # Basic risk factors
    risk_factors = {
        "cpa_risk": 1.0 - min(cpa / 0.5, 1.0),  # Higher risk for smaller CPA
        "tcpa_risk": 1.0 - min(tcpa / 30.0, 1.0) if tcpa > 0 else 1.0,  # Higher risk for smaller TCPA
        "speed_risk": min((ship_a["sog"] + ship_b["sog"]) / 30.0, 1.0),  # Higher risk for faster ships
        "approach_angle_risk": 0.0,  # Will be calculated
    }
    
    # Calculate approach angle (crossing situation is higher risk)
    angle_diff = abs(ship_a["cog"] - ship_b["cog"])
    if angle_diff > 180:
        angle_diff = 360 - angle_diff
    
    # Crossing at right angles is highest risk
    risk_factors["approach_angle_risk"] = math.sin(math.radians(angle_diff))
    
    # Ensemble risk calculation (weighted average)
    weights = {
        "cpa_risk": 0.4,
        "tcpa_risk": 0.3,
        "speed_risk": 0.2,
        "approach_angle_risk": 0.1
    }
    
    overall_risk = sum(risk_factors[k] * weights[k] for k in weights)
    
    # Calculate uncertainty based on data quality
    uncertainty = 0.2  # Base uncertainty
    
    return {
        "collision_id": collision["id"],
        "overall_risk": overall_risk,
        "risk_factors": risk_factors,
        "uncertainty": uncertainty,
        "explanation": {
            "cpa_factor": f"CPA of {cpa:.2f} nm contributes {risk_factors['cpa_risk']:.2f} to risk",
            "tcpa_factor": f"TCPA of {tcpa:.2f} min contributes {risk_factors['tcpa_risk']:.2f} to risk",
            "speed_factor": f"Combined speed of {ship_a['sog'] + ship_b['sog']:.2f} knots contributes {risk_factors['speed_risk']:.2f} to risk",
            "angle_factor": f"Approach angle of {angle_diff:.2f}° contributes {risk_factors['approach_angle_risk']:.2f} to risk"
        }
    }

def update_ml_predictions():
    """Update ML predictions for all vessels and collisions"""
    global TRAJECTORY_PREDICTIONS, RISK_ASSESSMENT
    
    # Update trajectory predictions for all vessels
    with data_lock:
        ships = SHIPS_DATA.copy()
        collisions = COLLISION_DATA.copy()
    
    # Generate trajectory predictions
    new_predictions = {}
    for ship in ships:
        mmsi = ship["mmsi"]
        prediction = predict_trajectory(mmsi)
        if prediction:
            new_predictions[mmsi] = prediction
    
    # Generate risk assessments
    new_risk_assessments = {}
    for collision in collisions:
        risk = assess_collision_risk(collision)
        new_risk_assessments[collision["id"]] = risk
    
    # Update global data
    with data_lock:
        TRAJECTORY_PREDICTIONS = new_predictions
        RISK_ASSESSMENT = new_risk_assessments

def check_collisions():
    """Check for potential collisions between ships"""
    global COLLISION_DATA
    
    with data_lock:
        ships = SHIPS_DATA.copy()
    
    new_collisions = []
    
    # Only check for collisions if we have at least 2 ships
    if len(ships) < 2:
        return
    
    # Check each pair of ships
    for i in range(len(ships)):
        for j in range(i+1, len(ships)):
            ship_a = ships[i]
            ship_b = ships[j]
            
            # Calculate CPA (Closest Point of Approach) and TCPA (Time to CPA)
            cpa, tcpa = compute_cpa_tcpa(ship_a, ship_b)
            
            # If CPA is less than 0.5 nautical miles and TCPA is less than 30 minutes
            if cpa < 0.5 and tcpa < 30:
                collision = {
                    "id": f"coll-{ship_a['mmsi']}-{ship_b['mmsi']}",
                    "ship_a": {
                        "mmsi": ship_a["mmsi"],
                        "latitude": ship_a["latitude"],
                        "longitude": ship_a["longitude"],
                        "cog": ship_a["cog"],
                        "sog": ship_a["sog"],
                        "ship_name": ship_a["ship_name"]
                    },
                    "ship_b": {
                        "mmsi": ship_b["mmsi"],
                        "latitude": ship_b["latitude"],
                        "longitude": ship_b["longitude"],
                        "cog": ship_b["cog"],
                        "sog": ship_b["sog"],
                        "ship_name": ship_b["ship_name"]
                    },
                    "cpa": cpa,
                    "tcpa": tcpa,
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                }
                
                new_collisions.append(collision)
    
    with data_lock:
        COLLISION_DATA = new_collisions

async def connect_ais_stream():
    global SHIPS_DATA, COLLISION_DATA
    uri = "wss://stream.aisstream.io/v0/stream"
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    
    # Check if running on Railway
    is_railway = os.environ.get('RAILWAY_ENVIRONMENT') is not None
    if is_railway:
        print("🚀 Running on Railway environment - using special connection settings")
    
    # Show full token for debugging
    token_display = AISSTREAM_TOKEN if len(AISSTREAM_TOKEN) < 10 else f"{AISSTREAM_TOKEN[:5]}...{AISSTREAM_TOKEN[-5:]}"
    print(f"⚠️ Connecting to AIS Stream with token: {token_display}")
    
    connection_attempts = 0
    max_attempts = 5
    reconnect_delay = 5
    
    while True:
        try:
            connection_attempts += 1
            print(f"Connection attempt {connection_attempts}/{max_attempts}...")
            
            # More detailed debugging
            print(f"Creating websocket connection to {uri}")
            
            # Railway-optimized connection settings
            connection_kwargs = {
                'uri': uri,
                'ping_interval': 20,
                'ping_timeout': 60,
                'close_timeout': 90,
                'ssl': ssl_context,
                'compression': None,
                'max_size': 2**23  # Larger message size
            }
            
            print(f"Connection parameters: {connection_kwargs}")
            async with websockets.connect(**connection_kwargs) as websocket:
                # Subscribe to AIS messages
                subscribe_message = {
                    "APIKey": AISSTREAM_TOKEN,
                    "MessageType": "Subscribe",
                    "BoundingBoxes": [
                        [[49.0, -8.0], [55.0, 8.0]],  # Expanded English Channel & North Sea 
                        [[35.0, -10.0], [45.0, 5.0]],  # Mediterranean area
                        [[20.0, -80.0], [40.0, -60.0]]  # North American eastern coast
                    ],
                    "OutputFormat": "JSON",
                    "Compression": "None",
                    "BufferSize": 1,
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }
                
                print(f"Sending subscription message to AIS Stream...")
                message_json = json.dumps(subscribe_message)
                print(f"Subscription message length: {len(message_json)} bytes")
                await websocket.send(message_json)
                print("✅ Successfully connected to AIS Stream and sent subscription")
                
                # Wait for acknowledgment
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=15.0)
                    print(f"Received subscription response: {response}")
                except asyncio.TimeoutError:
                    print("No subscription response received, continuing anyway")
                
                # Reset connection attempts after successful connection
                connection_attempts = 0
                reconnect_delay = 5
                
                # Process messages
                message_count = 0
                async for message_json in websocket:
                    try:
                        message_count += 1
                        message = json.loads(message_json)
                        msg_type = message.get("MessageType")
                        
                        if msg_type == "ShipStaticData":
                            process_ship_static_data(message)
                            if message_count % 10 == 0:
                                print(f"✅ Processed ship static data #{message_count}")
                        elif msg_type == "PositionReport":
                            ship_data = process_position_report(message)
                            if ship_data:
                                # Update the ships data with the new ship information
                                with data_lock:
                                    # Remove the old ship data if it exists
                                    SHIPS_DATA = [s for s in SHIPS_DATA if s["mmsi"] != ship_data["mmsi"]]
                                    # Add the new ship data
                                    SHIPS_DATA.append(ship_data)
                                    # Keep only the last 100 ships
                                    if len(SHIPS_DATA) > 100:
                                        SHIPS_DATA = SHIPS_DATA[-100:]
                                    
                                    # Log data received for debugging
                                    if message_count % 10 == 0:
                                        print(f"✅ Ships data updated, now tracking {len(SHIPS_DATA)} ships (message #{message_count})")
                                
                                # Check for collisions after receiving new ship data
                                check_collisions()
                        else:
                            print(f"Received message of type: {msg_type}")
                    except json.JSONDecodeError as e:
                        print(f"JSON decode error: {e}")
                        continue
                    except Exception as e:
                        print(f"Error processing AIS message: {e}")
        except websockets.exceptions.ConnectionClosed as e:
            print(f"⚠️ Websocket connection closed: {e}")
        except asyncio.exceptions.TimeoutError:
            print(f"⚠️ Websocket connection timeout")
        except Exception as e:
            print(f"⚠️ Connection error: {e}, {type(e).__name__}")
            import traceback
            traceback.print_exc()
        
        # Check if we've reached the maximum number of attempts
        if connection_attempts >= max_attempts:
            print(f"❌ Failed to connect after {max_attempts} attempts. Will retry again later.")
            # Longer delay after max attempts
            await asyncio.sleep(60)
            connection_attempts = 0
            # Increase timeout with exponential backoff
            reconnect_delay = min(reconnect_delay * 2, 60)
        
        print(f"Reconnecting to AIS Stream in {reconnect_delay} seconds...")
        await asyncio.sleep(reconnect_delay)

def start_ais_stream():
    """Start the AIS Stream in a separate thread"""
    def run_async_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(connect_ais_stream())
    
    thread = threading.Thread(target=run_async_loop)
    thread.daemon = True
    thread.start()
    return thread

def start_ml_predictions():
    """Start the ML prediction loop in a separate thread"""
    def run_prediction_loop():
        while True:
            try:
                update_ml_predictions()
            except Exception as e:
                print(f"Error updating ML predictions: {e}")
            
            # Update predictions every 15 seconds
            time.sleep(15)
    
    thread = threading.Thread(target=run_prediction_loop)
    thread.daemon = True
    thread.start()
    return thread

##################################################
# API Routes for ML-enhanced frontend
##################################################
@app.route('/api/vessels')
def api_vessels():
    """API endpoint for vessel data that matches the frontend expectations"""
    with data_lock:
        ships_copy = SHIPS_DATA.copy()
    
    # Format ships data for the frontend
    vessels = []
    for ship in ships_copy:
        vessel = {
            "mmsi": ship["mmsi"],
            "name": ship["ship_name"],
            "lat": ship["latitude"],
            "lon": ship["longitude"],
            "sog": ship["sog"],
            "cog": ship["cog"],
            "heading": ship["heading"] if ship["heading"] is not None else ship["cog"],
            "length": ship["length"],
            "width": ship["width"],
            "timestamp": ship["timestamp"]
        }
        vessels.append(vessel)
    
    return jsonify(vessels)

@app.route('/api/collisions')
def api_collisions():
    """API endpoint for collision data that matches the frontend expectations"""
    max_cpa = request.args.get('max_cpa', default=0.5, type=float)
    max_tcpa = request.args.get('max_tcpa', default=30.0, type=float)
    
    with data_lock:
        collisions_copy = COLLISION_DATA.copy()
        risk_assessment_copy = RISK_ASSESSMENT.copy()
    
    # Filter collisions based on CPA and TCPA thresholds
    filtered_collisions = [
        c for c in collisions_copy 
        if c["cpa"] <= max_cpa and c["tcpa"] <= max_tcpa
    ]
    
    # Format for frontend
    result = []
    for collision in filtered_collisions:
        # Add risk assessment data if available
        risk_data = risk_assessment_copy.get(collision["id"], {})
        overall_risk = risk_data.get("overall_risk", 0.5)  # Default risk if not available
        
        formatted = {
            "id": collision["id"],
            "vessel_a": {
                "mmsi": collision["ship_a"]["mmsi"],
                "name": collision["ship_a"]["ship_name"],
                "lat": collision["ship_a"]["latitude"],
                "lon": collision["ship_a"]["longitude"],
                "sog": collision["ship_a"]["sog"],
                "cog": collision["ship_a"]["cog"]
            },
            "vessel_b": {
                "mmsi": collision["ship_b"]["mmsi"],
                "name": collision["ship_b"]["ship_name"],
                "lat": collision["ship_b"]["latitude"],
                "lon": collision["ship_b"]["longitude"],
                "sog": collision["ship_b"]["sog"],
                "cog": collision["ship_b"]["cog"]
            },
            "cpa": collision["cpa"],
            "tcpa": collision["tcpa"],
            "risk": overall_risk,
            "timestamp": collision["timestamp"]
        }
        result.append(formatted)
    
    return jsonify(result)

@app.route('/api/trajectories')
def api_trajectories():
    """API endpoint for predicted vessel trajectories"""
    mmsi = request.args.get('mmsi')
    
    with data_lock:
        if mmsi:
            # Return trajectory for specific vessel
            try:
                mmsi = int(mmsi)
                trajectory = TRAJECTORY_PREDICTIONS.get(mmsi, [])
                return jsonify(trajectory)
            except (ValueError, TypeError):
                return jsonify({"error": "Invalid MMSI format"}), 400
        else:
            # Return all trajectories
            return jsonify(TRAJECTORY_PREDICTIONS)

@app.route('/api/risk_assessment')
def api_risk_assessment():
    """API endpoint for detailed risk assessment"""
    collision_id = request.args.get('collision_id')
    
    with data_lock:
        if collision_id:
            # Return risk assessment for specific collision
            risk = RISK_ASSESSMENT.get(collision_id, {})
            return jsonify(risk)
        else:
            # Return all risk assessments
            return jsonify(RISK_ASSESSMENT)

@app.route('/api/heatmap')
def api_heatmap():
    """API endpoint for risk heatmap data"""
    # In a real implementation, this would generate a proper heatmap
    # based on vessel density and collision risks
    
    with data_lock:
        ships = SHIPS_DATA.copy()
        collisions = COLLISION_DATA.copy()
    
    # Simple heatmap based on ship density and collision locations
    heatmap_points = []
    
    # Add points for each ship
    for ship in ships:
        heatmap_points.append({
            "lat": ship["latitude"],
            "lon": ship["longitude"],
            "weight": 0.3  # Base weight for ship presence
        })
    
    # Add higher weight points for collision areas
    for collision in collisions:
        # Midpoint between the two ships
        lat = (collision["ship_a"]["latitude"] + collision["ship_b"]["latitude"]) / 2
        lon = (collision["ship_a"]["longitude"] + collision["ship_b"]["longitude"]) / 2
        
        # Higher weight for closer CPA and TCPA
        weight = 1.0 - (collision["cpa"] / 0.5) * 0.5  # 0.5 to 1.0 based on CPA
        if collision["tcpa"] > 0:
            weight *= 1.0 - (collision["tcpa"] / 30.0) * 0.5  # Reduce weight for far-future collisions
        
        heatmap_points.append({
            "lat": lat,
            "lon": lon,
            "weight": min(1.0, weight)  # Cap at 1.0
        })
    
    return jsonify(heatmap_points)

##################################################
# Existing API routes with improved websocket handling
##################################################
@app.route('/ws')
def handle_ws():
    """Handle the websocket polling - return HTTP 200 to prevent errors"""
    with data_lock:
        ships_copy = SHIPS_DATA.copy()
        collisions_copy = COLLISION_DATA.copy()
    
    # Return a proper response with data and 200 status code
    return jsonify({
        "message": "Websocket support is not available, use REST API endpoints instead",
        "status": "ok",
        "data": {
            "ships": ships_copy[:5],
            "collisions": collisions_copy[:2]
        }
    }), 200

@app.route('/')
def index():
    """Main route with diagnostic information"""
    # Get diagnostic information
    with data_lock:
        ship_count = len(SHIPS_DATA)
        collision_count = len(COLLISION_DATA)
        last_update = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        if ship_count > 0:
            last_ship = SHIPS_DATA[-1]["timestamp"]
        else:
            last_ship = "No ships yet"
    
    # Include diagnostic info in a way that doesn't affect the UI
    context = {
        'diagnostics': {
            'ship_count': ship_count,
            'collision_count': collision_count,
            'last_update': last_update,
            'last_ship': last_ship,
            'token_prefix': AISSTREAM_TOKEN[:5] + "..."
        }
    }
    
    # Log diagnostic info to console
    print(f"Diagnostic info - Ships: {ship_count}, Collisions: {collision_count}, Last ship: {last_ship}")
    
    return render_template('index.html', **context)

@app.route('/ships')
def ships():
    """
    Returns real-time ship data from AIS Stream
    """
    with data_lock:
        return jsonify(SHIPS_DATA)

@app.route('/collisions')
def collisions():
    """
    Returns real-time collision data
    """
    max_cpa = request.args.get('max_cpa', default=0.5, type=float)
    max_tcpa = request.args.get('max_tcpa', default=30.0, type=float)
    
    with data_lock:
        filtered_collisions = [c for c in COLLISION_DATA if c["cpa"] <= max_cpa and c["tcpa"] <= max_tcpa]
        return jsonify(filtered_collisions)

@app.route('/history')
def history():
    return render_template('history.html')

@app.route("/history_filelist")
def history_filelist():
    # Return real history files if they exist, otherwise empty list
    return jsonify([])

@app.route("/history_file")
def history_file():
    # Return real historical data or empty array
    return jsonify([])

@app.route('/api/status')
def api_status():
    """API endpoint for checking service health and data status"""
    with data_lock:
        ship_count = len(SHIPS_DATA)
        collision_count = len(COLLISION_DATA)
        last_update = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        
        # Calculate how long the service has been receiving data
        if ship_count > 0:
            last_ship_time = SHIPS_DATA[-1]["timestamp"]
            try:
                last_ship_dt = datetime.fromisoformat(last_ship_time.replace("Z", "+00:00"))
                time_since_last = (datetime.now(timezone.utc) - last_ship_dt).total_seconds()
            except (ValueError, TypeError):
                time_since_last = -1
        else:
            last_ship_time = "No ships yet"
            time_since_last = -1
    
    status = {
        "status": "ok",
        "version": "1.0",
        "ship_count": ship_count,
        "collision_count": collision_count,
        "last_update": last_update,
        "last_ship": last_ship_time,
        "seconds_since_last_ship": time_since_last,
        "ais_token_valid": len(AISSTREAM_TOKEN) > 10,
        "token_prefix": AISSTREAM_TOKEN[:5] + "..." if len(AISSTREAM_TOKEN) > 5 else "Invalid"
    }
    
    return jsonify(status)

##################################################
# Uruchomienie
##################################################
if __name__ == '__main__':
    print("Starting Enhanced AIS Collision Detection App with ML capabilities...")
    print("Server URL: http://localhost:5000/")
    
    # Start AIS Stream in a separate thread
    ais_thread = start_ais_stream()
    
    # Start ML prediction thread
    ml_thread = start_ml_predictions()
    
    try:
        # Start Flask server
        app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
    except Exception as e:
        print(f"Error starting server: {e}") 