# AIS Collision Detection System

A real-time maritime collision detection system with machine learning capabilities for trajectory prediction and risk assessment.

## Features

- Real-time AIS data processing
- Vessel trajectory prediction
- Multi-factor collision risk assessment
- Risk visualization with heatmaps
- Explainable risk factors

## Deployment to Render

1. Create a new Web Service on Render
2. Connect your repository
3. Use the following settings:
   - **Environment**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn wsgi:app`
   - **Add Environment Variables**:
     - `AISSTREAM_TOKEN`: Your AIS Stream API token

## API Endpoints

- `/api/vessels` - Real-time vessel data
- `/api/collisions` - Collision predictions with risk assessment
- `/api/trajectories` - Predicted vessel paths
- `/api/risk_assessment` - Detailed collision risk analysis
- `/api/heatmap` - Risk density visualization data

## Local Development

```
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
python app_demo.py
``` 