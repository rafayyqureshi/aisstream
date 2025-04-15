#!/usr/bin/env python3
import os
# Set Railway environment variable
os.environ['RAILWAY_ENVIRONMENT'] = 'true'

# Import app with cloud environment initialization
from app import initialize_app

# Initialize the app with cloud-specific settings
app = initialize_app()

if __name__ == "__main__":
    # Get port from environment variable (Railway sets this)
    port = int(os.environ.get("PORT", 5000))
    # Start the app with host 0.0.0.0 to make it accessible externally
    app.run(host="0.0.0.0", port=port) 