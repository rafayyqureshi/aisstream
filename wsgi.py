import os

# Set RENDER environment variable to ensure demo data is loaded
os.environ['RENDER'] = 'true'

from app import app

if __name__ == "__main__":
    app.run() 