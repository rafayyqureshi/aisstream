import os

# Set RENDER environment variable to true for production
os.environ['RENDER'] = 'true'

from app import app

if __name__ == "__main__":
    app.run() 