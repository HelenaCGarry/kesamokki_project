# Import the necessary module
from dotenv import load_dotenv
import os

# Load environment variables from the .env file (if present)
load_dotenv()

# Access environment variables as if they came from the actual environment
OPENROUTESERVICE_KEY = os.getenv('OPENROUTESERVICE_API_KEY')
GOOGLE_KEY = os.getenv('GOOGLE_API_KEY')

# Example usage
print(f'ORS: {OPENROUTESERVICE_KEY}')
print(f'GOOGLE: {GOOGLE_KEY}')

