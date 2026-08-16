import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID", "35485985"))
API_HASH = os.getenv("API_HASH", "5441c09a9c8bf58374e1f8f227b95794")
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
BASE_URL = os.getenv("BASE_URL", "https://filetostream-9257652a6256.herokuapp.com").rstrip("/")
PORT = int(os.getenv("PORT", "8080"))
