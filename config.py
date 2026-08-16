import os
from dotenv import load_dotenv

# Load variables from .env file if running locally
load_dotenv()

# -------------------------------------------------------------------
# Telegram API Credentials
# Obtain API_ID and API_HASH from https://my.telegram.org
# -------------------------------------------------------------------
API_ID = int(os.getenv("API_ID", "35485985"))
API_HASH = os.getenv("API_HASH", "5441c09a9c8bf58374e1f8f227b95794")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8788884009:AAEifV0e9MVaLtUzQD40uVoaO1WtxA1VUFs)

# Telegram HTTP API endpoint (Defaults to official API server)
TELEGRAM_API_URL = os.getenv("TELEGRAM_API_URL", "https://api.telegram.org")

# -------------------------------------------------------------------
# Server & Network Settings
# -------------------------------------------------------------------
BASE_URL = os.getenv(
    "BASE_URL", 
    "https://filetostream-9257652a6256.herokuapp.com"
).rstrip("/")

# Heroku dynamically binds to PORT environment variable
PORT = int(os.getenv("PORT", "8080"))

# -------------------------------------------------------------------
# Database Settings (MongoDB Atlas / Async Motor)
# -------------------------------------------------------------------
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://gopaljibillionaire_db_user:lZXfbyvE3u92EdP5@cluster0.cusdpcp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
DATABASE_NAME = os.getenv("DATABASE_NAME", "telegram_stream_bot")

# -------------------------------------------------------------------
# Force Subscribe Channel Setup
# Set REQUIRED_CHANNEL_ID to 0 or leave empty if you want to disable force-sub
# -------------------------------------------------------------------
REQUIRED_CHANNEL_ID_RAW = os.getenv("REQUIRED_CHANNEL_ID", "-1003985304953").strip()
try:
    REQUIRED_CHANNEL_ID = int(REQUIRED_CHANNEL_ID_RAW) if REQUIRED_CHANNEL_ID_RAW else 0
except ValueError:
    REQUIRED_CHANNEL_ID = 0

REQUIRED_CHANNEL_USERNAME = os.getenv(
    "REQUIRED_CHANNEL_USERNAME", 
    "yagamicorporation"
).replace("@", "")

# -------------------------------------------------------------------
# Admin Management
# Comma-separated list of Telegram User IDs who can use admin commands
# -------------------------------------------------------------------
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "7952327997")
ADMIN_IDS = [
    int(x.strip()) 
    for x in ADMIN_IDS_RAW.split(",") 
    if x.strip().lstrip("-").isdigit()
]

# -------------------------------------------------------------------
# Additional Bot Configuration & Limits
# -------------------------------------------------------------------
FREE_LINK_EXPIRY_HOURS = int(os.getenv("FREE_LINK_EXPIRY_HOURS", "24"))
MAX_FREE_FILE_SIZE_MB = int(os.getenv("MAX_FREE_FILE_SIZE_MB", "2000"))
