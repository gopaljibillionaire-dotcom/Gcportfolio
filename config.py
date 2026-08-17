import os
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------------------------
# Telegram API Credentials & Token
# -------------------------------------------------------------------
API_ID = int(os.getenv("API_ID", "35485985"))
API_HASH = os.getenv("API_HASH", "5441c09a9c8bf58374e1f8f227b95794")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8788884009:AAEifV0e9MVaLtUzQD40uVoaO1WtxA1VUFs")

# -------------------------------------------------------------------
# Storage & Channel Configuration
# -------------------------------------------------------------------
BIN_CHANNEL_RAW = os.getenv("BIN_CHANNEL", "-1004412044372").strip()
if BIN_CHANNEL_RAW.startswith("@binfileshere"):
    BIN_CHANNEL = BIN_CHANNEL_RAW
else:
    try:
        BIN_CHANNEL = int(BIN_CHANNEL_RAW)
    except ValueError:
        BIN_CHANNEL = -1004412044372

REQUIRED_CHANNEL_USERNAME = os.getenv("REQUIRED_CHANNEL_USERNAME", "yagamicorporation").replace("@", "").strip()

# -------------------------------------------------------------------
# Server Settings
# -------------------------------------------------------------------
BASE_URL = os.getenv("BASE_URL", "https://filetostream-915a8dcd0cea.herokuapp.com/").rstrip("/")
PORT = int(os.getenv("PORT", "8080"))

# -------------------------------------------------------------------
# Database (MongoDB Atlas)
# -------------------------------------------------------------------
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://gopaljibillionaire_db_user:R2dB1hEM1sjHmUzc@cluster0.y1akz97.mongodb.net/?appName=Cluster0"
)
DATABASE_NAME = os.getenv("DATABASE_NAME", "telegram_stream_bot")

# -------------------------------------------------------------------
# Admin Management
# -------------------------------------------------------------------
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "7952327997")
ADMIN_IDS = [
    int(x.strip())
    for x in ADMIN_IDS_RAW.split(",")
    if x.strip().lstrip("-").isdigit()
]
