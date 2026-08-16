import os
from dotenv import load_dotenv

load_dotenv()

# Bot Credentials & Server Settings
BOT_TOKEN = os.getenv("BOT_TOKEN", "8788884009:AAEifV0e9MVaLtUzQD40uVoaO1WtxA1VUFs")
BASE_URL = os.getenv("BASE_URL", "https://filetostream-9257652a6256.herokuapp.com").rstrip("/")
PORT = int(os.getenv("PORT", "8080"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

# Database (MongoDB Atlas)
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://gopaljibillionaire_db_user:lZXfbyvE3u92EdP5@cluster0.cusdpcp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
DATABASE_NAME = os.getenv("DATABASE_NAME", "telegram_stream_bot")

# Verification Channel
REQUIRED_CHANNEL_ID_RAW = os.getenv("REQUIRED_CHANNEL_ID", "-1003985304953").strip()
try:
    REQUIRED_CHANNEL_ID = int(REQUIRED_CHANNEL_ID_RAW)
except ValueError:
    REQUIRED_CHANNEL_ID = REQUIRED_CHANNEL_ID_RAW

REQUIRED_CHANNEL_USERNAME = os.getenv("REQUIRED_CHANNEL_USERNAME", "yagamicorporation").replace("@", "")

# Admin & Payments
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "7952327997")
ADMIN_IDS = [
    int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().lstrip("-").isdigit()
]
PAYMENT_PROVIDER_TOKEN = os.getenv("PAYMENT_PROVIDER_TOKEN", "")

# Operational Limits & Defaults
FREE_LINK_EXPIRY_HOURS = int(os.getenv("FREE_LINK_EXPIRY_HOURS", "24"))
MAX_FREE_FILE_SIZE_MB = int(os.getenv("MAX_FREE_FILE_SIZE_MB", "2000"))
PREMIUM_PRICE_INR = float(os.getenv("PREMIUM_PRICE_INR", "149"))
PREMIUM_PRICE_USD = float(os.getenv("PREMIUM_PRICE_USD", "2"))
PREMIUM_PRICE_XTR = int(os.getenv("PREMIUM_PRICE_XTR", "250"))
