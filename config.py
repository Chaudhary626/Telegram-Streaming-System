"""
Configuration loader for TG Stream Server.
Reads from .env file and provides typed constants.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ─── Telegram ────────────────────────────────────────────────
API_ID: int = int(os.getenv("API_ID", "0"))
API_HASH: str = os.getenv("API_HASH", "")
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
CHANNEL_ID: int = int(os.getenv("CHANNEL_ID", "0"))

# ─── Database ────────────────────────────────────────────────
DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: int = int(os.getenv("DB_PORT", "3306"))
DB_NAME: str = os.getenv("DB_NAME", "")
DB_USER: str = os.getenv("DB_USER", "")
DB_PASS: str = os.getenv("DB_PASS", "")

# ─── Stream Server ───────────────────────────────────────────
STREAM_SECRET: str = os.getenv("STREAM_SECRET", "change-me")
STREAM_HOST: str = os.getenv("STREAM_HOST", "0.0.0.0")
STREAM_PORT: int = int(os.getenv("STREAM_PORT", "8080"))
TOKEN_LIFETIME: int = int(os.getenv("TOKEN_LIFETIME", "14400"))

# Public URL of this server (used by bot to generate links)
# e.g. https://your-app.onrender.com or https://stream.yourdomain.com
STREAM_BASE_URL: str = os.getenv("STREAM_BASE_URL", "").rstrip("/")

ALLOWED_ORIGINS: list = [
    o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()
]

# ─── Performance ─────────────────────────────────────────────
# Telegram MTProto max chunk = 1MB (1048576 bytes)
# This is the maximum allowed by Telegram's upload.GetFile
CHUNK_SIZE: int = min(
    int(os.getenv("CHUNK_SIZE", "1048576")),
    1048576  # Hard cap at 1MB — Telegram protocol limit
)
MAX_STREAMS_PER_IP: int = int(os.getenv("MAX_STREAMS_PER_IP", "5"))

# ─── Validation ──────────────────────────────────────────────
def validate():
    """Check required config values are set."""
    errors = []
    if not API_ID or API_ID == 12345678:
        errors.append("API_ID: Get from https://my.telegram.org")
    if not API_HASH or API_HASH == "your_api_hash_from_my_telegram_org":
        errors.append("API_HASH: Get from https://my.telegram.org")
    if not BOT_TOKEN or "your_bot_token" in BOT_TOKEN:
        errors.append("BOT_TOKEN: Get from @BotFather")
    if STREAM_SECRET == "change-me":
        errors.append("STREAM_SECRET: Generate with: python -c \"import secrets; print(secrets.token_hex(32))\"")
    if not DB_NAME:
        errors.append("DB_NAME: Set your MySQL database name")
    return errors
