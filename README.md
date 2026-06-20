# Telegram MTProto Video Stream Server

Stream videos from Telegram on your website with **NO 20MB limit**.
Uses MTProto protocol (Pyrogram) to stream files up to **4GB** directly from Telegram data centers.

## Architecture

```
User Browser → Your Website (PHP) → Stream Server (Python/FastAPI) → Telegram DC (MTProto)
                                         ↑ NO Bot API ↑
                                         ↑ NO 20MB limit ↑
```

## Quick Setup

### 1. Get Telegram Credentials

1. Go to https://my.telegram.org
2. Log in with your phone number
3. Go to "API development tools"
4. Create a new application
5. Copy **API_ID** and **API_HASH**
6. You also need your **BOT_TOKEN** from @BotFather

### 2. Configure

```bash
cd telegram-streamer
cp .env.example .env
nano .env  # Fill in your values
```

Required values:
- `API_ID` — From my.telegram.org
- `API_HASH` — From my.telegram.org  
- `BOT_TOKEN` — From @BotFather
- `CHANNEL_ID` — Your channel ID (-100...)
- `DB_HOST/DB_NAME/DB_USER/DB_PASS` — Same as your PHP website
- `STREAM_SECRET` — Generate: `python -c "import secrets; print(secrets.token_hex(32))"`

### 3. Deploy

#### Option A: Railway (Recommended - Easiest)

1. Push `telegram-streamer/` to a GitHub repo
2. Go to https://railway.app → New Project → Deploy from GitHub
3. Add environment variables from your `.env`
4. Railway will auto-detect the Dockerfile
5. Note your deployed URL (e.g., `https://your-app.railway.app`)

#### Option B: Render

1. Push to GitHub
2. Go to https://render.com → New Web Service
3. Set build command: `pip install -r requirements.txt`
4. Set start command: `uvicorn server:app --host 0.0.0.0 --port $PORT`
5. Add environment variables

#### Option C: VPS (DigitalOcean/Hetzner)

```bash
# On your VPS:
git clone your-repo
cd telegram-streamer
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env  # Fill in values

# Run with systemd or screen:
uvicorn server:app --host 0.0.0.0 --port 8080
```

#### Option D: Local Testing

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your values
python server.py
```

### 4. Connect to PHP Website

Edit `config/config.php` on your Hostinger website:

```php
define('STREAM_SERVER_URL', 'https://your-app.railway.app');  // Your deployed URL
define('STREAM_SERVER_SECRET', 'same-secret-as-python-env');  // Must match!
```

### 5. Enable Remote MySQL (if using Hostinger)

1. Log into Hostinger hPanel
2. Go to Databases → Remote MySQL
3. Add the IP of your stream server (or `%` for any IP during testing)

### 6. Test

1. Send a video to your bot on Telegram
2. Check the bot responds with file_id
3. Open: `https://your-stream-server/health`
4. Should show: `"twenty_mb_limit": "BYPASSED"`

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /stream/{file_id}?token=...&expires=...&size=...` | Stream video (supports Range) |
| `HEAD /stream/{file_id}?token=...&expires=...&size=...` | Get file headers |
| `GET /health` | Server status |
| `GET /test/{file_id}?key=SECRET` | Test file accessibility |

## How It Works

1. **Video Upload**: You upload video to Telegram channel
2. **Bot Detection**: Bot auto-saves `file_id` + metadata to MySQL
3. **User Plays**: Browser requests video from PHP website
4. **PHP Signs URL**: PHP generates HMAC-signed URL → Python server
5. **MTProto Stream**: Python server uses `upload.GetFile` RPC (MTProto)
6. **Direct DC**: Connects to Telegram data center, downloads in 1MB chunks
7. **HTTP Response**: Sends chunks to browser as `video/mp4` with Range support
8. **Seeking**: Browser Range requests → Python reads from correct offset

## Troubleshooting

| Issue | Fix |
|---|---|
| "Invalid API_ID" | Check API_ID/API_HASH from my.telegram.org |
| "BOT_TOKEN invalid" | Check token from @BotFather |
| Videos don't play | Check browser Console + Network tab |
| CORS errors | Add your domain to ALLOWED_ORIGINS in .env |
| Database connection failed | Enable Remote MySQL in Hostinger |
| 403 on stream URL | Check STREAM_SERVER_SECRET matches in both PHP and Python |
