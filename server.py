"""
TG Stream Server — Combined FastAPI + Pyrogram Bot

Single process that handles:
1. Video streaming via MTProto (no 20MB limit)
2. Bot auto-detection of videos from channel
3. Health check / diagnostics

Architecture:
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│ Browser /   │────▶│ FastAPI      │────▶│ Pyrogram     │
│ Video Player│◀────│ (HTTP)       │◀────│ (MTProto)    │
└─────────────┘     └──────────────┘     └──────────────┘
                           │                     │
                    /stream/{id}          upload.GetFile
                    /health               (direct DC)
                                               │
                                    ┌──────────────┐
                                    │ Telegram DC  │
                                    │ (no limit)   │
                                    └──────────────┘
"""
import time
import hmac
import hashlib
import logging
import asyncio
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pyrogram import Client, filters, raw
from pyrogram.types import Message
from pyrogram.handlers import MessageHandler
from urllib.parse import quote as url_quote

from config import (
    API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID,
    STREAM_SECRET, STREAM_HOST, STREAM_PORT,
    STREAM_BASE_URL, TOKEN_LIFETIME, CHUNK_SIZE,
    ALLOWED_ORIGINS, validate as validate_config,
)
from streamer import TelegramStreamer
from database import (
    ensure_tables, save_video, get_video_by_file_id,
)

# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("tg-stream")

# ══════════════════════════════════════════════════════════════
# PYROGRAM CLIENT
# ══════════════════════════════════════════════════════════════
tg_client = Client(
    name="stream_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,     # No session file (stateless deploy)
    no_updates=False,    # Receive updates for bot commands
)

# Global streamer instance (initialized on startup)
streamer: Optional[TelegramStreamer] = None

# ══════════════════════════════════════════════════════════════
# FASTAPI APP
# ══════════════════════════════════════════════════════════════
app = FastAPI(
    title="TG Stream Server",
    description="MTProto video streaming — No 20MB limit",
    docs_url=None,   # Disable docs in production
    redoc_url=None,
)

# CORS — allow your website to make requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS != ["*"] else ["*"],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["Range", "Content-Type"],
    expose_headers=[
        "Content-Range", "Content-Length", "Accept-Ranges",
        "Content-Type", "Content-Disposition",
    ],
)


# ══════════════════════════════════════════════════════════════
# STARTUP / SHUTDOWN
# ══════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    """Initialize Pyrogram client and database on server start."""
    global streamer

    # Validate config
    config_errors = validate_config()
    if config_errors:
        for err in config_errors:
            logger.error(f"Config error: {err}")
        logger.error("Fix the above errors in your .env file!")
        return

    # Start Pyrogram (MTProto connection)
    await tg_client.start()
    streamer = TelegramStreamer(tg_client)

    # Ensure database tables exist
    try:
        ensure_tables()
    except Exception as e:
        logger.warning(f"Database init warning: {e}")

    # Register bot handlers
    _register_bot_handlers()

    me = await tg_client.get_me()
    logger.info(f"✅ Stream server started!")
    logger.info(f"   Bot: @{me.username} (ID: {me.id})")
    logger.info(f"   MTProto: Connected (NO 20MB limit)")
    logger.info(f"   Chunk size: {CHUNK_SIZE // 1024}KB")
    logger.info(f"   Port: {STREAM_PORT}")


@app.on_event("shutdown")
async def shutdown():
    """Clean up on server stop."""
    if tg_client.is_connected:
        await tg_client.stop()
    logger.info("Stream server stopped.")


# ══════════════════════════════════════════════════════════════
# TOKEN VALIDATION & LINK GENERATION
# ══════════════════════════════════════════════════════════════

def _validate_stream_token(
    file_id: str, token: str, expires: int, file_size: int = 0
) -> bool:
    """Validate HMAC-SHA256 token from PHP website.
    
    Token format: HMAC(file_id:expires:file_size, SECRET)
    This ensures only your PHP website can generate valid stream URLs.
    """
    if not token or not expires:
        return False

    # Check expiry
    if time.time() > expires:
        return False

    # Recompute HMAC
    payload = f"{file_id}:{expires}:{file_size}"
    expected = hmac.new(
        STREAM_SECRET.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(token, expected)


def _generate_page_token(file_id: str) -> str:
    """Generate a simple HMAC token for player/embed page access.
    
    This token does NOT expire (page-level access).
    The actual stream URL inside the page has its own time-limited token.
    """
    return hmac.new(
        STREAM_SECRET.encode("utf-8"),
        f"page:{file_id}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()[:32]


def _validate_page_token(file_id: str, token: str) -> bool:
    """Validate a player/embed page token."""
    expected = _generate_page_token(file_id)
    return hmac.compare_digest(token, expected)


def _make_stream_url(file_id: str, file_size: int = 0) -> str:
    """Generate a time-limited signed stream URL (server-side).
    
    Used by /player and /embed pages to create the <video> src.
    """
    expires = int(time.time()) + TOKEN_LIFETIME
    payload = f"{file_id}:{expires}:{file_size}"
    token = hmac.new(
        STREAM_SECRET.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    base = STREAM_BASE_URL or f"http://localhost:{STREAM_PORT}"
    return (
        f"{base}/stream/{url_quote(file_id)}"
        f"?token={token}&expires={expires}&size={file_size}"
    )


def _generate_links(file_id: str, file_size: int = 0) -> dict:
    """Generate ALL link types for a file_id.
    
    Returns dict with: stream, player, embed, html5, website
    """
    base = STREAM_BASE_URL or f"http://localhost:{STREAM_PORT}"
    page_token = _generate_page_token(file_id)
    encoded_fid = url_quote(file_id)

    return {
        "stream": _make_stream_url(file_id, file_size),
        "player": f"{base}/player/{encoded_fid}?pt={page_token}",
        "embed":  f"{base}/embed/{encoded_fid}?pt={page_token}",
    }


# ══════════════════════════════════════════════════════════════
# STREAM ENDPOINT — THE MAIN EVENT
# ══════════════════════════════════════════════════════════════

@app.get("/stream/{file_id}")
async def stream_video(
    file_id: str,
    request: Request,
    token: str = "",
    expires: int = 0,
    size: int = 0,
):
    """
    Stream video from Telegram via MTProto.
    
    ┌──────────────────────────────────────────────────────┐
    │  NO Bot API. NO getFile. NO 20MB limit.              │
    │  Uses MTProto upload.GetFile → direct DC streaming.  │
    │  Supports 1GB–4GB files with full seek support.      │
    └──────────────────────────────────────────────────────┘
    
    Query params (set by PHP website):
        token:   HMAC-SHA256 signature
        expires: Unix timestamp when token expires
        size:    File size in bytes (for Content-Range headers)
    
    Headers supported:
        Range: bytes=START-END (for video seeking)
    
    Returns:
        200 OK              — Full file
        206 Partial Content — Range request (seeking)
        403 Forbidden       — Invalid/expired token
        503 Unavailable     — Telegram connection issue
    """
    if not streamer:
        raise HTTPException(503, "Stream server not initialized")

    # ── Validate token ──────────────────────────────────────
    if not _validate_stream_token(file_id, token, expires, size):
        raise HTTPException(403, "Invalid or expired stream token")

    # ── Determine file size ─────────────────────────────────
    file_size = size
    if not file_size:
        # Try database lookup
        video = get_video_by_file_id(file_id)
        if video:
            file_size = video.get("file_size", 0) or 0
        # If still no size, try probing Telegram
        if not file_size:
            try:
                file_size = await streamer.get_file_size(file_id)
            except Exception:
                pass
    else:
        # Cache the known size
        streamer.cache_file_size(file_id, file_size)

    # ── Parse Range header ──────────────────────────────────
    range_header = request.headers.get("range", "")
    start = 0
    end = file_size - 1 if file_size > 0 else None
    is_range = False

    if range_header and file_size > 0:
        is_range = True
        range_str = range_header.replace("bytes=", "")
        parts = range_str.split("-")

        start = int(parts[0]) if parts[0] else 0
        end = int(parts[1]) if (len(parts) > 1 and parts[1]) else file_size - 1

        # Clamp
        if end >= file_size:
            end = file_size - 1
        if start > end:
            raise HTTPException(416, "Range not satisfiable")

    content_length = (end - start + 1) if end is not None else None

    # ── Response headers ────────────────────────────────────
    headers = {
        "Content-Type": "video/mp4",
        "Accept-Ranges": "bytes",
        "Content-Disposition": "inline",
        "X-Content-Type-Options": "nosniff",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Stream-Method": "mtproto",  # Debug: confirm no Bot API
    }

    if file_size > 0 and content_length:
        headers["Content-Length"] = str(content_length)
    if is_range and file_size > 0:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

    status_code = 206 if is_range else 200

    # ── Stream via MTProto ──────────────────────────────────
    async def generate():
        try:
            async for chunk in streamer.stream(
                file_id_str=file_id,
                offset=start,
                end=end,
            ):
                yield chunk
        except Exception as e:
            logger.error(f"Stream error for {file_id[:20]}...: {e}")

    return StreamingResponse(
        generate(),
        status_code=status_code,
        headers=headers,
        media_type="video/mp4",
    )


@app.head("/stream/{file_id}")
async def stream_head(
    file_id: str,
    token: str = "",
    expires: int = 0,
    size: int = 0,
):
    """Handle HEAD request — browsers send this before Range requests."""
    if not _validate_stream_token(file_id, token, expires, size):
        raise HTTPException(403, "Invalid token")

    file_size = size
    if not file_size:
        video = get_video_by_file_id(file_id)
        if video:
            file_size = video.get("file_size", 0) or 0

    headers = {
        "Content-Type": "video/mp4",
        "Accept-Ranges": "bytes",
        "Content-Disposition": "inline",
        "X-Stream-Method": "mtproto",
    }
    if file_size > 0:
        headers["Content-Length"] = str(file_size)

    return JSONResponse(content=None, headers=headers)


# ══════════════════════════════════════════════════════════════
# PLAYER PAGE — Full HTML5 video player
# ══════════════════════════════════════════════════════════════

_PLAYER_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} — Stream Player</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    background: #0a0a0f;
    color: #e0e0e0;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
  }}
  .player-wrapper {{
    width: 100%;
    max-width: 960px;
    margin: 20px auto;
    padding: 0 16px;
  }}
  .player-container {{
    position: relative;
    width: 100%;
    background: #000;
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 8px 32px rgba(0,0,0,0.6);
  }}
  video {{
    width: 100%;
    display: block;
    max-height: 80vh;
  }}
  .info {{
    padding: 20px 0;
    text-align: center;
  }}
  .info h1 {{
    font-size: 1.2rem;
    font-weight: 600;
    color: #fff;
    margin-bottom: 8px;
  }}
  .info .meta {{
    font-size: 0.85rem;
    color: #888;
  }}
  .badge {{
    display: inline-block;
    background: linear-gradient(135deg, #667eea, #764ba2);
    color: #fff;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.75rem;
    margin-left: 6px;
  }}
  .branding {{
    margin-top: auto;
    padding: 20px;
    text-align: center;
    color: #444;
    font-size: 0.75rem;
  }}
</style>
</head>
<body>
  <div class="player-wrapper">
    <div class="player-container">
      <video id="videoPlayer" controls autoplay playsinline
             preload="auto" controlsList="nodownload">
        <source src="{stream_url}" type="video/mp4">
        Your browser does not support HTML5 video.
      </video>
    </div>
    <div class="info">
      <h1>{title}</h1>
      <p class="meta">
        {quality} <span class="badge">MTProto</span>
        {size_info}
      </p>
    </div>
  </div>
  <div class="branding">Powered by TG Stream Server</div>
  <script>
    // Disable right-click on video to prevent easy downloading
    document.getElementById('videoPlayer').addEventListener('contextmenu', e => e.preventDefault());
  </script>
</body>
</html>
"""

@app.get("/player/{file_id}", response_class=HTMLResponse)
async def player_page(file_id: str, pt: str = ""):
    """
    Full HTML5 video player page.
    
    URL: /player/{file_id}?pt=PAGE_TOKEN
    Bot generates this link when a video is saved.
    """
    # Validate page token
    if not pt or not _validate_page_token(file_id, pt):
        raise HTTPException(403, "Invalid or missing page token")

    # Look up video metadata
    video = get_video_by_file_id(file_id)
    file_size = 0
    title = "Video Stream"
    quality = ""
    size_info = ""

    if video:
        file_size = video.get("file_size", 0) or 0
        title = video.get("file_name", "") or video.get("caption", "") or "Video Stream"
        quality = video.get("quality", "") or ""
        if file_size > 0:
            size_info = f"• {_format_size(file_size)}"

    # Generate time-limited stream URL for the <video> src
    stream_url = _make_stream_url(file_id, file_size)

    html = _PLAYER_HTML.format(
        title=title,
        stream_url=stream_url,
        quality=quality,
        size_info=size_info,
    )
    return HTMLResponse(content=html)


# ══════════════════════════════════════════════════════════════
# EMBED PAGE — Minimal iFrame player
# ══════════════════════════════════════════════════════════════

_EMBED_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Embed Player</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  html, body {{ width: 100%; height: 100%; overflow: hidden; background: #000; }}
  video {{ width: 100%; height: 100%; object-fit: contain; }}
</style>
</head>
<body>
  <video id="vp" controls autoplay playsinline preload="auto" controlsList="nodownload">
    <source src="{stream_url}" type="video/mp4">
  </video>
  <script>
    document.getElementById('vp').addEventListener('contextmenu', e => e.preventDefault());
  </script>
</body>
</html>
"""

@app.get("/embed/{file_id}", response_class=HTMLResponse)
async def embed_page(file_id: str, pt: str = ""):
    """
    Minimal iFrame-embeddable video player.
    
    URL: /embed/{file_id}?pt=PAGE_TOKEN
    Use in iframe: <iframe src="URL" allowfullscreen></iframe>
    """
    if not pt or not _validate_page_token(file_id, pt):
        raise HTTPException(403, "Invalid or missing page token")

    video = get_video_by_file_id(file_id)
    file_size = video.get("file_size", 0) if video else 0
    stream_url = _make_stream_url(file_id, file_size or 0)

    return HTMLResponse(content=_EMBED_HTML.format(stream_url=stream_url))


# ══════════════════════════════════════════════════════════════
# HEALTH CHECK & DEBUG
# ══════════════════════════════════════════════════════════════

@app.get("/health")
async def health_check():
    """Health check — verify MTProto connection status."""
    connected = tg_client.is_connected if tg_client else False

    return JSONResponse({
        "status": "ok" if connected else "degraded",
        "telegram": "connected" if connected else "disconnected",
        "protocol": "MTProto",
        "bot_api_used": False,
        "twenty_mb_limit": "BYPASSED",
        "max_file_size": "4GB (Telegram limit)",
        "chunk_size": f"{CHUNK_SIZE // 1024}KB",
    })


@app.get("/test/{file_id}")
async def test_file_id(file_id: str, key: str = ""):
    """Debug: test if a file_id is accessible via MTProto.
    
    Requires the STREAM_SECRET as ?key= parameter for security.
    """
    if not hmac.compare_digest(key, STREAM_SECRET):
        raise HTTPException(403, "Invalid key")

    if not streamer:
        return JSONResponse({"error": "Streamer not initialized"}, 503)

    result = await streamer.validate_file_id(file_id)
    return JSONResponse(result)


# ══════════════════════════════════════════════════════════════
# BOT HANDLERS — Auto-detect videos from channel
# ══════════════════════════════════════════════════════════════

def _detect_quality(w: int, h: int) -> str:
    """Auto-detect video quality from resolution."""
    res = min(w, h) if w > 0 and h > 0 else max(w, h)
    if res >= 2160:
        return "2160p/4K"
    if res >= 1440:
        return "1440p"
    if res >= 1080:
        return "1080p"
    if res >= 720:
        return "720p"
    return "480p"


def _format_size(size_bytes: int) -> str:
    """Format bytes to human readable."""
    if size_bytes >= 1073741824:
        return f"{size_bytes / 1073741824:.2f} GB"
    if size_bytes >= 1048576:
        return f"{size_bytes / 1048576:.2f} MB"
    return f"{size_bytes / 1024:.2f} KB"


def _register_bot_handlers():
    """Register Pyrogram message handlers for the bot."""

    # ── Handle videos from channel posts ─────────────────────
    @tg_client.on_message(filters.channel & (filters.video | filters.document))
    async def on_channel_video(_client: Client, message: Message):
        """Auto-save videos posted to channel."""
        video = message.video
        doc = message.document

        # Determine if it's a video
        media = None
        if video:
            media = video
        elif doc and (doc.mime_type or "").startswith("video/"):
            media = doc
        else:
            return

        data = {
            "file_id": media.file_id,
            "file_unique_id": media.file_unique_id,
            "file_size": media.file_size or 0,
            "duration": getattr(media, "duration", 0) or 0,
            "width": getattr(media, "width", 0) or 0,
            "height": getattr(media, "height", 0) or 0,
            "file_name": getattr(media, "file_name", "") or "",
            "mime_type": media.mime_type or "video/mp4",
            "caption": message.caption or "",
            "message_id": message.id,
            "channel_id": message.chat.id,
            "quality": _detect_quality(
                getattr(media, "width", 0) or 0,
                getattr(media, "height", 0) or 0,
            ),
        }

        try:
            save_video(data)
            size_str = _format_size(media.file_size or 0)
            logger.info(
                f"📹 Channel video saved: {data['file_name'] or media.file_unique_id} "
                f"({size_str}, {data['quality']})"
            )
        except Exception as e:
            logger.error(f"Failed to save channel video: {e}")

    # ── Handle videos sent directly to bot ───────────────────
    @tg_client.on_message(filters.private & (filters.video | filters.document))
    async def on_private_video(_client: Client, message: Message):
        """Process videos forwarded/sent directly to bot."""
        video = message.video
        doc = message.document

        media = None
        if video:
            media = video
        elif doc and (doc.mime_type or "").startswith("video/"):
            media = doc
        else:
            return

        data = {
            "file_id": media.file_id,
            "file_unique_id": media.file_unique_id,
            "file_size": media.file_size or 0,
            "duration": getattr(media, "duration", 0) or 0,
            "width": getattr(media, "width", 0) or 0,
            "height": getattr(media, "height", 0) or 0,
            "file_name": getattr(media, "file_name", "") or "",
            "mime_type": media.mime_type or "video/mp4",
            "caption": message.caption or "",
            "message_id": message.id,
            "channel_id": message.chat.id,
            "quality": _detect_quality(
                getattr(media, "width", 0) or 0,
                getattr(media, "height", 0) or 0,
            ),
        }

        try:
            save_video(data)
            size_str = _format_size(media.file_size or 0)
            file_size = media.file_size or 0

            # Generate all links
            links = _generate_links(media.file_id, file_size)

            await message.reply_text(
                f"✅ **Video Saved to Database!**\n\n"
                f"📝 **File:** {data['file_name'] or 'N/A'}\n"
                f"📐 **Resolution:** {data['width']}×{data['height']} ({data['quality']})\n"
                f"⏱ **Duration:** {data['duration']}s\n"
                f"💾 **Size:** {size_str}\n\n"
                f"🔑 **File ID:**\n`{media.file_id}`\n\n"
                f"━━━━━ 🔗 **Stream Links** ━━━━━\n\n"
                f"▶️ **Player Page:**\n{links['player']}\n\n"
                f"📺 **Direct Stream:**\n{links['stream']}\n\n"
                f"🖼 **iFrame Embed:**\n`<iframe src=\"{links['embed']}\" width=\"720\" height=\"405\" allowfullscreen></iframe>`\n\n"
                f"🌐 **HTML5 Video:**\n`<video src=\"{links['stream']}\" controls></video>`\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"⚡ All links use **MTProto** streaming\n"
                f"(NO 20MB limit — full file accessible).",
                quote=True,
                disable_web_page_preview=True,
            )
        except Exception as e:
            logger.error(f"Failed to process private video: {e}")
            await message.reply_text(f"❌ Error: {e}", quote=True)

    # ── Bot commands ─────────────────────────────────────────
    @tg_client.on_message(filters.private & filters.command("start"))
    async def cmd_start(_client: Client, message: Message):
        await message.reply_text(
            "🎬 **TG Video Stream Bot**\n\n"
            "Send me a video or forward from your channel.\n"
            "I'll extract the `file_id` and save it to the database.\n\n"
            "**How it works:**\n"
            "1. Upload video to your Telegram channel\n"
            "2. Forward it to me (or I auto-detect from channel)\n"
            "3. I save `file_id` + metadata to MySQL\n"
            "4. Your website streams it via MTProto\n\n"
            "⚡ **No 20MB limit** — Videos up to 4GB!\n\n"
            "**Commands:**\n"
            "/start — This message\n"
            "/status — Server status\n"
            "/test `file_id` — Test a file_id",
        )

    @tg_client.on_message(filters.private & filters.command("status"))
    async def cmd_status(_client: Client, message: Message):
        connected = tg_client.is_connected
        await message.reply_text(
            f"📊 **Server Status**\n\n"
            f"🔌 MTProto: {'✅ Connected' if connected else '❌ Disconnected'}\n"
            f"📦 Chunk Size: {CHUNK_SIZE // 1024}KB\n"
            f"🔓 20MB Limit: **BYPASSED**\n"
            f"📂 Max File Size: 4GB\n"
            f"🌐 Stream Port: {STREAM_PORT}",
        )

    @tg_client.on_message(filters.private & filters.command("test"))
    async def cmd_test(_client: Client, message: Message):
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await message.reply_text(
                "Usage: /test `<file_id>`\n\n"
                "Tests if the file_id is accessible via MTProto.",
            )
            return

        test_fid = parts[1].strip()
        msg = await message.reply_text("🔍 Testing file_id via MTProto...")

        if streamer:
            result = await streamer.validate_file_id(test_fid)
            if result.get("valid"):
                await msg.edit_text(
                    f"✅ **File ID is VALID!**\n\n"
                    f"DC: {result.get('dc_id', '?')}\n"
                    f"First chunk: {result.get('first_chunk_size', 0)} bytes\n"
                    f"Complete in 1 chunk: {'Yes' if result.get('is_complete') else 'No (large file)'}\n\n"
                    f"⚡ This file can be streamed via MTProto.",
                )
            else:
                await msg.edit_text(
                    f"❌ **File ID is INVALID**\n\n"
                    f"Error: {result.get('error', 'Unknown')}\n\n"
                    f"Make sure the file_id belongs to YOUR bot.",
                )
        else:
            await msg.edit_text("❌ Streamer not initialized.")

    logger.info("Bot handlers registered.")


# ══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server:app",
        host=STREAM_HOST,
        port=STREAM_PORT,
        log_level="info",
        access_log=True,
    )
