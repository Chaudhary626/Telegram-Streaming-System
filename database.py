"""
Database operations for TG Stream Server.
Uses the SAME MySQL database as the PHP website.
"""
import pymysql
import logging
from contextlib import contextmanager
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

logger = logging.getLogger(__name__)


@contextmanager
def get_connection():
    """Get a MySQL connection with auto-close."""
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=10,
        read_timeout=30,
    )
    try:
        yield conn
    finally:
        conn.close()


def ensure_tables():
    """Create tg_videos table if it doesn't exist.
    
    This table stores metadata from the bot's auto-detection.
    The PHP website's streaming_sources table is also used.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tg_videos (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    file_id VARCHAR(512) NOT NULL,
                    file_unique_id VARCHAR(128) NOT NULL,
                    file_size BIGINT DEFAULT 0,
                    duration INT DEFAULT 0,
                    width INT DEFAULT 0,
                    height INT DEFAULT 0,
                    file_name VARCHAR(512) DEFAULT '',
                    mime_type VARCHAR(64) DEFAULT 'video/mp4',
                    caption TEXT,
                    message_id INT DEFAULT 0,
                    channel_id BIGINT DEFAULT 0,
                    quality VARCHAR(20) DEFAULT '720p',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_file_id (file_id(100)),
                    INDEX idx_unique_id (file_unique_id(100))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
    logger.info("Database tables verified.")


def save_video(data: dict) -> int:
    """Save or update video metadata from bot detection.
    
    Returns the inserted/updated row ID.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tg_videos 
                    (file_id, file_unique_id, file_size, duration, width, height,
                     file_name, mime_type, caption, message_id, channel_id, quality)
                VALUES 
                    (%(file_id)s, %(file_unique_id)s, %(file_size)s, %(duration)s, 
                     %(width)s, %(height)s, %(file_name)s, %(mime_type)s, 
                     %(caption)s, %(message_id)s, %(channel_id)s, %(quality)s)
                ON DUPLICATE KEY UPDATE
                    file_id = VALUES(file_id),
                    file_size = VALUES(file_size),
                    duration = VALUES(duration),
                    width = VALUES(width),
                    height = VALUES(height),
                    file_name = VALUES(file_name),
                    caption = VALUES(caption)
            """, data)
            return cur.lastrowid


def get_video_by_file_id(file_id: str) -> dict | None:
    """Look up video metadata by file_id.
    
    Checks both tg_videos AND streaming_sources tables.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # First check tg_videos (has exact byte size)
            cur.execute(
                "SELECT * FROM tg_videos WHERE file_id = %s LIMIT 1",
                (file_id,)
            )
            row = cur.fetchone()
            if row:
                return row

            # Fallback: check streaming_sources (PHP system)
            cur.execute(
                "SELECT telegram_file_id AS file_id, file_size_mb, duration_seconds AS duration "
                "FROM streaming_sources WHERE telegram_file_id = %s LIMIT 1",
                (file_id,)
            )
            row = cur.fetchone()
            if row:
                # Convert MB to bytes
                row["file_size"] = int(float(row.get("file_size_mb", 0)) * 1024 * 1024)
                return row

    return None


def get_all_videos(limit: int = 100) -> list:
    """Get all saved videos (for admin listing)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM tg_videos ORDER BY created_at DESC LIMIT %s",
                (limit,)
            )
            return cur.fetchall()
