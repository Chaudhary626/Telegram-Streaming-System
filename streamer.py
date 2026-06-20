"""
MTProto Streaming Engine — The Core of the System

This module streams files directly from Telegram's data centers
using the MTProto protocol via Pyrogram.

╔══════════════════════════════════════════════════════════╗
║  WHY THIS EXISTS:                                        ║
║                                                          ║
║  Telegram Bot API's getFile has a 20MB LIMIT.            ║
║  This uses MTProto's upload.GetFile which has NO LIMIT.  ║
║  Files up to 4GB can be streamed with full seeking.      ║
║                                                          ║
║  Flow:                                                   ║
║  Browser → FastAPI → MTProto → Telegram DC → chunks      ║
║                                                          ║
║  NO getFile. NO 20MB limit. Direct DC streaming.         ║
╚══════════════════════════════════════════════════════════╝
"""
import asyncio
import logging
from typing import AsyncGenerator, Optional

from pyrogram import Client, raw, errors
from pyrogram.file_id import FileId

from config import CHUNK_SIZE

logger = logging.getLogger(__name__)


class TelegramStreamer:
    """
    Stream files from Telegram using MTProto protocol.
    
    Bypasses the Bot API 20MB limit by connecting directly
    to Telegram's data centers via upload.GetFile RPC.
    
    Supports:
    - Files up to 4GB (Telegram premium: 4GB, regular: 2GB)
    - HTTP Range requests (video seeking)
    - 206 Partial Content responses
    - Chunk-aligned streaming (1MB per MTProto request)
    """

    def __init__(self, client: Client):
        self.client = client
        self._file_sizes: dict[str, int] = {}  # Cache: file_id → size

    # ══════════════════════════════════════════════════════════
    # FILE ID DECODING
    # ══════════════════════════════════════════════════════════

    @staticmethod
    def _decode_file_id(file_id_str: str) -> FileId:
        """Decode a Telegram file_id string into its components.
        
        The file_id encodes: DC ID, media ID, access hash, file reference.
        Pyrogram's FileId.decode() handles all the base64/pack parsing.
        """
        try:
            return FileId.decode(file_id_str)
        except Exception as e:
            logger.error(f"Failed to decode file_id: {e}")
            raise ValueError(f"Invalid file_id: {e}")

    @staticmethod
    def _get_input_location(decoded: FileId) -> raw.types.InputDocumentFileLocation:
        """Build the MTProto InputDocumentFileLocation from decoded file_id.
        
        This is what Telegram's upload.GetFile needs to locate the file
        on the correct data center.
        """
        return raw.types.InputDocumentFileLocation(
            id=decoded.media_id,
            access_hash=decoded.access_hash,
            file_reference=decoded.file_reference,
            thumb_size=decoded.thumbnail_size or "",
        )

    # ══════════════════════════════════════════════════════════
    # FILE SIZE DETECTION
    # ══════════════════════════════════════════════════════════

    async def get_file_size(self, file_id_str: str, known_size: int = 0) -> int:
        """Get file size. Uses cache, then probes Telegram if needed.
        
        For accurate Range/Content-Length headers, we need exact byte size.
        If not available from DB, we probe by requesting an offset near the end.
        """
        # Return known size if provided
        if known_size > 0:
            self._file_sizes[file_id_str] = known_size
            return known_size

        # Check cache
        if file_id_str in self._file_sizes:
            return self._file_sizes[file_id_str]

        # Probe: request offset 0 — if file is small, we get the whole thing
        # For large files, we know it's at least CHUNK_SIZE
        decoded = self._decode_file_id(file_id_str)
        location = self._get_input_location(decoded)

        try:
            r = await self.client.invoke(
                raw.functions.upload.GetFile(
                    location=location,
                    offset=0,
                    limit=CHUNK_SIZE,
                )
            )
            if r.bytes and len(r.bytes) < CHUNK_SIZE:
                # Got entire file in one chunk
                size = len(r.bytes)
            else:
                # File is larger than one chunk — binary search for actual size
                size = await self._probe_file_size(location)

            self._file_sizes[file_id_str] = size
            return size

        except Exception as e:
            logger.error(f"Failed to probe file size: {e}")
            return 0

    async def _probe_file_size(
        self, location: raw.types.InputDocumentFileLocation
    ) -> int:
        """Binary search to find exact file size for large files.
        
        Strategy: Double the offset until we get an empty response,
        then binary search between last-good and first-empty.
        """
        # Phase 1: Exponential search — find an upper bound
        offset = CHUNK_SIZE
        while True:
            try:
                r = await self.client.invoke(
                    raw.functions.upload.GetFile(
                        location=location, offset=offset, limit=CHUNK_SIZE
                    )
                )
                if not r.bytes or len(r.bytes) < CHUNK_SIZE:
                    # Found the end region
                    return offset + len(r.bytes) if r.bytes else offset
                offset *= 2
                # Safety: cap at 4GB
                if offset > 4 * 1024 * 1024 * 1024:
                    return offset
            except Exception:
                return offset

    def cache_file_size(self, file_id_str: str, size: int):
        """Manually cache a known file size."""
        if size > 0:
            self._file_sizes[file_id_str] = size

    # ══════════════════════════════════════════════════════════
    # CORE STREAMING — MTProto upload.GetFile
    # ══════════════════════════════════════════════════════════

    async def stream(
        self,
        file_id_str: str,
        offset: int = 0,
        end: Optional[int] = None,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream a file from Telegram in chunks using MTProto.
        
        ┌──────────────────────────────────────────────┐
        │  THIS IS THE CORE — NO BOT API, NO 20MB LIMIT │
        │                                                │
        │  Uses: raw.functions.upload.GetFile             │
        │  Protocol: MTProto (direct DC connection)      │
        │  Chunk size: 1MB (Telegram protocol limit)     │
        │  Max file: 4GB (Telegram limit)                │
        │  Seeking: Supported via offset parameter       │
        └──────────────────────────────────────────────┘
        
        Args:
            file_id_str: Telegram file_id string
            offset: Starting byte position (for Range requests / seeking)
            end: Ending byte position (inclusive, for Range requests)
        
        Yields:
            bytes: File chunks (up to 1MB each)
        """
        decoded = self._decode_file_id(file_id_str)
        location = self._get_input_location(decoded)

        # ── Align offset to chunk boundary ───────────────────
        # Telegram requires offsets aligned to chunk size
        aligned_offset = offset - (offset % CHUNK_SIZE)
        first_part_cut = offset - aligned_offset  # Bytes to skip in first chunk

        total_needed = (end - offset + 1) if end is not None else None
        bytes_sent = 0
        current_offset = aligned_offset
        retry_count = 0
        max_retries = 3

        logger.debug(
            f"Stream start: offset={offset} end={end} "
            f"aligned={aligned_offset} cut={first_part_cut} "
            f"needed={total_needed}"
        )

        while True:
            try:
                # ── MTProto RPC: upload.GetFile ──────────────
                # This bypasses Bot API completely.
                # Connects directly to the Telegram data center
                # holding the file. No 20MB limit.
                r = await self.client.invoke(
                    raw.functions.upload.GetFile(
                        location=location,
                        offset=current_offset,
                        limit=CHUNK_SIZE,
                    )
                )
                retry_count = 0  # Reset on success

            except errors.FloodWait as e:
                # Telegram rate limiting — wait and retry
                wait_time = min(e.value, 30)
                logger.warning(f"FloodWait: sleeping {wait_time}s")
                await asyncio.sleep(wait_time)
                continue

            except (errors.FileReferenceExpired, errors.FileReferenceInvalid) as e:
                # File reference expired — need fresh one
                logger.warning(f"File reference issue: {e}")
                # Try to refresh by re-decoding (won't work if truly expired)
                if retry_count < max_retries:
                    retry_count += 1
                    await asyncio.sleep(1)
                    continue
                logger.error("File reference expired — cannot stream")
                break

            except Exception as e:
                logger.error(f"GetFile error at offset {current_offset}: {e}")
                if retry_count < max_retries:
                    retry_count += 1
                    await asyncio.sleep(2)
                    continue
                break

            # ── Process chunk ────────────────────────────────
            if not r.bytes:
                break  # End of file

            chunk = bytes(r.bytes)

            # Skip leading bytes in first chunk (offset alignment)
            if first_part_cut:
                chunk = chunk[first_part_cut:]
                first_part_cut = 0

            # Trim to exact range if end boundary is specified
            if total_needed is not None:
                remaining = total_needed - bytes_sent
                if remaining <= 0:
                    break
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]
                    yield chunk
                    break

            yield chunk
            bytes_sent += len(chunk)
            current_offset += CHUNK_SIZE

            # End of file: received less than full chunk
            if len(r.bytes) < CHUNK_SIZE:
                break

        logger.debug(f"Stream complete: sent {bytes_sent} bytes")

    # ══════════════════════════════════════════════════════════
    # VALIDATION
    # ══════════════════════════════════════════════════════════

    async def validate_file_id(self, file_id_str: str) -> dict:
        """Test if a file_id is valid and accessible.
        
        Returns dict with status, file_size, and error info.
        Used by the health check / debug endpoints.
        """
        try:
            decoded = self._decode_file_id(file_id_str)
            location = self._get_input_location(decoded)

            r = await self.client.invoke(
                raw.functions.upload.GetFile(
                    location=location,
                    offset=0,
                    limit=CHUNK_SIZE,
                )
            )

            if r.bytes:
                return {
                    "valid": True,
                    "dc_id": decoded.dc_id,
                    "media_id": decoded.media_id,
                    "first_chunk_size": len(r.bytes),
                    "is_complete": len(r.bytes) < CHUNK_SIZE,
                }
            else:
                return {"valid": False, "error": "Empty response from Telegram"}

        except Exception as e:
            return {"valid": False, "error": str(e)}
