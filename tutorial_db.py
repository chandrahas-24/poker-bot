import aiosqlite
import asyncio
from datetime import datetime

DB_PATH = "tutorial.db"
_db: aiosqlite.Connection | None = None
_write_lock = asyncio.Lock()


async def _get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        _db = await aiosqlite.connect(DB_PATH, timeout=30)
        await _db.execute("PRAGMA journal_mode=WAL")
        await _db.execute("PRAGMA synchronous=NORMAL")
    return _db


async def init_db():
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tutorial_completions (
                user_id INTEGER PRIMARY KEY,
                ts      TEXT NOT NULL
            )
        """)
        await db.commit()


async def has_completed_tutorial(user_id: int) -> bool:
    """Return True if this user has ever finished the tutorial."""
    db = await _get_db()
    async with db.execute(
            "SELECT 1 FROM tutorial_completions WHERE user_id = ?", (user_id,)
    ) as c:
        return (await c.fetchone()) is not None


async def mark_tutorial_complete(user_id: int) -> bool:
    """
    Record a tutorial completion.
    Returns True  — first completion (reward should be given).
    Returns False — already completed before (no reward).
    """
    db_conn = await _get_db()
    async with db_conn.execute(
            "SELECT 1 FROM tutorial_completions WHERE user_id = ?", (user_id,)
    ) as c:
        already = (await c.fetchone()) is not None
    if already:
        return False

    async with _write_lock:
        await db_conn.execute(
            "INSERT OR IGNORE INTO tutorial_completions (user_id, ts) VALUES (?, ?)",
            (user_id, datetime.utcnow().isoformat()),
        )
        await db_conn.commit()
    return True