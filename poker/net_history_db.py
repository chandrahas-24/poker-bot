"""
Poker net-chips history — a separate SQLite file (config.POKER_NET_HISTORY_DB_PATH)
backing the /poker stats "Graph" button.

One row per player per hand in which their net changed: (ts, user_id, net_chips),
where net_chips is the player's ABSOLUTE cumulative net (chips_won - chips_lost)
right after that hand.

Replaces the manually-maintained CSV: on first run with an empty table it is
seeded ONCE from config.NET_CHIPS_HISTORY_CSV_PATH (timestamp,user_id,net_chips),
unchanged consecutive values are dropped (get_history redraws flat stretches),
and one point at seed time is added for anyone whose current stats net differs
from their last CSV value. After that the CSV is never read again.

Writes are best-effort: callers wrap them in try/except so a problem here can
never affect hand recording.
"""
import asyncio
import csv
import logging
import os
import sqlite3
import time

import aiosqlite
import config

log = logging.getLogger("poker")

DB_PATH = config.POKER_NET_HISTORY_DB_PATH
CSV_PATH = config.NET_CHIPS_HISTORY_CSV_PATH
MAX_CSV_BYTES = 200 * 1024 * 1024   # same safety ceiling the old CSV reader had
MAX_POINTS = 2500                    # graph is downsampled to about this many raw points

_db: aiosqlite.Connection | None = None
_init_lock = asyncio.Lock()
_write_lock = asyncio.Lock()


def _init_and_seed_sync():
    """Create the table and, if empty, seed from the CSV. Plain sqlite3,
    run via asyncio.to_thread (the CSV can be ~20MB)."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=30)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("""
            CREATE TABLE IF NOT EXISTS net_history (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        INTEGER NOT NULL,
                user_id   INTEGER NOT NULL,
                net_chips INTEGER NOT NULL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_net_history_user_ts ON net_history(user_id, ts)")
        con.commit()
        if con.execute("SELECT 1 FROM net_history LIMIT 1").fetchone() is None:
            try:
                _seed(con)
            except Exception:
                log.exception("poker net history seed failed (graphs will start from new hands)")
                con.rollback()
    finally:
        con.close()


def _seed(con: sqlite3.Connection):
    imported = 0
    if os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) <= MAX_CSV_BYTES:
        with open(CSV_PATH, newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None) or []
            try:
                ts_i, uid_i, nc_i = header.index("timestamp"), header.index("user_id"), header.index("net_chips")
            except ValueError:
                ts_i = None
                log.error("poker net history: CSV is missing an expected header column")
            if ts_i is not None:
                batch = []
                for row in reader:
                    try:
                        batch.append((int(row[ts_i]), int(row[uid_i]), int(row[nc_i])))
                    except (IndexError, ValueError):
                        continue  # malformed manual rows are skipped
                    if len(batch) >= 50_000:
                        con.executemany("INSERT INTO net_history (ts, user_id, net_chips) VALUES (?, ?, ?)", batch)
                        imported += len(batch)
                        batch.clear()
                if batch:
                    con.executemany("INSERT INTO net_history (ts, user_id, net_chips) VALUES (?, ?, ?)", batch)
                    imported += len(batch)
        # Drop rows that repeat the previous value for that user (hourly flat
        # stretches). Order-independent: done in SQL over (user_id, ts).
        con.execute("""
            DELETE FROM net_history WHERE id IN (
                SELECT id FROM (
                    SELECT id, net_chips,
                           LAG(net_chips) OVER (PARTITION BY user_id ORDER BY ts, id) AS prev
                    FROM net_history
                ) WHERE prev IS NOT NULL AND prev = net_chips
            )
        """)
    # Catch up to the live stats table: one point now for anyone whose current
    # net differs from their last imported value.
    now = int(time.time())
    main = sqlite3.connect(config.DB_PATH, timeout=30)
    try:
        nets = main.execute("SELECT user_id, chips_won - chips_lost FROM stats").fetchall()
    finally:
        main.close()
    for uid, net in nets:
        last = con.execute("SELECT net_chips FROM net_history WHERE user_id=? ORDER BY ts DESC, id DESC LIMIT 1",
                           (uid,)).fetchone()
        if last is None or last[0] != net:
            con.execute("INSERT INTO net_history (ts, user_id, net_chips) VALUES (?, ?, ?)", (now, uid, net))
    con.commit()
    log.info("poker net history seeded: %d CSV rows read", imported)


async def _get_db() -> aiosqlite.Connection:
    global _db
    if _db is not None:
        return _db
    async with _init_lock:
        if _db is not None:
            return _db
        await asyncio.to_thread(_init_and_seed_sync)
        conn = await aiosqlite.connect(DB_PATH, timeout=30)
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA busy_timeout=10000")
        _db = conn
    return _db


async def init():
    """Open + seed now (called from poker.database.init_db at startup so the
    one-time CSV import doesn't land in the middle of a hand)."""
    await _get_db()


async def record(user_id: int, net_after: int, delta: int):
    """Call after a hand's stats commit, only when the player's net changed."""
    conn = await _get_db()
    now = int(time.time())
    async with _write_lock:
        async with conn.execute("SELECT 1 FROM net_history WHERE user_id=? LIMIT 1", (user_id,)) as c:
            first = await c.fetchone() is None
        if first:
            await conn.execute("INSERT INTO net_history (ts, user_id, net_chips) VALUES (?, ?, ?)",
                               (now - 1, user_id, net_after - delta))
        await conn.execute("INSERT INTO net_history (ts, user_id, net_chips) VALUES (?, ?, ?)",
                           (now, user_id, net_after))
        await conn.commit()


async def record_absolute(user_id: int, net_chips: int):
    """Out-of-hand change to someone's net (setstat, removestats)."""
    conn = await _get_db()
    async with _write_lock:
        await conn.execute("INSERT INTO net_history (ts, user_id, net_chips) VALUES (?, ?, ?)",
                           (int(time.time()), user_id, net_chips))
        await conn.commit()


async def get_history(user_id: int, start_ts: int | None = None, end_ts: int | None = None) -> list[dict]:
    """[{ts, net_chips}, ...] ascending, shaped as a step function so the
    graph is flat while a player is idle:
      - if start_ts is given, a baseline point at start_ts (last value before it)
      - long series are thinned to ~MAX_POINTS (keeps the last point)
      - before every change a point is added 1s earlier at the previous value
      - the line is extended flat to end_ts (capped at "now")
    Never raises — returns [] on any failure."""
    try:
        conn = await _get_db()
        async with conn.execute(
                "SELECT ts, net_chips FROM net_history WHERE user_id=? ORDER BY ts, id", (user_id,)) as c:
            raw = [(r[0], r[1]) for r in await c.fetchall()]
        if not raw:
            return []

        series = [r for r in raw if (start_ts is None or r[0] >= start_ts) and (end_ts is None or r[0] <= end_ts)]
        if start_ts is not None:
            before = [r for r in raw if r[0] < start_ts]
            if before:
                series.insert(0, (start_ts, before[-1][1]))
        if not series:
            return []

        if len(series) > MAX_POINTS:
            k = -(-len(series) // MAX_POINTS)
            last = series[-1]
            series = series[::k]
            if series[-1] != last:
                series.append(last)

        rows: list[tuple[int, int]] = []
        for ts, n in series:
            if rows and n != rows[-1][1] and ts - 1 > rows[-1][0]:
                rows.append((ts - 1, rows[-1][1]))
            rows.append((ts, n))

        now = int(time.time())
        tail_ts = now if end_ts is None else min(end_ts, now)
        if tail_ts > rows[-1][0]:
            rows.append((tail_ts, rows[-1][1]))
        return [{"ts": ts, "net_chips": n} for ts, n in rows]
    except Exception:
        log.exception("poker net history read failed")
        return []