"""
UNO net-chips history — a separate SQLite file (config.UNO_STATS_DB_PATH)
that backs the /uno stats "Graph" button.

One row per player per settled round: (ts, user_id, net_chips), where
net_chips is the player's ABSOLUTE cumulative net (chips_won - chips_lost)
right after that round — the same shape as poker's historical CSV, just
written live by the bot instead of extracted from backups.

Everything here is best-effort from the game's point of view: callers in
database.py wrap writes in try/except so a stats-db problem can never
block a round settlement.

On first use with an empty history table it backfills itself from the main
UNO db's round_log_players, so existing rounds show up in graphs too.
"""
import asyncio
import logging
import time
from datetime import datetime, timezone

import aiosqlite
import config

log = logging.getLogger("uno")

DB_PATH = config.UNO_STATS_DB_PATH

_db: aiosqlite.Connection | None = None
_init_lock = asyncio.Lock()
_write_lock = asyncio.Lock()


async def _get_db() -> aiosqlite.Connection:
    global _db
    if _db is not None:
        return _db
    async with _init_lock:
        if _db is not None:
            return _db
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(DB_PATH, timeout=30)
        await conn.execute("PRAGMA busy_timeout=10000")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS net_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ts         INTEGER NOT NULL,
                user_id    INTEGER NOT NULL,
                net_chips  INTEGER NOT NULL,
                round_uuid TEXT
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_net_history_user_ts ON net_history(user_id, ts)")
        await conn.commit()
        async with conn.execute("SELECT 1 FROM net_history LIMIT 1") as c:
            empty = await c.fetchone() is None
        if empty:
            try:
                await _backfill(conn)
            except Exception:
                log.exception("UNO net history backfill failed (graphs will start from new rounds)")
                await conn.rollback()
        _db = conn
    return _db


async def _backfill(conn: aiosqlite.Connection):
    """Rebuild history from the main db's per-round log, then add one final
    point for anyone whose current stats net differs (e.g. /unoadmin setstat)."""
    from . import database as main_db  # lazy: database.py imports this module
    mdb = await main_db._get_db()
    async with mdb.execute("""
        SELECT p.user_id, p.net, r.ts, r.round_uuid
        FROM round_log_players p JOIN round_log r ON r.id = p.round_id
        ORDER BY r.ts, r.id
    """) as c:
        rounds = await c.fetchall()
    cum: dict[int, int] = {}
    out = []
    for uid, net, ts_iso, ruuid in rounds:
        ts = int(datetime.fromisoformat(ts_iso).replace(tzinfo=timezone.utc).timestamp())
        if uid not in cum:
            cum[uid] = 0
            out.append((ts - 1, uid, 0, None))  # zero baseline so a first round already graphs
        cum[uid] += net
        out.append((ts, uid, cum[uid], ruuid))
    now = int(time.time())
    async with mdb.execute("SELECT user_id, chips_won - chips_lost FROM stats") as c:
        for uid, net in await c.fetchall():
            if cum.get(uid, 0) != net:
                out.append((now, uid, net, None))
    if out:
        await conn.executemany(
            "INSERT INTO net_history (ts, user_id, net_chips, round_uuid) VALUES (?, ?, ?, ?)", out)
        await conn.commit()


async def record_round(rows: list[tuple[int, int, int]], round_uuid: str | None = None):
    """rows: [(user_id, net_chips_after_round, round_delta)]. Call once per
    settled round, after the main db has committed."""
    if not rows:
        return
    conn = await _get_db()
    now = int(time.time())
    async with _write_lock:
        for uid, net_after, delta in rows:
            async with conn.execute("SELECT 1 FROM net_history WHERE user_id=? LIMIT 1", (uid,)) as c:
                first = await c.fetchone() is None
            if first:
                await conn.execute(
                    "INSERT INTO net_history (ts, user_id, net_chips) VALUES (?, ?, ?)",
                    (now - 1, uid, net_after - delta))
            await conn.execute(
                "INSERT INTO net_history (ts, user_id, net_chips, round_uuid) VALUES (?, ?, ?, ?)",
                (now, uid, net_after, round_uuid))
        await conn.commit()


async def record_absolute(user_id: int, net_chips: int):
    """Out-of-round change to someone's net (setstat, removestats)."""
    conn = await _get_db()
    async with _write_lock:
        await conn.execute("INSERT INTO net_history (ts, user_id, net_chips) VALUES (?, ?, ?)",
                           (int(time.time()), user_id, net_chips))
        await conn.commit()


async def clear_all():
    conn = await _get_db()
    async with _write_lock:
        await conn.execute("DELETE FROM net_history")
        await conn.commit()


async def get_history(user_id: int, start_ts: int | None = None, end_ts: int | None = None) -> list[dict]:
    """[{ts, net_chips}, ...] ascending, same contract as poker's get_history,
    but shaped as a step function so the graph is flat while a player is idle
    instead of sloping between rounds:
      - before every change, a point is added 1s earlier at the previous value
      - the line is extended flat to end_ts (capped at "now") from the last point
    If start_ts is given, a baseline point at start_ts (last known value
    before it) is prepended. Never raises — returns [] on any failure."""
    try:
        conn = await _get_db()
        async with conn.execute(
                "SELECT ts, net_chips FROM net_history WHERE user_id=? ORDER BY ts, id", (user_id,)) as c:
            raw = [(r[0], r[1]) for r in await c.fetchall()]
        if not raw:
            return []

        rows: list[tuple[int, int]] = []
        for ts, n in raw:
            if rows and n != rows[-1][1] and ts - 1 > rows[-1][0]:
                rows.append((ts - 1, rows[-1][1]))
            rows.append((ts, n))

        now = int(time.time())
        tail_ts = now if end_ts is None else min(end_ts, now)
        if tail_ts > rows[-1][0]:
            rows.append((tail_ts, rows[-1][1]))

        baseline = None
        if start_ts is not None:
            before = [r for r in rows if r[0] < start_ts]
            if before:
                baseline = {"ts": start_ts, "net_chips": before[-1][1]}
        hist = [{"ts": ts, "net_chips": n} for ts, n in rows
                if (start_ts is None or ts >= start_ts) and (end_ts is None or ts <= end_ts)]
        if baseline:
            hist.insert(0, baseline)
        return hist
    except Exception:
        log.exception("UNO net history read failed")
        return []