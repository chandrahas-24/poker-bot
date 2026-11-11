import aiosqlite
import asyncio
import os
import logging
import shutil
import time
from datetime import datetime, timedelta
import config
from poker.net_history_db import shape_history

log = logging.getLogger("tournament")

DB_PATH = config.TOURNAMENT_DB_PATH
_db: aiosqlite.Connection | None = None
_write_lock = asyncio.Lock()

async def _get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        _db = await aiosqlite.connect(DB_PATH, timeout=30)
        await _db.execute("PRAGMA journal_mode=WAL")
        await _db.execute("PRAGMA synchronous=NORMAL")
        await _db.execute("PRAGMA busy_timeout=10000")
        await _db.execute("PRAGMA temp_store=MEMORY")
        _db.row_factory = aiosqlite.Row
    return _db

async def init_db():
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                balance INTEGER DEFAULT 0,
                hands_won INTEGER DEFAULT 0,
                hands_played INTEGER DEFAULT 0,
                registered_at TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chips_in_play (
                user_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                amount INTEGER NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS hand_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                winner_id INTEGER,
                winner_name TEXT,
                pot INTEGER NOT NULL,
                summary TEXT NOT NULL,
                ts TEXT NOT NULL
            )
        """)
        # Staff-editable tournament settings (key/value), e.g. blinds
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tourney_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)


        # Add the activity tracking columns safely
        try:
            await db.execute("ALTER TABLE players ADD COLUMN period_wagered INTEGER DEFAULT 0")
            await db.execute("ALTER TABLE players ADD COLUMN last_activity TEXT")
            await db.execute("ALTER TABLE players ADD COLUMN target_wager INTEGER")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise

        # 🛠️ VPIP Tracking Columns
        try:
            await db.execute("ALTER TABLE players ADD COLUMN vpip_count INTEGER DEFAULT 0")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise

        # Highlights columns (each in its own try so one existing column never skips the rest)
        for col in ("win_streak", "max_win_streak", "all_in_wins"):
            try:
                await db.execute(f"ALTER TABLE players ADD COLUMN {col} INTEGER DEFAULT 0")
            except aiosqlite.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    raise

        # Net-chips history (graph on the Highlights page)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS net_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                net_chips INTEGER NOT NULL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_net_history_user_ts ON net_history(user_id, ts)")

        await db.commit()

async def checkpoint():
    db = await _get_db()
    async with _write_lock:
        await db.execute("PRAGMA wal_checkpoint(TRUNCATE)")

# ── Settings ──

DEFAULT_SMALL_BLIND = 25
DEFAULT_BIG_BLIND = 50

async def get_blinds() -> tuple[int, int]:
    db = await _get_db()
    async with db.execute("SELECT key, value FROM tourney_settings WHERE key IN ('small_blind', 'big_blind')") as c:
        vals = {r[0]: int(r[1]) for r in await c.fetchall()}
    return vals.get('small_blind', DEFAULT_SMALL_BLIND), vals.get('big_blind', DEFAULT_BIG_BLIND)

async def set_blinds(small_blind: int, big_blind: int):
    db = await _get_db()
    async with _write_lock:
        await db.execute("INSERT OR REPLACE INTO tourney_settings (key, value) VALUES ('small_blind', ?)", (str(small_blind),))
        await db.execute("INSERT OR REPLACE INTO tourney_settings (key, value) VALUES ('big_blind', ?)", (str(big_blind),))
        await db.commit()

# ── Registration ──

async def is_registered(user_id: int) -> bool:
    db = await _get_db()
    async with db.execute("SELECT user_id FROM players WHERE user_id=?", (user_id,)) as c:
        row = await c.fetchone()
        return bool(row)


async def register_player(user_id: int, username: str) -> bool:
    if await is_registered(user_id):
        return False
    db = await _get_db()
    now = datetime.utcnow().isoformat()
    target = int(config.TOURNAMENT_STARTING_CHIPS * config.TOURNAMENT_ACTIVITY_QUOTA)

    async with _write_lock:
        await db.execute("""
            INSERT INTO players (user_id, username, balance, registered_at, last_activity, period_wagered, target_wager)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, username, config.TOURNAMENT_STARTING_CHIPS, now, now, 0, target))
        await db.commit()
    return True

# ── Chip Operations ──

async def get_balance(user_id: int) -> int:
    db = await _get_db()
    async with db.execute("SELECT balance FROM players WHERE user_id=?", (user_id,)) as c:
        row = await c.fetchone()
        return row[0] if row else 0

async def add_chips(user_id: int, username: str, amount: int) -> int:
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            UPDATE players SET username = ?, balance = balance + ? WHERE user_id = ?
        """, (username, amount, user_id))
        await db.commit()
    return await get_balance(user_id)

async def deduct_chips(user_id: int, amount: int) -> bool:
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE players SET balance = balance - ? WHERE user_id = ? AND balance >= ?",
            (amount, user_id, amount)
        )
        # 🛠️ FIXED: Pull changes() before committing
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            success = bool(row and row[0] > 0)
        await db.commit()
        return success

async def buy_in(user_id: int, amount: int) -> bool:
    success = await deduct_chips(user_id, amount)
    if success:
        # Assuming we need to track this player exists in chips_in_play
        # Typically the game engine handles chips_in_play marking, but we'll provide the helper
        return True
    return False

async def return_chips(user_id: int, amount: int):
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE players SET balance = balance + ? WHERE user_id = ?",
            (amount, user_id)
        )
        await db.commit()

async def mark_chips_in_play(user_id: int, username: str, amount: int):
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO chips_in_play (user_id, username, amount) VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                amount   = chips_in_play.amount + excluded.amount
        """, (user_id, username, amount))
        await db.commit()

async def clear_chips_in_play(user_id: int):
    db = await _get_db()
    async with _write_lock:
        await db.execute("DELETE FROM chips_in_play WHERE user_id=?", (user_id,))
        await db.commit()

async def sync_chips_in_play(player_chip_map: dict):
    """Batch update chips_in_play for all players in one transaction."""
    if not player_chip_map:
        return
    db = await _get_db()
    async with _write_lock:
        for user_id, total in player_chip_map.items():
            if total > 0:
                await db.execute(
                    "UPDATE chips_in_play SET amount=? WHERE user_id=?", (total, user_id)
                )
            else:
                await db.execute(
                    "DELETE FROM chips_in_play WHERE user_id=?", (user_id,)
                )
        await db.commit()

async def recover_chips_in_play() -> list[dict]:
    db = await _get_db()
    async with db.execute("SELECT * FROM chips_in_play") as c:
        rows = [dict(r) for r in await c.fetchall()]
    async with _write_lock:
        for r in rows:
            await db.execute(
                "UPDATE players SET balance = balance + ? WHERE user_id = ?",
                (r["amount"], r["user_id"])
            )
        await db.execute("DELETE FROM chips_in_play")
        await db.commit()
    return rows

# ── Hand Processing ──

async def process_hand_result(result, table_name: str):
    db = await _get_db()
    now = datetime.utcnow().isoformat()
    
    # Identify winner(s)
    winner_id = None
    winner_name = None
    if result.winners:
        winner_id = result.winners[0].user_id
        winner_name = result.winners[0].display_name

    async with _write_lock:
        # Record hand log
        await db.execute("""
            INSERT INTO hand_logs (table_name, winner_id, winner_name, pot, summary, ts)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (table_name, winner_id, winner_name, result.pot, result.summary, now))

        # Update stats
        for user_id in (result.chip_deltas or {}):
            won = any(w.user_id == user_id for w in result.winners)

            # Catch the VPIP flag from the engine (default to 0 if not present)
            vpip_inc = 1 if (hasattr(result, 'vpip_ids') and user_id in result.vpip_ids) else 0

            # Same definition as poker's record_hand_full
            all_in_win = 1 if (won and getattr(result, 'allin_user_ids', None) and user_id in result.allin_user_ids) else 0

            await db.execute("""
                        UPDATE players SET 
                        hands_played = hands_played + 1,
                        hands_won = hands_won + ?,
                        vpip_count = COALESCE(vpip_count, 0) + ?,
                        win_streak = CASE WHEN ? THEN COALESCE(win_streak, 0) + 1 ELSE 0 END,
                        max_win_streak = MAX(COALESCE(max_win_streak, 0),
                                             CASE WHEN ? THEN COALESCE(win_streak, 0) + 1 ELSE 0 END),
                        all_in_wins = COALESCE(all_in_wins, 0) + ?
                        WHERE user_id = ?
                    """, (1 if won else 0, vpip_inc, won, won, all_in_win, user_id))
        await db.commit()

async def log_hand(table_name: str, winner_id: int, winner_name: str, pot: int, summary: str):
    db = await _get_db()
    now = datetime.utcnow().isoformat()
    async with _write_lock:
        await db.execute("""
            INSERT INTO hand_logs (table_name, winner_id, winner_name, pot, summary, ts)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (table_name, winner_id, winner_name, pot, summary, now))
        await db.commit()

# ── Net-chips history (graph) ──

async def _net_of(db, user_id: int) -> int | None:
    async with db.execute("""
        SELECT p.balance + COALESCE(c.amount, 0)
        FROM players p LEFT JOIN chips_in_play c ON p.user_id = c.user_id
        WHERE p.user_id = ?
    """, (user_id,)) as c:
        row = await c.fetchone()
    return None if row is None else row[0] - config.TOURNAMENT_STARTING_CHIPS


async def record_net_history(deltas: dict):
    """Call after a hand's chips_in_play sync. deltas = {user_id: chip delta}; only
    changed nets are recorded. Same shape as poker's net_history_db.record
    (first point also gets a baseline 1s earlier). Best-effort — never affects the hand."""
    try:
        db = await _get_db()
        now = int(time.time())
        async with _write_lock:
            for uid, delta in deltas.items():
                if not delta:
                    continue
                net_after = await _net_of(db, uid)
                if net_after is None:
                    continue
                async with db.execute("SELECT 1 FROM net_history WHERE user_id=? LIMIT 1", (uid,)) as c:
                    first = await c.fetchone() is None
                if first:
                    await db.execute("INSERT INTO net_history (ts, user_id, net_chips) VALUES (?, ?, ?)",
                                     (now - 1, uid, net_after - delta))
                await db.execute("INSERT INTO net_history (ts, user_id, net_chips) VALUES (?, ?, ?)",
                                 (now, uid, net_after))
            await db.commit()
    except Exception:
        log.exception("tournament net history write failed")


async def record_net_absolute(user_id: int):
    """Out-of-hand change to someone's net (addchips/removechips, wipe penalties).
    Records their current net. Best-effort."""
    try:
        db = await _get_db()
        async with _write_lock:
            net = await _net_of(db, user_id)
            if net is not None:
                await db.execute("INSERT INTO net_history (ts, user_id, net_chips) VALUES (?, ?, ?)",
                                 (int(time.time()), user_id, net))
                await db.commit()
    except Exception:
        log.exception("tournament net history write failed")


async def get_history(user_id: int, start_ts: int | None = None, end_ts: int | None = None) -> list[dict]:
    """[{ts, net_chips}, ...] — same contract as poker's get_history. Never raises."""
    try:
        db = await _get_db()
        async with db.execute(
                "SELECT ts, net_chips FROM net_history WHERE user_id=? ORDER BY ts, id", (user_id,)) as c:
            raw = [(r[0], r[1]) for r in await c.fetchall()]
        return shape_history(raw, start_ts, end_ts)
    except Exception:
        log.exception("tournament net history read failed")
        return []


# ── Stats & Leaderboards ──

async def get_player_stats(user_id: int) -> dict | None:
    db = await _get_db()
    async with db.execute("""
        SELECT p.*, COALESCE(c.amount, 0) as chips_in_play
        FROM players p
        LEFT JOIN chips_in_play c ON p.user_id = c.user_id
        WHERE p.user_id = ?
    """, (user_id,)) as c:
        row = await c.fetchone()
        if not row:
            return None
        stats = dict(row)
        stats['total_chips'] = stats['balance'] + stats['chips_in_play']
        stats['net_chips'] = stats['total_chips'] - config.TOURNAMENT_STARTING_CHIPS
        stats['wallet'] = stats['balance']  # same key poker/uno stats use (leaderboard image)

        # Calculate win%
        hands_played = stats['hands_played']
        hands_won = stats['hands_won']
        stats['win_rate'] = (hands_won / hands_played * 100) if hands_played > 0 else 0

        # Calculate VPIP% using the dedicated denominator
        vpip_count = stats.get('vpip_count', 0)
        vpip_hands = stats.get('hands_played', 0)
        stats['vpip_rate'] = (vpip_count / vpip_hands * 100) if vpip_hands > 0 else 0
        
        # Get rank
        stats['rank'] = await get_player_rank(user_id)
        return stats

async def get_player_rank(user_id: int) -> int | None:
    db = await _get_db()
    
    # First get the user's total chips
    async with db.execute("""
        SELECT (p.balance + COALESCE(c.amount, 0)) as total_chips
        FROM players p
        LEFT JOIN chips_in_play c ON p.user_id = c.user_id
        WHERE p.user_id = ?
    """, (user_id,)) as c:
        row = await c.fetchone()
        if not row:
            return None
        user_total = row[0]

    # Count how many players have more chips
    async with db.execute("""
        SELECT COUNT(*) + 1 FROM (
            SELECT p.user_id, (p.balance + COALESCE(c.amount, 0)) as total_chips
            FROM players p
            LEFT JOIN chips_in_play c ON p.user_id = c.user_id
        ) WHERE total_chips > ?
    """, (user_total,)) as c:
        row = await c.fetchone()
        return row[0] if row else None

async def get_individual_leaderboard(limit: int = 10) -> list[dict]:
    db = await _get_db()
    async with db.execute("""
        SELECT p.user_id, p.username, p.balance, p.hands_played, p.hands_won, (p.balance + COALESCE(c.amount, 0)) as total_chips
        FROM players p
        LEFT JOIN chips_in_play c ON p.user_id = c.user_id
        ORDER BY total_chips DESC
        LIMIT ?
    """, (limit,)) as c:
        rows = [dict(r) for r in await c.fetchall()]
    for r in rows:
        r['net_chips'] = r['total_chips'] - config.TOURNAMENT_STARTING_CHIPS
    return rows

async def log_period_wagers(wagers: dict):
    """Adds chips wagered to the daily tracker and stamps last activity."""
    db = await _get_db()
    now = datetime.utcnow().isoformat()
    async with _write_lock:
        for user_id, amount in wagers.items():
            if amount > 0:
                await db.execute("""
                    UPDATE players 
                    SET period_wagered = COALESCE(period_wagered, 0) + ?,
                        last_activity = ?
                    WHERE user_id = ?
                """, (amount, now, user_id))
        await db.commit()


def _utcnow_naive() -> datetime:
    return datetime.utcnow()


def _in_grace(registered_at: str | None, at: datetime) -> bool:
    """True if a wipe happening at `at` falls within the grace period after registering."""
    if not registered_at:
        return False
    try:
        reg = datetime.fromisoformat(registered_at).replace(tzinfo=None)
    except (ValueError, TypeError):
        return False
    return at < reg + timedelta(hours=config.TOURNAMENT_GRACE_HOURS)


async def _get_totals(db) -> list[dict]:
    """All players with total chips (wallet + table stack), richest first. Ties keep a stable order."""
    async with db.execute("""
        SELECT p.user_id, p.username, p.balance,
               COALESCE(c.amount, 0) AS chips_in_play,
               (p.balance + COALESCE(c.amount, 0)) AS total_chips,
               COALESCE(p.period_wagered, 0) AS wagered,
               p.target_wager, p.registered_at
        FROM players p
        LEFT JOIN chips_in_play c ON p.user_id = c.user_id
        ORDER BY total_chips DESC, p.user_id ASC
    """) as c:
        return [dict(r) for r in await c.fetchall()]


def _top_n(rows: list[dict], n: int = 10) -> list[dict]:
    """Top n by total chips, including EVERYONE tied with the nth place (no user-id tiebreak)."""
    if len(rows) <= n:
        return list(rows)
    cutoff = rows[n - 1]['total_chips']
    return [r for r in rows if r['total_chips'] >= cutoff]


async def get_top_ids() -> set[int]:
    db = await _get_db()
    return {r['user_id'] for r in _top_n(await _get_totals(db))}


async def get_players_below_quota(at: datetime) -> list[dict]:
    """Top-10 players (ties included) who are not in grace at `at` and are under their quota.
    Used by reminders; `at` should be the time of the next wipe."""
    db = await _get_db()
    out = []
    for r in _top_n(await _get_totals(db)):
        if _in_grace(r['registered_at'], at):
            continue
        target = r['target_wager'] if r['target_wager'] is not None else int(r['total_chips'] * config.TOURNAMENT_ACTIVITY_QUOTA)
        if r['wagered'] < target:
            out.append({
                "user_id": r['user_id'], "username": r['username'],
                "total_chips": r['total_chips'], "target": target, "wagered": r['wagered'],
                "shortfall": target - r['wagered'],
                "penalty": int(r['total_chips'] * config.TOURNAMENT_PENALTY_PCT),
            })
    return out


async def process_daily_tourney_check(dry_run: bool = False) -> tuple[list[dict], int]:
    """
    Daily wipe. Every top-10 player (ties included) outside their grace period who wagered less than
    their target loses TOURNAMENT_PENALTY_PCT of total chips: wallet first, remainder is returned as
    `stack_penalty` for the caller to take from the table stack.
    Then (unless dry_run) the daily tracker resets and targets are recomputed from post-penalty totals.
    Returns (penalties, number_of_players_checked).
    """
    db = await _get_db()
    now_dt = _utcnow_naive()
    penalties, checked = [], 0

    async with _write_lock:
        rows = await _get_totals(db)
        for r in _top_n(rows):
            if _in_grace(r['registered_at'], now_dt):
                continue
            checked += 1
            total = r['total_chips']
            target = r['target_wager'] if r['target_wager'] is not None else int(total * config.TOURNAMENT_ACTIVITY_QUOTA)
            if r['wagered'] >= target:
                continue

            penalty = int(total * config.TOURNAMENT_PENALTY_PCT)
            wallet_part = min(r['balance'], penalty)
            stack_part = min(r['chips_in_play'], penalty - wallet_part)
            if not dry_run and wallet_part:
                await db.execute("UPDATE players SET balance = balance - ? WHERE user_id = ?",
                                 (wallet_part, r['user_id']))
            penalties.append({
                "user_id": r['user_id'], "username": r['username'],
                "target": target, "actual": r['wagered'],
                "penalty": wallet_part + stack_part,
                "wallet_part": wallet_part, "stack_penalty": stack_part,
                "total_before": total, "total_after": total - wallet_part - stack_part,
            })

        if not dry_run:
            pen_after = {p['user_id']: p['total_after'] for p in penalties}
            await db.execute("UPDATE players SET period_wagered = 0")
            for r in rows:
                total_after = pen_after.get(r['user_id'], r['total_chips'])
                await db.execute("UPDATE players SET target_wager = ? WHERE user_id = ?",
                                 (int(total_after * config.TOURNAMENT_ACTIVITY_QUOTA), r['user_id']))
            await db.commit()

    return penalties, checked


async def reduce_chips_in_play(user_id: int, amount: int):
    """Removes chips from a player's recorded table stack (penalty fallback when no live table holds them)."""
    db = await _get_db()
    async with _write_lock:
        await db.execute("UPDATE chips_in_play SET amount = MAX(0, amount - ?) WHERE user_id = ?", (amount, user_id))
        await db.commit()


async def refresh_player_name(user_id: int, username: str):
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE players SET username = ? WHERE user_id = ?",
            (username, user_id)
        )
        await db.commit()