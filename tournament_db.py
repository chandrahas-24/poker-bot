import aiosqlite
import asyncio
import os
import shutil
from datetime import datetime
import config

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
            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                team_id INTEGER DEFAULT NULL,
                balance INTEGER DEFAULT 0,
                hands_won INTEGER DEFAULT 0,
                hands_played INTEGER DEFAULT 0,
                registered_at TEXT NOT NULL,
                FOREIGN KEY (team_id) REFERENCES teams(id)
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
        await db.commit()

async def checkpoint():
    db = await _get_db()
    async with _write_lock:
        await db.execute("PRAGMA wal_checkpoint(TRUNCATE)")

# ── Registration ──

async def is_registered(user_id: int) -> bool:
    db = await _get_db()
    async with db.execute("SELECT user_id FROM players WHERE user_id=?", (user_id,)) as c:
        row = await c.fetchone()
        return bool(row)

async def register_player(user_id: int, username: str, team_id: int | None = None) -> bool:
    if await is_registered(user_id):
        return False
    db = await _get_db()
    now = datetime.utcnow().isoformat()
    async with _write_lock:
        await db.execute("""
            INSERT INTO players (user_id, username, team_id, balance, registered_at)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, username, team_id, config.TOURNAMENT_STARTING_CHIPS, now))
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
        await db.commit()
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            return bool(row and row[0] > 0)

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
            await db.execute("""
                UPDATE players SET hands_played = hands_played + 1,
                hands_won = hands_won + ? WHERE user_id = ?
            """, (1 if won else 0, user_id))
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

# ── Team Management ──

async def create_team(name: str, created_by: int) -> bool:
    db = await _get_db()
    now = datetime.utcnow().isoformat()
    try:
        async with _write_lock:
            await db.execute(
                "INSERT INTO teams (name, created_by, created_at) VALUES (?, ?, ?)",
                (name, created_by, now)
            )
            await db.commit()
        return True
    except aiosqlite.IntegrityError:
        return False

async def get_team_by_name(name: str) -> dict | None:
    db = await _get_db()
    async with db.execute("SELECT * FROM teams WHERE name=?", (name,)) as c:
        row = await c.fetchone()
        return dict(row) if row else None
        
async def get_team_by_id(team_id: int) -> dict | None:
    db = await _get_db()
    async with db.execute("SELECT * FROM teams WHERE id=?", (team_id,)) as c:
        row = await c.fetchone()
        return dict(row) if row else None

async def get_all_teams() -> list[dict]:
    db = await _get_db()
    async with db.execute("SELECT * FROM teams") as c:
        return [dict(r) for r in await c.fetchall()]

async def add_player_to_team(user_id: int, team_id: int) -> bool:
    db = await _get_db()
    async with _write_lock:
        await db.execute("UPDATE players SET team_id = ? WHERE user_id = ?", (team_id, user_id))
        await db.commit()
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            return bool(row and row[0] > 0)

async def remove_player_from_team(user_id: int) -> bool:
    db = await _get_db()
    async with _write_lock:
        await db.execute("UPDATE players SET team_id = NULL WHERE user_id = ?", (user_id,))
        await db.commit()
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            return bool(row and row[0] > 0)

async def get_team_roster(team_id: int) -> list[dict]:
    db = await _get_db()
    async with db.execute("""
        SELECT p.*, (p.balance + COALESCE(c.amount, 0)) as total_chips
        FROM players p
        LEFT JOIN chips_in_play c ON p.user_id = c.user_id
        WHERE p.team_id = ?
        ORDER BY total_chips DESC
    """, (team_id,)) as c:
        return [dict(r) for r in await c.fetchall()]

# ── Stats & Leaderboards ──

async def get_player_stats(user_id: int) -> dict | None:
    db = await _get_db()
    async with db.execute("""
        SELECT p.*, t.name as team_name, COALESCE(c.amount, 0) as chips_in_play
        FROM players p
        LEFT JOIN teams t ON p.team_id = t.id
        LEFT JOIN chips_in_play c ON p.user_id = c.user_id
        WHERE p.user_id = ?
    """, (user_id,)) as c:
        row = await c.fetchone()
        if not row:
            return None
        stats = dict(row)
        stats['total_chips'] = stats['balance'] + stats['chips_in_play']
        
        # Calculate win%
        hands_played = stats['hands_played']
        hands_won = stats['hands_won']
        stats['win_rate'] = (hands_won / hands_played * 100) if hands_played > 0 else 0
        
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
        return [dict(r) for r in await c.fetchall()]

async def get_team_leaderboard(limit: int = 10) -> list[dict]:
    db = await _get_db()
    async with db.execute("""
        SELECT t.id, t.name, SUM(p.hands_won) as total_wins
        FROM teams t
        JOIN players p ON t.id = p.team_id
        GROUP BY t.id, t.name
        ORDER BY total_wins DESC
        LIMIT ?
    """, (limit,)) as c:
        return [dict(r) for r in await c.fetchall()]
