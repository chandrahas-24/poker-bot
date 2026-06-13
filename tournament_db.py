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
                leader_id INTEGER DEFAULT NULL,
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
        # Add the 2-day cycle tracker
        await db.execute("""
                    CREATE TABLE IF NOT EXISTS tourney_state (
                        id INTEGER PRIMARY KEY CHECK (id=1), 
                        cycle_day INTEGER DEFAULT 1
                    )
                """)
        await db.execute("INSERT OR IGNORE INTO tourney_state (id, cycle_day) VALUES (1, 1)")


        # Add the activity tracking columns safely
        try:
            await db.execute("ALTER TABLE players ADD COLUMN period_wagered INTEGER DEFAULT 0")
            await db.execute("ALTER TABLE players ADD COLUMN last_activity TEXT")
            await db.execute("ALTER TABLE players ADD COLUMN target_wager INTEGER DEFAULT 1250")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise

        try:
            await db.execute("ALTER TABLE teams ADD COLUMN leader_id INTEGER")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise

        # 🛠️ VPIP Tracking Columns
        try:
            await db.execute("ALTER TABLE players ADD COLUMN vpip_count INTEGER DEFAULT 0")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise

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
    target = int(config.TOURNAMENT_STARTING_CHIPS * 0.25)

    async with _write_lock:
        await db.execute("""
            INSERT INTO players (user_id, username, team_id, balance, registered_at, last_activity, period_wagered, target_wager)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, username, team_id, config.TOURNAMENT_STARTING_CHIPS, now, now, 0, target))
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

            await db.execute("""
                        UPDATE players SET 
                        hands_played = hands_played + 1,
                        hands_won = hands_won + ?,
                        vpip_count = COALESCE(vpip_count, 0) + ?
                        WHERE user_id = ?
                    """, (1 if won else 0, vpip_inc, user_id))
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
            # 🛠️ FIXED: Staff create teams, so leader_id defaults to NULL
            await db.execute(
                "INSERT INTO teams (name, created_by, leader_id, created_at) VALUES (?, ?, NULL, ?)",
                (name, created_by, now)
            )
            await db.commit()
        return True
    except aiosqlite.IntegrityError:
        return False

async def set_team_leader(team_id: int, user_id: int) -> bool:
    db = await _get_db()
    async with _write_lock:
        await db.execute("UPDATE teams SET leader_id = ? WHERE id = ?", (user_id, team_id))
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            success = bool(row and row[0] > 0)
        await db.commit()
        return success

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

async def get_all_teams_info() -> list[dict]:
    db = await _get_db()
    async with db.execute("""
        SELECT t.id, t.name, t.leader_id,
               (SELECT username FROM players WHERE user_id = t.leader_id) as leader_name,
               COUNT(p.user_id) as member_count
        FROM teams t
        LEFT JOIN players p ON t.id = p.team_id
        GROUP BY t.id
        ORDER BY t.name ASC
    """) as c:
        return [dict(r) for r in await c.fetchall()]

async def add_player_to_team(user_id: int, team_id: int) -> bool:
    db = await _get_db()
    async with _write_lock:
        async with db.execute("SELECT COUNT(*) FROM players WHERE team_id = ?", (team_id,)) as c:
            count = (await c.fetchone())[0]
        if count >= 4:
            return False

        await db.execute("UPDATE players SET team_id = ? WHERE user_id = ?", (team_id, user_id))
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            success = bool(row and row[0] > 0)
        await db.commit()
        return success

async def remove_player_from_team(user_id: int) -> bool:
    db = await _get_db()
    async with _write_lock:
        await db.execute("UPDATE players SET team_id = NULL WHERE user_id = ?", (user_id,))
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            success = bool(row and row[0] > 0)
        await db.commit()
        return success

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
        return [dict(r) for r in await c.fetchall()]

async def get_team_leaderboard(limit: int = 10) -> list[dict]:
    db = await _get_db()
    async with db.execute("""
        SELECT 
            t.id, 
            COALESCE(t.name, p.username || ' (Solo)') as name, 
            SUM(p.hands_won) as total_wins
        FROM players p
        LEFT JOIN teams t ON p.team_id = t.id
        GROUP BY COALESCE(p.team_id, p.user_id)
        ORDER BY total_wins DESC
        LIMIT ?
    """, (limit,)) as c:
        return [dict(r) for r in await c.fetchall()]


async def log_period_wagers(wagers: dict):
    """Adds chips wagered to the 48-hour tracker and stamps last activity."""
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


async def process_daily_tourney_check() -> tuple[int, list[dict], list[dict]]:
    import datetime
    db = await _get_db()
    warnings, penalties = [], []
    now_dt = datetime.datetime.utcnow()

    async with _write_lock:
        # 🛠️ FIXED: Snapshot the Top 10 INSIDE the write lock to prevent stale data races
        async with db.execute("""
            SELECT p.user_id, p.username, (p.balance + COALESCE(c.amount, 0)) as total_chips
            FROM players p
            LEFT JOIN chips_in_play c ON p.user_id = c.user_id
            ORDER BY total_chips DESC LIMIT 10
        """) as c:
            top_10 = [dict(r) for r in await c.fetchall()]

        async with db.execute("SELECT cycle_day FROM tourney_state WHERE id=1") as c:
            row = await c.fetchone()
            cycle_day = row[0] if row else 1

        if cycle_day == 1:
            # --- DAY 1: WARNING PHASE ---
            for player in top_10:
                user_id, total_chips = player['user_id'], player['total_chips']
                async with db.execute("SELECT COALESCE(period_wagered, 0), COALESCE(target_wager, 1250), registered_at FROM players WHERE user_id=?", (user_id,)) as c:
                    w = await c.fetchone()
                    wagered = w[0] if w else 0
                    target = w[1] if w else int(total_chips * 0.25)
                    reg_str = w[2] if w else now_dt.isoformat()

                # 🛠️ FIXED: Ignore brand new players who just entered the Top 10
                reg_dt = datetime.datetime.fromisoformat(reg_str)
                if now_dt < reg_dt + datetime.timedelta(hours=48):
                    continue

                if wagered < target:
                    warnings.append({
                        "user_id": user_id, "username": player['username'],
                        "shortfall": target - wagered, "target": target, "wagered": wagered, "total_chips": total_chips
                    })
            await db.execute("UPDATE tourney_state SET cycle_day = 2 WHERE id=1")

        else:
            # --- DAY 2: ENFORCEMENT PHASE ---
            for player in top_10:
                user_id, total_chips = player['user_id'], player['total_chips']
                async with db.execute("SELECT COALESCE(period_wagered, 0), COALESCE(target_wager, 1250), registered_at FROM players WHERE user_id=?", (user_id,)) as c:
                    w = await c.fetchone()
                    wagered = w[0] if w else 0
                    target = w[1] if w else int(total_chips * 0.25)
                    reg_str = w[2] if w else now_dt.isoformat()

                reg_dt = datetime.datetime.fromisoformat(reg_str)
                if now_dt < reg_dt + datetime.timedelta(hours=48):
                    continue

                if wagered < target:
                    shortfall = target - wagered
                    await db.execute("UPDATE players SET balance = MAX(0, balance - ?) WHERE user_id = ?", (shortfall, user_id))
                    penalties.append({
                        "user_id": user_id, "username": player['username'],
                        "shortfall": shortfall, "target": target, "actual": wagered, "total_chips": total_chips
                    })

            await db.execute("UPDATE players SET period_wagered = 0")
            await db.execute("""
                UPDATE players 
                SET target_wager = CAST((balance + COALESCE((SELECT amount FROM chips_in_play WHERE user_id = players.user_id), 0)) * 0.25 AS INTEGER)
            """)
            await db.execute("UPDATE tourney_state SET cycle_day = 1 WHERE id=1")

        await db.commit()

    return cycle_day, warnings, penalties


async def get_team_dominance_warning(active_user_ids: list[int]) -> str | None:
    """Checks if any team holds >50% of the provided seats. Returns a warning string if true."""
    if len(active_user_ids) < 2:
        return None  # Normal engine logic will pause the game if only 1 person is left anyway

    db = await _get_db()
    team_counts = {}

    # Count how many players belong to each team at this specific table
    placeholders = ",".join("?" * len(active_user_ids))
    async with db.execute(
            f"SELECT team_id FROM players WHERE user_id IN ({placeholders}) AND team_id IS NOT NULL",
            active_user_ids
    ) as c:
        for row in await c.fetchall():
            tid = row[0]
            team_counts[tid] = team_counts.get(tid, 0) + 1

    total_active = len(active_user_ids)

    # Check if any team crossed the 50% line
    for tid, count in team_counts.items():
        if count > (total_active / 2.0):
            # Fetch the actual team name so the bot can call them out in chat
            async with db.execute("SELECT name FROM teams WHERE id=?", (tid,)) as c:
                t_row = await c.fetchone()
                t_name = t_row[0] if t_row else "A team"

            return (
                f"Team **{t_name}** currently controls more than 50% of the active seats ({count}/{total_active}). "
                f"The game has been paused. To resume, other players must join, or someone from {t_name} must leave."
            )

    return None


async def delete_team(team_name: str) -> bool:
    """Deletes a team and unassigns its players. Returns True if successful."""
    db = await _get_db()
    async with _write_lock:
        async with db.execute("SELECT id FROM teams WHERE name = ? COLLATE NOCASE", (team_name,)) as c:
            row = await c.fetchone()

        if not row:
            return False

        team_id = row[0]

        await db.execute("UPDATE players SET team_id = NULL WHERE team_id = ?", (team_id,))
        await db.execute("DELETE FROM teams WHERE id = ?", (team_id,))
        await db.commit()
        return True

async def refresh_player_name(user_id: int, username: str):
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE players SET username = ? WHERE user_id = ?",
            (username, user_id)
        )
        await db.commit()