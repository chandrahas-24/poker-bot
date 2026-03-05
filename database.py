import aiosqlite
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "poker.db")


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                user_id  INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                balance  INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                user_id      INTEGER PRIMARY KEY,
                username     TEXT NOT NULL,
                hands_played INTEGER DEFAULT 0,
                hands_won    INTEGER DEFAULT 0,
                chips_won    INTEGER DEFAULT 0,
                chips_lost   INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS hand_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ts         TEXT NOT NULL,
                guild_id   INTEGER NOT NULL,
                table_id   TEXT NOT NULL,
                table_name TEXT NOT NULL,
                hand_num   INTEGER NOT NULL,
                summary    TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chip_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ts         TEXT NOT NULL,
                admin_id   INTEGER NOT NULL,
                admin_name TEXT NOT NULL,
                user_id    INTEGER NOT NULL,
                user_name  TEXT NOT NULL,
                amount     INTEGER NOT NULL,
                note       TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id          INTEGER PRIMARY KEY,
                small_blind       INTEGER DEFAULT 25,
                big_blind         INTEGER DEFAULT 50,
                min_wallet        INTEGER DEFAULT 50,
                next_hand_delay   INTEGER DEFAULT 30,
                manager_role_id   INTEGER DEFAULT NULL,
                log_channel_id    INTEGER DEFAULT NULL,
                turn_timeout      INTEGER DEFAULT 300,
                resend_after_msgs INTEGER DEFAULT 10
            )
        """)
        # Add columns if upgrading from older DB
        for col, default in [
            ("next_hand_delay", 30),
            ("turn_timeout", 300),
            ("resend_after_msgs", 10),
        ]:
            try:
                await db.execute(f"ALTER TABLE guild_settings ADD COLUMN {col} INTEGER DEFAULT {default}")
            except Exception:
                pass
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chips_in_play (
                user_id  INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                amount   INTEGER NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS poker_bans (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id   INTEGER NOT NULL,
                user_id    INTEGER NOT NULL,
                username   TEXT NOT NULL,
                table_name TEXT,       -- NULL = server-wide ban, otherwise table-specific
                banned_by  INTEGER NOT NULL,
                ts         TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        TEXT NOT NULL,
                action    TEXT NOT NULL,
                user_id   INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                detail    TEXT
            )
        """)
        await db.commit()


# ── Guild settings ────────────────────────────────────────────────────────────

async def get_settings(guild_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM guild_settings WHERE guild_id=?", (guild_id,)) as c:
            row = await c.fetchone()
            if row:
                return dict(row)
            return {"guild_id": guild_id, "small_blind": 25, "big_blind": 50,
                    "min_wallet": 50, "next_hand_delay": 30,
                    "manager_role_id": None, "log_channel_id": None,
                    "turn_timeout": 300, "resend_after_msgs": 10}


async def set_settings(guild_id: int, **kwargs):
    current = await get_settings(guild_id)
    current.update({k: v for k, v in kwargs.items() if v is not None})
    async with aiosqlite.connect(DB_PATH) as db:
        # Ensure new keys have defaults if missing
        current.setdefault("turn_timeout", 300)
        current.setdefault("resend_after_msgs", 10)
        await db.execute("""
            INSERT INTO guild_settings
                (guild_id, small_blind, big_blind, min_wallet, next_hand_delay, manager_role_id, log_channel_id, turn_timeout, resend_after_msgs)
            VALUES (:guild_id, :small_blind, :big_blind, :min_wallet, :next_hand_delay, :manager_role_id, :log_channel_id, :turn_timeout, :resend_after_msgs)
            ON CONFLICT(guild_id) DO UPDATE SET
                small_blind       = :small_blind,
                big_blind         = :big_blind,
                min_wallet        = :min_wallet,
                next_hand_delay   = :next_hand_delay,
                manager_role_id   = :manager_role_id,
                log_channel_id    = :log_channel_id,
                turn_timeout      = :turn_timeout,
                resend_after_msgs = :resend_after_msgs
        """, current)
        await db.commit()


# ── Wallet ────────────────────────────────────────────────────────────────────

async def get_balance(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT balance FROM wallets WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else 0


async def add_chips(admin_id: int, admin_name: str, user_id: int, user_name: str,
                    amount: int, note: str = "") -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance) VALUES (?, ?, MAX(0, ?))
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                balance  = MAX(0, balance + ?)
        """, (user_id, user_name, amount, amount))
        await db.execute("""
            INSERT INTO chip_log (ts, admin_id, admin_name, user_id, user_name, amount, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (datetime.utcnow().isoformat(), admin_id, admin_name, user_id, user_name, amount, note))
        await db.commit()
        async with db.execute("SELECT balance FROM wallets WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else 0


async def deduct_chips(user_id: int, amount: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE wallets SET balance = balance - ? WHERE user_id = ? AND balance >= ?",
                         (amount, user_id, amount))
        await db.commit()
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            return bool(row and row[0] > 0)


async def return_chips(user_id: int, amount: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance) VALUES (?, '', ?)
            ON CONFLICT(user_id) DO UPDATE SET balance = balance + ?
        """, (user_id, amount, amount))
        await db.commit()


async def upsert_wallet_name(user_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance) VALUES (?, ?, 0)
            ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
        """, (user_id, username))
        await db.commit()


# ── Stats ─────────────────────────────────────────────────────────────────────

async def record_hand(user_id: int, username: str, won: bool, net_chips: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO stats (user_id, username, hands_played, hands_won, chips_won, chips_lost)
            VALUES (?, ?, 1, ?, MAX(0,?), MAX(0,?))
            ON CONFLICT(user_id) DO UPDATE SET
                username     = excluded.username,
                hands_played = hands_played + 1,
                hands_won    = hands_won + ?,
                chips_won    = chips_won  + MAX(0,?),
                chips_lost   = chips_lost + MAX(0,?)
        """, (user_id, username, 1 if won else 0, net_chips, -net_chips,
              1 if won else 0, net_chips, -net_chips))
        await db.commit()


async def get_leaderboard(limit: int = 10) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT s.user_id, s.username, s.hands_played, s.hands_won, s.chips_won, s.chips_lost,
                   (s.chips_won - s.chips_lost) AS net_chips,
                   COALESCE(w.balance, 0) AS wallet
            FROM stats s LEFT JOIN wallets w ON s.user_id = w.user_id
            ORDER BY net_chips DESC LIMIT ?
        """, (limit,)) as c:
            return [dict(r) for r in await c.fetchall()]

async def get_player_rank(user_id: int) -> int | None:
    """Returns 1-based rank by net chips, or None if not in stats."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM stats WHERE user_id=?", (user_id,)) as c:
            if not await c.fetchone():
                return None
        async with db.execute("""
            SELECT COUNT(*) + 1 FROM stats
            WHERE (chips_won - chips_lost) > (
                SELECT chips_won - chips_lost FROM stats WHERE user_id = ?
            )
        """, (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else None


async def get_player_stats(user_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT s.username, s.hands_played, s.hands_won, s.chips_won, s.chips_lost,
                   (s.chips_won - s.chips_lost) AS net_chips,
                   COALESCE(w.balance, 0) AS wallet
            FROM stats s LEFT JOIN wallets w ON s.user_id = w.user_id
            WHERE s.user_id = ?
        """, (user_id,)) as c:
            row = await c.fetchone()
            return dict(row) if row else None


# ── Hand log ──────────────────────────────────────────────────────────────────

async def log_hand(guild_id: int, table_id: str, table_name: str, hand_num: int, summary: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO hand_log (ts, guild_id, table_id, table_name, hand_num, summary)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), guild_id, table_id, table_name, hand_num, summary))
        await db.commit()


# ── In-play chip recovery ─────────────────────────────────────────────────────

async def mark_chips_in_play(user_id: int, username: str, amount: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO chips_in_play (user_id, username, amount) VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET 
                username=excluded.username, 
                amount=chips_in_play.amount + excluded.amount
        """, (user_id, username, amount))
        await db.commit()


async def update_chips_in_play(user_id: int, amount: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE chips_in_play SET amount=? WHERE user_id=?", (amount, user_id))
        await db.commit()


async def clear_chips_in_play(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM chips_in_play WHERE user_id=?", (user_id,))
        await db.commit()


async def recover_chips_in_play() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM chips_in_play") as c:
            rows = [dict(r) for r in await c.fetchall()]
        for r in rows:
            await db.execute("""
                INSERT INTO wallets (user_id, username, balance) VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET balance = balance + ?
            """, (r["user_id"], r["username"], r["amount"], r["amount"]))
        await db.execute("DELETE FROM chips_in_play")
        await db.commit()
        return rows


# ── Audit log ─────────────────────────────────────────────────────────────────

async def write_audit(action: str, user_id: int, user_name: str, detail: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO audit_log (ts, action, user_id, user_name, detail)
            VALUES (?, ?, ?, ?, ?)
        """, (datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), action, user_id, user_name, detail))
        await db.commit()


# ── Reset ─────────────────────────────────────────────────────────────────────

# ── Bans ──────────────────────────────────────────────────────────────────────

async def ban_player(guild_id: int, user_id: int, username: str, banned_by: int, table_name: str | None = None):
    """Ban user server-wide (table_name=None) or from a specific table."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Ensure table exists (migration safety)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS poker_bans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
                username TEXT NOT NULL, table_name TEXT,
                banned_by INTEGER NOT NULL, ts TEXT NOT NULL
            )
        """)
        await db.commit()
        # Avoid duplicate bans
        async with db.execute(
                "SELECT id FROM poker_bans WHERE guild_id=? AND user_id=? AND table_name IS ?",
                (guild_id, user_id, table_name)
        ) as c:
            if await c.fetchone():
                return False  # already banned at this scope
        await db.execute(
            "INSERT INTO poker_bans (guild_id, user_id, username, table_name, banned_by, ts) VALUES (?,?,?,?,?,?)",
            (guild_id, user_id, username, table_name, banned_by, datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
        )
        await db.commit()
    return True


async def unban_player(guild_id: int, user_id: int, table_name: str | None = None) -> int:
    """Remove ban. Returns number of bans removed."""
    async with aiosqlite.connect(DB_PATH) as db:
        if table_name is None:
            # Remove ALL bans for this user in this guild
            async with db.execute(
                    "SELECT COUNT(*) FROM poker_bans WHERE guild_id=? AND user_id=?",
                    (guild_id, user_id)
            ) as c:
                count = (await c.fetchone())[0]
            await db.execute("DELETE FROM poker_bans WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        else:
            # Remove only the table-specific ban
            async with db.execute(
                    "SELECT COUNT(*) FROM poker_bans WHERE guild_id=? AND user_id=? AND table_name=?",
                    (guild_id, user_id, table_name)
            ) as c:
                count = (await c.fetchone())[0]
            await db.execute(
                "DELETE FROM poker_bans WHERE guild_id=? AND user_id=? AND table_name=?",
                (guild_id, user_id, table_name)
            )
        await db.commit()
        return count


async def is_banned(guild_id: int, user_id: int, table_name: str | None = None) -> bool:
    """True if user is server-wide banned OR banned from specific table_name."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # Server-wide ban
            async with db.execute(
                    "SELECT id FROM poker_bans WHERE guild_id=? AND user_id=? AND table_name IS NULL",
                    (guild_id, user_id)
            ) as c:
                if await c.fetchone():
                    return True
            # Table-specific ban
            if table_name:
                async with db.execute(
                        "SELECT id FROM poker_bans WHERE guild_id=? AND user_id=? AND table_name=?",
                        (guild_id, user_id, table_name)
                ) as c:
                    if await c.fetchone():
                        return True
    except Exception as e:
        print(f"[db] is_banned error: {e}")
    return False


async def delete_player_stats(user_id: int) -> bool:
    """Remove a player's stats entry from the leaderboard. Returns True if found."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM stats WHERE user_id=?", (user_id,)) as c:
            if not await c.fetchone():
                return False
        await db.execute("DELETE FROM stats WHERE user_id=?", (user_id,))
        await db.commit()
    return True


async def reset_database(admin_id: int, admin_name: str):
    """Wipe all gameplay data and log who did it."""
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM wallets")
        await db.execute("DELETE FROM stats")
        await db.execute("DELETE FROM hand_log")
        await db.execute("DELETE FROM chip_log")
        await db.execute("DELETE FROM chips_in_play")
        await db.execute("DELETE FROM guild_settings")
        await db.execute("DELETE FROM poker_bans")
        # Keep audit_log intact — insert the reset event after clearing everything else
        await db.execute("""
            INSERT INTO audit_log (ts, action, user_id, user_name, detail)
            VALUES (?, 'DATABASE_RESET', ?, ?, 'Full database reset performed')
        """, (ts, admin_id, admin_name))
        await db.commit()