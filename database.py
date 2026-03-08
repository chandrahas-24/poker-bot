import aiosqlite
import asyncio
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "poker.db")

# ── Singleton connection + serialised-write lock ──────────────────────────────
# SQLite only allows one writer at a time.  Opening a new connection per call
# means concurrent button clicks race for the write lock and raise
# "database is locked".  We keep ONE shared connection and route all writes
# through a single asyncio.Lock so they are naturally serialised.

_db: aiosqlite.Connection | None = None
_write_lock = asyncio.Lock()


async def _get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        _db = await aiosqlite.connect(DB_PATH, timeout=30)
        await _db.execute("PRAGMA journal_mode=WAL")
        await _db.execute("PRAGMA busy_timeout=10000")
        _db.row_factory = aiosqlite.Row
    return _db


async def init_db():
    db = await _get_db()
    async with _write_lock:
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
        for col, default in [
            ("next_hand_delay", 30),
            ("turn_timeout", 300),
            ("resend_after_msgs", 10),
            ("muck_time", 15),
            ("max_wallet", 0),
        ]:
            try:
                await db.execute(
                    f"ALTER TABLE guild_settings ADD COLUMN {col} INTEGER DEFAULT {default}"
                )
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
                table_name TEXT,
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
    db = await _get_db()
    async with db.execute("SELECT * FROM guild_settings WHERE guild_id=?", (guild_id,)) as c:
        row = await c.fetchone()
        if row:
            return dict(row)
        return {
            "guild_id": guild_id, "small_blind": 25, "big_blind": 50,
            "min_wallet": 50, "max_wallet": 0, "next_hand_delay": 30,
            "manager_role_id": None, "log_channel_id": None,
            "turn_timeout": 300, "resend_after_msgs": 10, "muck_time": 15,
        }


async def set_settings(guild_id: int, **kwargs):
    current = await get_settings(guild_id)
    current.update({k: v for k, v in kwargs.items() if v is not None})
    current.setdefault("turn_timeout", 300)
    current.setdefault("resend_after_msgs", 10)
    current.setdefault("muck_time", 15)
    current.setdefault("max_wallet", 2000)
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO guild_settings
                (guild_id, small_blind, big_blind, min_wallet, max_wallet,
                 next_hand_delay, manager_role_id, log_channel_id,
                 turn_timeout, resend_after_msgs, muck_time)
            VALUES
                (:guild_id, :small_blind, :big_blind, :min_wallet, :max_wallet,
                 :next_hand_delay, :manager_role_id, :log_channel_id,
                 :turn_timeout, :resend_after_msgs, :muck_time)
            ON CONFLICT(guild_id) DO UPDATE SET
                small_blind       = :small_blind,
                big_blind         = :big_blind,
                min_wallet        = :min_wallet,
                max_wallet        = :max_wallet,
                next_hand_delay   = :next_hand_delay,
                manager_role_id   = :manager_role_id,
                log_channel_id    = :log_channel_id,
                turn_timeout      = :turn_timeout,
                resend_after_msgs = :resend_after_msgs,
                muck_time         = :muck_time
        """, current)
        await db.commit()


# ── Wallet ────────────────────────────────────────────────────────────────────

async def get_balance(user_id: int) -> int:
    db = await _get_db()
    async with db.execute("SELECT balance FROM wallets WHERE user_id=?", (user_id,)) as c:
        row = await c.fetchone()
        return row[0] if row else 0


async def add_chips(admin_id: int, admin_name: str, user_id: int, user_name: str,
                    amount: int, note: str = "") -> int:
    db = await _get_db()
    async with _write_lock:
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
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE wallets SET balance = balance - ? WHERE user_id = ? AND balance >= ?",
            (amount, user_id, amount)
        )
        await db.commit()
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            return bool(row and row[0] > 0)


async def return_chips(user_id: int, amount: int):
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance) VALUES (?, '', ?)
            ON CONFLICT(user_id) DO UPDATE SET balance = balance + ?
        """, (user_id, amount, amount))
        await db.commit()


async def upsert_wallet_name(user_id: int, username: str):
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance) VALUES (?, ?, 0)
            ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
        """, (user_id, username))
        await db.commit()


# ── Stats ─────────────────────────────────────────────────────────────────────

async def record_hand(user_id: int, username: str, won: bool, net_chips: int):
    db = await _get_db()
    async with _write_lock:
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
    db = await _get_db()
    async with db.execute("""
        SELECT s.user_id, s.username, s.hands_played, s.hands_won, s.chips_won, s.chips_lost,
               (s.chips_won - s.chips_lost) AS net_chips,
               COALESCE(w.balance, 0) AS wallet
        FROM stats s LEFT JOIN wallets w ON s.user_id = w.user_id
        ORDER BY net_chips DESC LIMIT ?
    """, (limit,)) as c:
        return [dict(r) for r in await c.fetchall()]


async def get_player_rank(user_id: int) -> int | None:
    db = await _get_db()
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
    db = await _get_db()
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
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO hand_log (ts, guild_id, table_id, table_name, hand_num, summary)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), guild_id, table_id, table_name, hand_num, summary))
        await db.commit()


# ── In-play chip recovery ─────────────────────────────────────────────────────

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


async def update_chips_in_play(user_id: int, amount: int):
    db = await _get_db()
    async with _write_lock:
        await db.execute("UPDATE chips_in_play SET amount=? WHERE user_id=?", (amount, user_id))
        await db.commit()


async def clear_chips_in_play(user_id: int):
    db = await _get_db()
    async with _write_lock:
        await db.execute("DELETE FROM chips_in_play WHERE user_id=?", (user_id,))
        await db.commit()


async def recover_chips_in_play() -> list[dict]:
    db = await _get_db()
    async with db.execute("SELECT * FROM chips_in_play") as c:
        rows = [dict(r) for r in await c.fetchall()]
    async with _write_lock:
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
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO audit_log (ts, action, user_id, user_name, detail)
            VALUES (?, ?, ?, ?, ?)
        """, (datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), action, user_id, user_name, detail))
        await db.commit()


# ── Bans ──────────────────────────────────────────────────────────────────────

async def ban_player(guild_id: int, user_id: int, username: str, banned_by: int,
                     table_name: str | None = None):
    db = await _get_db()
    async with _write_lock:
        async with db.execute(
            "SELECT id FROM poker_bans WHERE guild_id=? AND user_id=? AND table_name IS ?",
            (guild_id, user_id, table_name)
        ) as c:
            if await c.fetchone():
                return False
        await db.execute(
            "INSERT INTO poker_bans (guild_id, user_id, username, table_name, banned_by, ts) "
            "VALUES (?,?,?,?,?,?)",
            (guild_id, user_id, username, table_name, banned_by,
             datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
        )
        await db.commit()
    return True


async def unban_player(guild_id: int, user_id: int, table_name: str | None = None) -> int:
    db = await _get_db()
    async with _write_lock:
        if table_name is None:
            async with db.execute(
                "SELECT COUNT(*) FROM poker_bans WHERE guild_id=? AND user_id=?",
                (guild_id, user_id)
            ) as c:
                count = (await c.fetchone())[0]
            await db.execute(
                "DELETE FROM poker_bans WHERE guild_id=? AND user_id=?", (guild_id, user_id)
            )
        else:
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
    try:
        db = await _get_db()
        async with db.execute(
            "SELECT id FROM poker_bans WHERE guild_id=? AND user_id=? AND table_name IS NULL",
            (guild_id, user_id)
        ) as c:
            if await c.fetchone():
                return True
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


async def get_all_bans(guild_id: int) -> list[dict]:
    db = await _get_db()
    async with db.execute("""
        SELECT user_id, username, table_name, banned_by, ts
        FROM poker_bans
        WHERE guild_id = ?
        ORDER BY ts DESC
    """, (guild_id,)) as c:
        return [dict(r) for r in await c.fetchall()]


async def delete_player_stats(user_id: int) -> bool:
    db = await _get_db()
    async with db.execute("SELECT user_id FROM stats WHERE user_id=?", (user_id,)) as c:
        if not await c.fetchone():
            return False
    async with _write_lock:
        await db.execute("DELETE FROM stats WHERE user_id=?", (user_id,))
        await db.commit()
    return True


async def reset_database(admin_id: int, admin_name: str):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    db = await _get_db()
    async with _write_lock:
        await db.execute("DELETE FROM wallets")
        await db.execute("DELETE FROM stats")
        await db.execute("DELETE FROM hand_log")
        await db.execute("DELETE FROM chip_log")
        await db.execute("DELETE FROM chips_in_play")
        await db.execute("DELETE FROM poker_bans")
        await db.execute("""
            INSERT INTO audit_log (ts, action, user_id, user_name, detail)
            VALUES (?, 'DATABASE_RESET', ?, ?, 'Full database reset performed')
        """, (ts, admin_id, admin_name))
        await db.commit()