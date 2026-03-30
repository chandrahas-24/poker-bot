import aiosqlite
import asyncio
import os
from datetime import datetime, timedelta

# 1. FORCE the bot to look inside the Railway Volume
# Replace '/app/data/poker.db' with whatever your Mount Path + filename is
DB_PATH = "/app/data/poker.db"

_db: aiosqlite.Connection | None = None
_write_lock = asyncio.Lock()

# ── Settings cache to reduce database queries ─────────────────────────────────
_settings_cache: dict[int, dict] = {}
_cache_lock = asyncio.Lock()


async def _get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        _db = await aiosqlite.connect(DB_PATH, timeout=30)
        await _db.execute("PRAGMA journal_mode=WAL")
        await _db.execute("PRAGMA synchronous=NORMAL") # 🚨 ADD THIS: Makes commits instant!
        await _db.execute("PRAGMA busy_timeout=10000")

        await _db.execute("PRAGMA temp_store=MEMORY")

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

        await db.execute("""
                    CREATE TABLE IF NOT EXISTS house_revenue (
                        ts TEXT,
                        amount INTEGER
                    )
                """)

        await db.execute("""
                    CREATE TABLE IF NOT EXISTS jackpot (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        amount INTEGER DEFAULT 0
                    )
                """)
        # Initialize with 1 row if empty
        async with db.execute("SELECT COUNT(*) FROM jackpot") as c:
            if (await c.fetchone())[0] == 0:
                await db.execute("INSERT INTO jackpot (id, amount) VALUES (1, 0)")

        # Safely upgrade existing stats table
        for col, default in [
            ("total_tipped", 0),
            ("win_streak", 0),
            ("max_win_streak", 0),
            ("pocket_aces_wins", 0),
            ("all_in_wins", 0),
            ("quads_wins", 0),
            ("straight_flush_wins", 0),
            ("royal_flush_wins", 0),
            ("times_wiped", 0),
        ]:
            try:
                await db.execute(f"ALTER TABLE stats ADD COLUMN {col} INTEGER DEFAULT {default}")
            except Exception:
                pass

        # Safely upgrade existing wallets table without wiping data
        try:
            await db.execute("ALTER TABLE wallets ADD COLUMN pending_cashout INTEGER DEFAULT 0")
        except Exception:
            pass
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS player_cosmetics (
                user_id          INTEGER PRIMARY KEY,
                active_title     TEXT DEFAULT NULL,
                active_win_msg   TEXT DEFAULT NULL,
                unlocked_titles  TEXT DEFAULT '[]',
                unlocked_win_msgs TEXT DEFAULT '["gg"]'
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS custom_cosmetics (
                cosmetic_id TEXT PRIMARY KEY,
                kind        TEXT NOT NULL,
                display     TEXT NOT NULL,
                description TEXT DEFAULT '',
                rarity      TEXT DEFAULT 'rare',
                hidden      INTEGER DEFAULT 0
            )
        """)
        
        await db.commit()
        await init_inactivity_tracking(db)
        await load_custom_cosmetics()  # Load custom cosmetics from database


# ── Guild settings ────────────────────────────────────────────────────────────

async def get_settings(guild_id: int) -> dict:
    """
    Get guild settings with caching to reduce database queries.
    Cache is automatically cleared when settings are updated.
    """
    async with _cache_lock:
        # Check cache first
        if guild_id in _settings_cache:
            return _settings_cache[guild_id].copy()

    # Not in cache - load from database
    db = await _get_db()
    async with db.execute("SELECT * FROM guild_settings WHERE guild_id=?", (guild_id,)) as c:
        row = await c.fetchone()
        if row:
            settings = dict(row)
        else:
            settings = {
                "guild_id": guild_id, "small_blind": 25, "big_blind": 50,
                "min_wallet": 50, "max_wallet": 0, "next_hand_delay": 30,
                "manager_role_id": None, "log_channel_id": None,
                "turn_timeout": 300, "resend_after_msgs": 10, "muck_time": 15,
            }

    # Update cache
    async with _cache_lock:
        _settings_cache[guild_id] = settings.copy()

    return settings


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

    # Clear cache for this guild to force reload on next get
    async with _cache_lock:
        _settings_cache.pop(guild_id, None)


def clear_settings_cache(guild_id: int | None = None):
    """
    Clear settings cache. If guild_id is None, clears all cached settings.
    Useful for manual cache invalidation if needed.
    """
    global _settings_cache
    if guild_id is None:
        _settings_cache = {}
    else:
        _settings_cache.pop(guild_id, None)


# ── Wallet ────────────────────────────────────────────────────────────────────

async def get_balance(user_id: int) -> int:
    db = await _get_db()
    async with db.execute("SELECT balance FROM wallets WHERE user_id=?", (user_id,)) as c:
        row = await c.fetchone()
        return row[0] if row else 0


async def add_chips(admin_id: int, admin_name: str, user_id: int, user_name: str,
                    amount: int, note: str = "") -> int:
    db = await _get_db()
    now = datetime.utcnow().isoformat()

    async with _write_lock:
        # 1. Add the chips AND start the clock if it's currently NULL
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance, last_activity) 
            VALUES (?, ?, MAX(0, ?), ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                balance  = MAX(0, balance + ?),
                last_activity = COALESCE(wallets.last_activity, ?)
        """, (user_id, user_name, amount, now, amount, now))

        # 2. Use your existing chip_log table!
        await db.execute("""
            INSERT INTO chip_log (ts, admin_id, admin_name, user_id, user_name, amount, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (now, admin_id, admin_name, user_id, user_name, amount, note))

        await db.commit()

        # 3. Fetch the new balance
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
                    INSERT INTO wallets (user_id, username, balance) VALUES (?, 'Unknown Player', ?)
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

async def record_hand_full(
    user_id: int,
    username: str,
    won: bool,
    net_chips: int,
    pocket_aces: bool = False,
    all_in_win: bool = False,
    quads_win: bool = False,
    straight_flush_win: bool = False,
    royal_flush_win: bool = False,
    chips_wagered: int = 0,
):
    """
    Single-commit replacement for the record_hand + update_win_streak +
    increment_* call chain. Acquires _write_lock exactly once.
    """
    db = await _get_db()
    async with _write_lock:
        # 1. Upsert base stats + achievement counters in one statement
        await db.execute("""
            INSERT INTO stats (
                user_id, username, hands_played, hands_won, chips_won, chips_lost,
                win_streak, max_win_streak,
                pocket_aces_wins, all_in_wins, quads_wins,
                straight_flush_wins, royal_flush_wins
            )
            VALUES (
                ?, ?, 1, ?, MAX(0,?), MAX(0,?),
                ?, ?,
                ?, ?, ?,
                ?, ?
            )
            ON CONFLICT(user_id) DO UPDATE SET
                username            = excluded.username,
                hands_played        = hands_played + 1,
                hands_won           = hands_won    + ?,
                chips_won           = chips_won    + MAX(0, ?),
                chips_lost          = chips_lost   + MAX(0, ?),
                win_streak          = CASE WHEN ? THEN win_streak + 1 ELSE 0 END,
                max_win_streak      = MAX(max_win_streak,
                                         CASE WHEN ? THEN win_streak + 1 ELSE 0 END),
                pocket_aces_wins    = pocket_aces_wins    + ?,
                all_in_wins         = all_in_wins         + ?,
                quads_wins          = quads_wins          + ?,
                straight_flush_wins = straight_flush_wins + ?,
                royal_flush_wins    = royal_flush_wins    + ?
        """, (
            # INSERT values
            user_id, username,
            1 if won else 0, net_chips, -net_chips,
            (1 if won else 0), (1 if won else 0),          # win_streak seed / max_win_streak seed
            1 if pocket_aces else 0,
            1 if all_in_win else 0,
            1 if quads_win else 0,
            1 if straight_flush_win else 0,
            1 if royal_flush_win else 0,
            # ON CONFLICT SET values
            1 if won else 0,
            net_chips, -net_chips,
            won, won,                                       # win_streak CASE / max_win_streak CASE
            1 if pocket_aces else 0,
            1 if all_in_win else 0,
            1 if quads_win else 0,
            1 if straight_flush_win else 0,
            1 if royal_flush_win else 0,
        ))
        await db.commit()

    # Activity tracking (outside write_lock — has its own lock internally)
    if abs(net_chips) > 0 or chips_wagered > 0:
        await mark_player_active(user_id, max(abs(net_chips), chips_wagered))


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
               COALESCE(s.total_tipped, 0) AS total_tipped,
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
        await db.execute("DELETE FROM house_revenue")  # <-- FIXED: Clears the revenue tracker
        await db.execute("DELETE FROM player_cosmetics")
        await db.execute("DELETE FROM jackpot")
        await db.execute("INSERT INTO jackpot (id, amount) VALUES (1, 0)")
        await db.execute("""
            INSERT INTO audit_log (ts, action, user_id, user_name, detail)
            VALUES (?, 'DATABASE_RESET', ?, ?, 'Full database reset performed')
        """, (ts, admin_id, admin_name))
        await db.commit()

    # Clear settings cache after reset
    clear_settings_cache()


# --- VAULT & CASHOUT FUNCTIONS ---

async def get_wallet(user_id: int) -> tuple[int, int]:
    """Returns (available_balance, pending_cashout)"""
    db = await _get_db()
    async with db.execute("SELECT balance, pending_cashout FROM wallets WHERE user_id=?", (user_id,)) as c:
        row = await c.fetchone()
        return (row[0], row[1]) if row else (0, 0)


async def request_cashout(user_id: int, amount: int) -> bool:
    """Moves chips to pending vault with NO tax."""
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE wallets SET balance = balance - ?, pending_cashout = pending_cashout + ? WHERE user_id = ? AND balance >= ?",
            (amount, amount, user_id, amount)
        )
        await db.commit()
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            return bool(row and row[0] > 0)


async def pay_cashout(user_id: int, amount: int) -> bool:
    """Staff pays out chips: deducts the specific amount from pending vault."""
    db = await _get_db()
    async with _write_lock:
        async with db.execute("SELECT pending_cashout FROM wallets WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            pending = row[0] if row else 0

        if pending < amount:
            return False

        await db.execute(
            "UPDATE wallets SET pending_cashout = pending_cashout - ? WHERE user_id = ?",
            (amount, user_id)
        )
        await db.commit()
        return True


# --- REVENUE & ECONOMY FUNCTIONS ---

async def log_tax(tax_amount: int):
    """Splits the 6% engine tax: 1/6th goes to Jackpot, 5/6ths to House."""
    if tax_amount <= 0: return
    jackpot_cut = (tax_amount // 6) # 1% of the original profit
    house_cut = tax_amount - jackpot_cut # 5% of the original profit
    db = await _get_db()
    async with _write_lock:
        await db.execute("INSERT INTO house_revenue (ts, amount) VALUES (?, ?)", (datetime.utcnow().isoformat(), house_cut))
        # 🚨 FIX: Safely target only the primary row
        await db.execute("UPDATE jackpot SET amount = amount + ? WHERE rowid = (SELECT MIN(rowid) FROM jackpot)", (jackpot_cut,))
        await db.commit()

async def get_jackpot() -> int:
    """Returns current jackpot size."""
    db = await _get_db()
    async with db.execute("SELECT amount FROM jackpot") as c:
        row = await c.fetchone()
        return row[0] if row else 0

async def adjust_jackpot(amount: int):
    """Admin command to add or remove chips from the jackpot."""
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE jackpot SET amount = MAX(0, amount + ?) WHERE rowid = (SELECT MIN(rowid) FROM jackpot)", (amount,))
        await db.commit()


async def get_revenue_stats() -> dict:
    db = await _get_db()
    now = datetime.utcnow()
    daily_cut  = (now - timedelta(hours=24)).isoformat()
    weekly_cut = (now - timedelta(hours=168)).isoformat()
    monthly_cut= (now - timedelta(hours=720)).isoformat()

    async with db.execute("""
        SELECT
            COALESCE(SUM(amount), 0),
            COALESCE(SUM(CASE WHEN ts >= ? THEN amount ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN ts >= ? THEN amount ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN ts >= ? THEN amount ELSE 0 END), 0)
        FROM house_revenue
    """, (monthly_cut, weekly_cut, daily_cut)) as c:
        row = await c.fetchone()

    return {
        "all_time": row[0],
        "monthly":  row[1],
        "weekly":   row[2],
        "daily":    row[3],
    }


async def get_economy_totals() -> tuple[int, int]:
    """Returns (total_available_wallets, total_pending_cashouts)"""
    db = await _get_db()
    async with db.execute("SELECT SUM(balance), SUM(pending_cashout) FROM wallets") as c:
        row = await c.fetchone()
        return (row[0] or 0, row[1] or 0)


# --- TIP TRACKING ---

async def record_tip(user_id: int, username: str, amount: int):
    """Adds to a player's all-time tipped amount."""
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO stats (user_id, username, hands_played, hands_won, chips_won, chips_lost, total_tipped)
            VALUES (?, ?, 0, 0, 0, 0, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username     = excluded.username,
                total_tipped = COALESCE(total_tipped, 0) + ?
        """, (user_id, username, amount, amount))
        await db.commit()


async def get_tip_leaderboard(limit: int = 10) -> list[dict]:
    """Fetch the top tippers."""
    db = await _get_db()
    async with db.execute("""
        SELECT user_id, username, COALESCE(total_tipped, 0) AS total_tipped
        FROM stats
        WHERE total_tipped > 0
        ORDER BY total_tipped DESC LIMIT ?
    """, (limit,)) as c:
        return [dict(r) for r in await c.fetchall()]


async def sweep_all_wallets() -> list[tuple[int, str, int]]:
    """One-time migration: Zeroes all wallets and returns amounts owed."""
    db = await _get_db()
    async with _write_lock:
        # Get everyone who has usable balance OR pending cashouts
        async with db.execute(
                "SELECT user_id, username, balance, pending_cashout FROM wallets WHERE balance > 0 OR pending_cashout > 0") as c:
            rows = await c.fetchall()

        payouts = []
        for r in rows:
            total = r[2] + r[3]  # Combine usable balance + already pending cashouts
            payouts.append((r[0], r[1], total))

        # Zero out the economy
        await db.execute("UPDATE wallets SET balance = 0, pending_cashout = 0")
        await db.execute("DELETE FROM chips_in_play")  # Safety wipe
        await db.commit()

        return payouts


# ── Cosmetics: Titles & Win Messages ─────────────────────────────────────────
 
import json, random
 
RARITY_LABEL = {
    "common":    "Common",
    "uncommon":   "Uncommon",
    "rare":       "Rare",
    "legendary":  "Legendary",
    "unique":     "Unique",
}
 
TITLES: dict[str, dict] = {
    # ── Achievement titles ──────────────────────────────────────────────────
    "grinder": {
        "display": "💪 Grinder",
        "description": "Play 100 hands",
        "rarity": "uncommon",
        "hidden": False,
    },
    "no_lifer": {
        "display": "💀 No Lifer",
        "description": "Play 1000 hands",
        "rarity": "rare",
        "hidden": False,
    },
    "high_roller": {
        "display": "🤑 High Roller",
        "description": "Win 100 hands",
        "rarity": "rare",
        "hidden": False,
    },
    "champion": {
        "display": "🏆 Champion",
        "description": "Win 500 hands",
        "rarity": "legendary",
        "hidden": False,
    },
    "stupidly_rich": {
        "display": "💸 Stupidly Rich",
        "description": "Net gain of +50,000 chips all-time",
        "rarity": "legendary",
        "hidden": False,
    },
    "hot_streak": {
        "display": "🔥 Hot Streak",
        "description": "Win 5 hands in a row",
        "rarity": "rare",
        "hidden": False,
    },
    "unstoppable": {
        "display": "⚡ Unstoppable",
        "description": "Win 10 hands in a row",
        "rarity": "legendary",
        "hidden": False,
    },
    "lucky": {
        "display": "🍀 Lucky",
        "description": "Win 5 hands with pocket aces",
        "rarity": "legendary",
        "hidden": False,
    },
    "all_in_hero": {
        "display": "All-In Hero",
        "description": "Win 25 all-in hands",
        "rarity": "rare",
        "hidden": False,
    },
    "quad_win": {
        "display": "Fantastic Four",
        "description": "Win a hand with Four of a Kind",
        "rarity": "rare",
        "hidden": False,
    },
    "quads_4": {
        "display": "QuadQuadQuadQuad",
        "description": "Win four hands with Four of a Kind",
        "rarity": "rare",
        "hidden": False,
    },
    "suited_up": {
        "display": "♠ Suited up.",
        "description": "Win a hand with a Straight Flush",
        "rarity": "legendary",
        "hidden": False,
    },
    "rf_win": {
        "display": "👑 Royalty",
        "description": "Win a hand with a Royal Flush",
        "rarity": "legendary",
        "hidden": False,
    },
    
    # ── Legendary rare drops ────────────────────────────────────────────────
    "blessed": {
        "display": "🌟 Blessed",
        "description": "Favored by the poker gods. Extremely rare.",
        "rarity": "legendary",
        "hidden": False,
    },
    "chosen_one": {
        "display": "✨ Chosen One",
        "description": "One in a million. The cards chose you.",
        "rarity": "legendary",
        "hidden": False,
    },
    
    # ── Nothing to see here ─────────────────────────────────────────────────
    "guard": {
        "display": "BELIEVES IN EGIRL SAROSHINA FOREVER 😍😍😍",
        "description": "",
        "rarity": "unique",
        "hidden": True,
        "special_user": 804762802451382283,
    },
    
    "bay": {
        "display": "the won't fix",
        "description": "",
        "rarity": "unique",
        "hidden": True,
        "special_user": 1339935869598961728,
    },
}
 
WIN_MESSAGES: dict[str, dict] = {
    # ── Common ─────────────────────────────────────────────────────────────
    "gg": {
        "display": "👋 gg",
        "description": "Thanks for playing!",
        "rarity": "common",
        "hidden": False,
    },
    # ── Achievement unlocks ─────────────────────────────────────────────────
    "noobs": {
        "display": "🇱 noobs",
        "description": "Play 50 hands",
        "rarity": "uncommon",
        "hidden": False,
    },
    "l_losers": {
        "display": "🤡 L losers",
        "description": "Play 500 hands",
        "rarity": "uncommon",
        "hidden": False,
    },
    "too_easy": {
        "display": "😤 Too easy",
        "description": "Win 50 hands",
        "rarity": "uncommon",
        "hidden": False,
    },
    "not_even_close": {
        "display": "💅 Not even close",
        "description": "Win 250 hands",
        "rarity": "uncommon",
        "hidden": False,
    },
    "boring": {
        "display": "🥱 Boring",
        "description": "Win 3 hands in a row",
        "rarity": "rare",
        "hidden": False,
    },
    "skill_issue": {
        "display": "🧠 Skill issue",
        "description": "Win 3 hands with pocket aces",
        "rarity": "rare",
        "hidden": False,
    },
    "touch_grass": {
        "display": "🌿 Touch grass",
        "description": "Play 5000 hands",
        "rarity": "legendary",
        "hidden": False,
    },
    "meow": {
        "display": "🐱 Meow",
        "description": "get wiped from inactivity",
        "rarity": "uncommon",
        "hidden": False,
    },
    "quad_winmsg": {
        "display": "Quad squad",
        "description": "Win a hand with Four of a Kind",
        "rarity": "uncommon",
        "hidden": False,
    },
    "straight_shit": {
        "display": "Got that shit straight 🗣",
        "description": "Win a hand with a Straight Flush",
        "rarity": "rare",
        "hidden": False,
    },
    "rf_winmsg": {
        "display": "👑 Peak poker achieved",
        "description": "Win a hand with a Royal Flush",
        "rarity": "legendary",
        "hidden": False,
    },
    # ── Legendary rare drop ─────────────────────────────────────────────────
    "touched_by_aces": {
        "display": "✨ The cards chose me",
        "description": "??? (extremely rare drop)",
        "rarity": "legendary",
        "hidden": False,
    },
    
    "blown_away": {
        "display": "🌬 Blown Away",
        "description": "Net loss of 10,000 all-time",
        "rarity": "legendary",
        "hidden": False,
    },
    
    # ── Nothing to see here ──────────────────────────────────────────────
    "guard": {
        "display": "dev hacks enabled 🗿",
        "description": "",
        "rarity": "unique",
        "hidden": True,
        "special_user": 804762802451382283,
    },
    
    "bay": {
        "display": "Diagnosis: winning. <:bay_lolipop:1488128797864890460>",
        "description": "",
        "rarity": "unique",
        "hidden": True,
        "special_user": 1339935869598961728,
    },
}
 
 
async def get_cosmetics(user_id: int) -> dict:
    """
    Returns the player's cosmetics dict (always safe to call, never None).
    Automatically unlocks special user cosmetics if the user_id matches.
    """
    db = await _get_db()
    async with db.execute(
        "SELECT active_title, active_win_msg, unlocked_titles, unlocked_win_msgs FROM player_cosmetics WHERE user_id=?",
        (user_id,)
    ) as c:
        row = await c.fetchone()
    
    if row:
        cosmetics = {
            "active_title":    row[0],
            "active_win_msg":  row[1],
            "unlocked_titles": json.loads(row[2] or "[]"),
            "unlocked_win_msgs": json.loads(row[3] or '["gg"]'),
        }
    else:
        # New player defaults
        cosmetics = {
            "active_title": None,
            "active_win_msg": None,
            "unlocked_titles": [],
            "unlocked_win_msgs": ["gg"],
        }
    
    # Auto-unlock special user cosmetics
    special_titles = [tid for tid, info in TITLES.items() 
                     if info.get("special_user") == user_id and tid not in cosmetics["unlocked_titles"]]
    special_winmsgs = [mid for mid, info in WIN_MESSAGES.items() 
                      if info.get("special_user") == user_id and mid not in cosmetics["unlocked_win_msgs"]]
    
    if special_titles or special_winmsgs:
        cosmetics["unlocked_titles"].extend(special_titles)
        cosmetics["unlocked_win_msgs"].extend(special_winmsgs)
        # Save the auto-unlocked cosmetics to database
        async with _write_lock:
            await db.execute("""
                INSERT INTO player_cosmetics (user_id, unlocked_titles, unlocked_win_msgs)
                VALUES (?, '[]', '["gg"]')
                ON CONFLICT(user_id) DO NOTHING
            """, (user_id,))
            await db.execute("""
                UPDATE player_cosmetics 
                SET unlocked_titles = ?, unlocked_win_msgs = ?
                WHERE user_id = ?
            """, (json.dumps(cosmetics["unlocked_titles"]), 
                  json.dumps(cosmetics["unlocked_win_msgs"]), 
                  user_id))
            await db.commit()
    
    return cosmetics
 
 
async def unlock_cosmetic(user_id: int, kind: str, cosmetic_id: str) -> bool:
    """Unlock a title ('title') or win message ('winmsg'). Returns True if newly unlocked."""
    cosmetics = await get_cosmetics(user_id)
    key = "unlocked_titles" if kind == "title" else "unlocked_win_msgs"
    if cosmetic_id in cosmetics[key]:
        return False  # Already owned
    cosmetics[key].append(cosmetic_id)
    db = await _get_db()
    col = "unlocked_titles" if kind == "title" else "unlocked_win_msgs"
    async with _write_lock:
        await db.execute("""
            INSERT INTO player_cosmetics (user_id, unlocked_titles, unlocked_win_msgs)
            VALUES (?, '[]', '["gg"]')
            ON CONFLICT(user_id) DO NOTHING
        """, (user_id,))
        await db.execute(
            f"UPDATE player_cosmetics SET {col} = ? WHERE user_id = ?",
            (json.dumps(cosmetics[key]), user_id)
        )
        await db.commit()
    return True
 
 
async def set_active_title(user_id: int, title_id: str | None) -> bool:
    """Equip a title. Pass None to remove. Returns False if not unlocked."""
    if title_id is not None:
        cosmetics = await get_cosmetics(user_id)
        if title_id not in cosmetics["unlocked_titles"]:
            return False
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO player_cosmetics (user_id, unlocked_titles, unlocked_win_msgs)
            VALUES (?, '[]', '["gg"]')
            ON CONFLICT(user_id) DO NOTHING
        """, (user_id,))
        await db.execute("UPDATE player_cosmetics SET active_title = ? WHERE user_id = ?", (title_id, user_id))
        await db.commit()
    return True
 
 
async def set_active_win_msg(user_id: int, msg_id: str | None) -> bool:
    """Equip a win message. Pass None to remove. Returns False if not unlocked."""
    if msg_id is not None:
        cosmetics = await get_cosmetics(user_id)
        if msg_id not in cosmetics["unlocked_win_msgs"]:
            return False
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO player_cosmetics (user_id, unlocked_titles, unlocked_win_msgs)
            VALUES (?, '[]', '["gg"]')
            ON CONFLICT(user_id) DO NOTHING
        """, (user_id,))
        await db.execute("UPDATE player_cosmetics SET active_win_msg = ? WHERE user_id = ?", (msg_id, user_id))
        await db.commit()
    return True
 

async def create_custom_cosmetic(kind: str, cosmetic_id: str, display: str, description: str = "", 
                                  rarity: str = "rare", hidden: bool = False) -> bool:
    """
    Create a custom title or win message dynamically and persist it to the database.
    kind: 'title' or 'winmsg'
    cosmetic_id: unique identifier (e.g., 'event_winner_2026')
    display: display text (e.g., '🏆 Event Winner')
    description: optional description
    rarity: common, uncommon, rare, legendary, or unique
    hidden: if True, only visible to users who own it
    
    Returns True if created, False if ID already exists.
    """
    catalog = TITLES if kind == "title" else WIN_MESSAGES
    
    if cosmetic_id in catalog:
        return False  # ID already exists
    
    # Add to in-memory catalog
    catalog[cosmetic_id] = {
        "display": display,
        "description": description,
        "rarity": rarity,
        "hidden": hidden,
    }
    
    # Persist to database
    db = await _get_db()
    async with _write_lock:
        try:
            await db.execute("""
                INSERT INTO custom_cosmetics (cosmetic_id, kind, display, description, rarity, hidden)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (cosmetic_id, kind, display, description, rarity, 1 if hidden else 0))
            await db.commit()
            return True
        except Exception as e:
            print(f"[database] Error saving custom cosmetic: {e}")
            # Remove from in-memory catalog if database save failed
            del catalog[cosmetic_id]
            return False


async def load_custom_cosmetics():
    """Load all custom cosmetics from the database into memory on startup."""
    db = await _get_db()
    try:
        async with db.execute("SELECT cosmetic_id, kind, display, description, rarity, hidden FROM custom_cosmetics") as c:
            rows = await c.fetchall()
            
        for row in rows:
            cosmetic_id, kind, display, description, rarity, hidden = row
            catalog = TITLES if kind == "title" else WIN_MESSAGES
            
            # Only load if not already defined (hardcoded cosmetics take precedence)
            if cosmetic_id not in catalog:
                catalog[cosmetic_id] = {
                    "display": display,
                    "description": description,
                    "rarity": rarity,
                    "hidden": bool(hidden),
                }
        
        if rows:
            print(f"✅ Loaded {len(rows)} custom cosmetic(s) from database")
    except Exception as e:
        print(f"[database] Error loading custom cosmetics: {e}")


def get_visible_cosmetics_for_user(user_id: int, owned_ids: set[str], catalog: dict) -> dict:
    """
    Filter cosmetics to only show:
    - Non-hidden cosmetics (visible to everyone)
    - Hidden cosmetics that the user owns
    - Special user cosmetics for that user
    
    Returns a filtered catalog dict.
    """
    visible = {}
    for cid, info in catalog.items():
        # Show if not hidden
        if not info.get("hidden", False):
            visible[cid] = info
        # Show if user owns it
        elif cid in owned_ids:
            visible[cid] = info
        # Show if it's a special cosmetic for this user
        elif info.get("special_user") == user_id:
            visible[cid] = info
    
    return visible


 
async def update_win_streak(user_id: int, won: bool) -> tuple[int, int]:
    """Increment or reset win streak. Returns (current_streak, max_streak)."""
    db = await _get_db()
    async with _write_lock:
        if won:
            await db.execute("""
                UPDATE stats
                SET win_streak     = win_streak + 1,
                    max_win_streak = MAX(max_win_streak, win_streak + 1)
                WHERE user_id = ?
            """, (user_id,))
        else:
            await db.execute("UPDATE stats SET win_streak = 0 WHERE user_id = ?", (user_id,))
        await db.commit()
        async with db.execute(
            "SELECT COALESCE(win_streak,0), COALESCE(max_win_streak,0) FROM stats WHERE user_id=?",
            (user_id,)
        ) as c:
            row = await c.fetchone()
            return (row[0], row[1]) if row else (0, 0)
 
 
async def increment_pocket_aces_wins(user_id: int) -> int:
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE stats SET pocket_aces_wins = pocket_aces_wins + 1 WHERE user_id = ?", (user_id,))
        await db.commit()
        async with db.execute("SELECT COALESCE(pocket_aces_wins,0) FROM stats WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone(); return row[0] if row else 0
 
 
async def increment_all_in_wins(user_id: int) -> int:
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE stats SET all_in_wins = all_in_wins + 1 WHERE user_id = ?", (user_id,))
        await db.commit()
        async with db.execute("SELECT COALESCE(all_in_wins,0) FROM stats WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone(); return row[0] if row else 0


async def increment_quads_wins(user_id: int) -> int:
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE stats SET quads_wins = quads_wins + 1 WHERE user_id = ?", (user_id,))
        await db.commit()
        async with db.execute("SELECT COALESCE(quads_wins,0) FROM stats WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone(); return row[0] if row else 0


async def increment_straight_flush_wins(user_id: int) -> int:
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE stats SET straight_flush_wins = straight_flush_wins + 1 WHERE user_id = ?", (user_id,))
        await db.commit()
        async with db.execute("SELECT COALESCE(straight_flush_wins,0) FROM stats WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone(); return row[0] if row else 0


async def increment_royal_flush_wins(user_id: int) -> int:
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE stats SET royal_flush_wins = royal_flush_wins + 1 WHERE user_id = ?", (user_id,))
        await db.commit()
        async with db.execute("SELECT COALESCE(royal_flush_wins,0) FROM stats WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone(); return row[0] if row else 0
 
async def _get_times_wiped(user_id: int) -> int:
    db = await _get_db()
    async with db.execute("SELECT COALESCE(times_wiped,0) FROM stats WHERE user_id=?", (user_id,)) as c:
        row = await c.fetchone()
        return row[0] if row else 0
 
async def check_achievements(user_id: int, won: bool = False, pot_won: int = 0) -> list[tuple[str, str]]:
    """
    Check for newly unlocked titles and win messages. Call after updating stats.
    Returns list of ('title'|'winmsg', cosmetic_id) tuples for anything newly earned.
    Batched: performs exactly 1 read + 1 write total, regardless of how many unlock.
    """
    db = await _get_db()

    # ── 1. One read: fetch stats + cosmetics together ────────────────────────
    async with db.execute("""
        SELECT COALESCE(s.hands_played,0), COALESCE(s.hands_won,0),
               COALESCE(s.chips_won,0),   COALESCE(s.chips_lost,0),
               COALESCE(s.win_streak,0),  COALESCE(s.max_win_streak,0),
               COALESCE(s.pocket_aces_wins,0), COALESCE(s.all_in_wins,0),
               COALESCE(s.quads_wins,0),  COALESCE(s.straight_flush_wins,0),
               COALESCE(s.royal_flush_wins,0),
               COALESCE(c.unlocked_titles,'[]'),
               COALESCE(c.unlocked_win_msgs,'["gg"]')
        FROM stats s
        LEFT JOIN player_cosmetics c ON s.user_id = c.user_id
        WHERE s.user_id = ?
    """, (user_id,)) as cur:
        row = await cur.fetchone()
    if not row:
        return []

    (hands_played, hands_won, chips_won, chips_lost, _, max_streak,
     aa_wins, allin_wins, quads_wins, sf_wins, rf_wins,
     _titles_json, _msgs_json) = row

    net = chips_won - chips_lost
    owned_titles = set(json.loads(_titles_json))
    owned_msgs   = set(json.loads(_msgs_json))

    # ── 2. Compute everything in-memory — zero DB calls ──────────────────────
    newly: list[tuple[str, str]] = []
    new_titles = list(owned_titles)
    new_msgs   = list(owned_msgs)

    # Only query times_wiped if meow isn't already owned — avoids a DB hit every hand
    times_wiped = 0
    if "meow" not in owned_msgs:
        times_wiped = await _get_times_wiped(user_id)

    title_checks = {
        "grinder":       hands_played >= 100,
        "no_lifer":      hands_played >= 1000,
        "high_roller":   hands_won    >= 100,
        "champion":      hands_won    >= 500,
        "stupidly_rich": net          >= 50000,
        "hot_streak":    max_streak   >= 5,
        "unstoppable":   max_streak   >= 10,
        "lucky":         aa_wins      >= 5,
        "all_in_hero":   allin_wins   >= 25,
        "quad_win":      quads_wins   >= 1,
        "quads_4":       quads_wins   >= 4,
        "suited_up":     sf_wins      >= 1,
        "rf_win":        rf_wins      >= 1,
    }
    for tid, condition in title_checks.items():
        if condition and tid not in owned_titles:
            newly.append(("title", tid))
            new_titles.append(tid)
            owned_titles.add(tid)   # prevent duplicates within this run

    if won:
        if "blessed" not in owned_titles and random.random() < 0.001:
            newly.append(("title", "blessed"))
            new_titles.append("blessed")
            owned_titles.add("blessed")
        if pot_won >= 1000 and "chosen_one" not in owned_titles and random.random() < 0.000001:
            newly.append(("title", "chosen_one"))
            new_titles.append("chosen_one")
            owned_titles.add("chosen_one")

    msg_checks = {
        "gg":             True,
        "noobs":          hands_played >= 50,
        "l_losers":       hands_played >= 500,
        "too_easy":       hands_won    >= 50,
        "not_even_close": hands_won    >= 250,
        "boring":         max_streak   >= 3,
        "skill_issue":    aa_wins      >= 3,
        "touch_grass":    hands_played >= 5000,
        "meow":           times_wiped  >= 1,
        "quad_winmsg":    quads_wins   >= 1,
        "straight_shit":  sf_wins      >= 1,
        "rf_winmsg":      rf_wins      >= 1,
        "blown_away":     net          <= -10000,
    }
    for mid, condition in msg_checks.items():
        if condition and mid not in owned_msgs:
            newly.append(("winmsg", mid))
            new_msgs.append(mid)
            owned_msgs.add(mid)

    if won and "touched_by_aces" not in owned_msgs and random.random() < 0.001:
        newly.append(("winmsg", "touched_by_aces"))
        new_msgs.append("touched_by_aces")

    if not newly:
        return []

    # ── 3. One write: persist all newly unlocked cosmetics together ───────────
    async with _write_lock:
        await db.execute("""
            INSERT INTO player_cosmetics (user_id, unlocked_titles, unlocked_win_msgs)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                unlocked_titles  = excluded.unlocked_titles,
                unlocked_win_msgs = excluded.unlocked_win_msgs
        """, (user_id, json.dumps(new_titles), json.dumps(new_msgs)))
        await db.commit()

    return newly

# INACTIVITY MONITOR

"""
Inactivity Monitor Extension

- Tracks last activity date AND hands played
- Requires minimum meaningful engagement (not just one hand every 3 days)
- Activity only counts if you actually play hands (not just join tables)
- Optional: Require minimum chips wagered to count as "active"
"""

# ────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────────────────────────────────

INACTIVITY_DAYS = 2  # Days before considering inactive
MIN_HANDS_PER_PERIOD = 10  # Minimum hands to count as "active" (prevents one-hand cheese)
MIN_CHIPS_WAGERED = 500  # Minimum total chips bet to count as active (optional, 0 to disable)
GRACE_PERIOD_DAYS = 0  # Extra day grace period before wiping (total = INACTIVITY_DAYS + GRACE_PERIOD)


# ────────────────────────────────────────────────────────────────────────────
# DATABASE SCHEMA ADDITIONS
# ────────────────────────────────────────────────────────────────────────────

async def init_inactivity_tracking(db: aiosqlite.Connection):
    """
    Add this to your init_db() function.
    Creates the necessary columns and tables for activity tracking.
    """
    async with db.execute("PRAGMA table_info(wallets)") as cursor:
        columns = [row[1] for row in await cursor.fetchall()]

    # Add last_activity timestamp to wallets
    if "last_activity" not in columns:
        await db.execute("ALTER TABLE wallets ADD COLUMN last_activity TEXT")
        # Set initial activity to now for existing users
        await db.execute("UPDATE wallets SET last_activity = ? WHERE last_activity IS NULL",
                         (datetime.utcnow().isoformat(),))

    # Add activity counter (rolling window)
    if "recent_hands" not in columns:
        await db.execute("ALTER TABLE wallets ADD COLUMN recent_hands INTEGER DEFAULT 0")

    if "recent_chips_wagered" not in columns:
        await db.execute("ALTER TABLE wallets ADD COLUMN recent_chips_wagered INTEGER DEFAULT 0")

    await db.commit()


# ────────────────────────────────────────────────────────────────────────────
# ACTIVITY TRACKING FUNCTIONS
# ────────────────────────────────────────────────────────────────────────────

async def mark_player_active(user_id: int, chips_amount: int = 0):
    db = await _get_db()
    now = datetime.utcnow().isoformat()
    async with _write_lock:
        await db.execute("""
            UPDATE wallets SET
                recent_hands         = CASE
                    WHEN (recent_hands + 1) >= ? AND (recent_chips_wagered + ?) >= ?
                    THEN 0
                    ELSE recent_hands + 1
                END,
                recent_chips_wagered = CASE
                    WHEN (recent_hands + 1) >= ? AND (recent_chips_wagered + ?) >= ?
                    THEN 0
                    ELSE recent_chips_wagered + ?
                END,
                last_activity        = CASE
                    WHEN (recent_hands + 1) >= ? AND (recent_chips_wagered + ?) >= ?
                    THEN MAX(last_activity, ?)
                    ELSE last_activity
                END
            WHERE user_id = ?
        """, (
            MIN_HANDS_PER_PERIOD, chips_amount, MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else 0,
            MIN_HANDS_PER_PERIOD, chips_amount, MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else 0,
            chips_amount,
            MIN_HANDS_PER_PERIOD, chips_amount, MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else 0,
            now,
            user_id,
        ))
        await db.commit()


async def reset_activity_counters(user_id: int):
    """
    Reset the rolling window counters for a user.
    Called after successful activity check or after inactivity wipe.
    """
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            UPDATE wallets 
            SET recent_hands = 0,
                recent_chips_wagered = 0
            WHERE user_id = ?
        """, (user_id,))
        await db.commit()


# ────────────────────────────────────────────────────────────────────────────
# INACTIVITY DETECTION
# ────────────────────────────────────────────────────────────────────────────

async def get_inactive_players() -> list[dict]:
    """
    Find players who are truly inactive (not gaming the system).

    Returns list of players who:
    1. Haven't been active for INACTIVITY_DAYS + GRACE_PERIOD_DAYS
    2. OR have been "active" but below minimum thresholds (cheese detection)
    """
    db = await _get_db()
    cutoff_date = datetime.utcnow() - timedelta(days=INACTIVITY_DAYS + GRACE_PERIOD_DAYS)
    cutoff_iso = cutoff_date.isoformat()

    # Find players with old activity date AND insufficient engagement
    async with db.execute("""
        SELECT 
            user_id, 
            username, 
            balance,
            pending_cashout,
            last_activity,
            recent_hands,
            recent_chips_wagered
        FROM wallets
        WHERE (balance > 0 OR pending_cashout > 0)
          AND (
              last_activity < ? 
              OR (
                  last_activity < ? 
                  AND recent_hands < ?
              )
              OR (
                  last_activity < ?
                  AND recent_chips_wagered < ?
              )
          )
    """, (
            cutoff_iso,  # Truly inactive
            (datetime.utcnow() - timedelta(days=INACTIVITY_DAYS)).isoformat(),  # Active but not enough hands
            MIN_HANDS_PER_PERIOD,
            (datetime.utcnow() - timedelta(days=INACTIVITY_DAYS)).isoformat(),  # Active but not enough wagered
            MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else -1  # Disable if 0
    )) as c:
        return [dict(row) for row in await c.fetchall()]


async def wipe_inactive_players() -> list[dict]:
    # Wipe chips from inactive players and return the list of affected users.
    # Also logs the action
    now = datetime.utcnow().isoformat()
    inactive = await get_inactive_players()
    if not inactive:
        return []

    db = await _get_db()
    wiped = []

    async with _write_lock:
        for player in inactive:
            user_id = player["user_id"]
            username = player["username"]
            total_wiped = player["balance"]

            if total_wiped <= 0:
                continue

            # Wipe their chips
            await db.execute("""
                UPDATE wallets 
                SET balance = 0,
                    recent_hands = 0,
                    recent_chips_wagered = 0,
                    last_activity = ?        
                WHERE user_id = ?
            """, (now, user_id))

            await db.execute("""
                            UPDATE stats SET times_wiped = COALESCE(times_wiped, 0) + 1 WHERE user_id = ?
                        """, (user_id,))

            # Log to audit
            await db.execute("""
                INSERT INTO audit_log (ts, action, user_id, user_name, detail)
                VALUES (?, 'INACTIVITY_WIPE', ?, ?, ?)
            """, (
                datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
                user_id,
                username,
                f"Wiped {total_wiped} chips after {INACTIVITY_DAYS}+ days inactive (hands: {player['recent_hands']}, wagered: {player['recent_chips_wagered']})"
            ))

            wiped.append({
                "user_id": user_id,
                "username": username,
                "amount_wiped": total_wiped,
                "last_activity": player["last_activity"],
                "recent_hands": player["recent_hands"],
                "recent_chips_wagered": player["recent_chips_wagered"]
            })

        await db.commit()

    return wiped


'''
# ────────────────────────────────────────────────────────────────────────────
# (optional) WARNING SYSTEM 
# ────────────────────────────────────────────────────────────────────────────

async def get_players_at_risk() -> list[dict]:
    db = await _get_db()
    wipe_cutoff    = (datetime.utcnow() - timedelta(days=INACTIVITY_DAYS)).isoformat()
    warning_cutoff = (datetime.utcnow() - timedelta(days=max(0, INACTIVITY_DAYS - 1))).isoformat()
    async with db.execute("""
        SELECT user_id, username, balance, pending_cashout, last_activity,
               recent_hands, recent_chips_wagered
        FROM wallets
        WHERE (balance > 0 OR pending_cashout > 0)
          AND last_activity >= ? AND last_activity < ?
    """, (wipe_cutoff, warning_cutoff)) as c:
        return [dict(row) for row in await c.fetchall()]
'''


# ────────────────────────────────────────────────────────────────────────────
# BACKGROUND TASK FOR BOT
# ────────────────────────────────────────────────────────────────────────────



# ────────────────────────────────────────────────────────────────────────────
# INTEGRATION POINTS
# ────────────────────────────────────────────────────────────────────────────

"""
OPTIONAL: Add admin command to manually check status:
    @app_commands.command(name="activity_check")
    async def activity_check(self, interaction: discord.Interaction, user: discord.Member = None):
        target = user or interaction.user
        stats = await get_player_activity_stats(target.id)
        # Display stats...
"""


# ────────────────────────────────────────────────────────────────────────────
# HELPER: Get player's current activity status
# ────────────────────────────────────────────────────────────────────────────

async def get_player_activity_stats(user_id: int) -> dict | None:
    """
    Get detailed activity stats for a player.
    Useful for admin commands or player self-check.
    """
    db = await _get_db()
    async with db.execute("""
        SELECT 
            username,
            balance,
            pending_cashout,
            last_activity,
            recent_hands,
            recent_chips_wagered
        FROM wallets
        WHERE user_id = ?
    """, (user_id,)) as c:
        row = await c.fetchone()
        if not row:
            return None

        data = dict(row)
        last_active = datetime.fromisoformat(data["last_activity"])
        days_inactive = (datetime.utcnow() - last_active).days
        days_until_wipe = max(0, (INACTIVITY_DAYS + GRACE_PERIOD_DAYS) - days_inactive)

        data["days_inactive"] = days_inactive
        data["days_until_wipe"] = days_until_wipe
        data["is_at_risk"] = days_until_wipe <= 1
        data["meets_hand_requirement"] = data["recent_hands"] >= MIN_HANDS_PER_PERIOD
        data["meets_wager_requirement"] = data[
                                              "recent_chips_wagered"] >= MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else True

        return data

async def get_cosmetics_bulk(user_ids: list[int]) -> dict[int, dict]:
    """Fetch active_title for multiple players in one query. Returns {user_id: cosmetics_dict}."""
    if not user_ids:
        return {}
    db = await _get_db()
    placeholders = ",".join("?" * len(user_ids))
    async with db.execute(
        f"SELECT user_id, active_title, active_win_msg, unlocked_titles, unlocked_win_msgs "
        f"FROM player_cosmetics WHERE user_id IN ({placeholders})",
        tuple(user_ids)
    ) as c:
        rows = await c.fetchall()

    result = {}
    for row in rows:
        result[row[0]] = {
            "active_title":     row[1],
            "active_win_msg":   row[2],
            "unlocked_titles":  json.loads(row[3] or "[]"),
            "unlocked_win_msgs": json.loads(row[4] or '["gg"]'),
        }
    # Fill in defaults for players not yet in player_cosmetics
    for uid in user_ids:
        if uid not in result:
            result[uid] = {
                "active_title": None, "active_win_msg": None,
                "unlocked_titles": [], "unlocked_win_msgs": ["gg"],
            }
    return result

async def sync_chips_in_play(player_chip_map: dict[int, int]):
    """
    Batch update chips_in_play for all players in one transaction.
    player_chip_map: {user_id: total_chips} — pass 0 to clear that player.
    """
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