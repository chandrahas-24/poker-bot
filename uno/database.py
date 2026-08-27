"""
UNO's own wallet/economy database — deliberately separate from
poker/database.py's SQLite file and tables. A player's UNO chip balance
is tracked independently of their poker balance.

The functions here are a direct port of poker/database.py's economy
functions (same names, same signatures, same behavior) so uno_cog.py can
use them as a drop-in — only the underlying DB file and schema differ.
Anything poker-specific (hands, showdowns, blinds, tournaments, cosmetics,
etc.) was intentionally left out; this only carries what UNO's economy
actually needs: wallets, chip_log, chips_in_play, and inactivity wipes.
"""
import aiosqlite
import asyncio
import math
from datetime import datetime, timedelta
import config

DB_PATH = config.UNO_DB_PATH

INACTIVITY_DAYS = config.UNO_INACTIVITY_DAYS
MIN_ROUNDS_PER_PERIOD = config.UNO_MIN_ROUNDS_PER_PERIOD
MIN_CHIPS_WAGERED = config.UNO_MIN_CHIPS_WAGERED
WIPE_TAX_RATE = config.UNO_WIPE_TAX_RATE

_db: aiosqlite.Connection | None = None
_write_lock = asyncio.Lock()


async def _get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
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
            CREATE TABLE IF NOT EXISTS wallets (
                user_id              INTEGER PRIMARY KEY,
                username             TEXT NOT NULL,
                balance              INTEGER DEFAULT 0,
                pending_cashout      INTEGER DEFAULT 0,
                last_activity        TEXT,
                recent_rounds        INTEGER DEFAULT 0,
                recent_chips_wagered INTEGER DEFAULT 0
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
            CREATE TABLE IF NOT EXISTS chips_in_play (
                user_id  INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                amount   INTEGER NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS house_revenue (
                ts     TEXT,
                amount INTEGER,
                source TEXT DEFAULT 'game'
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
            CREATE TABLE IF NOT EXISTS currency_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                event_type  TEXT NOT NULL,
                amount      INTEGER NOT NULL,
                description TEXT NOT NULL,
                ts          TEXT NOT NULL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_uno_currency_user ON currency_log(user_id)")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS uno_bans (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id   INTEGER NOT NULL,
                user_id    INTEGER NOT NULL,
                username   TEXT NOT NULL,
                banned_by  INTEGER NOT NULL,
                ts         TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id        INTEGER PRIMARY KEY,
                manager_role_id INTEGER
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                user_id       INTEGER PRIMARY KEY,
                username      TEXT NOT NULL,
                rounds_played INTEGER DEFAULT 0,
                rounds_won    INTEGER DEFAULT 0,
                chips_won     INTEGER DEFAULT 0,
                chips_lost    INTEGER DEFAULT 0,
                times_wiped   INTEGER DEFAULT 0
            )
        """)
        await db.commit()


# ── Wallet / chip movement ──────────────────────────────────────────────────

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
        # Dust revive: only reset the inactivity clock if their balance was below the min bet
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance, last_activity, recent_rounds, recent_chips_wagered)
            VALUES (?, ?, MAX(0, ?), ?, 0, 0)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                last_activity = CASE WHEN balance < ? THEN ? ELSE COALESCE(wallets.last_activity, ?) END,
                recent_rounds = CASE WHEN balance < ? THEN 0 ELSE COALESCE(wallets.recent_rounds, 0) END,
                recent_chips_wagered = CASE WHEN balance < ? THEN 0 ELSE COALESCE(wallets.recent_chips_wagered, 0) END,
                balance = MAX(0, balance + ?)
        """, (user_id, user_name, amount, now,
              config.UNO_MIN_BET, now, now,
              config.UNO_MIN_BET,
              config.UNO_MIN_BET,
              amount))

        await db.execute("""
            INSERT INTO chip_log (ts, admin_id, admin_name, user_id, user_name, amount, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (now, admin_id, admin_name, user_id, user_name, amount, note))
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
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            success = bool(row and row[0] > 0)
        await db.commit()
        return success


async def return_chips(user_id: int, amount: int):
    db = await _get_db()
    now = datetime.utcnow().isoformat()
    async with _write_lock:
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance, last_activity)
            VALUES (?, 'Unknown Player', ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET balance = balance + ?
        """, (user_id, amount, now, amount))
        await db.commit()


async def upsert_wallet_name(user_id: int, username: str):
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance) VALUES (?, ?, 0)
            ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
        """, (user_id, username))
        await db.commit()


async def get_wallet(user_id: int) -> tuple[int, int]:
    """Returns (available_balance, pending_cashout)."""
    db = await _get_db()
    async with db.execute("SELECT balance, pending_cashout FROM wallets WHERE user_id=?", (user_id,)) as c:
        row = await c.fetchone()
        return (row[0], row[1]) if row else (0, 0)


async def request_cashout(user_id: int, amount: int) -> bool:
    """Moves chips to the pending vault — no tax, staff pays it out manually."""
    db = await _get_db()
    async with _write_lock:
        await db.execute(
            "UPDATE wallets SET balance = balance - ?, pending_cashout = pending_cashout + ? "
            "WHERE user_id = ? AND balance >= ?",
            (amount, amount, user_id, amount)
        )
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            success = bool(row and row[0] > 0)
        await db.commit()
        return success


async def pay_cashout(user_id: int, amount: int) -> bool:
    """Staff confirms a payout: deducts the amount from the pending vault."""
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


# ── Currency log (per-transaction audit trail for /uno currencylog) ────────

async def log_currency_event(user_id: int, event_type: str, amount: int, description: str):
    """event_type: 'Round', 'Cash In', 'Cash Out', 'Wipe', or similar — mirrors poker's convention."""
    if amount == 0:
        return  # don't clutter the log with 0-chip events
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO currency_log (user_id, event_type, amount, description, ts)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, event_type, amount, description, datetime.utcnow().isoformat()))
        await db.commit()


async def get_currency_logs(user_id: int) -> list[dict]:
    db = await _get_db()
    async with db.execute(
        "SELECT * FROM currency_log WHERE user_id = ? ORDER BY id DESC", (user_id,)
    ) as c:
        return [dict(r) for r in await c.fetchall()]


# ── Bans (guild-wide — UNO has no named tables to scope a ban to) ──────────

async def ban_player(guild_id: int, user_id: int, username: str, banned_by: int) -> bool:
    db = await _get_db()
    async with _write_lock:
        async with db.execute(
            "SELECT id FROM uno_bans WHERE guild_id=? AND user_id=?", (guild_id, user_id)
        ) as c:
            if await c.fetchone():
                return False
        await db.execute(
            "INSERT INTO uno_bans (guild_id, user_id, username, banned_by, ts) VALUES (?,?,?,?,?)",
            (guild_id, user_id, username, banned_by, datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
        )
        await db.commit()
    return True


async def unban_player(guild_id: int, user_id: int) -> bool:
    db = await _get_db()
    async with _write_lock:
        async with db.execute(
            "SELECT id FROM uno_bans WHERE guild_id=? AND user_id=?", (guild_id, user_id)
        ) as c:
            existed = bool(await c.fetchone())
        await db.execute(
            "DELETE FROM uno_bans WHERE guild_id=? AND user_id=?", (guild_id, user_id)
        )
        await db.commit()
        return existed


async def is_banned(guild_id: int, user_id: int) -> bool:
    db = await _get_db()
    async with db.execute(
        "SELECT id FROM uno_bans WHERE guild_id=? AND user_id=?", (guild_id, user_id)
    ) as c:
        return bool(await c.fetchone())


async def get_all_bans(guild_id: int) -> list[dict]:
    db = await _get_db()
    async with db.execute("""
        SELECT user_id, username, banned_by, ts FROM uno_bans
        WHERE guild_id = ? ORDER BY ts DESC
    """, (guild_id,)) as c:
        return [dict(r) for r in await c.fetchall()]


# ── Per-guild settings (manager_role_id — cached, same shape as poker's) ────

_settings_cache: dict[int, dict] = {}
_cache_lock = asyncio.Lock()


async def get_settings(guild_id: int) -> dict:
    async with _cache_lock:
        if guild_id in _settings_cache:
            return _settings_cache[guild_id].copy()

    db = await _get_db()
    async with db.execute("SELECT * FROM guild_settings WHERE guild_id=?", (guild_id,)) as c:
        row = await c.fetchone()
        settings = dict(row) if row else {"guild_id": guild_id, "manager_role_id": None}

    async with _cache_lock:
        _settings_cache[guild_id] = settings.copy()
    return settings


async def set_settings(guild_id: int, **kwargs):
    current = await get_settings(guild_id)
    current.update({k: v for k, v in kwargs.items() if v is not None})
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO guild_settings (guild_id, manager_role_id)
            VALUES (:guild_id, :manager_role_id)
            ON CONFLICT(guild_id) DO UPDATE SET manager_role_id = excluded.manager_role_id
        """, current)
        await db.commit()
    async with _cache_lock:
        _settings_cache[guild_id] = current.copy()


# ── Player stats / leaderboard ──────────────────────────────────────────────

async def record_round_result(user_id: int, username: str, won: bool, net_chips: int, chips_wagered: int = 0):
    """Call once per bettor when a round settles. Single-commit upsert,
    same shape as poker's record_hand_full — also feeds activity tracking."""
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO stats (user_id, username, rounds_played, rounds_won, chips_won, chips_lost)
            VALUES (?, ?, 1, ?, MAX(0,?), MAX(0,?))
            ON CONFLICT(user_id) DO UPDATE SET
                username      = excluded.username,
                rounds_played = rounds_played + 1,
                rounds_won    = rounds_won    + ?,
                chips_won     = chips_won     + MAX(0, ?),
                chips_lost    = chips_lost    + MAX(0, ?)
        """, (
            user_id, username, (1 if won else 0), net_chips, -net_chips,
            (1 if won else 0), net_chips, -net_chips,
        ))
        await db.commit()

    if abs(net_chips) > 0 or chips_wagered > 0:
        await mark_player_active(user_id, max(abs(net_chips), chips_wagered))


async def get_leaderboard(limit: int = 10) -> list[dict]:
    db = await _get_db()
    async with db.execute("""
        SELECT s.user_id, s.username, s.rounds_played, s.rounds_won, s.chips_won, s.chips_lost,
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
        SELECT s.username, s.rounds_played, s.rounds_won, s.chips_won, s.chips_lost,
               (s.chips_won - s.chips_lost) AS net_chips,
               COALESCE(w.balance, 0) AS wallet,
               COALESCE(s.times_wiped, 0) AS times_wiped
        FROM stats s LEFT JOIN wallets w ON s.user_id = w.user_id
        WHERE s.user_id = ?
    """, (user_id,)) as c:
        row = await c.fetchone()
        return dict(row) if row else None


async def delete_player_stats(user_id: int) -> bool:
    db = await _get_db()
    async with _write_lock:
        await db.execute("DELETE FROM stats WHERE user_id=?", (user_id,))
        async with db.execute("SELECT changes()") as c:
            row = await c.fetchone()
            deleted = bool(row and row[0] > 0)
        await db.commit()
        return deleted


# ── Revenue / economy dashboards ────────────────────────────────────────────

async def log_house_revenue(amount: int, source: str = 'game'):
    """source='wipe_tax' (inactivity wipes) or 'adjustment' (manual /unoadmin adjustrevenue)."""
    db = await _get_db()
    async with _write_lock:
        await db.execute("INSERT INTO house_revenue (ts, amount, source) VALUES (?, ?, ?)",
                          (datetime.utcnow().isoformat(), amount, source))
        await db.commit()


async def get_revenue_stats() -> dict:
    db = await _get_db()
    now = datetime.utcnow()
    daily_cut = (now - timedelta(hours=24)).isoformat()
    weekly_cut = (now - timedelta(hours=168)).isoformat()
    monthly_cut = (now - timedelta(hours=720)).isoformat()

    async with db.execute("""
        SELECT
            COALESCE(SUM(amount), 0),
            COALESCE(SUM(CASE WHEN ts >= ? AND source != 'adjustment' THEN amount ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN ts >= ? AND source != 'adjustment' THEN amount ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN ts >= ? AND source != 'adjustment' THEN amount ELSE 0 END), 0)
        FROM house_revenue
    """, (monthly_cut, weekly_cut, daily_cut)) as c:
        row = await c.fetchone()

    return {"all_time": row[0], "monthly": row[1], "weekly": row[2], "daily": row[3]}


async def get_economy_totals() -> tuple[int, int]:
    """Returns (total_available_wallets, total_pending_cashouts)."""
    db = await _get_db()
    async with db.execute("SELECT SUM(balance), SUM(pending_cashout) FROM wallets") as c:
        row = await c.fetchone()
        return (row[0] or 0, row[1] or 0)


async def get_total_chips_in_play() -> int:
    db = await _get_db()
    async with db.execute("SELECT SUM(amount) FROM chips_in_play") as c:
        row = await c.fetchone()
        return row[0] or 0


# ── Chips currently locked into an active table ─────────────────────────────

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
    """Same crash-recovery role as poker's version: on bot restart, whatever
    was still sitting in chips_in_play (tables that never settled) gets
    folded back into the owner's balance rather than being lost."""
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


# ── Inactivity wipe ──────────────────────────────────────────────────────────
# Same shape as poker's: a rolling "did they play enough recently" window,
# and a same-rate (20%) tax + auto-cashout of the rest for anyone who didn't.

async def mark_player_active(user_id: int, chips_wagered: int = 0):
    """Call once per player when a round they bet in ends. Bumps the rolling
    window, resetting it (and the inactivity clock) once they've cleared
    both the round-count and chips-wagered bar for the period."""
    db = await _get_db()
    now = datetime.utcnow().isoformat()
    async with _write_lock:
        await db.execute("""
            UPDATE wallets SET
                recent_rounds = CASE
                    WHEN (recent_rounds + 1) >= ? AND (recent_chips_wagered + ?) >= ?
                    THEN 0 ELSE recent_rounds + 1 END,
                recent_chips_wagered = CASE
                    WHEN (recent_rounds + 1) >= ? AND (recent_chips_wagered + ?) >= ?
                    THEN 0 ELSE recent_chips_wagered + ? END,
                last_activity = CASE
                    WHEN (recent_rounds + 1) >= ? AND (recent_chips_wagered + ?) >= ?
                    THEN MAX(COALESCE(last_activity, ?), ?) ELSE COALESCE(last_activity, ?) END
            WHERE user_id = ?
        """, (
            MIN_ROUNDS_PER_PERIOD, chips_wagered, MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else 0,
            MIN_ROUNDS_PER_PERIOD, chips_wagered, MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else 0, chips_wagered,
            MIN_ROUNDS_PER_PERIOD, chips_wagered, MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else 0, now, now, now,
            user_id,
        ))
        await db.commit()


async def get_players_at_risk() -> list[dict]:
    """O(1) query for players exactly 1 day away from a wipe."""
    db = await _get_db()
    async with db.execute("""
        SELECT user_id, username, balance, pending_cashout,
               IFNULL(recent_rounds, 0) as recent_rounds,
               IFNULL(recent_chips_wagered, 0) as recent_chips_wagered,
               last_activity
        FROM wallets
        WHERE balance > 0 AND last_activity IS NOT NULL
    """) as cursor:
        rows = [dict(r) for r in await cursor.fetchall()]

    at_risk = []
    now = datetime.utcnow()
    for r in rows:
        try:
            last_active = datetime.fromisoformat(r["last_activity"])
        except (ValueError, TypeError):
            continue
        days_inactive = (now - last_active).days
        meets_rounds = r["recent_rounds"] >= MIN_ROUNDS_PER_PERIOD
        meets_wager = r["recent_chips_wagered"] >= MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else True
        if not (meets_rounds and meets_wager) and days_inactive == (INACTIVITY_DAYS - 1):
            r["days_inactive"] = days_inactive
            at_risk.append(r)
    return at_risk


async def get_inactive_players() -> list[dict]:
    db = await _get_db()
    async with db.execute("""
        SELECT user_id, username, balance, pending_cashout,
               IFNULL(recent_rounds, 0) as recent_rounds,
               IFNULL(recent_chips_wagered, 0) as recent_chips_wagered,
               last_activity
        FROM wallets
        WHERE balance > 0 AND last_activity IS NOT NULL
    """) as cursor:
        rows = [dict(r) for r in await cursor.fetchall()]

    inactive = []
    now = datetime.utcnow()
    for r in rows:
        try:
            last_active = datetime.fromisoformat(r["last_activity"])
        except (ValueError, TypeError):
            continue
        days_inactive = (now - last_active).days
        meets_rounds = r["recent_rounds"] >= MIN_ROUNDS_PER_PERIOD
        meets_wager = r["recent_chips_wagered"] >= MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else True
        if not (meets_rounds and meets_wager) and days_inactive >= INACTIVITY_DAYS:
            r["days_inactive"] = days_inactive
            inactive.append(r)
    return inactive


async def wipe_inactive_players() -> list[dict]:
    """Wipe chips from inactive players — 20% tax (logged as house revenue),
    the remaining 80% auto-queued as a pending cashout. Same split poker uses."""
    now = datetime.utcnow().isoformat()
    inactive = await get_inactive_players()
    if not inactive:
        return []

    db = await _get_db()
    wiped = []
    async with _write_lock:
        for player in inactive:
            user_id, username, balance = player["user_id"], player["username"], player["balance"]
            if balance <= 0:
                continue

            tax_amount = math.ceil(balance * WIPE_TAX_RATE)
            cashout_amount = balance - tax_amount

            await db.execute("""
                UPDATE wallets
                SET balance = 0,
                    pending_cashout = pending_cashout + ?,
                    recent_rounds = 0,
                    recent_chips_wagered = 0,
                    last_activity = ?
                WHERE user_id = ?
            """, (cashout_amount, now, user_id))
            await db.execute("""
                INSERT INTO stats (user_id, username, times_wiped) VALUES (?, ?, 1)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    times_wiped = COALESCE(times_wiped, 0) + 1
            """, (user_id, username))

            await db.execute(
                "INSERT INTO house_revenue (ts, amount, source) VALUES (?, ?, 'wipe_tax')",
                (now, tax_amount)
            )
            await db.execute("""
                INSERT INTO audit_log (ts, action, user_id, user_name, detail)
                VALUES (?, 'INACTIVITY_WIPE', ?, ?, ?)
            """, (
                datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), user_id, username,
                (f"Wiped {balance} chips after {INACTIVITY_DAYS}+ days inactive "
                 f"(rounds: {player['recent_rounds']}, wagered: {player['recent_chips_wagered']}) — "
                 f"tax: {tax_amount}, queued cashout: {cashout_amount}")
            ))

            if tax_amount > 0:
                await db.execute("""
                    INSERT INTO currency_log (user_id, event_type, amount, description, ts)
                    VALUES (?, 'Wipe', ?, 'Inactivity Tax (20%)', ?)
                """, (user_id, -tax_amount, now))
            if cashout_amount > 0:
                await db.execute("""
                    INSERT INTO currency_log (user_id, event_type, amount, description, ts)
                    VALUES (?, 'Cash Out', ?, 'Auto-Cashout (Inactivity)', ?)
                """, (user_id, -cashout_amount, now))

            wiped.append({
                "user_id": user_id,
                "username": username,
                "amount_wiped": balance,
                "tax_amount": tax_amount,
                "cashout_amount": cashout_amount,
                "last_activity": player["last_activity"],
                "recent_rounds": player["recent_rounds"],
                "recent_chips_wagered": player["recent_chips_wagered"],
            })

        await db.commit()
    return wiped