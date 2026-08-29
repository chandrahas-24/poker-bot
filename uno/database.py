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
                manager_role_id INTEGER,
                log_channel_id  INTEGER
            )
        """)
        for col, coltype in [("manager_role_id", "INTEGER"), ("log_channel_id", "INTEGER")]:
            try:
                await db.execute(f"ALTER TABLE guild_settings ADD COLUMN {col} {coltype}")
            except aiosqlite.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    raise
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
        # Structured round log — one row per settled round (round_log) plus one
        # row per bettor in that round (round_log_players), instead of poker's
        # hand_log free-text summary blob. Every column here is a plain number
        # or ID, so it's directly queryable via /unoadmin sql or an external
        # tool (e.g. "average net per player over time", "tax collected per
        # week", "win rate by deck count") without parsing text.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS round_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                round_uuid  TEXT UNIQUE,
                ts          TEXT NOT NULL,
                guild_id    INTEGER NOT NULL,
                channel_id  INTEGER NOT NULL,
                num_players INTEGER NOT NULL,
                num_decks   INTEGER NOT NULL,
                total_pot   INTEGER NOT NULL,
                total_tax   INTEGER NOT NULL,
                winner_id   INTEGER,
                winner_name TEXT
            )
        """)
        try:
            await db.execute("ALTER TABLE round_log ADD COLUMN round_uuid TEXT")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_round_log_uuid ON round_log(round_uuid)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_round_log_guild ON round_log(guild_id, ts)")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS round_log_players (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id  INTEGER NOT NULL REFERENCES round_log(id),
                user_id   INTEGER NOT NULL,
                username  TEXT NOT NULL,
                bet       INTEGER NOT NULL,
                gross     INTEGER NOT NULL,
                tax       INTEGER NOT NULL,
                net       INTEGER NOT NULL,
                placement INTEGER,
                won       INTEGER NOT NULL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_round_log_players_round ON round_log_players(round_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_round_log_players_user ON round_log_players(user_id)")

        # Full per-turn replay log — batched writes only (see
        # log_turn_events_bulk below and the flush task in uno_cog.py).
        # round_uuid links these rows to their eventual round_log row
        # (generated by the engine at round-start, before round_log's
        # autoincrement id exists yet — see GameState.round_uuid).
        await db.execute("""
            CREATE TABLE IF NOT EXISTS turn_log (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                round_uuid       TEXT NOT NULL,
                turn_num         INTEGER NOT NULL,
                ts               TEXT NOT NULL,
                user_id          INTEGER NOT NULL,
                username         TEXT NOT NULL,
                action           TEXT NOT NULL,
                card             TEXT,
                chosen_color     TEXT,
                hand_size_after  INTEGER,
                direction        INTEGER,
                extra            TEXT
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_turn_log_round ON turn_log(round_uuid, turn_num)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_turn_log_user ON turn_log(user_id)")
        await db.commit()


# ── Round log (structured — see round_log/round_log_players above) ─────────

async def log_round(round_uuid: str, guild_id: int, channel_id: int, num_players: int, num_decks: int,
                     total_pot: int, total_tax: int, winner_id: int | None, winner_name: str | None,
                     players: list[dict]) -> int:
    """players: [{user_id, username, bet, gross, tax, net, placement, won}, ...]
    One call per settled round — writes the parent row plus one child row
    per bettor in a single transaction. round_uuid is the same id the
    engine generated at round-start (GameState.round_uuid) — it's what
    lets turn_log rows (written throughout the round, before this row
    exists) join back to this one. Returns the new round_log.id."""
    db = await _get_db()
    ts = datetime.utcnow().isoformat()
    async with _write_lock:
        cursor = await db.execute("""
            INSERT INTO round_log (round_uuid, ts, guild_id, channel_id, num_players, num_decks,
                                    total_pot, total_tax, winner_id, winner_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (round_uuid, ts, guild_id, channel_id, num_players, num_decks, total_pot, total_tax, winner_id, winner_name))
        round_id = cursor.lastrowid
        await db.executemany("""
            INSERT INTO round_log_players (round_id, user_id, username, bet, gross, tax, net, placement, won)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (round_id, p["user_id"], p["username"], p["bet"], p["gross"], p["tax"], p["net"],
             p.get("placement"), 1 if p["won"] else 0)
            for p in players
        ])
        await db.commit()
    return round_id


async def log_turn_events_bulk(events: list[dict]):
    """The batching write: one transaction for however many turn events have
    piled up since the last flush — across every active table, not just one
    — instead of a commit (and disk fsync) per individual play/draw/pass.
    Called by the cog's periodic flush task and once more at round-end to
    catch anything not yet flushed. No-ops on an empty list."""
    if not events:
        return
    db = await _get_db()
    async with _write_lock:
        await db.executemany("""
            INSERT INTO turn_log (round_uuid, turn_num, ts, user_id, username, action,
                                   card, chosen_color, hand_size_after, direction, extra)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (e["round_uuid"], e["turn_num"], e["ts"], e["user_id"], e["username"], e["action"],
             e.get("card"), e.get("chosen_color"), e.get("hand_size_after"), e.get("direction"), e.get("extra"))
            for e in events
        ])
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
        settings = dict(row) if row else {"guild_id": guild_id, "manager_role_id": None, "log_channel_id": None}

    async with _cache_lock:
        _settings_cache[guild_id] = settings.copy()
    return settings


async def set_settings(guild_id: int, **kwargs):
    current = await get_settings(guild_id)
    current.update({k: v for k, v in kwargs.items() if v is not None})
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO guild_settings (guild_id, manager_role_id, log_channel_id)
            VALUES (:guild_id, :manager_role_id, :log_channel_id)
            ON CONFLICT(guild_id) DO UPDATE SET
                manager_role_id = excluded.manager_role_id,
                log_channel_id  = excluded.log_channel_id
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


async def get_player_activity_stats(user_id: int) -> dict | None:
    """Detailed activity stats for a player — admin check_inactive / self-check myactivity."""
    db = await _get_db()
    async with db.execute("""
        SELECT username, balance, pending_cashout, last_activity, recent_rounds, recent_chips_wagered
        FROM wallets WHERE user_id = ?
    """, (user_id,)) as c:
        row = await c.fetchone()
        if not row:
            return None

    data = dict(row)
    if not data.get("last_activity"):
        return None
    last_active = datetime.fromisoformat(data["last_activity"])
    days_inactive = (datetime.utcnow() - last_active).days
    days_until_wipe = max(0, INACTIVITY_DAYS - days_inactive)

    data["days_inactive"] = days_inactive
    data["days_until_wipe"] = days_until_wipe
    data["is_at_risk"] = days_until_wipe <= 1
    data["meets_rounds_requirement"] = data["recent_rounds"] >= MIN_ROUNDS_PER_PERIOD
    data["meets_wager_requirement"] = data["recent_chips_wagered"] >= MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else True
    return data


async def reset_database(admin_id: int, admin_name: str):
    """Wipes ALL UNO economy data — wallets, stats, logs, settings, bans. Irreversible."""
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    db = await _get_db()
    async with _write_lock:
        await db.execute("DELETE FROM wallets")
        await db.execute("DELETE FROM stats")
        await db.execute("DELETE FROM chip_log")
        await db.execute("DELETE FROM chips_in_play")
        await db.execute("DELETE FROM uno_bans")
        await db.execute("DELETE FROM house_revenue")
        await db.execute("DELETE FROM currency_log")
        await db.execute("""
            INSERT INTO audit_log (ts, action, user_id, user_name, detail)
            VALUES (?, 'DATABASE_RESET', ?, ?, 'Full UNO database reset performed')
        """, (ts, admin_id, admin_name))
        await db.commit()

    async with _cache_lock:
        _settings_cache.clear()
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