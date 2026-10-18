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
class _RollbackLock:
    """asyncio lock that rolls back the shared connection if the guarded block
    exits with ANY exception (incl. cancellation). Without this, an error
    between a write and its commit() leaves the implicit transaction open on
    the global connection and the next caller's commit() persists the
    half-finished work."""

    def __init__(self):
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        await self._lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if exc_type is not None and _db is not None:
                try:
                    await _db.rollback()
                except Exception:
                    pass
        finally:
            self._lock.release()
        return False


_write_lock = _RollbackLock()


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
        await db.execute("""
            CREATE TABLE IF NOT EXISTS table_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ts         TEXT NOT NULL,
                guild_id   INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                table_id   TEXT NOT NULL,
                round_num  INTEGER,
                event      TEXT NOT NULL,
                actor_id   INTEGER,
                detail     TEXT
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_table_log_table ON table_log(table_id, ts)")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS seat_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ts         TEXT NOT NULL,
                table_id   TEXT NOT NULL,
                round_num  INTEGER,
                round_uuid TEXT,
                user_id    INTEGER NOT NULL,
                username   TEXT NOT NULL,
                event      TEXT NOT NULL,
                amount     INTEGER,
                detail     TEXT
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_seat_log_table ON seat_log(table_id, ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_seat_log_user ON seat_log(user_id)")

        # Columns added after a table already existed on disk — CREATE TABLE
        # IF NOT EXISTS never adds them to an existing file, so each needs an
        # explicit ALTER (same pattern/reason as the guild_settings and
        # round_log.round_uuid migrations above).
        for table, col, coltype in [
            ("guild_settings", "min_bet", "INTEGER"),
            ("guild_settings", "max_bet", "INTEGER"),
            ("guild_settings", "turn_timeout", "INTEGER"),
            ("guild_settings", "bet_window", "INTEGER"),
            ("guild_settings", "resend_after_msgs", "INTEGER"),
            ("guild_settings", "max_players", "INTEGER"),
            ("guild_settings", "num_decks", "INTEGER"),
            ("stats", "total_tipped", "INTEGER DEFAULT 0"),
            ("round_log", "table_id", "TEXT"),
            ("round_log", "round_num", "INTEGER"),
            ("round_log", "tax_rate", "REAL"),
            ("round_log", "dealer_id", "INTEGER"),
            ("round_log", "dealer_name", "TEXT"),
            ("round_log_players", "round_uuid", "TEXT"),
            ("round_log_players", "table_id", "TEXT"),
            ("round_log_players", "round_num", "INTEGER"),
            ("turn_log", "table_id", "TEXT"),
            ("turn_log", "round_num", "INTEGER"),
            ("currency_log", "table_id", "TEXT"),
            ("currency_log", "round_num", "INTEGER"),
            ("currency_log", "round_uuid", "TEXT"),
            ("house_revenue", "table_id", "TEXT"),
            ("house_revenue", "round_num", "INTEGER"),
            ("house_revenue", "round_uuid", "TEXT"),
            ("chip_log", "table_id", "TEXT"),
        ]:
            try:
                await db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
            except aiosqlite.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    raise
        # Idempotency records: a staff cashout ticket (keyed by its Discord
        # message id) can be paid at most once, and a donation message can
        # be credited at most once, regardless of what happens to the
        # Discord-side display afterwards.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS paid_cashout_tickets (
                ticket_id INTEGER PRIMARY KEY,
                user_id   INTEGER NOT NULL,
                amount    INTEGER NOT NULL,
                ts        TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS processed_donations (
                message_id INTEGER PRIMARY KEY,
                user_id    INTEGER NOT NULL,
                chips      INTEGER NOT NULL,
                ts         TEXT NOT NULL
            )
        """)
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
                                   card, chosen_color, hand_size_after, direction, extra, table_id, round_num)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (e["round_uuid"], e["turn_num"], e["ts"], e["user_id"], e["username"], e["action"],
             e.get("card"), e.get("chosen_color"), e.get("hand_size_after"), e.get("direction"), e.get("extra"),
             e.get("table_id"), e.get("round_num"))
            for e in events
        ])
        await db.commit()


# ── Wallet / chip movement ──────────────────────────────────────────────────

async def get_balance(user_id: int) -> int:
    db = await _get_db()
    async with db.execute("SELECT balance FROM wallets WHERE user_id=?", (user_id,)) as c:
        row = await c.fetchone()
        return row[0] if row else 0


async def credit_wallet(user_id: int, username: str, amount: int) -> int:
    """Adds chips WITHOUT a chip_log entry — for automatic game payouts
    (pot wins, uncontested-bet refunds), as opposed to add_chips() which
    is for staff-attributed actions (admin_id/admin_name, /unomgr
    addchips, the donation logger) and always writes one."""
    db = await _get_db()
    now = datetime.utcnow().isoformat()
    async with _write_lock:
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance, last_activity, recent_rounds, recent_chips_wagered)
            VALUES (?, ?, MAX(0, ?), ?, 0, 0)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                last_activity = CASE WHEN balance < ? THEN ? ELSE COALESCE(wallets.last_activity, ?) END,
                recent_rounds = CASE WHEN balance < ? THEN 0 ELSE COALESCE(wallets.recent_rounds, 0) END,
                recent_chips_wagered = CASE WHEN balance < ? THEN 0 ELSE COALESCE(wallets.recent_chips_wagered, 0) END,
                balance = MAX(0, balance + ?)
        """, (user_id, username, amount, now,
              config.UNO_MIN_BET, now, now,
              config.UNO_MIN_BET,
              config.UNO_MIN_BET,
              amount))
        await db.commit()
        async with db.execute("SELECT balance FROM wallets WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else 0


async def add_chips(admin_id: int, admin_name: str, user_id: int, user_name: str,
                     amount: int, note: str = "", table_id: str | None = None) -> int:
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
                        INSERT INTO chip_log (ts, admin_id, admin_name, user_id, user_name, amount, note, table_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (now, admin_id, admin_name, user_id, user_name, amount, note, table_id))
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


async def pay_cashout(user_id: int, amount: int, ticket_id: int | None = None) -> bool:
    """Staff confirms a payout: deducts the amount from the pending vault.
    With ticket_id (the ticket's Discord message id) the payment is
    idempotent: the ticket is recorded in the SAME transaction as the
    deduction, so a second attempt for the same ticket returns False and
    changes nothing, whatever happened to the message edit."""
    db = await _get_db()
    async with _write_lock:
        if ticket_id is not None:
            cur = await db.execute(
                "INSERT OR IGNORE INTO paid_cashout_tickets (ticket_id, user_id, amount, ts) VALUES (?,?,?,?)",
                (ticket_id, user_id, amount, datetime.utcnow().isoformat()))
            if cur.rowcount == 0:
                return False
        async with db.execute("SELECT pending_cashout FROM wallets WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            pending = row[0] if row else 0
        if pending < amount:
            await db.rollback()  # also discards the ticket record
            return False
        await db.execute(
            "UPDATE wallets SET pending_cashout = pending_cashout - ? WHERE user_id = ?",
            (amount, user_id)
        )
        await db.commit()
        return True


async def is_ticket_paid(ticket_id: int) -> bool:
    db = await _get_db()
    async with db.execute("SELECT 1 FROM paid_cashout_tickets WHERE ticket_id=?", (ticket_id,)) as c:
        return bool(await c.fetchone())


async def credit_donation(message_id: int, admin_id: int, admin_name: str, user_id: int, user_name: str,
                          chips: int, note: str = "") -> int | None:
    """Credits a donation exactly once per Discord message id: the dedupe
    record, wallet credit, chip_log and currency_log all commit together.
    Returns the new balance, or None if this message was already credited."""
    db = await _get_db()
    now = datetime.utcnow().isoformat()
    async with _write_lock:
        cur = await db.execute(
            "INSERT OR IGNORE INTO processed_donations (message_id, user_id, chips, ts) VALUES (?,?,?,?)",
            (message_id, user_id, chips, now))
        if cur.rowcount == 0:
            return None
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance, last_activity, recent_rounds, recent_chips_wagered)
            VALUES (?, ?, MAX(0, ?), ?, 0, 0)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                last_activity = CASE WHEN balance < ? THEN ? ELSE COALESCE(wallets.last_activity, ?) END,
                recent_rounds = CASE WHEN balance < ? THEN 0 ELSE COALESCE(wallets.recent_rounds, 0) END,
                recent_chips_wagered = CASE WHEN balance < ? THEN 0 ELSE COALESCE(wallets.recent_chips_wagered, 0) END,
                balance = MAX(0, balance + ?)
        """, (user_id, user_name, chips, now,
              config.UNO_MIN_BET, now, now, config.UNO_MIN_BET, config.UNO_MIN_BET, chips))
        await db.execute("""
            INSERT INTO chip_log (ts, admin_id, admin_name, user_id, user_name, amount, note, table_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
        """, (now, admin_id, admin_name, user_id, user_name, chips, note))
        await db.execute("""
            INSERT INTO currency_log (user_id, event_type, amount, description, ts)
            VALUES (?, 'Cash In', ?, 'Dank Memer Donation', ?)
        """, (user_id, chips, now))
        async with db.execute("SELECT balance FROM wallets WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
        await db.commit()
        return row[0] if row else 0


# ── Currency log (per-transaction audit trail for /uno currencylog) ────────

async def log_currency_event(user_id: int, event_type: str, amount: int, description: str,
                              table_id: str | None = None, round_num: int | None = None,
                              round_uuid: str | None = None):
    """event_type: 'Round', 'Cash In', 'Cash Out', 'Wipe', or similar — mirrors poker's convention."""
    if amount == 0:
        return  # don't clutter the log with 0-chip events
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO currency_log (user_id, event_type, amount, description, ts, table_id, round_num, round_uuid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, event_type, amount, description, datetime.utcnow().isoformat(), table_id, round_num, round_uuid))
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


SETTING_KEYS = ("manager_role_id", "log_channel_id", "min_bet", "max_bet", "turn_timeout",
                "bet_window", "resend_after_msgs", "max_players", "num_decks")


def _setting_defaults() -> dict:
    """Fallbacks for any guild_settings column that's still NULL — kept in
    config.py so a deployment's defaults change in one place without
    having to touch every guild's stored row."""
    return {
        "manager_role_id": None,
        "log_channel_id": None,
        "min_bet": config.UNO_MIN_BET,
        "max_bet": config.UNO_MAX_BET,                      # 0 = no limit
        "turn_timeout": config.UNO_TURN_TIMEOUT,
        "bet_window": config.UNO_BET_WINDOW,
        "resend_after_msgs": config.UNO_RESEND_AFTER_MSGS,
        "max_players": config.UNO_MAX_PLAYERS,
        "num_decks": config.UNO_NUM_DECKS,
    }


async def get_settings(guild_id: int) -> dict:
    async with _cache_lock:
        if guild_id in _settings_cache:
            return _settings_cache[guild_id].copy()

    db = await _get_db()
    async with db.execute("SELECT * FROM guild_settings WHERE guild_id=?", (guild_id,)) as c:
        row = await c.fetchone()
    stored = dict(row) if row else {}

    settings = {"guild_id": guild_id}
    for key, default in _setting_defaults().items():
        value = stored.get(key)
        settings[key] = default if value is None else value

    async with _cache_lock:
        _settings_cache[guild_id] = settings.copy()
    return settings


async def set_settings(guild_id: int, **kwargs):
    """Writes ONLY the keys passed (NULL elsewhere stays NULL, so unset
    settings keep following config.py defaults instead of being frozen
    at whatever the default was the first time anyone touched settings)."""
    changes = {k: v for k, v in kwargs.items() if k in SETTING_KEYS and v is not None}
    if not changes:
        return
    db = await _get_db()
    async with _write_lock:
        await db.execute("INSERT OR IGNORE INTO guild_settings (guild_id) VALUES (?)", (guild_id,))
        cols = ", ".join(f"{k} = ?" for k in changes)
        await db.execute(f"UPDATE guild_settings SET {cols} WHERE guild_id = ?", (*changes.values(), guild_id))
        await db.commit()
    async with _cache_lock:
        _settings_cache.pop(guild_id, None)


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

async def log_house_revenue(amount: int, source: str = 'game', table_id: str | None = None,
                             round_num: int | None = None, round_uuid: str | None = None):
    """source='wipe_tax' (inactivity wipes) or 'adjustment' (manual /unoadmin adjustrevenue)."""
    db = await _get_db()
    async with _write_lock:
        await db.execute("INSERT INTO house_revenue (ts, amount, source, table_id, round_num, round_uuid) "
                         "VALUES (?, ?, ?, ?, ?, ?)",
                         (datetime.utcnow().isoformat(), amount, source, table_id, round_num, round_uuid))
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


async def get_chips_in_play_detail() -> list[dict]:
    """Read-only preview of what recover_chips_in_play() would refund and
    clear — used by /unoadmin recoverchips to show staff exactly who/what
    before they confirm the (otherwise irreversible) action."""
    db = await _get_db()
    async with db.execute("SELECT * FROM chips_in_play") as c:
        return [dict(r) for r in await c.fetchall()]


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

async def log_table_event(guild_id: int, channel_id: int, table_id: str, event: str,
                           round_num: int | None = None, actor_id: int | None = None, detail: str | None = None):
    """event: 'open', 'close', 'dealer', 'settings'."""
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO table_log (ts, guild_id, channel_id, table_id, round_num, event, actor_id, detail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.utcnow().isoformat(), guild_id, channel_id, table_id, round_num, event, actor_id, detail))
        await db.commit()


async def log_seat_event(table_id: str, round_num: int | None, round_uuid: str | None, user_id: int,
                          username: str, event: str, amount: int | None = None, detail: str | None = None):
    """event: join, change_bet, leave_queued, leave_cancelled, leave, quit, kick, left_guild,
    cashed_out (settlement-time cashout; detail = quit/kicked/left_guild/afk_kick/leave), below_min, closed.
    amount = chips on the table after the event, or chips returned to the wallet for events that remove the seat."""
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO seat_log (ts, table_id, round_num, round_uuid, user_id, username, event, amount, detail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.utcnow().isoformat(), table_id, round_num, round_uuid, user_id, username, event, amount, detail))
        await db.commit()

# ── Table money (a seat's amount lives in chips_in_play for as long as they're seated) ──

async def table_set_amount(user_id: int, username: str, new_amount: int) -> tuple[bool, int]:
    """Atomically sets a player's on-table amount to `new_amount`, moving
    the difference from/to their wallet in the SAME transaction — a join
    (0 -> bet), a raise (needs wallet chips) or a lower bet (refunds the
    difference) can't leave wallet and table out of step if the bot dies
    between two separate statements. Returns (ok, previous_amount); ok is
    False (nothing changed) if the wallet can't cover an increase."""
    db = await _get_db()
    async with _write_lock:
        async with db.execute("SELECT amount FROM chips_in_play WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
        old = row[0] if row else 0
        diff = new_amount - old
        if diff > 0:
            await db.execute("UPDATE wallets SET balance = balance - ? WHERE user_id = ? AND balance >= ?",
                              (diff, user_id, diff))
            async with db.execute("SELECT changes()") as c:
                ch = await c.fetchone()
            if not (ch and ch[0] > 0):
                await db.rollback()
                return False, old
        elif diff < 0:
            await db.execute("""
                INSERT INTO wallets (user_id, username, balance) VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET balance = balance + ?
            """, (user_id, username, -diff, -diff))
        if new_amount > 0:
            await db.execute("""
                INSERT INTO chips_in_play (user_id, username, amount) VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET username = excluded.username, amount = excluded.amount
            """, (user_id, username, new_amount))
        else:
            await db.execute("DELETE FROM chips_in_play WHERE user_id=?", (user_id,))
        await db.commit()
        return True, old


async def table_cash_out(user_id: int, username: str = "Unknown Player") -> int:
    """Everything a player has on the table goes back to their wallet and
    their chips_in_play row is removed, in one transaction. Returns the
    amount moved (0 if they had nothing on the table)."""
    db = await _get_db()
    async with _write_lock:
        async with db.execute("SELECT amount FROM chips_in_play WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
        amount = row[0] if row else 0
        if amount > 0:
            await db.execute("""
                INSERT INTO wallets (user_id, username, balance) VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET balance = balance + ?
            """, (user_id, username, amount, amount))
        await db.execute("DELETE FROM chips_in_play WHERE user_id=?", (user_id,))
        await db.commit()
        return amount


async def round_exists(round_uuid: str) -> bool:
    """True if this round's settlement has already been applied."""
    db = await _get_db()
    async with db.execute("SELECT 1 FROM round_log WHERE round_uuid=?", (round_uuid,)) as c:
        return bool(await c.fetchone())


async def get_table_amount(user_id: int) -> int:
    db = await _get_db()
    async with db.execute("SELECT amount FROM chips_in_play WHERE user_id=?", (user_id,)) as c:
        row = await c.fetchone()
        return row[0] if row else 0


async def apply_round_settlement(round_uuid: str, guild_id: int, channel_id: int, table_id: str,
                                  round_num: int, num_decks: int, winner_id: int | None,
                                winner_name: str | None, players: list[dict], channel_label: str,
                                 dealer_id: int | None = None, dealer_name: str | None = None,
                                 tax_rate: float | None = None) -> int | None:
    """Applies one finished round's money + bookkeeping as a SINGLE
    transaction: new on-table amounts, house revenue, round_log(+players),
    stats, activity and currency_log all commit together or not at all.
    Idempotent on round_uuid — if that round was already applied (a
    crash/resume replaying settlement for a round that had in fact
    already committed), it returns None and changes nothing, so a
    resumed table can never pay the same round out twice.

    players: [{user_id, username, bet, change, tax, net, new_bet, placement, won}]"""
    db = await _get_db()
    ts = datetime.utcnow().isoformat()
    total_tax = sum(p["tax"] for p in players)
    async with _write_lock:
        try:
            async with db.execute("SELECT id FROM round_log WHERE round_uuid=?", (round_uuid,)) as c:
                if await c.fetchone():
                    return None
            cursor = await db.execute("""
                                INSERT INTO round_log (round_uuid, ts, guild_id, channel_id, num_players, num_decks,
                                        total_pot, total_tax, winner_id, winner_name, table_id, round_num,
                                        tax_rate, dealer_id, dealer_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (round_uuid, ts, guild_id, channel_id, len(players), num_decks,
                  sum(p["bet"] for p in players), total_tax, winner_id, winner_name, table_id, round_num,
                  tax_rate, dealer_id, dealer_name))
            round_id = cursor.lastrowid
            await db.executemany("""
                                INSERT INTO round_log_players (round_id, user_id, username, bet, gross, tax, net, placement, won,
                                               round_uuid, table_id, round_num)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [(round_id, p["user_id"], p["username"], p["bet"], p["bet"] + p["change"], p["tax"],
                   p["net"], p.get("placement"), 1 if p["won"] else 0, round_uuid, table_id, round_num)
                  for p in players])

            for p in players:
                uid, net = p["user_id"], p["net"]
                if p["new_bet"] > 0:
                    await db.execute("""
                        INSERT INTO chips_in_play (user_id, username, amount) VALUES (?, ?, ?)
                        ON CONFLICT(user_id) DO UPDATE SET username = excluded.username, amount = excluded.amount
                    """, (uid, p["username"], p["new_bet"]))
                else:
                    await db.execute("DELETE FROM chips_in_play WHERE user_id=?", (uid,))
                await db.execute("""
                    INSERT INTO stats (user_id, username, rounds_played, rounds_won, chips_won, chips_lost)
                    VALUES (?, ?, 1, ?, MAX(0,?), MAX(0,?))
                    ON CONFLICT(user_id) DO UPDATE SET
                        username      = excluded.username,
                        rounds_played = rounds_played + 1,
                        rounds_won    = rounds_won    + ?,
                        chips_won     = chips_won     + MAX(0, ?),
                        chips_lost    = chips_lost    + MAX(0, ?)
                """, (uid, p["username"], 1 if p["won"] else 0, net, -net,
                      1 if p["won"] else 0, net, -net))
                await _mark_player_active_tx(db, uid, max(abs(net), p["bet"]))
                if net != 0:
                    await db.execute("""
                        INSERT INTO currency_log (user_id, event_type, amount, description, ts,
                                                  table_id, round_num, round_uuid)
                        VALUES (?, 'Round', ?, ?, ?, ?, ?, ?)
                    """, (uid, net, f"UNO round #{round_num} (table {table_id}) in #{channel_label}", ts,
                          table_id, round_num, round_uuid))
            if total_tax > 0:
                await db.execute("INSERT INTO house_revenue (ts, amount, source, table_id, round_num, round_uuid) "
                                 "VALUES (?, ?, 'winners_tax', ?, ?, ?)",
                                 (ts, total_tax, table_id, round_num, round_uuid))
            await db.commit()
            return round_id
        except Exception:
            await db.rollback()
            raise


# ── Tips ─────────────────────────────────────────────────────────────────────

async def tip_transfer(user_id: int, username: str, dealer_id: int, dealer_name: str,
                       tip: int, from_table: int = 0, table_id: str | None = None,
                       round_num: int | None = None) -> tuple[bool, int]:
    """Whole tip in ONE transaction: optionally pull `from_table` chips off the
    tipper's table amount into their wallet, deduct `tip` from the wallet,
    credit the dealer, and write chip_log / tips / currency_log. Either
    everything applies or nothing does. Returns (ok, new_table_amount);
    new_table_amount is the authoritative chips_in_play value after the call."""
    now = datetime.utcnow().isoformat()
    async with _write_lock:
        db = await _get_db()
        async with db.execute("SELECT amount FROM chips_in_play WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
        table_amt = row[0] if row else 0
        if from_table > 0:
            if table_amt < from_table:
                return False, table_amt
            table_amt -= from_table
            await db.execute("""
                INSERT INTO wallets (user_id, username, balance) VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET balance = balance + ?
            """, (user_id, username, from_table, from_table))
            if table_amt > 0:
                await db.execute("UPDATE chips_in_play SET amount=? WHERE user_id=?", (table_amt, user_id))
            else:
                await db.execute("DELETE FROM chips_in_play WHERE user_id=?", (user_id,))
        cur = await db.execute(
            "UPDATE wallets SET balance = balance - ? WHERE user_id = ? AND balance >= ?",
            (tip, user_id, tip))
        if cur.rowcount <= 0:
            await db.rollback()
            return False, (table_amt + from_table)
        await db.execute("""
            INSERT INTO wallets (user_id, username, balance, last_activity, recent_rounds, recent_chips_wagered)
            VALUES (?, ?, MAX(0, ?), ?, 0, 0)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                last_activity = CASE WHEN balance < ? THEN ? ELSE COALESCE(wallets.last_activity, ?) END,
                recent_rounds = CASE WHEN balance < ? THEN 0 ELSE COALESCE(wallets.recent_rounds, 0) END,
                recent_chips_wagered = CASE WHEN balance < ? THEN 0 ELSE COALESCE(wallets.recent_chips_wagered, 0) END,
                balance = MAX(0, balance + ?)
        """, (dealer_id, dealer_name, tip, now,
              config.UNO_MIN_BET, now, now, config.UNO_MIN_BET, config.UNO_MIN_BET, tip))
        await db.execute("""
            INSERT INTO chip_log (ts, admin_id, admin_name, user_id, user_name, amount, note, table_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (now, user_id, username, dealer_id, dealer_name, tip, f"Tip from {username}", table_id))
        await db.execute("""
            INSERT INTO stats (user_id, username, total_tipped) VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                total_tipped = COALESCE(total_tipped, 0) + ?
        """, (user_id, username, tip, tip))
        for uid, amt, desc in ((user_id, -tip, f"Tipped {dealer_name}"),
                               (dealer_id, tip, f"Tip from {username}")):
            await db.execute("""
                INSERT INTO currency_log (user_id, event_type, amount, description, ts, table_id, round_num)
                VALUES (?, 'Tip', ?, ?, ?, ?, ?)
            """, (uid, amt, desc, now, table_id, round_num))
        await db.commit()
        return True, table_amt


async def record_tip(user_id: int, username: str, amount: int):
    db = await _get_db()
    async with _write_lock:
        await db.execute("""
            INSERT INTO stats (user_id, username, total_tipped) VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                total_tipped = COALESCE(total_tipped, 0) + ?
        """, (user_id, username, amount, amount))
        await db.commit()


async def get_tip_leaderboard(limit: int = 10) -> list[dict]:
    db = await _get_db()
    async with db.execute("""
        SELECT user_id, username, total_tipped FROM stats
        WHERE COALESCE(total_tipped, 0) > 0 ORDER BY total_tipped DESC LIMIT ?
    """, (limit,)) as c:
        return [dict(r) for r in await c.fetchall()]


# ── Inactivity wipe ──────────────────────────────────────────────────────────
# Same shape as poker's: a rolling "did they play enough recently" window,
# and a same-rate (20%) tax + auto-cashout of the rest for anyone who didn't.

async def _mark_player_active_tx(db, user_id: int, chips_wagered: int = 0):
    """The UPDATE itself, without taking the write lock or committing —
    so apply_round_settlement can fold it into its one big transaction."""
    now = datetime.utcnow().isoformat()
    wager_bar = MIN_CHIPS_WAGERED if MIN_CHIPS_WAGERED > 0 else 0
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
        MIN_ROUNDS_PER_PERIOD, chips_wagered, wager_bar,
        MIN_ROUNDS_PER_PERIOD, chips_wagered, wager_bar, chips_wagered,
        MIN_ROUNDS_PER_PERIOD, chips_wagered, wager_bar, now, now, now,
        user_id,
    ))


async def mark_player_active(user_id: int, chips_wagered: int = 0):
    """Call once per player when a round they bet in ends. Bumps the rolling
    window, resetting it (and the inactivity clock) once they've cleared
    both the round-count and chips-wagered bar for the period."""
    db = await _get_db()
    async with _write_lock:
        await _mark_player_active_tx(db, user_id, chips_wagered)
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
        await db.execute("DELETE FROM table_log")
        await db.execute("DELETE FROM seat_log")
        await db.execute("""
            INSERT INTO audit_log (ts, action, user_id, user_name, detail)
            VALUES (?, 'DATABASE_RESET', ?, ?, 'Full UNO database reset performed')
        """, (ts, admin_id, admin_name))
        await db.commit()

    async with _cache_lock:
        _settings_cache.clear()


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
    db = await _get_db()
    wiped = []
    async with _write_lock:
        # Eligibility and balances are read INSIDE the write lock so nothing
        # can credit a wallet or mark a player active between the read and
        # the wipe; the UPDATE below additionally requires the balance to be
        # unchanged.
        inactive = await get_inactive_players()
        for player in inactive:
            user_id, username, balance = player["user_id"], player["username"], player["balance"]
            if balance <= 0:
                continue

            tax_amount = math.ceil(balance * WIPE_TAX_RATE)
            cashout_amount = balance - tax_amount

            cur = await db.execute("""
                UPDATE wallets
                SET balance = 0,
                    pending_cashout = pending_cashout + ?,
                    recent_rounds = 0,
                    recent_chips_wagered = 0,
                    last_activity = ?
                WHERE user_id = ? AND balance = ?
            """, (cashout_amount, now, user_id, balance))
            if cur.rowcount != 1:
                continue
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
                    VALUES (?, 'Wipe', ?, ?, ?)
                """, (user_id, -tax_amount, f"Inactivity Tax ({round(WIPE_TAX_RATE * 100)}%)", now))
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