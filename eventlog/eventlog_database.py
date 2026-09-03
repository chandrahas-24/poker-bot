import aiosqlite
import config

DB_PATH = config.EVENTLOG_DB_PATH
async def init_log_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS event_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                staff_id INTEGER,
                staff_username TEXT,
                message_id INTEGER UNIQUE,
                donator_username TEXT,
                event_type TEXT,
                event_prize_msg TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                mistake INTEGER NOT NULL DEFAULT 0
            )
        ''')

        try:
            await db.execute('ALTER TABLE event_logs ADD COLUMN staff_username TEXT')
        except aiosqlite.OperationalError:
            pass

        try:
            await db.execute(
                'ALTER TABLE event_logs ADD COLUMN mistake INTEGER NOT NULL DEFAULT 0'
            )
        except aiosqlite.OperationalError:
            pass

        await db.commit()


async def save_embed_log(staff_id: int, staff_username: str, message_id: int, donator_username: str, event_type: str,
                         event_prize_msg: str, timestamp: str) -> bool:
    """Inserts the parsed embed data into the database."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:

            await db.execute(
                """
                INSERT INTO event_logs
                (staff_id, staff_username, message_id, donator_username, event_type, event_prize_msg, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    staff_id,
                    staff_username,
                    message_id,
                    donator_username,
                    event_type,
                    event_prize_msg,
                    timestamp
                )
            )
            await db.commit()
            return True
    except aiosqlite.IntegrityError:
        return False


async def fetch_logs(limit: int = 10, donator: str = None):
    """Fetches recent logs, optionally filtering by donator username."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        query = "SELECT * FROM event_logs"
        params = []

        if donator:
            query += " WHERE donator_username LIKE ?"
            params.append(f"%{donator}%")

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        async with db.execute(query, params) as cursor:
            return await cursor.fetchall()


async def execute_ro_query(query: str):
    """Executes a raw SQL query in strict read-only mode."""
    # reject non-SELECT queries immediately
    if not query.strip().upper().startswith("SELECT"):
        raise ValueError("Only SELECT queries are permitted.")

    # Open connection using URI to enforce read-only mode
    db_uri = "file:eventlog_database.db?mode=ro"
    async with aiosqlite.connect(db_uri, uri=True) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query) as cursor:
            # Fetch all rows (we'll limit the display on Discord's side)
            return await cursor.fetchall()


async def fetch_staff_stats(start_date: str, end_date: str):
    """Fetches total events hosted per staff member between start_date and end_date."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        query = """
            SELECT staff_id, COUNT(*) as total_events 
            FROM event_logs 
            WHERE timestamp >= ?
              AND timestamp <= ?
              AND mistake = 0
            GROUP BY staff_id 
            ORDER BY total_events DESC
        """
        async with db.execute(query, (f"{start_date} 00:00:00", f"{end_date} 23:59:59")) as cursor:
            return await cursor.fetchall()