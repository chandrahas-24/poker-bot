import sqlite3
import math

DB_PATH = "poker.db"


def migrate_db():
    print("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. EXTRACT REVENUE STATS
    print("\n--- 1. REVENUE STATS ---")
    try:
        cursor.execute("SELECT SUM(amount) FROM house_revenue")
        total_rev = cursor.fetchone()[0] or 0
        print(f"Total House Revenue (All-Time): {total_rev:,} chips")
    except sqlite3.OperationalError:
        print("No house_revenue table found.")

    # 2. APPLY 5% TAX TO WALLETS
    print("\n--- 2. APPLYING 5% TAX TO WALLETS ---")
    cursor.execute("SELECT user_id, username, balance FROM wallets WHERE balance > 0")
    wallets = cursor.fetchall()

    total_taxed = 0
    for uid, uname, bal in wallets:
        new_bal = math.floor(bal * 0.95)
        tax_taken = bal - new_bal
        total_taxed += tax_taken

        cursor.execute("UPDATE wallets SET balance = ? WHERE user_id = ?", (new_bal, uid))

    print(f"Successfully processed {len(wallets)} wallets.")
    print(f"Total chips removed from the economy: {total_taxed:,}")

    # 3. INITIALIZE ACTIVITY TIMERS TO APRIL 2ND
    print("\n--- 3. INITIALIZING ACTIVITY TIMERS ---")

    # 🚨 Future-date for the timer start!
    start_date = "2026-04-02T00:00:00"

    for col, default in [("last_activity", "TEXT"), ("recent_hands", "INTEGER DEFAULT 0"),
                         ("recent_chips_wagered", "INTEGER DEFAULT 0")]:
        try:
            cursor.execute(f"ALTER TABLE wallets ADD COLUMN {col} {default}")
        except sqlite3.OperationalError:
            pass

    cursor.execute("UPDATE wallets SET last_activity = ?, recent_hands = 0, recent_chips_wagered = 0", (start_date,))
    print(f"Set last_activity to {start_date} for all users.")
    print("The activity wipe timer will officially begin ticking down on April 2nd.")

    # Commit and close
    conn.commit()
    conn.close()
    print("\n✅ Migration complete! You can now boot up the new bot.")


if __name__ == "__main__":
    migrate_db()