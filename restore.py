import shutil
import os

# Make sure this matches the DB_PATH you just set in database.py
VOLUME_PATH = "/app/data/poker.db"

print("Starting database restore...")
if os.path.exists("poker.db"):
    # 1. Delete corrupted volume files
    for ext in ["", "-wal", "-shm"]:
        bad_file = f"{VOLUME_PATH}{ext}"
        if os.path.exists(bad_file):
            os.remove(bad_file)
            print(f"Deleted old volume file: {bad_file}")

    # 2. Copy the good database into the volume
    os.makedirs(os.path.dirname(VOLUME_PATH), exist_ok=True)
    shutil.copy2("poker.db", VOLUME_PATH)
    print(f"✅ Successfully copied good poker.db to the persistent volume at {VOLUME_PATH}!")
else:
    print("❌ Could not find the good poker.db in the root folder!")