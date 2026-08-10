"""
Bulk-uploads every image in CARDS_DIR as a Discord *application* emoji
(global, usable in any server the bot is in — not tied to one guild's
50/250 emoji slot limit).

Usage:
    BOT_TOKEN=xxx APPLICATION_ID=xxx python upload_emojis.py

Requires: pip install requests
"""

import os
import sys
import time
import base64
import mimetypes
import requests
from pathlib import Path

CARDS_DIR = Path(__file__).parent / "uno_cards"
BOT_TOKEN = ''
APPLICATION_ID = 1477451825106915399

API_BASE = "https://discord.com/api/v10"


def sanitize_emoji_name(name: str) -> str:
    """Discord emoji names: 2-32 chars, alphanumeric + underscore only."""
    cleaned = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    cleaned = cleaned.strip("_")
    if len(cleaned) < 2:
        cleaned = cleaned + "_c"
    return cleaned[:32]


def image_to_data_uri(path: str) -> str:
    mime, _ = mimetypes.guess_type(path)
    mime = mime or "image/png"
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"


def list_existing_emoji_names(headers) -> set:
    resp = requests.get(f"{API_BASE}/applications/{APPLICATION_ID}/emojis", headers=headers)
    resp.raise_for_status()
    return {e["name"] for e in resp.json().get("items", [])}


def upload_emoji(name: str, path: str, headers) -> dict | None:
    try:
        payload = {"name": name, "image": image_to_data_uri(path)}
        resp = requests.post(
            f"{API_BASE}/applications/{APPLICATION_ID}/emojis",
            headers=headers,
            json=payload,
        )
        if resp.status_code == 429:
            retry_after = resp.json().get("retry_after", 1.5)
            print(f"  rate limited, waiting {retry_after}s...")
            time.sleep(retry_after)
            return upload_emoji(name, path, headers)

        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        print(f"  ❌ failed: {name} -> {e} | {resp.text}")
        return None


def main():
    if not BOT_TOKEN or not APPLICATION_ID:
        print("Set BOT_TOKEN and APPLICATION_ID env vars first.")
        sys.exit(1)

    if not os.path.isdir(CARDS_DIR):
        print(f"Folder not found: {CARDS_DIR}")
        sys.exit(1)

    headers = {"Authorization": f"Bot {BOT_TOKEN}"}

    print("Fetching existing application emojis...")
    existing = list_existing_emoji_names(headers)
    print(f"  {len(existing)} already uploaded, will skip those.\n")

    files = sorted(
        f for f in os.listdir(CARDS_DIR)
        if f.lower().endswith((".png", ".jpg", ".jpeg", ".webp"))
    )

    name_map = {}  # original filename -> emoji name used
    uploaded, skipped, failed = 0, 0, 0

    for fname in files:
        base_name = os.path.splitext(fname)[0]
        emoji_name = sanitize_emoji_name(base_name)
        path = os.path.join(CARDS_DIR, fname)

        if emoji_name in existing:
            print(f"skip (exists): {emoji_name}")
            name_map[base_name] = emoji_name
            skipped += 1
            continue

        result = upload_emoji(emoji_name, path, headers)
        if result:
            print(f"✅ uploaded: {emoji_name}  (id: {result['id']})")
            name_map[base_name] = emoji_name
            uploaded += 1
            existing.add(emoji_name)
        else:
            failed += 1

        # Application emoji upload rate limit is generous but be polite
        time.sleep(0.4)

    print(f"\nDone. uploaded={uploaded} skipped={skipped} failed={failed}")

    # Write out an EMOJI_MAP snippet you can paste straight into the cog
    print("\n--- paste into EMOJI_MAP (fetch real IDs via GET .../emojis) ---")
    for base_name in sorted(name_map):
        print(f'    "{base_name}": "<:{name_map[base_name]}:REPLACE_WITH_ID>",')


if __name__ == "__main__":
    main()