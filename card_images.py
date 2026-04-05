"""
card_images.py - resizes cards to small thumbnails before sending.
OPTIMIZED: Pre-loads and caches all resized cards at module load time.
Uses Pillow only once at startup, then serves from memory cache.
File naming: 2_of_clubs.png, king_of_hearts.png, ace_of_spades.png, back.png
"""

import os, io
import discord
from PIL import Image
from treys import Card

CARDS_DIR   = os.path.join(os.path.dirname(__file__), "cards")
CARD_HEIGHT = 80   # height in pixels — change this to tune size
CARD_GAP    = 4    # gap between cards in the strip

RANK_NAMES = {
    'A': 'ace',  '2': '2',  '3': '3', '4': '4', '5': '5',
    '6': '6',    '7': '7',  '8': '8', '9': '9', 'T': '10',
    'J': 'jack', 'Q': 'queen', 'K': 'king',
}
SUIT_NAMES = {'s': 'spades', 'h': 'hearts', 'd': 'diamonds', 'c': 'clubs'}

# ── Cache for pre-resized cards (loaded once at startup) ──────────────────────
_ACE_OF_SPADES_INT: int = Card.new('As')
_card_cache: dict[int, Image.Image] = {}
_back_cache: Image.Image | None = None
_egirl_saro_cache: Image.Image | None = None
_cache_loaded = False

def card_filename(card_int: int) -> str:
    s = Card.int_to_str(card_int)
    return f"{RANK_NAMES[s[0]]}_of_{SUIT_NAMES[s[1]]}.png"

def card_path(card_int: int) -> str:
    return os.path.join(CARDS_DIR, card_filename(card_int))

def back_path() -> str:
    return os.path.join(CARDS_DIR, "back.png")

def cards_available() -> bool:
    """Check if card images directory exists and has the required files."""
    if not os.path.isdir(CARDS_DIR):
        return False
    return len([f for f in os.listdir(CARDS_DIR) if f.endswith(".png") and f != "back.png"]) >= 52

def _resize(path: str) -> Image.Image:
    """Resize a single card image to the target height (preserving aspect ratio)."""
    img = Image.open(path).convert("RGBA")
    w = int(img.width * CARD_HEIGHT / img.height)
    return img.resize((w, CARD_HEIGHT), Image.LANCZOS)

def _load_cache():
    """
    Pre-load and cache all 52 cards + back at startup.
    Called automatically on first use or can be called explicitly.
    """
    global _cache_loaded, _back_cache

    if _cache_loaded:
        return  # Already loaded

    if not cards_available():
        print("⚠️  Card images not available — skipping cache load")
        return

    # Cache all 52 cards
    for rank in RANK_NAMES.keys():
        for suit in SUIT_NAMES.keys():
            card_str = f"{rank}{suit}"
            card_int = Card.new(card_str)
            path = card_path(card_int)

            if os.path.exists(path):
                _card_cache[card_int] = _resize(path)

    # Cache the back card
    back = back_path()
    if os.path.exists(back):
        _back_cache = _resize(back)

    global _egirl_saro_cache
    egirl_saro_path = os.path.join(CARDS_DIR, "egirl_ace_of_spades.png")
    if os.path.exists(egirl_saro_path):
        _egirl_saro_cache = _resize(egirl_saro_path)
        print("egirl saro cached")

    _cache_loaded = True
    print(f"✅ Cached {len(_card_cache)} cards + back in memory")


def make_strip(card_ints: list[int], backs: int = 0, is_hole: bool = False, egirl_saro: bool = False) -> discord.File:
    """
    Stitch all cards into one small horizontal strip PNG and return as a File.
    Uses pre-loaded cache — no disk I/O or resampling at call time.
    """
    # Use cached images if available, fall back to live resize if cache missed
    def _get_card(card_int: int) -> Image.Image:
        if egirl_saro and card_int == _ACE_OF_SPADES_INT and _egirl_saro_cache is not None:
            return _egirl_saro_cache.copy()
        if card_int in _card_cache:
            return _card_cache[card_int].copy()
        return _resize(card_path(card_int))

    def _get_back() -> Image.Image:
        if _back_cache is not None:
            return _back_cache.copy()
        return _resize(back_path())

    images = [_get_card(c) for c in card_ints]
    images += [_get_back() for _ in range(backs)]

    if not images:
        buf = io.BytesIO()
        Image.new("RGBA", (1, 1)).save(buf, "PNG")
        buf.seek(0)
        return discord.File(buf, filename="cards.png")

    pad_edge  = 12
    pad_right = 30 if is_hole else 0

    W = sum(img.width for img in images) + CARD_GAP * (len(images) - 1) + pad_right + (pad_edge * 2)
    H = CARD_HEIGHT + (pad_edge * 2)

    strip = Image.new("RGBA", (W, H), (0, 0, 0, 0))

    x = pad_edge
    for img in images:
        strip.paste(img, (x, pad_edge), img)
        x += img.width + CARD_GAP

    buf = io.BytesIO()
    strip.save(buf, "PNG", optimize=True)
    buf.seek(0)
    return discord.File(buf, filename="cards.png")

# ── Auto-load cache on module import ──────────────────────────────────────────
# This runs once when the module is first imported
_load_cache()