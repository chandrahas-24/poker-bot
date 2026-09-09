"""
card_images.py - resizes cards to small thumbnails before sending.
OPTIMIZED: Pre-loads and caches all resized cards at module load time.
Uses Pillow only once at startup, then serves from memory cache.
File naming: 2_of_clubs.png, king_of_hearts.png, ace_of_spades.png, back.png
"""

import os, io
import random as _random
import discord
from PIL import Image, ImageFilter, ImageEnhance, ImageDraw
from treys import Card
from . import shiny_cards
from . import database as db

UNO_COLORS = ["red", "blue", "green", "yellow"]

CARDS_DIR    = os.path.join(os.path.dirname(__file__), "cards")
BORDERS_DIR  = os.path.join(CARDS_DIR, "borders")

# Card sizing
# "Normal" is default, "Compact" toggleable for better mobile viewing
# scales border and overlays
CARD_HEIGHT         = 112  # normal height in pixels
CARD_HEIGHT_COMPACT = 80   # compact/"mobile viewing" height in pixels
CARD_GAP            = 5    # gap between cards in the strip (normal size)
CARD_GAP_COMPACT    = 4    # gap between cards in the strip (compact size)

BORDER_MARGIN_RATIO = 0.125

def _border_margin(card_height: int) -> int:
    return max(4, round(card_height * BORDER_MARGIN_RATIO))

RANK_NAMES = {
    'A': 'ace',  '2': '2',  '3': '3', '4': '4', '5': '5',
    '6': '6',    '7': '7',  '8': '8', '9': '9', 'T': '10',
    'J': 'jack', 'Q': 'queen', 'K': 'king',
}
SUIT_NAMES = {'s': 'spades', 'h': 'hearts', 'd': 'diamonds', 'c': 'clubs'}

# ── Cache for pre-resized cards (loaded once at startup) ──────────────────────
# "_compact" of each below, holding the same art resized to CARD_HEIGHT_COMPACT instead of CARD_HEIGHT
# pre-renders all
_card_cache: dict[int, Image.Image] = {}
_back_cache: Image.Image | None = None
_shiny_cache: dict[str, Image.Image] = {}  # shiny_id -> resized art
_uno_cache: dict[str, Image.Image] = {}    # Chaos: Uno Reverse — color -> resized art
_cute_overlay_cache: Image.Image | None = None  # Chaos: Cute Mode — one overlay for all cards
_border_underlay_cache: dict[str, Image.Image] = {}  # Card Borders cosmetic — border_id -> resized underlay art
_border_overlay_cache: dict[str, Image.Image] = {}   # Card Borders cosmetic — border_id -> resized overlay art

_card_cache_compact: dict[int, Image.Image] = {}
_back_cache_compact: Image.Image | None = None
_shiny_cache_compact: dict[str, Image.Image] = {}
_uno_cache_compact: dict[str, Image.Image] = {}
_border_underlay_cache_compact: dict[str, Image.Image] = {}
_border_overlay_cache_compact: dict[str, Image.Image] = {}

_cache_loaded = False

# for reverse boards
BACK_SENTINEL = -1

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

def _resize(path: str, height: int = CARD_HEIGHT) -> Image.Image:
    """Resize a single card image to the target height (preserving aspect ratio)."""
    img = Image.open(path).convert("RGBA")
    w = int(img.width * height / img.height)
    return img.resize((w, height), Image.LANCZOS)

def _resize_border(path: str, card_height: int = CARD_HEIGHT) -> Image.Image:
    """
    Resize a Card Border frame to card_height + 2*border_margin (preserving
    aspect ratio) — always slightly larger than a resized card, since the
    border renders UNDER the card art with its own edges peeking out on
    every side once the card is centered on top of it.
    """
    img = Image.open(path).convert("RGBA")
    target_h = card_height + _border_margin(card_height) * 2
    w = int(img.width * target_h / img.height)
    return img.resize((w, target_h), Image.LANCZOS)

def _load_cache():
    """
    Pre-load and cache all 52 cards + back at startup, at both the normal
    and compact size tiers.
    Called automatically on first use or can be called explicitly.
    """
    global _cache_loaded, _back_cache, _back_cache_compact, _cute_overlay_cache

    if _cache_loaded:
        return  # Already loaded

    if not cards_available():
        print("⚠️  Card images not available — skipping cache load")
        return

    # Cache all 52 cards, at both sizes
    for rank in RANK_NAMES.keys():
        for suit in SUIT_NAMES.keys():
            card_str = f"{rank}{suit}"
            card_int = Card.new(card_str)
            path = card_path(card_int)

            if os.path.exists(path):
                _card_cache[card_int] = _resize(path, CARD_HEIGHT)
                _card_cache_compact[card_int] = _resize(path, CARD_HEIGHT_COMPACT)

    # Cache the back card
    back = back_path()
    if os.path.exists(back):
        _back_cache = _resize(back, CARD_HEIGHT)
        _back_cache_compact = _resize(back, CARD_HEIGHT_COMPACT)

    # Cache every shiny card's art, if its file has been dropped in yet.
    for shiny in shiny_cards.SHINY_CARDS:
        shiny_path = os.path.join(CARDS_DIR, shiny.image)
        if os.path.exists(shiny_path):
            _shiny_cache[shiny.id] = _resize(shiny_path, CARD_HEIGHT)
            _shiny_cache_compact[shiny.id] = _resize(shiny_path, CARD_HEIGHT_COMPACT)
            print(f"✨ {shiny.display_name} ({shiny.id}) shiny art cached")
        else:
            print(f"⚠️  Shiny art missing for {shiny.id}: expected {shiny_path}")

    # Uno Reverse cards
    for color in UNO_COLORS:
        uno_path = os.path.join(CARDS_DIR, f"reverse_{color}.png")
        if os.path.exists(uno_path):
            _uno_cache[color] = _resize(uno_path, CARD_HEIGHT)
            _uno_cache_compact[color] = _resize(uno_path, CARD_HEIGHT_COMPACT)
            # print(f"🔄 Uno Reverse ({color}) art cached")
        else:
            print(f"⚠️  Uno Reverse art missing for {color}: expected {uno_path}")

    # Cute overlay
    cute_path = os.path.join(CARDS_DIR, "cute.png")
    if os.path.exists(cute_path):
        _cute_overlay_cache = Image.open(cute_path).convert("RGBA")
        # print("💕 Cute Mode overlay cached")
        pass
        
    else:
        print(f"⚠️  Cute Mode overlay missing: expected {cute_path}")

    # card borders
    for border_id, info in db.BORDERS.items():
        cached_any = False
        for field, cache, cache_compact in (
            ("underlay", _border_underlay_cache, _border_underlay_cache_compact),
            ("overlay", _border_overlay_cache, _border_overlay_cache_compact),
        ):
            image_name = info.get(field)
            if not image_name:
                continue
            border_path = os.path.join(BORDERS_DIR, image_name)
            if os.path.exists(border_path):
                cache[border_id] = _resize_border(border_path, CARD_HEIGHT)
                cache_compact[border_id] = _resize_border(border_path, CARD_HEIGHT_COMPACT)
                cached_any = True
            else:
                print(f"⚠️  Border {field} art missing for {border_id}: expected {border_path}")
        if cached_any:
            # print(f"🖼️  {info['display']} ({border_id}) border art cached")
            pass

    _cache_loaded = True
    print(f"✅ Cached {len(_card_cache)} cards + back in memory (normal + compact)")


def _apply_cute_overlay(img: Image.Image) -> Image.Image:
    if _cute_overlay_cache is None:
        return img
    base = img.convert("RGBA")
    overlay = _cute_overlay_cache.resize(base.size, Image.LANCZOS)
    return Image.alpha_composite(base, overlay)


def _apply_blindness(img: Image.Image) -> Image.Image:
    # blindness mod; apply fx to blur cards
    messy = img.convert("RGBA")
    w, h = messy.size

    # pixelate (downscale then upscale w/ nearest neighbor)
    small = messy.resize((max(1, w // 10), max(1, h // 10)), Image.BILINEAR)
    messy = small.resize((w, h), Image.NEAREST)

    # heavy blur
    messy = messy.filter(ImageFilter.GaussianBlur(radius=9))

    # colour corrupt w/ hue change
    rng = _random.Random()
    r, g, b, a = messy.split()
    channels = [r, g, b]
    rng.shuffle(channels)          # swaps color channels around
    shifted = Image.merge("RGB", channels)
    shifted = ImageEnhance.Color(shifted).enhance(1.6)  # push the wrong colors harder
    messy = Image.merge("RGBA", (*shifted.split(), a))

    # smudge lines
    draw = ImageDraw.Draw(messy)
    for _ in range(9):
        x1, y1 = rng.randint(0, w), rng.randint(0, h)
        x2, y2 = rng.randint(0, w), rng.randint(0, h)
        color = (rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255), 160)
        draw.line((x1, y1, x2, y2), fill=color, width=rng.randint(5, 10))

    return messy


def _apply_border(card_img: Image.Image, underlay: Image.Image | None, overlay: Image.Image | None) -> Image.Image:
    # underlay + overlay
    # wraps over blind/cute mods
    frame = underlay if underlay is not None else overlay
    fw, fh = frame.size if frame is not None else card_img.size
    cw, ch = card_img.size

    combo = Image.new("RGBA", (fw, fh), (0, 0, 0, 0))
    if underlay is not None:
        combo.alpha_composite(underlay if underlay.size == (fw, fh) else underlay.resize((fw, fh), Image.LANCZOS))
    x = (fw - cw) // 2
    y = (fh - ch) // 2
    combo.paste(card_img, (x, y), card_img)
    if overlay is not None:
        combo.alpha_composite(overlay if overlay.size == (fw, fh) else overlay.resize((fw, fh), Image.LANCZOS))
    return combo


def make_strip(card_ints: list[int], backs: int = 0, is_hole: bool = False, shiny_ids: list[str] | None = None,
                blind_positions: set[int] | None = None, extra_uno_color: str | None = None,
                cute_mode: bool = False, border_id: str | None = None, compact: bool = False) -> discord.File:
    """
    Stitch all cards into one small horizontal strip PNG and return as a File.
    Uses pre-loaded cache — no disk I/O or resampling at call time.

    `shiny_ids` should be the list of shiny card ids (see shiny_cards.py) the
    player actually rolled this hand — e.g. `player.shiny_ids`. Any card in
    `card_ints` whose treys value matches one of those shiny ids' card is
    drawn using that shiny's art instead of the normal card art.
    """
    shiny_ids = shiny_ids or []
    blind_positions = blind_positions or set()

    card_cache = _card_cache_compact if compact else _card_cache
    back_cache = _back_cache_compact if compact else _back_cache
    shiny_cache = _shiny_cache_compact if compact else _shiny_cache
    uno_cache = _uno_cache_compact if compact else _uno_cache
    underlay_cache = _border_underlay_cache_compact if compact else _border_underlay_cache
    overlay_cache = _border_overlay_cache_compact if compact else _border_overlay_cache
    target_height = CARD_HEIGHT_COMPACT if compact else CARD_HEIGHT
    card_gap = CARD_GAP_COMPACT if compact else CARD_GAP

    underlay_img = underlay_cache.get(border_id) if border_id else None
    overlay_img = overlay_cache.get(border_id) if border_id else None
    has_border = underlay_img is not None or overlay_img is not None

    # Use cached images if available, fall back to live resize if cache missed
    def _get_card(card_int: int) -> Image.Image:
        for sid in shiny_ids:
            shiny = shiny_cards.get_by_id(sid)
            if shiny and shiny.card_int == card_int and sid in shiny_cache:
                return shiny_cache[sid].copy()
        if card_int in card_cache:
            return card_cache[card_int].copy()
        return _resize(card_path(card_int), target_height)

    def _get_back() -> Image.Image:
        if back_cache is not None:
            return back_cache.copy()
        return _resize(back_path(), target_height)

    images = []
    for idx, c in enumerate(card_ints):
        if c == BACK_SENTINEL:
            images.append(_get_back())
            continue
        img = _get_card(c)
        if idx in blind_positions:
            img = _apply_blindness(img)
        if cute_mode:
            img = _apply_cute_overlay(img)
        if has_border:
            img = _apply_border(img, underlay_img, overlay_img)
        images.append(img)
    if extra_uno_color and extra_uno_color in uno_cache:
        images.append(uno_cache[extra_uno_color].copy())
    images += [_get_back() for _ in range(backs)]

    if not images:
        buf = io.BytesIO()
        Image.new("RGBA", (1, 1)).save(buf, "PNG")
        buf.seek(0)
        return discord.File(buf, filename="cards.png")

    pad_edge  = 12
    pad_right = 30 if is_hole else 0

    # CARD_HEIGHT normally
    # resizes with borders
    
    max_h = max(img.height for img in images)

    W = sum(img.width for img in images) + card_gap * (len(images) - 1) + pad_right + (pad_edge * 2)
    H = max_h + (pad_edge * 2)

    strip = Image.new("RGBA", (W, H), (0, 0, 0, 0))

    x = pad_edge
    for img in images:
        y = pad_edge + (max_h - img.height) // 2
        strip.paste(img, (x, y), img)
        x += img.width + card_gap

    buf = io.BytesIO()
    strip.save(buf, "PNG", optimize=True)
    buf.seek(0)
    return discord.File(buf, filename="cards.png")


def make_double_board_strip(board1: list[int], board2: list[int],
                             blind1: set[int] | None = None, blind2: set[int] | None = None,
                             cute_mode: bool = False, compact: bool = False) -> discord.File:
    """
    Double Board: stack boards
    
    no effect from compact
    """
    def _strip_only(cards, blind, backs):
        # Re-uses make_strip's card lookup/blur logic by calling it directly
        # and re-opening the PNG bytes as an Image so we can compose it.
        f = make_strip(cards, backs, False, None, blind, cute_mode=cute_mode, compact=compact)
        f.fp.seek(0)
        return Image.open(f.fp).convert("RGBA")

    img1 = _strip_only(board1, blind1 or set(), max(0, 5 - len(board1)))
    img2 = _strip_only(board2, blind2 or set(), max(0, 5 - len(board2)))

    gap = 6
    W = max(img1.width, img2.width)
    H = img1.height + gap + img2.height
    combined = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    combined.paste(img1, (0, 0), img1)
    combined.paste(img2, (0, img1.height + gap), img2)

    buf = io.BytesIO()
    combined.save(buf, "PNG", optimize=True)
    buf.seek(0)
    return discord.File(buf, filename="cards.png")  # matches build_embed's hardcoded attachment:// reference

# ── Auto-load cache on module import ──────────────────────────────────────────
# This runs once when the module is first imported
_load_cache()