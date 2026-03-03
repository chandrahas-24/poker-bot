"""
card_images.py - resizes cards to small thumbnails before sending.
Uses Pillow only at send time, no caching needed (cards are tiny when small).
File naming: 2_of_clubs.png, king_of_hearts.png, ace_of_spades.png, back.png
"""

import os, io
import discord
from PIL import Image
from treys import Card

CARDS_DIR   = os.path.join(os.path.dirname(__file__), "cards")
CARD_HEIGHT = 80    # height in pixels — change this to tune size
CARD_GAP    = 4     # gap between cards in the strip

RANK_NAMES = {
    'A': 'ace',  '2': '2',  '3': '3', '4': '4', '5': '5',
    '6': '6',    '7': '7',  '8': '8', '9': '9', 'T': '10',
    'J': 'jack', 'Q': 'queen', 'K': 'king',
}
SUIT_NAMES = {'s': 'spades', 'h': 'hearts', 'd': 'diamonds', 'c': 'clubs'}

def card_filename(card_int: int) -> str:
    s = Card.int_to_str(card_int)
    return f"{RANK_NAMES[s[0]]}_of_{SUIT_NAMES[s[1]]}.png"

def card_path(card_int: int) -> str:
    return os.path.join(CARDS_DIR, card_filename(card_int))

def back_path() -> str:
    return os.path.join(CARDS_DIR, "back.png")

def cards_available() -> bool:
    if not os.path.isdir(CARDS_DIR):
        return False
    return len([f for f in os.listdir(CARDS_DIR) if f.endswith(".png") and f != "back.png"]) >= 52

def _resize(path: str) -> Image.Image:
    img = Image.open(path).convert("RGBA")
    w = int(img.width * CARD_HEIGHT / img.height)
    return img.resize((w, CARD_HEIGHT), Image.LANCZOS)

def make_strip(card_ints: list[int], backs: int = 0) -> discord.File:
    """
    Stitch all cards into one small horizontal strip PNG and return as a File.
    Single attachment = Discord renders it at a fixed small size instead of
    expanding it to fill the full message width.
    """
    images = [_resize(card_path(c)) for c in card_ints]
    images += [_resize(back_path()) for _ in range(backs)]

    if not images:
        buf = io.BytesIO(); Image.new("RGBA", (1,1)).save(buf, "PNG"); buf.seek(0)
        return discord.File(buf, filename="cards.png")

    W = sum(img.width for img in images) + CARD_GAP * (len(images) - 1)
    H = CARD_HEIGHT
    strip = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    x = 0
    for img in images:
        strip.paste(img, (x, 0), img)
        x += img.width + CARD_GAP

    buf = io.BytesIO()
    strip.save(buf, "PNG", optimize=True)
    buf.seek(0)
    return discord.File(buf, filename="cards.png")