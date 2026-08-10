from PIL import Image
import os
import math

CARDS_DIR = "./uno"

CARD_HEIGHT = 300         # baseline card height used across all hand sizes
OVERLAP = 40               # horizontal overlap between adjacent cards in a row
REFERENCE_ROW_COUNT = 6    # hand size whose natural row width sets the "canonical" width
MAX_SINGLE_ROW = 10        # hands larger than this split into two rows
ROW_GAP = 20                # vertical gap between rows when split


def _load_card_image(name: str, card_height: int) -> Image.Image:
    """Disk-backed loader — only used by stitch_hand() (unodemo.py's demo
    commands). The real game uses compose_hand_image() with a pre-loaded
    in-memory cache instead; see uno_cog.py's card image cache."""
    path = None
    for ext in (".png", ".jpg", ".jpeg", ".webp"):
        candidate = os.path.join(CARDS_DIR, name + ext)
        if os.path.exists(candidate):
            path = candidate
            break
    if not path:
        raise FileNotFoundError(f"Card image not found for: {name}")

    img = Image.open(path).convert("RGBA")
    return _resize_to_height(img, card_height)


def _resize_to_height(img: Image.Image, card_height: int) -> Image.Image:
    ratio = card_height / img.height
    return img.resize((int(img.width * ratio), card_height))


def _row_width(n_cards: int, card_width: int, overlap: int) -> int:
    if n_cards <= 0:
        return 0
    return card_width + (n_cards - 1) * (card_width - overlap)


def _paste_row(canvas: Image.Image, images: list[Image.Image], x_start: int, y: int, overlap: int):
    x = x_start
    card_width = images[0].width
    for img in images:
        canvas.paste(img, (x, y), img)
        x += card_width - overlap


def _compose(images: list[Image.Image], overlap: int, card_height: int) -> Image.Image:
    """
    Shared compositing core — takes already-loaded, already-resized RGBA
    card images and lays them out fanned/padded/split exactly as before:

      - Hands smaller than REFERENCE_ROW_COUNT (6) are padded with empty
        transparent space so the cards render at the same visual size as
        a 6-card hand — otherwise Discord upscales a narrow image and the
        cards look oversized for a 1-3 card hand.
      - Hands of REFERENCE_ROW_COUNT..MAX_SINGLE_ROW (6-10) render as a
        single row at natural width, unchanged from before.
      - Hands larger than MAX_SINGLE_ROW (10) split across two rows so
        the image grows taller instead of very wide, with cards staying
        the same size as the 6-10 card range (never shrunk to fit).

    Returns the composited PIL Image — caller decides whether to save it
    to disk or encode it straight to bytes.
    """
    if not images:
        raise ValueError("No cards to stitch")

    card_width = images[0].width
    n = len(images)
    reference_width = _row_width(REFERENCE_ROW_COUNT, card_width, overlap)

    if n <= MAX_SINGLE_ROW:
        row_width = _row_width(n, card_width, overlap)
        canvas_width = max(row_width, reference_width) if n < REFERENCE_ROW_COUNT else row_width

        canvas = Image.new("RGBA", (canvas_width, card_height), (0, 0, 0, 0))
        x_start = (canvas_width - row_width) // 2  # centers a padded (smaller) hand
        _paste_row(canvas, images, x_start, 0, overlap)
    else:
        # split into two rows — first row gets the extra card if odd count
        top_n = math.ceil(n / 2)
        top_images, bottom_images = images[:top_n], images[top_n:]

        top_width = _row_width(len(top_images), card_width, overlap)
        bottom_width = _row_width(len(bottom_images), card_width, overlap)
        canvas_width = max(top_width, bottom_width, reference_width)
        canvas_height = card_height * 2 + ROW_GAP

        canvas = Image.new("RGBA", (canvas_width, canvas_height), (0, 0, 0, 0))
        _paste_row(canvas, top_images, (canvas_width - top_width) // 2, 0, overlap)
        _paste_row(canvas, bottom_images, (canvas_width - bottom_width) // 2, card_height + ROW_GAP, overlap)

    return canvas


def compose_hand_image(card_names: list[str], image_cache: dict[str, Image.Image],
                        overlap: int = OVERLAP, card_height: int = CARD_HEIGHT) -> Image.Image:
    """
    In-memory hand composite — no disk I/O at all. image_cache is a dict
    of card_name -> already-loaded RGBA PIL Image (see uno_cog.py's
    startup card cache). Returns the composited Image directly; caller
    encodes it to bytes (io.BytesIO) for a discord.File attachment
    instead of saving anywhere.
    """
    if not card_names:
        raise ValueError("No cards to stitch")

    missing = [name for name in card_names if name not in image_cache]
    if missing:
        raise FileNotFoundError(f"Card image(s) not in cache: {', '.join(missing)}")

    images = [_resize_to_height(image_cache[name], card_height) for name in card_names]
    return _compose(images, overlap, card_height)


def stitch_hand(card_names: list[str], out_path: str = "./hand_preview.png",
                 overlap: int = OVERLAP, card_height: int = CARD_HEIGHT) -> str:
    """
    Disk-backed version, kept for unodemo.py's demo/testing commands
    (which want a file path to work with directly). Reads card images
    from disk on every call and saves the result to out_path. The real
    game uses compose_hand_image() instead — see uno_cog.py.
    """
    images = [_load_card_image(name, card_height) for name in card_names]
    canvas = _compose(images, overlap, card_height)
    canvas.save(out_path)
    return out_path


if __name__ == "__main__":
    # quick manual sanity check across the size ranges
    print(stitch_hand(["Blue_4", "Green_Skip"], out_path="./_test_2.png"))
    print(stitch_hand(["Blue_4", "Green_Skip", "Red_1", "Yellow_2", "Blue_9", "Green_7"], out_path="./_test_6.png"))
    print(stitch_hand(["Blue_4"] * 15, out_path="./_test_15.png"))