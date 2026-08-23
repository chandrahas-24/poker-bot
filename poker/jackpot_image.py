"""Pillow renderer for the poker jackpot panel.

Uses the supplied 1024x768 template as the static artwork and replaces only
runtime values. Put the template at:
    poker/assets/jackpot_template.png

The template contains the server logo, cards, shiny-card artwork, chip artwork,
and all decorative elements. This renderer only changes the jackpot amounts
(and optionally the payout percentages).
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ASSETS = Path(__file__).resolve().parent / "assets"
TEMPLATE = ASSETS / "jackpot_template.png"
FONT_BOLD = ASSETS / "DejaVuSans-Bold.ttf"

# Template is 1024 x 768.
W, H = 1024, 768

# Static artwork locations from the supplied template.
# The chip is cropped from the template so it always matches the exact chip
# shown in the design.
TOTAL_CHIP_BOX = (338, 207, 403, 274)
ROW_CHIP_BOX = (163, 543, 210, 590)

# Areas containing the old runtime numbers. They are intentionally larger than
# the old text so values with more digits cannot leave remnants behind.
TOTAL_VALUE_BOX = (45, 184, 410, 282)
ROW_VALUE_BOXES = [
    (45, 535, 213, 600),
    (270, 535, 474, 600),
    (520, 535, 735, 600),
    (775, 535, 970, 600),
]

# Background colors sampled from the template. Keeping these very close to the
# source avoids creating obvious rectangles when runtime text is replaced.
TOP_BG = (15, 20, 26)
PANEL_BGS = [
    (16, 21, 27),
    (16, 21, 27),
    (16, 21, 27),
    (16, 21, 27),
]

WHITE = (242, 244, 247)


def _font(size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(FONT_BOLD), size)


def _fit_font(text: str, max_width: int, start_size: int, min_size: int = 22):
    """Return the largest bold DejaVu font that fits max_width."""
    size = start_size
    while size > min_size:
        f = _font(size)
        bbox = f.getbbox(text)
        if bbox[2] - bbox[0] <= max_width:
            return f
        size -= 1
    return _font(min_size)


def _text_width(draw: ImageDraw.ImageDraw, text: str, font) -> int:
    box = draw.textbbox((0, 0), text, font=font)
    return box[2] - box[0]


def _paste_chip_group(
    image: Image.Image,
    draw: ImageDraw.ImageDraw,
    amount: int,
    region: tuple[int, int, int, int],
    chip: Image.Image,
    start_font: int,
    chip_gap: int = 10,
):
    """Replace one amount + chip group and keep the group centered."""
    x1, y1, x2, y2 = region
    max_width = x2 - x1

    text = f"{amount:,}"

    # Leave enough room for the chip while fitting the entire group.
    chip_w, chip_h = chip.size
    font = _fit_font(
        text,
        max_width=max_width - chip_w - chip_gap,
        start_size=start_font,
        min_size=20,
    )

    text_w = _text_width(draw, text, font)
    group_w = text_w + chip_gap + chip_w

    # Center the number + chip as a single unit.
    gx = x1 + max(0, (max_width - group_w) // 2)

    bbox = font.getbbox(text)
    text_h = bbox[3] - bbox[1]
    chip_y = y1 + ((y2 - y1) - chip_h) // 2
    text_y = y1 + ((y2 - y1) - text_h) // 2 - bbox[1]

    draw.text((gx, text_y), text, font=font, fill=WHITE)
    image.alpha_composite(chip, (gx + text_w + chip_gap, chip_y))


def _clear_box(draw: ImageDraw.ImageDraw, box, fill):
    draw.rectangle(box, fill=fill)


def generate_jackpot_image(
    total: int,
    quads: int,
    straight_flush: int,
    royal_flush: int,
    shiny_card: int,
    *,
    quads_pct: int = 5,
    straight_flush_pct: int = 20,
    royal_flush_pct: int = 60,
    shiny_card_pct: int = 80,
) -> BytesIO:
    """Render the jackpot panel with live values.

    The first five arguments are the values returned by your jackpot system:
        total, quads, straight_flush, royal_flush, shiny_card

    The payout percentages default to the current rules: 5/20/60/80.
    They are optional so the same renderer can support future rule changes.
    """
    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Jackpot template not found: {TEMPLATE}")
    if not FONT_BOLD.exists():
        raise FileNotFoundError(f"Font not found: {FONT_BOLD}")

    image = Image.open(TEMPLATE).convert("RGBA")
    if image.size != (W, H):
        image = image.resize((W, H), Image.Resampling.LANCZOS)

    draw = ImageDraw.Draw(image)

    # Crop the exact chip artwork from the supplied template.
    total_chip = image.crop(TOTAL_CHIP_BOX)
    row_chip = image.crop(ROW_CHIP_BOX)

    # Clear old runtime values. Static artwork remains untouched.
    _clear_box(draw, TOTAL_VALUE_BOX, TOP_BG)
    for box, bg in zip(ROW_VALUE_BOXES, PANEL_BGS):
        _clear_box(draw, box, bg)

    # Redraw the live amounts.
    _paste_chip_group(
        image, draw, total, TOTAL_VALUE_BOX, total_chip, start_font=68, chip_gap=12
    )

    row_values = [quads, straight_flush, royal_flush, shiny_card]
    for value, box in zip(row_values, ROW_VALUE_BOXES):
        _paste_chip_group(
            image, draw, value, box, row_chip, start_font=39, chip_gap=8
        )

    # Payout percentages, labels, cards, logos, and all other decorative
    # elements remain part of the static template. Only jackpot amounts are
    # runtime data.

    output = BytesIO()
    image.save(output, format="PNG", optimize=True)
    output.seek(0)
    return output


__all__ = ["generate_jackpot_image"]
