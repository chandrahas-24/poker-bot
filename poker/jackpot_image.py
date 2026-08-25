from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
ASSETS_DIR = BASE_DIR / "assets"

TEMPLATE = ASSETS_DIR / "jackpot_template.png"
CHIP_PATH = ASSETS_DIR / "md_chip.png"
FONT_PATH = ASSETS_DIR / "Rockwell_Extra_Bold_Regular.ttf"


# ============================================================
# COLORS
# ============================================================

BG = (17, 21, 26)          # #11151A
WHITE = (240, 240, 240)
MUTED = (155, 158, 165)


# ============================================================
# FONTS
# ============================================================

def get_font(size: int):
    return ImageFont.truetype(
        str(FONT_PATH),
        size,
    )


# ============================================================
# DRAWING HELPERS
# ============================================================

def draw_centered(
    draw: ImageDraw.ImageDraw,
    cx: float,
    cy: float,
    text: str,
    font,
    fill=WHITE,
):
    """
    Draw text centered around the supplied coordinates.
    Uses the actual font bounding box so Rockwell's glyph
    metrics don't cause alignment problems.
    """

    bbox = draw.textbbox(
        (0, 0),
        text,
        font=font,
    )

    width = bbox[2] - bbox[0]
    height = bbox[3] - bbox[1]

    x = cx - width / 2 - bbox[0]
    y = cy - height / 2 - bbox[1]

    draw.text(
        (x, y),
        text,
        font=font,
        fill=fill,
    )


def erase_region(
    draw: ImageDraw.ImageDraw,
    box,
):
    """
    Remove an existing dynamic element from the template.
    """

    draw.rectangle(
        box,
        fill=BG,
    )


def load_chip(size: int):
    """
    Load the MD chip and resize it.
    """

    chip = Image.open(CHIP_PATH).convert("RGBA")

    return chip.resize(
        (size, size),
        Image.Resampling.LANCZOS,
    )


# ============================================================
# REWARD AMOUNT
# ============================================================

def draw_reward_amount(
    img: Image.Image,
    draw: ImageDraw.ImageDraw,
    center_x: int,
    center_y: int,
    amount: str,
    font,
    chip,
):
    """
    Draw a reward amount with an MD chip immediately after it,
    centered as a single visual group.
    """

    bbox = draw.textbbox(
        (0, 0),
        amount,
        font=font,
    )

    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]

    gap = 9

    total_width = (
        text_width
        + gap
        + chip.width
    )

    left = center_x - total_width / 2

    text_y = (
        center_y
        - text_height / 2
        - bbox[1]
    )

    draw.text(
        (
            left - bbox[0],
            text_y,
        ),
        amount,
        font=font,
        fill=WHITE,
    )

    chip_x = int(
        left
        + text_width
        + gap
    )

    chip_y = int(
        center_y
        - chip.height / 2
    )

    img.alpha_composite(
        chip,
        (chip_x, chip_y),
    )


# ============================================================
# MAIN GENERATOR
# ============================================================

def generate_jackpot_image(
    total_pool: int,
    quads: int,
    straight_flush: int,
    royal_flush: int,
    shiny_card_win: int,
    output_path=None,
):
    """
    Generate the jackpot image from the fixed template.

    Dynamic values:
        total_pool
        quads
        straight_flush
        royal_flush
        shiny_card_win

    The payout percentage section at the bottom of the
    template is intentionally NOT modified.
    """

    # ========================================================
    # LOAD TEMPLATE
    # ========================================================

    if not TEMPLATE.exists():
        raise FileNotFoundError(
            f"Jackpot template not found: {TEMPLATE}"
        )

    if not CHIP_PATH.exists():
        raise FileNotFoundError(
            f"MD chip image not found: {CHIP_PATH}"
        )

    if not FONT_PATH.exists():
        raise FileNotFoundError(
            f"Font not found: {FONT_PATH}"
        )

    img = Image.open(TEMPLATE).convert("RGBA")
    draw = ImageDraw.Draw(img)

    # ========================================================
    # FONTS
    # ========================================================

    title_font = get_font(40)
    pool_label_font = get_font(20)
    total_font = get_font(68)

    reward_title_font = get_font(20)
    reward_amount_font = get_font(40)

    # ========================================================
    # CHIPS
    # ========================================================

    total_chip = load_chip(62)
    reward_chip = load_chip(46)

    # ========================================================
    # CARD CENTERS
    # ========================================================

    card_centers = {
        "quads": 145,
        "straight": 386,
        "royal": 635,
        "shiny": 879,
    }

    # ========================================================
    # HEADER
    # ========================================================

    # Remove the template's original Jackpot title.
    #
    # The DEN server logo is untouched.

    erase_region(
        draw,
        (125, 45, 320, 112),
    )

    # Center vertically with the DEN logo and keep the title
    # close to it.

    draw_centered(
        draw,
        225,
        82,
        "Jackpot",
        title_font,
        WHITE,
    )

    # ========================================================
    # TOTAL POOL LABEL
    # ========================================================

    erase_region(
        draw,
        (50, 148, 205, 185),
    )

    draw.text(
        (57, 150),
        "TOTAL POOL",
        font=pool_label_font,
        fill=MUTED,
    )

    # ========================================================
    # TOTAL POOL AMOUNT
    # ========================================================

    # Remove BOTH the template number and template chip.

    erase_region(
        draw,
        (45, 188, 410, 280),
    )

    total_amount = f"{total_pool:,}"

    total_bbox = draw.textbbox(
        (0, 0),
        total_amount,
        font=total_font,
    )

    total_text_width = (
        total_bbox[2] - total_bbox[0]
    )

    total_text_height = (
        total_bbox[3] - total_bbox[1]
    )

    # Keep this LEFT ALIGNED like the template.

    total_x = 55
    total_center_y = 230
    total_gap = 13

    total_text_y = (
        total_center_y
        - total_text_height / 2
        - total_bbox[1]
    )

    draw.text(
        (
            total_x - total_bbox[0],
            total_text_y,
        ),
        total_amount,
        font=total_font,
        fill=WHITE,
    )

    img.alpha_composite(
        total_chip,
        (
            int(
                total_x
                + total_text_width
                + total_gap
            ),
            int(
                total_center_y
                - total_chip.height / 2
            ),
        ),
    )

    # ========================================================
    # REWARD TITLES
    # ========================================================

    # --------------------------------------------------------
    # QUADS
    # --------------------------------------------------------

    erase_region(
        draw,
        (70, 345, 220, 385),
    )

    draw_centered(
        draw,
        card_centers["quads"],
        365,
        "QUADS",
        reward_title_font,
    )

    # --------------------------------------------------------
    # STRAIGHT FLUSH
    # --------------------------------------------------------

    erase_region(
        draw,
        (275, 345, 497, 385),
    )

    draw_centered(
        draw,
        card_centers["straight"],
        365,
        "STRAIGHT FLUSH",
        reward_title_font,
    )

    # --------------------------------------------------------
    # ROYAL FLUSH
    # --------------------------------------------------------

    erase_region(
        draw,
        (525, 345, 745, 385),
    )

    draw_centered(
        draw,
        card_centers["royal"],
        365,
        "ROYAL FLUSH",
        reward_title_font,
    )

    # --------------------------------------------------------
    # SHINY CARD WIN
    # --------------------------------------------------------

    erase_region(
        draw,
        (770, 345, 985, 385),
    )

    draw_centered(
        draw,
        card_centers["shiny"],
        365,
        "SHINY CARD WIN",
        reward_title_font,
    )

    # ========================================================
    # REWARD AMOUNTS
    # ========================================================

    # Remove the old template amounts + chips.

    erase_region(
        draw,
        (65, 535, 215, 595),
    )

    erase_region(
        draw,
        (275, 535, 465, 595),
    )

    erase_region(
        draw,
        (520, 535, 715, 595),
    )

    erase_region(
        draw,
        (765, 535, 965, 595),
    )

    # Draw dynamic amounts.

    draw_reward_amount(
        img,
        draw,
        card_centers["quads"],
        565,
        f"{quads:,}",
        reward_amount_font,
        reward_chip,
    )

    draw_reward_amount(
        img,
        draw,
        card_centers["straight"],
        565,
        f"{straight_flush:,}",
        reward_amount_font,
        reward_chip,
    )

    draw_reward_amount(
        img,
        draw,
        card_centers["royal"],
        565,
        f"{royal_flush:,}",
        reward_amount_font,
        reward_chip,
    )

    draw_reward_amount(
        img,
        draw,
        card_centers["shiny"],
        565,
        f"{shiny_card_win:,}",
        reward_amount_font,
        reward_chip,
    )

    # ========================================================
    # IMPORTANT:
    #
    # DO NOT TOUCH THE PAYOUT SECTION.
    #
    # The template already contains:
    #
    #     ♣  QUADS          5%
    #     ♠  STRAIGHT FLUSH 20%
    #     ♥  ROYAL FLUSH    60%
    #     ✦  SHINY CARD WIN 80%
    #
    # Those are fixed and are intentionally left exactly as
    # they exist in jackpot_template.png.
    # ========================================================

    # ========================================================
    # OUTPUT
    # ========================================================

    if output_path is None:
        output_path = (
            ASSETS_DIR / "jackpot_generated.png"
        )
    else:
        output_path = Path(output_path)

    img = img.convert("RGB")

    img.save(
        output_path,
        format="PNG",
        optimize=True,
    )

    return output_path