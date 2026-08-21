from pathlib import Path
from io import BytesIO

from PIL import Image, ImageDraw, ImageFont


# ============================================================
# Paths
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

ASSETS_DIR = BASE_DIR / "assets"

FONT_REGULAR = ASSETS_DIR / "DejaVuSans.ttf"
FONT_BOLD = ASSETS_DIR / "DejaVuSans-Bold.ttf"

TWEMOJI_DIR = ASSETS_DIR / "twemoji"


# ============================================================
# Twemoji
# ============================================================

TWEMOJI = {
    "trophy": Image.open(
        TWEMOJI_DIR / "trophy.png"
    ).convert("RGBA"),

    "gold": Image.open(
        TWEMOJI_DIR / "gold.png"
    ).convert("RGBA"),

    "silver": Image.open(
        TWEMOJI_DIR / "silver.png"
    ).convert("RGBA"),

    "bronze": Image.open(
        TWEMOJI_DIR / "bronze.png"
    ).convert("RGBA"),

    "stats": Image.open(
        TWEMOJI_DIR / "stats.png"
    ).convert("RGBA"),
}


# ============================================================
# Fonts
# ============================================================

def _font(size: int, bold: bool = False):
    return ImageFont.truetype(
        FONT_BOLD if bold else FONT_REGULAR,
        size,
    )


# ============================================================
# Helper: paste emoji
# ============================================================

def _paste_scaled(
    image,
    emoji,
    position,
    size,
):
    """Paste a Twemoji image at a fixed maximum size."""

    if emoji is None:
        return

    emoji = emoji.copy()

    emoji.thumbnail(
        size,
        Image.Resampling.LANCZOS,
    )

    image.alpha_composite(
        emoji,
        position,
    )


# ============================================================
# Leaderboard image
# ============================================================

def generate_leaderboard_image(
    rows,
    caller_id,
    caller_row,
    caller_rank,
):
    WIDTH = 900
    ROW_HEIGHT = 62

    HEIGHT = (
        180
        + len(rows) * ROW_HEIGHT
        + 150
    )

    # ========================================================
    # Colors
    # ========================================================

    bg = (30, 31, 43)

    panel = (45, 46, 61)
    panel_inner = (52, 54, 71)

    white = (235, 237, 245)
    muted = (170, 173, 190)

    green = (80, 200, 120)
    red = (230, 80, 80)

    # Current player highlight
    player_highlight = (63, 66, 87)

    # Top 3 accent colors
    gold_accent = (241, 196, 15)
    silver_accent = (190, 195, 205)
    bronze_accent = (180, 105, 55)

    rank_accents = {
        1: gold_accent,
        2: silver_accent,
        3: bronze_accent,
    }

    # ========================================================
    # Canvas
    # ========================================================

    image = Image.new(
        "RGBA",
        (WIDTH, HEIGHT),
        bg + (255,),
    )

    draw = ImageDraw.Draw(image)

    # ========================================================
    # Main panel
    # ========================================================

    draw.rounded_rectangle(
        (
            20,
            20,
            WIDTH - 20,
            HEIGHT - 20,
        ),
        radius=18,
        fill=panel + (255,),
    )

    # ========================================================
    # Title
    # ========================================================

    title_y = 42

    _paste_scaled(
        image,
        TWEMOJI["trophy"],
        (55, title_y),
        (38, 38),
    )

    draw.text(
        (102, title_y),
        "Poker Leaderboard",
        font=_font(38, True),
        fill=white,
    )

    # ========================================================
    # Table
    # ========================================================

    table_x1 = 50
    table_x2 = WIDTH - 50

    table_y1 = 105

    table_y2 = (
        table_y1
        + 55
        + len(rows) * ROW_HEIGHT
    )

    draw.rounded_rectangle(
        (
            table_x1,
            table_y1,
            table_x2,
            table_y2,
        ),
        radius=12,
        fill=panel_inner + (255,),
    )

    # Existing column positions
    rank_x = 75
    player_x = 145
    win_x = 665
    net_x = 825

    # ========================================================
    # Headers
    # ========================================================

    draw.text(
        (player_x, table_y1 + 13),
        "Player",
        font=_font(24, True),
        fill=white,
    )

    draw.text(
        (win_x, table_y1 + 13),
        "Win%",
        font=_font(24, True),
        fill=white,
        anchor="ra",
    )

    draw.text(
        (net_x, table_y1 + 13),
        "Net",
        font=_font(24, True),
        fill=white,
        anchor="ra",
    )

    # ========================================================
    # Header divider
    # ========================================================

    divider_y = table_y1 + 55

    draw.line(
        (
            table_x1 + 20,
            divider_y,
            table_x2 - 20,
            divider_y,
        ),
        fill=(220, 222, 232),
        width=2,
    )

    # ========================================================
    # Rows
    # ========================================================

    for i, r in enumerate(rows):

        rank = i + 1

        y = (
            divider_y
            + 9
            + i * ROW_HEIGHT
        )

        is_you = (
            r["user_id"] == caller_id
        )

        # ----------------------------------------------------
        # Top 3 decorative accent
        # ----------------------------------------------------

        if rank in rank_accents:

            accent = rank_accents[rank]

            # Thin colored strip on the left only
            draw.rounded_rectangle(
                (
                    table_x1 + 8,
                    y - 3,
                    table_x1 + 14,
                    y + ROW_HEIGHT - 4,
                ),
                radius=3,
                fill=accent + (255,),
            )

        # ----------------------------------------------------
        # Current player highlight
        # ----------------------------------------------------

        elif is_you:

            draw.rounded_rectangle(
                (
                    table_x1 + 8,
                    y - 3,
                    table_x2 - 8,
                    y + ROW_HEIGHT - 4,
                ),
                radius=9,
                fill=player_highlight + (255,),
            )

        # ----------------------------------------------------
        # Win percentage
        # ----------------------------------------------------

        if r["hands_played"]:

            win_percentage = (
                r["hands_won"]
                / r["hands_played"]
                * 100
            )

            wp_text = f"{win_percentage:.0f}%"

        else:
            wp_text = "—"

        # ----------------------------------------------------
        # Net
        # ----------------------------------------------------

        net = r["net_chips"]
        net_text = f"{net:+d}"

        net_color = (
            green
            if net >= 0
            else red
        )

        # ----------------------------------------------------
        # Rank / medal
        # ----------------------------------------------------

        if rank == 1:

            _paste_scaled(
                image,
                TWEMOJI["gold"],
                (rank_x - 2, y + 10),
                (34, 34),
            )

        elif rank == 2:

            _paste_scaled(
                image,
                TWEMOJI["silver"],
                (rank_x - 2, y + 10),
                (34, 34),
            )

        elif rank == 3:

            _paste_scaled(
                image,
                TWEMOJI["bronze"],
                (rank_x - 2, y + 10),
                (34, 34),
            )

        else:

            draw.text(
                (rank_x, y + 10),
                f"{rank}.",
                font=_font(25, True),
                fill=white,
            )

        # ----------------------------------------------------
        # Username
        # ----------------------------------------------------

        # Discord username ONLY.
        username = r["username"]

        username_font = _font(
            24,
            True,
        )

        max_username_width = 470

        # Only truncate if the rendered username
        # physically exceeds the available column.
        while (
            len(username) > 1
            and draw.textbbox(
                (0, 0),
                username,
                font=username_font,
            )[2] > max_username_width
        ):
            username = username[:-1]

        if username != r["username"]:
            username += "…"

        draw.text(
            (player_x, y + 10),
            username,
            font=username_font,
            fill=white,
        )

        # ----------------------------------------------------
        # Win %
        # ----------------------------------------------------

        draw.text(
            (win_x, y + 10),
            wp_text,
            font=_font(24, True),
            fill=white,
            anchor="ra",
        )

        # ----------------------------------------------------
        # Net
        # ----------------------------------------------------

        draw.text(
            (net_x, y + 10),
            net_text,
            font=_font(24, True),
            fill=net_color,
            anchor="ra",
        )

    # ========================================================
    # Your Stats
    # ========================================================

    stats_y = table_y2 + 35

    _paste_scaled(
        image,
        TWEMOJI["stats"],
        (55, stats_y),
        (30, 30),
    )

    draw.text(
        (95, stats_y),
        "Your Stats",
        font=_font(28, True),
        fill=white,
    )

    # --------------------------------------------------------
    # Stats data
    # --------------------------------------------------------

    if caller_row:

        rank_text = (
            f"#{caller_rank}"
            if caller_rank
            else "—"
        )

        if caller_row["hands_played"]:

            caller_win_percentage = (
                caller_row["hands_won"]
                / caller_row["hands_played"]
                * 100
            )

            caller_wp_text = (
                f"{caller_win_percentage:.1f}%"
            )

        else:
            caller_wp_text = "—"

        caller_net = caller_row["net_chips"]

        stats_text = (
            f"{rank_text}  ·  "
            f"Win% {caller_wp_text}  ·  "
            f"Net {caller_net:+d}  ·  "
            f"Wallet {caller_row['wallet']}"
        )

        draw.text(
            (55, stats_y + 48),
            stats_text,
            font=_font(23, True),
            fill=muted,
        )

    else:

        draw.text(
            (55, stats_y + 48),
            "No hands played yet.",
            font=_font(23, True),
            fill=muted,
        )

    # ========================================================
    # Export
    # ========================================================

    output = BytesIO()

    image.convert("RGB").save(
        output,
        format="PNG",
        optimize=True,
    )

    output.seek(0)

    return output