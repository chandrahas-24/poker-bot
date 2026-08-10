import os
import discord
from discord import app_commands
from discord.ext import commands
from discord.ui import LayoutView, Container, TextDisplay, Separator, MediaGallery, ActionRow, Button

from .hand_render import stitch_hand, CARDS_DIR
from .cards import load_all_cards
import random


FALLBACK_EMOJI_BY_COLOR = {
    "Blue": "🟦",
    "Green": "🟩",
    "Red": "🟥",
    "Yellow": "🟨",
    "Wild": "⬛",
}


COLOR_ORDER = {"Red": 0, "Yellow": 1, "Green": 2, "Blue": 3, "Wild": 4}

# Within a color: numbers 0-9 first (ascending), then action cards, alphabetically.
ACTION_ORDER = {"Skip": 10, "Reverse": 11, "Draw_2": 12, "Draw_4": 13}


def sort_key(card_name: str):
    parts = card_name.split("_")
    color = parts[0]
    value = "_".join(parts[1:]) if len(parts) > 1 else parts[0]

    color_rank = COLOR_ORDER.get(color, 99)

    if value.isdigit():
        value_rank = int(value)
    elif color == "Wild":
        # "Wild" alone vs "Wild_Draw_4" -> keep plain Wild before Wild Draw 4
        value_rank = 0 if value == "Wild" or value == "" else ACTION_ORDER.get(value, 20)
    else:
        value_rank = ACTION_ORDER.get(value, 20)

    return (color_rank, value_rank)


def sort_hand(hand: list[str]) -> list[str]:
    return sorted(hand, key=sort_key)


SHORT_LABELS = {
    "Draw_2": "+2",
    "Draw_4": "+4",
    "Reverse": "R",
    "Skip": "S",
    "Wild": "W",
}


def display_label(card_name: str) -> str:
    """'Blue_4' -> '4', 'Blue_Draw_2' -> '+2', 'Wild_Draw_4' -> 'W+4', 'Wild' -> 'W'"""
    parts = card_name.split("_")
    if parts[0] == "Wild":
        rest = "_".join(parts[1:]) if len(parts) > 1 else ""
        if not rest:
            return "W"
        if rest == "Draw_4":
            return "W+4"
        return "W" + SHORT_LABELS.get(rest, rest)
    value = "_".join(parts[1:]) if len(parts) > 1 else parts[0]
    return SHORT_LABELS.get(value, value)


def sanitize_emoji_name(name: str) -> str:
    """Must match the same sanitizing used in upload_emojis.py"""
    cleaned = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    cleaned = cleaned.strip("_")
    if len(cleaned) < 2:
        cleaned = cleaned + "_c"
    return cleaned[:32]


MAX_ROWS = 5
MAX_BUTTONS_PER_ROW = 5
MAX_BUTTONS_TOTAL = MAX_ROWS * MAX_BUTTONS_PER_ROW


def group_rows_by_color(hand: list[str]) -> list[list[str]]:
    """
    Groups sorted cards into button rows: each color starts a fresh row
    (never mixes two colors in one row), wraps to a new row if a single
    color has more than 5 cards, and Wilds always end up in their own
    trailing row(s).
    """
    rows: list[list[str]] = []
    current_color = None
    current_row: list[str] = []

    for card_name in hand:
        color = card_name.split("_")[0]

        if color != current_color or len(current_row) >= MAX_BUTTONS_PER_ROW:
            if current_row:
                rows.append(current_row)
            current_row = []
            current_color = color

        current_row.append(card_name)

    if current_row:
        rows.append(current_row)

    return rows


def build_rows_with_fallback(hand: list[str]) -> tuple[list[list[str]], bool]:
    """
    Tries the color-grouped layout first. If that produces more than
    MAX_ROWS rows (can happen with an uneven color mix that forces lots
    of row breaks even though the total button count would fit), falls
    back to a simple flat chunk-of-5 layout that ignores color grouping
    but always fits. Returns (rows, used_fallback).
    """
    rows = group_rows_by_color(hand)
    if len(rows) <= MAX_ROWS:
        return rows, False

    flat_rows = [hand[i:i + MAX_BUTTONS_PER_ROW] for i in range(0, len(hand), MAX_BUTTONS_PER_ROW)]
    return flat_rows, True


class UnoDemo(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # card_name -> discord.Emoji (or discord.PartialEmoji), populated on first use
        self._emoji_cache: dict[str, discord.PartialEmoji] = {}
        self._emoji_cache_loaded = False

    async def _load_emoji_cache(self):
        """Fetch this application's global emojis and index them by sanitized name."""
        if self._emoji_cache_loaded:
            return
        try:
            app_emojis = await self.bot.fetch_application_emojis()
            for e in app_emojis:
                self._emoji_cache[e.name] = e
            print(f"[unodemo] loaded {len(app_emojis)} application emoji(s)")
        except Exception as ex:
            print(f"[unodemo] failed to load application emojis: {ex}")
        self._emoji_cache_loaded = True

    def emoji_for(self, card_name: str):
        sanitized = sanitize_emoji_name(card_name)
        if sanitized in self._emoji_cache:
            return self._emoji_cache[sanitized]
        color = card_name.split("_")[0]
        return FALLBACK_EMOJI_BY_COLOR.get(color, "🃏")

    @app_commands.command(name="unodemo", description="Test the stitched-hand button UI")
    @app_commands.describe(n="Number of random cards to draw (default 20)")
    async def unodemo(self, interaction: discord.Interaction, n: int = 20):
        await self._load_emoji_cache()

        # Pull a random sample of N cards from every card image available in CARDS_DIR
        all_cards = list(load_all_cards().keys())
        sample_size = max(1, min(n, len(all_cards)))
        hand = random.sample(all_cards, sample_size)
        hand = sort_hand(hand)

        truncated = False
        if len(hand) > MAX_BUTTONS_TOTAL:
            hand = hand[:MAX_BUTTONS_TOTAL]
            truncated = True

        try:
            image_path = stitch_hand(hand, out_path="./hand_preview_tmp.png")
        except FileNotFoundError as e:
            await interaction.response.send_message(
                f"⚠️ {e}\nMake sure card images are unzipped into `{CARDS_DIR}`.",
                ephemeral=True,
            )
            return

        filename = os.path.basename(image_path)
        file = discord.File(image_path, filename=filename)

        container = Container(accent_colour=discord.Colour.blurple())
        header = f"🎴 **Your hand** — {len(hand)} cards, tap one to play"
        if truncated:
            header += f" (capped at {MAX_BUTTONS_TOTAL} for this demo)"
        container.add_item(TextDisplay(header))
        container.add_item(MediaGallery(discord.MediaGalleryItem(f"attachment://{filename}")))
        container.add_item(Separator(spacing=discord.SeparatorSpacing.small))

        # Group into rows: each color gets its own row(s), never mixed;
        # falls back to a flat 5-per-row layout if color grouping alone
        # would need more than 5 rows.
        rows, used_fallback = build_rows_with_fallback(hand)
        if used_fallback:
            container.add_item(TextDisplay("_(color grouping didn't fit in 5 rows — showing a flat layout instead)_"))

        for row_cards in rows:
            row = ActionRow()
            for card_name in row_cards:
                row.add_item(Button(
                    label=display_label(card_name),
                    emoji=self.emoji_for(card_name),
                    style=discord.ButtonStyle.secondary,
                    custom_id=f"unodemo_play_{card_name}",
                ))
            container.add_item(row)

        view = LayoutView()
        view.add_item(container)

        await interaction.response.send_message(view=view, file=file, ephemeral=True)

    @app_commands.command(name="unodemoall", description="Send a hand demo for every size from 1 to 25 cards")
    async def unodemoall(self, interaction: discord.Interaction):
        await self._load_emoji_cache()
        await interaction.response.send_message("Sending demos for 1–25 cards...", ephemeral=True)

        all_cards = list(load_all_cards().keys())
        if not all_cards:
            await interaction.followup.send(
                f"⚠️ No card images found in `{CARDS_DIR}`.", ephemeral=True
            )
            return

        for size in range(1, 26):
            sample_size = min(size, len(all_cards))
            hand = random.sample(all_cards, sample_size)
            hand = sort_hand(hand)

            try:
                image_path = stitch_hand(hand, out_path=f"./hand_preview_{size}.png")
            except FileNotFoundError:
                continue

            filename = os.path.basename(image_path)
            file = discord.File(image_path, filename=filename)

            container = Container(accent_colour=discord.Colour.blurple())
            container.add_item(TextDisplay(f"🎴 **{size} card hand**"))
            container.add_item(MediaGallery(discord.MediaGalleryItem(f"attachment://{filename}")))
            container.add_item(Separator(spacing=discord.SeparatorSpacing.small))

            rows, used_fallback = build_rows_with_fallback(hand)
            if used_fallback:
                container.add_item(TextDisplay("_(flat layout — color grouping didn't fit)_"))

            for row_cards in rows:
                row = ActionRow()
                for card_name in row_cards:
                    row.add_item(Button(
                        label=display_label(card_name),
                        emoji=self.emoji_for(card_name),
                        style=discord.ButtonStyle.secondary,
                        custom_id=f"unodemo_play_{card_name}",
                    ))
                container.add_item(row)

            view = LayoutView()
            view.add_item(container)

            # Not ephemeral — visible in the channel
            await interaction.followup.send(view=view, file=file)

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type != discord.InteractionType.component:
            return
        custom_id = interaction.data.get("custom_id", "")
        if custom_id.startswith("unodemo_play_"):
            card = custom_id.removeprefix("unodemo_play_")
            await interaction.response.send_message(f"You played: {card}", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(UnoDemo(bot))