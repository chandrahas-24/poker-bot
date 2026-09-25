"""tax_experiment.py — "no flop no drop" tax shadow test.

Purely additive shadow logging: for every resolved hand, records what tax
was actually collected and whether the hand was decided by an uncontested
preflop fold (no community cards ever dealt). Never touches chips, pot, or
the real tax pipeline in taxation.py — this module only reads/writes its
own table (tax_experiment_log).

Hypothetical ("no flop no drop") tax is derived at query time:
    0              if resolved_preflop_fold
    actual_tax     otherwise

Kept fully separate from poker.py on purpose — this is a throwaway test
harness for a one-week run, meant to be easy to rip out afterward without
touching the main bot file.
"""

import os
import calendar
from datetime import datetime

import discord
from discord import app_commands
from discord.ext import commands
from PIL import Image, ImageDraw, ImageFont

import config
from . import database as db

GUILD_ROLE_ID = 1010238899320270999  # same access role /pokeradmin salt uses


async def log_hand(guild_id: int, table_id: str, table_name: str, hand_num: int,
                    player_count: int, resolved_preflop_fold: bool, actual_tax: int):
    """Best-effort shadow log. Never raises into the caller."""
    try:
        conn = await db._get_db()
        async with db._write_lock:
            await conn.execute(
                "INSERT INTO tax_experiment_log "
                "(ts, guild_id, table_id, table_name, hand_num, player_count, resolved_preflop_fold, actual_tax) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (datetime.utcnow().isoformat(), guild_id, table_id, table_name, hand_num,
                 player_count, int(resolved_preflop_fold), actual_tax)
            )
            await conn.commit()
    except Exception as e:
        print(f"[tax_experiment] write error: {e}")


def _has_access(interaction: discord.Interaction) -> bool:
    return (
        interaction.user.guild_permissions.administrator
        or interaction.user.id in config.DEV_USER_IDS
        or interaction.guild.get_role(GUILD_ROLE_ID) in interaction.user.roles
    )


async def _get_daily_breakdown(year_month: str) -> dict[int, tuple[int, int]]:
    """Returns {day: (actual_tax, would_lose)} for the given YYYY-MM."""
    conn = await db._get_db()
    daily: dict[int, tuple[int, int]] = {}
    query = """
        SELECT ts, actual_tax, resolved_preflop_fold
        FROM tax_experiment_log
        WHERE ts LIKE ?
    """
    async with conn.execute(query, (f"{year_month}%",)) as c:
        rows = await c.fetchall()
    for ts_str, actual_tax, preflop_fold in rows:
        try:
            day = int(ts_str.split("T")[0].split("-")[2])
        except Exception:
            continue
        actual_sum, lost_sum = daily.get(day, (0, 0))
        actual_sum += actual_tax
        if preflop_fold:
            lost_sum += actual_tax
        daily[day] = (actual_sum, lost_sum)
    return daily


async def _get_split_totals(year_month: str) -> dict:
    """Totals + a heads-up (2p) vs multiway (3+) breakdown of the 'would lose' amount."""
    conn = await db._get_db()
    query = """
        SELECT player_count, resolved_preflop_fold, SUM(actual_tax)
        FROM tax_experiment_log
        WHERE ts LIKE ?
        GROUP BY player_count, resolved_preflop_fold
    """
    totals = {"actual": 0, "lost_hu": 0, "lost_multi": 0}
    async with conn.execute(query, (f"{year_month}%",)) as c:
        rows = await c.fetchall()
    for player_count, preflop_fold, tax_sum in rows:
        tax_sum = tax_sum or 0
        totals["actual"] += tax_sum
        if preflop_fold:
            if player_count == 2:
                totals["lost_hu"] += tax_sum
            else:
                totals["lost_multi"] += tax_sum
    return totals


def _format_amt(val: int) -> str:
    if val == 0:
        return "0"
    if val >= 1_000_000:
        v = val / 1_000_000
        return f"{int(v)}M" if v == int(v) else f"{v:.1f}M"
    if val >= 100_000:
        v = val / 1_000
        return f"{int(v)}K" if v == int(v) else f"{v:.1f}K"
    return str(val)


def _render_calendar(daily: dict[int, tuple[int, int]], y_val: int, m_val: int) -> str:
    """Same visual language as /pokeradmin salt: dark cards, rounded corners,
    cyan date, green/amber values. Cards are taller to fit two numbers."""
    cal = calendar.Calendar(firstweekday=6)
    weeks = cal.monthdayscalendar(y_val, m_val)
    month_name = calendar.month_name[m_val]

    bg_color = (19, 19, 26)
    card_bg = (33, 33, 47)
    empty_card_bg = (24, 24, 33)
    header_color = (255, 255, 255)
    text_muted = (130, 130, 160)
    cyan_color = (56, 189, 248)
    green_color = (74, 222, 128)
    amber_color = (250, 176, 5)
    grey_color = (110, 120, 140)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    font_path_bold = os.path.join(base_dir, "assets", "Roboto-Bold.ttf")
    font_path_medium = os.path.join(base_dir, "assets", "Roboto-Medium.ttf")
    try:
        font_title = ImageFont.truetype(font_path_bold, 30)
        font_header = ImageFont.truetype(font_path_bold, 20)
        font_date = ImageFont.truetype(font_path_medium, 16)
        font_val = ImageFont.truetype(font_path_bold, 19)
        font_sub = ImageFont.truetype(font_path_medium, 14)
        font_total = ImageFont.truetype(font_path_bold, 22)
    except Exception:
        font_title = font_header = font_date = font_val = font_sub = font_total = ImageFont.load_default()

    num_weeks = len(weeks)
    padding = 20
    card_w = 115
    card_h = 112
    gap = 10

    header_h = 80
    weekdays_h = 40
    grid_h = card_h * num_weeks + gap * (num_weeks - 1)
    footer_h = 90

    img_w = padding * 2 + card_w * 7 + gap * 6
    img_h = padding + header_h + weekdays_h + grid_h + footer_h + padding

    img = Image.new("RGB", (img_w, img_h), bg_color)
    draw = ImageDraw.Draw(img)

    draw.text((padding, padding + 10), f"Tax Experiment — {month_name} {y_val}", font=font_title, fill=header_color)
    draw.text((padding, padding + 45), "green = actual tax  ·  amber = would lose (preflop-fold exempt)",
               font=font_sub, fill=text_muted)

    weekdays = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"]
    start_y = padding + header_h
    for idx, day_lbl in enumerate(weekdays):
        x = padding + idx * (card_w + gap)
        bbox = draw.textbbox((0, 0), day_lbl, font=font_header)
        text_w = bbox[2] - bbox[0]
        draw.text((x + (card_w - text_w) // 2, start_y), day_lbl, font=font_header, fill=text_muted)

    start_grid_y = start_y + weekdays_h
    for row_idx, week in enumerate(weeks):
        y = start_grid_y + row_idx * (card_h + gap)
        for col_idx, day in enumerate(week):
            x = padding + col_idx * (card_w + gap)
            if day == 0:
                draw.rounded_rectangle([x, y, x + card_w, y + card_h], radius=6, fill=empty_card_bg)
                continue

            draw.rounded_rectangle([x, y, x + card_w, y + card_h], radius=6, fill=card_bg)
            draw.text((x + 10, y + 8), f"{day:02d}", font=font_date, fill=cyan_color)

            actual_val, lost_val = daily.get(day, (0, 0))

            actual_str = _format_amt(actual_val)
            a_color = green_color if actual_val > 0 else grey_color
            bbox = draw.textbbox((0, 0), actual_str, font=font_val)
            tw = bbox[2] - bbox[0]
            draw.text((x + (card_w - tw) // 2, y + 42), actual_str, font=font_val, fill=a_color)

            lost_str = f"-{_format_amt(lost_val)}" if lost_val > 0 else "—"
            l_color = amber_color if lost_val > 0 else grey_color
            bbox = draw.textbbox((0, 0), lost_str, font=font_sub)
            tw = bbox[2] - bbox[0]
            draw.text((x + (card_w - tw) // 2, y + 78), lost_str, font=font_sub, fill=l_color)

    total_actual = sum(v[0] for v in daily.values())
    total_lost = sum(v[1] for v in daily.values())
    pct = f" ({total_lost / total_actual:.0%})" if total_actual > 0 else ""
    footer_y = start_grid_y + grid_h + 20
    draw.line([padding, footer_y, img_w - padding, footer_y], fill=(40, 40, 60), width=1)
    draw.text((padding, footer_y + 15), f"Total Actual Tax: {total_actual:,}", font=font_total, fill=green_color)
    draw.text((padding, footer_y + 45), f"Would Lose (preflop-exempt): {total_lost:,}{pct}", font=font_total, fill=amber_color)

    path = f"tax_experiment_{y_val}-{m_val:02d}.png"
    img.save(path)
    return path


class TaxExperimentCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="taxtest", description="[Admin] Shadow tax test — no flop no drop, doesn't touch real chips")
    @app_commands.describe(month="YYYY-MM format (e.g. 2026-07) - defaults to current month")
    @app_commands.guilds(discord.Object(id=config.GUILD_ID))
    async def taxtest(self, interaction: discord.Interaction, month: str = None):
        await interaction.response.defer(ephemeral=False)

        if not _has_access(interaction):
            await interaction.followup.send("❌ Server Administrators only.", ephemeral=True)
            return

        import re
        if month:
            if not re.match(r"^\d{4}-\d{2}$", month):
                await interaction.followup.send("❌ Invalid month format. Use YYYY-MM (e.g. 2026-07).")
                return
            year_month = month
        else:
            year_month = datetime.utcnow().strftime("%Y-%m")

        y_val, m_val = (int(x) for x in year_month.split("-"))

        try:
            daily = await _get_daily_breakdown(year_month)
        except Exception as e:
            await interaction.followup.send(f"❌ Query error: {e}")
            return

        totals = await _get_split_totals(year_month)
        hu = totals["lost_hu"]
        multi = totals["lost_multi"]
        split_line = (f"Of that: **{hu:,}** from heads-up hands, **{multi:,}** from 3+ player hands"
                      if (hu or multi) else "No preflop-fold hands logged yet this month.")

        try:
            img_path = _render_calendar(daily, y_val, m_val)
        except Exception as e:
            await interaction.followup.send(f"❌ Render error: {e}")
            return

        file = discord.File(img_path, filename=os.path.basename(img_path))
        await interaction.followup.send(content=split_line, file=file)

        try:
            if os.path.exists(img_path):
                os.remove(img_path)
        except Exception as e:
            print(f"[tax_experiment] cleanup error: {e}")


async def setup(bot: commands.Bot):
    await bot.add_cog(TaxExperimentCog(bot))
