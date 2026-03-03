"""
poker.py — Texas Hold'em bot
All commands under /poker prefix. Tables have names/IDs. Auto-next-hand. Threads for logging.
"""

import discord
from discord import app_commands
from discord.ext import commands
from engine import PokerGame, Street, hand_str
import database as db
from treys import Evaluator
import card_images
import os
import asyncio
import uuid
from datetime import datetime

evaluator  = Evaluator()
USE_IMAGES = card_images.cards_available()

TURN_TIMEOUT        = 300   # 5 min
NEXT_HAND_DELAY_DEFAULT = 30  # seconds between hands (overridden per guild)

# ── State ─────────────────────────────────────────────────────────────────────

class TableState:
    def __init__(self, name: str):
        self.id         = str(uuid.uuid4())[:8]   # short unique ID
        self.name       = name
        self.game       = PokerGame()

        self.hand_msg:  discord.Message | None = None
        self.board_msg: discord.Message | None = None
        self.ping_msg:  discord.Message | None = None
        self.log_thread: discord.Thread | None = None
        self.street_log: list[str] = []
        self.closing    = False    # set when /poker close is used
        self.auto_task: asyncio.Task | None = None
        self.timer_task: asyncio.Task | None = None

# key = (guild_id, channel_id)
tables: dict[tuple, TableState] = {}

def get_table(key: tuple) -> TableState | None:
    return tables.get(key)

def slog(t: TableState, text: str):
    t.street_log.append(text)

def slog_clear(t: TableState):
    t.street_log = []

# ── Permission helper ─────────────────────────────────────────────────────────

async def is_manager(interaction: discord.Interaction) -> bool:
    """Poker Manager = has the configured manager role. No extra server perms needed."""
    settings = await db.get_settings(interaction.guild_id)
    role_id  = settings.get("manager_role_id")
    if interaction.user.id == 1339935869598961728:
        return True
    if role_id:
        role = interaction.guild.get_role(int(role_id))
        if role and role in interaction.user.roles:
            return True
    # Fallback: server admin can always manage
    return interaction.user.guild_permissions.administrator

# ── Turn timer ────────────────────────────────────────────────────────────────

def cancel_timer(t: TableState):
    if t.timer_task and not t.timer_task.done():
        t.timer_task.cancel()
    t.timer_task = None

def start_timer(t: TableState, channel):
    cancel_timer(t)
    cp = t.game.current_player()
    if not cp or t.game.street in (Street.WAITING, Street.SHOWDOWN):
        return
    t.timer_task = asyncio.create_task(_turn_timer(t, channel, cp.user_id))

async def _turn_timer(t: TableState, channel, user_id: int):
    try:
        await asyncio.sleep(TURN_TIMEOUT)
    except asyncio.CancelledError:
        return

    if not t.game.is_turn(user_id):
        return

    p = t.game.get_player(user_id)
    if not p:
        return

    name = p.display_name
    ok, msg = t.game.fold(user_id)
    if ok:
        slog(t, msg)

    chips_back, _ = t.game.remove_player(user_id)
    if chips_back > 0:
        await db.return_chips(user_id, chips_back)
        await db.clear_chips_in_play(user_id)

    try:
        await channel.send(f"⏰ **{name}** timed out — auto-folded and removed. Chips returned to wallet.")
    except Exception:
        pass

    key = (channel.guild.id, channel.id)
    if t.game._hand_result:
        await _process_result(channel.guild, channel, t)

    await refresh(channel, t)

# ── Auto next hand ─────────────────────────────────────────────────────────────

def schedule_next_hand(t: TableState, channel):
    if t.auto_task and not t.auto_task.done():
        t.auto_task.cancel()
    t.auto_task = asyncio.create_task(_auto_next_hand(t, channel))

async def _auto_next_hand(t: TableState, channel):
    # Fetch configured delay
    settings = await db.get_settings(channel.guild.id)
    delay    = settings.get("next_hand_delay", NEXT_HAND_DELAY_DEFAULT)

    # Post countdown notice
    try:
        await channel.send(f"⏳ Next hand starting in **{delay}s**...")
    except Exception:
        pass

    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        return

    if t.closing:
        await _close_table(channel, t)
        return

    # Process any pending leaves first
    for uid in list(t.game.pending_leaves):
        p = t.game.get_player(uid)
        if p:
            await db.return_chips(uid, p.chips)
            await db.clear_chips_in_play(uid)

    # Check if enough players remain (including pending joins)
    active = [p for p in t.game.players if p.chips > 0]
    total  = len(active) + len(t.game.pending_joins)
    if total < 2:
        await channel.send(
            "⚠️ Not enough players for another hand. Table is waiting — a Manager can `/poker start` when ready."
        )
        return

    # Load settings
    settings = await db.get_settings(channel.guild.id)
    t.game.SMALL_BLIND = settings["small_blind"]
    t.game.BIG_BLIND   = settings["big_blind"]

    slog_clear(t)
    success, msg = t.game.start_hand()
    slog(t, msg)

    if not success:
        await channel.send(f"⚠️ Could not start next hand: {msg}")
        return

    await refresh(channel, t, new_hand=True)

async def _close_table(channel, t: TableState):
    """Return all chips and remove table after hand ends."""
    key = (channel.guild.id, channel.id)
    for p in list(t.game.players):
        if p.chips > 0:
            await db.return_chips(p.user_id, p.chips)
            await db.clear_chips_in_play(p.user_id)
    for p in list(t.game.pending_joins):
        await db.return_chips(p.user_id, p.chips)
        await db.clear_chips_in_play(p.user_id)
    tables.pop(key, None)
    await channel.send(f"🚪 **Table '{t.name}'** has been closed. All chips returned to wallets.")

# ── Log to thread ─────────────────────────────────────────────────────────────

# One shared log thread per guild (created once, reused forever)
_log_threads: dict[int, discord.Thread] = {}

async def ensure_log_thread(channel, t: TableState) -> discord.Thread | None:
    """
    Get the single shared poker log thread for this guild.
    Creates it once if it doesn't exist yet, then reuses it forever.
    All hand logs are posted as plain messages inside the same thread.
    """
    settings  = await db.get_settings(channel.guild.id)
    log_ch_id = settings.get("log_channel_id")
    if not log_ch_id:
        return None
    log_ch = channel.guild.get_channel(int(log_ch_id))
    if not log_ch:
        return None

    # Return cached thread if still alive
    existing = _log_threads.get(channel.guild.id)
    if existing:
        try:
            await channel.guild.fetch_channel(existing.id)
            return existing
        except Exception:
            _log_threads.pop(channel.guild.id, None)

    # Check if a "Poker Hand Log" thread already exists in the channel
    if hasattr(log_ch, 'threads'):
        for thread in log_ch.threads:
            if thread.name == "Poker Hand Log":
                _log_threads[channel.guild.id] = thread
                return thread

    # Create it for the first time
    try:
        thread = await log_ch.create_thread(
            name="Poker Hand Log",
            type=discord.ChannelType.public_thread
        )
        _log_threads[channel.guild.id] = thread
        return thread
    except Exception:
        return None

async def post_hand_log(channel, t: TableState, summary: str):
    thread = await ensure_log_thread(channel, t)
    if thread:
        await thread.send(f"**Table: {t.name}** `{t.id}`\n```\n{summary}\n```")

# ── Embed builder ─────────────────────────────────────────────────────────────

STREET_COLOR = {
    Street.WAITING:  0x5865F2,
    Street.PREFLOP:  0x36393F,
    Street.FLOP:     0x1F8B4C,
    Street.TURN:     0xE67E22,
    Street.RIVER:    0xE74C3C,
    Street.SHOWDOWN: 0xF1C40F,
}
STREET_LABEL = {
    Street.WAITING:  "🪑 Waiting for players",
    Street.PREFLOP:  "🃏 Pre-Flop",
    Street.FLOP:     "🌊 Flop",
    Street.TURN:     "↩️ Turn",
    Street.RIVER:    "🏁 River",
    Street.SHOWDOWN: "🏆 Showdown",
}

def player_line(p, game: PokerGame, idx: int) -> str:
    tag = " 🎰" if idx == game.dealer_idx else ""
    mention = f"<@{p.user_id}>"
    if p.folded:
        return f"~~{mention}~~ ~~{p.chips} 🪙~~ — folded{tag}"
    if p.all_in:
        return f"{mention} **{p.chips} 🪙** — ALL-IN 🚀{tag}"
    cp = game.current_player()
    if cp and cp.user_id == p.user_id:
        status = f"acting (bet {p.bet})" if p.bet else "acting"
    elif p.bet > 0:
        status = f"bet {p.bet}"
    else:
        status = "—"
    return f"{mention} **{p.chips} 🪙** — {status}{tag}"

def build_embed(t: TableState) -> discord.Embed:
    game  = t.game
    color = STREET_COLOR.get(game.street, 0x5865F2)
    label = STREET_LABEL.get(game.street, "")
    cp    = game.current_player()

    title = f"🃏 {t.name}  ·  Hand #{game.hand_num}" if game.hand_num else f"🃏 {t.name}"
    embed = discord.Embed(title=title, color=color)
    embed.set_footer(text=f"{label}  ·  Table ID: {t.id}" + ("  ·  Closing after this hand" if t.closing else ""))

    if game.street == Street.WAITING:
        embed.description = "Press **Join** to sit down. Host uses `/poker start` to deal."
    else:
        desc = f"**Pot:** {game.pot} 🪙"
        if game.current_bet:
            desc += f"  ·  **Bet:** {game.current_bet}"
        if cp:
            desc += f"\n⬅️ **{cp.display_name}'s turn**"
        embed.description = desc

    lines = [player_line(p, game, i) for i, p in enumerate(game.players)]
    for p in game.pending_joins:
        lines.append(f"<@{p.user_id}> **{p.chips} 🪙** — ⏳ next hand")
    if lines:
        embed.add_field(name="Players", value="\n".join(lines), inline=False)

    logs = t.street_log
    if logs:
        embed.add_field(name="This round", value="\n".join(logs[-8:]), inline=False)

    return embed

# ── Board image ───────────────────────────────────────────────────────────────

async def update_board(channel, t: TableState):
    game = t.game
    if not USE_IMAGES or game.street in (Street.WAITING, Street.PREFLOP):
        return
    strip   = card_images.make_strip(game.community, backs=5 - len(game.community))
    names   = {Street.FLOP: "Flop", Street.TURN: "Turn",
               Street.RIVER: "River", Street.SHOWDOWN: "Showdown"}
    caption = f"Board — {names.get(game.street, '')}  |  Pot: {game.pot} 🪙"
    if t.board_msg:
        try: await t.board_msg.delete()
        except discord.NotFound: pass
    t.board_msg = await channel.send(caption, file=strip)

# ── Turn ping ─────────────────────────────────────────────────────────────────

async def send_turn_ping(channel, t: TableState):
    if t.ping_msg:
        try: await t.ping_msg.delete()
        except discord.NotFound: pass
        t.ping_msg = None
    cp = t.game.current_player()
    if not cp or t.game.street in (Street.WAITING, Street.SHOWDOWN):
        return
    call_amt = t.game.call_amount(cp)
    hint = f"call **{call_amt}**, raise, or fold" if call_amt else "check or raise"
    t.ping_msg = await channel.send(f"<@{cp.user_id}> your turn — {hint}")

# ── Master refresh ────────────────────────────────────────────────────────────

async def refresh(channel, t: TableState, new_hand: bool = False):
    embed = build_embed(t)
    view  = GameView(t)

    if new_hand or not t.hand_msg:
        t.hand_msg = await channel.send(embed=embed, view=view)
    else:
        try:
            await t.hand_msg.edit(embed=embed, view=view)
        except (discord.NotFound, discord.HTTPException):
            t.hand_msg = await channel.send(embed=embed, view=view)

    await update_board(channel, t)
    await send_turn_ping(channel, t)
    start_timer(t, channel)

# ── Post-hand processing ──────────────────────────────────────────────────────

async def _process_result(guild, channel, t: TableState):
    result = t.game._hand_result
    if not result:
        return
    cancel_timer(t)

    for p in t.game.players:
        net = result.chip_deltas.get(p.user_id, 0)
        won = any(w.user_id == p.user_id for w in result.winners)
        await db.record_hand(p.user_id, p.display_name, won, net)

    # Update in-play amounts
    for p in t.game.players:
        if p.chips > 0:
            await db.update_chips_in_play(p.user_id, p.chips)
        else:
            await db.clear_chips_in_play(p.user_id)
    for uid in list(t.game.pending_leaves):
        p = t.game.get_player(uid)
        if p:
            await db.return_chips(uid, p.chips)
            await db.clear_chips_in_play(uid)

    # Log hand
    await db.log_hand(guild.id, t.id, t.name, t.game.hand_num, result.summary)
    await post_hand_log(channel, t, result.summary)

    # Schedule next hand
    schedule_next_hand(t, channel)

# ── Raise modal ───────────────────────────────────────────────────────────────

class RaiseModal(discord.ui.Modal, title="Raise"):
    amount = discord.ui.TextInput(label="Raise by how many chips?", placeholder="e.g. 200", min_length=1, max_length=7)

    def __init__(self, t: TableState, channel, guild):
        super().__init__()
        self.t = t; self.channel = channel; self.guild = guild

    async def on_submit(self, interaction: discord.Interaction):
        try:
            amount = int(self.amount.value)
        except ValueError:
            await interaction.response.send_message("❌ Enter a valid number.", ephemeral=True); return
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True); return
        success, msg = self.t.game.raise_bet(interaction.user.id, amount)
        if not success:
            await interaction.response.send_message(msg, ephemeral=True); return
        if any(m in msg for m in ["🌊", "↩️", "🏁", "🃏 **Showdown"]):
            slog_clear(self.t)
        slog(self.t, msg)
        await interaction.response.defer(ephemeral=True)
        if self.t.game._hand_result:
            await _process_result(self.guild, self.channel, self.t)
        await refresh(self.channel, self.t)

# ── Join modal ────────────────────────────────────────────────────────────────

class JoinModal(discord.ui.Modal, title="Buy In"):
    amount = discord.ui.TextInput(label="How many chips to bring to table?", placeholder="e.g. 500", min_length=1, max_length=8)

    def __init__(self, t: TableState, bal: int, min_w: int):
        super().__init__()
        self.t     = t
        self.bal   = bal
        self.min_w = min_w
        self.amount.placeholder = f"min {min_w} — max {bal}  (wallet: {bal})"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            chips = int(self.amount.value)
        except ValueError:
            await interaction.response.send_message("❌ Enter a valid number.", ephemeral=True); return

        if chips < self.min_w:
            await interaction.response.send_message(f"❌ Minimum buy-in is **{self.min_w}** chips.", ephemeral=True); return
        if chips > self.bal:
            await interaction.response.send_message(f"❌ You only have **{self.bal}** chips in your wallet.", ephemeral=True); return

        t = self.t
        ok = await db.deduct_chips(interaction.user.id, chips)
        if not ok:
            await interaction.response.send_message("❌ Failed to deduct chips.", ephemeral=True); return
        await db.mark_chips_in_play(interaction.user.id, interaction.user.display_name, chips)

        msg = t.game.add_player(interaction.user.id, interaction.user.display_name, chips)
        slog(t, msg)
        await interaction.response.defer(ephemeral=True)

        key = (interaction.guild_id, interaction.channel_id)
        await refresh(interaction.channel, t)

# ── Game View ─────────────────────────────────────────────────────────────────

class GameView(discord.ui.View):
    def __init__(self, t: TableState):
        super().__init__(timeout=None)
        self.t = t
        game   = t.game
        in_hand = game.street not in (Street.WAITING, Street.SHOWDOWN)
        self.btn_join.disabled  = in_hand
        for b in [self.btn_call, self.btn_check, self.btn_raise, self.btn_fold]:
            b.disabled = not in_hand

    async def _do_action(self, interaction: discord.Interaction, fn, *args):
        ok, msg = fn(*args)
        if not ok:
            await interaction.response.send_message(msg, ephemeral=True); return
        if any(m in msg for m in ["🌊", "↩️", "🏁", "🃏 **Showdown"]):
            slog_clear(self.t)
        slog(self.t, msg)
        await interaction.response.defer(ephemeral=True)
        if self.t.game._hand_result:
            await _process_result(interaction.guild, interaction.channel, self.t)
        await refresh(interaction.channel, self.t)

    @discord.ui.button(label="Join",  style=discord.ButtonStyle.green, row=0)
    async def btn_join(self, interaction: discord.Interaction, button: discord.ui.Button):
        await db.upsert_wallet_name(interaction.user.id, interaction.user.display_name)
        settings  = await db.get_settings(interaction.guild_id)
        min_w     = settings.get("min_wallet", 50)
        bal       = await db.get_balance(interaction.user.id)
        if bal < min_w:
            await interaction.response.send_message(
                f"❌ Need at least **{min_w}** chips in your wallet to join. Your wallet: **{bal}**.", ephemeral=True); return
        await interaction.response.send_modal(JoinModal(self.t, bal, min_w))

    @discord.ui.button(label="Leave", style=discord.ButtonStyle.red, row=0)
    async def btn_leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        chips_back, msg = self.t.game.remove_player(interaction.user.id)
        if chips_back > 0:
            await db.return_chips(interaction.user.id, chips_back)
            await db.clear_chips_in_play(interaction.user.id)
        slog(self.t, msg)
        await interaction.response.defer(ephemeral=True)
        await refresh(interaction.channel, self.t)

    @discord.ui.button(label="Call",  style=discord.ButtonStyle.green,  row=1)
    async def btn_call(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True); return
        await self._do_action(interaction, self.t.game.check_or_call, interaction.user.id)

    @discord.ui.button(label="Check", style=discord.ButtonStyle.blurple, row=1)
    async def btn_check(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True); return
        p = self.t.game.get_player(interaction.user.id)
        if p and self.t.game.call_amount(p) > 0:
            await interaction.response.send_message(
                f"❌ There's **{self.t.game.call_amount(p)}** to call. Use Call or Fold.", ephemeral=True); return
        await self._do_action(interaction, self.t.game.check_or_call, interaction.user.id)

    @discord.ui.button(label="Raise", style=discord.ButtonStyle.green, row=1)
    async def btn_raise(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True); return
        await interaction.response.send_modal(RaiseModal(self.t, interaction.channel, interaction.guild))

    @discord.ui.button(label="Fold",  style=discord.ButtonStyle.red,    row=1)
    async def btn_fold(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True); return
        await self._do_action(interaction, self.t.game.fold, interaction.user.id)

    @discord.ui.button(label="My Cards",    style=discord.ButtonStyle.grey, row=2)
    async def btn_hole(self, interaction: discord.Interaction, button: discord.ui.Button):
        p = self.t.game.get_player(interaction.user.id)
        if not p or not p.hole_cards:
            await interaction.response.send_message("❌ No cards right now.", ephemeral=True); return
        strength = ""
        if self.t.game.community and not p.folded:
            score    = evaluator.evaluate(p.hole_cards, self.t.game.community)
            rank     = evaluator.class_to_string(evaluator.get_rank_class(score))
            pct      = round((1 - score / 7462) * 100, 1)
            strength = f"\n**Hand:** {rank} (top {100-pct:.0f}%)"
        caption = f"Your hole cards — {p.chips} 🪙 at table{strength}"
        if USE_IMAGES:
            await interaction.response.send_message(caption, file=card_images.make_strip(p.hole_cards), ephemeral=True)
        else:
            await interaction.response.send_message(f"{caption}\n{hand_str(p.hole_cards)}", ephemeral=True)

    @discord.ui.button(label="Rankings",    style=discord.ButtonStyle.grey, row=2)
    async def btn_rankings(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "**Hand Rankings** *(best → worst)*\n"
            "```\n"
            "1.  Royal Flush       A K Q J 10 — same suit\n"
            "2.  Straight Flush    5 in a row — same suit\n"
            "3.  Four of a Kind    4 of same rank\n"
            "4.  Full House        3-of-a-kind + pair\n"
            "5.  Flush             Any 5 of same suit\n"
            "6.  Straight          5 in a row — any suits\n"
            "7.  Three of a Kind   3 of same rank\n"
            "8.  Two Pair          Two different pairs\n"
            "9.  One Pair          Two of same rank\n"
            "10. High Card         None of the above\n"
            "```", ephemeral=True)

    @discord.ui.button(label="Wallet",      style=discord.ButtonStyle.grey, row=2)
    async def btn_wallet(self, interaction: discord.Interaction, button: discord.ui.Button):
        bal = await db.get_balance(interaction.user.id)
        p   = self.t.game.get_player(interaction.user.id)
        table_str = f"\n**At table:** {p.chips} 🪙" if p else ""
        await interaction.response.send_message(f"**Wallet:** {bal} 🪙{table_str}", ephemeral=True)

# ── Cog ───────────────────────────────────────────────────────────────────────

class PokerCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    poker = app_commands.Group(name="poker", description="Texas Hold'em poker")

    # ── Table commands ────────────────────────────────────────────────────

    @poker.command(name="open", description="Open a new poker table in this channel")
    @app_commands.describe(name="Table name (e.g. 'High Stakes')")
    async def open_table(self, interaction: discord.Interaction, name: str = "Poker Table"):
        key = (interaction.guild_id, interaction.channel_id)
        if key in tables:
            await interaction.response.send_message("❌ There's already a table in this channel. Use `/poker close` first.", ephemeral=True); return
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Only Poker Managers can open a table.", ephemeral=True); return

        t = TableState(name)
        tables[key] = t

        settings = await db.get_settings(interaction.guild_id)
        t.game.SMALL_BLIND = settings["small_blind"]
        t.game.BIG_BLIND   = settings["big_blind"]

        await interaction.response.defer(ephemeral=True)
        await refresh(interaction.channel, t, new_hand=True)

    @poker.command(name="close", description="[Manager] Close table after current hand finishes")
    async def close_table(self, interaction: discord.Interaction):
        key = (interaction.guild_id, interaction.channel_id)
        t   = get_table(key)
        if not t:
            await interaction.response.send_message("❌ No table in this channel.", ephemeral=True); return
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Only Poker Managers can close a table.", ephemeral=True); return

        if t.game.street == Street.WAITING:
            # No hand in progress — close immediately
            await _close_table(interaction.channel, t)
            await interaction.response.send_message("✅ Table closed.", ephemeral=True)
        else:
            t.closing = True
            await interaction.response.send_message(
                "✅ Table will close after the current hand completes.", ephemeral=False)
            await refresh(interaction.channel, t)  # update embed to show "Closing"

    @poker.command(name="start", description="[Manager] Deal the first hand")
    async def start(self, interaction: discord.Interaction):
        key = (interaction.guild_id, interaction.channel_id)
        t   = get_table(key)
        if not t:
            await interaction.response.send_message("❌ No table here. Use `/poker open` first.", ephemeral=True); return
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        if t.game.street != Street.WAITING:
            await interaction.response.send_message("❌ A hand is already in progress.", ephemeral=True); return

        settings = await db.get_settings(interaction.guild_id)
        t.game.SMALL_BLIND = settings["small_blind"]
        t.game.BIG_BLIND   = settings["big_blind"]

        for uid in list(t.game.pending_leaves):
            p = t.game.get_player(uid)
            if p:
                await db.return_chips(uid, p.chips)
                await db.clear_chips_in_play(uid)

        slog_clear(t)
        success, msg = t.game.start_hand()
        slog(t, msg)

        if not success:
            await interaction.response.send_message(msg, ephemeral=True); return

        await interaction.response.defer(ephemeral=True)
        await refresh(interaction.channel, t, new_hand=True)

    @poker.command(name="table", description="Re-post the game panel")
    async def table_cmd(self, interaction: discord.Interaction):
        key = (interaction.guild_id, interaction.channel_id)
        t   = get_table(key)
        if not t:
            await interaction.response.send_message("❌ No table in this channel.", ephemeral=True); return
        t.board_msg = None; t.ping_msg = None
        await interaction.response.defer(ephemeral=True)
        await refresh(interaction.channel, t, new_hand=True)

    # ── Player commands ───────────────────────────────────────────────────

    @poker.command(name="wallet", description="Check your chip wallet balance")
    async def wallet(self, interaction: discord.Interaction):
        bal = await db.get_balance(interaction.user.id)
        key = (interaction.guild_id, interaction.channel_id)
        t   = get_table(key)
        p   = t.game.get_player(interaction.user.id) if t else None
        table_str = f"\n**At table:** {p.chips} 🪙" if p else ""
        await interaction.response.send_message(f"**Wallet:** {bal} 🪙{table_str}", ephemeral=True)

    # ── Manager commands ──────────────────────────────────────────────────

    @poker.command(name="addchips", description="[Manager] Add chips to a player's wallet")
    @app_commands.describe(user="Player", amount="Chips to add (negative to remove)", note="Optional reason")
    async def addchips(self, interaction: discord.Interaction, user: discord.Member, amount: int, note: str = ""):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        new_bal = await db.add_chips(
            interaction.user.id, interaction.user.display_name,
            user.id, user.display_name, amount, note)
        sign = "+" if amount >= 0 else ""
        await interaction.response.send_message(
            f"✅ **{sign}{amount}** chips → **{user.display_name}**  |  Balance: **{new_bal}** 🪙"
            + (f"\n> {note}" if note else ""))

    @poker.command(name="settings", description="[Manager] View table settings")
    async def settings_view(self, interaction: discord.Interaction):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        s = await db.get_settings(interaction.guild_id)
        role_str = f"<@&{s['manager_role_id']}>" if s.get("manager_role_id") else "*(not set)*"
        log_str  = f"<#{s['log_channel_id']}>"   if s.get("log_channel_id")  else "*(not set)*"
        embed = discord.Embed(title="⚙️ Poker Settings", color=0x5865F2)
        embed.add_field(name="Small Blind",    value=str(s["small_blind"]),  inline=True)
        embed.add_field(name="Big Blind",      value=str(s["big_blind"]),    inline=True)
        embed.add_field(name="Min Wallet",     value=str(s["min_wallet"]),   inline=True)
        embed.add_field(name="Next Hand Delay", value=f"{s.get('next_hand_delay', NEXT_HAND_DELAY_DEFAULT)}s", inline=True)
        embed.add_field(name="Manager Role",   value=role_str,               inline=False)
        embed.add_field(name="Log Channel",    value=log_str,                inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @poker.command(name="setblinds", description="[Manager] Set small and big blind amounts")
    @app_commands.describe(small="Small blind", big="Big blind")
    async def set_blinds(self, interaction: discord.Interaction, small: int, big: int):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        if small <= 0 or big <= small:
            await interaction.response.send_message("❌ Big blind must be greater than small blind.", ephemeral=True); return
        await db.set_settings(interaction.guild_id, small_blind=small, big_blind=big)
        await interaction.response.send_message(f"✅ Blinds: **{small}** / **{big}**")

    @poker.command(name="setminwallet", description="[Manager] Set minimum wallet to join")
    @app_commands.describe(amount="Minimum chips required")
    async def set_min_wallet(self, interaction: discord.Interaction, amount: int):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        if amount < 0:
            await interaction.response.send_message("❌ Must be 0 or more.", ephemeral=True); return
        await db.set_settings(interaction.guild_id, min_wallet=amount)
        await interaction.response.send_message(f"✅ Min wallet: **{amount}** chips")

    @poker.command(name="setnexthanddelay", description="[Manager] Set the delay between hands (seconds)")
    @app_commands.describe(seconds="Seconds to wait between hands (5–300)")
    async def set_next_hand_delay(self, interaction: discord.Interaction, seconds: int):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        if seconds < 5 or seconds > 300:
            await interaction.response.send_message("❌ Must be between 5 and 300 seconds.", ephemeral=True); return
        await db.set_settings(interaction.guild_id, next_hand_delay=seconds)
        await interaction.response.send_message(f"✅ Next hand delay set to **{seconds}s**.")

    @poker.command(name="setlogchannel", description="[Manager] Set channel where hand logs are posted (as threads)")
    @app_commands.describe(channel="The channel to post log threads in")
    async def set_log_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        await db.set_settings(interaction.guild_id, log_channel_id=channel.id)
        await interaction.response.send_message(f"✅ Log channel set to {channel.mention}. Each table gets its own thread.")

    @poker.command(name="setmanagerrole", description="[Admin] Set the Poker Manager role")
    @app_commands.describe(role="Role that gets poker manager access")
    async def set_manager_role(self, interaction: discord.Interaction, role: discord.Role):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Server Administrator only.", ephemeral=True); return
        await db.set_settings(interaction.guild_id, manager_role_id=role.id)
        await interaction.response.send_message(f"✅ Poker Manager role set to **{role.name}**.")

    @poker.command(name="resetdb", description="[Admin] Wipe all poker data from the database")
    async def reset_db(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Server Administrator only.", ephemeral=True); return
        # Confirmation step
        view = ConfirmResetView(interaction.user.id)
        await interaction.response.send_message(
            "⚠️ **This will permanently delete all wallets, stats, logs and settings.**\nAre you sure?",
            view=view, ephemeral=True)

    # ── Stats / leaderboard ───────────────────────────────────────────────

    @poker.command(name="leaderboard", description="Top poker players by net chips")
    async def leaderboard(self, interaction: discord.Interaction):
        rows = await db.get_leaderboard(10)
        if not rows:
            await interaction.response.send_message("No stats yet!", ephemeral=True); return
        lines = ["**Poker Leaderboard**", "```"]
        lines.append(f"{'#':<3} {'Player':<18} {'Hands':>6} {'Win%':>5} {'Net':>8} {'Wallet':>8}")
        lines.append("─" * 54)
        for i, r in enumerate(rows):
            wp  = f"{r['hands_won']/r['hands_played']*100:.0f}%" if r['hands_played'] else "—"
            net = r['net_chips']; sign = "+" if net >= 0 else ""
            lines.append(f"{str(i+1)+'.':<3} {r['username']:<18} {r['hands_played']:>6} {wp:>5} {sign+str(net):>8} {r['wallet']:>8}")
        lines.append("```")
        await interaction.response.send_message("\n".join(lines))

    @poker.command(name="stats", description="View your poker stats")
    async def stats(self, interaction: discord.Interaction):
        row = await db.get_player_stats(interaction.user.id)
        if not row:
            await interaction.response.send_message("No stats yet!", ephemeral=True); return
        net   = row['net_chips']
        embed = discord.Embed(title=f"Stats — {row['username']}", color=0x2ecc71 if net >= 0 else 0xe74c3c)
        wp    = f"{row['hands_won']/row['hands_played']*100:.1f}%" if row['hands_played'] else "—"
        embed.add_field(name="Hands",  value=str(row['hands_played']), inline=True)
        embed.add_field(name="Won",    value=str(row['hands_won']),    inline=True)
        embed.add_field(name="Win %",  value=wp,                        inline=True)
        embed.add_field(name="Net",    value=f"{'+'if net>=0 else ''}{net} 🪙", inline=True)
        embed.add_field(name="Wallet", value=f"{row['wallet']} 🪙",    inline=True)
        await interaction.response.send_message(embed=embed)

# ── Confirm reset view ────────────────────────────────────────────────────────

class ConfirmResetView(discord.ui.View):
    def __init__(self, admin_id: int):
        super().__init__(timeout=30)
        self.admin_id = admin_id

    @discord.ui.button(label="Yes, wipe everything", style=discord.ButtonStyle.red)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.admin_id:
            await interaction.response.send_message("❌ Not your button.", ephemeral=True); return
        await db.reset_database(interaction.user.id, interaction.user.display_name)
        tables.clear()
        await interaction.response.edit_message(
            content=f"✅ Database reset by **{interaction.user.display_name}**. All data wiped.", view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Cancelled.", view=None)

async def setup(bot):
    await bot.add_cog(PokerCog(bot))