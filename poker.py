"""poker.py — Texas Hold'em bot"""

import discord
from discord import app_commands
from discord.ext import commands, tasks
from engine import PokerGame, Street, hand_str
import database as db
from treys import Evaluator, Card
import card_images
import os, asyncio, uuid, zipfile
from datetime import datetime, timedelta, time as dt_time, timezone as _tz
import time
import math

evaluator  = Evaluator()
USE_IMAGES = card_images.cards_available()

TURN_TIMEOUT_DEFAULT    = 300
NEXT_HAND_DELAY_DEFAULT = 30
TABLE_RESEND_MSGS       = 10

# ── TableState ────────────────────────────────────────────────────────────────

def parse_chips(value: str) -> int | None:
    """Parse chip amounts like 500, 2k, 1.5k, 2e3, 2000."""
    try:
        v = value.strip().lower().replace(",", "")
        if v.endswith("k"):
            return int(float(v[:-1]) * 1000)
        return int(float(v))
    except (ValueError, TypeError):
        return None

class TableState:
    def __init__(self, name: str, manager_id: int):
        self.id           = str(uuid.uuid4())[:8]
        self.name         = name
        self.manager_id   = manager_id
        self.game         = PokerGame()
        self.cosmetics_cache: dict = {}
        self.active_view: discord.ui.View | None = None
        self.hand_msg:    discord.Message | None = None
        self.board_file: discord.File | None = None  # card strip to attach on next embed edit
        self.ping_msg:    discord.Message | None = None
        self.between_msg: discord.Message | None = None
        self.street_log:  list[str] = []
        self.closing      = False
        self.auto_task:   asyncio.Task | None = None
        self.timer_task: asyncio.Task | None = None
        self.timer_user_id: int | None = None
        self.ping_user_id: int | None = None
        self.msg_count = 0
        self.resend_threshold = TABLE_RESEND_MSGS
        self.session_allin_winners: set[int] = set()

tables: dict[tuple, TableState] = {}

def get_table(key: tuple) -> TableState | None:
    return tables.get(key)

def slog(t: TableState, text: str):
    t.street_log.append(text)

def slog_clear(t: TableState):
    t.street_log = []

# ── Permissions ───────────────────────────────────────────────────────────────

async def is_manager(interaction: discord.Interaction) -> bool:
    settings = await db.get_settings(interaction.guild_id)
    role_id  = settings.get("manager_role_id")
    if role_id:
        role = interaction.guild.get_role(int(role_id))
        if role and role in interaction.user.roles:
            return True
    return interaction.user.guild_permissions.administrator

# ── Turn timer ────────────────────────────────────────────────────────────────

def cancel_timer(t: TableState):
    if t.timer_task and not t.timer_task.done():
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        # Prevent the task from committing suicide
        if t.timer_task != current:
            t.timer_task.cancel()
    t.timer_task = None
    t.timer_user_id = None

def start_timer(t: TableState, channel):
    cp = t.game.current_player()
    if not cp or t.game.street in (Street.WAITING, Street.SHOWDOWN):
        cancel_timer(t)
        return
    # Same player's timer is already running — leave it completely alone
    if (t.timer_task and not t.timer_task.done()
            and t.timer_user_id == cp.user_id):
        return
    cancel_timer(t)
    t.timer_user_id = cp.user_id
    t.timer_task = asyncio.create_task(_turn_timer(t, channel, cp.user_id))

async def _turn_timer(t: TableState, channel, user_id: int):
    settings   = await db.get_settings(channel.guild.id)
    timeout    = settings.get("turn_timeout", TURN_TIMEOUT_DEFAULT)
    deadline   = int(time.time()) + timeout
    warn_after = timeout - max(timeout // 5, 15)  # warn with ~1/5 remaining, floor of 15s notice

    # ── Warning phase ──────────────────────────────────────────────────────
    try:
        await asyncio.sleep(warn_after)
    except asyncio.CancelledError:
        return

    if not t.game.is_turn(user_id):
        return
    p = t.game.get_player(user_id)
    if not p:
        return

    warn_msg = await channel.send(
        f"⚠️ <@{user_id}> — act now! You'll be auto-folded <t:{deadline}:R>."
    )

    # ── Fold phase ─────────────────────────────────────────────────────────
    try:
        await asyncio.sleep(timeout - warn_after)
    except asyncio.CancelledError:
        try:
            await warn_msg.delete()
        except Exception:
            pass
        return

    try:
        await warn_msg.delete()
    except Exception:
        pass

    if not t.game.is_turn(user_id):
        return
    p = t.game.get_player(user_id)
    if not p:
        return

    name = p.display_name
    if user_id not in t.game.kicked_users:
        t.game.kicked_users.append(user_id)
    if user_id not in t.game.pending_leaves:
        t.game.pending_leaves.append(user_id)
    if not p.folded:
        ok, fold_msg = t.game.force_fold(user_id)
        if ok:
            parts = fold_msg.split("\n")
            if any(m in fold_msg for m in ["🌊", "↩️", "🏁", "Showdown"]):
                slog_clear(t)
            for part in parts:
                if part.strip():
                    slog(t, part)
    await channel.send(f"⏰ **{name}** timed out and was auto-folded. They will be removed after this hand.")
    if t.game._hand_result:
        await _process_result(channel.guild, channel, t)
    else:
        await refresh(channel, t)

# ── Auto next hand ────────────────────────────────────────────────────────────

def schedule_next_hand(t: TableState, channel):
    if t.auto_task and not t.auto_task.done():
        t.auto_task.cancel()
    t.auto_task = asyncio.create_task(_auto_next_hand(t, channel))

async def _auto_next_hand(t: TableState, channel):
    settings = await db.get_settings(channel.guild.id)
    delay    = settings.get("next_hand_delay", NEXT_HAND_DELAY_DEFAULT)
    view = None
    try:
        view = BetweenHandsView(t)
        t.between_msg = await channel.send(f"⏳ Next hand starting in **{delay}s**...", view=view)
    except Exception:
        pass

    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        if view: view.stop()
        return

    if view: view.stop()

    if t.between_msg:
        try:
            await t.between_msg.delete()
        except Exception:
            pass
        t.between_msg = None

    if t.closing:
        await _close_table(channel, t)
        return

    # pending_leaves chips were already returned in _process_result.
    # Don't return again — just let start_hand->_process_pending remove them from game.players.

    # Auto-remove players below big blind
    bb = t.game.BIG_BLIND
    for p in list(t.game.players):
        if (p.chips + p.pending_rebuy) < bb and p.user_id not in t.game.pending_leaves:
            # FIX: Include pending_rebuy and remove them immediately
            total_to_return = p.chips + p.pending_rebuy
            if total_to_return > 0:
                await db.return_chips(p.user_id, total_to_return)
            await db.clear_chips_in_play(p.user_id)

            t.game.players.remove(p)  # <-- Remove them right now

            try:
                await channel.send(
                    f"🚪 **{p.display_name}** has been removed — stack (**{p.chips}** <:poker_chip:1488128491881758760>) is below the big blind (**{bb}** <:poker_chip:1488128491881758760>). Chips returned to wallet.")
            except Exception:
                pass

    active = [p for p in t.game.players if (p.chips + p.pending_rebuy) >= bb and p.user_id not in t.game.pending_leaves]
    pending_with_chips = [p for p in t.game.pending_joins if (p.chips + p.pending_rebuy) >= bb]
    total = len(active) + len(pending_with_chips)

    if total < 2:
        await refresh(channel, t, cosmetics_cache=None)
        await channel.send("⚠️ Not enough players for another hand. Waiting for a Manager to `/poker start`.")
        return

    t.game.SMALL_BLIND = settings["small_blind"]
    t.game.BIG_BLIND   = settings["big_blind"]
    t.resend_threshold = settings.get("resend_after_msgs", TABLE_RESEND_MSGS)

    slog_clear(t)
    success, msg = t.game.start_hand()
    slog(t, msg)

    if not success:
        await channel.send(f"⚠️ Could not start next hand: {msg}")
        return

    t.msg_count = 0
    await refresh(channel, t, new_hand=True, cosmetics_cache=t.cosmetics_cache)

async def _close_table(channel, t: TableState):
    if getattr(t, 'is_fully_closed', False):
        return
    t.is_fully_closed = True

    t.closing = True
    key = (channel.guild.id, channel.id)
    cancel_timer(t)

    if getattr(t, 'active_view', None):
        t.active_view.stop()

    if t.auto_task and not t.auto_task.done():
        t.auto_task.cancel()
    tables.pop(key, None)

    if t.hand_msg:
        try:
            # Fire-and-forget: Tell Discord to remove the buttons, but DO NOT wait for it to finish
            asyncio.create_task(t.hand_msg.edit(view=None))
        except Exception:
            pass

    # Return chips for seated players not already paid out via pending_leaves
    for p in list(t.game.players):
        if p.user_id not in t.game.pending_leaves:
            total_to_return = p.chips + p.pending_rebuy
            if total_to_return > 0:
                await db.return_chips(p.user_id, total_to_return)
                await db.clear_chips_in_play(p.user_id)
    # Return chips for pending joins
    for p in list(t.game.pending_joins):
        total_to_return = p.chips + p.pending_rebuy
        if total_to_return > 0:
            await db.return_chips(p.user_id, total_to_return)
            await db.clear_chips_in_play(p.user_id)
    await channel.send(f"🚪 **Table '{t.name}'** closed. All chips returned.")


# ── Log thread ────────────────────────────────────────────────────────────────

_log_threads: dict[int, discord.Thread] = {}


async def ensure_log_thread(channel, t: TableState) -> discord.Thread | None:
    settings = await db.get_settings(channel.guild.id)
    log_ch_id = settings.get("log_channel_id")
    if not log_ch_id:
        return None
    log_ch = channel.guild.get_channel(int(log_ch_id))
    if not log_ch:
        return None
    existing = _log_threads.get(channel.guild.id)
    if existing:
        # ZERO LAG: Trust the memory cache, do not ask Discord!
        return existing

    if hasattr(log_ch, 'threads'):
        # 🚨 1. Try Discord's fast memory cache first (O(1) lookup)
        thread = discord.utils.get(log_ch.threads, name="Poker Hand Log")
        if thread:
            _log_threads[channel.guild.id] = thread
            return thread

        # 🚨 2. Only hit the API to search archived threads if it's completely missing
        async for arch_thread in log_ch.archived_threads(limit=10):
            if arch_thread.name == "Poker Hand Log":
                _log_threads[channel.guild.id] = arch_thread
                return arch_thread
    try:
        thread = await log_ch.create_thread(name="Poker Hand Log", type=discord.ChannelType.public_thread)
        _log_threads[channel.guild.id] = thread
        return thread
    except Exception:
        return None


async def post_hand_log(channel, t: TableState, result):
    thread = await ensure_log_thread(channel, t)
    if not thread:
        return
    game = t.game

    header = f"Hand #{game.hand_num} | Table: {t.name} ({t.id}) | Pot: {result.pot}"
    if getattr(result, "tax", 0) > 0:
        header += f" | Tax: {result.tax}"

    lines = [header]

    # Build name lookup from result snapshots — safe for players who already left
    _name_map = {}
    for _p in (result.showdown_players or []):
        _name_map[_p.user_id] = _p.display_name
    for _p in (result.winners or []):
        _name_map[_p.user_id] = _p.display_name
    for _uid in result.chip_deltas:
        if _uid not in _name_map:
            _live = game.get_player(_uid)
            if _live:
                _name_map[_uid] = _live.display_name

    async def uid_str(uid):
        uname = _name_map.get(uid, "Unknown")
        return f"{uname} ({uid})"

    if hasattr(result, 'community') and result.community:
        lines.append(f"Board: {hand_str(result.community)}")

    pot_results = result.pot_results or []
    ranks = result.winner_ranks or {}

    _player_map = {_p.user_id: _p for _p in (result.showdown_players or [])}
    for uid, delta in result.chip_deltas.items():
        sign = "+" if delta > 0 else ""
        ustr = await uid_str(uid)
        rank = ranks.get(uid)
        sp = _player_map.get(uid)
        cards = hand_str(sp.hole_cards) if sp and sp.hole_cards else "folded"
        rank_part = f" [{rank}]" if rank else ""

        # FIX: Explicitly label it as Net Profit/Loss and fix the double math signs
        lines.append(f"  {ustr}: {cards}{rank_part}  Net: {sign}{delta}")

    if pot_results:
        for i, (amt, winners) in enumerate(pot_results):
            label = "Main pot" if i == 0 else f"Side pot {i}"
            wstrs = [await uid_str(w.user_id) for w in winners]
            each = amt // len(winners)
            lines.append(f"  {label} ({amt}): {', '.join(wstrs)}" + (f" ({each} each)" if len(winners) > 1 else ""))
    else:
        for w in result.winners:
            lines.append(f"  Winner (fold): {await uid_str(w.user_id)}")

    body = "\n".join(lines)
    try:
        # FIRE AND FORGET
        await thread.send(f"```\n{body}\n```")
    except Exception:
        # If thread was deleted, quietly clear the cache so it rebuilds next hand
        _log_threads.pop(channel.guild.id, None)


async def post_tip_log(channel, t: TableState, tipper_id: int, tipper_name: str, amount: int, recipient_id: int,
                       recipient_name: str):
    thread = await ensure_log_thread(channel, t)
    if thread:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        try:
            await thread.send(
                f"💸 **Tip** [{ts}] — {amount} \n **{tipper_name}** ({tipper_id}) to **{recipient_name}** ({recipient_id}) at table `{t.name}`")
        except Exception:
            _log_threads.pop(channel.guild.id, None)
# ── Embed ─────────────────────────────────────────────────────────────────────

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

def player_line(p, game: PokerGame, idx: int, title: str | None = None) -> str:
    tag       = " 🎰" if idx == game.dealer_idx else ""
    title_str = f" `{title}`" if title else ""
    mention   = f"<@{p.user_id}>"
    if p.folded:
        return f"~~{mention}{title_str}~~ ~~{p.chips} <:poker_chip:1488128491881758760>~~ — folded{tag}"
    if p.all_in:
        return f"{mention}{title_str} **{p.chips} <:poker_chip:1488128491881758760>** — ALL-IN 🚀{tag}"
    cp = game.current_player()
    if cp and cp.user_id == p.user_id:
        status = f"acting (bet {p.bet})" if p.bet else "acting"
    elif p.bet > 0:
        status = f"bet {p.bet}"
    else:
        status = "—"
    return f"{mention}{title_str} **{p.chips} <:poker_chip:1488128491881758760>** — {status}{tag}"

def build_embed(t: TableState, title_cache: dict[int, str | None] | None = None) -> discord.Embed:
    game  = t.game
    color = STREET_COLOR.get(game.street, 0x5865F2)
    label = STREET_LABEL.get(game.street, "")
    cp    = game.current_player()
    title = f"🃏 {t.name}  ·  Hand #{game.hand_num}" if game.hand_num else f"🃏 {t.name}"
    embed = discord.Embed(title=title, color=color)
    footer = f"{label}  ·  Table ID: {t.id}"
    if t.closing:
        footer += "  ·  Closing after this hand"
    embed.set_footer(text=footer)
    if t.board_file:
        embed.set_image(url="attachment://cards.png")

    if game.street == Street.WAITING:
        embed.description = "Press **Join** to sit down. Manager uses `/poker start` to deal."
    else:
        embed.description = None

    tc = title_cache or {}
    lines = [player_line(p, game, i, tc.get(p.user_id)) for i, p in enumerate(game.players)]
    for p in game.pending_joins:
        lines.append(f"<@{p.user_id}> **{p.chips} <:poker_chip:1488128491881758760>** — ⏳ next hand")
    if lines:
        embed.add_field(name=f"Players ({len(game.players)}/12)", value="\n".join(lines), inline=False)

    if t.street_log:
        embed.add_field(name="This round", value="\n".join(t.street_log[-8:]), inline=False)

    # Pot / turn as last field — sits right above the board image
    if game.street not in (Street.WAITING,):
        pot_line = f"**Pot:** {game.pot} <:poker_chip:1488128491881758760>"
        if game.current_bet:
            pot_line += f"  ·  **Bet:** {game.current_bet}"
        if cp:
            pot_line += f"\n⬅️ **{cp.display_name}'s turn**"
        embed.add_field(name="\u200b", value=pot_line, inline=False)

    return embed

# ── Board image ───────────────────────────────────────────────────────────────

async def update_board(t: TableState):
    """Generate card strip File object — attached directly to the embed message."""
    game = t.game
    if not USE_IMAGES or game.street in (Street.WAITING, Street.PREFLOP) or not game.community:
        t.board_file = None
        return
    backs = max(0, 5 - len(game.community))

    # Push image generation to a background thread!
    t.board_file = await asyncio.to_thread(card_images.make_strip, game.community, backs)
# ── Auto-delete helper ────────────────────────────────────────────────────────

async def _delete_after(message: discord.Message, delay: float):
    """Delete a message after `delay` seconds. Silently ignores errors."""
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass

# ── Turn ping ─────────────────────────────────────────────────────────────────

async def send_turn_ping(channel, t: TableState):
    cp = t.game.current_player()

    # 1. If hand is over or waiting, clean up and bail
    if not cp or t.game.street in (Street.WAITING, Street.SHOWDOWN):
        if t.ping_msg:
            try:
                await t.ping_msg.delete()
            except discord.NotFound:
                pass
            t.ping_msg = None
        t.ping_user_id = None
        return

    # 2. If it is still this exact player's turn, DO NOT delete their ping!
    if t.ping_user_id == cp.user_id:
        return

    # 3. Turn has advanced! Delete the old ping.
    if t.ping_msg:
        try:
            await t.ping_msg.delete()
        except discord.NotFound:
            pass
        t.ping_msg = None

    # 4. Claim the lock and send the new ping
    t.ping_user_id = cp.user_id
    call_amt = t.game.call_amount(cp)
    hint     = f"call **{call_amt}**, raise, or fold" if call_amt else "check or raise"
    t.ping_msg = await channel.send(f"<@{cp.user_id}> your turn — {hint}")

# ── Refresh ───────────────────────────────────────────────────────────────────

async def refresh(channel, t: TableState, new_hand: bool = False, cosmetics_cache: dict = None):
    await update_board(t)
    title_cache: dict[int, str | None] = {}
    try:
        if cosmetics_cache is None:
            cosmetics_cache = await db.get_cosmetics_bulk([p.user_id for p in t.game.players])
        for uid, cosmetics in cosmetics_cache.items():
            tid = cosmetics.get("active_title")
            if tid and tid in db.TITLES:
                title_cache[uid] = db.TITLES[tid]["display"]
    except Exception:
        pass

    embed = build_embed(t, title_cache)   # sets attachment://board.png if file present

    if getattr(t, 'active_view', None):
        t.active_view.stop()
    
    view  = GameView(t)
    t.active_view = view

    f     = t.board_file
    if new_hand or not t.hand_msg:
        if new_hand:
            t.ping_user_id = None  # allow ping to re-send beneath the new embed
        t.hand_msg = await channel.send(embed=embed, view=view, file=f)
    else:
        try:
            # Edit with new attachment — Discord replaces the previous one
            await t.hand_msg.edit(embed=embed, view=view, attachments=([f] if f else []))
        except (discord.NotFound, discord.HTTPException):
            t.hand_msg = await channel.send(embed=embed, view=view, file=f)
    t.board_file = None  # consumed
    await send_turn_ping(channel, t)
    start_timer(t, channel)

# ── Post-hand ─────────────────────────────────────────────────────────────────

def _slog_result(t: TableState, result):
    """Put a clean winner line into street_log so the embed shows correct info."""
    game        = t.game
    ranks       = result.winner_ranks or {}
    pot_results = result.pot_results
    # Use result.community — game.community is already cleared by _end_hand at this point.
    if result.community:
        slog(t, f"🃏 Board: {hand_str(result.community)}")

    if not pot_results or len(pot_results) == 1:
        if len(result.winners) == 1:
            w = result.winners[0]
            gained = result.chip_deltas.get(w.user_id, 0)
            rank = ranks.get(w.user_id)
            rs = f" ({rank})" if rank else ""

            # FIX: Smart sign formatting
            sign = "+" if gained > 0 else ""
            slog(t, f"🏆 **{w.display_name}** won **{sign}{gained}** <:poker_chip:1488128491881758760>{rs}")
        else:
            split = result.pot // max(len(result.winners), 1)
            names = ", ".join(f"**{w.display_name}**" for w in result.winners)
            slog(t, f"🤝 Split: {names} each **+{split}** <:poker_chip:1488128491881758760>")
    else:
        for i, (amt, winners) in enumerate(pot_results):
            label = "Main" if i == 0 else f"Side {i}"
            if len(winners) == 1:
                w      = winners[0]
                rank   = ranks.get(w.user_id)
                rs     = f" ({rank})" if rank else ""
                slog(t, f"🏆 **{label}** ({amt}<:poker_chip:1488128491881758760>) → **{w.display_name}**{rs}")
            else:
                each  = amt // len(winners)
                names = ", ".join(f"**{w.display_name}**" for w in winners)
                slog(t, f"🤝 **{label}** ({amt}<:poker_chip:1488128491881758760>) split → {names} ({each} each)")


async def _announce_winner(channel, t: TableState, result, cosmetics_cache: dict = None):
    game = t.game
    ranks = result.winner_ranks or {}
    pot_results = result.pot_results  # [(amount, [PokerPlayer, ...]), ...]

    _cos_cache = cosmetics_cache or {}

    def _title_str(uid: int) -> str:

        cos = _cos_cache.get(uid, {})
        tid = cos.get("active_title")
        return f" `{db.TITLES[tid]['display']}`" if tid and tid in db.TITLES else ""

    def _win_msg_str(uid: int) -> str:

        cos = _cos_cache.get(uid, {})
        mid = cos.get("active_win_msg")
        return f"{db.WIN_MESSAGES[mid]['display']}" if mid and mid in db.WIN_MESSAGES else ""

    def _build_quotes(winners: list, single: bool = False) -> str:
        quotes = []
        seen_uids = set()
        for w in winners:
            if w.user_id in seen_uids:
                continue
            seen_uids.add(w.user_id)
            wm = _win_msg_str(w.user_id)
            if wm:
                if single:
                    quotes.append(f">  *\"{wm}\"*")
                else:
                    quotes.append(f">  **{w.display_name}:** *\"{wm}\"*")
        return "\n".join(quotes) if quotes else ""

    # 🏆 Create the sleek winner "Receipt" Embed
    embed = discord.Embed(title=f"🏆 Hand #{game.hand_num} Results", color=0xF1C40F)

    # 1. Add Board if it exists (with a forced empty line \u200b below it)
    if result.community:
        embed.add_field(name="🃏 Board", value=f"{hand_str(result.community)}\n\u200b", inline=False)

    if not pot_results or len(pot_results) == 1:
        # ── Single Pot (or Fold Win) ──
        if len(result.winners) == 1:
            w = result.winners[0]
            gained = result.chip_deltas.get(w.user_id, 0)
            rank = ranks.get(w.user_id)
            rs = f" with **{rank}**" if rank else ""
            sign = "+" if gained > 0 else ""
            title_str = _title_str(w.user_id)

            desc = f"**{w.display_name}**{title_str}\nWon **{sign}{gained}** <:poker_chip:1488128491881758760>{rs}"

            quotes = _build_quotes(result.winners, single=True)
            if quotes:
                desc += f"\n\n{quotes}"

            # Add forced empty line after the quotes
            embed.description = desc + "\n\u200b"

            pre_tax_pot = result.pot + getattr(result, "tax", 0)
            embed.add_field(name="Pot", value=f"{pre_tax_pot} <:poker_chip:1488128491881758760>", inline=True)
            embed.add_field(name="New Stack", value=f"{w.chips} <:poker_chip:1488128491881758760>", inline=True)
        else:
            # ── True Split Pot ──
            split = result.pot // len(result.winners)
            desc = "🤝 **Split Pot!**\n\n"
            for w in result.winners:
                rank = ranks.get(w.user_id)
                rs = f" ({rank})" if rank else ""
                title_str = _title_str(w.user_id)
                desc += f"• **{w.display_name}**{title_str} won **+{split}** <:poker_chip:1488128491881758760>{rs}\n"

            quotes = _build_quotes(result.winners)
            if quotes:
                desc += f"\n{quotes}"

            embed.description = desc + "\n\u200b"

            # Add forced empty line after the quotes
            pre_tax_pot = result.pot + getattr(result, "tax", 0)
            embed.add_field(name="Total Pot", value=f"{pre_tax_pot} <:poker_chip:1488128491881758760>", inline=True)
    else:
        # ── Multiple Side Pots ──
        desc = ""
        for i, (amt, winners) in enumerate(pot_results):
            label = "Main Pot" if i == 0 else f"Side Pot {i}"
            icon = "🥇" if i == 0 else "🥈"
            if len(winners) == 1:
                w = winners[0]
                rank = ranks.get(w.user_id)
                rs = f" ({rank})" if rank else ""
                title_str = _title_str(w.user_id)
                gained = result.chip_deltas.get(w.user_id, 0)  # 🚨 EXACT POST-TAX AMOUNT
                desc += f"{icon} **{label}**\n↳ **{w.display_name}**{title_str} won **+{gained}** <:poker_chip:1488128491881758760>{rs}\n\n"
            else:
                desc += f"🤝 **{label}** (Split)\n"
                for w in winners:
                    rank = ranks.get(w.user_id)
                    rs = f" ({rank})" if rank else ""
                    title_str = _title_str(w.user_id)
                    gained = result.chip_deltas.get(w.user_id, 0)  # 🚨 EXACT POST-TAX AMOUNT
                    desc += f"↳ **{w.display_name}**{title_str} won **+{gained}** <:poker_chip:1488128491881758760>{rs}\n"
                desc += "\n"

        quotes = _build_quotes(result.winners)
        if quotes:
            desc += f"{quotes}"

        # Add forced empty line after the quotes
        embed.description = desc.strip() + "\n\u200b"

        stack_lines = []
        seen = set()
        for _, winners in pot_results:
            for w in winners:
                if w.user_id not in seen:
                    seen.add(w.user_id)
                    gained = result.chip_deltas.get(w.user_id, 0)
                    sign = "+" if gained > 0 else ""
                    stack_lines.append(f"**{w.display_name}**: {sign}{gained} → **{w.chips}**")
        if stack_lines:
            embed.add_field(name="💰 Final Stacks", value="\n".join(stack_lines), inline=False)

    # 🚀 Send the final embed instead of raw text
    await channel.send(embed=embed)

async def _process_result(guild, channel, t: TableState):
    result = t.game._hand_result
    if not result:
        return
    cancel_timer(t)
    # Cancel any pending auto-next-hand task — if it fires before we finish
    # processing, it calls start_hand() which clears _hand_result and starts
    # a new hand, causing _process_result to silently bail out.
    if t.auto_task and not t.auto_task.done():
        t.auto_task.cancel()
    t.auto_task = None

    # Stats + achievements — one DB write per player instead of 6-8
    try:
        sp_map = {sp.user_id: sp for sp in (result.showdown_players or [])}

        for p in t.game.players:
            won = any(w.user_id == p.user_id for w in result.winners)
            net = result.chip_deltas.get(p.user_id, 0)
            pot_won = net if won else 0
            sp = sp_map.get(p.user_id)

            # Determine achievement flags in Python — no extra DB reads
            pocket_aces = False
            if won and p.hole_cards:
                pocket_aces = [Card.int_to_str(c)[0] for c in p.hole_cards].count('A') == 2

            all_in_win = bool(won and sp and sp.all_in)

            quads_win = sf_win = rf_win = False

            # FIX: Evaluate the winner's hand even if everyone folded (no showdown)
            if won and sp and sp.hole_cards and result.community and len(result.community) >= 3:
                score = evaluator.evaluate(sp.hole_cards, result.community)
                rank_str = evaluator.class_to_string(evaluator.get_rank_class(score))

                if rank_str == "Four of a Kind":
                    quads_win = True
                elif rank_str == "Straight Flush":
                    sf_win = True
                    rf_win = (score == 1)

            await db.record_hand_full(
                p.user_id, p.display_name, won, net,
                pocket_aces=pocket_aces,
                all_in_win=all_in_win,
                quads_win=quads_win,
                straight_flush_win=sf_win,
                royal_flush_win=rf_win,
            )

            # Check for newly unlocked cosmetics (now 1 read + 1 write internally)
            newly = await db.check_achievements(p.user_id, won=won, pot_won=pot_won)
            if not newly:
                continue

            lines = [f"🎉 <@{p.user_id}> unlocked new cosmetics!"]
            for kind, cid in newly:
                catalog = db.TITLES if kind == "title" else db.WIN_MESSAGES
                item = catalog.get(cid, {})
                display = item.get("display", cid)
                rarity = db.RARITY_LABEL.get(item.get("rarity", "uncommon"), "")
                icon = "🎖️" if kind == "title" else "💬"
                lines.append(f"  {icon} **{display}** *{rarity}*")
            try:
                msg = await channel.send("\n".join(lines))
                asyncio.create_task(_delete_after(msg, 45))
            except Exception as e:
                print(f"[poker] achievement announce error: {e}")

    except Exception as e:
        print(f"[poker] stats/achievement error: {e}")

    try:
        chip_map = {p.user_id: p.chips + p.pending_rebuy for p in t.game.players}
        await db.sync_chips_in_play(chip_map)
    except Exception as e:
        print(f"[poker] chips_in_play error: {e}")

    # 🚨 LOG THE TAX TO REVENUE/JACKPOT
    try:
        if getattr(result, "tax", 0) > 0:
            await db.log_tax(result.tax)
    except Exception as e:
        print(f"[poker] log_tax error: {e}")

    try:
        log_task = asyncio.create_task(post_hand_log(channel, t, result))
        log_task.add_done_callback(lambda task: print(f"[Log Error] {task.exception()}") if task.exception() else None)
        await db.log_hand(guild.id, t.id, t.name, t.game.hand_num, result.summary)
    except Exception as e:
        print(f"[poker] log_hand error: {e}")

    try:
        for uid in list(t.game.pending_leaves):
            p = t.game.get_player(uid)
            if p:
                total_to_return = p.chips + p.pending_rebuy
                if total_to_return > 0:
                    await db.return_chips(uid, total_to_return)
                    await db.clear_chips_in_play(uid)
        # Remove them from game.players now so the post-hand embed is clean.
        # _process_pending in start_hand will find pending_leaves already empty and skip.
        for uid in list(t.game.pending_leaves):
            p = t.game.get_player(uid)
            if p:
                t.game.players.remove(p)
        t.game.pending_leaves.clear()
        t.game.kicked_users.clear()
    except Exception as e:
        print(f"[poker] pending_leaves return error: {e}")

    all_cosmetics = {}
    try:
        all_cosmetics = await db.get_cosmetics_bulk([p.user_id for p in t.game.players])
        t.cosmetics_cache = all_cosmetics
        await _announce_winner(channel, t, result, cosmetics_cache=all_cosmetics)
    except Exception as e:
        print(f"[poker] _announce_winner error: {e}")

    try:
        _slog_result(t, result)
        await refresh(channel, t, cosmetics_cache=all_cosmetics)
    except Exception as e:
        print(f"[poker] refresh error: {e}")

    # 1. ALWAYS run the reveal phase if there was a showdown, even if closing
    if result.showdown_players:
        await _reveal_phase(channel, t, result)

    t.game._hand_result = None

    # 2. THEN check if we need to close the table or schedule the next hand
    if t.closing:
        await _close_table(channel, t)
    else:
        # Verify the table wasn't closed while we were waiting for the Muck buttons
        key = (channel.guild.id, channel.id)
        if get_table(key) is t and not t.closing:
            schedule_next_hand(t, channel)

# ── Showdown reveal (muck / show) ─────────────────────────────────────────────

class ShowdownRevealView(discord.ui.View):
    """Non-winners can show or muck after a showdown. Winners are auto-shown by the engine."""
    def __init__(self, t: TableState, result, pending_user_ids: list[int], timeout: int = TURN_TIMEOUT_DEFAULT):
        super().__init__(timeout=timeout)
        self.t       = t
        self.result  = result
        self.pending = set(pending_user_ids)
        self._done   = asyncio.Event()

    async def _resolve(self, user_id: int):
        self.pending.discard(user_id)
        if not self.pending:
            self._done.set()

    async def on_timeout(self):
        self._done.set()

    @discord.ui.button(label="Show Hand 👁️", style=discord.ButtonStyle.green)
    async def show_hand(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id not in self.pending:
            await interaction.response.send_message("❌ Nothing to show.", ephemeral=True);
            return
        sp = next((p for p in self.result.showdown_players if p.user_id == interaction.user.id), None)
        if not sp or not sp.hole_cards:
            await interaction.response.send_message("❌ No cards found.", ephemeral=True);
            return

        # FIX: Only calculate poker hand rank if there are enough community cards (Flop or later)
        if len(self.result.community) >= 3:
            score = evaluator.evaluate(sp.hole_cards, self.result.community)
            rank_str = f" — *{evaluator.class_to_string(evaluator.get_rank_class(score))}*"
        else:
            rank_str = ""

        caption = f"👁️ **{interaction.user.display_name}** shows: {hand_str(sp.hole_cards)}{rank_str}"
        if USE_IMAGES:
            await interaction.response.defer()
            file = await asyncio.to_thread(card_images.make_strip, sp.hole_cards)
            await interaction.followup.send(caption, file=file)
        else:
            await interaction.response.send_message(caption)
        await self._resolve(interaction.user.id)


async def _reveal_phase(channel, t: TableState, result):
    settings = await db.get_settings(channel.guild.id)
    timeout = settings.get("muck_time", 15)  # Fetches custom time, defaults to 15s

    # ── 1. Uncontested Win (Everyone Folded) ──────────────
    if not result.pot_results:
        winner = result.winners[0] if result.winners else None
        if winner and winner.hole_cards:
            deadline = int(time.time()) + timeout
            view = ShowdownRevealView(t, result, [winner.user_id], timeout=timeout)
            msg = await channel.send(
                f"👁️ <@{winner.user_id}> — everyone folded! Show your hand or muck? *(auto-mucks <t:{deadline}:R>)*",
                view=view
            )
            try:
                await asyncio.wait_for(view._done.wait(), timeout=timeout + 1)
            except asyncio.TimeoutError:
                pass
            try:
                await msg.delete()
            except Exception:
                pass
        return

    # ── 2. Contested Showdown ─────────────────────────────
    winner_ids = {w.user_id for w in result.winners}

    # A. Automatically reveal winners' cards directly to the channel (No buttons)
    for w in result.winners:
        if w.hole_cards:
            score = evaluator.evaluate(w.hole_cards, result.community)
            rank_str = evaluator.class_to_string(evaluator.get_rank_class(score))
            caption = f"🏆 **{w.display_name}** wins and shows: {hand_str(w.hole_cards)} — *{rank_str}*"
            if USE_IMAGES:
                file = await asyncio.to_thread(card_images.make_strip, w.hole_cards)
                await channel.send(caption, file=file)
            else:
                await channel.send(caption)

    # B. Prompt losers with a Show/Muck button
    candidates = [p for p in (result.showdown_players or []) if p.user_id not in winner_ids]
    if not candidates:
        return  # Chop pot — everyone tied and won, so everyone already showed automatically

    deadline = int(time.time()) + timeout
    pending_ids = [p.user_id for p in candidates]
    mentions = " ".join(f"<@{uid}>" for uid in pending_ids)

    view = ShowdownRevealView(t, result, pending_ids, timeout=timeout)
    msg = await channel.send(
        f"👁️ {mentions} — show or muck? *(auto-mucks <t:{deadline}:R>)*",
        view=view
    )

    try:
        await asyncio.wait_for(view._done.wait(), timeout=timeout + 1)
    except asyncio.TimeoutError:
        pass
    try:
        await msg.delete()
    except Exception:
        pass

# ── Between-hands view ────────────────────────────────────────────────────────

class TipModal(discord.ui.Modal, title="Tip Dealer"):
    amount = discord.ui.TextInput(label="How many chips to tip?", placeholder="e.g. 50", min_length=1, max_length=7)

    def __init__(self, t: TableState, wallet_bal: int = 0, table_chips: int = 0):
        super().__init__()
        self.t           = t
        self.wallet_bal  = wallet_bal
        self.table_chips = table_chips
        total = wallet_bal + table_chips
        self.amount.placeholder = f"e.g. 50  (table: {table_chips} | wallet: {wallet_bal} | total: {total})"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            tip = int(self.amount.value)
        except ValueError:
            await interaction.response.send_message("❌ Enter a valid number.", ephemeral=True); return
        if tip <= 0:
            await interaction.response.send_message("❌ Tip must be more than 0.", ephemeral=True); return
        if interaction.user.id == self.t.manager_id:
            await interaction.response.send_message("❌ You can't tip yourself.", ephemeral=True); return

        # Defer before any DB work
        await interaction.response.defer(ephemeral=False)

        p           = self.t.game.get_player(interaction.user.id)
        table_chips = p.chips if p else 0
        wallet_bal  = await db.get_balance(interaction.user.id)

        from_table  = min(tip, table_chips)
        from_wallet = tip - from_table

        if from_wallet > wallet_bal:
            await interaction.followup.send(
                f"❌ Not enough chips. Table: **{table_chips}**, Wallet: **{wallet_bal}**.", ephemeral=True); return

        if from_table > 0 and p:
            p.chips -= from_table
            await db.update_chips_in_play(interaction.user.id, p.chips)
        if from_wallet > 0:
            ok = await db.deduct_chips(interaction.user.id, from_wallet)
            if not ok:
                if from_table > 0 and p:
                    p.chips += from_table
                    await db.update_chips_in_play(interaction.user.id, p.chips)
                await interaction.followup.send("❌ Failed to deduct wallet chips.", ephemeral=True); return

        manager_id = self.t.manager_id
        manager_name = "Dealer"
        try:
            # ZERO LAG: Check table memory first, then fast cache. No fetching!
            p_mgr = self.t.game.get_player(manager_id)
            if p_mgr:
                manager_name = p_mgr.display_name
            else:
                member = interaction.guild.get_member(manager_id)
                if member:
                    manager_name = member.display_name
        except Exception:
            pass

        await db.add_chips(interaction.user.id, interaction.user.display_name,
                           manager_id, manager_name, tip, f"Tip from {interaction.user.display_name}")
        await post_tip_log(interaction.channel, self.t, interaction.user.id, interaction.user.display_name, tip, manager_id, manager_name)
        await db.record_tip(interaction.user.id, interaction.user.display_name, tip)
        await interaction.followup.send(
            f"💸 **{interaction.user.display_name}** tipped **{tip}** chips to **{manager_name}**!", ephemeral=False)


class RebuyModal(discord.ui.Modal, title="Add Chips from Wallet"):
    amount = discord.ui.TextInput(label="How many chips to add?", min_length=1, max_length=8)

    def __init__(self, t: TableState, wallet_bal: int, max_w: int, current_stack: int):
        super().__init__()
        self.t = t
        self.wallet_bal = wallet_bal
        self.max_w = max_w
        self.current_stack = current_stack

        allowed = max_w - current_stack if max_w > 0 else wallet_bal
        self.actual_max = min(allowed, wallet_bal)
        self.amount.placeholder = f"1–{self.actual_max}  (wallet: {wallet_bal})"

    async def on_submit(self, interaction: discord.Interaction):
        chips = parse_chips(self.amount.value)
        if chips is None or chips <= 0:
            await interaction.response.send_message("❌ Enter a valid amount (e.g. 500, 2k).", ephemeral=True);
            return
        if chips > self.wallet_bal:
            await interaction.response.send_message(f"❌ You only have **{self.wallet_bal}** in your wallet.",
                                                    ephemeral=True);
            return
        if self.max_w > 0 and (self.current_stack + chips) > self.max_w:
            await interaction.response.send_message(
                f"❌ Maximum table stack is **{self.max_w}**. You can only add up to **{self.actual_max}** more chips.",
                ephemeral=True);
            return

        # Defer before DB writes
        await interaction.response.defer(ephemeral=False)

        ok = await db.deduct_chips(interaction.user.id, chips)
        if not ok:
            await interaction.followup.send("❌ Failed to deduct chips.", ephemeral=True);
            return

        msg = self.t.game.queue_rebuy(interaction.user.id, chips)
        if msg.startswith("❌"):
            await db.return_chips(interaction.user.id, chips)
            await interaction.followup.send(msg, ephemeral=True);
            return

        await db.mark_chips_in_play(interaction.user.id, interaction.user.display_name, chips)
        await interaction.followup.send(msg, ephemeral=False)

class BetweenHandsView(discord.ui.View):
    def __init__(self, t: TableState):
        super().__init__(timeout=None)
        self.t = t

    @discord.ui.button(label="Tip Dealer 💸", style=discord.ButtonStyle.blurple)
    async def tip_dealer(self, interaction: discord.Interaction, button: discord.ui.Button):
        t = self.t
        if t.closing:  # <-- ADDED GUARD
            await interaction.response.send_message("❌ This table is closing.", ephemeral=True);
            return
        if interaction.user.id == t.manager_id:
            await interaction.response.send_message("❌ You can't tip yourself.", ephemeral=True); return
        p           = t.game.get_player(interaction.user.id)
        table_chips = p.chips if p else 0
        # wallet_bal and zero-chips check moved into TipModal.on_submit (which defers first)
        await interaction.response.send_modal(TipModal(t, 0, table_chips))

    @discord.ui.button(label="Add Chips 💰", style=discord.ButtonStyle.green)
    async def add_chips(self, interaction: discord.Interaction, button: discord.ui.Button):
        t = self.t
        if t.closing:  # <-- ADDED GUARD
            await interaction.response.send_message("❌ This table is closing.", ephemeral=True);
            return
        p = t.game.get_player(interaction.user.id)
        pj = next((pj for pj in t.game.pending_joins if pj.user_id == interaction.user.id), None)

        if not p and not pj:
            await interaction.response.send_message("❌ You're not at the table.", ephemeral=True);
            return

        # Calculate current stack without any DB call
        current_stack = 0
        if p:
            current_stack = p.chips + p.pending_rebuy
        elif pj:
            current_stack = pj.chips + pj.pending_rebuy

        # All DB reads (wallet, settings) happen inside RebuyModal.on_submit after defer()
        # We need wallet_bal and max_w for the modal placeholder — fetch them now but
        # send_modal is the FIRST await on interaction.response so we're within 3 s.
        wallet_bal = await db.get_balance(interaction.user.id)
        if wallet_bal <= 0:
            await interaction.response.send_message("❌ Your wallet is empty.", ephemeral=True);
            return
        settings = await db.get_settings(interaction.guild_id)
        max_w = settings.get("max_wallet", 0)
        if max_w > 0 and current_stack >= max_w:
            await interaction.response.send_message(
                f"❌ You are already at or above the maximum table stack of **{max_w}**.", ephemeral=True);
            return
        await interaction.response.send_modal(RebuyModal(t, wallet_bal, max_w, current_stack))

# ── Raise picker view ─────────────────────────────────────────────────────────

class RaiseCustomModal(discord.ui.Modal, title="Custom Raise"):
    amount = discord.ui.TextInput(label="Raise BY how many chips?", placeholder="e.g. 200", min_length=1, max_length=7)

    def __init__(self, t: TableState, channel, guild):
        super().__init__()
        self.t = t; self.channel = channel; self.guild = guild

    async def on_submit(self, interaction: discord.Interaction):
        # Defer first — only one response allowed
        await interaction.response.defer()
        raise_amount = parse_chips(self.amount.value)
        if raise_amount is None:
            await interaction.followup.send("❌ Enter a valid amount (e.g. 500, 2k, 1.5k).", ephemeral=True); return
        uid = interaction.user.id
        p   = self.t.game.get_player(uid)
        if not p or not self.t.game.is_turn(uid):
            await interaction.followup.send("❌ It's not your turn.", ephemeral=True); return
        if raise_amount <= 0:
            await interaction.followup.send("❌ Must be greater than 0.", ephemeral=True); return
        success, msg = self.t.game.raise_bet(uid, raise_amount)
        if not success:
            await interaction.followup.send(msg, ephemeral=True); return
        if any(m in msg for m in ["🌊", "↩️", "🏁", "Showdown"]):
            slog_clear(self.t)
        slog(self.t, msg)
        if self.t.game._hand_result:
            await _process_result(interaction.guild, self.channel, self.t)
        else:
            await refresh(self.channel, self.t)

class RaisePickerView(discord.ui.View):
    """Shown when player clicks Raise — offers preset options."""
    def __init__(self, t: TableState, channel, guild, timeout: float):
        super().__init__(timeout=timeout)
        self.t = t; self.channel = channel; self.guild = guild

    async def _do_raise(self, interaction: discord.Interaction, raise_amount: int):
        await interaction.response.defer()
        uid = interaction.user.id
        if not self.t.game.is_turn(uid):
            await interaction.followup.send("❌ It's not your turn.", ephemeral=True); return
        success, msg = self.t.game.raise_bet(uid, raise_amount)
        if not success:
            await interaction.followup.send(msg, ephemeral=True); return
        if any(m in msg for m in ["🌊", "↩️", "🏁", "Showdown"]):
            slog_clear(self.t)
        slog(self.t, msg)
        if self.t.game._hand_result:
            await _process_result(self.guild, self.channel, self.t)
        else:
            await refresh(self.channel, self.t)

    @discord.ui.button(label="1/3 Pot", style=discord.ButtonStyle.green, row=0)
    async def third_pot(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True)
            return
        g = self.t.game
        p = g.get_player(interaction.user.id)
        if not p: await interaction.response.send_message("❌ Not your turn.", ephemeral=True); return
        amount = max(g.call_amount(p), g.pot // 3)
        await self._do_raise(interaction, amount)

    @discord.ui.button(label="1/2 Pot", style=discord.ButtonStyle.green, row=0)
    async def half_pot(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True)
            return
        g = self.t.game
        p = g.get_player(interaction.user.id)
        if not p: await interaction.response.send_message("❌ Not your turn.", ephemeral=True); return
        amount = max(g.call_amount(p), g.pot // 2)
        await self._do_raise(interaction, amount)

    @discord.ui.button(label="1/2 Stack", style=discord.ButtonStyle.blurple, row=0)
    async def half_stack(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True)
            return
        g = self.t.game
        p = g.get_player(interaction.user.id)
        if not p: await interaction.response.send_message("❌ Not your turn.", ephemeral=True); return
        amount = max(g.call_amount(p), p.chips // 2)
        await self._do_raise(interaction, amount)

    @discord.ui.button(label="All In 🚀", style=discord.ButtonStyle.red, row=0)
    async def all_in(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True)
            return
        await interaction.response.defer()
        g = self.t.game
        p = g.get_player(interaction.user.id)
        if not p:
            await interaction.followup.send("❌ Not your turn.", ephemeral=True); return
        call_needed  = g.call_amount(p)
        raise_on_top = p.chips - call_needed
        if raise_on_top <= 0:
            success, msg = g.check_or_call(interaction.user.id)
        else:
            success, msg = g.raise_bet(interaction.user.id, raise_on_top)
        if not success:
            await interaction.followup.send(msg, ephemeral=True); return
        if any(m in msg for m in ["🌊", "↩️", "🏁", "Showdown"]):
            slog_clear(self.t)
        slog(self.t, msg)
        if self.t.game._hand_result:
            await _process_result(self.guild, self.channel, self.t)
        else:
            await refresh(self.channel, self.t)

    @discord.ui.button(label="Custom…", style=discord.ButtonStyle.grey, row=0)
    async def custom(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RaiseCustomModal(self.t, self.channel, self.guild))

# ── Join modal ────────────────────────────────────────────────────────────────

class JoinModal(discord.ui.Modal, title="Buy In"):
    amount = discord.ui.TextInput(label="How many chips to bring to table?", min_length=1, max_length=8)

    def __init__(self, t: TableState, bal: int, min_w: int, max_w: int):
        super().__init__()
        self.t = t; self.bal = bal; self.min_w = min_w; self.max_w = max_w
        limit_str = f"{max_w}" if max_w > 0 else "None"
        self.amount.placeholder = f"min {min_w} — max {limit_str}  (wallet: {bal})"

    async def on_submit(self, interaction: discord.Interaction):
        if self.t.closing:
            await interaction.response.send_message("❌ This table has been closed.", ephemeral=True);
            return
        chips = parse_chips(self.amount.value)
        if chips is None:
            await interaction.response.send_message("❌ Enter a valid amount (e.g. 500, 2k).", ephemeral=True); return
        if chips < self.min_w:
            await interaction.response.send_message(f"❌ Minimum buy-in is **{self.min_w}** chips.", ephemeral=True); return
        if self.max_w > 0 and chips > self.max_w:
            await interaction.response.send_message(f"❌ Maximum buy-in is **{self.max_w}** chips.", ephemeral=True); return
        if chips > self.bal:
            await interaction.response.send_message(f"❌ You only have **{self.bal}** chips.", ephemeral=True); return

        # Defer before all DB work
        await interaction.response.defer(ephemeral=True)

        await db.upsert_wallet_name(interaction.user.id, interaction.user.name)

        t = self.t
        if await db.is_banned(interaction.guild_id, interaction.user.id, t.name):
            await interaction.followup.send("❌ You are banned from this table.", ephemeral=True); return

        ok = await db.deduct_chips(interaction.user.id, chips)
        if not ok:
            await interaction.followup.send("❌ Failed to deduct chips.", ephemeral=True);
            return

        # STRICT USERNAME: We now inject .name instead of .display_name!
        await db.mark_chips_in_play(interaction.user.id, interaction.user.name, chips)

        msg = t.game.add_player(interaction.user.id, interaction.user.name, chips)
        if msg.startswith("❌"):
            await db.return_chips(interaction.user.id, chips)
            await db.clear_chips_in_play(interaction.user.id)
            await interaction.followup.send(msg, ephemeral=True);
            return

        await interaction.channel.send(f"✅ **{interaction.user.display_name}** joined the table with **{chips}** <:poker_chip:1488128491881758760>!")
        await refresh(interaction.channel, t)
        await interaction.followup.send("✅ Successfully joined!", ephemeral=True)

# ── Game View ─────────────────────────────────────────────────────────────────

class GameView(discord.ui.View):
    def __init__(self, t: TableState):
        super().__init__(timeout=None)
        self.t = t
        in_hand = t.game.street not in (Street.WAITING, Street.SHOWDOWN)
        table_full = (len(t.game.players) + len(t.game.pending_joins)) >= 12
        self.btn_join.disabled =  table_full or t.closing

        self.btn_leave.disabled = t.closing  # <-- ADD THIS LINE

        for b in [self.btn_call, self.btn_check, self.btn_raise, self.btn_fold]:
            b.disabled = not in_hand

    async def _do_action(self, interaction: discord.Interaction, fn, *args):
        ok, msg = fn(*args)
        if not ok:
            await interaction.response.send_message(msg, ephemeral=True); return
        parts = msg.split("\n")
        street_markers = ["🌊", "↩️", "🏁"]
        if any(m in msg for m in street_markers + ["Showdown"]):
            slog_clear(self.t)
        slog(self.t, parts[0])
        for part in parts[1:]:
            if any(m in part for m in street_markers):
                slog(self.t, part); break
        await interaction.response.defer()
        if self.t.game._hand_result:
            await _process_result(interaction.guild, interaction.channel, self.t)
        else:
            await refresh(interaction.channel, self.t)

    @discord.ui.button(label="Join", style=discord.ButtonStyle.green, row=0)
    async def btn_join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.t.closing:
            await interaction.response.send_message("❌ This table is closing.", ephemeral=True); return
        if interaction.user.id in self.t.game.kicked_users:
            await interaction.response.send_message("❌ You have been kicked and cannot rejoin until the next table.", ephemeral=True); return
        if await db.is_banned(interaction.guild_id, interaction.user.id, self.t.name):
            await interaction.response.send_message("❌ You are banned from this table.", ephemeral=True); return
        settings = await db.get_settings(interaction.guild_id)
        min_w = settings.get("min_wallet", 50)
        max_w = settings.get("max_wallet", 0)  # <--- Fetch Max
        bal = await db.get_balance(interaction.user.id)
        if bal < min_w:
            await interaction.response.send_message(
                f"❌ Need at least **{min_w}** chips to join. Wallet: **{bal}**.", ephemeral=True);
            return
        await interaction.response.send_modal(JoinModal(self.t, bal, min_w, max_w))  # <--- Pass Max

    @discord.ui.button(label="Leave", style=discord.ButtonStyle.red, row=0)
    async def btn_leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.t.closing:
            await interaction.response.send_message("❌ Table is closing — your chips will be returned automatically.",
                                                    ephemeral=True);
            return
        if interaction.user.id in self.t.game.kicked_users:
            await interaction.response.send_message("❌ You have been kicked and will be removed after this hand.",
                                                    ephemeral=True);
            return
        if interaction.user.id in self.t.game.pending_leaves:
            await interaction.response.send_message("❌ You are already queued to leave after this hand.",
                                                    ephemeral=True);
            return
        p = self.t.game.get_player(interaction.user.id)
        pj = next((pj for pj in self.t.game.pending_joins if pj.user_id == interaction.user.id), None)
        if not p and not pj:
            await interaction.response.send_message("❌ You're not at the table.", ephemeral=True);
            return

        chips_back, msg = self.t.game.remove_player(interaction.user.id)

        # 1. DEFER IMMEDIATELY BEFORE DB WRITES
        await interaction.response.defer()

        # 2. Safely write to DB
        if chips_back > 0:
            await db.return_chips(interaction.user.id, chips_back)
            await db.clear_chips_in_play(interaction.user.id)

        if "will leave" in msg:
            await interaction.channel.send(f"👋 **{interaction.user.display_name}** will leave after this hand.")
        elif "left" in msg or "cashed out" in msg:
            await interaction.channel.send(
                f"👋 **{interaction.user.display_name}** left the table. Chips returned to wallet.")
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
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True);
            return

        await interaction.response.defer(ephemeral=True)

        settings = await db.get_settings(interaction.guild_id)
        afk_time = settings.get("turn_timeout", TURN_TIMEOUT_DEFAULT)

        view = RaisePickerView(self.t, interaction.channel, interaction.guild, timeout=afk_time)

        g = self.t.game
        p = g.get_player(interaction.user.id)
        call_amt = g.call_amount(p) if p else 0
        pot_third = max(call_amt, g.pot // 3) if p else 0
        pot_half = max(call_amt, g.pot // 2) if p else 0
        stack_half = max(call_amt, p.chips // 2) if p else 0
        await interaction.followup.send(
            f"**Raise options** — Pot: {g.pot} <:poker_chip:1488128491881758760>  |  Call: {call_amt}  |  Stack: {p.chips if p else '?'}\n"
            f"· 1/3 Pot = +{pot_third}  · 1/2 Pot = +{pot_half}  · 1/2 Stack = +{stack_half}",
            view=view, ephemeral=True)

    @discord.ui.button(label="Fold",  style=discord.ButtonStyle.red,    row=1)
    async def btn_fold(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True); return
        await self._do_action(interaction, self.t.game.fold, interaction.user.id)

    @discord.ui.button(label="My Cards", style=discord.ButtonStyle.grey, row=2)
    async def btn_hole(self, interaction: discord.Interaction, button: discord.ui.Button):
        p = self.t.game.get_player(interaction.user.id)
        if not p or not p.hole_cards:
            await interaction.response.send_message("❌ No cards right now.", ephemeral=True);
            return

        strength = ""
        if self.t.game.community and not p.folded:
            score = evaluator.evaluate(p.hole_cards, self.t.game.community)
            rank = evaluator.class_to_string(evaluator.get_rank_class(score))
            pct = round((1 - score / 7462) * 100, 1)
            strength = f"\n**Hand:** {rank} (top {100 - pct:.0f}%)"

        caption = f"Your hole cards — {p.chips} <:poker_chip:1488128491881758760> at table{strength}\n**Cards:** {hand_str(p.hole_cards)}"

        # 1. INSTANTLY send the text so players with slow internet see their cards immediately
        await interaction.response.send_message(caption, ephemeral=True)

        # 2. Generate the heavy image and patch it in a second later
        if USE_IMAGES:
            try:
                # Add '0' for backs, and 'True' for is_hole
                file = await asyncio.to_thread(card_images.make_strip, p.hole_cards, 0, True)
                await interaction.edit_original_response(attachments=[file])
            except Exception:
                pass

    @discord.ui.button(label="Rankings", style=discord.ButtonStyle.grey, row=2)
    async def btn_rankings(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "**Hand Rankings** *(best → worst)*\n```\n"
            "1.  Royal Flush       A K Q J 10 — same suit\n"
            "2.  Straight Flush    5 in a row — same suit\n"
            "3.  Four of a Kind    4 of same rank\n"
            "4.  Full House        3-of-a-kind + pair\n"
            "5.  Flush             Any 5 of same suit\n"
            "6.  Straight          5 in a row — any suits\n"
            "7.  Three of a Kind   3 of same rank\n"
            "8.  Two Pair          Two different pairs\n"
            "9.  One Pair          Two of same rank\n"
            "10. High Card         None of the above\n```", ephemeral=True)

    @discord.ui.button(label="Wallet", style=discord.ButtonStyle.grey, row=2)
    async def btn_wallet(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        uid = interaction.user.id
        bal, pending = await db.get_wallet(uid)
        p = self.t.game.get_player(uid)
        table_str = f"  |  **At table:** {p.chips} <:poker_chip:1488128491881758760>" if p else ""
        pending_str = f"  |  **Pending Cashout:** 🔒 {pending} <:poker_chip:1488128491881758760>" if pending > 0 else ""
        await interaction.followup.send(f"**Your Wallet:** {bal} <:poker_chip:1488128491881758760>{table_str}{pending_str}", ephemeral=True)

# ── Confirm DB reset ──────────────────────────────────────────────────────────

class ConfirmResetView1(discord.ui.View):
    def __init__(self, admin_id: int):
        super().__init__(timeout=30)
        self.admin_id = admin_id

    @discord.ui.button(label="Yes, I'm sure", style=discord.ButtonStyle.red)
    async def step1(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.admin_id:
            await interaction.response.send_message("❌ Not your button.", ephemeral=True); return
        view = ConfirmResetView2(self.admin_id)
        await interaction.response.edit_message(
            content="⚠️ **Final confirmation.** This CANNOT be undone.", view=view)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Cancelled.", view=None)

class ConfirmResetView2(discord.ui.View):
    def __init__(self, admin_id: int):
        super().__init__(timeout=30)
        self.admin_id = admin_id

    @discord.ui.button(label="WIPE EVERYTHING", style=discord.ButtonStyle.red)
    async def step2(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.admin_id:
            await interaction.response.send_message("❌ Not your button.", ephemeral=True);
            return

        # 1. DEFER FIRST
        await interaction.response.defer()

        # 2. DO THE WIPE
        await db.reset_database(interaction.user.id, interaction.user.display_name)
        tables.clear()

        # 3. USE FOLLOWUP FOR EDITS
        await interaction.edit_original_response(
            content=f"✅ Database wiped by **{interaction.user.display_name}**.", view=None)

# ── Cosmetics UI ─────────────────────────────────────────────────────────────

def _build_cosmetics_embed_and_view(user_id: int, cosmetics: dict):
    """Build the /poker titles embed and its interactive select-menu view."""
    owned_titles = set(cosmetics["unlocked_titles"])
    owned_msgs   = set(cosmetics["unlocked_win_msgs"])
    active_t     = cosmetics.get("active_title")
    active_m     = cosmetics.get("active_win_msg")

    embed = discord.Embed(title="🎖️ Your Cosmetics", color=0x9b59b6)

    # Get visible cosmetics for this user
    visible_titles = db.get_visible_cosmetics_for_user(user_id, owned_titles, db.TITLES)
    visible_winmsgs = db.get_visible_cosmetics_for_user(user_id, owned_msgs, db.WIN_MESSAGES)

    # ── Titles field ───────────────────────────────────────────────────────────
    t_lines = []
    for tid, info in visible_titles.items():
        rarity = db.RARITY_LABEL.get(info["rarity"], "")
        if tid in owned_titles:
            equipped = "  ◀ **equipped**" if tid == active_t else ""
            t_lines.append(f"✅ {info['display']} {rarity}{equipped}")
        else:
            desc = info['description'] if info['rarity'] != 'legendary' else "???"
            t_lines.append(f"🔒 ~~{info['display']}~~ — *{desc}*")
    
    total_visible = len(visible_titles)
    embed.add_field(
        name=f"🎖️ Titles  ({len(owned_titles)}/{total_visible} unlocked)",
        value="\n".join(t_lines) or "None yet.",
        inline=False,
    )

    # ── Win messages field ────────────────────────────────────────────────────
    m_lines = []
    for mid, info in visible_winmsgs.items():
        rarity = db.RARITY_LABEL.get(info["rarity"], "")
        if mid in owned_msgs:
            equipped = "  ◀ **equipped**" if mid == active_m else ""
            m_lines.append(f"✅ {info['display']} {rarity}{equipped}")
        else:
            desc = info['description'] if info['rarity'] != 'legendary' else "???"
            m_lines.append(f"🔒 ~~{info['display']}~~ — *{desc}*")
    
    total_visible_msgs = len(visible_winmsgs)
    embed.add_field(
        name=f"💬 Win Messages  ({len(owned_msgs)}/{total_visible_msgs} unlocked)",
        value="\n".join(m_lines) or "None yet.",
        inline=False,
    )

    embed.set_footer(text="Use the dropdowns below to equip — only your unlocked items appear.")
    view = CosmeticsView(user_id, owned_titles, owned_msgs, active_t, active_m)
    return embed, view


class CosmeticsView(discord.ui.View):
    """Attach two Select menus to /poker titles so the user can equip without typing IDs."""

    def __init__(
        self,
        user_id: int,
        owned_titles: set[str],
        owned_msgs: set[str],
        active_title: str | None,
        active_msg: str | None,
    ):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.message: discord.Message | discord.WebhookMessage | None = None

        # ── Title select ───────────────────────────────────────────────────────
        title_opts = [discord.SelectOption(label="— Remove title —", value="none", emoji="❌")]
        for tid in owned_titles:
            info = db.TITLES.get(tid)
            if info:
                title_opts.append(discord.SelectOption(
                    label=info["display"],
                    value=tid,
                    default=(tid == active_title),
                ))
        # Discord requires 1–25 options; cap just in case
        title_opts = title_opts[:25]

        title_select = discord.ui.Select(
            placeholder="🎖️ Equip a title…",
            options=title_opts,
            custom_id="cosmetics:title",
            row=0,
        )
        title_select.callback = self._on_title_select
        self.add_item(title_select)

        # ── Win-message select ────────────────────────────────────────────────
        msg_opts = [discord.SelectOption(label="— Remove win message —", value="none", emoji="❌")]
        for mid in owned_msgs:
            info = db.WIN_MESSAGES.get(mid)
            if info:
                msg_opts.append(discord.SelectOption(
                    label=info["display"],
                    value=mid,
                    default=(mid == active_msg),
                ))
        msg_opts = msg_opts[:25]

        msg_select = discord.ui.Select(
            placeholder="💬 Equip a win message…",
            options=msg_opts,
            custom_id="cosmetics:winmsg",
            row=1,
        )
        msg_select.callback = self._on_msg_select
        self.add_item(msg_select)

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This panel belongs to someone else.", ephemeral=True)
            return False
        return True

    async def _on_title_select(self, interaction: discord.Interaction):
        if not await self._guard(interaction): return
        chosen = interaction.data["values"][0]
        tid = None if chosen == "none" else chosen
        await db.set_active_title(self.user_id, tid)
        cosmetics = await db.get_cosmetics(self.user_id)
        embed, new_view = _build_cosmetics_embed_and_view(self.user_id, cosmetics)
        new_view.message = self.message
        label = db.TITLES[tid]["display"] if tid else "removed"
        await interaction.response.edit_message(
            content=f"✅ Title set to **{label}**." if tid else "✅ Title removed.",
            embed=embed, view=new_view)

    async def _on_msg_select(self, interaction: discord.Interaction):
        if not await self._guard(interaction): return
        chosen = interaction.data["values"][0]
        mid = None if chosen == "none" else chosen
        await db.set_active_win_msg(self.user_id, mid)
        cosmetics = await db.get_cosmetics(self.user_id)
        embed, new_view = _build_cosmetics_embed_and_view(self.user_id, cosmetics)
        new_view.message = self.message
        label = db.WIN_MESSAGES[mid]["display"] if mid else "removed"
        await interaction.response.edit_message(
            content=f"✅ Win message set to **{label}**." if mid else "✅ Win message removed.",
            embed=embed, view=new_view)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass


# ── Autocomplete helpers for /poker equiptitle and /poker equipwinmsg ─────────

async def _autocomplete_title(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    """Only shows titles the user has already unlocked."""
    cosmetics = await db.get_cosmetics(interaction.user.id)
    owned = set(cosmetics["unlocked_titles"])
    choices = [app_commands.Choice(name="— Remove title —", value="none")]
    for tid in owned:
        info = db.TITLES.get(tid)
        if info and current.lower() in info["display"].lower():
            choices.append(app_commands.Choice(name=info["display"], value=tid))
    return choices[:25]


async def _autocomplete_winmsg(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    """Only shows win messages the user has already unlocked."""
    cosmetics = await db.get_cosmetics(interaction.user.id)
    owned = set(cosmetics["unlocked_win_msgs"])
    choices = [app_commands.Choice(name="— Remove win message —", value="none")]
    for mid in owned:
        info = db.WIN_MESSAGES.get(mid)
        if info and current.lower() in info["display"].lower():
            choices.append(app_commands.Choice(name=info["display"], value=mid))
    return choices[:25]


async def _autocomplete_grant_cosmetic(
        interaction: discord.Interaction,
        current: str,
) -> list[app_commands.Choice[str]]:
    """Shows all available cosmetics for admins to grant."""
    # Check which 'kind' the admin selected in the previous dropdown
    kind = getattr(interaction.namespace, "kind", None)

    if kind == "title":
        catalog = db.TITLES
    elif kind == "winmsg":
        catalog = db.WIN_MESSAGES
    else:
        # If they haven't selected a kind yet, return empty to force them to pick one first
        return []

    choices = []
    for cid, info in catalog.items():
        display_text = f"{info['display']} ({cid})"
        if current.lower() in display_text.lower():
            # Discord limits choice names to 100 characters
            choices.append(app_commands.Choice(name=display_text[:100], value=cid))

    # Discord limits autocomplete to 25 results at a time
    return choices[:25]

# ── Cog ───────────────────────────────────────────────────────────────────────

class PokerCog(commands.Cog):

    DEV_USER_ID = [1339935869598961728,804762802451382283] # baymax for backups

    def __init__(self, bot):
        self.bot = bot
        self.daily_backup.start()

    def cog_unload(self):
        self.daily_backup.cancel()

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return
        key = (message.guild.id, message.channel.id)
        t = get_table(key)
        if not t or t.game.street == Street.WAITING:
            return

        t.msg_count += 1
        if t.msg_count >= t.resend_threshold:
            t.msg_count = 0
            t.hand_msg = None
            await refresh(message.channel, t, new_hand=True)

    # ── THE BACKUP ENGINE (Hidden Helper) ──────────────────────────────────
    async def _send_backup(self, user: discord.User):
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M")

        # 1. Get the absolute path to the directory this script lives in
        base_dir = os.path.dirname(os.path.abspath(__file__))
        clean_zip_name = f"poker_backup_{date_str}.zip"
        zip_path = os.path.join("/app/data", clean_zip_name)

        files_to_zip = ["/app/data/poker.db", "/app/data/poker.db-wal", "/app/data/poker.db-shm"]

        try:
            # Force SQLite to flush the WAL to the main DB safely
            async with db._write_lock:
                conn = await db._get_db()
                await conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

            # 2. Write the zip file safely using absolute paths and arcname
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for full_path in files_to_zip:
                    if os.path.exists(full_path):
                        zipf.write(full_path, arcname=os.path.basename(full_path))

                        # 3. Send the file to Discord
            with open(zip_path, 'rb') as f:
                discord_file = discord.File(f, filename=clean_zip_name)
                await user.send(f"📦 **Database Backup** ({date_str})", file=discord_file)

        finally:
            # 4. ALWAYS clean up the zip file, even if the Discord send fails
            if os.path.exists(zip_path):
                os.remove(zip_path)

    # ── THE AUTO TIMER (Every 24 Hours) ────────────────────────────────────
    time_to_run = dt_time(hour=4, minute=0, tzinfo=_tz.utc)
    @tasks.loop(time=time_to_run)
    async def daily_backup(self):
        try:
            user = await self.bot.fetch_user(self.DEV_USER_ID[0])
            if user:
                await self._send_backup(user)
        except Exception as e:
            print(f"[Backup Task Error] {e}")

    @daily_backup.before_loop
    async def before_daily_backup(self):
        await self.bot.wait_until_ready()

    # ── THE BOUNCER ────────────────────────────────────────────────────────
    # This automatically runs before EVERY slash command in this file.
    # If it returns False, the command is instantly killed.

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        allowed_guild = int(os.getenv("GUILD_ID", "0"))

        # If a GUILD_ID is set in .env, block DMs and other servers
        if allowed_guild and interaction.guild_id != allowed_guild:
            await interaction.response.send_message(
                "❌ This bot is exclusively configured for another server.",
                ephemeral=True
            )
            return False

        return True

    poker = app_commands.Group(name="poker", description="Texas Hold'em poker")
    pokerset = app_commands.Group(name="pokerset", description="Configure poker settings")
    pokermgr = app_commands.Group(name="pokermgr", description="Poker manager commands")
    pokeradmin = app_commands.Group(name="pokeradmin", description="Poker economy and admin commands")
    # ── Table management ──────────────────────────────────────────────────

    @poker.command(name="open", description="[Manager] Open a poker table in this channel")
    @app_commands.describe(name="Table name")
    async def open_table(self, interaction: discord.Interaction, name: str = "Poker Table"):
        await interaction.response.defer(ephemeral=True)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True);
            return
        for (gid, cid), t in tables.items():
            if gid == interaction.guild_id:
                await interaction.followup.send(
                    f"❌ A table is already running in <#{cid}>. Close it first.", ephemeral=True);
                return
        t = TableState(name, interaction.user.id)
        tables[(interaction.guild_id, interaction.channel_id)] = t
        settings = await db.get_settings(interaction.guild_id)
        t.game.SMALL_BLIND = settings["small_blind"]
        t.game.BIG_BLIND   = settings["big_blind"]
        t.game.MIN_BUYIN = settings.get("min_wallet", 50)
        t.resend_threshold = settings.get("resend_after_msgs", TABLE_RESEND_MSGS)
        await refresh(interaction.channel, t, new_hand=True)
        await interaction.followup.send("✅ Table opened!", ephemeral=True)  # <-- ADD THIS

    @poker.command(name="close", description="[Manager] Close table after current hand")
    async def close_table(self, interaction: discord.Interaction):
        key = (interaction.guild_id, interaction.channel_id)
        t = get_table(key)
        await interaction.response.defer(ephemeral=False)
        if not t:
            await interaction.followup.send("❌ No table in this channel.", ephemeral=True)
            return
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        if t.game.street == Street.WAITING and not t.game._hand_result:
            # No hand running and no hand resolving — close immediately
            await _close_table(interaction.channel, t)
            await interaction.followup.send("✅ Table closed.", ephemeral=False)
        else:
            # Hand in progress
            t.closing = True
            if t.auto_task and not t.auto_task.done():
                t.auto_task.cancel()
            if t.between_msg:
                try:
                    await t.between_msg.delete()
                except Exception:
                    pass
                t.between_msg = None
            await interaction.followup.send("✅ Table will close after this hand.", ephemeral=False)
            await refresh(interaction.channel, t)

    @poker.command(name="start", description="[Manager] Deal the first hand")
    async def start(self, interaction: discord.Interaction):
        key = (interaction.guild_id, interaction.channel_id)
        t = get_table(key)
        await interaction.response.defer(ephemeral=True)
        if not t:
            await interaction.followup.send("❌ No table here. Use `/poker open` first.", ephemeral=True)
            return
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return
        if t.game.street != Street.WAITING or t.game._hand_result:
            await interaction.followup.send("❌ A hand is already in progress or resolving.", ephemeral=True)
            return

        settings = await db.get_settings(interaction.guild_id)
        t.game.SMALL_BLIND = settings["small_blind"]
        t.game.BIG_BLIND   = settings["big_blind"]
        t.game.MIN_BUYIN = settings.get("min_wallet", 50)
        t.resend_threshold = settings.get("resend_after_msgs", TABLE_RESEND_MSGS)
        for uid in list(t.game.pending_leaves):
            p = t.game.get_player(uid)
            if p:
                total_to_return = p.chips + p.pending_rebuy
                if total_to_return > 0:
                    await db.return_chips(uid, total_to_return)
                    await db.clear_chips_in_play(uid)
                t.game.players.remove(p)

        t.game.pending_leaves.clear()
        t.game.kicked_users.clear()

        slog_clear(t)
        success, msg = t.game.start_hand()
        slog(t, msg)
        if not success:
            await interaction.followup.send(msg, ephemeral=True);
            return
        t.msg_count = 0
        await refresh(interaction.channel, t, new_hand=True)
        await interaction.followup.send("✅ Hand started!", ephemeral=True)

    @poker.command(name="table", description="Re-post the game panel")
    async def table_cmd(self, interaction: discord.Interaction):
        key = (interaction.guild_id, interaction.channel_id)
        t   = get_table(key)
        if not t:
            await interaction.response.send_message("❌ No table in this channel.", ephemeral=True); return
        t.hand_msg = None; t.board_file = None; t.ping_msg = None; t.ping_user_id = None; t.msg_count = 0
        await interaction.response.defer(ephemeral=True)
        await refresh(interaction.channel, t, new_hand=True)
        await interaction.followup.send("✅ Table refreshed!", ephemeral=True)

    # ── Manager moderation commands ───────────────────────────────────────

    @pokermgr.command(name="kick", description="[Manager] Kick a player — force folds them and removes after hand")
    @app_commands.describe(user="Player to kick")
    async def kick(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        key = (interaction.guild_id, interaction.channel_id)
        t = get_table(key)
        if not t:
            await interaction.followup.send("❌ No table here.", ephemeral=True);
            return

        p = t.game.get_player(user.id)
        pj = next((x for x in t.game.pending_joins if x.user_id == user.id), None)

        if not p and not pj:
            await interaction.followup.send(f"❌ **{user.display_name}** is not at the table.", ephemeral=True);
            return

        # Kick from waiting list
        if pj:
            t.game.pending_joins.remove(pj)
            total_to_return = pj.chips + pj.pending_rebuy
            if total_to_return > 0:
                await db.return_chips(user.id, total_to_return)
                await db.clear_chips_in_play(user.id)
            await interaction.followup.send(f"🦵 **{user.display_name}** has been kicked from the waiting list.")
            return

        # Kick from table
        if t.game.street == Street.WAITING and not t.game._hand_result:
            t.game.players.remove(p)
            total_to_return = p.chips + p.pending_rebuy
            if total_to_return > 0:
                await db.return_chips(user.id, total_to_return)
                await db.clear_chips_in_play(user.id)
            await interaction.followup.send(
                f"🦵 **{user.display_name}** has been kicked and removed from the table.")
            await refresh(interaction.channel, t)
            return

        if user.id not in t.game.kicked_users:
            t.game.kicked_users.append(user.id)
        if user.id not in t.game.pending_leaves:
            t.game.pending_leaves.append(user.id)

        if not p.folded:
            ok, fold_msg = t.game.force_fold(user.id)
            if ok:
                parts = fold_msg.split("\n")
                if any(m in fold_msg for m in ["🌊", "↩️", "🏁", "Showdown"]):
                    slog_clear(t)
                for part in parts:
                    if part.strip():
                        slog(t, part)

        await interaction.followup.send(
            f"🦵 **{user.display_name}** has been kicked — force folded and will be removed after this hand.")

        if t.game._hand_result:
            await _process_result(interaction.guild, interaction.channel, t)
        else:
            await refresh(interaction.channel, t)

    @pokermgr.command(name="ban", description="[Manager] Ban a user — omit table name to ban server-wide")
    @app_commands.describe(user="Player to ban", table_name="Table name to ban from (leave blank for server-wide)")
    async def ban(self, interaction: discord.Interaction, user: discord.Member, table_name: str = None):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        # 2. Persist ban to DB
        added = await db.ban_player(interaction.guild_id, user.id, user.display_name,
                                    interaction.user.id, table_name)
        scope = f"table **{table_name}**" if table_name else "**all tables** (server-wide)"

        kicked_from = ""

        # Grab the single active table for this server
        active = next(((cid, table) for (gid, cid), table in tables.items() if gid == interaction.guild_id), None)

        if active:
            cid, t = active
            if table_name is None or t.name.lower() == table_name.lower():
                if user.id not in t.game.banned_users:
                    t.game.banned_users.append(user.id)

                p = t.game.get_player(user.id)
                pj = next((x for x in t.game.pending_joins if x.user_id == user.id), None)

                # Kick from waiting list
                if pj:
                    t.game.pending_joins.remove(pj)
                    total_to_return = pj.chips + pj.pending_rebuy
                    if total_to_return > 0:
                        await db.return_chips(user.id, total_to_return)
                        await db.clear_chips_in_play(user.id)

                # Kick from active table
                if p:
                    if t.game.street == Street.WAITING and not t.game._hand_result:
                        t.game.players.remove(p)
                        total_to_return = p.chips + p.pending_rebuy
                        if total_to_return > 0:
                            await db.return_chips(user.id, total_to_return)
                            await db.clear_chips_in_play(user.id)
                    else:
                        if user.id not in t.game.kicked_users:
                            t.game.kicked_users.append(user.id)
                        if user.id not in t.game.pending_leaves:
                            t.game.pending_leaves.append(user.id)

                        if not p.folded:
                            ok, fold_msg = t.game.force_fold(user.id)
                            if ok:
                                parts = fold_msg.split("\n")
                                if any(m in fold_msg for m in ["🌊", "↩️", "🏁", "Showdown"]):
                                    slog_clear(t)
                                for part in parts:
                                    if part.strip():
                                        slog(t, part)

                if p or pj:
                    kicked_from = f" Kicked from: {t.name}."
                    ch = interaction.guild.get_channel(cid)
                    if ch:
                        if t.game._hand_result:
                            await ch.send(
                                f"🔨 **{user.display_name}** has been banned and will be removed after this hand.")
                            await _process_result(interaction.guild, ch, t)
                        elif t.game.street == Street.WAITING:
                            await ch.send(f"🔨 **{user.display_name}** has been banned and removed from the table.")
                            await refresh(ch, t)
                        else:
                            await ch.send(
                                f"🔨 **{user.display_name}** has been banned and will be removed after this hand.")
                            await refresh(ch, t)

        if not added:
            await interaction.followup.send(f"ℹ️ **{user.display_name}** was already banned from {scope}.{kicked_from}",
                                            ephemeral=True)
        else:
            await interaction.followup.send(f"🔨 **{user.display_name}** banned from {scope}.{kicked_from}",
                                            ephemeral=not kicked_from)

    @pokermgr.command(name="unban", description="[Manager] Unban a user — omit table name to remove all bans")
    @app_commands.describe(user="Player to unban", table_name="Table to unban from (leave blank to remove all bans)")
    async def unban(self, interaction: discord.Interaction, user: discord.Member, table_name: str = None):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return
        removed = await db.unban_player(interaction.guild_id, user.id, table_name)
        scope = f"table **{table_name}**" if table_name else "all tables"

        active = next(((cid, table) for (gid, cid), table in tables.items() if gid == interaction.guild_id), None)

        if active:
            cid, t = active
            if table_name is None or t.name.lower() == (table_name or "").lower():
                if user.id in t.game.banned_users:
                    t.game.banned_users.remove(user.id)

        # FIXED: Send publicly
        if removed:
            await interaction.followup.send(f"✅ **{user.display_name}** unbanned from {scope}.", ephemeral=False)
        else:
            await interaction.followup.send(f"ℹ️ **{user.display_name}** had no bans for {scope}.", ephemeral=False)

    @pokermgr.command(name="forcefold", description="[Manager] Force a player to fold their hand")
    @app_commands.describe(user="Player to force fold")
    async def force_fold_cmd(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        key = (interaction.guild_id, interaction.channel_id)
        t = get_table(key)
        if not t:
            await interaction.followup.send("❌ No table here.", ephemeral=True);
            return
        if t.game.street == Street.WAITING:
            await interaction.followup.send("❌ No hand in progress.", ephemeral=True);
            return
        p = t.game.get_player(user.id)
        if not p:
            await interaction.followup.send(f"❌ **{user.display_name}** is not at the table.", ephemeral=True);
            return
        if p.folded:
            await interaction.followup.send(f"ℹ️ **{user.display_name}** is already folded.", ephemeral=True);
            return

        ok, msg = t.game.force_fold(user.id)
        if not ok:
            await interaction.followup.send(f"❌ {msg}", ephemeral=True);
            return

        slog(t, msg)
        await interaction.followup.send(f"✅ Force folded **{user.display_name}**.")

        if t.game._hand_result:
            await _process_result(interaction.guild, interaction.channel, t)
        else:
            await refresh(interaction.channel, t)

    # ── Player commands ───────────────────────────────────────────────────

    @poker.command(name="wallet", description="Check your chip wallet balance")
    @app_commands.describe(user="Player to check (leave blank for yourself)")
    async def wallet(self, interaction: discord.Interaction, user: discord.Member = None):
        await interaction.response.defer(ephemeral=False)
        target = user or interaction.user
        bal, pending = await db.get_wallet(target.id)
        key = (interaction.guild_id, interaction.channel_id)
        t = get_table(key)
        p = t.game.get_player(target.id) if t else None
        table_str = f"\n**At table:** {p.chips} <:poker_chip:1488128491881758760>" if p else ""
        pending_str = f"\n**Pending Cashout:** 🔒 {pending} <:poker_chip:1488128491881758760>" if pending > 0 else ""
        label = f"**{target.display_name}'s Wallet**" if user else "**Your Wallet**"
        await interaction.followup.send(f"{label}: {bal} <:poker_chip:1488128491881758760>{table_str}{pending_str}", ephemeral=False)

    @poker.command(name="tip", description="Tip the dealer between hands")
    @app_commands.describe(amount="How many chips to tip? (e.g. 50, 1k)")
    async def tip_cmd(self, interaction: discord.Interaction, amount: str):
        # 1. Defer instantly since we don't need a modal anymore
        await interaction.response.defer(ephemeral=False)

        tip = parse_chips(amount)
        if tip is None or tip <= 0:
            await interaction.followup.send("❌ Enter a valid amount greater than 0.", ephemeral=True)
            return

        key = (interaction.guild_id, interaction.channel_id)
        t = get_table(key)
        if not t:
            await interaction.followup.send("❌ No table in this channel.", ephemeral=True)
            return

        if interaction.user.id == t.manager_id:
            await interaction.followup.send("❌ You can't tip yourself.", ephemeral=True)
            return

        p = t.game.get_player(interaction.user.id)
        table_chips = p.chips if p else 0
        wallet_bal = await db.get_balance(interaction.user.id)

        # Pull from table first, then wallet
        from_table = min(tip, table_chips)
        from_wallet = tip - from_table

        if from_wallet > wallet_bal:
            await interaction.followup.send(
                f"❌ Not enough chips. Table: **{table_chips}**, Wallet: **{wallet_bal}**.", ephemeral=True)
            return

        # Deduct chips
        if from_table > 0 and p:
            p.chips -= from_table
            await db.update_chips_in_play(interaction.user.id, p.chips)
        if from_wallet > 0:
            ok = await db.deduct_chips(interaction.user.id, from_wallet)
            if not ok:
                # Rollback if wallet deduction fails
                if from_table > 0 and p:
                    p.chips += from_table
                    await db.update_chips_in_play(interaction.user.id, p.chips)
                await interaction.followup.send("❌ Failed to deduct wallet chips.", ephemeral=True)
                return

        # ZERO LAG: Get manager name without fetching
        manager_id = t.manager_id
        manager_name = "Dealer"
        try:
            p_mgr = t.game.get_player(manager_id)
            if p_mgr:
                manager_name = p_mgr.display_name
            else:
                member = interaction.guild.get_member(manager_id)
                if member:
                    manager_name = member.display_name
        except Exception:
            pass

        # Log and send
        await db.add_chips(interaction.user.id, interaction.user.display_name,
                           manager_id, manager_name, tip, f"Tip from {interaction.user.display_name}")

        await post_tip_log(interaction.channel, t, interaction.user.id, interaction.user.display_name, tip, manager_id,
                           manager_name)
        await db.record_tip(interaction.user.id, interaction.user.display_name, tip)

        await interaction.followup.send(
            f"💸 **{interaction.user.display_name}** tipped **{tip}** chips to **{manager_name}**!", ephemeral=False)

    @poker.command(name="leaderboard", description="Top poker players by net chips")
    async def leaderboard(self, interaction: discord.Interaction):

        await interaction.response.defer()

        rows = await db.get_leaderboard(10)
        caller_id = interaction.user.id
        caller_row = await db.get_player_stats(caller_id)
        if not rows:
            await interaction.followup.send("No stats yet!", ephemeral=True)
            return

        MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
        top_ids = {r['user_id'] for r in rows}

        table_lines = ["```"]
        table_lines.append(f"{'':4}{'Player':<18} {'Win%':>5} {'Net':>9} {'Wallet':>8}")
        table_lines.append("─" * 47)
        for i, r in enumerate(rows):
            rank = i + 1
            wp = f"{r['hands_won'] / r['hands_played'] * 100:.0f}%" if r['hands_played'] else "—"
            net = r['net_chips']
            sign = "+" if net >= 0 else ""
            uname = r['username'][:17]
            medal = MEDALS.get(rank, f"{rank}. ")
            you_tag = " ◀" if r['user_id'] == caller_id else ""
            table_lines.append(f"{medal:<4}{uname:<18} {wp:>5} {sign + str(net):>9} {r['wallet']:>8}{you_tag}")
        table_lines.append("```")

        embed = discord.Embed(
            title="🏆 Poker Leaderboard",
            description="\n".join(table_lines),
            color=0xF1C40F
        )

        # Caller's stats — shown at the bottom whether or not they're in the top 10
        if caller_row:
            caller_rank = await db.get_player_rank(caller_id)
            caller_net = caller_row['net_chips']
            caller_wp = f"{caller_row['hands_won'] / caller_row['hands_played'] * 100:.1f}%" if caller_row[
                'hands_played'] else "—"
            caller_sign = "+" if caller_net >= 0 else ""
            in_top = caller_id in top_ids
            rank_str = f"#{caller_rank}" if caller_rank else "—"
            label = f"📊 Your Stats  ·  {rank_str}" + (" *(in top 10)*" if in_top else "")
            embed.add_field(
                name=label,
                value=(
                    f"Win% **{caller_wp}**  ·  "
                    f"Net **{caller_sign}{caller_net}** <:poker_chip:1488128491881758760>  ·  "
                    f"Wallet **{caller_row['wallet']}** <:poker_chip:1488128491881758760>"
                ),
                inline=False
            )
        else:
            embed.add_field(name="📊 Your Stats", value="No hands played yet.", inline=False)

        await interaction.followup.send(embed=embed)

    @pokermgr.command(name="removestats", description="[Manager] Remove a player from the leaderboard")
    @app_commands.describe(user="Player to remove from leaderboard")
    async def remove_stats(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True);
            return
        removed = await db.delete_player_stats(user.id)
        if removed:
            await interaction.followup.send(f"✅ Removed **{user.name}** ({user.id}) from the leaderboard.")
        else:
            await interaction.followup.send(f"ℹ️ **{user.name}** has no stats on record.", ephemeral=True)

    @poker.command(name="stats", description="View your poker stats")
    @app_commands.describe(hidden="Hide the stats message from others? (Default: False)")
    async def stats(self, interaction: discord.Interaction, hidden: bool = False):
        # 1. Defer using the user's choice
        await interaction.response.defer(ephemeral=hidden)

        row = await db.get_player_stats(interaction.user.id)
        if not row:
            await interaction.followup.send("No stats yet!", ephemeral=hidden)
            return

        # FETCH THE RANK
        rank = await db.get_player_rank(interaction.user.id)
        rank_str = f"#{rank}" if rank else "Unranked"

        net = row['net_chips']

        # 🎨 Added the rank directly into the Title to keep the grid clean
        embed = discord.Embed(title=f"📊 Stats — {row['username']}",
                              color=0x2ecc71 if net >= 0 else 0xe74c3c)

        wp = f"{row['hands_won'] / row['hands_played'] * 100:.1f}%" if row['hands_played'] else "—"

        embed.add_field(name="Rank", value=str(rank_str), inline=True)
        embed.add_field(name="Hands", value=str(row['hands_played']), inline=True)
        embed.add_field(name="Win %", value=wp, inline=True)
        embed.add_field(name="Net", value=f"{'+' if net >= 0 else ''}{net} <:poker_chip:1488128491881758760>", inline=True)
        embed.add_field(name="Wallet", value=f"{row['wallet']} <:poker_chip:1488128491881758760>", inline=True)
        embed.add_field(name="Tipped", value=f"{row.get('total_tipped', 0):,} <:poker_chip:1488128491881758760>", inline=True)

        # 2. Send the final embed using the user's choice
        await interaction.followup.send(embed=embed, ephemeral=hidden)

    # ── Manager settings commands ─────────────────────────────────────────
    @pokermgr.command(name="addchips", description="[Manager] Add chips to a player's wallet")
    @app_commands.describe(user="Player", amount="Chips to add", note="Optional reason")
    async def mgr_addchips(self, interaction: discord.Interaction, user: discord.Member, amount: int, note: str = ""):
        await interaction.response.defer(ephemeral=False)

        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        allowed_str = os.getenv("ADD_CHIPS_CHANNELS", "")
        if allowed_str:
            allowed_channels = [int(c.strip()) for c in allowed_str.split(",") if c.strip().isdigit()]
            if allowed_channels and interaction.channel_id not in allowed_channels:
                mentions = ", ".join(f"<#{cid}>" for cid in allowed_channels)
                await interaction.followup.send(f"❌ This command is restricted to: {mentions}", ephemeral=True)
                return

        if amount <= 0:
            await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)
            return

        new_bal = await db.add_chips(interaction.user.id, interaction.user.display_name,
                                     user.id, user.display_name, amount, note)

        await interaction.followup.send(
            f"✅ **+{amount}** chips → **{user.mention}** |  Balance: **{new_bal}** <:poker_chip:1488128491881758760>"
            + (f"\n> {note}" if note else ""), ephemeral=False)

    @pokermgr.command(name="removechips", description="[Manager] Remove chips from a player's wallet")
    @app_commands.describe(user="Player", amount="Chips to remove", note="Optional reason")
    async def mgr_removechips(self, interaction: discord.Interaction, user: discord.Member, amount: int,
                              note: str = ""):
        await interaction.response.defer(ephemeral=False)

        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        allowed_str = os.getenv("REMOVE_CHIPS_CHANNELS", "")
        if allowed_str:
            allowed_channels = [int(c.strip()) for c in allowed_str.split(",") if c.strip().isdigit()]
            if allowed_channels and interaction.channel_id not in allowed_channels:
                mentions = ", ".join(f"<#{cid}>" for cid in allowed_channels)
                await interaction.followup.send(f"❌ This command is restricted to: {mentions}", ephemeral=True)
                return

        if amount <= 0:
            await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)
            return

        bal_before = await db.get_balance(user.id)
        if amount > bal_before:
            await interaction.followup.send(
                f"❌ **{user.display_name}** only has **{bal_before}** <:poker_chip:1488128491881758760> in their wallet. You cannot remove **{amount}**.",
                ephemeral=True)
            return

        new_bal = await db.add_chips(interaction.user.id, interaction.user.display_name,
                                     user.id, user.display_name, -amount, note)

        expected = bal_before - amount
        if new_bal > expected:
            await interaction.followup.send(
                f"⚠️ Only **{bal_before - new_bal}** chips could be removed — **{user.display_name}**'s balance changed concurrently. New balance: **{new_bal}** <:poker_chip:1488128491881758760>",
                ephemeral=True)
            return

        await interaction.followup.send(
            f"✅ **-{amount}** chips from **{user.mention}** |  Balance: **{new_bal}** <:poker_chip:1488128491881758760>"
            + (f"\n> {note}" if note else ""), ephemeral=False)

    @pokermgr.command(name="setdealer", description="[Manager] Change the dealer (who receives tips) for this table")
    @app_commands.describe(user="The new dealer")
    async def set_dealer(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        key = (interaction.guild_id, interaction.channel_id)
        t = get_table(key)
        if not t:
            await interaction.followup.send("❌ No table in this channel.", ephemeral=True)
            return

        # Switch the tip recipient
        t.manager_id = user.id

        await interaction.followup.send(
            f"🔄 **{user.mention}** has taken over as the dealer! All new tips will go to them.")

    @pokermgr.command(name="bans", description="[Manager] List all currently banned players")
    async def list_bans(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        bans = await db.get_all_bans(interaction.guild_id)

        if not bans:
            await interaction.followup.send("✅ There are currently no banned players in this server.", ephemeral=False)
            return

        lines = []
        for b in bans:
            scope = f"Table: **{b['table_name']}**" if b['table_name'] else "**Server-wide**"
            date_str = b['ts'].split(" ")[0]
            lines.append(f"• **{b['username']}** (`{b['user_id']}`) — {scope} *(on {date_str})*")

        description = "\n".join(lines)[:4096]

        embed = discord.Embed(
            title="🔨 Active Poker Bans",
            description=description,
            color=0xED4245
        )
        embed.set_footer(text=f"Total bans: {len(bans)}")

        # FIXED: Send publicly
        await interaction.followup.send(embed=embed, ephemeral=False)

    @poker.command(name="settings", description="[Manager] View table settings")
    async def settings_view(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return
        s = await db.get_settings(interaction.guild_id)
        role_str = f"<@&{s['manager_role_id']}>" if s.get("manager_role_id") else "*(not set)*"
        log_str = f"<#{s['log_channel_id']}>" if s.get("log_channel_id") else "*(not set)*"

        embed = discord.Embed(title="⚙️ Poker Settings", color=0x5865F2)

        # Row 1: Blinds
        embed.add_field(name="Small Blind", value=str(s["small_blind"]), inline=True)
        embed.add_field(name="Big Blind", value=str(s["big_blind"]), inline=True)
        embed.add_field(name="\u200b", value="\u200b", inline=True)  # Invisible 3rd slot

        # Row 2: Buy-ins
        embed.add_field(name="Min Buy-in", value=str(s["min_wallet"]), inline=True)
        max_val = str(s.get("max_wallet", 2000)) if s.get("max_wallet", 2000) > 0 else "None (No Limit)"
        embed.add_field(name="Max Buy-in", value=max_val, inline=True)
        embed.add_field(name="\u200b", value="\u200b", inline=True)  # Invisible 3rd slot

        # Row 3: Timers
        embed.add_field(name="Turn Timeout", value=f"{s.get('turn_timeout', TURN_TIMEOUT_DEFAULT)}s", inline=True)
        embed.add_field(name="Muck Timeout", value=f"{s.get('muck_time', 15)}s", inline=True)
        embed.add_field(name="\u200b", value="\u200b", inline=True)  # Invisible 3rd slot

        # Row 4: Misc
        embed.add_field(name="Next Hand Delay", value=f"{s.get('next_hand_delay', NEXT_HAND_DELAY_DEFAULT)}s",
                        inline=True)
        embed.add_field(name="Resend Embed", value=f"every {s.get('resend_after_msgs', TABLE_RESEND_MSGS)} msgs",
                        inline=True)
        embed.add_field(name="\u200b", value="\u200b", inline=True)  # Invisible 3rd slot

        # Row 5: Roles
        embed.add_field(name="Manager Role", value=role_str, inline=True)
        embed.add_field(name="Log Channel", value=log_str, inline=True)

        await interaction.followup.send(embed=embed, ephemeral=False)

    @pokerset.command(name="blinds", description="[Manager] Set small and big blind amounts")
    @app_commands.describe(small="Small blind", big="Big blind")
    async def set_blinds(self, interaction: discord.Interaction, small: int, big: int):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        if small <= 0 or big <= small:
            await interaction.response.send_message("❌ Big blind must be > small blind.", ephemeral=True); return
        await interaction.response.defer(ephemeral=False)
        await db.set_settings(interaction.guild_id, small_blind=small, big_blind=big)
        await interaction.followup.send(f"✅ Blinds: **{small}** / **{big}**")

    @pokerset.command(name="minbuyin", description="[Manager] Set minimum buy-in required to join")
    @app_commands.describe(amount="Minimum chips required")
    async def set_min_buyin(self, interaction: discord.Interaction, amount: int):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return
        if amount < 0:
            await interaction.response.send_message("❌ Must be 0 or more.", ephemeral=True);
            return
        # We leave the DB key as "min_wallet" so it doesn't break your database
        await interaction.response.defer(ephemeral=False)
        await db.set_settings(interaction.guild_id, min_wallet=amount)
        await interaction.followup.send(f"✅ Minimum buy-in: **{amount}** chips")

    @pokerset.command(name="maxbuyin", description="[Manager] Set maximum table stack (0 for unlimited)")
    @app_commands.describe(amount="Max chips allowed (0 = no limit)")
    async def set_max_buyin(self, interaction: discord.Interaction, amount: int):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return
        if amount < 0:
            await interaction.response.send_message("❌ Must be 0 or more.", ephemeral=True);
            return
        await interaction.response.defer(ephemeral=False)
        await db.set_settings(interaction.guild_id, max_wallet=amount)
        msg = f"**{amount}** chips" if amount > 0 else "**None** (Unlimited)"
        await interaction.followup.send(f"✅ Maximum buy-in set to: {msg}")

    @pokerset.command(name="nexthanddelay", description="[Manager] Set the delay between hands (seconds)")
    @app_commands.describe(seconds="Seconds to wait between hands (5–300)")
    async def set_next_hand_delay(self, interaction: discord.Interaction, seconds: int):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        if seconds < 5 or seconds > 300:
            await interaction.response.send_message("❌ Must be 5–300 seconds.", ephemeral=True); return
        await interaction.response.defer(ephemeral=False)
        await db.set_settings(interaction.guild_id, next_hand_delay=seconds)
        await interaction.followup.send(f"✅ Next hand delay: **{seconds}s**")

    @pokerset.command(name="turntimeout", description="[Manager] Set AFK fold timer (default 5 min)")
    @app_commands.describe(seconds="Seconds before auto-fold (30–600)")
    async def set_turn_timeout(self, interaction: discord.Interaction, seconds: int):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        if seconds < 30 or seconds > 600:
            await interaction.response.send_message("❌ Must be 30–600 seconds.", ephemeral=True); return
        await interaction.response.defer(ephemeral=False)
        await db.set_settings(interaction.guild_id, turn_timeout=seconds)
        await interaction.followup.send(f"✅ Turn timeout (AFK fold): **{seconds}s**")

    @pokerset.command(name="resend", description="[Manager] Set how many messages before embed is resent")
    @app_commands.describe(count="Number of messages (3–50)")
    async def set_resend(self, interaction: discord.Interaction, count: int):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        if count < 3 or count > 50:
            await interaction.response.send_message("❌ Must be 3–50.", ephemeral=True); return
        await interaction.response.defer(ephemeral=False)
        await db.set_settings(interaction.guild_id, resend_after_msgs=count)
        await interaction.followup.send(f"✅ Embed resend threshold: **{count}** messages")

    @pokerset.command(name="mucktime", description="[Manager] Set time limit for players to show/muck")
    @app_commands.describe(seconds="Seconds to wait (5–60)")
    async def set_muck_time(self, interaction: discord.Interaction, seconds: int):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return
        if seconds < 5 or seconds > 60:
            await interaction.response.send_message("❌ Must be 5–60 seconds.", ephemeral=True);
            return
        await interaction.response.defer(ephemeral=False)
        await db.set_settings(interaction.guild_id, muck_time=seconds)
        await interaction.followup.send(f"✅ Showdown muck timer: **{seconds}s**")

    @pokerset.command(name="logchannel", description="[Manager] Set channel for hand logs")
    @app_commands.describe(channel="The channel to post log thread in")
    async def set_log_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        await interaction.response.defer(ephemeral=False)
        await db.set_settings(interaction.guild_id, log_channel_id=channel.id)
        await interaction.followup.send(f"✅ Log channel: {channel.mention}")

    @pokerset.command(name="managerrole", description="[Admin] Set the Poker Manager role")
    @app_commands.describe(role="Role that gets poker manager access")
    async def set_manager_role(self, interaction: discord.Interaction, role: discord.Role):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Server Administrator only.", ephemeral=True); return
        await interaction.response.defer(ephemeral=False)
        await db.set_settings(interaction.guild_id, manager_role_id=role.id)
        await interaction.followup.send(f"✅ Poker Manager role: **{role.name}**")

    @poker.command(name="resetdb", description="[Admin] Wipe all poker data from the database")
    async def reset_db(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Server Administrator only.", ephemeral=True); return
        view = ConfirmResetView1(interaction.user.id)
        await interaction.response.send_message(
            "⚠️ **This will permanently delete all wallets, stats, logs and settings.**\nAre you sure?",
            view=view, ephemeral=True)

    @poker.command(name="rebuy", description="Add more chips to your table stack from your wallet")
    @app_commands.describe(amount="How many chips to add (e.g. 500, 2k)")
    async def rebuy(self, interaction: discord.Interaction, amount: str):
        await interaction.response.defer(ephemeral=True)
        chips = parse_chips(amount)
        if chips is None or chips <= 0:
            await interaction.followup.send("❌ Enter a valid amount.", ephemeral=True);
            return

        key = (interaction.guild_id, interaction.channel_id)
        t = get_table(key)
        if not t:
            await interaction.followup.send("❌ No table here.", ephemeral=True);
            return

        p = t.game.get_player(interaction.user.id)
        pj = next((x for x in t.game.pending_joins if x.user_id == interaction.user.id), None)

        if not p and not pj:
            await interaction.followup.send("❌ You're not at the table.", ephemeral=True);
            return

        wallet_bal = await db.get_balance(interaction.user.id)
        if chips > wallet_bal:
            await interaction.followup.send(f"❌ You only have **{wallet_bal}** in your wallet.", ephemeral=True);
            return

        current_stack = 0
        if p:
            current_stack = p.chips + p.pending_rebuy
        elif pj:
            current_stack = pj.chips + pj.pending_rebuy

        settings = await db.get_settings(interaction.guild_id)
        max_w = settings.get("max_wallet", 0)
        if max_w > 0 and (current_stack + chips) > max_w:
            allowed = max_w - current_stack
            actual_max = max(0, min(allowed, wallet_bal))
            await interaction.followup.send(
                f"❌ Maximum table stack is **{max_w}**. You can only add up to **{actual_max}** more chips.",
                ephemeral=True);
            return

        ok = await db.deduct_chips(interaction.user.id, chips)
        if not ok:
            await interaction.followup.send("❌ Failed to deduct chips.", ephemeral=True);
            return

        msg = t.game.queue_rebuy(interaction.user.id, chips)

        # FIXED: Check if queue failed, and refund if it did
        if msg.startswith("❌"):
            await db.return_chips(interaction.user.id, chips)
            await interaction.followup.send(msg, ephemeral=True)
            return

        await db.mark_chips_in_play(interaction.user.id, interaction.user.display_name, chips)

        await interaction.followup.send("✅ Chips queued successfully!", ephemeral=True)
        await interaction.channel.send(msg)
        if t.game.street == Street.WAITING:
            await refresh(interaction.channel, t)

    @poker.command(name="request_cashout", description="Lock chips for withdrawal and notify staff")
    @app_commands.describe(amount="Chips to cash out", note="Optional payment info")
    async def request_cashout(self, interaction: discord.Interaction, amount: str, note: str = ""):
        # FIXED: Defer ephemerally to hide from chat
        await interaction.response.defer(ephemeral=True)

        chips = parse_chips(amount)
        if chips is None or chips <= 0:
            await interaction.followup.send("❌ Enter a valid amount (e.g. 500, 2k).", ephemeral=True);
            return

        bal, _ = await db.get_wallet(interaction.user.id)
        if chips > bal:
            await interaction.followup.send(
                f"❌ You only have **{bal}** chips in your available wallet. (Leave the table first to cash out seated chips!)",
                ephemeral=True);
            return

        ok = await db.request_cashout(interaction.user.id, chips)
        if not ok:
            await interaction.followup.send("❌ Failed to process cashout.", ephemeral=True);
            return

        cashout_ch_id = os.getenv("CASHOUT_CHANNEL_ID")
        if cashout_ch_id:
            try:
                ch = interaction.guild.get_channel(int(cashout_ch_id))
                if ch:
                    ticket_msg = f"**Username:** {interaction.user.mention}\n**Amount:** {chips} <:poker_chip:1488128491881758760>"
                    if note: ticket_msg += f"\n**Notes:** {note}"
                    await ch.send(ticket_msg)
            except Exception:
                pass

        # FIXED: Send the final receipt ephemerally
        await interaction.followup.send(
            f"✅ Locked **{chips}** <:poker_chip:1488128491881758760> for cashout. Staff have been notified in the cashouts channel.",
            ephemeral=True
        )

    @pokermgr.command(name="pay_cashout", description="[Manager] Deduct paid chips from pending and send receipt")
    @app_commands.describe(user="Player who was paid", amount="Amount of chips paid")
    async def pay_cashout(self, interaction: discord.Interaction, user: discord.Member, amount: int):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        cashout_ch_id_str = os.getenv("CASHOUT_CHANNEL_ID")
        if cashout_ch_id_str:
            cashout_ch_id = int(cashout_ch_id_str)
            if interaction.channel_id != cashout_ch_id:
                await interaction.followup.send(f"❌ This command can only be used in <#{cashout_ch_id}>.",
                                                ephemeral=True)
                return

        if amount <= 0:
            await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)
            return

        ok = await db.pay_cashout(user.id, amount)
        if not ok:
            _, pending = await db.get_wallet(user.id)
            await interaction.followup.send(
                f"❌ **{user.display_name}** only has **{pending}** <:poker_chip:1488128491881758760> pending. You cannot deduct {amount}.",
                ephemeral=True);
            return

        await interaction.followup.send(
            f"✅ Successfully deducted **{amount}** <:poker_chip:1488128491881758760> from **{user.mention}**'s pending cashouts.")

    @pokeradmin.command(name="economy", description="[Admin] View total chips in circulation")
    async def economy(self, interaction: discord.Interaction):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_ID):
            await interaction.response.send_message("❌ Server Administrators only.", ephemeral=True);
            return
        await interaction.response.defer(ephemeral=False)

        avail, pending = await db.get_economy_totals()

        in_play = 0
        for key, t in tables.items():
            if key[0] == interaction.guild_id:
                for p in t.game.players + t.game.pending_joins:
                    in_play += p.chips + p.pending_rebuy

        total = avail + pending + in_play

        embed = discord.Embed(title="🏦 Casino Economy Dashboard", color=0x2ecc71)
        embed.add_field(name="Available in Wallets", value=f"{avail:,} <:poker_chip:1488128491881758760>", inline=False)
        embed.add_field(name="Locked Pending Cashouts", value=f"{pending:,} <:poker_chip:1488128491881758760>", inline=False)
        embed.add_field(name="Currently at Tables", value=f"{in_play:,} <:poker_chip:1488128491881758760>", inline=False)
        embed.add_field(name="Total Circulation", value=f"**{total:,} <:poker_chip:1488128491881758760>**", inline=False)

        await interaction.followup.send(embed=embed)

    @pokeradmin.command(name="revenue", description="[Admin] View projected house profits")
    async def revenue(self, interaction: discord.Interaction):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_ID):
            await interaction.response.send_message("❌ Server Administrators only.", ephemeral=True);
            return
        await interaction.response.defer(ephemeral=False)

        stats = await db.get_revenue_stats()

        embed = discord.Embed(title="📈 House Revenue (5% Tax)", color=0xf1c40f)
        embed.add_field(name="Past 24 Hours", value=f"{stats['daily']:,} <:poker_chip:1488128491881758760>", inline=True)
        embed.add_field(name="Past 7 Days", value=f"{stats['weekly']:,} <:poker_chip:1488128491881758760>", inline=True)
        embed.add_field(name="Past 30 Days", value=f"{stats['monthly']:,} <:poker_chip:1488128491881758760>", inline=True)
        embed.add_field(name="All-Time Profit", value=f"**{stats['all_time']:,} <:poker_chip:1488128491881758760>**", inline=False)

        await interaction.followup.send(embed=embed)

    @pokeradmin.command(name="adjustrevenue", description="[Admin] Manually adjust all-time revenue tracker")
    @app_commands.describe(amount="Amount to add (or negative to subtract)")
    async def adjustrevenue(self, interaction: discord.Interaction, amount: int):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Server Administrators only.", ephemeral=True);
            return
        await interaction.response.defer(ephemeral=False)

        db_conn = await db._get_db()
        async with db._write_lock:
            await db_conn.execute("INSERT INTO house_revenue (ts, amount) VALUES (?, ?)",
                                  (datetime.utcnow().isoformat(), amount))
            await db_conn.commit()

        word = "Added" if amount >= 0 else "Deducted"
        await interaction.followup.send(
            f"✅ {word} **{abs(amount)}** <:poker_chip:1488128491881758760> to the House Revenue tracker.")

    @pokeradmin.command(name="adjustjackpot", description="[Admin] Manually adjust the global jackpot")
    @app_commands.describe(amount="Amount to add (or negative to subtract)")
    async def adjustjackpot(self, interaction: discord.Interaction, amount: int):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=False)

        # Pass the amount directly (positive adds, negative subtracts)
        await db.adjust_jackpot(amount)
        new_jp = await db.get_jackpot()

        # Smart formatting for the receipt
        action = "Added" if amount >= 0 else "Removed"
        prep = "to" if amount >= 0 else "from"

        await interaction.followup.send(
            f"✅ {action} **{abs(amount):,}** <:poker_chip:1488128491881758760> {prep} the jackpot! New total: **{new_jp:,}** <:poker_chip:1488128491881758760>"
        )

    @poker.command(name="jackpot", description="View the current casino jackpot!")
    async def jackpot(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False)
        jp = await db.get_jackpot()

        # 🚨 Calculate splits: 50% Royal Flush, 20% Straight Flush, 5% Rest to Quads
        rf_cut = math.ceil(jp * 0.60)
        sf_cut = math.ceil(jp * 0.20)
        quads_cut = math.ceil(jp * 0.05)  # The rest goes to Quads

        desc = (
            "_ _\n"
            f"**Total:  {jp:,} <:poker_chip:1488128491881758760>**\n\n"
            f"- **Quads** : {quads_cut:,} <:poker_chip:1488128491881758760>\n\n"
            f"- **Straight Flush** : {sf_cut:,} <:poker_chip:1488128491881758760>\n\n"
            f"- **Royal Flush** : {rf_cut:,} <:poker_chip:1488128491881758760>\n\n"
            "_ _"
        )

        embed = discord.Embed(
            title="<a:md_den:996127219019690034> Jackpot",
            description=desc,
            color=0xFFD700  # Decimal 16766720
        )
        embed.set_thumbnail(
            url="https://media.discordapp.net/attachments/1478125269285081211/1488098208986038282/3d-casino-poker-cards-and-playing-chips-on-black-background-illustration-free-vector.png?ex=69cb8af4&is=69ca3974&hm=58f")
        embed.set_footer(text="• 5% Q, 20% SF, 60% RF")

        await interaction.followup.send(embed=embed)

    @pokeradmin.command(name="check_inactive", description="[Admin] Check who will be wiped soon")
    async def check_inactive(self, interaction: discord.Interaction):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_ID):
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)

        # 🚨 FIXED: Actually fetch the at-risk list instead of forcing it to []
        at_risk = await db.get_players_at_risk()
        inactive = await db.get_inactive_players()

        embed = discord.Embed(title="🔍 Inactivity Report", color=0xe74c3c)

        if at_risk:
            risk_lines = []
            for p in at_risk[:10]:  # Show top 10
                days_ago = p.get("days_inactive", 0)
                total = p["balance"] + p["pending_cashout"]
                risk_lines.append(
                    f"• **{p['username']}**: {total} chips ({days_ago}d ago, {p['recent_hands']} hands)"
                )
            embed.add_field(
                name=f"⚠️ At Risk - Wiping in <24h ({len(at_risk)} players)",
                value="\n".join(risk_lines) if risk_lines else "None",
                inline=False
            )

        if inactive:
            inactive_lines = []
            for p in inactive[:10]:
                raw_date = p["last_activity"]
                days_ago = (datetime.utcnow() - datetime.fromisoformat(raw_date)).days if isinstance(raw_date,
                                                                                                     str) else 0
                total = p["balance"] + p["pending_cashout"]
                inactive_lines.append(
                    f"• **{p['username']}**: {total} chips ({days_ago}d ago, {p['recent_hands']} hands)"
                )
            embed.add_field(
                name=f"💀 Will Be Wiped Next Run ({len(inactive)} players)",
                value="\n".join(inactive_lines) if inactive_lines else "None",
                inline=False
            )

        if not at_risk and not inactive:
            embed.description = "✅ All players are active! No chips will be wiped."

        await interaction.followup.send(embed=embed)


    @pokeradmin.command(name="force_wipe_inactive_players", description="[Admin] Manually trigger inactivity wipe NOW")
    async def force_wipe(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=False)

        wiped = await db.wipe_inactive_players()
        
        if not wiped:
            await interaction.followup.send("✅ No inactive players found. Nothing to wipe!")
            return
        
        summary = "\n".join([
            f"• **{w['username']}**: {w['amount_wiped']} chips (hands: {w['recent_hands']}, wagered: {w['recent_chips_wagered']})"
            for w in wiped[:20]  # Show first 20
        ])
        
        await interaction.followup.send(
            f"🧹 **Wiped {len(wiped)} inactive player(s):**\n{summary}"
        )


    @poker.command(name="myactivity", description="Check your activity status and see if you're at risk")
    async def myactivity(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)


        stats = await db.get_player_activity_stats(interaction.user.id)
        
        if not stats:
            await interaction.followup.send("❌ You don't have a wallet yet! Use `/wallet` to get started.", ephemeral=True)
            return

        # Calculate wipe timestamp
        raw = stats['last_activity']
        last_active = datetime.fromisoformat(raw).replace(tzinfo=_tz.utc) if isinstance(raw, str) else raw

        # 1. Find exactly when their 2-day clock runs out
        exact_expiration = last_active + timedelta(days=db.INACTIVITY_DAYS)

        # 2. Snap to the NEXT scheduled bot wipe (03:30 UTC)
        if exact_expiration.hour < 3 or (exact_expiration.hour == 3 and exact_expiration.minute <= 30):
            # The exact expiration happens before 3:30 AM today, so they get wiped today
            wipe_date = exact_expiration.replace(hour=3, minute=30, second=0, microsecond=0)
        else:
            # The exact expiration happens after 3:30 AM, so they survive until tomorrow's wipe
            wipe_date = (exact_expiration + timedelta(days=1)).replace(hour=3, minute=30, second=0, microsecond=0)

        wipe_timestamp = int(wipe_date.timestamp())
        
        # Build embed
        embed = discord.Embed(title=f"📊 Activity Status: {stats['username']}", color=0x3498db)
        
        # Basic Info with Discord Timestamps
        total_chips = stats['balance'] + stats['pending_cashout']
        embed.add_field(name="💰 Total Chips", value=f"{total_chips:,} chips", inline=True)
        embed.add_field(name="📅 Last Active", value=f"<t:{int(last_active.timestamp())}:R>", inline=True)
        
        # Wipe deadline with Discord timestamp
        if stats['days_until_wipe'] > 0:
            embed.add_field(
                name="⏰ Chips Wiped", 
                value=f"<t:{wipe_timestamp}:R>", 
                inline=True
            )
        else:
            embed.add_field(
                name="⏰ Chips Wiped", 
                value="**Next cleanup run!**", 
                inline=True
            )
        
        # Progress Bar Helper Function
        def progress_bar(current: int, required: int, length: int = 10) -> str:
            filled = min(int((current / max(required, 1)) * length), length)
            done   = "🟩" * filled
            empty  = "⬜" * (length - filled)
            pct    = min(int((current / max(required, 1)) * 100), 100)
            return f"{done}{empty}  **{current}/{required}** ({pct}%)"
        
        # Hands Progress with Visual Bar
        hands_bar = progress_bar(stats['recent_hands'], db.MIN_HANDS_PER_PERIOD)
        hands_status = "✅" if stats['meets_hand_requirement'] else "❌"
        embed.add_field(
            name=f"🃏 Hands Played {hands_status}", 
            value=hands_bar, 
            inline=False
        )
        
        # Chips Wagered Progress (if enabled)
        if db.MIN_CHIPS_WAGERED > 0:
            chips_bar = progress_bar(stats['recent_chips_wagered'], db.MIN_CHIPS_WAGERED)
            chips_status = "✅" if stats['meets_wager_requirement'] else "❌"
            embed.add_field(
                name=f"💵 Chips Wagered {chips_status}", 
                value=chips_bar, 
                inline=False
            )

            # Status with Dynamic Color
            days_left = stats['days_until_wipe']

            # Override: If they did their homework, they are safe!
            if stats['meets_hand_requirement'] and (db.MIN_CHIPS_WAGERED == 0 or stats['meets_wager_requirement']):
                status = "🟢 **SAFE** - Requirements met!"
                color = 0x2ecc71  # Green
                action = "You are fully protected from the next wipe."
            elif days_left >= 2:
                status = "🟢 **SAFE** - You have time."
                color = 0x2ecc71  # Green
                needed = db.MIN_HANDS_PER_PERIOD - stats['recent_hands']
                action = f"Play {needed} more hand(s) before the deadline."
            elif days_left == 1:
                status = "🟡 **WARNING** - 1 day left!"
                color = 0xf39c12  # Yellow
                needed = db.MIN_HANDS_PER_PERIOD - stats['recent_hands']
                action = f"**Play {needed} more hand(s) TODAY!**"
            else:
                status = "🔴 **CRITICAL** - Wipe imminent!"
                color = 0xe74c3c  # Red
                needed = db.MIN_HANDS_PER_PERIOD - stats['recent_hands']
                action = f"**Play {needed} more hand(s) IMMEDIATELY!**"

            embed.color = color
            embed.add_field(name="📈 Status", value=status, inline=False)

            # Clear action needed (if requirements not met)
            if not stats['meets_hand_requirement'] or (db.MIN_CHIPS_WAGERED > 0 and not stats['meets_wager_requirement']):
                embed.add_field(name="🎯 What You Need", value=action, inline=False)
        
        # Footer with helpful reminder
        embed.set_footer(text=f"Requirements reset every {db.INACTIVITY_DAYS} days. Run this command after playing to see updates!")
        
        await interaction.followup.send(embed=embed, ephemeral=True)



    # ── Titles & Win Messages ──────────────────────────────────────────────────

    @poker.command(name="titles", description="View and equip your unlocked titles and win messages")
    async def titles_cmd(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        cosmetics = await db.get_cosmetics(interaction.user.id)
        embed, view = _build_cosmetics_embed_and_view(interaction.user.id, cosmetics)
        view.message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    @poker.command(name="equiptitle", description="Equip one of your unlocked titles")
    @app_commands.describe(title_id="Your unlocked title — pick from the list")
    @app_commands.autocomplete(title_id=_autocomplete_title)
    async def equiptitle(self, interaction: discord.Interaction, title_id: str):
        await interaction.response.defer(ephemeral=True)
        if title_id == "none":
            await db.set_active_title(interaction.user.id, None)
            await interaction.followup.send("✅ Title removed.", ephemeral=True)
            return
        if title_id not in db.TITLES:
            await interaction.followup.send("❌ Unknown title. Use `/poker titles` to see your options.", ephemeral=True)
            return
        ok = await db.set_active_title(interaction.user.id, title_id)
        if not ok:
            info = db.TITLES[title_id]
            await interaction.followup.send(
                f"❌ You haven't unlocked **{info['display']}** yet.\n*{info['description']}*", ephemeral=True)
            return
        await interaction.followup.send(f"✅ Title set to **{db.TITLES[title_id]['display']}**!", ephemeral=True)

    @poker.command(name="equipwinmsg", description="Equip one of your unlocked win messages")
    @app_commands.describe(msg_id="Your unlocked win message — pick from the list")
    @app_commands.autocomplete(msg_id=_autocomplete_winmsg)
    async def equipwinmsg(self, interaction: discord.Interaction, msg_id: str):
        await interaction.response.defer(ephemeral=True)
        if msg_id == "none":
            await db.set_active_win_msg(interaction.user.id, None)
            await interaction.followup.send("✅ Win message removed.", ephemeral=True)
            return
        if msg_id not in db.WIN_MESSAGES:
            await interaction.followup.send("❌ Unknown win message. Use `/poker titles` to see your options.", ephemeral=True)
            return
        ok = await db.set_active_win_msg(interaction.user.id, msg_id)
        if not ok:
            info = db.WIN_MESSAGES[msg_id]
            desc = info['description'] if info['rarity'] != 'legendary' else "???"
            await interaction.followup.send(
                f"❌ You haven't unlocked **{info['display']}** yet.\n*{desc}*", ephemeral=True)
            return
        await interaction.followup.send(f"✅ Win message set to **{db.WIN_MESSAGES[msg_id]['display']}**!", ephemeral=True)

    @pokeradmin.command(name="grant_cosmetic", description="[Admin] Grant a title or win message to any player")
    @app_commands.describe(user="The player to receive the cosmetic", kind="Type of cosmetic",
                           cosmetic_id="Search for the cosmetic")
    @app_commands.choices(kind=[
        app_commands.Choice(name="Title", value="title"),
        app_commands.Choice(name="Win Message", value="winmsg"),
    ])
    @app_commands.autocomplete(cosmetic_id=_autocomplete_grant_cosmetic)
    async def grant_cosmetic(self, interaction: discord.Interaction, user: discord.Member, kind: str, cosmetic_id: str):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_ID):
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        k = kind.strip().lower()
        if k not in ("title", "winmsg"):
            await interaction.followup.send("❌ `kind` must be `title` or `winmsg`.", ephemeral=True); return
        catalog = db.TITLES if k == "title" else db.WIN_MESSAGES
        cid = cosmetic_id.strip().lower()
        if cid not in catalog:
            valid = ", ".join(f"`{x}`" for x in catalog)
            await interaction.followup.send(f"❌ Unknown ID `{cid}`.\nValid: {valid}", ephemeral=True); return
        newly = await db.unlock_cosmetic(user.id, k, cid)
        display = catalog[cid]["display"]
        cmd = "equiptitle" if k == "title" else "equipwinmsg"
        if newly:
            await interaction.followup.send(
                f"✅ Granted **{display}** to {user.mention}.\nThey can equip it with `/poker {cmd}`", ephemeral=True)
        else:
            await interaction.followup.send(f"ℹ️ {user.mention} already owns **{display}**.", ephemeral=True)

    @pokeradmin.command(name="makecustom", description="[Admin] Create a custom title or win message")
    @app_commands.describe(
        kind="'title' or 'winmsg'",
        cosmetic_id="Unique ID",
        display="Display text",
        description="Optional description",
        rarity="Rarity level",
        hidden="If true, only visible to users who own it"
    )
    @app_commands.choices(
        kind=[
            app_commands.Choice(name="Title", value="title"),
            app_commands.Choice(name="Win Message", value="winmsg"),
        ],
        rarity=[
            app_commands.Choice(name="Common", value="common"),
            app_commands.Choice(name="Uncommon", value="uncommon"),
            app_commands.Choice(name="Rare", value="rare"),
            app_commands.Choice(name="Legendary", value="legendary"),
            app_commands.Choice(name="Unique", value="unique"),
        ]
    )
    async def makecustom(
        self, 
        interaction: discord.Interaction, 
        kind: str,
        cosmetic_id: str,
        display: str,
        description: str = "",
        rarity: str = "rare",
        hidden: bool = False
    ):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_ID):
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        
        k = kind.strip().lower()
        if k not in ("title", "winmsg"):
            await interaction.followup.send("❌ `kind` must be `title` or `winmsg`.", ephemeral=True)
            return
        
        # Sanitize cosmetic_id (lowercase, replace spaces with underscores)
        cid = cosmetic_id.strip().lower().replace(" ", "_")
        
        # Check if ID already exists
        catalog = db.TITLES if k == "title" else db.WIN_MESSAGES
        if cid in catalog:
            await interaction.followup.send(f"❌ ID `{cid}` already exists. Choose a different ID.", ephemeral=True)
            return
        
        # Create the custom cosmetic
        success = await db.create_custom_cosmetic(k, cid, display, description, rarity, hidden)
        
        if success:
            visibility = "🔒 Hidden (event prize)" if hidden else "👁️ Visible to all"
            await interaction.followup.send(
                f"✅ Created custom {k}: `{display}` (`{cid}`)\n"
                f"Rarity: {db.RARITY_LABEL.get(rarity, rarity)}\n"
                f"Visibility: {visibility}\n\n"
                f"Use `/pokeradmin grant_cosmetic` to give it to players.",
                ephemeral=True
            )
        else:
            await interaction.followup.send(f"❌ Failed to create cosmetic.", ephemeral=True)

    @poker.command(name="tipleaders", description="Top generous players by total chips tipped")
    async def tipleaders(self, interaction: discord.Interaction):
        await interaction.response.defer()

        rows = await db.get_tip_leaderboard(10)
        caller_id = interaction.user.id
        caller_row = await db.get_player_stats(caller_id)

        if not rows:
            await interaction.followup.send("No tips recorded yet! Be the first to tip the dealer!", ephemeral=True)
            return

        MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
        top_ids = {r['user_id'] for r in rows}

        table_lines = ["```"]
        table_lines.append(f"{'':4}{'Player':<18} {'Tipped':>12}")
        table_lines.append("─" * 36)

        for i, r in enumerate(rows):
            rank = i + 1
            uname = r['username'][:17]
            medal = MEDALS.get(rank, f"{rank}. ")
            you_tag = " ◀" if r['user_id'] == caller_id else ""
            table_lines.append(f"{medal:<4}{uname:<18} {r['total_tipped']:>12,}{you_tag}")
        table_lines.append("```")

        embed = discord.Embed(
            title="💸 Top Tippers Leaderboard",
            description="\n".join(table_lines),
            color=0xE91E63  # Magenta color for tips
        )

        # Show the caller's tip stats at the bottom
        if caller_row:
            caller_tipped = caller_row.get('total_tipped', 0)
            in_top = caller_id in top_ids
            label = f"📊 Your Generosity" + (" *(in top 10)*" if in_top else "")
            embed.add_field(
                name=label,
                value=f"Total Tipped **{caller_tipped:,}** <:poker_chip:1488128491881758760>",
                inline=False
            )
        else:
            embed.add_field(name="📊 Your Generosity", value="No tips yet.", inline=False)

        await interaction.followup.send(embed=embed)


    @pokeradmin.command(name="backup", description="[Dev] Force a database backup to your DMs")
    async def force_backup(self, interaction: discord.Interaction):
        # Ironclad Security: Only YOU can run this
        if interaction.user.id not in self.DEV_USER_ID:
            await interaction.response.send_message("❌ This command is restricted to the bot developer.",
                                                    ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            await self._send_backup(interaction.user)
            await interaction.followup.send("✅ Backup sent directly to your DMs!", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send(
                "❌ Failed to send DM. Please check your Discord privacy settings to allow messages from server members.",
                ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Backup failed: {e}", ephemeral=True)

    @poker.command(name="testcards", description="[Dev] Generate a random 2-card hand to test image sizes")
    async def test_cards(self, interaction: discord.Interaction):
        if interaction.user.id not in self.DEV_USER_ID:
            await interaction.response.send_message("❌ This command is restricted to the bot developers.",
                                                    ephemeral=True)
            return

        # 1. Defer so the bot has time to process the image
        await interaction.response.defer(ephemeral=False)

        from treys import Deck

        # 2. Draw 2 random cards
        deck = Deck()
        cards = deck.draw(2)

        # 3. Stitch them using your image settings
        # (Using asyncio.to_thread just like your real bot does to prevent lag)
        file = await asyncio.to_thread(card_images.make_strip, cards, 0, True)

        # 4. Send the result!
        await interaction.followup.send(f"🃏 Test Hand: {hand_str(cards)}", file=file)

async def setup(bot):
    await bot.add_cog(PokerCog(bot))