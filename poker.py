"""poker.py — Texas Hold'em bot"""

import discord
from discord import app_commands
from discord.ext import commands
from engine import PokerGame, Street, hand_str
import database as db
from treys import Evaluator
import card_images
import os, asyncio, uuid
from datetime import datetime
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

# ── Message counter ───────────────────────────────────────────────────────────

async def on_channel_message(message: discord.Message):
    key = (message.guild.id, message.channel.id)
    t   = get_table(key)
    if not t or t.game.street == Street.WAITING:
        return
    t.msg_count += 1
    settings = await db.get_settings(message.guild.id)
    threshold = settings.get("resend_after_msgs", TABLE_RESEND_MSGS)
    if t.msg_count >= threshold:
        t.msg_count = 0
        t.hand_msg  = None
        await refresh(message.channel, t, new_hand=True)

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

    try:
        view = BetweenHandsView(t)
        t.between_msg = await channel.send(f"⏳ Next hand starting in **{delay}s**...", view=view)
    except Exception:
        pass

    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        return

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
                    f"🚪 **{p.display_name}** has been removed — stack (**{p.chips}** 🪙) is below the big blind (**{bb}** 🪙). Chips returned to wallet.")
            except Exception:
                pass

    active = [p for p in t.game.players if (p.chips + p.pending_rebuy) >= bb and p.user_id not in t.game.pending_leaves]
    pending_with_chips = [p for p in t.game.pending_joins if (p.chips + p.pending_rebuy) >= bb]
    total = len(active) + len(pending_with_chips)

    if total < 2:
        await refresh(channel, t)  # <--- ADD THIS so the embed updates before sleeping!
        await channel.send("⚠️ Not enough players for another hand. Waiting for a Manager to `/poker start`.")
        return

    t.game.SMALL_BLIND = settings["small_blind"]
    t.game.BIG_BLIND   = settings["big_blind"]

    slog_clear(t)
    success, msg = t.game.start_hand()
    slog(t, msg)

    if not success:
        await channel.send(f"⚠️ Could not start next hand: {msg}")
        return

    t.msg_count = 0
    await refresh(channel, t, new_hand=True)

async def _close_table(channel, t: TableState):
    t.closing = True
    key = (channel.guild.id, channel.id)
    cancel_timer(t)
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
        for thread in log_ch.threads:
            if thread.name == "Poker Hand Log":
                _log_threads[channel.guild.id] = thread
                return thread
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
    lines = [f"Hand #{game.hand_num} | Table: {t.name} ({t.id}) | Pot: {result.pot}"]

    # Helper: get "username (user_id)" for a user_id
    async def uid_str(uid):
        p = game.get_player(uid)
        uname = p.display_name if p else "Unknown"
        return f"{uname} ({uid})"

    from engine import hand_str
    if hasattr(result, 'community') and result.community:
        lines.append(f"Board: {hand_str(result.community)}")

    pot_results = result.pot_results or []
    ranks = result.winner_ranks or {}
    for p in t.game.players:
        delta = result.chip_deltas.get(p.user_id, 0)
        sign = "+" if delta >= 0 else ""
        ustr = await uid_str(p.user_id)
        rank = ranks.get(p.user_id)
        from engine import hand_str as hs
        cards = hs(p.hole_cards) if p.hole_cards else "folded"
        rank_part = f" [{rank}]" if rank else ""
        lines.append(f"  {ustr}: {cards}{rank_part}  {sign}{delta}")

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

def player_line(p, game: PokerGame, idx: int) -> str:
    tag     = " 🎰" if idx == game.dealer_idx else ""
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

    lines = [player_line(p, game, i) for i, p in enumerate(game.players)]
    for p in game.pending_joins:
        lines.append(f"<@{p.user_id}> **{p.chips} 🪙** — ⏳ next hand")
    if lines:
        embed.add_field(name=f"Players ({len(game.players)}/8)", value="\n".join(lines), inline=False)

    if t.street_log:
        embed.add_field(name="This round", value="\n".join(t.street_log[-8:]), inline=False)

    # Pot / turn as last field — sits right above the board image
    if game.street not in (Street.WAITING,):
        pot_line = f"**Pot:** {game.pot} 🪙"
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

async def refresh(channel, t: TableState, new_hand: bool = False):
    await update_board(t)          # generate File (sync, no upload needed)
    embed = build_embed(t)   # sets attachment://board.png if file present
    view  = GameView(t)
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
        from engine import hand_str
        slog(t, f"🃏 Board: {hand_str(result.community)}")

    if not pot_results or len(pot_results) == 1:
        if len(result.winners) == 1:
            w      = result.winners[0]
            gained = result.chip_deltas.get(w.user_id, 0)
            rank   = ranks.get(w.user_id)
            rs     = f" ({rank})" if rank else ""
            slog(t, f"🏆 **{w.display_name}** won **+{gained}** 🪙{rs}")
        else:
            split = result.pot // max(len(result.winners), 1)
            names = ", ".join(f"**{w.display_name}**" for w in result.winners)
            slog(t, f"🤝 Split: {names} each **+{split}** 🪙")
    else:
        for i, (amt, winners) in enumerate(pot_results):
            label = "Main" if i == 0 else f"Side {i}"
            if len(winners) == 1:
                w      = winners[0]
                rank   = ranks.get(w.user_id)
                rs     = f" ({rank})" if rank else ""
                slog(t, f"🏆 **{label}** ({amt}🪙) → **{w.display_name}**{rs}")
            else:
                each  = amt // len(winners)
                names = ", ".join(f"**{w.display_name}**" for w in winners)
                slog(t, f"🤝 **{label}** ({amt}🪙) split → {names} ({each} each)")

async def _announce_winner(channel, t: TableState, result):
    game        = t.game
    ranks       = result.winner_ranks or {}
    pot_results = result.pot_results  # [(amount, [PokerPlayer, ...]), ...]

    # Board line — present whenever there were community cards (fold wins mid-street included)
    board_str = f"\n🃏 Board: {hand_str(result.community)}" if result.community else ""

    if not pot_results or len(pot_results) == 1:
        # Single pot (or fold win — no pot_results)
        if len(result.winners) == 1:
            w      = result.winners[0]
            gained = result.chip_deltas.get(w.user_id, 0)
            rank   = ranks.get(w.user_id)
            rs     = f" with **{rank}**" if rank else ""
            await channel.send(
                f"🏆 **{w.display_name}** won **+{gained}** chips from Hand #{game.hand_num}{rs}!{board_str} "
                f"(Pot: **{result.pot}** 🪙 | Stack: **{w.chips}** 🪙)"
            )
        else:
            # True split
            parts = []
            for amt, winners in (pot_results or []):
                each = amt // len(winners)
                for w in winners:
                    rank = ranks.get(w.user_id)
                    parts.append(f"**{w.display_name}**" + (f" ({rank})" if rank else ""))
            if not parts:
                parts = [f"**{w.display_name}**" for w in result.winners]
            split = result.pot // len(result.winners)
            await channel.send(
                f"🤝 Split pot — Hand #{game.hand_num}: {', '.join(parts)} each won **{split}** 🪙{board_str} "
                f"(Pot: **{result.pot}** 🪙)"
            )
    else:
        # Multiple side pots — use exact pot_results from engine (guaranteed correct)
        lines = [f"🃏 **Hand #{game.hand_num} results** — {len(pot_results)} pots:"]
        if result.community:
            lines.append(f"🃏 Board: {hand_str(result.community)}")
        for i, (amt, winners) in enumerate(pot_results):
            label = "Main pot" if i == 0 else f"Side pot {i}"
            icon  = "🏆" if i == 0 else "🥈"
            if len(winners) == 1:
                w    = winners[0]
                rank = ranks.get(w.user_id)
                rs   = f" ({rank})" if rank else ""
                gained = result.chip_deltas.get(w.user_id, 0)
                lines.append(f"  {icon} **{label}** ({amt} 🪙) → **{w.display_name}**{rs}")
            else:
                each  = amt // len(winners)
                parts = []
                for w in winners:
                    rank = ranks.get(w.user_id)
                    parts.append(f"**{w.display_name}**" + (f" ({rank})" if rank else ""))
                lines.append(f"  🤝 **{label}** ({amt} 🪙) split → {', '.join(parts)} ({each} 🪙 each)")
        lines.append("")
        # Per-winner final stacks
        seen = set()
        for _, winners in pot_results:
            for w in winners:
                if w.user_id not in seen:
                    seen.add(w.user_id)
                    gained = result.chip_deltas.get(w.user_id, 0)
                    lines.append(f"  💰 **{w.display_name}**: +{gained} → **{w.chips}** 🪙")
        await channel.send("\n".join(lines))

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

    # Stats + DB — wrapped individually so a failure doesn't kill the announcement
    try:
        for p in t.game.players:
            net = result.chip_deltas.get(p.user_id, 0)
            won = any(w.user_id == p.user_id for w in result.winners)

            # ZERO LAG: Stop fetching from Discord.
            # p.display_name now securely holds their permanent username!
            await db.record_hand(p.user_id, p.display_name, won, net)

    except Exception as e:
        print(f"[poker] record_hand error: {e}")

    try:
        for p in t.game.players:
            total_chips = p.chips + p.pending_rebuy  # <--- FIX: Include queued chips
            if total_chips > 0:
                await db.update_chips_in_play(p.user_id, total_chips)
            else:
                await db.clear_chips_in_play(p.user_id)
    except Exception as e:
        print(f"[poker] chips_in_play error: {e}")

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

    try:
        await db.log_hand(guild.id, t.id, t.name, t.game.hand_num, result.summary)
    except Exception as e:
        print(f"[poker] log_hand error: {e}")

    try:
        await post_hand_log(channel, t, result)
    except Exception as e:
        print(f"[poker] post_hand_log error: {e}")

    # These must always run — winner announcement + embed update
    try:
        await _announce_winner(channel, t, result)
    except Exception as e:
        print(f"[poker] _announce_winner error: {e}")
    try:
        _slog_result(t, result)
        await refresh(channel, t)
    except Exception as e:
        print(f"[poker] refresh error: {e}")
    if t.closing:
        await _close_table(channel, t)
    else:
        if result.showdown_players:
            await _reveal_phase(channel, t, result)

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
        g = self.t.game
        p = g.get_player(interaction.user.id)
        if not p: await interaction.response.send_message("❌ Not your turn.", ephemeral=True); return
        amount = max(g.call_amount(p), g.pot // 3)
        await self._do_raise(interaction, amount)

    @discord.ui.button(label="1/2 Pot", style=discord.ButtonStyle.green, row=0)
    async def half_pot(self, interaction: discord.Interaction, button: discord.ui.Button):
        g = self.t.game
        p = g.get_player(interaction.user.id)
        if not p: await interaction.response.send_message("❌ Not your turn.", ephemeral=True); return
        amount = max(g.call_amount(p), g.pot // 2)
        await self._do_raise(interaction, amount)

    @discord.ui.button(label="1/2 Stack", style=discord.ButtonStyle.blurple, row=0)
    async def half_stack(self, interaction: discord.Interaction, button: discord.ui.Button):
        g = self.t.game
        p = g.get_player(interaction.user.id)
        if not p: await interaction.response.send_message("❌ Not your turn.", ephemeral=True); return
        amount = max(g.call_amount(p), p.chips // 2)
        await self._do_raise(interaction, amount)

    @discord.ui.button(label="All In 🚀", style=discord.ButtonStyle.red, row=0)
    async def all_in(self, interaction: discord.Interaction, button: discord.ui.Button):
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

        await interaction.channel.send(f"✅ **{interaction.user.name}** joined the table with **{chips}** 🪙!")
        await refresh(interaction.channel, t)
        await interaction.followup.send("✅ Successfully joined!", ephemeral=True)

# ── Game View ─────────────────────────────────────────────────────────────────

class GameView(discord.ui.View):
    def __init__(self, t: TableState):
        super().__init__(timeout=None)
        self.t = t
        in_hand = t.game.street not in (Street.WAITING, Street.SHOWDOWN)
        table_full = (len(t.game.players) + len(t.game.pending_joins)) >= 8
        self.btn_join.disabled = in_hand or table_full or t.closing

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
            f"**Raise options** — Pot: {g.pot} 🪙  |  Call: {call_amt}  |  Stack: {p.chips if p else '?'}\n"
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

        caption = f"Your hole cards — {p.chips} 🪙 at table{strength}\n**Cards:** {hand_str(p.hole_cards)}"

        # 1. INSTANTLY send the text so players with slow internet see their cards immediately
        await interaction.response.send_message(caption, ephemeral=True)

        # 2. Generate the heavy image and patch it in a second later
        if USE_IMAGES:
            try:
                file = await asyncio.to_thread(card_images.make_strip, p.hole_cards)
                await interaction.edit_original_response(attachments=[file])
            except Exception:
                pass  # If the image fails to upload, they already have the text!

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
        table_str = f"  |  **At table:** {p.chips} 🪙" if p else ""
        pending_str = f"  |  **Pending Cashout:** 🔒 {pending} 🪙" if pending > 0 else ""
        await interaction.followup.send(f"**Your Wallet:** {bal} 🪙{table_str}{pending_str}", ephemeral=True)

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

# ── Cog ───────────────────────────────────────────────────────────────────────

class PokerCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

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
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        for (gid, cid), t in tables.items():
            if gid == interaction.guild_id:
                await interaction.response.send_message(
                    f"❌ A table is already running in <#{cid}>. Close it first.", ephemeral=True); return
        t = TableState(name, interaction.user.id)
        tables[(interaction.guild_id, interaction.channel_id)] = t
        settings = await db.get_settings(interaction.guild_id)
        t.game.SMALL_BLIND = settings["small_blind"]
        t.game.BIG_BLIND   = settings["big_blind"]
        t.game.MIN_BUYIN = settings.get("min_wallet", 50)
        await interaction.response.defer(ephemeral=True)
        await refresh(interaction.channel, t, new_hand=True)
        await interaction.followup.send("✅ Table opened!", ephemeral=True)  # <-- ADD THIS

    @poker.command(name="close", description="[Manager] Close table after current hand")
    async def close_table(self, interaction: discord.Interaction):
        key = (interaction.guild_id, interaction.channel_id)
        t = get_table(key)
        if not t:
            await interaction.response.send_message("❌ No table in this channel.", ephemeral=True);
            return
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return

        # DEFER FIRST
        await interaction.response.defer(ephemeral=False)

        if t.game.street == Street.WAITING:
            # No hand running — close immediately
            await _close_table(interaction.channel, t)
            await interaction.followup.send("✅ Table closed.")
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
            await interaction.followup.send("✅ Table will close after this hand.")
            await refresh(interaction.channel, t)

    @poker.command(name="start", description="[Manager] Deal the first hand")
    async def start(self, interaction: discord.Interaction):
        key = (interaction.guild_id, interaction.channel_id)
        t = get_table(key)
        if not t:
            await interaction.response.send_message("❌ No table here. Use `/poker open` first.", ephemeral=True);
            return
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return
        if t.game.street != Street.WAITING:
            await interaction.response.send_message("❌ A hand is already in progress.", ephemeral=True);
            return

        await interaction.response.defer(ephemeral=True)

        settings = await db.get_settings(interaction.guild_id)
        t.game.SMALL_BLIND = settings["small_blind"]
        t.game.BIG_BLIND   = settings["big_blind"]
        t.game.MIN_BUYIN = settings.get("min_wallet", 50)
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
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return

        # 1. DEFER INSTANTLY
        await interaction.response.defer(ephemeral=False)

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
        if t.game.street == Street.WAITING:
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
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return

        # 1. DEFER FIRST
        await interaction.response.defer(ephemeral=True)

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
                    if t.game.street == Street.WAITING:
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
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return

        # FIXED: Defer publicly
        await interaction.response.defer(ephemeral=False)
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
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return

        # 1. DEFER INSTANTLY
        await interaction.response.defer(ephemeral=False)

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
        table_str = f"\n**At table:** {p.chips} 🪙" if p else ""
        pending_str = f"\n**Pending Cashout:** 🔒 {pending} 🪙" if pending > 0 else ""
        label = f"**{target.display_name}'s Wallet**" if user else "**Your Wallet**"
        await interaction.followup.send(f"{label}: {bal} 🪙{table_str}{pending_str}", ephemeral=False)

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
                    f"Net **{caller_sign}{caller_net}** 🪙  ·  "
                    f"Wallet **{caller_row['wallet']}** 🪙"
                ),
                inline=False
            )
        else:
            embed.add_field(name="📊 Your Stats", value="No hands played yet.", inline=False)

        await interaction.followup.send(embed=embed)

    @pokermgr.command(name="removestats", description="[Manager] Remove a player from the leaderboard")
    @app_commands.describe(user="Player to remove from leaderboard")
    async def remove_stats(self, interaction: discord.Interaction, user: discord.Member):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True); return
        await interaction.response.defer(ephemeral=True)
        removed = await db.delete_player_stats(user.id)
        if removed:
            await interaction.followup.send(f"✅ Removed **{user.name}** ({user.id}) from the leaderboard.")
        else:
            await interaction.followup.send(f"ℹ️ **{user.name}** has no stats on record.", ephemeral=True)

    @poker.command(name="stats", description="View your poker stats")
    async def stats(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        row = await db.get_player_stats(interaction.user.id)
        if not row:
            await interaction.followup.send("No stats yet!", ephemeral=True);
            return
        net = row['net_chips']
        embed = discord.Embed(title=f"Stats — {row['username']}", color=0x2ecc71 if net >= 0 else 0xe74c3c)
        wp = f"{row['hands_won'] / row['hands_played'] * 100:.1f}%" if row['hands_played'] else "—"
        embed.add_field(name="Hands", value=str(row['hands_played']), inline=True)
        embed.add_field(name="Won", value=str(row['hands_won']), inline=True)
        embed.add_field(name="Win %", value=wp, inline=True)
        embed.add_field(name="Net", value=f"{'+' if net >= 0 else ''}{net} 🪙", inline=True)
        embed.add_field(name="Wallet", value=f"{row['wallet']} 🪙", inline=True)

        # 💸 ADD THIS LINE:
        embed.add_field(name="Tipped", value=f"{row.get('total_tipped', 0):,} 🪙", inline=True)

        await interaction.followup.send(embed=embed, ephemeral=True)

    # ── Manager settings commands ─────────────────────────────────────────

    @pokermgr.command(name="addchips", description="[Manager] Add chips to a player's wallet")
    @app_commands.describe(user="Player", amount="Chips to add", note="Optional reason")
    async def mgr_addchips(self, interaction: discord.Interaction, user: discord.Member, amount: int, note: str = ""):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return

        allowed_str = os.getenv("ADD_CHIPS_CHANNELS", "")
        if allowed_str:
            allowed_channels = [int(c.strip()) for c in allowed_str.split(",") if c.strip().isdigit()]
            if allowed_channels and interaction.channel_id not in allowed_channels:
                mentions = ", ".join(f"<#{cid}>" for cid in allowed_channels)
                await interaction.response.send_message(f"❌ This command is restricted to: {mentions}", ephemeral=True)
                return

        if amount <= 0:
            await interaction.response.send_message("❌ Amount must be positive.", ephemeral=True);
            return

        # 1. DEFER PUBLICLY BEFORE DB WRITE
        await interaction.response.defer(ephemeral=False)

        new_bal = await db.add_chips(interaction.user.id, interaction.user.display_name,
                                     user.id, user.display_name, amount, note)

        # 💸 Log 5% Round-Up Revenue immediately!
        tax = math.ceil(amount * 0.05)
        await db.log_revenue(tax)

        # 2. USE FOLLOWUP.SEND
        await interaction.followup.send(
            f"✅ **+{amount}** chips → **{user.mention}** |  Balance: **{new_bal}** 🪙"
            + (f"\n> {note}" if note else ""))

    @pokermgr.command(name="removechips", description="[Manager] Remove chips from a player's wallet")
    @app_commands.describe(user="Player", amount="Chips to remove", note="Optional reason")
    async def mgr_removechips(self, interaction: discord.Interaction, user: discord.Member, amount: int,
                              note: str = ""):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return

        allowed_str = os.getenv("REMOVE_CHIPS_CHANNELS", "")
        if allowed_str:
            allowed_channels = [int(c.strip()) for c in allowed_str.split(",") if c.strip().isdigit()]
            if allowed_channels and interaction.channel_id not in allowed_channels:
                mentions = ", ".join(f"<#{cid}>" for cid in allowed_channels)
                await interaction.response.send_message(f"❌ This command is restricted to: {mentions}", ephemeral=True)
                return

        if amount <= 0:
            await interaction.response.send_message("❌ Amount must be positive.", ephemeral=True);
            return

        # FIXED: Check the player's balance before allowing the removal!
        bal = await db.get_balance(user.id)
        if amount > bal:
            await interaction.response.send_message(
                f"❌ **{user.display_name}** only has **{bal}** 🪙 in their wallet. You cannot remove **{amount}**.",
                ephemeral=True
            )
            return

        # 1. DEFER PUBLICLY BEFORE DB WRITE
        await interaction.response.defer(ephemeral=False)

        new_bal = await db.add_chips(interaction.user.id, interaction.user.display_name,
                                     user.id, user.display_name, -amount, note)

        # 2. USE FOLLOWUP.SEND
        await interaction.followup.send(
            f"✅ **-{amount}** chips from **{user.mention}** |  Balance: **{new_bal}** 🪙"
            + (f"\n> {note}" if note else ""))

    @pokermgr.command(name="bans", description="[Manager] List all currently banned players")
    async def list_bans(self, interaction: discord.Interaction):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return

        # FIXED: Defer publicly
        await interaction.response.defer(ephemeral=False)

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
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return

        await interaction.response.defer(ephemeral=False)
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

        tax = math.ceil(chips * 0.05)
        payout_amount = chips - tax

        ok = await db.request_cashout(interaction.user.id, chips, tax)
        if not ok:
            await interaction.followup.send("❌ Failed to process cashout.", ephemeral=True);
            return

        cashout_ch_id = os.getenv("CASHOUT_CHANNEL_ID")
        if cashout_ch_id:
            try:
                ch = interaction.guild.get_channel(int(cashout_ch_id))
                if ch:
                    ticket_msg = f"**Username:** {interaction.user.mention}\n**Chips Amount:** {payout_amount}\n*(Requested: {chips}, Tax: {tax}🪙)*"
                    if note: ticket_msg += f"\n**Notes:** {note}"
                    await ch.send(ticket_msg)
            except Exception:
                pass

        # FIXED: Send the final receipt ephemerally
        await interaction.followup.send(
            f"✅ Locked **{chips}** 🪙 for cashout (Payout: **{payout_amount}**, Tax: **{tax}**). Staff have been notified in the cashouts channel.",
            ephemeral=True
        )

    @pokermgr.command(name="pay_cashout", description="[Manager] Deduct paid chips from pending and send receipt")
    @app_commands.describe(user="Player who was paid", amount="Amount of chips paid")
    async def pay_cashout(self, interaction: discord.Interaction, user: discord.Member, amount: int):
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True);
            return

        # NEW: Check if the command is being run in the correct cashout channel
        cashout_ch_id_str = os.getenv("CASHOUT_CHANNEL_ID")
        if cashout_ch_id_str:
            cashout_ch_id = int(cashout_ch_id_str)
            if interaction.channel_id != cashout_ch_id:
                await interaction.response.send_message(f"❌ This command can only be used in <#{cashout_ch_id}>.",
                                                        ephemeral=True)
                return

        if amount <= 0:
            await interaction.response.send_message("❌ Amount must be positive.", ephemeral=True);
            return

        await interaction.response.defer(ephemeral=False)

        ok = await db.pay_cashout(user.id, amount)
        if not ok:
            _, pending = await db.get_wallet(user.id)
            await interaction.followup.send(
                f"❌ **{user.display_name}** only has **{pending}** 🪙 pending. You cannot deduct {amount}.",
                ephemeral=True);
            return

        await interaction.followup.send(
            f"✅ Successfully deducted **{amount}** 🪙 from **{user.mention}**'s pending cashouts.")

    @pokeradmin.command(name="economy", description="[Admin] View total chips in circulation")
    async def economy(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
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
        embed.add_field(name="Available in Wallets", value=f"{avail:,} 🪙", inline=False)
        embed.add_field(name="Locked Pending Cashouts", value=f"{pending:,} 🪙", inline=False)
        embed.add_field(name="Currently at Tables", value=f"{in_play:,} 🪙", inline=False)
        embed.add_field(name="Total Circulation", value=f"**{total:,} 🪙**", inline=False)

        await interaction.followup.send(embed=embed)

    @pokeradmin.command(name="revenue", description="[Admin] View projected house profits")
    async def revenue(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Server Administrators only.", ephemeral=True);
            return
        await interaction.response.defer(ephemeral=False)

        stats = await db.get_revenue_stats()

        embed = discord.Embed(title="📈 House Revenue (5% Tax)", color=0xf1c40f)
        embed.add_field(name="Past 24 Hours", value=f"{stats['daily']:,} 🪙", inline=True)
        embed.add_field(name="Past 7 Days", value=f"{stats['weekly']:,} 🪙", inline=True)
        embed.add_field(name="Past 30 Days", value=f"{stats['monthly']:,} 🪙", inline=True)
        embed.add_field(name="All-Time Profit", value=f"**{stats['all_time']:,} 🪙**", inline=False)

        await interaction.followup.send(embed=embed)

    @pokeradmin.command(name="adjustrevenue", description="[Admin] Manually adjust all-time revenue tracker")
    @app_commands.describe(amount="Amount to add (or negative to subtract)")
    async def adjustrevenue(self, interaction: discord.Interaction, amount: int):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Server Administrators only.", ephemeral=True);
            return
        await interaction.response.defer(ephemeral=False)

        await db.log_revenue(amount)
        word = "Added" if amount >= 0 else "Deducted"
        await interaction.followup.send(f"✅ {word} **{abs(amount)}** 🪙 to the House Revenue tracker.")



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
                value=f"Total Tipped **{caller_tipped:,}** 🪙",
                inline=False
            )
        else:
            embed.add_field(name="📊 Your Generosity", value="No tips yet.", inline=False)

        await interaction.followup.send(embed=embed)

async def setup(bot):
    await bot.add_cog(PokerCog(bot))