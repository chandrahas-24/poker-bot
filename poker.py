"""poker.py — Texas Hold'em bot"""

# pyrefly: ignore [missing-import]
import discord
from discord import app_commands
from discord.ext import commands, tasks
from engine import PokerGame, Street, hand_str
import database as db
from treys import Evaluator, Card
import card_images
import os, asyncio, uuid, zipfile, traceback
from datetime import datetime, timedelta, time as dt_time, timezone as _tz
import time
import math
import config
import jackpot
import taxation
from tutorial_cog import TutorialCog
import sys

evaluator  = Evaluator()
USE_IMAGES = card_images.cards_available()

TURN_TIMEOUT_DEFAULT    = config.TURN_TIMEOUT_DEFAULT
NEXT_HAND_DELAY_DEFAULT = config.NEXT_HAND_DELAY_DEFAULT
TABLE_RESEND_MSGS       = config.TABLE_RESEND_MSGS

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
    def __init__(self, name: str, manager_id: int, manager_name: str = "Unknown"):
        self.id           = str(uuid.uuid4())[:8]
        self.name         = name
        self.manager_id   = manager_id
        self.manager_name = manager_name
        self.game         = PokerGame()
        self.is_tournament = False
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
        self.timer_street = None
        self.turn_deadline: float = 0.0
        self.ping_user_id: int | None = None
        self.msg_count = 0
        self.resend_threshold = TABLE_RESEND_MSGS
        self.session_allin_winners: set[int] = set()
        self.rejoin_cooldowns: dict[int, float] = {}
        self.leave_cooldown_pending: set[int] = set()

    @property
    def is_tournament(self) -> bool:
        return getattr(self, "_is_tournament", False)

    @is_tournament.setter
    def is_tournament(self, val: bool):
        self._is_tournament = val
        if hasattr(self, "game") and self.game:
            self.game.is_tournament = val

_old_poker = sys.modules.get('poker')
if _old_poker and hasattr(_old_poker, 'TableState'):
    TableState = _old_poker.TableState

bot_instance = None
for mod_name in ('__main__', 'bot'):
    mod = sys.modules.get(mod_name)
    if mod and hasattr(mod, 'bot'):
        bot_instance = mod.bot
        break

if bot_instance and hasattr(bot_instance, 'poker_tables'):
    tables = bot_instance.poker_tables
else:
    tables = {}

def get_table(key: tuple) -> TableState | None:
    return tables.get(key)

def get_chip_emoji(t_or_game) -> str:
    if t_or_game is None:
        return config.POKER_CHIP_EMOJI
    if isinstance(t_or_game, TableState):
        if getattr(t_or_game, 'is_tournament', False):
            return config.TOURNAMENT_CHIP_EMOJI
        return config.POKER_CHIP_EMOJI
    for t in tables.values():
        if t.game is t_or_game:
            if getattr(t, 'is_tournament', False):
                return config.TOURNAMENT_CHIP_EMOJI
            break
    return config.POKER_CHIP_EMOJI

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

def _task_catcher(task: asyncio.Task):
    """Catches and prints silent errors from background tasks."""
    try:
        task.result()
    except asyncio.CancelledError:
        pass  # Normal behavior when we cancel a timer
    except Exception as e:
        print(f"🚨 [FATAL TABLE ERROR] Background task crashed: {e}")
        import traceback
        traceback.print_exc()

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
    t.timer_street = None
    t.turn_deadline = 0.0

def start_timer(t: TableState, channel):
    cp = t.game.current_player()
    if not cp or t.game.street in (Street.WAITING, Street.SHOWDOWN):
        cancel_timer(t)
        return
    # Same player's timer is already running — leave it completely alone
    if (t.timer_task and not t.timer_task.done()
            and t.timer_user_id == cp.user_id
            and t.timer_street == t.game.street):
        return
    cancel_timer(t)
    t.timer_user_id = cp.user_id
    t.timer_street = t.game.street
    t.timer_task = asyncio.create_task(_turn_timer(t, channel, cp.user_id))
    t.timer_task.add_done_callback(_task_catcher)


async def _turn_timer(t: TableState, channel, user_id: int):
    settings = await db.get_settings(channel.guild.id)
    timeout = settings.get("turn_timeout", TURN_TIMEOUT_DEFAULT)

    # Set the initial mutable deadline
    t.turn_deadline = time.time() + timeout
    warn_threshold = max(timeout // 5, 15)

    warn_msg = None
    warned = False

    # ── The Breathing Timer Loop ───────────────────────────────────────────
    while True:
        now = time.time()
        remaining = t.turn_deadline - now

        if remaining <= 0:
            break  # Time is up! Exit the loop to auto-fold.

        # Check if we need to warn them
        if remaining <= warn_threshold and not warned:
            # int(t.turn_deadline) ensures the Discord <t:..> tag dynamically shifts if we add time
            warn_msg = await channel.send(
                f"⚠️ <@{user_id}> — act now! You'll be auto-folded <t:{int(t.turn_deadline)}:R>."
            )
            warned = True

        # Sleep for just 1 second, then check the clock again
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            if warn_msg:
                try:
                    await warn_msg.delete()
                except (discord.NotFound, discord.HTTPException):
                    pass
            return

    # ── Fold phase ─────────────────────────────────────────────────────────
    if warn_msg:
        try:
            await warn_msg.delete()
        except (discord.NotFound, discord.HTTPException):
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
    t.leave_cooldown_pending.add(user_id)

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
        await refresh(channel, t, cosmetics_cache=t.cosmetics_cache)

# ── Auto next hand ────────────────────────────────────────────────────────────

def schedule_next_hand(t: TableState, channel):
    if t.auto_task and not t.auto_task.done():
        t.auto_task.cancel()
    t.auto_task = asyncio.create_task(_auto_next_hand(t, channel))
    t.auto_task.add_done_callback(_task_catcher)

async def _auto_next_hand(t: TableState, channel):
    settings = await db.get_settings(channel.guild.id)
    delay    = settings.get("next_hand_delay", NEXT_HAND_DELAY_DEFAULT)
    view = None
    try:
        if t.is_tournament:
            import tournament
            view = tournament.TournamentBetweenHandsView(t)
        else:
            view = BetweenHandsView(t)
        t.between_msg = await channel.send(f"⏳ Next hand starting in **{delay}s**...", view=view)
    except Exception as e:
        print(f"🚨 [ERROR] {e}")
        import traceback
        traceback.print_exc()

    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        if view: view.stop()
        return

    if view: view.stop()

    if t.between_msg:
        try:
            await t.between_msg.delete()
        except (discord.NotFound, discord.HTTPException):
            pass
        t.between_msg = None

    if t.closing:
        await _close_table(channel, t)
        return

    # pending_leaves chips were already returned in _process_result.
    # Don't return again — just let start_hand->_process_pending remove them from game.players.

    # Auto-remove or auto-rebuy players below big blind
    bb = t.game.BIG_BLIND
    for p in list(t.game.players):
        if (p.chips + p.pending_rebuy) < bb and p.user_id not in t.game.pending_leaves:

            is_tourney = getattr(t, 'is_tournament', False)
            if is_tourney:
                import tournament_db as tdb
                _bal = tdb.get_balance
                _deduct = tdb.deduct_chips
                _return = tdb.return_chips
                _clear = tdb.clear_chips_in_play
                _mark = tdb.mark_chips_in_play
                max_wallet = getattr(t.game, "MAX_BUYIN", 0)
            else:
                _bal = db.get_balance
                _deduct = db.deduct_chips
                _return = db.return_chips
                _clear = db.clear_chips_in_play
                _mark = db.mark_chips_in_play
                max_wallet = settings.get("max_wallet", 0)
            if not is_tourney:
                autorebuy_amount = await db.get_autorebuy(p.user_id)
            else:
                autorebuy_amount = 0
            triggered = False

            if autorebuy_amount > 0:
                current_total = p.chips + p.pending_rebuy

                # Set our target stack size
                target_stack = autorebuy_amount

                # Clamp the target stack to the table's max limit if there is one
                if max_wallet > 0:
                    target_stack = min(target_stack, max_wallet)

                # Calculate exactly how many chips are needed to reach the target
                top_up_needed = target_stack - current_total

                # Only proceed if they actually need chips, and the target is at least the Big Blind
                if top_up_needed > 0 and target_stack >= bb:
                    wallet_bal = await _bal(p.user_id)

                    # STRICT ALL-OR-NOTHING CHECK
                    if wallet_bal >= top_up_needed:
                        success = await _deduct(p.user_id, top_up_needed)
                        if success:
                            await _mark(p.user_id, p.display_name, top_up_needed)
                            t.game.queue_rebuy(p.user_id, top_up_needed, emoji=get_chip_emoji(t))
                            triggered = True
                            try:
                                await channel.send(
                                    f"♻️ **{p.display_name}** auto-topped up **{top_up_needed:,}** {get_chip_emoji(t)} to reach a stack of **{target_stack:,}** {get_chip_emoji(t)}.")
                            except (discord.HTTPException, discord.Forbidden) as e:
                                # 1. Log it to the console so the developer sees it
                                print(f"[Error] Channel send failed for auto-rebuy ({p.user_id}): {e}")

                                # 2. Force a backup receipt to your admin log channel
                                try:
                                    settings_log = await db.get_settings(channel.guild.id)
                                    log_ch_id = settings_log.get("log_channel_id")
                                    if log_ch_id:
                                        log_ch = channel.guild.get_channel(int(log_ch_id))
                                        if log_ch:
                                            await log_ch.send(
                                                f"⚠️ **SILENT REBUY:** {p.display_name} ({p.user_id}) auto-bought {top_up_needed} chips, but the public channel message failed to send.")
                                except Exception as e:
                                    print(f"🚨 [ERROR] {e}")
                                    import traceback
                                    traceback.print_exc()

            if not triggered:
                total_to_return = p.chips + p.pending_rebuy
                if total_to_return > 0:
                    await _return(p.user_id, total_to_return)
                await _clear(p.user_id)
                t.game.players.remove(p)
                try:
                    await channel.send(
                        f"🚪 **{p.display_name}** has been removed — stack (**{p.chips}** {get_chip_emoji(t)}) is below the big blind (**{bb}** {get_chip_emoji(t)}). Chips returned to wallet.")
                except (discord.HTTPException, discord.Forbidden) as e:
                    print(f"[Error] Failed to send below-BB kick msg for {p.user_id}: {e}")
                    try:
                        settings_log = await db.get_settings(channel.guild.id)
                        log_ch_id = settings_log.get("log_channel_id")
                        if log_ch_id:
                            log_ch = channel.guild.get_channel(int(log_ch_id))
                            if log_ch:
                                await log_ch.send(
                                    f"⚠️ **SILENT KICK:** {p.display_name} ({p.user_id}) was removed for being below BB. Chips returned. Public message failed.")
                    except Exception as e:
                        print(f"🚨 [ERROR] {e}")
                        import traceback
                        traceback.print_exc()

    active = [p for p in t.game.players if (p.chips + p.pending_rebuy) >= bb and p.user_id not in t.game.pending_leaves]
    pending_with_chips = [p for p in t.game.pending_joins if (p.chips + p.pending_rebuy) >= bb]
    total = len(active) + len(pending_with_chips)

    if total < 2:
        await refresh(channel, t, cosmetics_cache=None)
        await channel.send("⚠️ Not enough players for another hand. Waiting for a Manager to `/poker start`.")
        return

    if getattr(t, 'is_tournament', False):
        import tournament_db as tdb
        # Include pending_with_chips in the UID list
        active_uids = [p.user_id for p in active] + [p.user_id for p in pending_with_chips]
        dominance_warning = await tdb.get_team_dominance_warning(active_uids)
        if dominance_warning:
            await channel.send(f"⚠️ **Team stats abuse guard: **\n{dominance_warning}")
            await refresh(channel, t, cosmetics_cache=t.cosmetics_cache)
            return

    if not getattr(t, 'is_tournament', False):
        t.game.SMALL_BLIND = settings["small_blind"]
        t.game.BIG_BLIND   = settings["big_blind"]
        t.game.MIN_BUYIN = settings.get("min_wallet", 50)
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
        except Exception as e:
            print(f"🚨 [ERROR] {e}")
            import traceback
            traceback.print_exc()

    is_tourney = getattr(t, 'is_tournament', False)
    if is_tourney:
        import tournament_db as tdb
        _ret   = tdb.return_chips
        _clear = tdb.clear_chips_in_play
    else:
        _ret   = db.return_chips
        _clear = db.clear_chips_in_play

    for uid in list(t.game.pending_leaves):
        p = t.game.get_player(uid)
        if p:
            total_to_return = p.chips + p.pending_rebuy
            if total_to_return > 0:
                await _ret(uid, total_to_return)
            await _clear(uid)

    # Return chips for seated players not already paid out via pending_leaves
    for p in list(t.game.players):
        if p.user_id not in t.game.pending_leaves:
            total_to_return = p.chips + p.pending_rebuy
            if total_to_return > 0:
                await _ret(p.user_id, total_to_return)
            await _clear(p.user_id)

    # Return chips for pending joins
    for p in list(t.game.pending_joins):
        total_to_return = p.chips + p.pending_rebuy
        if total_to_return > 0:
            await _ret(p.user_id, total_to_return)
        await _clear(p.user_id)
    await channel.send(f"🚪 **Table '{t.name}'** closed. All chips returned.")


# ── Log thread ────────────────────────────────────────────────────────────────

_log_threads: dict[str, discord.Thread] = {}


async def ensure_log_thread(channel, t: TableState) -> discord.Thread | None:
    if t.is_tournament:
        # tournaments create their own threads as before
        settings = await db.get_settings(channel.guild.id)
        log_ch_id = settings.get("log_channel_id")
        if not log_ch_id:
            return None
        log_ch = channel.guild.get_channel(int(log_ch_id))
        if not log_ch:
            return None
        existing = _log_threads.get(t.id)
        if existing:
            return existing
        log_thread_name = f"Log {t.name}"
        try:
            thread = await log_ch.create_thread(name=log_thread_name, type=discord.ChannelType.public_thread)
            _log_threads[t.id] = thread
            return thread
        except Exception:
            traceback.print_exc()
            return None

    # Regular tables — use hardcoded thread ID
    existing = _log_threads.get(t.id)
    if existing:
        return existing
    thread = channel.guild.get_channel_or_thread(1480284795199033344) # log thread id hardcoded for now.
    if thread:
        _log_threads[t.id] = thread
    return thread


async def post_hand_log(channel, t: TableState, result):
    thread = await ensure_log_thread(channel, t)
    if not thread:
        return
    game = t.game

    rate, is_special = taxation.get_tax_config()

    header = f"Hand #{game.hand_num} | Table: {t.name} ({t.id}) | Pot: {result.pot}"
    if getattr(result, "tax", 0) > 0:
        if is_special:
            header += f" | Tax: {result.tax} (No revenue)"
        else:
            header += f" | Tax: {result.tax}"

    lines = [header]

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

    # 🚨 Grab the folded snapshot from the engine
    folded_ids = getattr(result, "folded_ids", set())

    _player_map = {_p.user_id: _p for _p in (result.showdown_players or [])}
    for uid, delta in result.chip_deltas.items():
        sign = "+" if delta > 0 else ""
        ustr = await uid_str(uid)
        rank = ranks.get(uid)
        sp = _player_map.get(uid)

        # 🚨 Check the snapshot to append (folded)
        if sp and sp.hole_cards:
            cards = hand_str(sp.hole_cards)
            if uid in folded_ids:
                cards += " (folded)"
        else:
            cards = "no cards"

        rank_part = f" [{rank}]" if rank else ""

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
        await thread.send(f"```\n{body}\n```")
    except (discord.NotFound, discord.HTTPException):
        _log_threads.pop(t.id, None)
    return body


async def post_tip_log(channel, t: TableState, tipper_id: int, tipper_name: str, amount: int, recipient_id: int,
                       recipient_name: str):
    thread = await ensure_log_thread(channel, t)
    if thread:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        try:
            await thread.send(
                f"💸 **Tip** [{ts}] — {amount} \n **{tipper_name}** ({tipper_id}) to **{recipient_name}** ({recipient_id}) at table `{t.name}`")
        except (discord.NotFound, discord.HTTPException):
            _log_threads.pop(t.id, None)
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
    tag = " 🔘" if idx == 0 else ""
    title_str = f" `{title}`" if title else ""
    mention   = f"<@{p.user_id}>"
    emoji     = get_chip_emoji(game)
    if p.folded:
        return f"~~{mention}{title_str}~~ ~~{p.chips} {emoji}~~ — folded{tag}"
    if p.all_in:
        return f"{mention}{title_str} **{p.chips} {emoji}** — ALL-IN 🚀{tag}"
    cp = game.current_player()
    if cp and cp.user_id == p.user_id:
        status = f"acting (bet {p.bet})" if p.bet else "acting"
    elif p.bet > 0:
        status = f"bet {p.bet}"
    else:
        status = "—"
    return f"{mention}{title_str} **{p.chips} {emoji}** — {status}{tag}"

def build_embed(t: TableState, title_cache: dict[int, str | None] | None = None, manager_name: str = "Unknown") -> discord.Embed:
    game  = t.game
    color = STREET_COLOR.get(game.street, 0x5865F2)
    label = STREET_LABEL.get(game.street, "")
    cp    = game.current_player()

    title = f"🃏 {t.name}"
    if game.hand_num:
        title += f"  ·  Hand #{game.hand_num}"
    title += f"  ·  {manager_name}"

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
    emoji = get_chip_emoji(t)
    for p in game.pending_joins:
        lines.append(f"<@{p.user_id}> **{p.chips} {emoji}** — ⏳ next hand")

    # 1. SAFE PLAYER CHUNKS (Groups of 6)
    if lines:
        chunk_size = config.PLAYERS_PER_FIELD
        chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]
        for i, chunk in enumerate(chunks):
            field_title = f"Players ({len(game.players)}/{config.MAX_PLAYERS})" if i == 0 else "\u200b"

            # Fallback just in case Discord strips the invisible character
            if not field_title.strip():
                field_title = "\u200b"

            chunk_text = "\n".join(chunk)
            if len(chunk_text) > 1024:
                chunk_text = chunk_text[:1020] + "..."
            embed.add_field(name=field_title, value=chunk_text, inline=False)

    # 2. SAFE STREET LOG (Hard-capped at 1024 characters)
    if t.street_log:
        log_text = "\n".join(t.street_log[-8:])
        if len(log_text) > 1024:
            log_text = log_text[:1020] + "..."
        embed.add_field(name="This round", value=log_text, inline=False)

    # 3. POT / TURN LOGIC
    if game.street not in (Street.WAITING,):
        pot_line = f"**Pot:** {game.pot} {emoji}"
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
    t.board_file = await asyncio.to_thread(card_images.make_strip, list(game.community), backs)
# ── Auto-delete helper ────────────────────────────────────────────────────────

async def _delete_after(message: discord.Message, delay: float):
    """Delete a message after `delay` seconds. Silently ignores errors."""
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except (discord.NotFound, discord.HTTPException):
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

    # Calculate what the text SHOULD say right now
    call_amt = t.game.call_amount(cp)
    hint     = f"call **{call_amt}**, raise, or fold" if call_amt else "check or raise"
    expected_content = f"<@{cp.user_id}> your turn — {hint}"

    # 2. If it is still this exact player's turn, DO NOT delete their ping!
    if t.ping_user_id == cp.user_id:
        # 🚨 FIX: If the required action changed (e.g., street advanced), edit the message!
        if t.ping_msg and t.ping_msg.content != expected_content:
            try:
                await t.ping_msg.edit(content=expected_content)
            except (discord.NotFound, discord.HTTPException):
                pass
        return

    # 3. Turn has advanced to a DIFFERENT player! Delete the old ping.
    if t.ping_msg:
        try:
            await t.ping_msg.delete()
        except discord.NotFound:
            pass
        t.ping_msg = None

    # 4. Claim the lock and send the new ping
    t.ping_user_id = cp.user_id
    t.ping_msg = await channel.send(expected_content)

# ── Action & Execution Helpers ────────────────────────────────────────────────

async def run_table_action(guild: discord.Guild, channel, t: TableState, interaction: discord.Interaction, fn, *args):
    if not interaction.response.is_done():
        await interaction.response.defer()
    
    uid = interaction.user.id
    ok, msg = fn(*args)
    if not ok:
        await interaction.followup.send(msg, ephemeral=True)
        return
        
    parts = msg.split("\n")
    street_markers = ["🌊", "↩️", "🏁"]
    if any(m in msg for m in street_markers + ["Showdown"]):
        slog_clear(t)
        
    for part in parts:
        if part.strip():
            slog(t, part)
            
    if t.game._hand_result:
        await _process_result(guild, channel, t)
    else:
        await refresh(channel, t, cosmetics_cache=t.cosmetics_cache)

class ActionConfirmView(discord.ui.View):
    def __init__(self, t: TableState, channel, guild, user_id: int, action_fn, action_args: list, prompt_text: str):
        super().__init__(timeout=30)
        self.t = t
        self.channel = channel
        self.guild = guild
        self.user_id = user_id
        self.action_fn = action_fn
        self.action_args = action_args
        self.prompt_text = prompt_text

    @discord.ui.button(label="Yes, Proceed", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This confirmation is not for you.", ephemeral=True)
            return
        
        await run_table_action(self.guild, self.channel, self.t, interaction, self.action_fn, *self.action_args)
        try:
            await interaction.edit_original_response(content="✅ Action confirmed.", view=None)
        except Exception:
            pass
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This confirmation is not for you.", ephemeral=True)
            return
        await interaction.response.edit_message(content="❌ Action cancelled.", view=None)
        self.stop()

async def leave_table_execute(guild: discord.Guild, channel, t: TableState, interaction: discord.Interaction):
    if t.closing:
        return "❌ Table is closing anyway."
        
    chips_back, msg = t.game.remove_player(interaction.user.id)
    
    if t.is_tournament:
        import tournament_db as tdb
        if chips_back > 0:
            await tdb.return_chips(interaction.user.id, chips_back)
        await tdb.clear_chips_in_play(interaction.user.id)
    else:
        if chips_back > 0:
            await db.return_chips(interaction.user.id, chips_back)
        await db.clear_chips_in_play(interaction.user.id)
        
    if "will leave" in msg:
        t.leave_cooldown_pending.add(interaction.user.id)
        await channel.send(f"👋 **{interaction.user.display_name}** will leave after this hand.")
    elif "left" in msg or "cashed out" in msg:
        cooldown = config.TOURNAMENT_REJOIN_COOLDOWN if getattr(t, 'is_tournament', False) else config.REGULAR_REJOIN_COOLDOWN
        t.rejoin_cooldowns[interaction.user.id] = time.time() + cooldown
        await channel.send(
            f"👋 **{interaction.user.display_name}** left the table. Chips returned to wallet.")
            
    await refresh(channel, t)
    return "✅ You left the table."

async def join_table_execute(interaction: discord.Interaction, t: TableState, chips: int, bal: int, rejoin_fee: int, min_w: int, max_w: int, is_deferred: bool = False):
    if not is_deferred:
        await interaction.response.defer(ephemeral=True)

    await db.upsert_wallet_name(interaction.user.id, interaction.user.name)

    if await db.is_banned(interaction.guild_id, interaction.user.id, t.name):
        await interaction.followup.send("❌ You are banned from this table.", ephemeral=True)
        return

    # Deduct rejoin fee first
    if rejoin_fee > 0:
        ok_fee = await db.deduct_chips(interaction.user.id, rejoin_fee)
        if not ok_fee:
            await interaction.followup.send("❌ Failed to deduct rejoin fee.", ephemeral=True)
            return
        t.rejoin_cooldowns.pop(interaction.user.id, None)

        try:
            await db.adjust_jackpot(rejoin_fee)
            await db.log_currency_event(interaction.user.id, "Jackpot", -rejoin_fee, "Paid rejoin bypass fee")
        except Exception as e:
            print(f"🚨 [ERROR] {e}")

        await interaction.channel.send(
            f"🎰 **{interaction.user.display_name}** paid **{rejoin_fee}** {get_chip_emoji(t)} directly to the **Jackpot** to bypass the rejoin cooldown!")

    ok = await db.deduct_chips(interaction.user.id, chips)
    if not ok:
        if rejoin_fee > 0:
            await db.return_chips(interaction.user.id, rejoin_fee)
        await interaction.followup.send(f"❌ Failed to deduct chips.", ephemeral=True)
        return

    await db.mark_chips_in_play(interaction.user.id, interaction.user.name, chips)

    msg = t.game.add_player(interaction.user.id, interaction.user.name, chips)
    if msg.startswith("❌"):
        await db.return_chips(interaction.user.id, chips)
        if rejoin_fee > 0:
            await db.return_chips(interaction.user.id, rejoin_fee)
        await db.clear_chips_in_play(interaction.user.id)
        await interaction.followup.send(msg, ephemeral=True)
        return

    await interaction.channel.send(f"✅ **{interaction.user.display_name}** joined the table with **{chips}** {get_chip_emoji(t)}!")
    await refresh(interaction.channel, t)
    await interaction.followup.send("✅ Successfully joined!", ephemeral=True)

class PreferencesModal(discord.ui.Modal):
    def __init__(self, user_id: int, pref_key: str, title: str, view):
        super().__init__(title=title)
        self.user_id = user_id
        self.pref_key = pref_key
        self.view = view

        label_str = "Enter Amount"
        placeholder_str = "e.g. 1000"

        if pref_key == "default_buyin_amount":
            label_str = "Default Stack (in BBs or 'max')"
            placeholder_str = "e.g. 100, 100BB, or max"
        elif pref_key == "auto_rebuy_amount":
            label_str = "Auto Rebuy Amount"
            placeholder_str = "e.g. 1000"
        elif pref_key.endswith("_threshold"):
            label_str = "Enter Threshold Amount"
            placeholder_str = "e.g. 500"

        self.value_input = discord.ui.TextInput(
            label=label_str,
            placeholder=placeholder_str,
            min_length=1,
            max_length=15
        )
        self.add_item(self.value_input)

    async def on_submit(self, interaction: discord.Interaction):
        input_str = self.value_input.value.strip().lower()
        if self.pref_key == "default_buyin_amount" and input_str in ["max", "full", "wallet", "all"]:
            val = -1
        else:
            raw_val = input_str.replace("bb", "").strip()
            val = parse_chips(raw_val)
            if val is None or val < 0:
                await interaction.response.send_message("❌ Enter a valid number greater than or equal to 0, or 'max'.", ephemeral=True)
                return

        if self.pref_key.endswith("_threshold"):
            mode_key = self.pref_key.replace("_threshold", "_mode")
            await db.set_player_preference(self.user_id, **{mode_key: "threshold", self.pref_key: val})
        else:
            await db.set_player_preference(self.user_id, **{self.pref_key: val})

        await self.view.refresh_preferences(interaction)

def button_to_dict(btn: discord.ui.Button) -> dict:
    return {
        "type": 2,
        "style": btn.style.value,
        "label": btn.label,
        "custom_id": btn.custom_id
    }

class PreferencesView(discord.ui.View):
    def __init__(self, user: discord.User | discord.Member):
        super().__init__(timeout=120)
        self.user = user
        self.user_id = user.id
        self.pref = {}

        # Instantiate buttons with custom_ids and callbacks
        self.btn_auto_rebuy = discord.ui.Button(custom_id="pref_auto_rebuy")
        self.btn_auto_rebuy.callback = self.toggle_auto_rebuy

        self.btn_auto_showdown = discord.ui.Button(custom_id="pref_auto_showdown")
        self.btn_auto_showdown.callback = self.toggle_auto_showdown

        self.btn_default_buyin = discord.ui.Button(custom_id="pref_default_buyin")
        self.btn_default_buyin.callback = self.toggle_default_buyin

        self.btn_confirm_all_in = discord.ui.Button(custom_id="pref_confirm_all_in")
        self.btn_confirm_all_in.callback = self.on_confirm_all_in_click

        self.btn_confirm_fold = discord.ui.Button(custom_id="pref_confirm_fold")
        self.btn_confirm_fold.callback = self.on_confirm_fold_click

        self.btn_confirm_leave = discord.ui.Button(custom_id="pref_confirm_leave")
        self.btn_confirm_leave.callback = self.toggle_confirm_leave

        self.btn_confirm_call_raise = discord.ui.Button(custom_id="pref_confirm_call_raise")
        self.btn_confirm_call_raise.callback = self.on_confirm_call_raise_click

        # Add items so they are registered in ViewStore for dispatching
        self.add_item(self.btn_auto_rebuy)
        self.add_item(self.btn_auto_showdown)
        self.add_item(self.btn_default_buyin)
        self.add_item(self.btn_confirm_all_in)
        self.add_item(self.btn_confirm_fold)
        self.add_item(self.btn_confirm_leave)
        self.add_item(self.btn_confirm_call_raise)

    def has_components_v2(self) -> bool:
        return True

    async def init_data(self):
        self.pref = await db.get_player_preference(self.user_id)
        self.update_button_states()

    def update_button_states(self):
        # Auto Rebuy
        arb_val = self.pref.get("auto_rebuy_amount", 0)
        self.btn_auto_rebuy.label = "Off" if arb_val == 0 else f"{arb_val:,}"
        self.btn_auto_rebuy.style = discord.ButtonStyle.red if arb_val == 0 else discord.ButtonStyle.green

        # Auto Showdown
        asd_val = self.pref.get("auto_showdown", "prompt")
        self.btn_auto_showdown.label = asd_val.title()
        if asd_val == "show":
            self.btn_auto_showdown.style = discord.ButtonStyle.green
        elif asd_val == "muck":
            self.btn_auto_showdown.style = discord.ButtonStyle.red
        else:
            self.btn_auto_showdown.style = discord.ButtonStyle.grey

        # Default Join Stack
        dbi_val = self.pref.get("default_buyin_amount", 0)
        if dbi_val == -1:
            self.btn_default_buyin.label = "Max Stack"
            self.btn_default_buyin.style = discord.ButtonStyle.green
        else:
            self.btn_default_buyin.label = "Off" if dbi_val == 0 else f"{dbi_val:,} BB"
            self.btn_default_buyin.style = discord.ButtonStyle.red if dbi_val == 0 else discord.ButtonStyle.green

        # Confirm All-In
        cai_mode = self.pref.get("confirm_all_in_mode", "always")
        cai_thresh = self.pref.get("confirm_all_in_threshold", 0)
        if cai_mode == "always":
            self.btn_confirm_all_in.label = "Always"
            self.btn_confirm_all_in.style = discord.ButtonStyle.green
        elif cai_mode == "never":
            self.btn_confirm_all_in.label = "Never"
            self.btn_confirm_all_in.style = discord.ButtonStyle.red
        else:
            self.btn_confirm_all_in.label = f"> {cai_thresh:,}"
            self.btn_confirm_all_in.style = discord.ButtonStyle.blurple

        # Confirm Fold
        cf_mode = self.pref.get("confirm_fold_mode", "always")
        cf_thresh = self.pref.get("confirm_fold_threshold", 0)
        if cf_mode == "always":
            self.btn_confirm_fold.label = "Always"
            self.btn_confirm_fold.style = discord.ButtonStyle.green
        elif cf_mode == "never":
            self.btn_confirm_fold.label = "Never"
            self.btn_confirm_fold.style = discord.ButtonStyle.red
        else:
            self.btn_confirm_fold.label = f"> {cf_thresh:,}"
            self.btn_confirm_fold.style = discord.ButtonStyle.blurple

        # Confirm Leave
        cl_val = self.pref.get("confirm_leave", 1)
        self.btn_confirm_leave.label = "Always" if cl_val == 1 else "Never"
        self.btn_confirm_leave.style = discord.ButtonStyle.green if cl_val == 1 else discord.ButtonStyle.red

        # Confirm Call/Raise
        ccr_mode = self.pref.get("confirm_call_raise_mode", "always")
        ccr_thresh = self.pref.get("confirm_call_raise_threshold", 0)
        if ccr_mode == "always":
            self.btn_confirm_call_raise.label = "Always"
            self.btn_confirm_call_raise.style = discord.ButtonStyle.green
        elif ccr_mode == "never":
            self.btn_confirm_call_raise.label = "Never"
            self.btn_confirm_call_raise.style = discord.ButtonStyle.red
        else:
            self.btn_confirm_call_raise.label = f"> {ccr_thresh:,}"
            self.btn_confirm_call_raise.style = discord.ButtonStyle.blurple

    def to_components(self) -> list[dict]:
        self.update_button_states()

        container = {
            "type": 17,
            "accent_color": 0x36393F,
            "components": [
                # Title Text Display
                {
                    "type": 10,
                    "content": "# Preferences\nConfigure your personal poker settings below."
                },
                # Separator
                {
                    "type": 14,
                    "divider": True,
                    "spacing": 1
                },
                # Section 1: Auto Rebuy
                {
                    "type": 9,
                    "components": [
                        {
                            "type": 10,
                            "content": "**Auto Rebuy**\nAutomatically top-up your stack between hands when you fall below the big blind."
                        }
                    ],
                    "accessory": button_to_dict(self.btn_auto_rebuy)
                },
                # Section 2: Auto Showdown
                {
                    "type": 9,
                    "components": [
                        {
                            "type": 10,
                            "content": "**Auto Showdown**\nChoose whether to prompt, show, or muck your cards automatically at showdown."
                        }
                    ],
                    "accessory": button_to_dict(self.btn_auto_showdown)
                },
                # Section 3: Default Buy-In
                {
                    "type": 9,
                    "components": [
                        {
                            "type": 10,
                            "content": "**Default Buy-In**\nSkip the join modal when entering a table by configuring a default stack size."
                        }
                    ],
                    "accessory": button_to_dict(self.btn_default_buyin)
                },
                # Separator
                {
                    "type": 14,
                    "divider": True,
                    "spacing": 1
                },
                # Section 4: Confirm All-In
                {
                    "type": 9,
                    "components": [
                        {
                            "type": 10,
                            "content": "**Confirm All-In**\nConfigure warning prompt before executing an All-In action."
                        }
                    ],
                    "accessory": button_to_dict(self.btn_confirm_all_in)
                },
                # Section 5: Confirm Fold
                {
                    "type": 9,
                    "components": [
                        {
                            "type": 10,
                            "content": "**Confirm Fold**\nConfigure warning prompt before folding. X is pot size. \nWill confirm if you have flush+ regardless of what u set."
                        }
                    ],
                    "accessory": button_to_dict(self.btn_confirm_fold)
                },
                # Section 6: Confirm Leave
                {
                    "type": 9,
                    "components": [
                        {
                            "type": 10,
                            "content": "**Confirm Leave**\nConfigure warning prompt before leaving a table."
                        }
                    ],
                    "accessory": button_to_dict(self.btn_confirm_leave)
                },
                # Section 7: Confirm Call/Raise
                {
                    "type": 9,
                    "components": [
                        {
                            "type": 10,
                            "content": "**Confirm Call/Raise**\nConfigure warning prompt when calling or raising exceeds X chips."
                        }
                    ],
                    "accessory": button_to_dict(self.btn_confirm_call_raise)
                }
            ]
        }
        return [container]

    async def toggle_auto_rebuy(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not your settings menu.", ephemeral=True)
            return
        val = self.pref.get("auto_rebuy_amount", 0)
        if val > 0:
            await db.set_player_preference(self.user_id, auto_rebuy_amount=0)
            await self.refresh_preferences(interaction)
        else:
            modal = PreferencesModal(self.user_id, "auto_rebuy_amount", "Auto Rebuy Amount", self)
            await interaction.response.send_modal(modal)

    async def toggle_auto_showdown(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not your settings menu.", ephemeral=True)
            return
        curr = self.pref.get("auto_showdown", "prompt")
        modes = ["prompt", "show", "muck"]
        nxt = modes[(modes.index(curr) + 1) % len(modes)]
        await db.set_player_preference(self.user_id, auto_showdown=nxt)
        await self.refresh_preferences(interaction)

    async def toggle_default_buyin(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not your settings menu.", ephemeral=True)
            return
        val = self.pref.get("default_buyin_amount", 0)
        if val > 0:
            await db.set_player_preference(self.user_id, default_buyin_amount=0)
            await self.refresh_preferences(interaction)
        else:
            modal = PreferencesModal(self.user_id, "default_buyin_amount", "Default Join Stack", self)
            await interaction.response.send_modal(modal)

    async def on_confirm_all_in_click(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not your settings menu.", ephemeral=True)
            return
        mode = self.pref.get("confirm_all_in_mode", "always")
        if mode == "always":
            await db.set_player_preference(self.user_id, confirm_all_in_mode="never")
            await self.refresh_preferences(interaction)
        elif mode == "never":
            modal = PreferencesModal(self.user_id, "confirm_all_in_threshold", "Set All-In Threshold", self)
            await interaction.response.send_modal(modal)
        else:
            await db.set_player_preference(self.user_id, confirm_all_in_mode="always")
            await self.refresh_preferences(interaction)

    async def on_confirm_fold_click(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not your settings menu.", ephemeral=True)
            return
        mode = self.pref.get("confirm_fold_mode", "always")
        if mode == "always":
            await db.set_player_preference(self.user_id, confirm_fold_mode="never")
            await self.refresh_preferences(interaction)
        elif mode == "never":
            modal = PreferencesModal(self.user_id, "confirm_fold_threshold", "Set Fold Threshold", self)
            await interaction.response.send_modal(modal)
        else:
            await db.set_player_preference(self.user_id, confirm_fold_mode="always")
            await self.refresh_preferences(interaction)

    async def toggle_confirm_leave(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not your settings menu.", ephemeral=True)
            return
        val = self.pref.get("confirm_leave", 1)
        new_val = 0 if val == 1 else 1
        await db.set_player_preference(self.user_id, confirm_leave=new_val)
        await self.refresh_preferences(interaction)

    async def on_confirm_call_raise_click(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not your settings menu.", ephemeral=True)
            return
        mode = self.pref.get("confirm_call_raise_mode", "always")
        if mode == "always":
            await db.set_player_preference(self.user_id, confirm_call_raise_mode="never")
            await self.refresh_preferences(interaction)
        elif mode == "never":
            modal = PreferencesModal(self.user_id, "confirm_call_raise_threshold", "Set Call/Raise Threshold", self)
            await interaction.response.send_modal(modal)
        else:
            await db.set_player_preference(self.user_id, confirm_call_raise_mode="always")
            await self.refresh_preferences(interaction)

    async def refresh_preferences(self, interaction: discord.Interaction):
        self.pref = await db.get_player_preference(self.user_id)
        if interaction.response.is_done():
            await interaction.followup.edit_message(message_id="@original", view=self)
        else:
            await interaction.response.edit_message(view=self)

# ── Refresh ───────────────────────────────────────────────────────────────────

async def refresh(channel, t: TableState, new_hand: bool = False, cosmetics_cache: dict = None):
    await update_board(t)
    title_cache: dict[int, str | None] = {}
    try:
        uids = [p.user_id for p in t.game.players]
        if not cosmetics_cache or any(uid not in cosmetics_cache for uid in uids):
            if getattr(t, 'is_tournament', False):
                if not getattr(t, 'cosmetics_cache', None) or any(uid not in t.cosmetics_cache for uid in uids):
                    t.cosmetics_cache = await db.get_cosmetics_bulk(uids)
                cosmetics_cache = t.cosmetics_cache
            else:
                cosmetics_cache = await db.get_cosmetics_bulk(uids)
                t.cosmetics_cache = cosmetics_cache
        for uid, cosmetics in cosmetics_cache.items():
            tid = cosmetics.get("active_title")
            if tid and tid in db.TITLES:
                title_cache[uid] = db.TITLES[tid]["display"]
    except Exception as e:
        print(f"🚨 [ERROR] {e}")
        import traceback
        traceback.print_exc()

    embed = build_embed(t, title_cache, t.manager_name)   # sets attachment://board.png if file present

    if t.is_tournament:
        import tournament
        view = tournament.TournamentGameView(t)
    else:
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
    emoji       = get_chip_emoji(t)
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
            slog(t, f"🏆 **{w.display_name}** won **{sign}{gained}** {emoji}{rs}")
        else:
            names_and_nets = []
            for w in result.winners:
                gained = result.chip_deltas.get(w.user_id, 0)
                sign = "+" if gained > 0 else ""
                names_and_nets.append(f"**{w.display_name}** ({sign}{gained})")
            slog(t, f"🤝 Split: {', '.join(names_and_nets)} {emoji}")
    else:
        for i, (amt, winners) in enumerate(pot_results):
            label = "Main" if i == 0 else f"Side {i}"
            if len(winners) == 1:
                w      = winners[0]
                rank   = ranks.get(w.user_id)
                rs     = f" ({rank})" if rank else ""
                slog(t, f"🏆 **{label}** ({amt}{emoji}) → **{w.display_name}**{rs}")
            else:
                each  = amt // len(winners)
                names = ", ".join(f"**{w.display_name}**" for w in winners)
                slog(t, f"🤝 **{label}** ({amt}{emoji}) split → {names} ({each} each)")


async def _announce_winner(channel, t: TableState, result, cosmetics_cache: dict = None):
    game = t.game
    ranks = result.winner_ranks or {}
    pot_results = result.pot_results  # [(amount, [PokerPlayer, ...]), ...]
    emoji = get_chip_emoji(t)

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
                    quotes.append(f"> *\"{wm}\"*")
                else:
                    quotes.append(f"> **{w.display_name}:** *\"{wm}\"*")
        return "\n".join(quotes)

    # 🏆 Create the sleek winner "Receipt" Embed
    embed = discord.Embed(title=f"🏆 Hand #{game.hand_num} Results", color=0xF1C40F)
    desc_lines = []

    if not pot_results or len(pot_results) == 1:
        # ── Single Pot (or Fold Win) ──
        if len(result.winners) == 1:
            w = result.winners[0]
            gained = result.chip_deltas.get(w.user_id, 0)
            rank = ranks.get(w.user_id)
            rs = f" with **{rank}**" if rank else ""
            sign = "+" if gained > 0 else ""

            desc_lines.append(f"**{w.display_name}**{_title_str(w.user_id)}")
            desc_lines.append(f"Won **{sign}{gained}** {emoji}{rs}")

            quotes = _build_quotes(result.winners, single=True)
            if quotes:
                desc_lines.append("")
                desc_lines.append(quotes)

            embed.description = "\n".join(desc_lines).strip() + "\n\u200b"

            if result.community:
                embed.add_field(name="🃏 Board", value=f"{hand_str(result.community)}\n\u200b", inline=False)

            pre_tax_pot = result.pot + getattr(result, "tax", 0)
            embed.add_field(name="Pot", value=f"{pre_tax_pot} {emoji}", inline=True)
            embed.add_field(name="New Stack", value=f"{w.chips} {emoji}", inline=True)
        else:
            # ── True Split Pot ──
            desc_lines.append("🤝 **Split Pot!**\n")
            for w in result.winners:
                gained = result.chip_deltas.get(w.user_id, 0)
                sign = "+" if gained > 0 else ""
                rank = ranks.get(w.user_id)
                rs = f" with **{rank}**" if rank else ""
                desc_lines.append(
                    f"• **{w.display_name}**{_title_str(w.user_id)} won **{sign}{gained}** {emoji}{rs}")

            quotes = _build_quotes(result.winners)
            if quotes:
                desc_lines.append("")
                desc_lines.append(quotes)

            embed.description = "\n".join(desc_lines).strip() + "\n\u200b"

            if result.community:
                embed.add_field(name="🃏 Board", value=f"{hand_str(result.community)}\n\u200b", inline=False)

            # Removed the Total Pot block as requested for split pots
    else:
        # ── Multiple Side Pots ──
        for i, (amt, winners) in enumerate(pot_results):
            label = "Main Pot" if i == 0 else f"Side Pot {i}"
            icon = "🥇" if i == 0 else "🥈"

            # 🚨 Updated format based on the screenshot!
            desc_lines.append(f"{icon} **{label}** {emoji} **{amt}**")

            if len(winners) == 1:
                w = winners[0]
                rank = ranks.get(w.user_id)
                rs = f" with **{rank}**" if rank else ""
                desc_lines.append(f"↳ **{w.display_name}**{_title_str(w.user_id)}{rs}")
            else:
                split_amt = amt // len(winners)
                for w in winners:
                    rank = ranks.get(w.user_id)
                    rs = f" with **{rank}**" if rank else ""
                    desc_lines.append(
                        f"↳ **{w.display_name}**{_title_str(w.user_id)} *(split {split_amt}* {emoji}*){rs}")
            desc_lines.append("")  # Empty line between pots

        quotes = _build_quotes(result.winners)
        if quotes:
            desc_lines.append(quotes)

        # Inject invisible spacer (\u200b) to force Discord to give us breathing room before the Board
        embed.description = "\n".join(desc_lines).strip() + "\n\u200b"

        if result.community:
            # Inject invisible spacer after the board cards
            embed.add_field(name="🃏 Board", value=f"{hand_str(result.community)}\n\u200b", inline=False)

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

    if not getattr(t, 'is_tournament', False):
        rate, is_special = taxation.get_tax_config()
        if is_special:
            pct_string = f"{rate * 100:g}%"
            embed.set_footer(text=f"✨ Jackpot Friday! Tax is {pct_string} (All of it goes to Jackpot) ✨")

    # 🚀 Send the final embed
    await channel.send(embed=embed)


async def _handle_egirl_saro(channel, t: TableState):
    # Announce and unlock cosmetics for any player dealt the saro ace this hand
    if not t.game.egirl_saro_holders:
        return
    for uid in list(t.game.egirl_saro_holders):
        p = t.game.get_player(uid)
        name = p.display_name if p else f"<@{uid}>"
        newly_title = await db.unlock_cosmetic(uid, "title", "sarosmommy")
        newly_winmsg1 = await db.unlock_cosmetic(uid, "winmsg", "egirl_ace_winmsg")
        newly_winmsg2 = await db.unlock_cosmetic(uid, "winmsg", "noo")

        # If ANY of the three are newly unlocked, trigger the first-time message
        first_time = newly_title or newly_winmsg1 or newly_winmsg2
        try:
            if first_time:
                await channel.send(
                    f"✨ **{name}** was dealt the shiny **e-girl Saroshi!**\n"
                    f"A legendary cosmetic has been unlocked — check `/poker titles`."
                )
            else:
                await channel.send(
                    f"✨ **{name}** was dealt the shiny **e-girl Saroshi** again!"
                )
        except (discord.HTTPException, discord.Forbidden) as e:
            print(f"[Error] Failed to announce E-girl Saro drop for {uid}: {e}")
    t.game.egirl_saro_holders.clear()


async def _process_result(guild, channel, t: TableState):
    result = t.game._hand_result
    if not result:
        return
    cancel_timer(t)

    if t.is_tournament:
        import tournament_db
        wagers_this_hand = result.wagers or {}
        if wagers_this_hand:
            await tournament_db.log_period_wagers(wagers_this_hand)
        await tournament_db.process_hand_result(result, t.name)
        # Sync each player's current stack into chips_in_play so wallet reflects live totals
        chip_map = {p.user_id: p.chips + p.pending_rebuy for p in t.game.players}
        try:
            await tournament_db.sync_chips_in_play(chip_map)
        except Exception:
            traceback.print_exc()
        # Return chips & set rejoin cooldown for pending leaves
        for uid in list(t.game.pending_leaves):
            p = t.game.get_player(uid)
            if p:
                total_to_return = p.chips + p.pending_rebuy
                if total_to_return > 0:
                    await tournament_db.return_chips(uid, total_to_return)
                await tournament_db.clear_chips_in_play(uid)
            if uid in t.leave_cooldown_pending:
                t.rejoin_cooldowns[uid] = time.time() + config.TOURNAMENT_REJOIN_COOLDOWN

        # Remove them from game.players now so the post-hand embed is clean.
        for uid in list(t.game.pending_leaves):
            p = t.game.get_player(uid)
            if p:
                t.game.players.remove(p)
        t.game.pending_leaves.clear()
        t.game.kicked_users.clear()
        t.leave_cooldown_pending.clear()

        await post_hand_log(channel, t, result)
        await _announce_winner(channel, t, result, cosmetics_cache=t.cosmetics_cache)

        if result.showdown_players:
            await _reveal_phase(channel, t, result)

        t.game._hand_result = None

        if t.closing:
            await _close_table(channel, t)
        else:
            # Verify table wasn't closed during final moments
            key = (channel.guild.id, channel.id)
            if get_table(key) is t and not t.closing:
                schedule_next_hand(t, channel)
        return
    # Cancel any pending auto-next-hand task — if it fires before we finish
    # processing, it calls start_hand() which clears _hand_result and starts
    # a new hand, causing _process_result to silently bail out.
    if t.auto_task and not t.auto_task.done():
        t.auto_task.cancel()
    t.auto_task = None

    # Stats + achievements — one DB write per player instead of 6-8
    jackpot_hits: list[tuple] = [] # collected here, announced after _announce_winner
    achievement_announces: list[str] = [] # same pattern, collected then sent after hand result
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

            all_in_win = bool(won and result.allin_user_ids and p.user_id in result.allin_user_ids)

            quads_win = sf_win = rf_win = False
            if won:
                quads_win, sf_win, rf_win = jackpot.evaluate_jackpot_tiers(p, result.community)

            did_vpip = bool(hasattr(result, "vpip_ids") and p.user_id in result.vpip_ids)

            # ────────────────────────────────────────────────────────────────

            await db.record_hand_full(
                p.user_id, p.display_name, won, net,
                pocket_aces=pocket_aces,
                all_in_win=all_in_win,
                quads_win=quads_win,
                straight_flush_win=sf_win,
                royal_flush_win=rf_win,
                vpip=did_vpip,
            )

            if net != 0:
                await db.log_currency_event(p.user_id, "Hand", net, f"Hand #{t.game.hand_num} at {t.name}")

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
            achievement_announces.append("\n".join(lines))

        # ── Jackpot split payout ──────────────────────────────────────────────
        folded_ids = getattr(result, "folded_ids", set())
        jackpot_hits = await jackpot.process_jackpot_hits(result.showdown_players or [], result.community,
                                                          folded_ids)

    except Exception as e:
        print(f"[poker] stats/achievement error: {e}")
        traceback.print_exc()

    try:
        chip_map = {p.user_id: p.chips + p.pending_rebuy for p in t.game.players}
        await db.sync_chips_in_play(chip_map)
    except Exception as e:
        print(f"[poker] chips_in_play error: {e}")
        traceback.print_exc()

    # 🚨 LOG THE TAX TO REVENUE/JACKPOT
    try:
        if getattr(result, "tax", 0) > 0:
            await taxation.process_and_log_tax(result.tax)
    except Exception as e:
        print(f"[poker] log_tax error: {e}")
        traceback.print_exc()

    try:
        log_body = await post_hand_log(channel, t, result)

        # Update the specific row we created when this hand started
        await db.log_hand(guild.id, t.id, t.name, t.game.hand_num, log_body or result.summary, t.manager_id, t.manager_name, result.action_history)
    except Exception as e:
        print(f"[poker] complete_hand_log error: {e}")
        traceback.print_exc()

    try:
        for uid in list(t.game.pending_leaves):
            p = t.game.get_player(uid)
            if p:
                total_to_return = p.chips + p.pending_rebuy
                if total_to_return > 0:
                    await db.return_chips(uid, total_to_return)
                await db.clear_chips_in_play(uid)
            # Voluntary leaves and AFK kicks get a 10-minute rejoin cooldown.
            # Chip-kicked players (below BB) are NOT in leave_cooldown_pending.
            if uid in t.leave_cooldown_pending:
                t.rejoin_cooldowns[uid] = time.time() + config.REGULAR_REJOIN_COOLDOWN
        # Remove them from game.players now so the post-hand embed is clean.
        # _process_pending in start_hand will find pending_leaves already empty and skip.
        for uid in list(t.game.pending_leaves):
            p = t.game.get_player(uid)
            if p:
                t.game.players.remove(p)
        t.game.pending_leaves.clear()
        t.game.kicked_users.clear()
        t.leave_cooldown_pending.clear()
    except Exception as e:
        print(f"[poker] pending_leaves return error: {e}")
        traceback.print_exc()

    all_cosmetics = {}
    try:
        all_cosmetics = await db.get_cosmetics_bulk([p.user_id for p in t.game.players])
        t.cosmetics_cache = all_cosmetics
        await _announce_winner(channel, t, result, cosmetics_cache=all_cosmetics)
    except Exception as e:
        print(f"[poker] _announce_winner error: {e}")
        traceback.print_exc()

    for (uid, jp_tier, actual, new_jp) in jackpot_hits:
        try:
            embed = discord.Embed(
                title="🎰 JACKPOT!!!",
                description=(
                    f"<@{uid}> triggered **{jp_tier}** and won "
                    f"⏣ **{actual:,},000,000** "
                    f"from the jackpot! *(added to wallet)*\n\n"
                    f"Jackpot remaining: **{new_jp:,}** <:poker_chip:1490458259855773707>"
                ),
                color=0xFFD700,
            )
            await channel.send(embed=embed)
        except Exception as e:
            print(f"[poker] jackpot announce error: {e}")
            traceback.print_exc()

        try:
            settings = await db.get_settings(channel.guild.id)
            log_ch_id = settings.get("log_channel_id")
            if log_ch_id:
                log_ch = channel.guild.get_channel(int(log_ch_id))
                if log_ch:
                    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
                    p_obj = t.game.get_player(uid)
                    p_name = p_obj.display_name if p_obj else f"User {uid}"
                    await log_ch.send(
                        f"🎰 **Jackpot Hit** [{ts}] — **{p_name}** ({uid}) won **{actual:,}** from **{jp_tier}** at table `{t.name}`.\n"
                        f"Remaining Jackpot: {new_jp:,}"
                    )
        except Exception as e:
            print(f"[poker] jackpot log error: {e}")
            traceback.print_exc()

    for msg_text in achievement_announces:
        try:
            await channel.send(msg_text)
        except Exception as e:
            print(f"[poker] achievement announce error: {e}")
            traceback.print_exc()

    try:
        _slog_result(t, result)
        await refresh(channel, t, cosmetics_cache=all_cosmetics)
    except Exception as e:
        print(f"[poker] refresh error: {e}")
        traceback.print_exc()

    # 1. ALWAYS run the reveal phase if there was a showdown, even if closing
    if result.showdown_players:
        try:
            await _reveal_phase(channel, t, result)
        except Exception as e:
            print(f"⚠️ Recovered from Discord API crash during reveal: {e}")
            traceback.print_exc()

    t.game._hand_result = None

    # 2. THEN check if we need to close the table or schedule the next hand
    await _handle_egirl_saro(channel, t)

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
            file = await asyncio.to_thread(card_images.make_strip, sp.hole_cards, 0, False, sp.egirl_saro)
            await interaction.followup.send(caption, file=file)
        else:
            await interaction.response.send_message(caption)
        await self._resolve(interaction.user.id)

    @discord.ui.button(label="Muck 🗑️", style=discord.ButtonStyle.grey)
    async def muck_hand(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 1. Ignore people who aren't prompted
        if interaction.user.id not in self.pending:
            await interaction.response.send_message("❌ You have nothing to muck.", ephemeral=True)
            return

        # 2. Silently confirm the muck
        await interaction.response.send_message("🗑️ You quietly mucked your hand.", ephemeral=True)

        # 3. Resolve them from the queue! If the queue empties, the hand instantly ends.
        await self._resolve(interaction.user.id)


async def _reveal_phase(channel, t: TableState, result):
    settings = await db.get_settings(channel.guild.id)
    timeout = settings.get("muck_time", 15)  # Fetches custom time, defaults to 15s

    # ── 1. Uncontested Win (Everyone Folded) ──────────────
    if not result.pot_results:
        winner = result.winners[0] if result.winners else None
        if winner and winner.hole_cards:
            pref = await db.get_player_preference(winner.user_id)
            auto_action = pref.get("auto_showdown", "prompt")
            if auto_action == "muck":
                return
            elif auto_action == "show":
                caption = f"👁️ **{winner.display_name}** shows: {hand_str(winner.hole_cards)}"
                if USE_IMAGES:
                    file = await asyncio.to_thread(card_images.make_strip, winner.hole_cards, 0, False, winner.egirl_saro)
                    await channel.send(caption, file=file)
                else:
                    await channel.send(caption)
                return

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
            except (discord.NotFound, discord.HTTPException):
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
                file = await asyncio.to_thread(card_images.make_strip, w.hole_cards, 0, False, w.egirl_saro)
                await channel.send(caption, file=file)
            else:
                await channel.send(caption)

    # B. Prompt losers with a Show/Muck button
    folded_ids = getattr(result, "folded_ids", set())
    candidates = [p for p in (result.showdown_players or []) if p.user_id not in winner_ids and p.user_id not in folded_ids]
    if not candidates:
        return  # Chop pot — everyone tied and won, so everyone already showed automatically

    pending_ids = []
    for p in candidates:
        pref = await db.get_player_preference(p.user_id)
        auto_action = pref.get("auto_showdown", "prompt")
        if auto_action == "muck":
            continue
        elif auto_action == "show":
            if len(result.community) >= 3:
                score = evaluator.evaluate(p.hole_cards, result.community)
                rank_str = f" — *{evaluator.class_to_string(evaluator.get_rank_class(score))}*"
            else:
                rank_str = ""
            caption = f"👁️ **{p.display_name}** shows: {hand_str(p.hole_cards)}{rank_str}"
            if USE_IMAGES:
                file = await asyncio.to_thread(card_images.make_strip, p.hole_cards, 0, False, p.egirl_saro)
                await channel.send(caption, file=file)
            else:
                await channel.send(caption)
        else:
            pending_ids.append(p.user_id)

    if not pending_ids:
        return

    deadline = int(time.time()) + timeout
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
    except (discord.NotFound, discord.HTTPException):
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

        from_wallet = min(tip, wallet_bal)
        from_table = tip - from_wallet

        if from_table > 0 and self.t.game.street != Street.WAITING:
            await interaction.followup.send("❌ You cannot tip chips from the table while a hand is in progress. Wait for the hand to finish.", ephemeral=True)
            return

        if from_table > table_chips:
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
        except Exception as e:
            print(f"🚨 [ERROR] {e}")
            import traceback
            traceback.print_exc()

        await db.add_chips(interaction.user.id, interaction.user.display_name,
                           manager_id, manager_name, tip, f"Tip from {interaction.user.display_name}")
        await post_tip_log(interaction.channel, self.t, interaction.user.id, interaction.user.display_name, tip, manager_id, manager_name)
        await db.record_tip(interaction.user.id, interaction.user.display_name, tip)
        await db.log_currency_event(interaction.user.id, "Tip", -tip, f"Tipped {manager_name}")
        await db.log_currency_event(manager_id, "Tip", tip, f"Tip from {interaction.user.display_name}")
        await interaction.followup.send(
            f"💸 **{interaction.user.display_name}** tipped **{tip}** chips to **{manager_name}**!", ephemeral=False)

        # Refresh the UI to reflect the deducted seated chips
        if self.t.game.street == Street.WAITING:
            await refresh(interaction.channel, self.t, cosmetics_cache=self.t.cosmetics_cache)


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

        # Calculate live stack size right now
        p = self.t.game.get_player(interaction.user.id)
        pj = next((x for x in self.t.game.pending_joins if x.user_id == interaction.user.id), None)
        live_stack = (p.chips + p.pending_rebuy) if p else ((pj.chips + pj.pending_rebuy) if pj else 0)

        if self.max_w > 0 and (live_stack + chips) > self.max_w:
            await interaction.response.send_message(
                f"❌ Maximum table stack is **{self.max_w}**. You can only add up to **{max(0, self.max_w - live_stack)}** more chips.",
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

        await db.mark_chips_in_play(interaction.user.id, interaction.user.name, chips)
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
        hand_running = self.t.game.street != Street.WAITING
        table_chips = 0 if hand_running else (p.chips if p else 0)
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

class AllInConfirmView(discord.ui.View):
    """Ephemeral prompt shown when a player clicks All In."""

    def __init__(self, t: TableState, channel, guild):
        super().__init__(timeout=30)
        self.t = t
        self.channel = channel
        self.guild = guild

    @discord.ui.button(label="Yes, All In 🚀", style=discord.ButtonStyle.red)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 1. Defer right away
        await interaction.response.defer()

        uid = interaction.user.id
        if not self.t.game.is_turn(uid):
            await interaction.edit_original_response(content="❌ It is no longer your turn.", view=None)
            self.stop()
            return

        g = self.t.game
        p = g.get_player(uid)

        # 2. Execute the All-In math
        call_needed = g.call_amount(p)
        raise_on_top = p.chips - call_needed

        if raise_on_top <= 0:
            success, msg = g.check_or_call(uid)
        else:
            success, msg = g.raise_bet(uid, raise_on_top)

        if not success:
            await interaction.edit_original_response(content=msg, view=None)
            self.stop()
            return

        # 3. Clean up the ephemeral prompt
        await interaction.edit_original_response(content="✅ You went all in!", view=None)

        # 4. Advance the game state
        if any(m in msg for m in ["🌊", "↩️", "🏁", "Showdown"]):
            slog_clear(self.t)

        for part in msg.split("\n"):
            if part.strip():
                slog(self.t, part)

        if self.t.game._hand_result:
            await _process_result(self.guild, self.channel, self.t)
        else:
            await refresh(self.channel, self.t, cosmetics_cache=self.t.cosmetics_cache)

        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ All-In cancelled.", view=None)
        self.stop()

class RaiseCustomModal(discord.ui.Modal, title="Custom Raise"):
    amount = discord.ui.TextInput(label="Raise BY how many chips?", placeholder="e.g. 200", min_length=1, max_length=7)

    def __init__(self, t: TableState, channel, guild):
        super().__init__()
        self.t = t; self.channel = channel; self.guild = guild

    async def on_submit(self, interaction: discord.Interaction):
        raise_amount = parse_chips(self.amount.value)
        if raise_amount is None:
            await interaction.response.send_message("❌ Enter a valid amount (e.g. 500, 2k, 1.5k).", ephemeral=True); return
        uid = interaction.user.id
        p   = self.t.game.get_player(uid)
        if not p or not self.t.game.is_turn(uid):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True); return
        if raise_amount <= 0:
            await interaction.response.send_message("❌ Must be greater than 0.", ephemeral=True); return

        pref = await db.get_player_preference(uid)
        mode = pref.get("confirm_call_raise_mode", "always")
        thresh = pref.get("confirm_call_raise_threshold", 0)
        should_confirm = True
        if mode == "never":
            should_confirm = False
        elif mode == "threshold":
            if raise_amount <= thresh:
                should_confirm = False

        if should_confirm:
            view = ActionConfirmView(self.t, self.channel, self.guild, uid, self.t.game.raise_bet, [uid, raise_amount], f"⚠️ **Are you sure you want to raise by {raise_amount:,} chips?**")
            await interaction.response.send_message(
                f"⚠️ **Are you sure you want to raise by {raise_amount:,} chips?**",
                view=view,
                ephemeral=True
            )
            return

        await run_table_action(self.guild, self.channel, self.t, interaction, self.t.game.raise_bet, uid, raise_amount)

class RaisePickerView(discord.ui.View):
    """Shown when player clicks Raise — offers preset options."""
    def __init__(self, t: TableState, channel, guild, timeout: float):
        super().__init__(timeout=timeout)
        self.t = t; self.channel = channel; self.guild = guild

        g = t.game
        cp = g.current_player()
        if cp:
            call_amt = g.call_amount(cp)
            min_raise_amt = g.last_raise_size if g.last_raise_size > 0 else g.BIG_BLIND
            pot_third = max(call_amt, g.pot // 3)
            pot_half = max(call_amt, g.pot // 2)

            self.btn_min_raise.label = f"Min +{min_raise_amt}"
            self.btn_third_pot.label = f"1/3 Pot +{pot_third}"
            self.btn_half_pot.label = f"1/2 Pot +{pot_half}"
            self.btn_all_in.label = f"All In"

    async def _do_raise(self, interaction: discord.Interaction, raise_amount: int):
        uid = interaction.user.id
        if not self.t.game.is_turn(uid):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True); return

        pref = await db.get_player_preference(uid)
        mode = pref.get("confirm_call_raise_mode", "always")
        thresh = pref.get("confirm_call_raise_threshold", 0)
        should_confirm = True
        if mode == "never":
            should_confirm = False
        elif mode == "threshold":
            if raise_amount <= thresh:
                should_confirm = False

        if should_confirm:
            view = ActionConfirmView(self.t, self.channel, self.guild, uid, self.t.game.raise_bet, [uid, raise_amount], f"⚠️ **Are you sure you want to raise by {raise_amount:,} chips?**")
            await interaction.response.send_message(
                f"⚠️ **Are you sure you want to raise by {raise_amount:,} chips?**",
                view=view,
                ephemeral=True
            )
            return

        await run_table_action(self.guild, self.channel, self.t, interaction, self.t.game.raise_bet, uid, raise_amount)

    @discord.ui.button(label="Min", style=discord.ButtonStyle.green, row=0)
    async def btn_min_raise(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True)
            return
        g = self.t.game
        p = g.get_player(interaction.user.id)
        if not p: await interaction.response.send_message("❌ Not your turn.", ephemeral=True); return
        min_raise_amt = g.last_raise_size if g.last_raise_size > 0 else g.BIG_BLIND
        await self._do_raise(interaction, min_raise_amt)

    @discord.ui.button(label="1/3 Pot", style=discord.ButtonStyle.green, row=0)
    async def btn_third_pot(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True)
            return
        g = self.t.game
        p = g.get_player(interaction.user.id)
        if not p: await interaction.response.send_message("❌ Not your turn.", ephemeral=True); return
        amount = max(g.call_amount(p), g.pot // 3)
        await self._do_raise(interaction, amount)

    @discord.ui.button(label="1/2 Pot", style=discord.ButtonStyle.green, row=0)
    async def btn_half_pot(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True)
            return
        g = self.t.game
        p = g.get_player(interaction.user.id)
        if not p: await interaction.response.send_message("❌ Not your turn.", ephemeral=True); return
        amount = max(g.call_amount(p), g.pot // 2)
        await self._do_raise(interaction, amount)

    @discord.ui.button(label="All In 🚀", style=discord.ButtonStyle.red, row=0)
    async def btn_all_in(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True)
            return

        g = self.t.game
        p = g.get_player(interaction.user.id)
        if not p: return

        pref = await db.get_player_preference(interaction.user.id)
        mode = pref.get("confirm_all_in_mode", "always")
        thresh = pref.get("confirm_all_in_threshold", 0)

        should_confirm = True
        if mode == "never":
            should_confirm = False
        elif mode == "threshold":
            if p.chips <= thresh:
                should_confirm = False

        if not should_confirm:
            await self._do_raise(interaction, p.chips)
            return

        view = ActionConfirmView(self.t, self.channel, self.guild, interaction.user.id, self.t.game.raise_bet, [interaction.user.id, p.chips], "⚠️ **Are you sure you want to go ALL IN?**\n*(This will commit all your remaining chips to the pot!)*")
        await interaction.response.send_message(
            "⚠️ **Are you sure you want to go ALL IN?**\n*(This will commit all your remaining chips to the pot!)*",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Custom…", style=discord.ButtonStyle.grey, row=0)
    async def custom(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RaiseCustomModal(self.t, self.channel, self.guild))


# ── Rejoin fee confirmation ───────────────────────────────────────────────────

class RejoinConfirmView(discord.ui.View):
    """Ephemeral prompt shown when a player tries to rejoin during their cooldown."""
    def __init__(self, t: TableState, fee: int, expiry: float, bal: int, min_w: int, max_w: int):
        super().__init__(timeout=30)
        self.t      = t
        self.fee    = fee
        self.expiry = expiry
        self.bal    = bal
        self.min_w  = min_w
        self.max_w  = max_w

    @discord.ui.button(label="Pay fee & join", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = interaction.user.id
        # Re-check in case the cooldown expired while the prompt was open
        live_expiry = self.t.rejoin_cooldowns.get(uid)
        if not live_expiry or time.time() >= live_expiry:
            self.t.rejoin_cooldowns.pop(uid, None)
            await interaction.response.edit_message(
                content="✅ Your cooldown already expired — click **Join** to rejoin normally!",
                view=None,
            )
            self.stop()
            return
        if self.t.closing:
            await interaction.response.edit_message(content="❌ This table is closing.", view=None)
            self.stop()
            return
        # Open the buy-in modal; fee deduction + cooldown clear happens inside on_submit
        await interaction.response.send_modal(
            JoinModal(self.t, self.bal, self.min_w, self.max_w, rejoin_fee=self.fee)
        )
        self.stop()

    @discord.ui.button(label="Wait it out", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Fee Cancelled", view=None)
        self.stop()


# ── Join modal ────────────────────────────────────────────────────────────────

class JoinModal(discord.ui.Modal, title="Buy In"):
    amount = discord.ui.TextInput(label="How many chips to bring to table?", min_length=1, max_length=8)

    def __init__(self, t: TableState, bal: int, min_w: int, max_w: int, rejoin_fee: int = 0):
        title = f"Buy In  (+{rejoin_fee} rejoin fee)" if rejoin_fee > 0 else "Buy In"
        super().__init__(title=title)
        self.t = t; self.bal = bal; self.min_w = min_w; self.max_w = max_w
        self.rejoin_fee = rejoin_fee
        limit_str   = f"{max_w}" if max_w > 0 else "None"
        usable_bal  = bal - rejoin_fee if rejoin_fee > 0 else bal
        fee_note    = f"  [{rejoin_fee} fee deducted]" if rejoin_fee > 0 else ""
        self.amount.placeholder = f"min {min_w} — max {limit_str}  (wallet: {usable_bal}{fee_note})"

    async def on_submit(self, interaction: discord.Interaction):
        if self.t.closing:
            await interaction.response.send_message("❌ This table has been closed.", ephemeral=True)
            return

        # Prevent multi-tabling across the entire bot
        uid = interaction.user.id
        for other_t in tables.values():
            if any(p.user_id == uid for p in other_t.game.players + other_t.game.pending_joins):
                if other_t is not self.t: # If they are at a DIFFERENT table
                    await interaction.response.send_message("❌ You are already seated at another table! You can only play at one table at a time.", ephemeral=True)
                    return

        # Double check cooldown
        if self.rejoin_fee == 0:
            expiry = self.t.rejoin_cooldowns.get(interaction.user.id)
            if expiry and time.time() < expiry:
                await interaction.response.send_message(
                    "❌ You are currently on a rejoin cooldown. Use the **Join** button to check bypass options.",
                    ephemeral=True
                )
                return
        chips = parse_chips(self.amount.value)
        usable_bal = self.bal - self.rejoin_fee
        if chips is None:
            await interaction.response.send_message("❌ Enter a valid amount (e.g. 500, 2k).", ephemeral=True); return
        if chips < self.min_w:
            await interaction.response.send_message(f"❌ Minimum buy-in is **{self.min_w}** {get_chip_emoji(self.t)}.", ephemeral=True); return
        if self.max_w > 0 and chips > self.max_w:
            await interaction.response.send_message(f"❌ Maximum buy-in is **{self.max_w}** {get_chip_emoji(self.t)}.", ephemeral=True); return
        if chips > usable_bal:
            await interaction.response.send_message(
                f"❌ You only have **{usable_bal}** {get_chip_emoji(self.t)} available"
                + (f" (wallet: {self.bal} − {self.rejoin_fee} rejoin fee)." if self.rejoin_fee else "."),
                ephemeral=True); return

        await join_table_execute(interaction, self.t, chips, self.bal, self.rejoin_fee, self.min_w, self.max_w)


class LeaveConfirmView(discord.ui.View):
    """Ephemeral prompt shown when a player clicks Leave."""

    def __init__(self, t: TableState):
        super().__init__(timeout=30)
        self.t = t

    @discord.ui.button(label="Yes, Leave", style=discord.ButtonStyle.red)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        res_msg = await leave_table_execute(interaction.guild, interaction.channel, self.t, interaction)
        await interaction.edit_original_response(content=res_msg, view=None)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Leave cancelled.", view=None)
        self.stop()

# ── Game View ─────────────────────────────────────────────────────────────────

class GameView(discord.ui.View):
    def __init__(self, t: TableState):
        super().__init__(timeout=None)
        self.t = t
        in_hand = t.game.street not in (Street.WAITING, Street.SHOWDOWN)
        table_full = (len(t.game.players) + len(t.game.pending_joins)) >= 12
        self.btn_join.disabled = table_full or t.closing

        self.btn_leave.disabled = t.closing  # <-- ADD THIS LINE

        cp = t.game.current_player()
        if cp and t.game.call_amount(cp) > 0:
            self.btn_check_call.label = "Call"
            self.btn_check_call.style = discord.ButtonStyle.green
        else:
            self.btn_check_call.label = "Check"
            self.btn_check_call.style = discord.ButtonStyle.blurple

        for b in [self.btn_check_call, self.btn_raise, self.btn_fold]:
            b.disabled = not in_hand

    async def _do_action(self, interaction: discord.Interaction, fn, *args):
        try:
            await interaction.response.defer()
        except discord.errors.NotFound:
            # Discord API dropped the interaction token!
            # 1. Inject 15 extra seconds onto the current player's clock
            self.t.turn_deadline += 30
            print(
                f"[Lag Comp] Token Dropped! Player: {interaction.user.display_name} | Table: {self.t.name} | Added 30s.")

            # 2. Tell the user exactly what happened so they know to try again
            try:
                await interaction.channel.send(
                    f"⚠️ <@{interaction.user.id}> Discord lost your click due to lag! "
                    f"30s added to your clock. *Please click your action again.*",
                    delete_after=20
                )
            except (discord.NotFound, discord.HTTPException):
                pass

            # 3. Bail out cleanly so the bot doesn't crash (their turn is NOT skipped)
            return

        ok, msg = fn(*args)
        if not ok:
            await interaction.followup.send(msg, ephemeral=True)
            return

        parts = msg.split("\n")
        street_markers = ["🌊", "↩️", "🏁"]
        if any(m in msg for m in street_markers + ["Showdown"]):
            slog_clear(self.t)

        for part in parts:
            if part.strip():
                slog(self.t, part)

        if self.t.game._hand_result:
            await _process_result(interaction.guild, interaction.channel, self.t)
        else:
            await refresh(interaction.channel, self.t, cosmetics_cache=self.t.cosmetics_cache)

    @discord.ui.button(label="Join", style=discord.ButtonStyle.green, row=0)
    async def btn_join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.t.closing:
            await interaction.response.send_message("❌ This table is closing.", ephemeral=True); return

        uid = interaction.user.id

        # Prevent multi-tabling across the entire bot
        for other_t in tables.values():
            if any(p.user_id == uid for p in other_t.game.players + other_t.game.pending_joins):
                if other_t is not self.t: # If they are at a DIFFERENT table
                    await interaction.response.send_message("❌ You are already seated at another table! You can only play at one table at a time.", ephemeral=True)
                    return

        if any(p.user_id == uid for p in self.t.game.players) or any(pj.user_id == uid for pj in self.t.game.pending_joins):
            await interaction.response.send_message("❌ You are already at this table or waiting to join.", ephemeral=True)
            return

        # Still mid-hand after an AFK kick — will be removed at hand end
        if uid in self.t.game.kicked_users:
            await interaction.response.send_message(
                "❌ You have been kicked and will be removed after this hand.", ephemeral=True); return

        if await db.is_banned(interaction.guild_id, uid, self.t.name):
            await interaction.response.send_message("❌ You are banned from this table.", ephemeral=True); return

        settings  = await db.get_settings(interaction.guild_id)
        min_w     = settings.get("min_wallet", 50)
        max_w     = settings.get("max_wallet", 0)
        bal       = await db.get_balance(uid)

        # ── Rejoin cooldown check ─────────────────────────────────────────────
        expiry = self.t.rejoin_cooldowns.get(uid)
        if expiry and time.time() < expiry:

            # 🚨 NEW: Fee is dynamically set to 2x the table's current Big Blind
            fee = self.t.game.BIG_BLIND * config.REJOIN_FEE_MULTIPLIER

            if fee > 0 and bal >= fee and bal - fee >= min_w:
                await interaction.response.send_message(
                    f"⏳ You recently left this table.\n"
                    f"Your cooldown expires <t:{int(expiry)}:R>.\n\n"
                    f"Pay **{fee}** {get_chip_emoji(self.t)} to the **Jackpot** to bypass and rejoin now?",
                    view=RejoinConfirmView(self.t, fee, expiry, bal, min_w, max_w),
                    ephemeral=True,
                )
            else:
                bypass_note = ""
                if fee > 0:
                    if bal < fee:
                        bypass_note = f"\n*(Bypass fee is **{fee}** {get_chip_emoji(self.t)} — you only have **{bal}**)*"
                    elif bal - fee < min_w:
                        bypass_note = f"\n*(After the **{fee}** {get_chip_emoji(self.t)} fee you'd be below the **{min_w}** chip minimum)*"
                await interaction.response.send_message(
                    f"⏳ You recently left this table.\n"
                    f"Your cooldown expires <t:{int(expiry)}:R>.{bypass_note}",
                    ephemeral=True,
                )
            return

        # Clear any expired entry
        self.t.rejoin_cooldowns.pop(uid, None)

        if bal < min_w:
            await interaction.response.send_message(
                f"❌ Need at least **{min_w}** {get_chip_emoji(self.t)} to join. Wallet: **{bal}** {get_chip_emoji(self.t)}.", ephemeral=True)
            return

        pref = await db.get_player_preference(uid)
        default_bb = pref.get("default_buyin_amount", 0)
        if default_bb > 0 or default_bb == -1:
            if default_bb == -1:
                default_stack = min(bal, max_w) if max_w > 0 else bal
            else:
                bb = self.t.game.BIG_BLIND
                default_stack = default_bb * bb

            if default_stack >= min_w and (max_w == 0 or default_stack <= max_w) and default_stack <= bal:
                await join_table_execute(interaction, self.t, default_stack, bal, rejoin_fee=0, min_w=min_w, max_w=max_w, is_deferred=False)
                return

        await interaction.response.send_modal(JoinModal(self.t, bal, min_w, max_w))


    @discord.ui.button(label="Leave", style=discord.ButtonStyle.red, row=0)
    async def btn_leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.t.closing:
            await interaction.response.send_message("❌ Table is closing — your chips will be returned automatically.",
                                                    ephemeral=True)
            return
        if interaction.user.id in self.t.game.kicked_users:
            await interaction.response.send_message("❌ You have been kicked and will be removed after this hand.",
                                                    ephemeral=True)
            return
        if interaction.user.id in self.t.game.pending_leaves:
            await interaction.response.send_message("❌ You are already queued to leave after this hand.",
                                                    ephemeral=True)
            return

        p = self.t.game.get_player(interaction.user.id)
        pj = next((pj for pj in self.t.game.pending_joins if pj.user_id == interaction.user.id), None)
        if not p and not pj:
            await interaction.response.send_message("❌ You're not at the table.", ephemeral=True)
            return

        pref = await db.get_player_preference(interaction.user.id)
        if not pref.get("confirm_leave", 1):
            await interaction.response.defer(ephemeral=True)
            res_msg = await leave_table_execute(interaction.guild, interaction.channel, self.t, interaction)
            await interaction.followup.send(res_msg, ephemeral=True)
            return

        view = LeaveConfirmView(self.t)
        await interaction.response.send_message(
            "⚠️ Are you sure you want to leave?\n*(This will trigger a 10-minute rejoin cooldown!)*",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Check", style=discord.ButtonStyle.green, row=1)
    async def btn_check_call(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True);
            return

        g = self.t.game
        p = g.get_player(interaction.user.id)
        if p:
            call_amt = g.call_amount(p)
            if call_amt > 0:
                pref = await db.get_player_preference(interaction.user.id)
                mode = pref.get("confirm_call_raise_mode", "always")
                thresh = pref.get("confirm_call_raise_threshold", 0)
                should_confirm = True
                if mode == "never":
                    should_confirm = False
                elif mode == "threshold":
                    if call_amt <= thresh:
                        should_confirm = False

                if should_confirm:
                    view = ActionConfirmView(self.t, interaction.channel, interaction.guild, interaction.user.id, self.t.game.check_or_call, [interaction.user.id], f"⚠️ **Are you sure you want to call {call_amt:,} chips?**")
                    await interaction.response.send_message(
                        f"⚠️ **Are you sure you want to call {call_amt:,} chips?**",
                        view=view,
                        ephemeral=True
                    )
                    return

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
        min_raise_amt = g.last_raise_size if g.last_raise_size > 0 else g.BIG_BLIND
        pot_third = max(call_amt, g.pot // 3) if p else 0
        pot_half = max(call_amt, g.pot // 2) if p else 0
        await interaction.followup.send(
            f"**Raise options** — Pot: {g.pot} {get_chip_emoji(self.t)}  |  Call: {call_amt}  |  Stack: {p.chips if p else '?'}\n",
            view=view, ephemeral=True)

    @discord.ui.button(label="Fold", style=discord.ButtonStyle.red, row=1)
    async def btn_fold(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = interaction.user.id
        if not self.t.game.is_turn(uid):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True)
            return

        p = self.t.game.get_player(uid)

        # Prevent accidental folds of Flush or higher (only works if Flop is out)
        is_strong_hand = False
        if p and p.hole_cards and len(self.t.game.community) >= 3:
            score = evaluator.evaluate(p.hole_cards, self.t.game.community)
            rank_class = evaluator.get_rank_class(score)

            # Treys rank classes: 1 (Straight Flush), 2 (Quads), 3 (Full House), 4 (Flush)
            if rank_class <= 4:
                is_strong_hand = True
                rank_name = evaluator.class_to_string(rank_class)

                # Pass `self` so the confirm view can access `_do_action`
                view = FoldConfirmView(self)

                await interaction.response.send_message(
                    f"⚠️ **Are you sure you want to fold?**\nYou currently have a **{rank_name}**!",
                    view=view,
                    ephemeral=True
                )
                return

        # If not Flush or better, check Fold confirmation preference based on pot size
        if not is_strong_hand:
            pref = await db.get_player_preference(uid)
            mode = pref.get("confirm_fold_mode", "always")
            thresh = pref.get("confirm_fold_threshold", 0)
            should_confirm = True
            if mode == "never":
                should_confirm = False
            elif mode == "threshold":
                if self.t.game.pot <= thresh:
                    should_confirm = False

            if should_confirm:
                view = ActionConfirmView(self.t, interaction.channel, interaction.guild, uid, self.t.game.fold, [uid], "⚠️ **Are you sure you want to fold?**")
                await interaction.response.send_message(
                    "⚠️ **Are you sure you want to fold?**",
                    view=view,
                    ephemeral=True
                )
                return

        # If they don't have a monster hand (or it's pre-flop), fold normally
        await self._do_action(interaction, self.t.game.fold, uid)

    @discord.ui.button(label="My Cards", style=discord.ButtonStyle.grey, row=2)
    async def btn_hole(self, interaction: discord.Interaction, button: discord.ui.Button):
        p = self.t.game.get_player(interaction.user.id)
        if not p or not p.hole_cards:
            await interaction.response.send_message("❌ No cards right now.", ephemeral=True);
            return

        strength = ""
        if self.t.game.community:
            score = evaluator.evaluate(p.hole_cards, self.t.game.community)
            rank = evaluator.class_to_string(evaluator.get_rank_class(score))
            pct = round((1 - score / 7462) * 100, 1)
            strength = f"\n**Hand:** {rank} (top {100 - pct:.0f}%)"

        caption = f"Your hole cards — {p.chips} {get_chip_emoji(self.t)} at table{strength}\n**Cards:** {hand_str(p.hole_cards)}"

        # 1. INSTANTLY send the text so players with slow internet see their cards immediately
        await interaction.response.send_message(caption, ephemeral=True)

        # 2. Generate the heavy image and patch it in a second later
        if USE_IMAGES:
            try:
                file = await asyncio.to_thread(card_images.make_strip, p.hole_cards, 0, True, p.egirl_saro)
                await interaction.edit_original_response(attachments=[file])
            except Exception as e:
                print(f"🚨 [ERROR] {e}")
                import traceback
                traceback.print_exc()

    @discord.ui.button(label="Rankings", style=discord.ButtonStyle.grey, row=2)
    async def btn_rankings(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "**Hand Rankings** *(best → worst)*\nhttps://media.discordapp.net/attachments/1479529924510613624/1501920657582198927/image.png?ex=69fdd41d&is=69fc829d&hm=dada0ee72bc584e7c26c8bd6b1d5e63bc947a865fd4367e9aca0c4a546c7e141&=&format=webp&quality=lossless&width=1136&height=1466",
            ephemeral=True
        )

    @discord.ui.button(label="Wallet", style=discord.ButtonStyle.grey, row=2)
    async def btn_wallet(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        uid = interaction.user.id
        bal, pending = await db.get_wallet(uid)
        p = self.t.game.get_player(uid)
        table_str = f"  |  **At table:** {p.chips} {get_chip_emoji(self.t)}" if p else ""
        pending_str = f"  |  **Pending Cashout:** 🔒 {pending} {get_chip_emoji(self.t)}" if pending > 0 else ""
        await interaction.followup.send(f"**Your Wallet:** {bal} {get_chip_emoji(self.t)}{table_str}{pending_str}", ephemeral=True)

    @discord.ui.button(label="Premove", style=discord.ButtonStyle.grey, row=0)
    async def btn_premove(self, interaction: discord.Interaction, button: discord.ui.Button):
        p = self.t.game.get_player(interaction.user.id)
        if not p:
            await interaction.response.send_message("❌ You're not at the table.", ephemeral=True);
            return
        if self.t.game.is_turn(interaction.user.id):
            await interaction.response.send_message("❌ It's your turn — just act normally.", ephemeral=True);
            return
        view = PremoveView(self.t, interaction.user.id)
        await interaction.response.send_message(
            f"**Set a premove** (chains/fallbacks are supported) — **Current**: {view._get_chain_str()}",
            view=view,
            ephemeral=True
        )

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
            desc = f" — *{info['description']}*" if info.get('description') else ""
            t_lines.append(f"✅ {info['display']} {rarity}{desc}{equipped}")
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
            desc = f" — *{info['description']}*" if info.get('description') else ""
            m_lines.append(f"✅ {info['display']} {rarity}{desc}{equipped}")
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
            except (discord.NotFound, discord.HTTPException):
                pass


class FoldConfirmView(discord.ui.View):
    """Ephemeral prompt shown when a player tries to fold a Flush or better."""

    def __init__(self, parent_view):
        super().__init__(timeout=30)
        self.parent_view = parent_view
        self.t = parent_view.t

    @discord.ui.button(label="Yes, Fold Anyway", style=discord.ButtonStyle.red)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = interaction.user.id
        if not self.t.game.is_turn(uid):
            await interaction.response.edit_message(content="❌ It is no longer your turn.", view=None)
            self.stop()
            return

        await self.parent_view._do_action(interaction, self.t.game.fold, uid)

        # Clean up the ephemeral prompt so it doesn't just sit there
        try:
            await interaction.edit_original_response(content="✅ You folded your hand.", view=None)
        except (discord.HTTPException, discord.NotFound):
            pass

        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Fold cancelled.", view=None)
        self.stop()

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

    DEV_USER_IDS = config.DEV_USER_IDS

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
        zip_path = os.path.join("", clean_zip_name)

        files_to_zip = [
            "poker.db",
            "poker.db-wal",
            "poker.db-shm",
            "tutorial.db",
            "tutorial.db-wal",
            "tutorial.db-shm"
            "eventlog_database.db"
            # "tournament.db",
            # "tournament.db-wal",
            # "tournament.db-shm"
        ]

        try:
            # Force SQLite to flush the WAL to the main DB safely for the main poker DB
            async with db._write_lock:
                conn = await db._get_db()
                await conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

            try:
                import tournament_db
                await tournament_db.checkpoint()
            except ImportError:
                pass
            except Exception:
                traceback.print_exc()

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

    # ── THE AUTO TIMER (Every Hour) ────────────────────────────────────
    backup_times = [
        dt_time(hour=h, minute=30, tzinfo=_tz.utc)
        for h in range(0, 24)
    ]
    @tasks.loop(time=backup_times)
    async def daily_backup(self):
        try:
            user_id = self.DEV_USER_IDS[0]
            user = self.bot.get_user(user_id) or await self.bot.fetch_user(user_id)
            await self._send_backup(user)
        except Exception as e:
            print(f"[Backup Task Error] {e}")
            traceback.print_exc()

    @daily_backup.before_loop
    async def before_daily_backup(self):
        await self.bot.wait_until_ready()

    poker = app_commands.Group(name="poker", description="Texas Hold'em poker")
    pokerset = app_commands.Group(name="pokerset", description="Configure poker settings")
    pokermgr = app_commands.Group(name="pokermgr", description="Poker manager commands")
    pokeradmin = app_commands.Group(name="pokeradmin", description="Poker economy and admin commands")

    @poker.command(name="ping", description="Check the bot's latency")
    async def ping(self, interaction: discord.Interaction):
        # self.bot.latency is in seconds, multiply by 1000 for ms
        latency_ms = round(self.bot.latency * 1000)
        await interaction.response.send_message(f'Pong! 🏓 Latency: {latency_ms}ms', ephemeral=True)

    # ── Table management ──────────────────────────────────────────────────

    @poker.command(name="open", description="[Manager] Open a poker table in this channel")
    @app_commands.describe(name="Table name")
    async def open_table(self, interaction: discord.Interaction, name: str = "Poker Table"):
        await interaction.response.defer(ephemeral=True)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True);
            return
        key = (interaction.guild_id, interaction.channel_id)
        if key in tables:
            await interaction.followup.send("❌ A table is already running in this channel. Close it first.", ephemeral=True)
            return

        for (gid, cid), t in tables.items():
            if gid == interaction.guild_id and not getattr(t, 'is_tournament', False):
                await interaction.followup.send(
                    f"❌ A regular poker table is already running in <#{cid}>. Close it first.", ephemeral=True);
                return
        t = TableState(name, interaction.user.id, interaction.user.name)
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
                except (discord.NotFound, discord.HTTPException):
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
        if not getattr(t, 'is_tournament', False):
            t.game.SMALL_BLIND = settings["small_blind"]
            t.game.BIG_BLIND   = settings["big_blind"]
            t.game.MIN_BUYIN = settings.get("min_wallet", 50)
        t.resend_threshold = settings.get("resend_after_msgs", TABLE_RESEND_MSGS)

        is_tourney = getattr(t, 'is_tournament', False)
        if is_tourney:
            import tournament_db as tdb
            _ret = tdb.return_chips
            _clear = tdb.clear_chips_in_play
        else:
            _ret = db.return_chips
            _clear = db.clear_chips_in_play

        for uid in list(t.game.pending_leaves):
            p = t.game.get_player(uid)
            if p:
                total_to_return = p.chips + p.pending_rebuy
                if total_to_return > 0:
                    await _ret(uid, total_to_return)
                await _clear(uid)
                t.game.players.remove(p)

        t.game.pending_leaves.clear()
        t.game.kicked_users.clear()

        if getattr(t, 'is_tournament', False):
            import tournament_db as tdb
            bb = t.game.BIG_BLIND

            # 1. Grab seated players who can play
            active = [p for p in t.game.players if
                      (p.chips + p.pending_rebuy) >= bb and p.user_id not in t.game.pending_leaves]

            # 2. Grab joining players who can play
            pending_with_chips = [p for p in t.game.pending_joins if
                                  (p.chips + p.pending_rebuy) >= bb]

            # 3. Combine their IDs for the strict dominance check
            active_uids = [p.user_id for p in active] + [p.user_id for p in pending_with_chips]

            dominance_warning = await tdb.get_team_dominance_warning(active_uids)
            if dominance_warning:
                await interaction.followup.send(f"⚠️ **Team stats abuse guard: **\n{dominance_warning}",
                                                ephemeral=True)
                return

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

        if getattr(t, 'is_tournament', False):
            await interaction.followup.send("❌ This is a tournament table. Please use `/tourneymgr kick` instead.", ephemeral=True)
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

        # Check if current channel table is a tournament table
        key = (interaction.guild_id, interaction.channel_id)
        t = get_table(key)
        if t and getattr(t, 'is_tournament', False):
            await interaction.followup.send("❌ This is a tournament table. Please use tournament moderation tools instead.", ephemeral=True)
            return

        if table_name:
            for t_state in tables.values():
                if t_state.name.lower() == table_name.lower() and getattr(t_state, 'is_tournament', False):
                    await interaction.followup.send("❌ That is a tournament table. Please use tournament moderation tools instead.", ephemeral=True)
                    return

        # 2. Persist ban to DB
        added = await db.ban_player(interaction.guild_id, user.id, user.display_name,
                                    interaction.user.id, table_name)
        scope = f"table **{table_name}**" if table_name else "**all tables** (server-wide)"

        kicked_from = ""

        # Grab the single active table for this server (excluding tournament tables)
        active = next(((cid, table) for (gid, cid), table in tables.items() if gid == interaction.guild_id and not getattr(table, 'is_tournament', False)), None)

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

        if getattr(t, 'is_tournament', False):
            await interaction.followup.send("❌ This is a tournament table. Please use `/tourneymgr forcefold` instead.", ephemeral=True)
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
        p = t.game.get_player(target.id) if (t and not getattr(t, 'is_tournament', False)) else None
        table_str = f"\n**At table:** {p.chips} <:poker_chip:1490458259855773707>" if p else ""
        pending_str = f"\n**Pending Cashout:** 🔒 {pending} <:poker_chip:1490458259855773707>" if pending > 0 else ""
        label = f"**{target.display_name}'s Wallet**" if user else "**Your Wallet**"
        await interaction.followup.send(f"{label}: {bal} <:poker_chip:1490458259855773707>{table_str}{pending_str}", ephemeral=False)

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

        if getattr(t, 'is_tournament', False):
            await interaction.followup.send("❌ Tipping is disabled on tournament tables.", ephemeral=True)
            return

        if interaction.user.id == t.manager_id:
            await interaction.followup.send("❌ You can't tip yourself.", ephemeral=True)
            return

        p = t.game.get_player(interaction.user.id)
        table_chips = p.chips if p else 0
        wallet_bal = await db.get_balance(interaction.user.id)

        # Pull from wallet first then table
        from_wallet = min(tip, wallet_bal)
        from_table = tip - from_wallet

        if from_table > 0 and t.game.street != Street.WAITING:
            await interaction.followup.send("❌ You cannot tip chips from the table while a hand is in progress. Wait for the hand to finish.", ephemeral=True)
            return

        if from_table > table_chips:
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
        except Exception as e:
            print(f"🚨 [ERROR] {e}")
            import traceback
            traceback.print_exc()

        # Log and send
        await db.add_chips(interaction.user.id, interaction.user.display_name,
                           manager_id, manager_name, tip, f"Tip from {interaction.user.display_name}")

        await post_tip_log(interaction.channel, t, interaction.user.id, interaction.user.display_name, tip, manager_id,
                           manager_name)
        await db.record_tip(interaction.user.id, interaction.user.display_name, tip)
        await db.log_currency_event(interaction.user.id, "Tip", -tip, f"Tipped {manager_name}")
        await db.log_currency_event(manager_id, "Tip", tip, f"Tip from {interaction.user.display_name}")

        await interaction.followup.send(
            f"💸 **{interaction.user.display_name}** tipped **{tip}** {config.POKER_CHIP_EMOJI} to **{manager_name}**!", ephemeral=False)

        # Refresh the UI to reflect the deducted seated chips
        if t.game.street == Street.WAITING:
            await refresh(interaction.channel, t, cosmetics_cache=t.cosmetics_cache)

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
        # Reduced width to 30 chars to prevent mobile line-wrapping
        table_lines.append(f"{'':3}{'Player':<18} {'Win%':>4} {'Net':>6}")
        table_lines.append("─" * 34)
        for i, r in enumerate(rows):
            rank = i + 1
            wp = f"{r['hands_won'] / r['hands_played'] * 100:.0f}%" if r['hands_played'] else "—"
            net = r['net_chips']
            sign = "+" if net >= 0 else ""
            uname = r['username'][:16]  # Truncate name heavily for mobile
            medal = MEDALS.get(rank, f"{rank}. ")
            you_tag = "<" if r['user_id'] == caller_id else ""
            table_lines.append(f"{medal:<3}{uname:<16} {wp:>4} {sign + str(net):>8} {you_tag}")
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
                    f"Net **{caller_sign}{caller_net}** <:poker_chip:1490458259855773707>  ·  "
                    f"Wallet **{caller_row['wallet']}** <:poker_chip:1490458259855773707>"
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
        await interaction.response.defer(ephemeral=hidden)

        row = await db.get_player_stats(interaction.user.id)
        if not row:
            await interaction.followup.send("No stats yet!", ephemeral=hidden)
            return

        rank = await db.get_player_rank(interaction.user.id)
        rank_str = f"#{rank}" if rank else "Unranked"

        # Fire up the interactive View!
        view = StatsView(interaction.user, row, rank_str)
        await interaction.followup.send(embed=view.build_basic_embed(), view=view, ephemeral=hidden)

    # ── Manager settings commands ─────────────────────────────────────────
    @pokermgr.command(name="addchips", description="[Manager] Add chips to a player's wallet")
    @app_commands.describe(user="Player", amount="Chips to add", note="Optional reason")
    async def mgr_addchips(self, interaction: discord.Interaction, user: discord.Member, amount: int, note: str = ""):
        await interaction.response.defer(ephemeral=False)

        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        if config.ADD_CHIPS_CHANNELS and interaction.channel_id not in config.ADD_CHIPS_CHANNELS:
            mentions = ", ".join(f"<#{cid}>" for cid in config.ADD_CHIPS_CHANNELS)
            await interaction.followup.send(f"❌ This command is restricted to: {mentions}", ephemeral=True)
            return

        if amount <= 0:
            await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)
            return

        new_bal = await db.add_chips(interaction.user.id, interaction.user.display_name,
                                     user.id, user.display_name, amount, note)

        desc = f"Staff Add: {note}" if note else "Staff Add"
        await db.log_currency_event(user.id, "Cash In", amount, desc)

        await interaction.followup.send(
            f"✅ **+{amount}** chips → **{user.mention}** |  Balance: **{new_bal}** <:poker_chip:1490458259855773707>"
            + (f"\n> {note}" if note else ""), ephemeral=False, allowed_mentions=discord.AllowedMentions(users=True))

    @pokermgr.command(name="removechips", description="[Manager] Remove chips from a player's wallet")
    @app_commands.describe(user="Player", amount="Chips to remove", note="Optional reason")
    async def mgr_removechips(self, interaction: discord.Interaction, user: discord.Member, amount: int,
                              note: str = ""):
        await interaction.response.defer(ephemeral=False)

        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        import config
        if config.REMOVE_CHIPS_CHANNELS and interaction.channel_id not in config.REMOVE_CHIPS_CHANNELS:
            mentions = ", ".join(f"<#{cid}>" for cid in config.REMOVE_CHIPS_CHANNELS)
            await interaction.followup.send(f"❌ This command is restricted to: {mentions}", ephemeral=True)
            return

        if amount <= 0:
            await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)
            return

        bal_before = await db.get_balance(user.id)
        if amount > bal_before:
            await interaction.followup.send(
                f"❌ **{user.display_name}** only has **{bal_before}** <:poker_chip:1490458259855773707> in their wallet. You cannot remove **{amount}**.",
                ephemeral=True)
            return

        new_bal = await db.add_chips(interaction.user.id, interaction.user.display_name,
                                     user.id, user.display_name, -amount, note)

        expected = bal_before - amount
        if new_bal > expected:
            await interaction.followup.send(
                f"⚠️ Only **{bal_before - new_bal}** chips could be removed — **{user.display_name}**'s balance changed concurrently. New balance: **{new_bal}** <:poker_chip:1490458259855773707>",
                ephemeral=True)
            return

        desc = f"Staff Remove: {note}" if note else "Staff Remove"
        await db.log_currency_event(user.id, "Cash Out", -amount, desc)

        await interaction.followup.send(
            f"✅ **-{amount}** chips from **{user.mention}** |  Balance: **{new_bal}** <:poker_chip:1490458259855773707>"
            + (f"\n> {note}" if note else ""), ephemeral=False, allowed_mentions=discord.AllowedMentions(users=True))

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
        t.manager_name = user.name  # Update the saved name!

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

    @pokerset.command(name="table", description="[Manager] Apply a global Stakes & Buy-in preset")
    @app_commands.describe(size="Choose the table size preset")
    @app_commands.choices(size=[
        app_commands.Choice(name="Small Table (5/10 Blinds, 50 to 1b Buy-in)", value="small"),
        app_commands.Choice(name="Medium Table (15/30 Blinds, 150 to 3k Buy-in)", value="medium"),
        app_commands.Choice(name="High Table (25/50 Blinds, 250 to 5k Buy-in)", value="high"),
    ])
    async def set_preset(self, interaction: discord.Interaction, size: app_commands.Choice[str]):
        # 1. Manager Check
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)

        # 2. Assign values based on selection
        if size.value == "small":
            sb, bb, min_b, max_b = 5, 10, 50, 1000
        elif size.value == "medium":
            sb, bb, min_b, max_b = 15, 30, 150, 3000
        elif size.value == "high":
            sb, bb, min_b, max_b = 25, 50, 250, 5000

        await db.set_settings(
            interaction.guild_id,
            small_blind=sb,
            big_blind=bb,
            min_wallet=min_b,
            max_wallet=max_b
        )

        # 4. Confirmation
        preset_name = size.name.split(" (")[0]  # Cleans up the string to just "Small Table"
        await interaction.followup.send(
            f"✅Applied: **{preset_name}**\n"
            f"Blinds: {sb}/{bb} | Buy-in: {min_b} to {max_b}"
        )

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

        is_tourney = getattr(t, 'is_tournament', False)
        if is_tourney:
            import tournament_db as tdb
            _bal = tdb.get_balance
            _deduct = tdb.deduct_chips
            _return = tdb.return_chips
            _mark = tdb.mark_chips_in_play
            max_w = getattr(t.game, "MAX_BUYIN", 0)
        else:
            _bal = db.get_balance
            _deduct = db.deduct_chips
            _return = db.return_chips
            _mark = db.mark_chips_in_play
            settings = await db.get_settings(interaction.guild_id)
            max_w = settings.get("max_wallet", 0)

        wallet_bal = await _bal(interaction.user.id)
        if chips > wallet_bal:
            await interaction.followup.send(f"❌ You only have **{wallet_bal}** {get_chip_emoji(t)} in your wallet.", ephemeral=True);
            return

        current_stack = 0
        if p:
            current_stack = p.chips + p.pending_rebuy
        elif pj:
            current_stack = pj.chips + pj.pending_rebuy

        if max_w > 0 and (current_stack + chips) > max_w:
            allowed = max_w - current_stack
            actual_max = max(0, min(allowed, wallet_bal))
            await interaction.followup.send(
                f"❌ Maximum table stack is **{max_w}** {get_chip_emoji(t)}. You can only add up to **{actual_max}** {get_chip_emoji(t)}.",
                ephemeral=True);
            return

        ok = await _deduct(interaction.user.id, chips)
        if not ok:
            await interaction.followup.send(f"❌ Failed to deduct {get_chip_emoji(t)}.", ephemeral=True);
            return

        msg = t.game.queue_rebuy(interaction.user.id, chips, emoji=get_chip_emoji(t))

        # FIXED: Check if queue failed, and refund if it did
        if msg.startswith("❌"):
            await _return(interaction.user.id, chips)
            await interaction.followup.send(msg, ephemeral=True)
            return

        await _mark(interaction.user.id, interaction.user.display_name, chips)

        await interaction.followup.send(f"✅ Chips queued successfully!", ephemeral=True)
        await interaction.channel.send(msg)
        if t.game.street == Street.WAITING:
            await refresh(interaction.channel, t)

    @poker.command(name="request_cashout", description="Lock chips for withdrawal and notify staff")
    @app_commands.describe(amount="Chips to cash out", note="Additonal notes")
    async def request_cashout(self, interaction: discord.Interaction, amount: str, note: app_commands.Range[str, 0, 50] = ""):
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

        desc = f"Requested Cashout: {note}" if note else "Requested Cashout"
        await db.log_currency_event(interaction.user.id, "Cash Out", -chips, desc)

        if config.CASHOUT_CHANNEL_ID:
            try:
                ch = interaction.guild.get_channel(config.CASHOUT_CHANNEL_ID)
                if ch:
                    ticket_msg = f"**Username:** {interaction.user.mention}\n**Amount:** {chips} <:poker_chip:1490458259855773707>"
                    if note: ticket_msg += f"\n**Notes:** {note}"
                    await ch.send(ticket_msg)
            except Exception as e:
                print(f"🚨 [ERROR] {e}")
                import traceback
                traceback.print_exc()

        # FIXED: Send the final receipt ephemerally
        await interaction.followup.send(
            f"✅ Locked **{chips}** <:poker_chip:1490458259855773707> for cashout. Staff have been notified in the cashouts channel.",
            ephemeral=True
        )

    @pokermgr.command(name="pay_cashout", description="[Manager] Deduct paid chips from pending and send receipt")
    @app_commands.describe(user="Player who was paid", amount="Amount of chips paid")
    async def pay_cashout(self, interaction: discord.Interaction, user: discord.Member, amount: int):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        if config.CASHOUT_CHANNEL_ID:
            if interaction.channel_id != config.CASHOUT_CHANNEL_ID:
                await interaction.followup.send(f"❌ This command can only be used in <#{config.CASHOUT_CHANNEL_ID}>.",
                                                ephemeral=True)
                return

        if amount <= 0:
            await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)
            return

        ok = await db.pay_cashout(user.id, amount)
        if not ok:
            _, pending = await db.get_wallet(user.id)
            await interaction.followup.send(
                f"❌ **{user.display_name}** only has **{pending}** <:poker_chip:1490458259855773707> pending. You cannot deduct {amount}.",
                ephemeral=True);
            return

        await interaction.followup.send(
            f"✅ Successfully deducted **{amount}** <:poker_chip:1490458259855773707> from **{user.mention}**'s pending cashouts.")

    @pokeradmin.command(name="economy", description="[Admin] View total chips in circulation")
    async def economy(self, interaction: discord.Interaction):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_IDS):
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
        embed.add_field(name="Available in Wallets", value=f"{avail:,} <:poker_chip:1490458259855773707>", inline=False)
        embed.add_field(name="Locked Pending Cashouts", value=f"{pending:,} <:poker_chip:1490458259855773707>", inline=False)
        embed.add_field(name="Currently at Tables", value=f"{in_play:,} <:poker_chip:1490458259855773707>", inline=False)
        embed.add_field(name="Total Circulation", value=f"**{total:,} <:poker_chip:1490458259855773707>**", inline=False)

        await interaction.followup.send(embed=embed)

    @pokeradmin.command(name="revenue", description="[Admin] View projected house profits")
    async def revenue(self, interaction: discord.Interaction):
        if not (
                interaction.user.guild_permissions.administrator
                or interaction.user.id in self.DEV_USER_IDS
                or interaction.guild.get_role(1010238899320270999) in interaction.user.roles
        ):
            await interaction.response.send_message("❌ Server Administrators only.", ephemeral=True);
            return
        await interaction.response.defer(ephemeral=False)

        stats = await db.get_revenue_stats()

        embed = discord.Embed(title="📈 House Revenue (4% Tax)", color=0xf1c40f)
        embed.add_field(name="Past 24 Hours", value=f"{stats['daily']:,} <:poker_chip:1490458259855773707>", inline=True)
        embed.add_field(name="Past 7 Days", value=f"{stats['weekly']:,} <:poker_chip:1490458259855773707>", inline=True)
        embed.add_field(name="Past 30 Days", value=f"{stats['monthly']:,} <:poker_chip:1490458259855773707>", inline=True)
        embed.add_field(name="All-Time Profit", value=f"**{stats['all_time']:,} <:poker_chip:1490458259855773707>**", inline=False)

        await interaction.followup.send(embed=embed)

    @pokeradmin.command(name="salt", description="[Admin] View daily house profits in a monthly calendar layout")
    @app_commands.describe(month="YYYY-MM format (e.g. 2026-07) - defaults to current month")
    async def salt(self, interaction: discord.Interaction, month: str = None):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_IDS):
            await interaction.response.send_message("❌ Server Administrators only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=False)

        # 1. Parse Year & Month
        import re
        from datetime import datetime
        if month:
            if not re.match(r"^\d{4}-\d{2}$", month):
                await interaction.followup.send("❌ Invalid month format. Use YYYY-MM (e.g. 2026-07).")
                return
            target_year_month = month
        else:
            target_year_month = datetime.utcnow().strftime("%Y-%m")

        year_str, month_str = target_year_month.split("-")
        y_val = int(year_str)
        m_val = int(month_str)

        # 2. Query Revenue Data
        db_conn = await db._get_db()
        daily_totals = {}
        query = """
            SELECT ts, amount 
            FROM house_revenue 
            WHERE ts LIKE ?
        """
        try:
            async with db_conn.execute(query, (f"{target_year_month}%",)) as c:
                rows = await c.fetchall()
                for ts_str, amt in rows:
                    try:
                        date_part = ts_str.split("T")[0]
                        day = int(date_part.split("-")[2])
                        daily_totals[day] = daily_totals.get(day, 0) + amt
                    except Exception:
                        pass
        except Exception as e:
            print(f"Error querying house revenue: {e}")

        # 3. Generate Calendar Grid Image
        import calendar
        import os
        from PIL import Image, ImageDraw, ImageFont

        try:
            cal = calendar.Calendar(firstweekday=6)
            weeks = cal.monthdayscalendar(y_val, m_val)
        except Exception:
            await interaction.followup.send("❌ Invalid year or month values.")
            return

        month_name = calendar.month_name[m_val]

        def format_revenue(val: int) -> str:
            if val == 0:
                return "0"
            if val >= 1_000_000:
                val_m = val / 1_000_000
                if val_m == int(val_m):
                    return f"{int(val_m)}M"
                return f"{val_m:.1f}M"
            elif val >= 100_000:
                val_k = val / 1_000
                if val_k == int(val_k):
                    return f"{int(val_k)}K"
                return f"{val_k:.1f}K"
            return str(val)

        # Colors for the PIL image
        bg_color = (19, 19, 26)       # Deep slate/black
        card_bg = (33, 33, 47)        # Lighter slate
        empty_card_bg = (24, 24, 33)  # Muted card for empty days
        header_color = (255, 255, 255)# White
        text_muted = (130, 130, 160)  # Muted grey-blue
        cyan_color = (56, 189, 248)   # Cyan for date
        green_color = (74, 222, 128)  # Bright green for positive revenue
        grey_color = (110, 120, 140)   # Grey for zero revenue

        # Fonts
        base_dir = os.path.dirname(os.path.abspath(__file__))
        font_path_bold = os.path.join(base_dir, "assets", "Roboto-Bold.ttf")
        font_path_medium = os.path.join(base_dir, "assets", "Roboto-Medium.ttf")
        try:
            font_title = ImageFont.truetype(font_path_bold, 34)
            font_header = ImageFont.truetype(font_path_bold, 22)
            font_date = ImageFont.truetype(font_path_medium, 18)
            font_rev = ImageFont.truetype(font_path_bold, 24)
            font_total = ImageFont.truetype(font_path_bold, 26)
        except Exception:
            font_title = font_header = font_date = font_rev = font_total = ImageFont.load_default()

        num_weeks = len(weeks)
        padding = 20
        card_w = 115
        card_h = 95
        gap = 10
        
        header_h = 90
        weekdays_h = 45
        grid_h = card_h * num_weeks + gap * (num_weeks - 1)
        footer_h = 70
        
        img_w = padding * 2 + card_w * 7 + gap * 6
        img_h = padding + header_h + weekdays_h + grid_h + footer_h + padding

        img = Image.new("RGB", (img_w, img_h), bg_color)
        draw = ImageDraw.Draw(img)

        # Title
        title_text = f"Poker Revenue — {month_name} {y_val}"
        draw.text((padding, padding + 15), title_text, font=font_title, fill=header_color)

        # Weekdays
        weekdays = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"]
        start_y = padding + header_h
        for idx, day_lbl in enumerate(weekdays):
            x = padding + idx * (card_w + gap)
            try:
                bbox = draw.textbbox((0, 0), day_lbl, font=font_header)
                text_w = bbox[2] - bbox[0]
            except Exception:
                text_w = len(day_lbl) * 12
            text_x = x + (card_w - text_w) // 2
            draw.text((text_x, start_y), day_lbl, font=font_header, fill=text_muted)

        # Cards Grid
        start_grid_y = start_y + weekdays_h
        for row_idx, week in enumerate(weeks):
            y = start_grid_y + row_idx * (card_h + gap)
            for col_idx, day in enumerate(week):
                x = padding + col_idx * (card_w + gap)
                
                if day == 0:
                    draw.rounded_rectangle([x, y, x + card_w, y + card_h], radius=6, fill=empty_card_bg)
                    try:
                        bbox = draw.textbbox((0, 0), ".", font=font_date)
                        text_w = bbox[2] - bbox[0]
                        text_h = bbox[3] - bbox[1]
                    except Exception:
                        text_w, text_h = 6, 6
                    draw.text((x + (card_w - text_w) // 2, y + (card_h - text_h) // 2 - 5), ".", font=font_date, fill=(50, 50, 70))
                else:
                    draw.rounded_rectangle([x, y, x + card_w, y + card_h], radius=6, fill=card_bg)
                    draw.text((x + 10, y + 8), f"{day:02d}", font=font_date, fill=cyan_color)
                    
                    rev_val = daily_totals.get(day, 0)
                    rev_str = format_revenue(rev_val)
                    color = green_color if rev_val > 0 else grey_color
                    
                    try:
                        bbox = draw.textbbox((0, 0), rev_str, font=font_rev)
                        text_w = bbox[2] - bbox[0]
                        text_h = bbox[3] - bbox[1]
                    except Exception:
                        text_w, text_h = len(rev_str) * 12, 18
                    draw.text((x + (card_w - text_w) // 2, y + 48), rev_str, font=font_rev, fill=color)

        # Footer
        total_rev = sum(daily_totals.values())
        footer_y = start_grid_y + grid_h + 20
        draw.line([padding, footer_y, img_w - padding, footer_y], fill=(40, 40, 60), width=1)
        
        total_text = f"Total Monthly Revenue: {total_rev:,} Chips"
        draw.text((padding, footer_y + 20), total_text, font=font_total, fill=green_color)

        # Save and send
        temp_img_path = f"revenue_{target_year_month}.png"
        img.save(temp_img_path)

        file = discord.File(temp_img_path, filename=f"revenue_{target_year_month}.png")
        await interaction.followup.send(file=file)

        # Clean up
        try:
            if os.path.exists(temp_img_path):
                os.remove(temp_img_path)
        except Exception as e:
            print(f"Error removing temp calendar image: {e}")

    @pokeradmin.command(name="adjustrevenue", description="[Admin] Manually adjust all-time revenue tracker")
    @app_commands.describe(amount="Amount to add (or negative to subtract)")
    async def adjustrevenue(self, interaction: discord.Interaction, amount: int):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Server Administrators only.", ephemeral=True);
            return
        await interaction.response.defer(ephemeral=False)

        db_conn = await db._get_db()
        async with db._write_lock:
            # source='adjustment' keeps this out of daily/weekly/monthly windows
            # so staff payouts don't drag down time-windowed revenue stats
            await db_conn.execute(
                "INSERT INTO house_revenue (ts, amount, source) VALUES (?, ?, 'adjustment')",
                (datetime.utcnow().isoformat(), amount)
            )
            await db_conn.commit()

        word = "Added" if amount >= 0 else "Deducted"
        await interaction.followup.send(
            f"✅ {word} **{abs(amount)}** <:poker_chip:1490458259855773707> to the House Revenue tracker."
            f"*(This adjustment is reflected in all-time totals only — not daily/weekly/monthly.)*"
        )

    @pokeradmin.command(name="set_activity", description="[Dev] Change a player's last_activity timestamp")
    @app_commands.describe(
        user="The player to modify",
        timestamp="Discord timestamp (e.g. <t:1716388800:R>) or raw Unix epoch"
    )
    async def set_activity(self, interaction: discord.Interaction, user: discord.Member, timestamp: str):
        # 1. Dev Auth Check
        if interaction.user.id not in self.DEV_USER_IDS:
            await interaction.response.send_message("❌ **Access Denied.** Devs only.", ephemeral=True)
            return

        import re
        from datetime import datetime

        # 2. Parse the Discord timestamp format (<t:1234567890> or <t:1234567890:R>)
        # Or accept a raw unix integer if you just type the numbers manually.
        match = re.search(r"<t:(\d+)", timestamp)
        if match:
            unix_ts = int(match.group(1))
        elif timestamp.isdigit():
            unix_ts = int(timestamp)
        else:
            await interaction.response.send_message(
                "❌ Invalid format. Please use a Discord timestamp like `<t:1716388800>` or `<t:1716388800:R>`.",
                ephemeral=True)
            return

        # 3. Convert Unix Epoch -> UTC Datetime -> ISO 8601 string (what your database uses)
        try:
            # utcfromtimestamp perfectly matches your database's utcnow() formatting
            new_iso = datetime.utcfromtimestamp(unix_ts).isoformat()
        except (ValueError, OSError, OverflowError) as e:
            await interaction.response.send_message(f"❌ Failed to parse date: {e}", ephemeral=True)
            return

        # 4. Update the database directly
        import database as db
        conn = await db._get_db()
        try:
            await conn.execute("UPDATE wallets SET last_activity = ? WHERE user_id = ?", (new_iso, user.id))
            await conn.commit()

            # Show a success message with the dynamically formatted Discord timestamp
            await interaction.response.send_message(
                f"✅ Successfully backdated **{user.display_name}**'s last activity to <t:{unix_ts}:F>!\n"
                f"*(Database saved exactly as:* `{new_iso}`*)*",
                ephemeral=True
            )
        except Exception as e:
            traceback.print_exc()
            await interaction.response.send_message(f"❌ Database error: {e}", ephemeral=True)

    @pokeradmin.command(name="adjustjackpot", description="[Admin] Manually adjust the global jackpot")
    @app_commands.describe(amount="Amount to add (or negative to subtract)")
    async def adjustjackpot(self, interaction: discord.Interaction, amount: int):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_IDS):
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
            f"✅ {action} **{abs(amount):,}** <:poker_chip:1490458259855773707> {prep} the jackpot! New total: **{new_jp:,}** <:poker_chip:1490458259855773707>"
        )

    @poker.command(name="jackpot", description="View the current casino jackpot!")
    async def jackpot_cmd(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False)
        jp, egirl_cut, rf_cut, sf_cut, quads_cut = await jackpot.get_jackpot_display_cuts()

        desc = (
            "_ _\n"
            f"**Total:  {jp:,} <:poker_chip:1490458259855773707>**\n\n"
            f"- **Quads** : {quads_cut:,} <:poker_chip:1490458259855773707>\n\n"
            f"- **Straight Flush** : {sf_cut:,} <:poker_chip:1490458259855773707>\n\n"
            f"- **Royal Flush** : {rf_cut:,} <:poker_chip:1490458259855773707>\n\n"
            f"- **Shiny Card Win** : {egirl_cut:,} <:poker_chip:1490458259855773707>\n\n"
            "_ _"
        )

        embed = discord.Embed(
            title="<a:md_den:996127219019690034> Jackpot",
            description=desc,
            color=0xFFD700  # Decimal 16766720
        )
        embed.set_thumbnail(
            url="https://media.discordapp.net/attachments/1478125269285081211/1488098208986038282/3d-casino-poker-cards-and-playing-chips-on-black-background-illustration-free-vector.png?ex=69cb8af4&is=69ca3974&hm=58f")
        embed.set_footer(text="• 5% Q, 20% SF, 60% RF, 80% Shiny")

        await interaction.followup.send(embed=embed)

    @pokeradmin.command(name="check_inactive", description="[Admin] Check who will be wiped soon")
    async def check_inactive(self, interaction: discord.Interaction):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_IDS):
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)

        # 🚨 FIXED: Actually fetch the at-risk list instead of forcing it to []
        at_risk = await db.get_players_at_risk()
        inactive = await db.get_inactive_players()

        embed = discord.Embed(title="🔍 Inactivity Report", color=0xe74c3c)

        if at_risk:
            risk_lines = []
            for p in at_risk[:15]:  # Show top 15
                days_ago = p.get("days_inactive", 0)
                total = p["balance"]
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
                total = p["balance"]
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

    @poker.command(name="myactivity", description="Check activity status and see if a player is at risk")
    @app_commands.describe(user="Player to check (Admins/Devs only, leave blank for yourself)")
    async def myactivity(self, interaction: discord.Interaction, user: discord.Member = None):
        target = user or interaction.user

        # 🚨 Permission Check: Block regular users from checking others
        if target.id != interaction.user.id:
            if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_IDS):
                await interaction.response.send_message(
                    "❌ Only Administrators or Devs can check other players' activity.", ephemeral=True)
                return

        await interaction.response.defer(ephemeral=True)
        stats = await db.get_player_activity_stats(target.id)

        if not stats:
            name_str = "You don't" if target.id == interaction.user.id else f"**{target.display_name}** doesn't"
            await interaction.followup.send(f"❌ {name_str} have a wallet yet!", ephemeral=True)
            return

        # Calculate wipe timestamp
        raw = stats['last_activity']
        last_active = datetime.fromisoformat(raw).replace(tzinfo=_tz.utc) if isinstance(raw, str) else raw

        # 1. Find exactly when their 2-day clock runs out
        exact_expiration = last_active + timedelta(days=db.INACTIVITY_DAYS)

        # 2. Snap to the NEXT scheduled bot wipe (03:30 UTC)
        if exact_expiration.hour < 3 or (exact_expiration.hour == 3 and exact_expiration.minute <= 30):
            wipe_date = exact_expiration.replace(hour=3, minute=30, second=0, microsecond=0)
        else:
            wipe_date = (exact_expiration + timedelta(days=1)).replace(hour=3, minute=30, second=0, microsecond=0)

        wipe_timestamp = int(wipe_date.timestamp())

        # Build embed
        embed = discord.Embed(title=f"📊 Activity Status: {stats['username']}", color=0x3498db)

        # Basic Info
        total_chips = stats['balance'] + stats['pending_cashout']
        embed.add_field(name="💰 Total Chips", value=f"{total_chips:,} chips", inline=True)
        embed.add_field(name="📅 Last Active", value=f"<t:{int(last_active.timestamp())}:R>", inline=True)

        # Wipe deadline
        if stats['days_until_wipe'] > 0:
            embed.add_field(name="⏰ Chips Wiped", value=f"<t:{wipe_timestamp}:R>", inline=True)
        else:
            embed.add_field(name="⏰ Chips Wiped", value="**Next cleanup run!**", inline=True)

        # Progress Bar Helper Function
        def progress_bar(current: int, required: int, length: int = 10) -> str:
            filled = min(int((current / max(required, 1)) * length), length)
            done = "🟩" * filled
            empty = "⬜" * (length - filled)
            pct = min(int((current / max(required, 1)) * 100), 100)
            return f"{done}{empty}  **{current}/{required}** ({pct}%)"

        # Hands Progress
        hands_bar = progress_bar(stats['recent_hands'], db.MIN_HANDS_PER_PERIOD)
        hands_status = "✅" if stats['meets_hand_requirement'] else "❌"
        embed.add_field(name=f"🃏 Hands Played {hands_status}", value=hands_bar, inline=False)

        # Chips Wagered Progress (if enabled)
        if db.MIN_CHIPS_WAGERED > 0:
            chips_bar = progress_bar(stats['recent_chips_wagered'], db.MIN_CHIPS_WAGERED)
            chips_status = "✅" if stats['meets_wager_requirement'] else "❌"
            embed.add_field(name=f"💵 Chips Wagered {chips_status}", value=chips_bar, inline=False)

            # Status logic
            days_left = stats['days_until_wipe']
            is_self = target.id == interaction.user.id
            pronoun = "You are" if is_self else "They are"
            action_pronoun = "You" if is_self else "They"

            if stats['meets_hand_requirement'] and (db.MIN_CHIPS_WAGERED == 0 or stats['meets_wager_requirement']):
                status = "🟢 **SAFE** - Requirements met!"
                color = 0x2ecc71
                action = f"{pronoun} fully protected from the next wipe."
            elif days_left >= 2:
                status = "🟢 **SAFE** - Time remaining."
                color = 0x2ecc71
                needed = db.MIN_HANDS_PER_PERIOD - stats['recent_hands']
                action = f"{action_pronoun} need to play {needed} more hand(s) before the deadline."
            elif days_left == 1:
                status = "🟡 **WARNING** - 1 day left!"
                color = 0xf39c12
                needed = db.MIN_HANDS_PER_PERIOD - stats['recent_hands']
                action = f"**{action_pronoun} need to play {needed} more hand(s) TODAY!**"
            else:
                status = "🔴 **CRITICAL** - Wipe imminent!"
                color = 0xe74c3c
                needed = db.MIN_HANDS_PER_PERIOD - stats['recent_hands']
                action = f"**{action_pronoun} need to play {needed} more hand(s) IMMEDIATELY!**"

            embed.color = color
            embed.add_field(name="📈 Status", value=status, inline=False)

            if not stats['meets_hand_requirement'] or (
                    db.MIN_CHIPS_WAGERED > 0 and not stats['meets_wager_requirement']):
                embed.add_field(name="🎯 What Is Needed", value=action, inline=False)

        embed.set_footer(text=f"Requirements reset every {db.INACTIVITY_DAYS} days.")
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
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_IDS):
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
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_IDS):
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
        table_lines.append(f"{'':4}{'Player':<16} {'Tipped':>7}")
        table_lines.append("─" * 34)

        for i, r in enumerate(rows):
            rank = i + 1
            uname = r['username'][:16]
            medal = MEDALS.get(rank, f"{rank}. ")
            you_tag = " ◀" if r['user_id'] == caller_id else ""
            table_lines.append(f"{medal:<4}{uname:<16} {r['total_tipped']:>7,}{you_tag}")
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
                value=f"Total Tipped **{caller_tipped:,}** <:poker_chip:1490458259855773707>",
                inline=False
            )
        else:
            embed.add_field(name="📊 Your Generosity", value="No tips yet.", inline=False)

        await interaction.followup.send(embed=embed)


    @pokeradmin.command(name="backup", description="[Dev] Force a database backup to your DMs")
    async def force_backup(self, interaction: discord.Interaction):
        # Ironclad Security: Only YOU can run this
        if interaction.user.id not in self.DEV_USER_IDS:
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
            traceback.print_exc()
            await interaction.followup.send(f"❌ Backup failed: {e}", ephemeral=True)

    @poker.command(name="testcards", description="[Dev] Generate a random 2-card hand to test image sizes")
    async def test_cards(self, interaction: discord.Interaction):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_IDS):
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
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

    @poker.command(name="currencylog", description="View recent chip transactions")
    @app_commands.describe(user="Player to check (Admins/Devs only, leave blank for yourself)")
    async def currencylog(self, interaction: discord.Interaction, user: discord.Member = None):
        target = user or interaction.user

        # 🚨 Permission Check
        if target.id != interaction.user.id:
            if not await is_manager(interaction):
                await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
                return

        await interaction.response.defer(ephemeral=True)
        logs = await db.get_currency_logs(target.id)

        # Pass the caller (to verify button clicks) AND the target (for the embed profile)
        view = CurrencyLogView(caller=interaction.user, target=target, logs=logs)
        await interaction.followup.send(embed=view.build_embed(), view=view, ephemeral = True)

    @poker.command(name="tutorial",
                   description="Learn Texas Hold'em with a guided 3-hand walkthrough (private · fake chips · wallet never touched)")
    async def tutorial(self, interaction: discord.Interaction):
        # Find the loaded TutorialCog and delegate to its handler
        cog = self.bot.get_cog("TutorialCog")
        if cog:
            await cog.tutorial(interaction)
        else:
            await interaction.response.send_message("❌ Tutorial is not available.", ephemeral=True)

    @poker.command(name="preferences", description="Configure your auto-rebuy, auto-showdown, and confirmation settings")
    async def preferences_cmd(self, interaction: discord.Interaction):
        view = PreferencesView(interaction.user)
        await view.init_data()
        await interaction.response.send_message(view=view, ephemeral=True)

    @pokeradmin.command(name="sql", description="[Dev] Run a read-only database query")
    @app_commands.describe(query="The SELECT query to run")
    async def run_sql(self, interaction: discord.Interaction, query: str):
        # 1. Ironclad Security Check
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_IDS):
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
            return

        # 2. String Check: Reject anything that isn't a SELECT
        if not query.strip().upper().startswith("SELECT"):
            await interaction.response.send_message("❌ Only SELECT queries are allowed.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)

        try:
            import aiosqlite
            # Force SQLite into strict Read-Only mode using URI parameters
            db_uri = f"file:{db.DB_PATH}?mode=ro"

            async with aiosqlite.connect(db_uri, uri=True) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(query) as cursor:
                    rows = await cursor.fetchall()

                    if not rows:
                        await interaction.followup.send("✅ Query executed successfully. No rows returned.",
                                                        ephemeral=False)
                        return

                    columns = list(rows[0].keys())

                    # Pass to Paginator (15 rows per page, Max 20 pages)
                    view = RawSQLPaginationView(columns=columns, rows=rows, title="Main DB Query", items_per_page=15,
                                                max_pages_limit=20)
                    await interaction.followup.send(embed=view.format_page(), view=view, ephemeral=False)

        except Exception as e:
            traceback.print_exc()
            await interaction.followup.send(f"❌ **SQL Error:**\n`{e}`", ephemeral=False)

    @pokeradmin.command(name="setstat", description="[Admin] Modify a player's poker statistics")
    @app_commands.describe(
        user="The player whose stats you want to change",
        stat="The specific statistic to modify",
        value="The new integer value for this stat"
    )
    @app_commands.choices(stat=[
        app_commands.Choice(name="Hands Played", value="hands_played"),
        app_commands.Choice(name="Hands Won", value="hands_won"),
        app_commands.Choice(name="Chips Won", value="chips_won"),
        app_commands.Choice(name="Chips Lost", value="chips_lost"),
        app_commands.Choice(name="Total Tipped", value="total_tipped"),
        app_commands.Choice(name="Current Win Streak", value="win_streak"),
        app_commands.Choice(name="Max Win Streak", value="max_win_streak"),
        app_commands.Choice(name="Pocket Aces Wins", value="pocket_aces_wins"),
        app_commands.Choice(name="All-In Wins", value="all_in_wins"),
        app_commands.Choice(name="Quads Wins", value="quads_wins"),
        app_commands.Choice(name="Straight Flush Wins", value="straight_flush_wins"),
        app_commands.Choice(name="Royal Flush Wins", value="royal_flush_wins"),
        app_commands.Choice(name="Times Wiped (Inactivity)", value="times_wiped"),
    ])
    async def setstat(self, interaction: discord.Interaction, user: discord.Member, stat: app_commands.Choice[str],
                      value: int):
        # 1. Security Check
        if interaction.user.id not in config.DEV_USER_IDS:
            await interaction.response.send_message("❌ **Access Denied.** You do not have permission.", ephemeral=True)
            return

        import database as db
        conn = await db._get_db()

        try:
            # 2. Extract the safe column name
            column_name = stat.value

            # 3. Update the database
            await conn.execute(f"UPDATE stats SET {column_name} = ? WHERE user_id = ?", (value, user.id))
            await conn.commit()

            await interaction.response.send_message(
                f"✅ Successfully updated **{stat.name}** to `{value:,}` for **{user.display_name}**!")

        except Exception as e:
            traceback.print_exc()
            await interaction.response.send_message(f"❌ Database error: {e}", ephemeral=True)

    @poker.command(name="gamble", description="Spin the wheel! Set the weights for your mystery move.")
    @app_commands.describe(
        call="Weight/chance to Call (e.g., 50)",
        fold="Weight/chance to Fold (e.g., 20)",
        check="Weight/chance to Check",
        allin="Weight/chance to go All-In"
    )
    async def gamble(
            self,
            interaction: discord.Interaction,
            call: float = None,
            fold: float = None,
            check: float = None,
            allin: float = None
    ):
        # 1. Standard table and player checks
        key = (interaction.guild_id, interaction.channel_id)
        t = tables.get(key)
        if not t or t.game.street == Street.WAITING:
            await interaction.response.send_message("❌ No active hand to gamble on.", ephemeral=True);
            return

        p = t.game.get_player(interaction.user.id)
        if not p or p.folded or p.all_in:
            await interaction.response.send_message("❌ You can't gamble right now.", ephemeral=True);
            return

        # 2. Safely collect the inputs
        inputs = {"call": call, "fold": fold, "check": check, "allin": allin}
        options, weights = [], []

        for action, weight in inputs.items():
            if weight is not None and weight > 0:
                options.append(action)
                weights.append(weight)

        if not options:
            await interaction.response.send_message(
                "❌ You must provide a weight greater than 0 for at least one action!", ephemeral=True)
            return

        # 3. Check if it's their turn right now
        is_active_turn = (t.game.current_idx >= 0 and t.game.players[t.game.current_idx].user_id == p.user_id)

        # 4. Filter out illegal moves BEFORE rolling the dice
        if is_active_turn:
            call_amt = t.game.call_amount(p)
            if "check" in options and call_amt > 0:
                idx = options.index("check")
                options.pop(idx)
                weights.pop(idx)
                if not options:
                    await interaction.response.send_message(
                        f"❌ You cannot Check (there is a bet of **{call_amt}** to call), and you didn't provide other options!",
                        ephemeral=True)
                    return

        # 5. Roll the dice
        import random
        chosen_action = random.choices(options, weights=weights, k=1)[0]

        if not is_active_turn:
            # --- QUEUE THE PREMOVE ---
            if chosen_action == "allin":
                await interaction.response.send_message(
                    "❌ You cannot queue an 'All-In' as a premove. You must wait for your turn!", ephemeral=True)
                return
            elif chosen_action == "fold":
                p.premove = {"action": "fold_any"}
            elif chosen_action == "check":
                p.premove = {"action": "call_upto", "amount": 0}
            elif chosen_action == "call":
                p.premove = {"action": "call_upto", "amount": 9999999}

            p.gamble_locked = True
            await interaction.response.send_message(f"🎲 **Gamble locked in!** (Rolled: {chosen_action.upper()})",
                                                    ephemeral=True)

        else:
            # --- EXECUTE INSTANTLY ---
            # 🛠️ FIX 1: Use followup.send to properly clear the "thinking..." state
            await interaction.response.defer()
            await interaction.followup.send(
                f"🎲 **{p.display_name}** spun the wheel of fate and rolled... **{chosen_action.upper()}**!")

            # 🛠️ FIX 2: Capture the engine's natively formatted message
            success, msg = False, ""
            if chosen_action == "fold":
                success, msg = t.game.fold(p.user_id)
            elif chosen_action == "check" or chosen_action == "call":
                success, msg = t.game.check_or_call(p.user_id)
            elif chosen_action == "allin":
                call_needed = t.game.call_amount(p)
                raise_on_top = p.chips - call_needed

                # If they only have enough to call (or less), process as a normal call
                if raise_on_top <= 0:
                    success, msg = t.game.check_or_call(p.user_id)
                else:
                    success, msg = t.game.raise_bet(p.user_id, raise_on_top)

            # 🛠️ FIX 3: Decorate the engine log with a dice emoji for the table embed
            if success and msg:
                parts = msg.split("\n")
                if any(m in msg for m in ["🌊", "↩️", "🏁", "Showdown"]):
                    slog_clear(t)

                for part in parts:
                    if part.strip():
                        # If the text contains their name, slap a dice in front of the native formatting
                        if p.display_name in part:
                            slog(t, f"🎲 {part.strip()}")
                        else:
                            slog(t, part.strip())

            # Advance the UI
            if t.game._hand_result:
                await _process_result(interaction.guild, interaction.channel, t)
            else:
                await refresh(interaction.channel, t, cosmetics_cache=getattr(t, 'cosmetics_cache', {}))

class StatsView(discord.ui.View):
    def __init__(self, user: discord.User | discord.Member, row: dict, rank_str: str):
        super().__init__(timeout=120)
        self.user = user
        self.row = row
        self.rank_str = rank_str

    def build_basic_embed(self) -> discord.Embed:
        net = self.row['net_chips']
        embed = discord.Embed(title=f"Player Stats — {self.row['username']}", color=0x2ecc71 if net > 0 else 0xe74c3c)
        if net == 0:
            embed.color =0xFFFFFF

        wp = f"{self.row['hands_won'] / self.row['hands_played'] * 100:.1f}%" if self.row['hands_played'] else "—"

        embed.add_field(name="Rank", value=str(self.rank_str), inline=True)
        embed.add_field(name="Hands Played", value=str(self.row['hands_played']), inline=True)
        embed.add_field(name="Win %", value=wp, inline=True)

        # 🚨 Custom poker chips restored for currency values
        embed.add_field(name="Net Chips", value=f"{'+' if net >= 0 else ''}{net:,} <:poker_chip:1490458259855773707>",
                        inline=True)
        embed.add_field(name="Wallet Balance", value=f"{self.row['wallet']:,} <:poker_chip:1490458259855773707>",
                        inline=True)
        embed.add_field(name="Total Tipped",
                        value=f"{self.row.get('total_tipped', 0):,} <:poker_chip:1490458259855773707>", inline=True)
        return embed

    def build_highlights_embed(self) -> discord.Embed:
        embed = discord.Embed(title=f"Career Highlights — {self.row['username']}", color=0x2b2d31)

        vpip_c = self.row.get('vpip_count', 0)
        vpip_h = self.row.get('vpip_hands', 0)
        vpip_str = f"{vpip_c / vpip_h * 100:.1f}%" if vpip_h > 0 else "—"

        # 🚨 Clean text formatting with zero emoji spam
        highlights = (
            f"**Current Win Streak:** `{self.row['win_streak']}`\n"
            f"**Best Win Streak:** `{self.row['max_win_streak']}`\n"
            f"**VPIP:** `{vpip_str}`\n"
            f"**Pocket Aces Wins:** `{self.row['pocket_aces_wins']}`\n"
            f"**All-In Wins:** `{self.row['all_in_wins']}`\n"
            f"**Four of a Kind:** `{self.row['quads_wins']}`\n"
            f"**Straight Flush:** `{self.row['straight_flush_wins']}`\n"
            f"**Royal Flush:** `{self.row['royal_flush_wins']}`\n"
            f"**Jackpot Winnings:** `{self.row.get('jackpot_winnings', 0):,}` <:poker_chip:1490458259855773707>\n"
        )

        if self.row.get('times_wiped', 0) > 0:
            highlights += f"**Times Wiped:** `{self.row['times_wiped']}`"

        embed.description = highlights
        return embed

    @discord.ui.button(label="Basic Stats", style=discord.ButtonStyle.blurple, disabled=True)
    async def btn_basic(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id:
            return await interaction.response.send_message("This is not your stats menu.", ephemeral=True)
        self.btn_basic.disabled = True
        self.btn_highlights.disabled = False
        await interaction.response.edit_message(embed=self.build_basic_embed(), view=self)

    @discord.ui.button(label="Highlights", style=discord.ButtonStyle.gray)
    async def btn_highlights(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id:
            return await interaction.response.send_message("This is not your stats menu.", ephemeral=True)
        self.btn_basic.disabled = False
        self.btn_highlights.disabled = True
        await interaction.response.edit_message(embed=self.build_highlights_embed(), view=self)


async def should_confirm_premove_all_in(t: TableState, user_id: int, move: dict) -> bool:
    p = t.game.get_player(user_id)
    if not p:
        return False
        
    is_all_in = False
    action = move["action"]
    
    if action == "raise_all_in":
        is_all_in = True
    elif action == "call_upto":
        if move["amount"] >= p.chips:
            is_all_in = True
    elif action == "raise_to":
        if move["amount"] >= p.chips + p.bet:
            is_all_in = True
    elif action == "raise_by":
        call_needed = t.game.current_bet - p.bet
        if call_needed + move["amount"] >= p.chips:
            is_all_in = True
            
    if not is_all_in:
        return False
        
    pref = await db.get_player_preference(user_id)
    cai_mode = pref.get("confirm_all_in_mode", "always")
    cai_thresh = pref.get("confirm_all_in_threshold", 0)
    
    if cai_mode == "never":
        return False
    elif cai_mode == "threshold":
        if p.chips <= cai_thresh:
            return False
            
    return True

class PremoveConfirmAllInView(discord.ui.View):
    def __init__(self, parent_view: discord.ui.View, pending_move: dict):
        super().__init__(timeout=60)
        self.parent_view = parent_view
        self.pending_move = pending_move

    @discord.ui.button(label="Yes, Proceed", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.parent_view._append(self.pending_move):
            await interaction.response.send_message("❌ Premove chain is limited to 5 actions. Click 'Cancel' to start over.", ephemeral=True)
            return
        await interaction.response.edit_message(
            content=f"⚡ **Premove Chain:** {self.parent_view._get_chain_str()}",
            view=self.parent_view
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            content=f"⚡ **Premove Chain:** {self.parent_view._get_chain_str()}",
            view=self.parent_view
        )

class PremoveView(discord.ui.View):
    def __init__(self, t: TableState, user_id: int):
        super().__init__(timeout=120)
        self.t = t
        self.user_id = user_id

    def _append(self, move: dict) -> bool:
        p = self.t.game.get_player(self.user_id)
        if p:
            if p.premove is None or not isinstance(p.premove, list):
                p.premove = []
            if len(p.premove) >= 5:
                return False
            p.premove.append(move)
            return True
        return False

    def _get_chain_str(self) -> str:
        p = self.t.game.get_player(self.user_id)
        if not p or not p.premove:
            return "None"
        
        labels = []
        moves = p.premove if isinstance(p.premove, list) else [p.premove]
        for m in moves:
            if m is None: continue
            act = m["action"]
            if act == "check":
                labels.append("Check")
            elif act == "call_any":
                labels.append("Call Any")
            elif act == "call_upto":
                labels.append(f"Call ≤ {m['amount']:,}")
            elif act == "fold_any":
                labels.append("Fold Any")
            elif act == "fold_if_gt":
                labels.append(f"Fold > {m['amount']:,}")
            elif act == "raise_all_in":
                labels.append("All-In")
            elif act == "raise_to":
                labels.append(f"Raise To {m['amount']:,}")
            elif act == "raise_by":
                labels.append(f"Raise By {m['amount']:,}")
        return " ➔ ".join(labels) if labels else "None"

    @discord.ui.button(label="Check", style=discord.ButtonStyle.blurple, row=0)
    async def pm_check(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._append({"action": "check"}):
            await interaction.response.send_message("❌ Premove chain is limited to 5 actions. Click 'Cancel' to start over.", ephemeral=True)
            return
        await interaction.response.edit_message(content=f"⚡ **Premove Chain:** {self._get_chain_str()}", view=self)

    @discord.ui.button(label="Fold Any", style=discord.ButtonStyle.red, row=0)
    async def pm_fold_any(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._append({"action": "fold_any"}):
            await interaction.response.send_message("❌ Premove chain is limited to 5 actions. Click 'Cancel' to start over.", ephemeral=True)
            return
        await interaction.response.edit_message(content=f"⚡ **Premove Chain:** {self._get_chain_str()}", view=self)

    @discord.ui.button(label="Call Any", style=discord.ButtonStyle.blurple, row=0)
    async def pm_call_any(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._append({"action": "call_any"}):
            await interaction.response.send_message("❌ Premove chain is limited to 5 actions. Click 'Cancel' to start over.", ephemeral=True)
            return
        await interaction.response.edit_message(content=f"⚡ **Premove Chain:** {self._get_chain_str()}", view=self)

    @discord.ui.button(label="All-In", style=discord.ButtonStyle.red, row=0)
    async def pm_all_in(self, interaction: discord.Interaction, button: discord.ui.Button):
        move = {"action": "raise_all_in"}
        if await should_confirm_premove_all_in(self.t, self.user_id, move):
            await interaction.response.edit_message(
                content="⚠️ **You are about to queue an All-In premove. Are you sure you want to proceed?**",
                view=PremoveConfirmAllInView(self, move)
            )
            return

        if not self._append(move):
            await interaction.response.send_message("❌ Premove chain is limited to 5 actions. Click 'Cancel' to start over.", ephemeral=True)
            return
        await interaction.response.edit_message(content=f"⚡ **Premove Chain:** {self._get_chain_str()}", view=self)

    @discord.ui.button(label="Call ≤ X", style=discord.ButtonStyle.green, row=1)
    async def pm_call_upto(self, interaction: discord.Interaction, button: discord.ui.Button):
        p = self.t.game.get_player(self.user_id)
        if p and p.premove and len(p.premove) >= 5:
            await interaction.response.send_message("❌ Premove chain is limited to 5 actions. Click 'Cancel' to start over.", ephemeral=True)
            return
        await interaction.response.send_modal(PremoveAmountModal(self.t, self.user_id, "call_upto", self))

    @discord.ui.button(label="Fold > X", style=discord.ButtonStyle.green, row=1)
    async def pm_fold_if_gt(self, interaction: discord.Interaction, button: discord.ui.Button):
        p = self.t.game.get_player(self.user_id)
        if p and p.premove and len(p.premove) >= 5:
            await interaction.response.send_message("❌ Premove chain is limited to 5 actions. Click 'Cancel' to start over.", ephemeral=True)
            return
        await interaction.response.send_modal(PremoveAmountModal(self.t, self.user_id, "fold_if_gt", self))

    @discord.ui.button(label="Raise To X", style=discord.ButtonStyle.green, row=1)
    async def pm_raise_to(self, interaction: discord.Interaction, button: discord.ui.Button):
        p = self.t.game.get_player(self.user_id)
        if p and p.premove and len(p.premove) >= 5:
            await interaction.response.send_message("❌ Premove chain is limited to 5 actions. Click 'Cancel' to start over.", ephemeral=True)
            return
        await interaction.response.send_modal(PremoveAmountModal(self.t, self.user_id, "raise_to", self))

    @discord.ui.button(label="Raise By X", style=discord.ButtonStyle.green, row=1)
    async def pm_raise_by(self, interaction: discord.Interaction, button: discord.ui.Button):
        p = self.t.game.get_player(self.user_id)
        if p and p.premove and len(p.premove) >= 5:
            await interaction.response.send_message("❌ Premove chain is limited to 5 actions. Click 'Cancel' to start over.", ephemeral=True)
            return
        await interaction.response.send_modal(PremoveAmountModal(self.t, self.user_id, "raise_by", self))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, row=2)
    async def pm_cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        p = self.t.game.get_player(self.user_id)
        if p:
            p.premove = None
        await interaction.response.edit_message(content="🚫 Premove chain cancelled.", view=None)

    @discord.ui.button(label="Done", style=discord.ButtonStyle.grey, row=2)
    async def pm_done(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content=f"✅ Premove chain: **{self._get_chain_str()}**", view=None)

class CurrencyLogView(discord.ui.View):
    def __init__(self, caller: discord.User | discord.Member, target: discord.User | discord.Member, logs: list[dict]):
        super().__init__(timeout=120)
        self.caller = caller   # The person clicking the buttons
        self.target = target   # The person whose logs we are viewing
        self.all_logs = logs
        self.logs = logs
        self.page = 0
        self.per_page = 5
        self.filter = "All"
        self.update_buttons()

    def update_buttons(self):
        max_pages = max(1, math.ceil(len(self.logs) / self.per_page))
        self.btn_first.disabled = self.page == 0
        self.btn_prev.disabled = self.page == 0
        self.btn_next.disabled = self.page >= max_pages - 1
        self.btn_last.disabled = self.page >= max_pages - 1

    def build_embed(self):
        embed = discord.Embed(title="Currency Log", color=0xF1C40F)
        # 🚨 Use the TARGET for the profile picture and name
        embed.set_author(name=self.target.display_name, icon_url=self.target.display_avatar.url)

        if not self.logs:
            embed.description = "No transactions found for this filter."
            return embed

        start = self.page * self.per_page
        end = start + self.per_page
        page_logs = self.logs[start:end]

        desc_lines = []
        for log in page_logs:
            dt = datetime.fromisoformat(log['ts']).replace(tzinfo=_tz.utc)
            unix_ts = int(dt.timestamp())
            sign = "+" if log['amount'] > 0 else ""
            desc_lines.append(f"**{log['description']}**")
            desc_lines.append(f"└ <t:{unix_ts}:R>")
            desc_lines.append(f"└ {sign}{log['amount']:,} <:poker_chip:1490458259855773707>")
            desc_lines.append("\u200b")

        embed.description = "\n".join(desc_lines)
        max_pages = max(1, math.ceil(len(self.logs) / self.per_page))
        embed.set_footer(text=f"Page {self.page + 1} of {max_pages}  •  Filter: {self.filter}")
        return embed

    @discord.ui.select(
        placeholder="Filter by type...",
        options=[
            discord.SelectOption(label="All", value="All", emoji="📋"),
            discord.SelectOption(label="Hands", value="Hand", emoji="🃏"),
            discord.SelectOption(label="Cash Ins", value="Cash In", emoji="📥"),
            discord.SelectOption(label="Cash Outs", value="Cash Out", emoji="📤"),
            discord.SelectOption(label="Tips", value="Tip", emoji="💸"),
            discord.SelectOption(label="Jackpots", value="Jackpot", emoji="🎰"),
            discord.SelectOption(label="Wipes", value="Wipe", emoji="🧹"),
        ],
        row=0
    )
    async def filter_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        # 🚨 Verify the CALLER is the one clicking
        if interaction.user.id != self.caller.id:
            await interaction.response.send_message("❌ This is not your menu.", ephemeral=True); return

        self.filter = select.values[0]
        if self.filter == "All":
            self.logs = self.all_logs
        else:
            self.logs = [log for log in self.all_logs if log['event_type'] == self.filter]

        self.page = 0
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(emoji="⏪", style=discord.ButtonStyle.blurple, row=1)
    async def btn_first(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id: return
        self.page = 0
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.blurple, row=1)
    async def btn_prev(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id: return
        self.page = max(0, self.page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.blurple, row=1)
    async def btn_next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id: return
        max_pages = max(1, math.ceil(len(self.logs) / self.per_page))
        self.page = min(max_pages - 1, self.page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(emoji="⏩", style=discord.ButtonStyle.blurple, row=1)
    async def btn_last(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id: return
        max_pages = max(1, math.ceil(len(self.logs) / self.per_page))
        self.page = max_pages - 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


class PremoveAmountModal(discord.ui.Modal):
    def __init__(self, t: TableState, user_id: int, action: str, view: discord.ui.View):
        if action == "call_upto":
            label = "Call Up To Amount"
        elif action == "fold_if_gt":
            label = "Fold If Bet > Amount"
        elif action == "raise_to":
            label = "Raise TO Total Amount"
        else:
            label = "Raise BY Amount (On Top)"

        super().__init__(title=label)
        self.t = t
        self.user_id = user_id
        self.action = action
        self.view = view

        self.amount_input = discord.ui.TextInput(
            label="Amount (in Chips or BBs)",
            placeholder="e.g. 500 or 10bb",
            min_length=1,
            max_length=15
        )
        self.add_item(self.amount_input)

    async def on_submit(self, interaction: discord.Interaction):
        orig_input = self.amount_input.value.lower()
        is_bb = "bb" in orig_input

        clean_input = orig_input.replace("bb", "").strip()
        parsed_val = parse_chips(clean_input)
        if parsed_val is None or parsed_val <= 0:
            await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
            return

        if is_bb:
            bb = self.t.game.BIG_BLIND
            amt = parsed_val * bb
        else:
            amt = parsed_val

        move = {"action": self.action, "amount": amt}
        if await should_confirm_premove_all_in(self.t, self.user_id, move):
            await interaction.response.edit_message(
                content="⚠️ **This premove would put you All-In. Are you sure you want to proceed?**",
                view=PremoveConfirmAllInView(self.view, move)
            )
            return

        if not self.view._append(move):
            await interaction.response.send_message("❌ Premove chain is limited to 5 actions. Click 'Cancel' to start over.", ephemeral=True)
            return

        await interaction.response.edit_message(
            content=f"⚡ **Premove Chain:** {self.view._get_chain_str()}",
            view=self.view
        )


class RawSQLPaginationView(discord.ui.View):
    def __init__(self, columns: list, rows: list, title: str, items_per_page: int = 15, max_pages_limit: int = 20):
        super().__init__(timeout=300)
        self.columns = columns
        self.rows = rows
        self.title = title
        self.items_per_page = items_per_page
        self.current_page = 0

        # Calculate actual pages, but HARD CAP it at max_pages_limit (20)
        calculated_pages = math.ceil(len(rows) / items_per_page) if rows else 1
        self.max_pages = min(calculated_pages, max_pages_limit)

        self.update_buttons()

    def update_buttons(self):
        """Enables/Disables navigation buttons based on current position."""
        is_first_page = self.current_page == 0
        is_last_page = self.current_page >= self.max_pages - 1

        self.btn_first.disabled = is_first_page
        self.btn_prev.disabled = is_first_page
        self.btn_next.disabled = is_last_page
        self.btn_last.disabled = is_last_page

    def format_page(self):
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_rows = self.rows[start_idx:end_idx]

        embed = discord.Embed(title=self.title, color=discord.Color.dark_theme())

        header = " | ".join(str(c) for c in self.columns)
        separator = "-" * len(header)
        lines = [header, separator]

        for r in page_rows:
            # Safely stringify columns and truncate long values to keep columns aligned
            row_str = " | ".join(str(r[col])[:40] for col in self.columns)
            lines.append(row_str)

        description = "\n" + "\n".join(lines) + "\n"

        # Failsafe against Discord's 4096 embed description limit
        if len(description) > 4000:
            description = description[:4000] + "\n...[Truncated]"

        embed.description = description
        embed.set_footer(text=f"Page {self.current_page + 1} of {self.max_pages} | Total Rows: {len(self.rows)}")
        return embed

    @discord.ui.button(label="⏪", style=discord.ButtonStyle.secondary, row=0)
    async def btn_first(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = 0
        self.update_buttons()
        await interaction.response.edit_message(embed=self.format_page(), view=self)

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.secondary, row=0)
    async def btn_prev(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.format_page(), view=self)

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.secondary, row=0)
    async def btn_next(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.format_page(), view=self)

    @discord.ui.button(label="⏩", style=discord.ButtonStyle.secondary, row=0)
    async def btn_last(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = self.max_pages - 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.format_page(), view=self)

async def _migrate_active_tables(bot):
    await asyncio.sleep(1)
    for key, t in list(tables.items()):
        guild_id, channel_id = key
        channel = bot.get_channel(channel_id)
        if not channel:
            try:
                channel = await bot.fetch_channel(channel_id)
            except Exception:
                continue

        # 1. Active hand migration
        if t.game.street not in (Street.WAITING, Street.SHOWDOWN):
            try:
                if t.timer_task and not t.timer_task.done():
                    t.timer_task.cancel()
                    t.timer_task = None
                await refresh(channel, t, cosmetics_cache=t.cosmetics_cache)
            except Exception as e:
                print(f"Error migrating active table {t.id}: {e}")

        # 2. Between hands migration
        elif t.between_msg:
            try:
                if t.auto_task and not t.auto_task.done():
                    t.auto_task.cancel()
                schedule_next_hand(t, channel)
            except Exception as e:
                print(f"Error migrating between-hand table {t.id}: {e}")

async def setup(bot):
    if not hasattr(bot, "poker_tables"):
        bot.poker_tables = tables
    asyncio.create_task(_migrate_active_tables(bot))
    await bot.add_cog(PokerCog(bot))