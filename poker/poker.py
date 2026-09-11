"""poker.py — Texas Hold'em bot"""

import discord
from discord import app_commands
from discord.ext import commands, tasks
from treys import Evaluator, Card
import os, asyncio, uuid, zipfile, traceback
import random
import re
from datetime import datetime, timedelta, time as dt_time, timezone as _tz, date
import time
import re
import math
import sys
import config
import subprocess

from .engine import PokerGame, Street, hand_str, card_str, UNO_COLOR_EMOJI, SUIT_EMOJI
from . import database as db
from . import jackpot
from . import taxation
from . import card_images
from . import shiny_cards
from . import chaos
from . import hand_eval
from tournament import tournament_db as tdb
import dateparser

from .leaderboard_image import generate_leaderboard_image
from .jackpot_image import generate_jackpot_image

evaluator  = Evaluator()
USE_IMAGES = card_images.cards_available()

TURN_TIMEOUT_DEFAULT    = config.TURN_TIMEOUT_DEFAULT
NEXT_HAND_DELAY_DEFAULT = config.NEXT_HAND_DELAY_DEFAULT
# ── AFK / decision-timeout system ───────────────────────────────
LARGE_POT_THRESHOLD   = config.LARGE_POT_THRESHOLD
LARGE_POT_EXTRA_TIME  = config.LARGE_POT_EXTRA_TIME
DAILY_AFK_LIMIT        = config.DAILY_AFK_LIMIT
CONSECUTIVE_AFK_LIMIT  = config.CONSECUTIVE_AFK_LIMIT
TABLE_RESEND_MSGS       = config.TABLE_RESEND_MSGS

# Chaos only
BLUFF_27_BONUS = 30  # chips taken from each other player's table stack

def _is_27_offsuit(hole_cards: list[int]) -> bool:
    # 2/7 offsuit only
    # triple threat not valid
    if len(hole_cards) != 2:
        return False
    strs = [Card.int_to_str(c) for c in hole_cards]
    ranks = {s[0] for s in strs}
    suits = {s[1] for s in strs}
    return ranks == {"2", "7"} and len(suits) == 2

def _has_six_and_seven(hole_cards: list[int]) -> bool:
    ranks = {Card.int_to_str(c)[0] for c in hole_cards}
    return "6" in ranks and "7" in ranks

SIXSEVEN_PATTERN = re.compile(r"\bsixx*\s*sevenn*\b|\b67\b", re.IGNORECASE)

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

DATE_FORMAT = "%Y-%m-%d"

def parse_date(value: str) -> date:
    dt = dateparser.parse(
        value,
        settings={
            "PREFER_DATES_FROM": "past",
            "DATE_ORDER": "YMD",
        },
    )

    if dt is None:
        raise ValueError("Invalid date")

    return dt.date()


async def date_autocomplete(
    interaction: discord.Interaction,
    current: str,
):
    current = current.lower()

    choices = []

    special = [
        ("Today", "today"),
        ("Yesterday", "yesterday"),
        ("Last Week", "last week")
    ]

    for name, value in special:
        if current in name.lower() or current in value:
            choices.append(app_commands.Choice(name=name, value=value))

    # Last 14 days through today
    for i in range(14, -1, -1):
        d = date.today() - timedelta(days=i)
        s = d.strftime(DATE_FORMAT)

        if current in s:
            choices.append(app_commands.Choice(name=s, value=s))

    return choices

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
        self.leave_cooldown_pending: set[int] = set()
        self.chaos_mode: bool = False           # True if this table's guild is set to the Chaos preset
        self.chaos_modifiers: list[str] = []    # active chaos modifier ids for the CURRENT hand
        self.chaos_hand_num: int = 0            # counts hands at this table, for the announcement embed title
        self.pending_gamble_bets: dict = {}      # Gamble the Gamble during the pre-deal window
        self.active_random_event: dict | None = None  # Random events: message-based ones (phrase chant, number guess) read this from on_message
        self.sixseven_bait_active: bool = False # 67 title after hand before next hand

    @property
    def is_tournament(self) -> bool:
        return getattr(self, "_is_tournament", False)

    @is_tournament.setter
    def is_tournament(self, val: bool):
        self._is_tournament = val
        if hasattr(self, "game") and self.game:
            self.game.is_tournament = val

_old_poker = sys.modules.get('poker.poker')
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

# Cross table rejoin locks
# manual kick, leaving, execution kick all globally lock w/ option for payment to override
# ban has no bypass

if bot_instance and hasattr(bot_instance, 'poker_global_locks'):
    global_rejoin_locks: dict[int, dict] = bot_instance.poker_global_locks
else:
    global_rejoin_locks: dict[int, dict] = {}


def apply_global_lock(user_id: int, seconds: int, payable: bool):
    # Locks user_id out of joining any table for `seconds`
    global_rejoin_locks[user_id] = {"expiry": time.time() + seconds, "payable": payable}


def get_global_lock(user_id: int) -> dict | None:
    # Returns {"expiry": float, "payable": bool} if user_id is currently locked out anywhere, else None
    lock = global_rejoin_locks.get(user_id)
    if lock is None:
        return None
    if time.time() >= lock["expiry"]:
        global_rejoin_locks.pop(user_id, None)
        return None
    return lock


def clear_global_lock(user_id: int):
    global_rejoin_locks.pop(user_id, None)

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

# Hidden Pot
_HIDDEN_ACTION_PREFIXES = ("🏳️", "✅", "📞")


def slog(t: TableState, text: str):
    if "hidden_pot" in getattr(t, "chaos_modifiers", []) and text.lstrip().startswith(_HIDDEN_ACTION_PREFIXES):
        return
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

# ── Moderation (duration / reason / DM) ─────────────────────────────────────

_DURATION_RE = re.compile(r"^\s*(\d+)\s*([mhdw])\s*$", re.IGNORECASE)
_DURATION_UNITS = {"m": 60, "h": 3600, "d": 86400, "w": 604800}
_DURATION_LABELS = {"m": "minute", "h": "hour", "d": "day", "w": "week"}


def parse_duration(duration: str | None) -> tuple[int | None, str]:
    """Parses a duration string like '10m', '2h', '7d', '1w' into (seconds, label).

    Returns (None, "Permanent") when duration is None/blank/"permanent".
    Raises ValueError on an unrecognized format.
    """
    if not duration or duration.strip().lower() in ("permanent", "perm", "forever"):
        return None, "Permanent"
    m = _DURATION_RE.match(duration)
    if not m:
        raise ValueError(
            "Invalid duration. Use a number + unit, e.g. `30m`, `12h`, `7d`, `2w`, or leave blank / `permanent`."
        )
    amount, unit = int(m.group(1)), m.group(2).lower()
    if amount <= 0:
        raise ValueError("Duration must be greater than zero.")
    seconds = amount * _DURATION_UNITS[unit]
    label_unit = _DURATION_LABELS[unit] + ("s" if amount != 1 else "")
    return seconds, f"{amount} {label_unit}"


def expires_at_str(seconds: int | None) -> str | None:
    if seconds is None:
        return None
    return (datetime.utcnow() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M UTC")


MOD_DM_FOOTER_TEXT = "Open a ticket to appeal this action."


async def send_mod_dm(user: discord.Member, action: str, reason: str | None,
                       duration_label: str | None, moderator: discord.Member, guild_name: str,
                       duration_field: str = "Expires"):
    """Best-effort DM to a user affected by a kick/ban. Never raises."""
    emoji = "🔨" if action.lower() == "ban" else "🦵"
    embed = discord.Embed(
        title=f"{emoji} You've been {action.lower()}ed from poker",
        color=0xED4245,
    )
    embed.add_field(name="Server", value=guild_name, inline=True)
    if duration_label:
        embed.add_field(name=duration_field, value=duration_label, inline=True)
    embed.add_field(name="Moderator", value=moderator.display_name, inline=True)
    embed.add_field(name="Reason", value=reason or "No reason given.", inline=False)
    embed.set_footer(text=MOD_DM_FOOTER_TEXT)
    try:
        await user.send(embed=embed)
    except discord.Forbidden:
        pass
    except Exception as e:
        print(f"[Moderation DM Error] Failed to DM {user.id}: {e}")


def format_mod_message(emoji: str, verb: str, user: discord.Member, location: str,
                        duration_label: str | None, reason: str | None, extra_line: str | None = None,
                        duration_field: str = "Expires") -> str:
    lines = [f"{emoji} <@{user.id}> {verb} from {location}."]
    if duration_label:
        lines.append(f"{duration_field}: {duration_label}")
    lines.append(f"Reason: {reason or 'No reason given'}")
    if extra_line:
        lines.append(extra_line)
    return "\n".join(lines)


def discord_timestamp(epoch_seconds: int, style: str = "R") -> str:
    return f"<t:{epoch_seconds}:{style}>"


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
    # Always (re)bind the callback to the currently-loaded code. This runs
    # on every single decision (start_timer fires after every refresh()),
    # so it also self-heals the hook after a cog/module reload instead of
    # leaving it pointed at a stale closure from the old module instance.
    t.game.on_player_acted = lambda uid: _afk_reset_consecutive(uid)

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


# ── AFK / decision-timeout helpers ──────────────────────────────
# State is global — one record per user_id across every table/guild — and
# persisted in the `afk_tracking` DB table (see database.py), not per-table.

def _afk_reset_consecutive(user_id: int):
    """Called synchronously from the engine right after a genuine decision
    (fold/check/call/raise, including one resolved via a queued premove).
    Engine action methods are synchronous, so the actual (rare) DB update is
    scheduled as a background task rather than awaited here directly."""
    task = asyncio.create_task(_afk_reset_consecutive_async(user_id))
    task.add_done_callback(_task_catcher)

async def _afk_reset_consecutive_async(user_id: int):
    state = await db.get_afk_state(user_id)
    if state["consecutive_count"] == 0:
        return  # nothing to persist — avoid a needless write
    await db.save_afk_state(
        user_id, state["daily_count"], state["daily_date"], 0
    )

async def _handle_forgiven_timeout(t: TableState, channel, user_id: int, p, state: dict):
    """Auto check/fold on behalf of an AFK player, using forgiveness."""
    name = p.display_name
    call_amt = t.game.call_amount(p)
    # If this miss will push them over the consecutive-AFK limit, they're
    # getting removed — don't let a legal "check" carry them (with live
    # equity) through further streets while still AFK. Force them out of
    # the current hand immediately instead.
    will_be_removed = (state["consecutive_count"] + 1) >= CONSECUTIVE_AFK_LIMIT

    # Suppress the on_player_acted callback for exactly this user_id so our
    # own automatic action doesn't get mistaken for a real decision and wipe
    # out the consecutive-AFK count we're about to record. Any other player
    # whose premove fires as a knock-on effect is unaffected.
    t.game._afk_auto_user_id = user_id
    try:
        if will_be_removed:
            ok, action_msg = t.game.fold(user_id)
        elif call_amt == 0:
            ok, action_msg = t.game.check_or_call(user_id)  # legal check
        else:
            ok, action_msg = t.game.fold(user_id)
    finally:
        t.game._afk_auto_user_id = None

    if not ok:
        # It's no longer this player's turn / they already acted — a race
        # resolved the decision through another path. Do nothing.
        return

    state["daily_count"] += 1
    state["consecutive_count"] += 1
    await db.save_afk_state(user_id, state["daily_count"], state["daily_date"], state["consecutive_count"])

    if any(m in action_msg for m in ["🌊", "↩️", "🏁", "Showdown"]):
        slog_clear(t)
    for part in action_msg.split("\n"):
        if part.strip():
            slog(t, part)

    verb = "folded" if (will_be_removed or call_amt > 0) else "checked"
    await channel.send(
        f"⏰ **{name}** timed out and was auto-{verb}. "
        f"({state['daily_count']}/{DAILY_AFK_LIMIT} timeouts used today)"
    )

    if state["consecutive_count"] >= CONSECUTIVE_AFK_LIMIT:
        if user_id not in t.game.kicked_users:
            t.game.kicked_users.append(user_id)
        if user_id not in t.game.pending_leaves:
            t.game.pending_leaves.append(user_id)
        t.leave_cooldown_pending.add(user_id)
        await channel.send(
            f"🚪 **{name}** missed **{CONSECUTIVE_AFK_LIMIT}** decisions in a row "
            f"and will be removed after this hand."
        )

    await _handle_post_action(channel.guild, channel, t)

async def _handle_old_timeout(t: TableState, channel, user_id: int, p):
    """Original (pre-forgiveness) timeout behavior: force-fold and remove
    after the hand. Used once a player has exhausted the daily forgiveness
    allowance."""
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
    await _handle_post_action(channel.guild, channel, t)

async def _turn_timer(t: TableState, channel, user_id: int):
    settings = await db.get_settings(channel.guild.id)
    timeout = settings.get("turn_timeout", TURN_TIMEOUT_DEFAULT)

    # Large-pot extra decision time — evaluated once, right as the timer
    # starts, from the pot at that exact moment. Does not touch the timer
    # for pots <= LARGE_POT_THRESHOLD.
    if t.game.pot > LARGE_POT_THRESHOLD:
        timeout += LARGE_POT_EXTRA_TIME

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
    # NOTE: deliberately NOT also checking p.acted here. _post_blind() sets
    # acted=True for whoever posts the small blind (and it isn't reset the
    # way the big blind's is), so in heads-up — and any time action folds
    # back around to the SB with no reopening raise — the SB's `acted` flag
    # is already True before they've made their real decision. is_turn() is
    # the correct and sufficient guard here; p.folded is kept as a cheap
    # extra check since a folded player is unambiguously done for the hand.
    if p.folded:
        return

    state = await db.get_afk_state(user_id)

    if state["daily_count"] < DAILY_AFK_LIMIT:
        await _handle_forgiven_timeout(t, channel, user_id, p, state)
    else:
        await _handle_old_timeout(t, channel, user_id, p)

# ── Auto next hand ────────────────────────────────────────────────────────────

def schedule_next_hand(t: TableState, channel):
    if t.auto_task and not t.auto_task.done():
        t.auto_task.cancel()
    t.auto_task = asyncio.create_task(_auto_next_hand(t, channel))
    t.auto_task.add_done_callback(_task_catcher)

async def _roll_chaos_for_hand(channel, t: TableState, settings: dict, player_count: int):
    # Called right before each start_hand() (non tutorial tables)
    # if chaos rolls modifiers & stores on TableState and PokerGame
    # clears mods on switching to normal table

    t.chaos_mode = settings.get("table_mode") == "chaos"
    t.game.is_chaos_hand = t.chaos_mode
    if not t.chaos_mode:
        t.chaos_modifiers = []
        t.game.chaos_modifiers = []
        return

    t.chaos_hand_num += 1
    t.chaos_modifiers = chaos.pick_modifiers(player_count)
    t.game.chaos_modifiers = t.chaos_modifiers

    # Leaky Jackpot
    # pull the DB jackpot down here (async), then hand the game engine a number to add to the pot synchronously.
    if "leaky_jackpot" in t.chaos_modifiers:
        jp = await db.get_jackpot()
        leak = math.ceil(jp * chaos.get("leaky_jackpot").params["leak_pct"])
        if leak > 0:
            await db.adjust_jackpot(-leak)
        t.game.pending_pot_boost = leak
    else:
        t.game.pending_pot_boost = 0

    try:
        await channel.send(embed=chaos.build_announcement_embed(t.chaos_modifiers, hand_num=t.chaos_hand_num))
        if "leaky_jackpot" in t.chaos_modifiers and t.game.pending_pot_boost > 0:
            await channel.send(
                f"💧 The **jackpot** sprung a leak of **{t.game.pending_pot_boost}** chips into this hand's pot!"
            )
    except (discord.HTTPException, discord.Forbidden) as e:
        print(f"[Error] Failed to send chaos announcement embed: {e}")

    # Gamble the Gamble timed pre-deal betting window.
    # cards don't exist yet at this point, so nobody can see their hand while betting, no extra guarding needed.
    if "gamble_the_gamble" in t.chaos_modifiers:
        t.pending_gamble_bets = await _run_gamble_the_gamble(channel, t)
    else:
        t.pending_gamble_bets = {}


async def _announce_bounty_if_active(channel, t: TableState):
    pass


async def _announce_uno_reverse_reminder(channel, t: TableState):
    # Uno Reverse reminder
    # mentions everyone at table
    mentions = " ".join(f"<@{p.user_id}>" for p in t.game.players_in_hand)
    if not mentions:
        return
    try:
        await channel.send(
            f"🔄 {mentions} if you still have an Uno Reverse card, now's your last chance to use it!"
        )
    except (discord.HTTPException, discord.Forbidden) as e:
        print(f"[Error] Failed to announce Uno Reverse reminder: {e}")


async def _auto_next_hand(t: TableState, channel):
    settings = await db.get_settings(channel.guild.id)
    delay    = settings.get("next_hand_delay", NEXT_HAND_DELAY_DEFAULT)
    view = None
    try:
        if t.is_tournament:
            from tournament import tournament
            view = tournament.TournamentBetweenHandsView(t)
        else:
            view = BetweenHandsView(t)
        t.between_msg = await channel.send(f"⏳ Next hand starting in **{delay}s**...", view=view)
    except Exception as e:
        print(f"🚨 [ERROR] {e}")
        import traceback
        traceback.print_exc()

    # Random events rolled between hands
    await _maybe_fire_random_event(channel, t, "")

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
        await db.log_dealer_event(t.id, t.name, t.manager_id, t.manager_name, 'no_players')
        await refresh(channel, t, cosmetics_cache=None)
        await channel.send("⚠️ Not enough players for another hand. Waiting for a Manager to `/poker start`.")
        return

    if getattr(t, 'is_tournament', False):
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

    if not getattr(t, 'is_tournament', False):
        await _roll_chaos_for_hand(channel, t, settings, total)

    slog_clear(t)
    success, msg = t.game.start_hand()
    slog(t, msg)

    if not success:
        await channel.send(f"⚠️ Could not start next hand: {msg}")
        return

    t.sixseven_bait_active = False  # "67" window closes the moment the next hand actually starts
    await _announce_bounty_if_active(channel, t)

    if t.pending_gamble_bets:
        await _resolve_gamble_the_gamble(channel, t, t.pending_gamble_bets)
        t.pending_gamble_bets = {}

    t.msg_count = 0
    await refresh(channel, t, new_hand=True, cosmetics_cache=t.cosmetics_cache)

async def _silent_strip_view(msg: discord.Message):
    try:
        await msg.edit(view=None)
    except (discord.NotFound, discord.HTTPException):
        pass


async def _close_table(channel, t: TableState):
    if getattr(t, 'is_fully_closed', False):
        return
    t.is_fully_closed = True
    await db.log_dealer_event(t.id, t.name, t.manager_id, t.manager_name, 'close')

    # closing chaos falls back to a normal Medium table.
    if getattr(t, "chaos_mode", False):
        try:
            reset = chaos.MEDIUM_RESET_SETTINGS
            await db.set_settings(
                channel.guild.id,
                small_blind=reset["small_blind"],
                big_blind=reset["big_blind"],
                min_wallet=reset["min_wallet"],
                max_wallet=reset["max_wallet"],
                table_mode="normal",
            )
        except Exception as e:
            print(f"🚨 [ERROR] Failed to reset chaos table settings on close: {e}")

    t.closing = True
    key = (channel.guild.id, channel.id)
    cancel_timer(t)

    if getattr(t, 'active_view', None):
        t.active_view.stop()

    if t.auto_task and not t.auto_task.done():
        t.auto_task.cancel()
    tables.pop(key, None)

    if t.hand_msg:
        # tell discord to remove buttons, don't wait for it to finish
        # Goes through _silent_strip_view rather than a bare `t.hand_msg.edit(view=None)` here
        asyncio.create_task(_silent_strip_view(t.hand_msg))

    is_tourney = getattr(t, 'is_tournament', False)
    if is_tourney:
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

    if hasattr(result, 'community2') and result.community2:
        lines.append(f"Board 1: {hand_str(result.community)}")
        lines.append(f"Board 2: {hand_str(result.community2)}")
    elif hasattr(result, 'community') and result.community:
        lines.append(f"Board: {hand_str(result.community)}")

    pot_results = result.pot_results or []
    pot_result_meta = result.pot_result_meta or [(i, 0) for i in range(len(pot_results))]
    ranks = result.winner_ranks or {}
    ranks2 = result.winner_ranks2 or {}
    double_board_active = bool(getattr(result, "community2", None))

    # 🚨 Grab the folded snapshot from the engine
    folded_ids = getattr(result, "folded_ids", set())

    _player_map = {_p.user_id: _p for _p in (result.showdown_players or [])}
    for uid, delta in result.chip_deltas.items():
        sign = "+" if delta > 0 else ""
        ustr = await uid_str(uid)
        sp = _player_map.get(uid)

        # 🚨 Check the snapshot to append (folded)
        if sp and sp.hole_cards:
            cards = hand_str(sp.hole_cards) + (" ✨" if sp.shiny_ids else "")
            if uid in folded_ids:
                cards += " (folded)"
        else:
            cards = "no cards"

        if double_board_active:
            # Each board gets its own rank shown separately
            board_parts = []
            r1 = ranks.get(uid)
            r2 = ranks2.get(uid)
            if r1:
                board_parts.append(f"B1: {r1}")
            if r2:
                board_parts.append(f"B2: {r2}")
            rank_part = f" [{', '.join(board_parts)}]" if board_parts else ""
        else:
            rank = ranks.get(uid)
            rank_part = f" [{rank}]" if rank else ""

        lines.append(f"  {ustr}: {cards}{rank_part}  Net: {sign}{delta}")

    if pot_results:
        for (amt, winners), (pot_idx, board_num) in zip(pot_results, pot_result_meta):
            label = "Main pot" if pot_idx == 0 else f"Side pot {pot_idx}"
            if board_num:
                label += f" (Board {board_num})"
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
    title_str = f" {title}" if title and title.startswith("<") else f" `{title}`" if title else ""
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

# ── Card Skins (hole cards only) ────────────────────────────────────────────

def _skin_render_args(p, skin_id: str | None) -> int | None:
    """
    Card Skins — rolls (once per hand, then caches on the player) whether this
    player's active skin's rare "easter egg" card art applies this hand, and
    if so which of their hole cards gets it. Returns the chosen card_int, or
    None. Safe to call from every render site — the roll only ever happens
    once per hand (see PokerPlayer.reset_for_hand).
    """
    if not skin_id or not p.hole_cards:
        return None
    info = db.SKINS.get(skin_id, {})
    egg_chance = info.get("easter_egg_chance")
    if not egg_chance or not info.get("easter_egg_overlay"):
        return None
    if not p.skin_batman_rolled:
        p.skin_batman_rolled = True
        p.skin_batman_card = random.choice(p.hole_cards) if random.random() < egg_chance else None
    return p.skin_batman_card

def build_embed(t: TableState, title_cache: dict[int, str | None] | None = None, manager_name: str = "Unknown") -> discord.Embed:
    game  = t.game
    color = STREET_COLOR.get(game.street, 0x5865F2)
    label = STREET_LABEL.get(game.street, "")
    cp    = game.current_player()

    # cute mode, visual only
    cute_mode = "cute_mode" in getattr(t, "chaos_modifiers", [])
    if cute_mode:
        color = 0xFFB6D9

    title = f"🃏 {t.name}"
    if cute_mode:
        title = f"💕 {t.name}"
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

    # 1.5 CHAOS MODIFIERS (only shown while active for the current hand)
    if getattr(t, "chaos_modifiers", None):
        embed.add_field(name="🎲 Chaos Modifiers Active",
                         value=chaos.modifiers_summary_line(t.chaos_modifiers), inline=False)

    # 2. SAFE STREET LOG (Hard-capped at 1024 characters)
    if t.street_log:
        log_text = "\n".join(t.street_log[-8:])
        if len(log_text) > 1024:
            log_text = log_text[:1020] + "..."
        embed.add_field(name="This round", value=log_text, inline=False)

    # 3. POT / TURN LOGIC
    if game.street not in (Street.WAITING,):
        # Chaos: Hidden Pot — pot size is hidden, but the current bet stays
        # visible since players still need it to call raises.
        if "hidden_pot" in getattr(t, "chaos_modifiers", []):
            pot_line = "**Pot:** 🙈 hidden"
        else:
            pot_line = f"**Pot:** {game.pot} {emoji}"
        if game.current_bet:
            pot_line += f"  ·  **Bet:** {game.current_bet}"
        if cp:
            pot_line += f"\n⬅️ **{cp.display_name}'s turn**"
        embed.add_field(name="\u200b", value=pot_line, inline=False)

    return embed

# ── Board image ───────────────────────────────────────────────────────────────

def _reverse_board_display(community: list[int]) -> list[int]:
    # reveal left to right
    n = len(community)
    slots = [card_images.BACK_SENTINEL] * 5
    if n >= 1:
        slots[4] = community[0]        # river (revealed 1st)
    if n >= 2:
        slots[3] = community[1]        # turn (revealed 2nd)
    if n >= 5:
        slots[0], slots[1], slots[2] = community[2], community[3], community[4]  # flop (revealed 3rd)
    return slots


def _remap_blind_indices_for_reverse(community: list[int], blind_idx: set[int]) -> set[int]:
    # translates blindness reveal order indices into reverse display positions
    n = len(community)
    mapping = {}
    if n >= 1:
        mapping[0] = 4
    if n >= 2:
        mapping[1] = 3
    if n >= 5:
        mapping[2], mapping[3], mapping[4] = 0, 1, 2
    return {mapping[i] for i in blind_idx if i in mapping}


async def update_board(t: TableState):
    """Generate card strip File object — attached directly to the embed message."""
    game = t.game
    if not USE_IMAGES or game.street in (Street.WAITING, Street.PREFLOP) or not game.community:
        t.board_file = None
        return

    cute_mode = "cute_mode" in t.chaos_modifiers

    if "double_board" in t.chaos_modifiers:
        if "reverse" in t.chaos_modifiers:
            # river, turn, then flop
            display_board1 = _reverse_board_display(game.community)
            display_blind1 = _remap_blind_indices_for_reverse(game.community, game.blinded_community_idx)
            display_board2 = _reverse_board_display(game.community2)
            display_blind2 = _remap_blind_indices_for_reverse(game.community2, game.blinded_community2_idx)
            t.board_file = await asyncio.to_thread(
                card_images.make_double_board_strip,
                display_board1, display_board2,
                display_blind1, display_blind2, cute_mode,
            )
            return

        t.board_file = await asyncio.to_thread(
            card_images.make_double_board_strip,
            list(game.community), list(game.community2),
            game.blinded_community_idx, game.blinded_community2_idx, cute_mode,
        )
        return

    if "reverse" in t.chaos_modifiers:
        display_cards = _reverse_board_display(game.community)
        display_blind = _remap_blind_indices_for_reverse(game.community, game.blinded_community_idx)
        t.board_file = await asyncio.to_thread(
            card_images.make_strip, display_cards, 0, False, None, display_blind, None, cute_mode
        )
        return

    backs = max(0, 5 - len(game.community))

    # Push image generation to a background thread!
    t.board_file = await asyncio.to_thread(
        card_images.make_strip, list(game.community), backs, False, None, game.blinded_community_idx,
        None, cute_mode
    )
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

RUNOUT_REVEAL_DELAY = 2.5  # seconds paused between each community card reveal during
                            # an all-in run-out (natural, or the "All In!" modifier) —
                            # see _handle_post_action and engine.py's runout_pause_pending


async def _handle_post_action(guild: discord.Guild, channel, t: TableState):
    for trigger_point in t.game.pending_random_event_triggers:
        event_id = chaos.pick_event_id()
        if event_id:
            asyncio.create_task(_run_random_event(channel, t, event_id, trigger_point))
    t.game.pending_random_event_triggers = []

    # all in reveals
    while t.game.runout_pause_pending:
        await refresh(channel, t, cosmetics_cache=t.cosmetics_cache, pause_turn=True)

        # uno reverse fires at turn to avoid all in bugs
        if t.game.uno_reverse_reminder_pending:
            t.game.uno_reverse_reminder_pending = False
            await _announce_uno_reverse_reminder(channel, t)
        await asyncio.sleep(RUNOUT_REVEAL_DELAY)
        tail = t.game.continue_runout()
        if tail:
            if any(m in tail for m in ("🌊", "↩️", "🏁", "Showdown")):
                slog_clear(t)
            for part in tail.split("\n"):
                if part.strip():
                    slog(t, part)

    # uno reverse
    if t.game.uno_reverse_reminder_pending:
        t.game.uno_reverse_reminder_pending = False
        await _announce_uno_reverse_reminder(channel, t)

    if t.game._hand_result:
        await _process_result(guild, channel, t)
        return

    # auction pauses river betting
    ran_auction = False
    if ("community_auction" in t.chaos_modifiers and t.game.street == Street.RIVER
            and len(t.game.community) == 5 and not t.game.community_auction_done):
        t.game.community_auction_done = True
        # Update the board image/embed so no turn ping, or inactivity timer
        await refresh(channel, t, cosmetics_cache=t.cosmetics_cache, pause_turn=True)
        await _run_community_auction(channel, t)
        ran_auction = True

    #
    if t.game.awaiting_runout_showdown:
        if not ran_auction:
            await refresh(channel, t, cosmetics_cache=t.cosmetics_cache, pause_turn=True)
            await asyncio.sleep(RUNOUT_REVEAL_DELAY)
        tail = t.game.resume_runout_showdown()
        if tail:
            if any(m in tail for m in ("🌊", "↩️", "🏁", "Showdown")):
                slog_clear(t)
            for part in tail.split("\n"):
                if part.strip():
                    slog(t, part)
        await _handle_post_action(guild, channel, t)
        return

    if ran_auction:
        # resume turn ping and inactivity timers
        await refresh(channel, t, cosmetics_cache=t.cosmetics_cache)
        return

    await refresh(channel, t, cosmetics_cache=t.cosmetics_cache)


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

    await _handle_post_action(guild, channel, t)

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
        apply_global_lock(interaction.user.id, cooldown, payable=True)
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
        clear_global_lock(interaction.user.id)

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
            label_str = "Default Stack (chips, BBs, or 'max')"
            placeholder_str = "e.g. 500, 100BB, or max"
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
            if self.pref_key == "default_buyin_amount":
                if input_str.endswith("bb"):
                    raw_val = input_str[:-2].strip()
                    bb_amount = parse_chips(raw_val)

                    if bb_amount is None or bb_amount < 0:
                        await interaction.response.send_message(
                            "❌ Enter a valid amount like 100BB, 500, or max.",
                            ephemeral=True
                        )
                        return

                    # Store relative values as negative
                    val = -bb_amount

                else:
                    val = parse_chips(input_str)

            else:
                val = parse_chips(input_str)
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

        self.btn_card_size = discord.ui.Button(custom_id="pref_card_size")
        self.btn_card_size.callback = self.toggle_card_size

        # Add items so they are registered in ViewStore for dispatching
        self.add_item(self.btn_auto_rebuy)
        self.add_item(self.btn_auto_showdown)
        self.add_item(self.btn_default_buyin)
        self.add_item(self.btn_confirm_all_in)
        self.add_item(self.btn_confirm_fold)
        self.add_item(self.btn_confirm_leave)
        self.add_item(self.btn_confirm_call_raise)
        self.add_item(self.btn_card_size)

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
        elif dbi_val < -1:
            self.btn_default_buyin.label = f"{abs(dbi_val)} BB"
        else:
            self.btn_default_buyin.label = "Off" if dbi_val == 0 else f"{dbi_val:,}"
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

        # Card Size (private "My Cards" view only)
        cs_val = self.pref.get("card_size", "normal")
        self.btn_card_size.label = "Compact" if cs_val == "compact" else "Normal"
        self.btn_card_size.style = discord.ButtonStyle.blurple if cs_val == "compact" else discord.ButtonStyle.grey

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
                },
                # Separator
                {
                    "type": 14,
                    "divider": True,
                    "spacing": 1
                },
                # Section 8: Card Size
                {
                    "type": 9,
                    "components": [
                        {
                            "type": 10,
                            "content": "**Card Size**\nShrink your own hole-card image (My Cards only) for a better fit on small/mobile screens. Doesn't affect what other players see."
                        }
                    ],
                    "accessory": button_to_dict(self.btn_card_size)
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

    async def toggle_card_size(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not your settings menu.", ephemeral=True)
            return
        val = self.pref.get("card_size", "normal")
        new_val = "normal" if val == "compact" else "compact"
        await db.set_player_preference(self.user_id, card_size=new_val)
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

async def refresh(channel, t: TableState, new_hand: bool = False, cosmetics_cache: dict = None,
                   pause_turn: bool = False):
    """
    `pause_turn=True` posts/edits the board embed and image as normal, but
    withholds the interactive action view and skips the turn ping /
    inactivity timer entirely — nobody can act (or get auto-folded) while
    it's on. Used for Chaos: Community Auction (freshly-dealt river shown
    before the auction opens) and for the dramatic run-out reveal (each
    street shown in turn while an all-in hand plays itself out with no
    betting to pause for in the first place). A normal (pause_turn=False)
    refresh() afterward is what brings the active view, ping, and timer
    back.
    """
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

    if pause_turn:
        # bet pause
        view = AuctionCardsOnlyView(t)
    elif t.is_tournament:
        from tournament import tournament
        view = tournament.TournamentGameView(t)
    else:
        view  = GameView(t)
    t.active_view = view

    f     = t.board_file
    send_view = view if view is not None else discord.utils.MISSING
    if new_hand or not t.hand_msg:
        if new_hand:
            t.ping_user_id = None  # allow ping to re-send beneath the new embed
        t.hand_msg = await channel.send(embed=embed, view=send_view, file=f)
    else:
        try:
            # Edit with new attachment — Discord replaces the previous one
            await t.hand_msg.edit(embed=embed, view=view, attachments=([f] if f else []))
        except (discord.NotFound, discord.HTTPException):
            t.hand_msg = await channel.send(embed=embed, view=send_view, file=f)
    t.board_file = None  # consumed

    if pause_turn:
        # timer + ping pause
        cancel_timer(t)
        if t.ping_msg:
            try:
                await t.ping_msg.delete()
            except (discord.NotFound, discord.HTTPException):
                pass
            t.ping_msg = None
        t.ping_user_id = None
        return

    await send_turn_ping(channel, t)
    start_timer(t, channel)

# ── Post-hand ─────────────────────────────────────────────────────────────────

def _slog_result(t: TableState, result):
    """Put a clean winner line into street_log so the embed shows correct info."""
    game        = t.game
    ranks       = result.winner_ranks or {}
    ranks2      = result.winner_ranks2 or {}
    pot_results = result.pot_results
    pot_result_meta = result.pot_result_meta or [(i, 0) for i in range(len(pot_results or []))]
    double_board_active = bool(result.community2)
    emoji       = get_chip_emoji(t)
    hidden_pot  = "hidden_pot" in getattr(t, "chaos_modifiers", [])

    # Use result.community — game.community is already cleared by _end_hand at this point.
    if double_board_active:
        slog(t, f"🃏 Board 1: {hand_str(result.community)}")
        slog(t, f"🃏 Board 2: {hand_str(result.community2)}")
    elif result.community:
        slog(t, f"🃏 Board: {hand_str(result.community)}")

    distinct_pots = len({pot_idx for pot_idx, _ in pot_result_meta})
    if not pot_results or (distinct_pots <= 1 and not double_board_active):
        if len(result.winners) == 1:
            w = result.winners[0]
            gained = result.chip_deltas.get(w.user_id, 0)
            rank = ranks.get(w.user_id)
            rs = f" ({rank})" if rank else ""

            if hidden_pot:
                slog(t, f"🏆 **{w.display_name}** won 🙈 hidden{rs}")
            else:
                # FIX: Smart sign formatting
                sign = "+" if gained > 0 else ""
                slog(t, f"🏆 **{w.display_name}** won **{sign}{gained}** {emoji}{rs}")
        else:
            names_and_nets = []
            for w in result.winners:
                if hidden_pot:
                    names_and_nets.append(f"**{w.display_name}**")
                else:
                    gained = result.chip_deltas.get(w.user_id, 0)
                    sign = "+" if gained > 0 else ""
                    names_and_nets.append(f"**{w.display_name}** ({sign}{gained})")
            tail = "" if hidden_pot else f" {emoji}"
            slog(t, f"🤝 Split: {', '.join(names_and_nets)}{tail}")

    else:
        for (amt, winners), (pot_idx, board_num) in zip(pot_results, pot_result_meta):
            label = "Main" if pot_idx == 0 else f"Side {pot_idx}"
            if board_num:
                label += f" (Board {board_num})"
            amt_str = "🙈 hidden" if hidden_pot else f"{amt}{emoji}"
            board_ranks = ranks2 if board_num == 2 else ranks
            if len(winners) == 1:
                w      = winners[0]
                rank   = board_ranks.get(w.user_id)
                rs     = f" ({rank})" if rank else ""
                slog(t, f"🏆 **{label}** ({amt_str}) → **{w.display_name}**{rs}")
            else:
                names = ", ".join(f"**{w.display_name}**" for w in winners)
                if hidden_pot:
                    slog(t, f"🤝 **{label}** ({amt_str}) split → {names}")
                else:
                    each = amt // len(winners)
                    slog(t, f"🤝 **{label}** ({amt_str}) split → {names} ({each} each)")


async def _announce_winner(channel, t: TableState, result, cosmetics_cache: dict = None):
    game = t.game
    ranks = result.winner_ranks or {}
    ranks2 = result.winner_ranks2 or {}
    pot_results = result.pot_results  # [(amount, [PokerPlayer, ...]), ...]
    pot_result_meta = result.pot_result_meta or [(i, 0) for i in range(len(pot_results or []))]
    double_board_active = bool(result.community2)
    emoji = get_chip_emoji(t)

    _cos_cache = cosmetics_cache or {}

    def _title_str(uid: int) -> str:
        cos = _cos_cache.get(uid, {})
        tid = cos.get("active_title")
        if not tid or tid not in db.TITLES:
            return ""
        display = db.TITLES[tid]["display"]
        return f" {display}" if display.startswith("<") else f" `{display}`"

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

    def _add_board_fields():
        if double_board_active:
            embed.add_field(name="🃏 Board 1", value=f"{hand_str(result.community)}\n\u200b", inline=True)
            embed.add_field(name="🃏 Board 2", value=f"{hand_str(result.community2)}\n\u200b", inline=True)
        elif result.community:
            embed.add_field(name="🃏 Board", value=f"{hand_str(result.community)}\n\u200b", inline=False)

    # 🏆 Create the sleek winner "Receipt" Embed
    embed = discord.Embed(title=f"🏆 Hand #{game.hand_num} Results", color=0xF1C40F)
    desc_lines = []

    distinct_pots = len({pot_idx for pot_idx, _ in pot_result_meta})
    if not pot_results or (distinct_pots <= 1 and not double_board_active):
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

            _add_board_fields()

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

            _add_board_fields()

            # Removed the Total Pot block as requested for split pots
    else:
        # Multiple Side Pots and/or Double Board splits
        for (amt, winners), (pot_idx, board_num) in zip(pot_results, pot_result_meta):
            label = "Main Pot" if pot_idx == 0 else f"Side Pot {pot_idx}"
            if board_num:
                label += f" — Board {board_num}"
            icon = "🥇" if pot_idx == 0 else "🥈"
            board_ranks = ranks2 if board_num == 2 else ranks

            desc_lines.append(f"{icon} **{label}** {emoji} **{amt}**")

            if len(winners) == 1:
                w = winners[0]
                rank = board_ranks.get(w.user_id)
                rs = f" with **{rank}**" if rank else ""
                desc_lines.append(f"↳ **{w.display_name}**{_title_str(w.user_id)}{rs}")
            else:
                split_amt = amt // len(winners)
                for w in winners:
                    rank = board_ranks.get(w.user_id)
                    rs = f" with **{rank}**" if rank else ""
                    desc_lines.append(
                        f"↳ **{w.display_name}**{_title_str(w.user_id)} *(split {split_amt}* {emoji}*){rs}")
            desc_lines.append("")  # Empty line between pots

        quotes = _build_quotes(result.winners)
        if quotes:
            desc_lines.append(quotes)

        # Inject invisible spacer (\u200b) to force Discord to give us breathing room before the Board
        embed.description = "\n".join(desc_lines).strip() + "\n\u200b"

        _add_board_fields()

        stack_lines = []
        seen = set()
        for _, winners in pot_results:
            for w in winners:
                if w.user_id not in seen:
                    seen.add(w.user_id)
                    gained = result.chip_deltas.get(w.user_id, 0)
                    sign = "+" if gained > 0 else ""
                    before = w.chips - gained
                    stack_lines.append(f"**{w.display_name}**: {before} → **{w.chips}** ({sign}{gained})")
        if stack_lines:
            embed.add_field(name="💰 Final Stacks", value="\n".join(stack_lines), inline=False)

    # Bounty successes in the same embed as the round's winners
    bounty_results = getattr(result, "bounty_results", None) or []
    if bounty_results:
        bounty_lines = [
            f"<@{hunter_uid}> claimed <@{target_uid}>'s bounty — **+{paid}** {emoji}!"
            for hunter_uid, target_uid, paid in bounty_results
        ]
        embed.add_field(name="🎯💰 Bounty Claimed!", value="\n".join(bounty_lines), inline=False)

    if not getattr(t, 'is_tournament', False):
        rate, is_special = taxation.get_tax_config()
        if is_special:
            pct_string = f"{rate * 100:g}%"
            embed.set_footer(text=f"✨ Jackpot Friday! Tax is {pct_string} (All of it goes to Jackpot) ✨")

    # 🚀 Send the final embed
    await channel.send(embed=embed)


async def _handle_shiny_cards(channel, t: TableState):
    # Announce and unlock cosmetics for any player dealt a shiny card this hand
    if not t.game.shiny_holders:
        return
    frenzy_active = "shiny_frenzy" in t.game.chaos_modifiers
    min_shinies = chaos.get("shiny_frenzy").params["min_shinies"] if frenzy_active else 1

    for shiny_id, uids in list(t.game.shiny_holders.items()):
        shiny = shiny_cards.get_by_id(shiny_id)
        if not shiny:
            continue
        for uid in list(uids):
            p = t.game.get_player(uid)
            name = p.display_name if p else f"<@{uid}>"

            # Shiny Frenzy: a single shiny doesn't qualify for cosmetics
            qualifies = not frenzy_active or len(getattr(p, "shiny_ids", None) or []) >= min_shinies
            if not qualifies:
                continue

            newly = False
            if shiny.title_id:
                newly = await db.unlock_cosmetic(uid, "title", shiny.title_id) or newly
            for winmsg_id in shiny.winmsg_ids:
                newly = await db.unlock_cosmetic(uid, "winmsg", winmsg_id) or newly

            try:
                if newly:
                    await channel.send(
                        f"{shiny.emoji} **{name}** was dealt the shiny **{shiny.display_name}!**\n"
                        f"A legendary cosmetic has been unlocked — check `/poker titles`."
                    )
                else:
                    await channel.send(
                        f"{shiny.emoji} **{name}** was dealt the shiny **{shiny.display_name}** again!"
                    )
            except (discord.HTTPException, discord.Forbidden) as e:
                print(f"[Error] Failed to announce {shiny.display_name} drop for {uid}: {e}")
    t.game.shiny_holders.clear()


async def _process_result(guild, channel, t: TableState):
    result = t.game._hand_result
    if not result:
        return
    cancel_timer(t)

    if t.is_tournament:
        from tournament import tournament_db
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
                apply_global_lock(uid, config.TOURNAMENT_REJOIN_COOLDOWN, payable=True)

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

        # random event chance
        if t.game.was_runout_hand:
            await _maybe_fire_random_event(channel, t, "")

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
    frenzy_misses: list = []       # Shiny Frenzy: players who pulled exactly 1 shiny (didn't qualify)
    achievement_announces: list[str] = [] # same pattern, collected then sent after hand result
    chaos_kick_bans: dict[int, int] = {}  # user_id -> ban seconds override (Execution, future events)
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
            newly = list(await db.check_achievements(p.user_id, won=won, pot_won=pot_won))

            # Card Skins cosmetic: winning at a chaos table unlocks the "chaos" skin.
            # If Cute Mode was also active this hand, the winner additionally has a
            # 20% chance to unlock the "cute" skin (if they haven't already).
            if "chaos" in t.chaos_modifiers:
                if await db.unlock_cosmetic(p.user_id, "skin", "chaos"):
                    newly.append(("skin", "chaos"))
                if "cute_mode" in t.chaos_modifiers:
                    cosmetics_now = await db.get_cosmetics(p.user_id)
                    if "cute" not in cosmetics_now.get("unlocked_skins", []) and random.random() < 0.20:
                        if await db.unlock_cosmetic(p.user_id, "skin", "cute"):
                            newly.append(("skin", "cute"))

            if not newly:
                continue

            lines = [f"🎉 <@{p.user_id}> unlocked new cosmetics!"]
            for kind, cid in newly:
                catalog = db.catalog_for_kind(kind)
                item = catalog.get(cid, {})
                display = item.get("display", cid)
                rarity = db.RARITY_LABEL.get(item.get("rarity", "uncommon"), "")
                icon = {"title": "🎖", "winmsg": "💬", "border": "🖼", "skin": "🎨"}.get(kind, "🎁")
                lines.append(f"  {icon} **{display}** *{rarity}*")
            achievement_announces.append("\n".join(lines))

        # ── Jackpot split payout ──────────────────────────────────────────────
        folded_ids = getattr(result, "folded_ids", set())
        winner_ids = {w.user_id for w in (result.winners or [])}
        frenzy_active = "shiny_frenzy" in t.chaos_modifiers
        jackpot_hits, frenzy_misses = await jackpot.process_jackpot_hits(
            result.showdown_players or [], result.community, folded_ids, winner_ids, frenzy=frenzy_active
        )

        # Execution
        # Routed through the exact same pending_leaves/rejoin_cooldown machinery as a voluntary leave with adjustable time

        # w/ Losers' Hand
        # `t.chaos_modifiers` is passed through so pick_execution_victim can detect Losers' Hand and target the best hand
        if "execution" in t.chaos_modifiers and result.showdown_players:
            exec_mod = chaos.get("execution")
            victim = chaos.pick_execution_victim(evaluator, hand_eval, result, t.chaos_modifiers)
            if (victim and victim.user_id not in t.game.pending_leaves
                    and random.random() < exec_mod.params["kick_chance"]):
                uid = victim.user_id
                if uid not in t.game.kicked_users:
                    t.game.kicked_users.append(uid)
                t.game.pending_leaves.append(uid)
                t.leave_cooldown_pending.add(uid)
                chaos_kick_bans[uid] = exec_mod.params["ban_seconds"]
                try:
                    embed = discord.Embed(
                        title="💀 Execution",
                        description=f"Unlucky! <@{uid}> had the worst hand and got kicked.",
                        color=0xE74C3C,
                    )
                    await channel.send(embed=embed)
                except (discord.HTTPException, discord.Forbidden) as e:
                    print(f"[Error] Failed to announce Execution kick: {e}")

        # Ragebait
        # reveal the cursed player, credit the jackpot
        if "ragebait" in t.chaos_modifiers and getattr(result, "ragebait_target", None):
            rb_uid = result.ragebait_target
            if result.ragebait_jackpot > 0:
                await db.adjust_jackpot(result.ragebait_jackpot)
            try:
                embed = discord.Embed(
                    title="😤 Ragebait",
                    description=f"<@{rb_uid}> was cursed!" + (
                        f"\nWell... they won the hand, but **{result.ragebait_jackpot}** chips "
                        f"were generously donated to the jackpot (totally voluntary)"
                        if result.ragebait_jackpot > 0 else ""
                    ),
                    color=0xE74C3C,
                )
                await channel.send(embed=embed)
            except (discord.HTTPException, discord.Forbidden) as e:
                print(f"[Error] Failed to announce Ragebait reveal: {e}")

        # 2/7 offsuit bonus on win only
        if (t.chaos_mode and len(result.winners or []) == 1 and not result.winner_ranks
                and "triple_hole" not in t.chaos_modifiers):
            bluffer = result.winners[0]
            if _is_27_offsuit(bluffer.hole_cards):
                collected = 0
                for other in t.game.players:
                    if other.user_id == bluffer.user_id:
                        continue
                    take = min(BLUFF_27_BONUS, other.chips)
                    if take > 0:
                        other.chips -= take
                        collected += take
                if collected > 0:
                    bluffer.chips += collected
                    try:
                        await channel.send(
                            f"🤡 <@{bluffer.user_id}> took it down with **2-7 offsuit** and bluffed "
                            f"the whole table off their hands! Skims **{collected}** chip(s) off "
                            f"everyone else's stack."
                        )
                    except (discord.HTTPException, discord.Forbidden) as e:
                        print(f"[Error] Failed to announce 2-7 bluff bonus: {e}")

        # 67 title
        if t.chaos_mode and any(_has_six_and_seven(w.hole_cards) for w in (result.winners or [])):
            t.sixseven_bait_active = True

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
            # Voluntary leaves and AFK kicks get a 10-minute rejoin cooldown,
            # payable like every other kick. Chip-kicked players (below BB)
            # are NOT in leave_cooldown_pending.
            if uid in t.leave_cooldown_pending:
                apply_global_lock(uid, config.REGULAR_REJOIN_COOLDOWN, payable=True)
        # chaos kicks
        for uid, ban_seconds in chaos_kick_bans.items():
            apply_global_lock(uid, ban_seconds, payable=True)
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

    for p in frenzy_misses:
        try:
            await channel.send(
                f"✨ <@{p.user_id}> pulled one shiny card.. so close!"
            )
        except Exception as e:
            print(f"[poker] frenzy miss announce error: {e}")
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

    # Random event
    if t.game.was_runout_hand:
        await _maybe_fire_random_event(channel, t, "")

    t.game._hand_result = None

    # 2. THEN check if we need to close the table or schedule the next hand
    await _handle_shiny_cards(channel, t)

    if t.closing:
        await _close_table(channel, t)
    else:
        # Verify the table wasn't closed while we were waiting for the Muck buttons
        key = (channel.guild.id, channel.id)
        if get_table(key) is t and not t.closing:
            schedule_next_hand(t, channel)

# ── Showdown reveal (muck / show) ─────────────────────────────────────────────

def _hand_rank_display(hole_cards: list, community: list, community2: list | None,
                        double_board_active: bool) -> str:
    """
    shows hand's rank on each board for double board
    """
    parts = []
    if len(hole_cards) + len(community) >= 5:
        score = hand_eval.evaluate_any(evaluator, hole_cards, community)
        rank = evaluator.class_to_string(evaluator.get_rank_class(score))
        parts.append(f"Board 1: {rank}" if double_board_active else rank)
    if double_board_active and community2 and len(hole_cards) + len(community2) >= 5:
        score2 = hand_eval.evaluate_any(evaluator, hole_cards, community2)
        rank2 = evaluator.class_to_string(evaluator.get_rank_class(score2))
        parts.append(f"Board 2: {rank2}")
    return f" — *{', '.join(parts)}*" if parts else ""


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

        # FIX: Only calculate poker hand rank once there are enough total cards to
        # rank (5+). Usually that means the flop is out, but under the "Reverse"
        # chaos modifier the board can have just 1-2 cards on a real street.
        rank_str = _hand_rank_display(sp.hole_cards, self.result.community,
                                       getattr(self.result, "community2", None),
                                       bool(getattr(self.result, "community2", None)))

        shiny = " ✨" if sp.shiny_ids else ""
        caption = f"👁️ **{interaction.user.display_name}** shows: {hand_str(sp.hole_cards)}{shiny}{rank_str}"
        if USE_IMAGES:
            await interaction.response.defer()
            cosmetics = await db.get_cosmetics(interaction.user.id)
            file = await asyncio.to_thread(
                card_images.make_strip, sp.hole_cards, 0, False, sp.shiny_ids,
                border_id=cosmetics.get("active_border"), cute_mode="cute_mode" in self.t.chaos_modifiers,
                skin_id=cosmetics.get("active_skin"),
                skin_batman_card=_skin_render_args(sp, cosmetics.get("active_skin")))
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
                    cosmetics = await db.get_cosmetics(winner.user_id)
                    file = await asyncio.to_thread(
                        card_images.make_strip, winner.hole_cards, 0, False, winner.shiny_ids,
                        border_id=cosmetics.get("active_border"), cute_mode="cute_mode" in t.chaos_modifiers,
                        skin_id=cosmetics.get("active_skin"),
                        skin_batman_card=_skin_render_args(winner, cosmetics.get("active_skin")))
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
    double_board_active = bool(result.community2)
    pot_results = result.pot_results or []
    pot_result_meta = result.pot_result_meta or [(i, 0) for i in range(len(pot_results))]
    ranks = result.winner_ranks or {}
    ranks2 = result.winner_ranks2 or {}

    # Per winner, the board(s) they ACTUALLY won and the rank they won it
    # with — never a fresh re-evaluation against board 1 alone. Under
    # Double Board the same hole cards can rank completely differently on
    # each board (or even tie/lose on one while winning the other), so
    # this only ever attributes a rank to a winner for a board a pot
    # result says they won — exactly the same source of truth the Hand
    # Results embed already uses, so the two can never disagree again.
    winner_board_ranks: dict[int, list[tuple[int, str]]] = {}
    for (_, pot_winners), (_, board_num) in zip(pot_results, pot_result_meta):
        board_ranks = ranks2 if board_num == 2 else ranks
        for pw in pot_winners:
            rank = board_ranks.get(pw.user_id)
            if not rank:
                continue
            entry = (board_num, rank)
            lst = winner_board_ranks.setdefault(pw.user_id, [])
            if entry not in lst:
                lst.append(entry)

    def _winner_rank_str(uid: int, hole_cards: list, community: list) -> str:
        won = winner_board_ranks.get(uid, [])
        if won:
            if len(won) == 1 or not double_board_active:
                return f" — *{won[0][1]}*"
            parts = ", ".join(f"Board {bn}: {r}" for bn, r in sorted(won))
            return f" — *{parts}*"
        # Fallback for the rare case a winner isn't in either ranks dict
        # (shouldn't happen — every entry in result.winners comes from a
        # pot_results winners list) — re-derive against board 1 rather
        # than show nothing.
        if len(hole_cards) + len(community) >= 5:
            score = hand_eval.evaluate_any(evaluator, hole_cards, community)
            return f" — *{evaluator.class_to_string(evaluator.get_rank_class(score))}*"
        return ""

    # A. Automatically reveal winners' cards directly to the channel (No buttons)
    for w in result.winners:
        if w.hole_cards:
            rank_str = _winner_rank_str(w.user_id, w.hole_cards, result.community)
            shiny = " ✨" if w.shiny_ids else ""
            caption = f"🏆 **{w.display_name}** wins and shows: {hand_str(w.hole_cards)}{shiny}{rank_str}"
            if USE_IMAGES:
                cosmetics = await db.get_cosmetics(w.user_id)
                file = await asyncio.to_thread(
                    card_images.make_strip, w.hole_cards, 0, False, w.shiny_ids,
                    border_id=cosmetics.get("active_border"), cute_mode="cute_mode" in t.chaos_modifiers,
                    skin_id=cosmetics.get("active_skin"),
                    skin_batman_card=_skin_render_args(w, cosmetics.get("active_skin")))
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
            rank_str = _hand_rank_display(p.hole_cards, result.community, result.community2, double_board_active)
            caption = f"👁️ **{p.display_name}** shows: {hand_str(p.hole_cards)}{rank_str}"
            if USE_IMAGES:
                cosmetics = await db.get_cosmetics(p.user_id)
                file = await asyncio.to_thread(
                    card_images.make_strip, p.hole_cards, 0, False, p.shiny_ids,
                    border_id=cosmetics.get("active_border"), cute_mode="cute_mode" in t.chaos_modifiers,
                    skin_id=cosmetics.get("active_skin"),
                    skin_batman_card=_skin_render_args(p, cosmetics.get("active_skin")))
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

        await _handle_post_action(self.guild, self.channel, self.t)

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
        hidden_pot = "hidden_pot" in getattr(t, "chaos_modifiers", [])
        if cp:
            call_amt = g.call_amount(cp)
            min_raise_amt = g.last_raise_size if g.last_raise_size > 0 else g.BIG_BLIND

            self.btn_min_raise.label = f"Min +{min_raise_amt}"
            if hidden_pot:
                # Chaos: Hidden Pot — 1/3 Pot and 1/2 Pot are computed
                # straight off g.pot, and raise amounts stay visible under
                # this modifier (that's the whole point — see its
                # description), so offering these presets would let anyone
                # back-calculate the pot from the raise that goes out.
                # Dropped entirely rather than just relabeled — Min and All
                # In don't depend on the pot, so they're unaffected.
                self.remove_item(self.btn_third_pot)
                self.remove_item(self.btn_half_pot)
            else:
                pot_third = max(call_amt, g.pot // 3)
                pot_half = max(call_amt, g.pot // 2)
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
        # Re-check in case the lock expired — or changed — while the prompt was open
        lock = get_global_lock(uid)
        if not lock:
            await interaction.response.edit_message(
                content="✅ Your cooldown already expired — click **Join** to rejoin normally!",
                view=None,
            )
            self.stop()
            return
        if not lock["payable"]:
            # A non-payable ban landed on top of the payable kick this
            # prompt was originally shown for (e.g. the Giveaway random
            # event fired while they had this open) — no fee can buy past
            # that, so pull the offer rather than let them pay for nothing.
            await interaction.response.edit_message(
                content="❌ You've since been banned — no fee can bypass that. You'll need to wait it out.",
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
            lock = get_global_lock(interaction.user.id)
            if lock:
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

# Random events

def _random_event_ping(t: TableState) -> str:
    # mention everyone at table
    ids = [p.user_id for p in t.game.players]
    if not ids:
        return ""
    return " ".join(f"<@{uid}>" for uid in ids)


async def _announce_random_event(channel, t: TableState, event_id: str, trigger_point: str | None = None, *, description: str | None = None, view=None):
    # standard msg
    embed = chaos.build_event_embed(event_id, trigger_point, description=description)
    try:
        return await channel.send(content=_random_event_ping(t), embed=embed, view=view)
    except (discord.HTTPException, discord.Forbidden) as e:
        print(f"[Error] Failed to announce random event '{event_id}': {e}")
        return None


async def _run_random_event(channel, t: TableState, event_id: str, trigger_point: str | None = None):
    handler = _RANDOM_EVENT_HANDLERS.get(event_id)
    if not handler:
        return
    try:
        await handler(channel, t, trigger_point)
    except Exception as e:
        print(f"[Error] Random event '{event_id}' crashed: {e}")
        import traceback
        traceback.print_exc()


async def _maybe_fire_random_event(channel, t: TableState, trigger_point: str):
    # chaos.RANDOM_EVENT_CHANCE chance
    # chance to appear during flop/turn/river/first_reveal
    if not t.chaos_mode:
        return
    if not chaos.should_random_event_fire():
        return
    event_id = chaos.pick_event_id()
    if event_id:
        asyncio.create_task(_run_random_event(channel, t, event_id, trigger_point))


async def _handle_random_event_message(message: discord.Message, t: TableState):
    # msg check
    ev = t.active_random_event
    if not ev:
        return
    if ev["type"] == "egirl_lover":
        await _handle_egirl_lover_message(message, t)
    elif ev["type"] == "number_guess":
        await _handle_number_guess_message(message, t)


# standard response
EVENT_JOIN_SUCCESS = "✅ You have successfully joined!"
EVENT_JOIN_ALREADY = "❌ You have already joined."
EVENT_JOIN_MISSED = "❌ You missed this event."


def _event_expired(expiry: float) -> bool:
    return time.time() >= expiry


async def _send_event_result(channel, announce_msg, embed: discord.Embed):
    # result embed as a reply to original embed
    # fallback for fails
    if announce_msg is not None:
        try:
            await announce_msg.reply(embed=embed, mention_author=False)
            return
        except (discord.HTTPException, discord.NotFound, discord.Forbidden):
            pass
    try:
        await channel.send(embed=embed)
    except (discord.HTTPException, discord.Forbidden):
        pass


# egirl lover

_EGIRL_LOVER_ORDINALS = ("first", "second", "third")


def _ordinal(place: int) -> str:
    return _EGIRL_LOVER_ORDINALS[place - 1] if place <= len(_EGIRL_LOVER_ORDINALS) else f"{place}th"


async def _award_egirl_lover_prize(uid: int) -> str:
    # picks one unowned cosmetic from pool, if all owned fallsback to consolation_chips
    gp = chaos.EVENTS_BY_ID["egirl_lover"].params
    pool = gp["cosmetic_pool"]
    try:
        cosmetics = await db.get_cosmetics(uid)
        owned = {("title", tid) for tid in cosmetics["unlocked_titles"]} | \
                {("winmsg", mid) for mid in cosmetics["unlocked_win_msgs"]}
    except Exception as e:
        print(f"[Error] egirl_lover ownership lookup failed for {uid}: {e}")
        owned = set()
    available = [item for item in pool if item not in owned]

    if not available:
        chips = gp["consolation_chips"]
        try:
            await db.return_chips(uid, chips)
            await db.log_house_revenue(-chips, source="chaos_event")
            await db.log_currency_event(uid, "Random Event", chips, "egirl lover")
        except Exception as e:
            print(f"[Error] egirl_lover consolation payout failed for {uid}: {e}")
        return f"got **{chips}** chips"

    kind, cosmetic_id = random.choice(available)
    try:
        await db.unlock_cosmetic(uid, kind, cosmetic_id)
    except Exception as e:
        print(f"[Error] egirl_lover cosmetic unlock failed for {uid}: {e}")
    if kind == "title":
        display = db.TITLES.get(cosmetic_id, {}).get("display", cosmetic_id)
        return f"unlocked title **{display}**"
    else:
        display = db.WIN_MESSAGES.get(cosmetic_id, {}).get("display", cosmetic_id)
        return f"unlocked win message **{display}**"


async def _run_egirl_lover(channel, t: TableState, trigger_point: str | None = None):
    gp = chaos.EVENTS_BY_ID["egirl_lover"].params
    phrase = gp["phrase"]
    needed = gp["winners_needed"]
    t.active_random_event = {"type": "egirl_lover", "phrase": phrase.lower(), "winners": [], "announce_msg": None}
    msg = await _announce_random_event(
        channel, t, "egirl_lover", trigger_point,
        description=f"Type in chat!:\n> {phrase}\n\n"
                     f"First **{needed}** people win!",
    )
    if t.active_random_event is not None and t.active_random_event.get("type") == "egirl_lover":
        t.active_random_event["announce_msg"] = msg

    await asyncio.sleep(60)
    ev = t.active_random_event
    if ev and ev.get("type") == "egirl_lover":
        t.active_random_event = None
        # still post result if ended
        if ev["winners"]:
            lines = "\n".join(f"**{_ordinal(i + 1)}** — <@{w['uid']}> {w['prize_desc']}"
                               for i, w in enumerate(ev["winners"]))
            desc = f"Time ran out with only **{len(ev['winners'])}/{needed}** spots filled:\n{lines}"
        else:
            desc = "Nobody really loves <@412651268142792704> I guess"
        await _send_event_result(channel, ev.get("announce_msg"),
                                  chaos.build_event_result_embed("egirl_lover", desc, no_result=True))


async def _handle_egirl_lover_message(message: discord.Message, t: TableState):
    ev = t.active_random_event
    gp = chaos.EVENTS_BY_ID["egirl_lover"].params
    if message.content.strip().lower() != ev["phrase"]:
        return

    uid = message.author.id
    if any(w["uid"] == uid for w in ev["winners"]):
        return  # don't repeat winners

    prize_desc = await _award_egirl_lover_prize(uid)
    ev["winners"].append({"uid": uid, "prize_desc": prize_desc})

    # react with check on message
    try:
        await message.add_reaction("✅")
    except (discord.HTTPException, discord.Forbidden):
        pass

    needed = gp["winners_needed"]
    if len(ev["winners"]) >= needed:
        t.active_random_event = None
        lines = "\n".join(f"**{_ordinal(i + 1)}** — <@{w['uid']}> {w['prize_desc']}"
                           for i, w in enumerate(ev["winners"]))
        await _send_event_result(message.channel, ev.get("announce_msg"),
                                  chaos.build_event_result_embed("egirl_lover", lines))


# Guess the Number

async def _run_number_guess(channel, t: TableState, trigger_point: str | None = None):
    target = random.randint(1, 25)
    t.active_random_event = {"type": "number_guess", "target": target, "announce_msg": None}
    msg = await _announce_random_event(
        channel, t, "number_guess", trigger_point,
        description="I'm thinking of a number between **1-25**. First correct guess in chat wins!",
    )
    if t.active_random_event is not None and t.active_random_event.get("type") == "number_guess":
        t.active_random_event["announce_msg"] = msg

    await asyncio.sleep(45)
    ev = t.active_random_event
    if ev and ev.get("type") == "number_guess":
        t.active_random_event = None
        await _send_event_result(channel, ev.get("announce_msg"), chaos.build_event_result_embed(
            "number_guess", f"Nobody guessed the right number. The number was **{target}**.", no_result=True))


async def _handle_number_guess_message(message: discord.Message, t: TableState):
    ev = t.active_random_event
    content = message.content.strip()
    if content.isdigit() and int(content) == ev["target"]:
        t.active_random_event = None
        gp = chaos.EVENTS_BY_ID["number_guess"].params
        prize = random.randint(gp["min_prize"], gp["max_prize"])
        try:
            await db.return_chips(message.author.id, prize)
            await db.log_house_revenue(-prize, source="chaos_event")
            await db.log_currency_event(message.author.id, "Random Event", prize, "Guess the Number")
        except Exception as e:
            print(f"[Error] number_guess payout failed: {e}")
        await _send_event_result(message.channel, ev.get("announce_msg"), chaos.build_event_result_embed(
            "number_guess",
            f"<@{message.author.id}> guessed **{ev['target']}** correctly and won **{prize}** chips!"
        ))


# Think Fast

class ThinkFastView(discord.ui.View):
    def __init__(self, window: int = 30):
        super().__init__(timeout=window)
        self.expiry = time.time() + window
        self.claimed = False
        self.winner_uid: int | None = None

    @discord.ui.button(label="⚡ Click Me!", style=discord.ButtonStyle.blurple)
    async def click(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = interaction.user.id
        if _event_expired(self.expiry):
            await interaction.response.send_message(EVENT_JOIN_MISSED, ephemeral=True)
            return
        if self.claimed:
            if uid == self.winner_uid:
                await interaction.response.send_message(EVENT_JOIN_ALREADY, ephemeral=True)
            else:
                await interaction.response.send_message("❌ Someone else already claimed this!", ephemeral=True)
            return
        gp = chaos.EVENTS_BY_ID["think_fast"].params
        balance, _ = await db.get_wallet(uid)
        if balance < gp["min_wallet"]:
            await interaction.response.send_message(
                f"❌ You need at least **{gp['min_wallet']}** chips in your wallet to try.", ephemeral=True)
            return
        self.claimed = True
        self.winner_uid = uid
        button.disabled = True

        delta = random.randint(gp["min_delta"], gp["max_delta"])
        if delta >= 0:
            await db.return_chips(uid, delta)
        else:
            await db.deduct_chips(uid, min(-delta, balance))
        try:
            await db.log_house_revenue(-delta, source="chaos_event")
        except Exception as e:
            print(f"[Error] think_fast revenue log failed: {e}")

        title_note = ""
        new_balance, _ = await db.get_wallet(uid)
        if new_balance < 1:
            try:
                await db.unlock_cosmetic(uid, "title", gp["unlucky_title_id"])
            except Exception as e:
                print(f"[Error] think_fast title unlock failed: {e}")
            title_note = " and got the 🥀 title!"

        await interaction.response.send_message(EVENT_JOIN_SUCCESS, ephemeral=True)
        try:
            await interaction.message.edit(view=self)  # disables the button on the public announcement
        except (discord.HTTPException, discord.NotFound):
            pass
        await _send_event_result(interaction.channel, interaction.message, chaos.build_event_result_embed(
            "think_fast", f"<@{uid}> clicked first and got **{delta:+d}** chips{title_note}!"
        ))
        self.stop()


async def _run_think_fast(channel, t: TableState, trigger_point: str | None = None):
    gp = chaos.EVENTS_BY_ID["think_fast"].params
    await _announce_random_event(
        channel, t, "think_fast", trigger_point,
        description=f"First person to click the button gets between **{gp['min_delta']}** and **+{gp['max_delta']}** chips. "
                     f"\nRequires **{gp['min_wallet']}+** chips in your wallet to try.",
        view=ThinkFastView(),
    )


# Giveaway

async def _run_giveaway_react(channel, t: TableState, trigger_point: str | None = None):
    gp = chaos.EVENTS_BY_ID["giveaway_react"].params
    emoji = gp["react_emoji"]
    window = 10
    msg = await _announce_random_event(
        channel, t, "giveaway_react", trigger_point,
        description=f"React with {emoji} within **{window} seconds** for a shot at the prize!"
                    f"\nWinner has 70% chance of **chips**, 30% odds of **90 minute ban**",
    )
    if msg is None:
        return
    try:
        await msg.add_reaction(emoji)
    except (discord.HTTPException, discord.Forbidden) as e:
        print(f"[Error] Failed to react to giveaway prompt: {e}")
        return

    # Strip off any reaction that isn't the giveaway emoji the moment it
    # lands, so a wrong emoji never actually "registers" on the message —
    # it just gets removed again straight away. This is on top of (not a
    # replacement for) the emoji filter below when tallying reactors; it's
    # what stops a wrong-emoji react from ever having a chance to reach —
    # or trip up — that part.
    async def _strip_wrong_emoji(payload: discord.RawReactionActionEvent):
        if payload.message_id != msg.id or bot_instance is None:
            return
        if payload.user_id == bot_instance.user.id:
            return  # ignore the bot's own reaction
        if str(payload.emoji) == emoji:
            return  # the right emoji — leave it alone
        try:
            user = payload.member or bot_instance.get_user(payload.user_id) \
                or await bot_instance.fetch_user(payload.user_id)
            await msg.remove_reaction(payload.emoji, user)
        except (discord.HTTPException, discord.Forbidden, discord.NotFound):
            pass  # e.g. missing Manage Messages — not worth crashing the event over

    if bot_instance is not None:
        bot_instance.add_listener(_strip_wrong_emoji, "on_raw_reaction_add")

    try:
        await asyncio.sleep(window)
    finally:
        if bot_instance is not None:
            bot_instance.remove_listener(_strip_wrong_emoji, "on_raw_reaction_add")

    try:
        fresh = await channel.fetch_message(msg.id)
    except (discord.NotFound, discord.HTTPException):
        return
    try:
        await fresh.clear_reactions()
    except (discord.HTTPException, discord.Forbidden):
        pass

    reactors = []
    try:
        for reaction in fresh.reactions:
            if str(reaction.emoji) != emoji:
                continue  # any other emoji still on the message is simply ignored
            async for user in reaction.users():
                if not user.bot:
                    reactors.append(user)
    except (discord.HTTPException, discord.Forbidden) as e:
        print(f"[Error] Failed to read giveaway reactors: {e}")
        # fall through with whatever we collected (possibly none) rather
        # than letting this bubble up and leave the event unresolved

    if not reactors:
        await _send_event_result(channel, msg, chaos.build_event_result_embed(
            "giveaway_react", "Nobody reacted in time.", no_result=True))
        return

    winner = random.choice(reactors)
    if random.random() < gp["kick_chance"]:
        # forced ban
        # they still finish their current hand if at table
        if t.game.get_player(winner.id):
            if winner.id not in t.game.kicked_users:
                t.game.kicked_users.append(winner.id)
            if winner.id not in t.game.pending_leaves:
                t.game.pending_leaves.append(winner.id)
            # not added to t.leave_cooldown_pending, shouldn't be payable.
            apply_global_lock(winner.id, gp["ban_seconds"], payable=False)
        try:
            await db.unlock_cosmetic(winner.id, "title", gp["kick_title_id"])
        except Exception as e:
            print(f"[Error] giveaway title unlock failed: {e}")
        await _send_event_result(channel, msg, chaos.build_event_result_embed(
            "giveaway_react", f"<@{winner.id}> won a stinky ban and the 🦶 title!"))
    else:
        prize = random.randint(gp["min_prize"], gp["max_prize"])
        await db.return_chips(winner.id, prize)
        try:
            await db.log_house_revenue(-prize, source="chaos_event")
        except Exception as e:
            print(f"[Error] giveaway revenue log failed: {e}")
        await _send_event_result(channel, msg, chaos.build_event_result_embed(
            "giveaway_react", f"<@{winner.id}> won the giveaway and won **{prize}** chips!"))


# Redistribution

class RedistributionView(discord.ui.View):
    def __init__(self, amount: int, entrants: set, window: int):
        super().__init__(timeout=window)
        self.amount = amount
        self.entrants = entrants
        self.expiry = time.time() + window

    @discord.ui.button(label="Enter", style=discord.ButtonStyle.green)
    async def enter(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = interaction.user.id
        if _event_expired(self.expiry):
            await interaction.response.send_message(EVENT_JOIN_MISSED, ephemeral=True)
            return
        if uid in self.entrants:
            await interaction.response.send_message(EVENT_JOIN_ALREADY, ephemeral=True)
            return
        balance, _ = await db.get_wallet(uid)
        if balance < self.amount:
            await interaction.response.send_message(
                f"❌ You need **{self.amount}** chips in your wallet to enter.", ephemeral=True)
            return
        if not await db.deduct_chips(uid, self.amount):
            await interaction.response.send_message("❌ Couldn't deduct chips. Try again.", ephemeral=True)
            return
        self.entrants.add(uid)
        await interaction.response.send_message(EVENT_JOIN_SUCCESS, ephemeral=True)


async def _run_redistribution(channel, t: TableState, trigger_point: str | None = None):
    gp = chaos.EVENTS_BY_ID["redistribution"].params
    amount = random.choice(gp["buy_in_options"])
    window = gp["window_seconds"]
    entrants: set = set()
    msg = await _announce_random_event(
        channel, t, "redistribution", trigger_point,
        description=f"Buy in for **{amount}** chips. \nAfter **{window}s**, the whole pool gets "
                     f"redistributed randomly among everyone who entered.",
        view=RedistributionView(amount, entrants, window),
    )
    if msg is None:
        return

    await asyncio.sleep(window)
    try:
        await msg.edit(view=None)
    except (discord.HTTPException, discord.NotFound):
        pass
    if not entrants:
        await _send_event_result(channel, msg, chaos.build_event_result_embed(
            "redistribution", "Nobody entered.", no_result=True))
        return

    pool = amount * len(entrants)
    entrants_list = list(entrants)
    random.shuffle(entrants_list)
    weights = [random.random() for _ in entrants_list]
    total_w = sum(weights)
    remaining = pool
    payouts = []
    for i, uid in enumerate(entrants_list):
        share = remaining if i == len(entrants_list) - 1 else int(pool * weights[i] / total_w)
        remaining -= share
        payouts.append((uid, share))
        if share > 0:
            await db.return_chips(uid, share)

    lines = "\n".join(f"<@{uid}> — **{share}**" for uid, share in payouts)
    await _send_event_result(channel, msg, chaos.build_event_result_embed(
        "redistribution",
        f"Pool was **{pool}** chips with **{len(entrants)}** entries:\n{lines}"
    ))


# Chip Shower

class ChipShowerView(discord.ui.View):
    def __init__(self, window: int, max_claimers: int):
        super().__init__(timeout=window + 3)
        self.claimed_uids: set = set()
        self.claims: list[tuple[int, int]] = []  # (uid, amount) — for the results embed
        self.total_given = 0
        self.expiry = time.time() + window
        self.max_claimers = max_claimers

    @discord.ui.button(label="🌧️ Grab Chips!", style=discord.ButtonStyle.green)
    async def grab(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = interaction.user.id
        if _event_expired(self.expiry):
            await interaction.response.send_message(EVENT_JOIN_MISSED, ephemeral=True)
            return
        if uid in self.claimed_uids:
            await interaction.response.send_message(EVENT_JOIN_ALREADY, ephemeral=True)
            return
        if len(self.claimed_uids) >= self.max_claimers:
            await interaction.response.send_message(
                f"❌ Too late!", ephemeral=True)
            return
        self.claimed_uids.add(uid)
        gp = chaos.EVENTS_BY_ID["chip_shower"].params
        amount = random.randint(gp["min_prize"], gp["max_prize"])
        self.claims.append((uid, amount))
        self.total_given += amount
        await db.return_chips(uid, amount)
        await interaction.response.send_message(f"{EVENT_JOIN_SUCCESS} You reach out and **{amount}** chips fall into your wallet!",
                                                  ephemeral=True)
        if len(self.claimed_uids) >= self.max_claimers:
            button.disabled = True
            try:
                await interaction.message.edit(view=self)
            except (discord.HTTPException, discord.NotFound):
                pass


async def _run_chip_shower(channel, t: TableState, trigger_point: str | None = None):
    gp = chaos.EVENTS_BY_ID["chip_shower"].params
    window = gp["window_seconds"]
    max_claimers = gp["max_claimers"]
    view = ChipShowerView(window, max_claimers)
    msg = await _announce_random_event(
        channel, t, "chip_shower", trigger_point,
        description=f"It's raining chips! You have **{window} seconds** to grab some!",
        view=view,
    )
    if msg is None:
        return

    await asyncio.sleep(window)
    try:
        await msg.edit(view=None)
    except (discord.HTTPException, discord.NotFound):
        pass
    if view.total_given > 0:
        try:
            await db.log_house_revenue(-view.total_given, source="chaos_event")
        except Exception as e:
            print(f"[Error] chip_shower revenue log failed: {e}")
        lines = "\n".join(f"<@{uid}> — **{amount}**" for uid, amount in view.claims)
        await _send_event_result(channel, msg, chaos.build_event_result_embed(
            "chip_shower",
            f"**{len(view.claimed_uids)}** people grabbed a total of **{view.total_given}** chips:\n{lines}"
        ))
    else:
        await _send_event_result(channel, msg, chaos.build_event_result_embed(
            "chip_shower", "Nobody grabbed anything.", no_result=True))


# Lottery

class LotteryEntryModal(discord.ui.Modal, title="🎟️ Lottery Entry"):
    amount = discord.ui.TextInput(label="Chips to enter with", max_length=8)

    def __init__(self, entries: dict, expiry: float):
        super().__init__()
        self.entries = entries
        self.expiry = expiry

    async def on_submit(self, interaction: discord.Interaction):
        # checks modal
        if _event_expired(self.expiry):
            await interaction.response.send_message(EVENT_JOIN_MISSED, ephemeral=True)
            return
        chips = parse_chips(self.amount.value)
        if chips is None or chips <= 0:
            await interaction.response.send_message("❌ Enter a valid amount.", ephemeral=True)
            return
        if interaction.user.id in self.entries:
            await interaction.response.send_message(EVENT_JOIN_ALREADY, ephemeral=True)
            return
        balance, _ = await db.get_wallet(interaction.user.id)
        if chips > balance:
            await interaction.response.send_message(f"❌ You only have **{balance}** chips in your wallet.", ephemeral=True)
            return
        if not await db.deduct_chips(interaction.user.id, chips):
            await interaction.response.send_message("❌ Couldn't deduct chips, try again.", ephemeral=True)
            return
        self.entries[interaction.user.id] = chips
        await interaction.response.send_message(f"{EVENT_JOIN_SUCCESS} Entered with **{chips}** chips.",
                                                  ephemeral=True)


class LotteryView(discord.ui.View):
    def __init__(self, entries: dict, window: int):
        super().__init__(timeout=window + 5)
        self.entries = entries
        self.expiry = time.time() + window

    @discord.ui.button(label="🎟️ Enter!", style=discord.ButtonStyle.blurple)
    async def enter(self, interaction: discord.Interaction, button: discord.ui.Button):
        if _event_expired(self.expiry):
            await interaction.response.send_message(EVENT_JOIN_MISSED, ephemeral=True)
            return
        await interaction.response.send_modal(LotteryEntryModal(self.entries, self.expiry))


async def _run_lottery(channel, t: TableState, trigger_point: str | None = None):
    gp = chaos.EVENTS_BY_ID["lottery"].params
    window = 30
    entries: dict[int, int] = {}
    msg = await _announce_random_event(
        channel, t, "lottery", trigger_point,
        description=(
            f"Enter with any amount. Your chances of winning increase if you put in more chips!\n"
            f"**{int(gp['tax_pct'] * 100)}%** of the pool is taxed to house revenue, "
            f"**{int(gp['jackpot_pct'] * 100)}%** feeds the jackpot, the rest goes to the winner.\n\n"
            f"**{window} seconds** to enter."
        ),
        view=LotteryView(entries, window),
    )
    if msg is None:
        return

    await asyncio.sleep(window)
    try:
        await msg.edit(view=None)
    except (discord.HTTPException, discord.NotFound):
        pass
    if not entries:
        await _send_event_result(channel, msg, chaos.build_event_result_embed(
            "lottery", "Nobody entered.", no_result=True))
        return

    total = sum(entries.values())
    r = random.uniform(0, total)
    cum = 0
    winner_uid = list(entries.keys())[-1]
    for uid, amt in entries.items():
        cum += amt
        if r <= cum:
            winner_uid = uid
            break

    tax = math.ceil(total * gp["tax_pct"])
    jackpot_cut = math.ceil(total * gp["jackpot_pct"])
    winnings = total - tax - jackpot_cut
    await db.return_chips(winner_uid, winnings)
    try:
        await db.log_house_revenue(tax, source="chaos_event")
    except Exception as e:
        print(f"[Error] lottery tax log failed: {e}")
    try:
        await db.adjust_jackpot(jackpot_cut)
    except Exception as e:
        print(f"[Error] lottery jackpot log failed: {e}")
    await _send_event_result(channel, msg, chaos.build_event_result_embed(
        "lottery",
        f"<@{winner_uid}> entered with **{entries[winner_uid]}** chips and won **{winnings}** chips! "
        f"Total entries: **{total}** chips."
    ))


# Double or Nothing

class DoubleOrNothingLoopView(discord.ui.View):
    def __init__(self, user_id: int, current_amount: int, announce_msg, player_states: dict,
                 round_num: int = 1, ephemeral_msg=None):
        super().__init__(timeout=30)
        self.user_id = user_id
        self.current_amount = current_amount
        self.announce_msg = announce_msg  # so busted/cashed-out can reply to the original announcement
        self.player_states = player_states  # shared with DoubleOrNothingStartView
        self.round_num = round_num
        self.ephemeral_msg = ephemeral_msg  # the actual ephemeral message this view is attached to can still edit it


    @discord.ui.button(label="Double it! 🎭", style=discord.ButtonStyle.red)
    async def double(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not your round.", ephemeral=True)
            return
        gp = chaos.EVENTS_BY_ID["double_or_nothing"].params
        if random.random() < gp["double_chance"]:
            self.current_amount *= 2

            # Hard cap on rounds, hitting forces an automatic cash-out right here instead of offering another round.
            if self.round_num >= gp["max_rounds"]:
                self.player_states[self.user_id] = "cashed_out"
                await db.return_chips(self.user_id, self.current_amount)
                try:
                    await db.log_house_revenue(-self.current_amount, source="chaos_event")
                except Exception as e:
                    print(f"[Error] double_or_nothing revenue log failed: {e}")
                await interaction.response.edit_message(
                    content=f"🏆 Auto-cashed out with **{self.current_amount}** chips.", view=None)
                await _send_event_result(interaction.channel, self.announce_msg, chaos.build_event_result_embed(
                    "double_or_nothing",
                    f"<@{self.user_id}> cashed out with **{self.current_amount}** chips!"
                ))
                self.stop()
                return

            new_view = DoubleOrNothingLoopView(self.user_id, self.current_amount, self.announce_msg, self.player_states, self.round_num + 1, self.ephemeral_msg)
            await interaction.response.edit_message(
                content=f"🎯 **Success!** You're at **{self.current_amount}** chips. Double again or cash out?",
                view=new_view,
            )
        else:
            self.player_states[self.user_id] = "busted"
            await interaction.response.edit_message(
                content=f"💥 **Busted!**", view=None)
            await _send_event_result(interaction.channel, self.announce_msg, chaos.build_event_result_embed(
                "double_or_nothing", f"<@{self.user_id}> busted.",
                no_result=True))
        self.stop()

    @discord.ui.button(label="Cash Out 💰", style=discord.ButtonStyle.grey)
    async def cash_out(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not your round.", ephemeral=True)
            return
        self.player_states[self.user_id] = "cashed_out"
        await db.return_chips(self.user_id, self.current_amount)
        try:
            await db.log_house_revenue(-self.current_amount, source="chaos_event")
        except Exception as e:
            print(f"[Error] double_or_nothing revenue log failed: {e}")
        await interaction.response.edit_message(
            content=f"💰 **Cashed out with {self.current_amount} chips!**", view=None)
        await _send_event_result(interaction.channel, self.announce_msg, chaos.build_event_result_embed(
            "double_or_nothing", f"<@{self.user_id}> cashed out with **{self.current_amount}** chips!"))
        self.stop()

    async def on_timeout(self):
        # 30s inaction gets cashed
        if self.player_states.get(self.user_id) != "playing":
            return  # already resolved
        self.player_states[self.user_id] = "cashed_out"
        try:
            await db.return_chips(self.user_id, self.current_amount)
            await db.log_house_revenue(-self.current_amount, source="chaos_event")
        except Exception as e:
            print(f"[Error] double_or_nothing timeout auto-cashout failed: {e}")
        if self.ephemeral_msg:
            try:
                await self.ephemeral_msg.edit(
                    content=f"⏰ **Time's up!** Auto-cashed out **{self.current_amount}** chips.", view=None)
            except (discord.HTTPException, discord.NotFound):
                pass
        try:
            await _send_event_result(self.announce_msg.channel, self.announce_msg, chaos.build_event_result_embed(
                "double_or_nothing",
                f"<@{self.user_id}> cashed out with **{self.current_amount}** chips!"
            ))
        except Exception as e:
            print(f"[Error] double_or_nothing timeout announce failed: {e}")




class DoubleOrNothingStartView(discord.ui.View):
    def __init__(self, window: int):
        super().__init__(timeout=window)
        # Per-user: "playing", "busted", or "cashed_out".
        self.player_states: dict[int, str] = {}
        self.expiry = time.time() + window

    @discord.ui.button(label="🎯 Start!", style=discord.ButtonStyle.green)
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = interaction.user.id
        if _event_expired(self.expiry):
            await interaction.response.send_message(EVENT_JOIN_MISSED, ephemeral=True)
            return
        if uid in self.player_states:
            #
            if self.player_states[uid] == "busted":
                await interaction.response.send_message("❌ You already lost! No second tries this event.",
                                                          ephemeral=True)
            else:
                await interaction.response.send_message(EVENT_JOIN_ALREADY, ephemeral=True)
            return

        gp = chaos.EVENTS_BY_ID["double_or_nothing"].params
        seed = gp["seed_chips"]
        balance, _ = await db.get_wallet(uid)
        if balance < seed:
            await interaction.response.send_message(
                f"❌ You need **{seed}** chips in your wallet to buy in.", ephemeral=True)
            return
        if not await db.deduct_chips(uid, seed):
            await interaction.response.send_message("❌ Couldn't deduct chips, try again.", ephemeral=True)
            return
        try:
            await db.log_house_revenue(seed, source="chaos_event")
        except Exception as e:
            print(f"[Error] double_or_nothing entry revenue log failed: {e}")

        self.player_states[uid] = "playing"
        await interaction.response.send_message(EVENT_JOIN_SUCCESS, ephemeral=True)

        loop_view = DoubleOrNothingLoopView(uid, seed, interaction.message, self.player_states)
        ephemeral_msg = await interaction.followup.send(
            f"🎯 <@{uid}> bought in for **{seed}** chips! Double it or cash out?",
            view=loop_view, ephemeral=True, wait=True,
        )
        loop_view.ephemeral_msg = ephemeral_msg

async def _run_double_or_nothing(channel, t: TableState, trigger_point: str | None = None):
    gp = chaos.EVENTS_BY_ID["double_or_nothing"].params
    window = gp["window_seconds"]
    msg = await _announce_random_event(
        channel, t, "double_or_nothing", trigger_point,
        description=f"Buy in for **{gp['seed_chips']}** chips, then it's a 50/50 to double or lose it "
                     f"all each round. **{window}s** to buy in.",
        view=DoubleOrNothingStartView(window),
    )
    if msg is None:
        return
    await asyncio.sleep(window)
    try:
        await msg.edit(view=None)
    except (discord.HTTPException, discord.NotFound):
        pass


_RANDOM_EVENT_HANDLERS = {
    "egirl_lover": _run_egirl_lover,
    "number_guess": _run_number_guess,
    "think_fast": _run_think_fast,
    "giveaway_react": _run_giveaway_react,
    "redistribution": _run_redistribution,
    "chip_shower": _run_chip_shower,
    "lottery": _run_lottery,
    "double_or_nothing": _run_double_or_nothing,
}


# Community Auction

COMMUNITY_AUCTION_WINDOW_SECONDS = 40


class CommunityAuctionBidModal(discord.ui.Modal, title="🔨 Place Your Bid"):
    amount = discord.ui.TextInput(label="Bid amount", max_length=6)

    def __init__(self, t: TableState, board_num: int, card_idx: int, bids: dict, board_label: str):
        super().__init__()
        self.t = t
        self.board_num = board_num
        self.card_idx = card_idx
        self.bids = bids  # (user_id, board_num) -> (card_idx, amount, source) shared across the whole auction
        self.board_label = board_label  # e.g. "Board 1 Card 3", or just "Card 3" outside Double Board

    async def on_submit(self, interaction: discord.Interaction):
        chips = parse_chips(self.amount.value)
        if chips is None or chips <= 0:
            await interaction.response.send_message("❌ Enter a valid bid amount.", ephemeral=True)
            return
        p = self.t.game.get_player(interaction.user.id)
        if not p:
            await interaction.response.send_message("❌ You're not seated at this table.", ephemeral=True)
            return

        # Bids draw from the wallet first, fall back to table chips
        wallet_balance, _ = await db.get_wallet(interaction.user.id)
        if wallet_balance > 0:
            source, available = "wallet", wallet_balance
        else:
            source, available = "table", p.chips

        if available <= 0:
            await interaction.response.send_message("❌ You have **0** chips to bid with.", ephemeral=True)
            return
        if chips > available:
            where = "in your wallet" if source == "wallet" else "at the table"
            await interaction.response.send_message(f"❌ You only have **{available}** chips {where}.", ephemeral=True)
            return

        # One active bid at a time per board
        self.bids[(interaction.user.id, self.board_num)] = (self.card_idx, chips, source)
        where = "your wallet" if source == "wallet" else "your table chips"
        await interaction.response.send_message(
            f"🔨 Bid placed: **{chips}** chips on **{self.board_label}**. "
            f"Placing another bid on this board will replace this one.", ephemeral=True)


class CommunityAuctionView(discord.ui.View):
    def __init__(self, t: TableState, bids: dict, n_cards_board1: int, n_cards_board2: int = 0):
        super().__init__(timeout=COMMUNITY_AUCTION_WINDOW_SECONDS + 5)
        self.t = t
        self.bids = bids
        self.double_board = n_cards_board2 > 0
        for i in range(n_cards_board1):
            label = f"B1 · Card {i + 1}" if self.double_board else f"Card {i + 1}"
            self.add_item(self._make_button(1, i, label, row=0))
        for i in range(n_cards_board2):
            self.add_item(self._make_button(2, i, f"B2 · Card {i + 1}", row=1))

    def _make_button(self, board_num: int, idx: int, label: str, row: int) -> discord.ui.Button:
        btn = discord.ui.Button(label=label, style=discord.ButtonStyle.blurple, row=row)

        async def callback(interaction: discord.Interaction):
            board_label = f"Board {board_num} Card {idx + 1}" if self.double_board else f"Card {idx + 1}"
            await interaction.response.send_modal(
                CommunityAuctionBidModal(self.t, board_num, idx, self.bids, board_label))

        btn.callback = callback
        return btn


async def _resolve_auction_bids_for_board(channel, t: TableState, community: list, blind_idx: set,
                                           bids_for_board: dict[int, tuple[int, int, str]],
                                           board_tag: str) -> bool:
    # Resolves one  board's worth of auction bids
    if not bids_for_board:
        return False

    # The single highest bid amount on this  board
    global_best = max(amount for _, amount, _ in bids_for_board.values())

    # Every card on this board that received a bid at that top amount, ties qualify
    winning_cards: dict[int, list[int]] = {}
    for uid, (card_idx, amount, source) in bids_for_board.items():
        if amount == global_best:
            winning_cards.setdefault(card_idx, []).append(uid)

    replaced_any = False
    for card_idx, uids in winning_cards.items():
        if card_idx >= len(community) or not t.game.deck.cards:
            continue  # state changed underneath us or deck exhausted skip safely

        # Reverify each bidder can still cover their bid
        payers: list[tuple["PokerPlayer", str]] = []
        for uid in uids:
            p = t.game.get_player(uid)
            if not p:
                continue
            _, _, source = bids_for_board[uid]
            if source == "wallet":
                wallet_balance, _ = await db.get_wallet(uid)
                if wallet_balance >= global_best:
                    payers.append((p, source))
            else:
                if p.chips >= global_best:
                    payers.append((p, source))
        if not payers:
            continue  # nobody who tied at the top can actually still afford it

        old_card = community[card_idx]
        new_card = t.game.deck.draw(1)[0]
        community[card_idx] = new_card
        replaced_any = True

        for p, source in payers:
            if source == "wallet":
                await db.deduct_chips(p.user_id, global_best)
            else:
                p.chips -= global_best
            try:
                await db.log_house_revenue(global_best, source="chaos_event")
            except Exception as e:
                print(f"[Error] Failed to log Community Auction revenue: {e}")

        old_label = "❓🌫" if card_idx in blind_idx else card_str(old_card)
        new_label = "❓🌫" if card_idx in blind_idx else card_str(new_card)
        names = " and ".join(f"<@{p.user_id}>" for p, _ in payers)
        label = f"{board_tag} card #{card_idx + 1}" if board_tag else f"card #{card_idx + 1}"
        try:
            await channel.send(
                f"🔨 {names} bet **{global_best}** chips, {label} got replaced "
                f"from **{old_label}** to **{new_label}**!"
            )
        except (discord.HTTPException, discord.Forbidden) as e:
            print(f"[Error] Failed to announce Community Auction replacement: {e}")

    return replaced_any


async def _run_community_auction(channel, t: TableState):
    double_board_active = "double_board" in t.chaos_modifiers
    bids: dict[tuple[int, int], tuple[int, int, str]] = {}  # (user_id, board_num) -> (card_idx, amount, source)

    board1 = list(t.game.community)
    blind1 = t.game.blinded_community_idx
    card_lines1 = "  ".join(
        f"**{i + 1}.** {'❓🌫' if i in blind1 else card_str(c)}" for i, c in enumerate(board1)
    )


    if double_board_active:
        board2 = list(t.game.community2)
        blind2 = t.game.blinded_community2_idx
        card_lines2 = "  ".join(
            f"**{i + 1}.** {'🌫️❓' if i in blind2 else card_str(c)}" for i, c in enumerate(board2)
        )
        description = (
            f"**Board 1:** {card_lines1}\n**Board 2:** {card_lines2}\n\n"
            f"Bid your chips to replace a card you don't like! "
            f"Only the single **highest bid on each board** actually pays and gets replaced.\n\n"
            f"**{COMMUNITY_AUCTION_WINDOW_SECONDS} seconds** to bid."
        )
    else:
        blind2 = set()
        description = (
            f"{card_lines1}\n\nBid your chips to replace a card you don't like! "
            f"Only the single **highest bid across all 5 cards** actually pays and gets replaced.\n\n"
            f"**{COMMUNITY_AUCTION_WINDOW_SECONDS} seconds** to bid."
        )

    embed = discord.Embed(title="🔨 Community Card Auction", description=description, color=0xE67E22)
    try:
        msg = await channel.send(
            embed=embed,
            view=CommunityAuctionView(t, bids, len(board1), len(board2) if double_board_active else 0),
        )
    except (discord.HTTPException, discord.Forbidden) as e:
        print(f"[Error] Failed to send Community Auction prompt: {e}")
        return

    await asyncio.sleep(COMMUNITY_AUCTION_WINDOW_SECONDS)
    try:
        await msg.edit(view=None)
    except (discord.HTTPException, discord.NotFound):
        pass

    if not bids:
        return

    if double_board_active:
        bids_b1 = {uid: v for (uid, board_num), v in bids.items() if board_num == 1}
        bids_b2 = {uid: v for (uid, board_num), v in bids.items() if board_num == 2}
        replaced1 = await _resolve_auction_bids_for_board(
            channel, t, t.game.community, blind1, bids_b1, "Board 1")
        replaced2 = await _resolve_auction_bids_for_board(
            channel, t, t.game.community2, blind2, bids_b2, "Board 2")
        replaced_any = replaced1 or replaced2
    else:
        bids_flat = {uid: v for (uid, board_num), v in bids.items()}
        replaced_any = await _resolve_auction_bids_for_board(
            channel, t, t.game.community, blind1, bids_flat, "")

    if replaced_any:
        # Board image still shows the pre-auction cards until this
        await refresh(channel, t, cosmetics_cache=t.cosmetics_cache)


# Gamble the Gamble

GAMBLE_WINDOW_SECONDS = 75

_GAMBLE_RANK_ALIASES = {
    "A": "A", "ACE": "A",
    "K": "K", "KING": "K",
    "Q": "Q", "QUEEN": "Q",
    "J": "J", "JACK": "J",
    "T": "T", "10": "T", "TEN": "T",
    **{str(n): str(n) for n in range(2, 10)},
}
_GAMBLE_SUIT_ALIASES = {
    "S": "s", "SPADE": "s", "SPADES": "s", "♠": "s", "♠️": "s",
    "H": "h", "HEART": "h", "HEARTS": "h", "♥": "h", "♥️": "h",
    "D": "d", "DIAMOND": "d", "DIAMONDS": "d", "♦": "d", "♦️": "d",
    "C": "c", "CLUB": "c", "CLUBS": "c", "♣": "c", "♣️": "c",
}


class GambleTheGambleModal(discord.ui.Modal, title="🎲 Gamble the Gamble"):
    rank = discord.ui.TextInput(label="Rank (A, 2-9, 10/T, J, Q, K)", max_length=5)
    suit = discord.ui.TextInput(label="Suit (Spades/Hearts/Diamonds/Clubs)", max_length=10)
    amount = discord.ui.TextInput(label="Bet amount", max_length=6)

    def __init__(self, t: TableState, bets: dict, deadline: float):
        super().__init__()
        self.t = t
        self.bets = bets
        self.deadline = deadline

    async def on_submit(self, interaction: discord.Interaction):
        if time.time() > self.deadline:
            await interaction.response.send_message(
                "❌ Betting window has closed.", ephemeral=True)
            return

        rank_char = _GAMBLE_RANK_ALIASES.get(self.rank.value.strip().upper())
        suit_char = _GAMBLE_SUIT_ALIASES.get(self.suit.value.strip().upper())
        if not rank_char or not suit_char:
            await interaction.response.send_message(
                "❌ Invalid entry.", ephemeral=True)
            return

        gp = chaos.get("gamble_the_gamble").params
        chips = parse_chips(self.amount.value)
        if chips is None or chips <= 0:
            await interaction.response.send_message("❌ Enter a valid bet amount.", ephemeral=True)
            return
        if chips > gp["max_bet"]:
            await interaction.response.send_message(f"❌ Max bet is **{gp['max_bet']}** chips.", ephemeral=True)
            return

        p = self.t.game.get_player(interaction.user.id)
        if not p:
            p = next((pj for pj in self.t.game.pending_joins if pj.user_id == interaction.user.id), None)
        if not p:
            await interaction.response.send_message("❌ You're not seated at this table.", ephemeral=True)
            return
        if interaction.user.id in self.bets:
            await interaction.response.send_message("❌ You've already placed a bet this round.", ephemeral=True)
            return
        if chips > p.chips:
            await interaction.response.send_message(f"❌ You only have **{p.chips}** chips at the table.", ephemeral=True)
            return

        p.chips -= chips  # deducted now; paid back out (with any winnings) once resolved
        self.bets[interaction.user.id] = (rank_char, suit_char, chips)
        await interaction.response.send_message(
            f"🎲 Bet placed: **{chips}** chips on **{rank_char}{SUIT_EMOJI.get(suit_char, '')}**. Good luck!",
            ephemeral=True)


class GambleTheGambleView(discord.ui.View):
    def __init__(self, t: TableState, bets: dict, deadline: float):
        super().__init__(timeout=GAMBLE_WINDOW_SECONDS + 5)
        self.t = t
        self.bets = bets
        self.deadline = deadline

    @discord.ui.button(label="🎲 Place Bet", style=discord.ButtonStyle.blurple)
    async def place_bet(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(GambleTheGambleModal(self.t, self.bets, self.deadline))


async def _run_gamble_the_gamble(channel, t: TableState) -> dict:

    bets: dict[int, tuple] = {}
    deadline = time.time() + GAMBLE_WINDOW_SECONDS
    gp = chaos.get("gamble_the_gamble").params
    embed = discord.Embed(
        title="🎲 Gamble the Gamble",
        description=(
            f"Before hands are dealt, bet up to **{gp['max_bet']}** chips on a rank and suit!\n\n"
            f"**{gp['exact_mult']}x** back if you nail both rank AND suit\n"
            f"**{gp['split_mult']}x** back if you get rank on one card and suit on another\n"
            f"**{gp['rank_mult']}x** back for the rank only\n"
            f"**{gp['suit_mult']}x** back for the suit only\n\n"
            f"**{GAMBLE_WINDOW_SECONDS} seconds** to place a bet."
        ),
        color=0x2ECC71,
    )
    try:
        msg = await channel.send(embed=embed, view=GambleTheGambleView(t, bets, deadline))
    except (discord.HTTPException, discord.Forbidden) as e:
        print(f"[Error] Failed to send Gamble the Gamble prompt: {e}")
        return bets

    await asyncio.sleep(GAMBLE_WINDOW_SECONDS)
    try:
        await msg.edit(view=None)
    except (discord.HTTPException, discord.NotFound):
        pass
    return bets


async def _resolve_gamble_the_gamble(channel, t: TableState, bets: dict):
    # checks whole hand
    if not bets:
        return
    gp = chaos.get("gamble_the_gamble").params

    for uid, (rank_char, suit_char, amount) in bets.items():
        p = t.game.get_player(uid)
        if not p or not p.hole_cards:
            continue  # if they left before the deal, bet chips stay deducted

        mult, result_text = 0, "didn't guess right"
        exact_hit = False
        rank_hit = False
        suit_hit = False
        for card in p.hole_cards:
            actual_rank = Card.STR_RANKS[Card.get_rank_int(card)]
            actual_suit = Card.INT_SUIT_TO_CHAR_SUIT[Card.get_suit_int(card)]
            rank_match = rank_char == actual_rank
            suit_match = suit_char == actual_suit

            if rank_match and suit_match:
                exact_hit = True
                break  # can't beat an exact match — stop looking
            rank_hit = rank_hit or rank_match
            suit_hit = suit_hit or suit_match

        if exact_hit:
            mult, result_text = gp["exact_mult"], "guessed the right suit and number"
        elif rank_hit and suit_hit:
            mult, result_text = gp["split_mult"], "guessed the right number on one card and the right suit on another"
        elif rank_hit:
            mult, result_text = gp["rank_mult"], "guessed the right number"
        elif suit_hit:
            mult, result_text = gp["suit_mult"], "guessed the right suit"

        # int(), not round() — mult can now be fractional (suit_mult=0.5),
        # and a naive float payout would leave p.chips holding a fractional
        # chip count for odd bet amounts (e.g. 501 * 0.5 = 250.5).
        payout = int(amount * mult)
        if payout > 0:
            p.chips += payout
        net = payout - amount

        try:
            await db.log_house_revenue(-net, source="chaos_event")
        except Exception as e:
            print(f"[Error] Failed to log Gamble the Gamble revenue: {e}")
        try:
            await channel.send(
                f"🎲 <@{uid}> {result_text} — {'+' if net >= 0 else ''}{net} chips!"
            )
        except (discord.HTTPException, discord.Forbidden) as e:
            print(f"[Error] Failed to announce Gamble the Gamble result: {e}")


# Uno Reverse swap button

class UnoSwapView(discord.ui.View):
    """Shown in the ephemeral 'My Cards' response when the clicking player
    holds an unused Uno Reverse card and it's still early enough to use it
    button disables after use"""

    def __init__(self, t: TableState, channel):
        super().__init__(timeout=120)
        self.t = t
        self.channel = channel

    @discord.ui.button(label="🔄 Swap Hands", style=discord.ButtonStyle.blurple)
    async def swap(self, interaction: discord.Interaction, button: discord.ui.Button):
        ok, msg, target = self.t.game.swap_hands(interaction.user.id)
        if not ok:
            await interaction.response.edit_message(content=msg, view=None)
            self.stop()
            return
        button.disabled = True
        await interaction.response.edit_message(content=f"✅ {msg}", view=self)
        try:
            await self.channel.send(msg)
        except (discord.HTTPException, discord.Forbidden) as e:
            print(f"[Error] Failed to announce Uno swap: {e}")
        self.stop()

# Pulled out of GameView.btn_hole
async def send_my_cards(t: TableState, interaction: discord.Interaction):
    p = t.game.get_player(interaction.user.id)
    if not p or not p.hole_cards:
        await interaction.response.send_message("❌ No cards right now.", ephemeral=True)
        return

    # can't see hand before blind
    game = t.game
    if ("bomb_pot" in game.chaos_modifiers and game.street.name == "PREFLOP"
            and not p.all_in and p.bet < game.current_bet):
        await interaction.response.send_message(
            "💣 **Bomb Pot** -- call or fold before you can see your hand.", ephemeral=True)
        return

    # Classified Report
    # see next seats hand
    target = p
    classified_note = ""
    if "classified_report" in t.chaos_modifiers:
        players = t.game.players
        try:
            idx = players.index(p)
        except ValueError:
            idx = None
        if idx is not None and len(players) > 1:
            target = players[(idx + 1) % len(players)]
            classified_note = f"🕵️ You have <@{target.user_id}>'s hand.\n"
        # 1 player fallback to own hand cuz idk

    # Blindness
    blind_idx = t.game.blinded_hole_idx.get(target.user_id)
    blind_positions = {blind_idx} if blind_idx is not None else set()
    if blind_positions:
        card_labels = [
            "❓🌫" if i in blind_positions else card_str(c)
            for i, c in enumerate(target.hole_cards)
        ]
        cards_text = "  ".join(card_labels)
    else:
        cards_text = hand_str(target.hole_cards)

    strength = ""
    visible_hole = [c for i, c in enumerate(target.hole_cards) if i not in blind_positions]
    # remove strength, could add smth to calc strengh with only the revealed cards too
    # no card emoji indicators
    visible_board1 = [c for i, c in enumerate(t.game.community)
                       if i not in t.game.blinded_community_idx]
    double_board_active = "double_board" in t.game.chaos_modifiers
    if double_board_active:
        # Double board show the ranking against each board separately
        visible_board2 = [c for i, c in enumerate(t.game.community2)
                           if i not in t.game.blinded_community2_idx]
        board_lines = []
        for board_num, board in ((1, visible_board1), (2, visible_board2)):
            if len(visible_hole) + len(board) >= 5:
                score = hand_eval.evaluate_any(evaluator, visible_hole, board)
                rank = evaluator.class_to_string(evaluator.get_rank_class(score))
                pct = round((1 - score / 7462) * 100, 1)
                board_lines.append(f"**Board {board_num}:** {rank} (top {100 - pct:.0f}%)")
        if board_lines:
            strength = "\n" + "\n".join(board_lines)
    elif len(visible_hole) + len(visible_board1) >= 5:
        score = hand_eval.evaluate_any(evaluator, visible_hole, visible_board1)
        rank = evaluator.class_to_string(evaluator.get_rank_class(score))
        pct = round((1 - score / 7462) * 100, 1)
        strength = f"\n**Hand:** {rank} (top {100 - pct:.0f}%)"

    shiny = " ✨" if target.shiny_ids else ""
    # always reflects the clicking user's own card
    uno_note = ""
    uno_view = discord.utils.MISSING
    if p.uno_color:
        uno_note = f"\n{UNO_COLOR_EMOJI.get(p.uno_color, '🔄')} You have a **{p.uno_color.title()} Uno Reverse** card!"
        if t.game.street.name in ("PREFLOP", "FLOP", "TURN"):
            uno_view = UnoSwapView(t, interaction.channel)
        else:
            what = "flop" if "reverse" in t.game.chaos_modifiers else "river"
            uno_note += f"\n*(too late to swap now! The {what}'s already out)*"

    # Bounty gets own private target
    # Classified Report shouldn't override
    bounty_note = ""
    if "bounty" in t.chaos_modifiers:
        hunt_target = t.game.bounty_targets.get(p.user_id)
        if hunt_target is not None:
            bounty_note = f"\n🎯💰 Get <@{hunt_target}> to fold to you and claim their chips!"

    if classified_note:
        caption = f"{classified_note}{strength}\n**Cards:** {cards_text}{shiny}{uno_note}{bounty_note}"
    else:
        caption = f"Your hole cards — {target.chips} {get_chip_emoji(t)} at table{strength}\n**Cards:** {cards_text}{shiny}{uno_note}{bounty_note}"

    if USE_IMAGES:
        await interaction.response.defer(ephemeral=True)
        try:
            # Card Borders cosmetic
            target_cosmetics = await db.get_cosmetics(target.user_id)
            # Card Size preference — this response is ephemeral (only the
            # clicking user sees it), so it's safe to size it for their
            # screen specifically, regardless of whose hand is being shown.
            viewer_pref = await db.get_player_preference(interaction.user.id)
            compact = viewer_pref.get("card_size") == "compact"
            file = await asyncio.to_thread(
                card_images.make_strip, target.hole_cards, 0, True, target.shiny_ids, blind_positions,
                p.uno_color,  # your own Uno Reverse card, appended to whichever hand you're viewing
                "cute_mode" in t.chaos_modifiers,
                border_id=target_cosmetics.get("active_border"),
                compact=compact,
            )
            await interaction.followup.send(caption, file=file, view=uno_view, ephemeral=True)
        except Exception as e:
            print(f"🚨 [ERROR] {e}")
            import traceback
            traceback.print_exc()
            await interaction.followup.send(caption, view=uno_view, ephemeral=True)  # text-only fallback
        return

    await interaction.response.send_message(caption, view=uno_view, ephemeral=True)


class AuctionCardsOnlyView(discord.ui.View):
    # let players still view hand during auction
    def __init__(self, t: TableState):
        super().__init__(timeout=None)
        self.t = t

    @discord.ui.button(label="My Cards", style=discord.ButtonStyle.grey)
    async def btn_hole(self, interaction: discord.Interaction, button: discord.ui.Button):
        await send_my_cards(self.t, interaction)


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

        await _handle_post_action(interaction.guild, interaction.channel, self.t)

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

        # ── Rejoin lock check (cross-table — see global_rejoin_locks) ──────────
        lock = get_global_lock(uid)
        if lock:
            expiry = lock["expiry"]
            if not lock["payable"]:
                # A ban (e.g. the Giveaway random event) — no fee bypasses this.
                await interaction.response.send_message(
                    f"❌ You're temporarily banned. It expires <t:{int(expiry)}:R>.",
                    ephemeral=True,
                )
                return

            # 🚨 NEW: Fee is dynamically set to 2x the table's current Big Blind
            fee = self.t.game.BIG_BLIND * config.REJOIN_FEE_MULTIPLIER

            if fee > 0 and bal >= fee and bal - fee >= min_w:
                await interaction.response.send_message(
                    f"⏳ You recently left a table.\n"
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
                    f"⏳ You recently left a table.\n"
                    f"Your cooldown expires <t:{int(expiry)}:R>.{bypass_note}",
                    ephemeral=True,
                )
            return

        clear_global_lock(uid)

        if bal < min_w:
            await interaction.response.send_message(
                f"❌ Need at least **{min_w}** {get_chip_emoji(self.t)} to join. Wallet: **{bal}** {get_chip_emoji(self.t)}.", ephemeral=True)
            return

        pref = await db.get_player_preference(uid)
        default_bb = pref.get("default_buyin_amount", 0)
        if default_bb != 0:

            if default_bb == -1:
                # Max wallet
                default_stack = min(bal, max_w) if max_w > 0 else bal

            elif default_bb < 0:
                # Relative BB amount
                bb = self.t.game.BIG_BLIND
                default_stack = abs(default_bb) * bb

            else:
                # Absolute chip amount
                default_stack = default_bb

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
        pot_display = "🙈 hidden" if "hidden_pot" in self.t.chaos_modifiers else f"{g.pot} {get_chip_emoji(self.t)}"
        await interaction.followup.send(
            f"**Raise options** — Pot: {pot_display}  |  Call: {call_amt}  |  Stack: {p.chips if p else '?'}\n",
            view=view, ephemeral=True)

    @discord.ui.button(label="Fold", style=discord.ButtonStyle.red, row=1)
    async def btn_fold(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = interaction.user.id
        if not self.t.game.is_turn(uid):
            await interaction.response.send_message("❌ It's not your turn.", ephemeral=True)
            return

        p = self.t.game.get_player(uid)

        # Prevent accidental folds of Flush or higher (only works if there are
        # enough total cards to rank a hand — under "Reverse" the board can
        # have just 1-2 cards on a real street, unlike the usual 3+).
        # Chaos: Blindness — this must never evaluate off cards the player
        # can't actually see: their own smudged hole card, or any smudged
        # community card (community blindness hides that card from
        # EVERYONE, not just this player). Left unfiltered, "You currently
        # have a Flush!" would hand back exactly the information Blindness
        # is supposed to be withholding. Filter both out first, same as
        # the My Cards strength preview does.
        is_strong_hand = False
        if p and p.hole_cards:
            blind_hole_idx = self.t.game.blinded_hole_idx.get(uid)
            visible_hole = [c for i, c in enumerate(p.hole_cards) if i != blind_hole_idx]
            visible_board = [c for i, c in enumerate(self.t.game.community)
                              if i not in self.t.game.blinded_community_idx]
            if len(visible_hole) + len(visible_board) >= 5:
                score = hand_eval.evaluate_any(evaluator, visible_hole, visible_board)
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
        await send_my_cards(self.t, interaction)

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

def _build_cosmetics_embed_and_view(user_id: int, cosmetics: dict, page: str = "titles"):
    """Build the /poker titles embed and its interactive select-menu view on separate pages."""
    owned_titles = set(cosmetics["unlocked_titles"])
    owned_msgs = set(cosmetics["unlocked_win_msgs"])
    owned_skins = set(cosmetics.get("unlocked_skins", []))
    active_t = cosmetics.get("active_title")
    active_m = cosmetics.get("active_win_msg")
    active_sk = cosmetics.get("active_skin")

    embed = discord.Embed(color=0x9b59b6)

    # ── TITLES PAGE ────────────────────────────────────────────────────────────
    if page == "titles":
        embed.title = f"🎖️ Titles ({len(owned_titles)}/{len(db.get_visible_cosmetics_for_user(user_id, owned_titles, db.TITLES))} unlocked)"
        visible_titles = db.get_visible_cosmetics_for_user(user_id, owned_titles, db.TITLES)
        t_lines = []
        for tid, info in visible_titles.items():
            rarity = db.RARITY_LABEL.get(info["rarity"], "")
            display_str = (
                f"{info['display']} - {rarity}"
                if info["display"].startswith("<")
                else f"`{info['display']}` - {rarity}"
            ) if rarity else (
                f"{info['display']}"
                if info["display"].startswith("<")
                else f"`{info['display']}`"
            )
            if tid in owned_titles:
                equipped = "  ◀ **equipped**" if tid == active_t else ""
                desc = f" — *{info['description']}*" if info.get('description') else ""
                t_lines.append(f"✅ {display_str}{desc}{equipped}")
            else:
                desc = info['description'] if info['rarity'] != 'legendary' else "???"
                t_lines.append(f"🔒 {display_str} — *{desc}*")

        # Group exactly 10 items per field
        chunk_size = 10
        t_chunks = [t_lines[i:i + chunk_size] for i in range(0, len(t_lines), chunk_size)]

        if not t_chunks:
            embed.add_field(name="\u200b", value="None yet.", inline=False)
        else:
            for chunk in t_chunks:
                embed.add_field(name="\u200b", value="\n".join(chunk)[:1024], inline=False)

    # ── WIN MESSAGES PAGE ──────────────────────────────────────────────────────
    elif page == "winmsgs":
        visible_winmsgs = db.get_visible_cosmetics_for_user(user_id, owned_msgs, db.WIN_MESSAGES)
        embed.title = f"💬 Win Messages ({len(owned_msgs)}/{len(visible_winmsgs)} unlocked)"
        m_lines = []
        for mid, info in visible_winmsgs.items():
            rarity = db.RARITY_LABEL.get(info["rarity"], "")
            display_str = f"{info['display']} - {rarity}" if rarity else f"{info['display']}"
            if mid in owned_msgs:
                equipped = "  ◀ **equipped**" if mid == active_m else ""
                desc = f" — *{info['description']}*" if info.get('description') else ""
                m_lines.append(f"✅ {display_str}{desc}{equipped}")
            else:
                desc = info['description'] if info['rarity'] != 'legendary' else "???"
                m_lines.append(f"🔒 {display_str} — *{desc}*")

        # Group exactly 10 items per field
        chunk_size = 10
        m_chunks = [m_lines[i:i + chunk_size] for i in range(0, len(m_lines), chunk_size)]

        if not m_chunks:
            embed.add_field(name="\u200b", value="None yet.", inline=False)
        else:
            for chunk in m_chunks:
                embed.add_field(name="\u200b", value="\n".join(chunk)[:1024], inline=False)

        # ── CARD SKINS PAGE ────────────────────────────────────────────────────────
        if page == "skins":
            visible_skins = db.get_visible_cosmetics_for_user(user_id, owned_skins, db.SKINS)
            embed.title = f"🎨 Card Skins ({len(owned_skins)}/{len(visible_skins)} unlocked)"
            s_lines = []
            for sid, info in visible_skins.items():
                display_str = f"**{info['display']}**"
                desc = f" — *{info['desc']}*" if 'desc' in info else ""
                if sid in owned_skins:
                    equipped = "  ◀ **equipped**" if sid == active_sk else ""
                    s_lines.append(f"✅ {display_str}{desc}{equipped}")
                else:
                    s_lines.append(f"🔒 {display_str} — *{desc}*")

            chunk_size = 15
            s_chunks = [s_lines[i:i + chunk_size] for i in range(0, len(s_lines), chunk_size)]
            if not s_chunks:
                embed.description = "*No card skins available yet.*"
            else:
                for chunk in s_chunks:
                    embed.add_field(name="\u200b", value="\n".join(chunk), inline=False)

            embed.set_footer(text="Equipped skins show on your hole cards for My Cards and at showdown.")

    if page != "skins":
        embed.set_footer(text="Use the dropdown below to equip — only your unlocked items appear.")
    view = CosmeticsView(user_id, owned_titles, owned_msgs, owned_skins, active_t, active_m, active_sk, page)
    return embed, view

class CosmeticsView(discord.ui.View):
    """Attach Select menus and a page toggle button to /poker titles."""

    _ALL_PAGES = ["titles", "winmsgs", "skins"]
    _PAGE_INFO = {"titles": ("View Titles", "🎖"),
                  "winmsgs": ("View Win Messages", "💬"),
                  "skins": ("View Card Skins", "🎨")}

    def __init__(
        self,
        user_id: int,
        owned_titles: set[str],
        owned_msgs: set[str],
        owned_skins: set[str],
        active_title: str | None,
        active_msg: str | None,
        active_skin: str | None,
        page: str = "titles"
    ):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.page = page
        self.message: discord.Message | discord.WebhookMessage | None = None

        if self.page == "titles":
            # ── Title select ───────────────────────────────────────────────────
            title_opts = [discord.SelectOption(label="— Remove title —", value="none", emoji="❌")]
            for tid in owned_titles:
                info = db.TITLES.get(tid)
                if info:
                    title_opts.append(discord.SelectOption(
                        label=info["display"], value=tid, default=(tid == active_title)
                    ))

            title_select = discord.ui.Select(
                placeholder="🎖️ Equip a title…", options=title_opts[:25], custom_id="cosmetics:title", row=0
            )
            title_select.callback = self._on_title_select
            self.add_item(title_select)

        elif self.page == "winmsgs":
            # ── Win-message select ─────────────────────────────────────────────
            msg_opts = [discord.SelectOption(label="— Remove win message —", value="none", emoji="❌")]
            for mid in owned_msgs:
                info = db.WIN_MESSAGES.get(mid)
                if info:
                    msg_opts.append(discord.SelectOption(
                        label=info["display"], value=mid, default=(mid == active_msg)
                    ))

            msg_select = discord.ui.Select(
                placeholder="💬 Equip a win message…", options=msg_opts[:25], custom_id="cosmetics:winmsg", row=0
            )
            msg_select.callback = self._on_msg_select
            self.add_item(msg_select)

        else:
            # ── Card Skin select ─────────────────────────────────────────────────
            skin_opts = [discord.SelectOption(label="— Remove skin —", value="none", emoji="❌")]
            for sid in owned_skins:
                info = db.SKINS.get(sid)
                if info:
                    skin_opts.append(discord.SelectOption(
                        label=info["display"], value=sid, default=(sid == active_skin)
                    ))
            skin_select = discord.ui.Select(
                placeholder="🎨 Equip a card skin…", options=skin_opts[:25], custom_id="cosmetics:skin", row=0
            )
            skin_select.callback = self._on_skin_select
            self.add_item(skin_select)

        # Pagination — one button per OTHER page (not the one you're on),
        # so both alternatives are always one click away instead of
        # cycling titles → win messages → skins → titles via a single
        # "next" button.
        for target_page in self._ALL_PAGES:
            if target_page == self.page:
                continue
            label, emoji = self._PAGE_META[target_page]
            switch_btn = discord.ui.Button(label=label, style=discord.ButtonStyle.primary, row=1, emoji=emoji)
            switch_btn.callback = self._make_switch_callback(target_page)
            self.add_item(switch_btn)

    def _make_switch_callback(self, target_page: str):
        async def _callback(interaction: discord.Interaction):
            await self._on_switch(interaction, target_page)
        return _callback

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This panel belongs to someone else.", ephemeral=True)
            return False
        return True

    async def _on_switch(self, interaction: discord.Interaction, target_page: str):
        if not await self._guard(interaction): return
        cosmetics = await db.get_cosmetics(self.user_id)
        embed, new_view = _build_cosmetics_embed_and_view(self.user_id, cosmetics, page=target_page)
        new_view.message = self.message
        await interaction.response.edit_message(embed=embed, view=new_view)

    async def _on_title_select(self, interaction: discord.Interaction):
        if not await self._guard(interaction): return
        chosen = interaction.data["values"][0]
        tid = None if chosen == "none" else chosen
        await db.set_active_title(self.user_id, tid)
        cosmetics = await db.get_cosmetics(self.user_id)
        embed, new_view = _build_cosmetics_embed_and_view(self.user_id, cosmetics, page=self.page)
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
        embed, new_view = _build_cosmetics_embed_and_view(self.user_id, cosmetics, page=self.page)
        new_view.message = self.message
        label = db.WIN_MESSAGES[mid]["display"] if mid else "removed"
        await interaction.response.edit_message(
            content=f"✅ Win message set to **{label}**." if mid else "✅ Win message removed.",
            embed=embed, view=new_view)

    async def _on_skin_select(self, interaction: discord.Interaction):
        chosen = interaction.data["values"][0]
        sid = None if chosen == "none" else chosen
        await db.set_active_skin(self.user_id, sid)

        label = db.SKINS[sid]["display"] if sid else "removed"
        await interaction.response.send_message(
            content=f"✅ Card skin set to **{label}**." if sid else "✅ Card skin removed.",
            ephemeral=True
        )
        self.stop()

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


async def _autocomplete_skin(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    """Only shows card skins the user has already unlocked."""
    cosmetics = await db.get_cosmetics(interaction.user.id)
    owned = set(cosmetics.get("unlocked_skins", []))
    choices = [app_commands.Choice(name="— Remove card skin —", value="none")]
    for sid in owned:
        info = db.SKINS.get(sid)
        if info and current.lower() in info["display"].lower():
            choices.append(app_commands.Choice(name=info["display"], value=sid))
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
    elif kind == "skin":
        catalog = db.SKINS
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
        self.expire_bans.start()

    async def cog_load(self):
        # Reload custom cosmetics from the database whenever the poker cog is loaded/reloaded.
        await db.load_custom_cosmetics()

    def cog_unload(self):
        self.daily_backup.cancel()
        self.expire_bans.cancel()

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return
        key = (message.guild.id, message.channel.id)
        t = get_table(key)
        if not t:
            return

        # ── Random events: message-based ones (phrase chant, number guess)
        # are checked independently of hand state, since an event can
        # still be resolving between hands.
        if t.active_random_event:
            await _handle_random_event_message(message, t)

        # ── Chaos: "67" title bait — also checked independently of hand
        # state, since the whole point is it's open between hands (from
        # right after a 6-7 win until the next hand starts).
        if t.sixseven_bait_active and SIXSEVEN_PATTERN.search(message.content):
            try:
                if await db.unlock_cosmetic(message.author.id, "title", "gen_alpha"):
                    await message.channel.send(
                        f"💀 <@{message.author.id}> said it. Unlocked the **gen alpha** title."
                    )
            except Exception as e:
                print(f"[poker] 67 bait unlock error: {e}")
                traceback.print_exc()

        if t.game.street == Street.WAITING:
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
        clean_zip_name = f"data_backup_{date_str}.zip"
        zip_path = os.path.join("", clean_zip_name)

        data_dir = os.path.join(base_dir, "..", "data")

        try:
            # Force SQLite to flush the WAL to the main DB safely for the main poker DB
            async with db._write_lock:
                conn = await db._get_db()
                await conn.execute("PRAGMA wal_checkpoint(RESTART)")

            try:
                from tournament import tournament_db
                await tournament_db.checkpoint()
            except ImportError:
                pass
            except Exception:
                traceback.print_exc()

            # 2. Write the zip file safely using absolute paths and arcname
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(data_dir):
                    for file in files:
                        full_path = os.path.join(root, file)

                        zipf.write(
                            full_path,
                            arcname=os.path.relpath(full_path, data_dir)
                        )

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

    # ── Expired ban sweeper ──────────────────────────────────────────────
    @tasks.loop(minutes=5)
    async def expire_bans(self):
        try:
            expired = await db.get_expired_bans()
            for b in expired:
                await db.delete_ban_by_id(b['id'])
                guild = self.bot.get_guild(b['guild_id'])
                # Clear from any live in-memory table state for this guild.
                for (gid, cid), t in tables.items():
                    if gid == b['guild_id'] and (b['table_name'] is None or t.name.lower() == (b['table_name'] or '').lower()):
                        if b['user_id'] in t.game.banned_users:
                            t.game.banned_users.remove(b['user_id'])
                if guild:
                    try:
                        user = self.bot.get_user(b['user_id']) or await self.bot.fetch_user(b['user_id'])
                        embed = discord.Embed(
                            title="✅ Your poker ban has expired",
                            description=f"You can rejoin the tables in **{guild.name}** again.",
                            color=0x57F287,
                        )
                        await user.send(embed=embed)
                    except Exception:
                        pass
        except Exception as e:
            print(f"[Ban Expiry Task Error] {e}")
            traceback.print_exc()

    @expire_bans.before_loop
    async def before_expire_bans(self):
        await self.bot.wait_until_ready()

    poker = app_commands.Group(name="poker", description="Texas Hold'em poker", guild_ids=[config.GUILD_ID])
    pokerset = app_commands.Group(name="pokerset", description="Configure poker settings", guild_ids=[config.GUILD_ID])
    pokermgr = app_commands.Group(name="pokermgr", description="Poker manager commands", guild_ids=[config.GUILD_ID])
    pokeradmin = app_commands.Group(name="pokeradmin", description="Poker economy and admin commands", guild_ids=[config.GUILD_ID])

    @poker.command(name="ping", description="Check the bot's latency")
    async def ping(self, interaction: discord.Interaction):
        # self.bot.latency is in seconds, multiply by 1000 for ms
        latency_ms = round(self.bot.latency * 1000)
        await interaction.response.send_message(f'Pong! 🏓 Latency: {latency_ms}ms', ephemeral=True)

    # ── Table management ──────────────────────────────────────────────────

    @poker.command(name="open", description="[Manager] Open a poker table in this channel")
    @app_commands.describe(name="Table name")
    async def open_table(self, interaction: discord.Interaction, name: app_commands.Range[str, 1, 25] = "Poker Table"):
        await interaction.response.defer(ephemeral=True)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True);
            return
        key = (interaction.guild_id, interaction.channel_id)
        if key in tables:
            await interaction.followup.send("❌ A table is already running in this channel. Close it first.", ephemeral=True)
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
        else:
            bb = t.game.BIG_BLIND
            active = [p for p in t.game.players if (p.chips + p.pending_rebuy) >= bb]
            pending_with_chips = [p for p in t.game.pending_joins if (p.chips + p.pending_rebuy) >= bb]
            await _roll_chaos_for_hand(interaction.channel, t, settings, len(active) + len(pending_with_chips))

        slog_clear(t)
        success, msg = t.game.start_hand()
        slog(t, msg)
        if not success:
            await interaction.followup.send(msg, ephemeral=True);
            return

        t.sixseven_bait_active = False  # Chaos: "67" bait window closes the moment the next hand actually starts
        await _announce_bounty_if_active(interaction.channel, t)

        if t.pending_gamble_bets:
            await _resolve_gamble_the_gamble(interaction.channel, t, t.pending_gamble_bets)
            t.pending_gamble_bets = {}

        t.msg_count = 0
        await db.log_dealer_event(t.id, t.name, t.manager_id, t.manager_name, 'start')
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
    @app_commands.describe(user="Player to kick", reason="Reason for the kick",
                           duration="Optional rejoin cooldown, e.g. 10m, 2h, 1d (leave blank for none)")
    async def kick(self, interaction: discord.Interaction, user: discord.Member,
                   reason: str = None, duration: str = None):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        cooldown_seconds = None
        duration_display = None
        if duration and duration.strip():
            try:
                cooldown_seconds, _ = parse_duration(duration)
            except ValueError as e:
                await interaction.followup.send(f"❌ {e}", ephemeral=True)
                return
            if cooldown_seconds is None:
                await interaction.followup.send(
                    "❌ A kick can't be permanent — give a duration like `10m`, `2h`, `1d`, or leave it blank.",
                    ephemeral=True)
                return
            duration_display = discord_timestamp(int(time.time() + cooldown_seconds))

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

        if cooldown_seconds is not None:
            t.rejoin_cooldowns[user.id] = time.time() + cooldown_seconds
        await send_mod_dm(user, "kick", reason, duration_display, interaction.user, interaction.guild.name,
                          duration_field="Expires")

        # Kick from waiting list
        if pj:
            t.game.pending_joins.remove(pj)
            total_to_return = pj.chips + pj.pending_rebuy
            if total_to_return > 0:
                await db.return_chips(user.id, total_to_return)
            await db.clear_chips_in_play(user.id)
            apply_global_lock(user.id, config.REGULAR_REJOIN_COOLDOWN, payable=True)
            await interaction.followup.send(format_mod_message("🦵", "kicked", user, t.name, duration_display, reason, duration_field="Expires"))
            return

        # Kick from table
        if t.game.street == Street.WAITING and not t.game._hand_result:
            t.game.players.remove(p)
            total_to_return = p.chips + p.pending_rebuy
            if total_to_return > 0:
                await db.return_chips(user.id, total_to_return)
            await db.clear_chips_in_play(user.id)
            apply_global_lock(user.id, config.REGULAR_REJOIN_COOLDOWN, payable=True)
            await interaction.followup.send(format_mod_message("🦵", "kicked", user, t.name, duration_display, reason, duration_field="Expires"))
            await refresh(interaction.channel, t)
            return

        if user.id not in t.game.kicked_users:
            t.game.kicked_users.append(user.id)
        if user.id not in t.game.pending_leaves:
            t.game.pending_leaves.append(user.id)
        # Applied now rather than routed through leave_cooldown_pending —
        # this is unambiguously a kick regardless of which pending-leave
        # bucket it'd otherwise land in, so there's no need to wait for
        # _process_result's generic drain to decide the duration.
        apply_global_lock(user.id, config.REGULAR_REJOIN_COOLDOWN, payable=True)

        if not p.folded:
            ok, fold_msg = t.game.force_fold(user.id)
            if ok:
                parts = fold_msg.split("\n")
                if any(m in fold_msg for m in ["🌊", "↩️", "🏁", "Showdown"]):
                    slog_clear(t)
                for part in parts:
                    if part.strip():
                        slog(t, part)

        await interaction.followup.send(format_mod_message("🦵", "kicked", user, t.name, duration_display, reason, duration_field="Expires"))

        await _handle_post_action(interaction.guild, interaction.channel, t)

    @pokermgr.command(name="ban", description="[Manager] Ban a user — omit table name to ban server-wide")
    @app_commands.describe(user="Player to ban", table_name="Table name to ban from (leave blank for server-wide)",
                           reason="Reason for the ban", duration="Ban length, e.g. 7d, 12h, 2w (leave blank for permanent)")
    async def ban(self, interaction: discord.Interaction, user: discord.Member, table_name: str = None,
                 reason: str = None, duration: str = None):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        try:
            ban_seconds, _ = parse_duration(duration)
        except ValueError as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)
            return
        duration_display = discord_timestamp(int(time.time() + ban_seconds)) if ban_seconds is not None else "Never"

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
                                    interaction.user.id, table_name, reason,
                                    expires_at_str(ban_seconds))
        scope = f"table **{table_name}**" if table_name else "**all tables** (server-wide)"

        if added:
            await send_mod_dm(user, "ban", reason, duration_display, interaction.user, interaction.guild.name,
                              duration_field="Expires")

        kicked_from = ""
        kicked_from_table = None

        # Every live, non-tournament table in this guild — a server-wide ban
        # (table_name=None) is applied to ALL of them, not just one; a
        # named ban only to the one whose name matches. Multiple regular
        # tables can be running at once in different channels, so this can
        # no longer just grab "the" active table.
        guild_tables = [(cid, tbl) for (gid, cid), tbl in tables.items()
                         if gid == interaction.guild_id and not getattr(tbl, 'is_tournament', False)]
        if table_name:
            guild_tables = [(cid, tbl) for cid, tbl in guild_tables if tbl.name.lower() == table_name.lower()]

        for cid, t in guild_tables:
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
                kicked_from.append(t.name)
                kicked_from_table = t.name
                ch = interaction.guild.get_channel(cid)
                if ch:
                    if t.game.street == Street.WAITING and not t.game._hand_result:
                        await ch.send(f"🔨 **{user.display_name}** has been banned and removed from the table.")
                        await refresh(ch, t)
                else:
                    await ch.send(
                        f"🔨 **{user.display_name}** has been banned and will be removed after this hand.")
                    await _handle_post_action(interaction.guild, ch, t)

        kicked_note = f" Kicked from: {', '.join(kicked_from)}." if kicked_from else ""

        if not added:
            await interaction.followup.send(f"ℹ️ **{user.display_name}** was already banned from {scope}.{kicked_note}",
                                            ephemeral=True)
        else:
            location = table_name if table_name else "all tables"
            extra_line = f"Kicked from: {kicked_from_table}" if kicked_from_table else None
            await interaction.followup.send(
                format_mod_message("🔨", "banned", user, location, duration_display, reason, extra_line,
                                   duration_field="Expires"),
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

        # Same multi-table handling as /pokermgr ban — see its comment.
        guild_tables = [(cid, tbl) for (gid, cid), tbl in tables.items()
                         if gid == interaction.guild_id and not getattr(tbl, 'is_tournament', False)]
        if table_name:
            guild_tables = [(cid, tbl) for cid, tbl in guild_tables if tbl.name.lower() == table_name.lower()]

        for cid, t in guild_tables:
            if user.id in t.game.banned_users:
                t.game.banned_users.remove(user.id)

        # FIXED: Send publicly
        if removed:
            await interaction.followup.send(f"✅ **{user.display_name}** unbanned from {scope}.", ephemeral=False)
        else:
            await interaction.followup.send(f"ℹ️ **{user.display_name}** had no bans for {scope}.", ephemeral=False)

    @pokermgr.command(name="unlock", description="[Manager] Clear a player's active rejoin cooldown/ban early")
    @app_commands.describe(user="Player to unlock")
    async def unlock(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        # This only ever touches global_rejoin_locks — the temporary,
        # cross-table cooldown/ban a kick or a random event applies (see
        # apply_global_lock). It does NOT touch a persistent /pokermgr ban
        # (that's db.is_banned/poker_bans, its own separate system with
        # its own /pokermgr unban) — a manager reaching for "unlock" on
        # someone with a real ban stays blocked, on purpose.
        lock = get_global_lock(user.id)
        if not lock:
            await interaction.followup.send(f"ℹ️ **{user.display_name}** has no active cooldown or ban.", ephemeral=True)
            return

        kind = "kick cooldown" if lock["payable"] else "ban"
        clear_global_lock(user.id)
        await interaction.followup.send(f"🔓 Cleared **{user.display_name}**'s {kind} early — they can rejoin any table now.")

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

        await _handle_post_action(interaction.guild, interaction.channel, t)

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
            await interaction.followup.send(
                "No stats yet!",
                ephemeral=True
            )
            return

        top_ids = {r["user_id"] for r in rows}

        caller_rank = None
        if caller_row:
            caller_rank = await db.get_player_rank(caller_id)

        # Generate PNG
        image_data = generate_leaderboard_image(
            rows=rows,
            caller_id=caller_id,
            caller_row=caller_row,
            caller_rank=caller_rank,
        )

        file = discord.File(
            image_data,
            filename="poker_leaderboard.png",
        )

        # Components V2
        container = discord.ui.Container(
            discord.ui.MediaGallery(
                discord.MediaGalleryItem(
                    "attachment://poker_leaderboard.png"
                )
            ),
            accent_color=0xF1C40F,
        )

        view = discord.ui.LayoutView()
        view.add_item(container)

        await interaction.followup.send(
            file=file,
            view=view,
        )

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
        await db.log_dealer_event(t.id, t.name, user.id, user.name, 'dealer_change')

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
            expiry = f" — expires {b['expires_at']}" if b.get('expires_at') else " — permanent"
            reason = f" — *{b['reason']}*" if b.get('reason') else ""
            lines.append(f"• **{b['username']}** (`{b['user_id']}`) — {scope} *(on {date_str})*{expiry}{reason}")

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

    class _TableSizeModal(discord.ui.Modal, title="Set Table Size"):
        size = discord.ui.TextInput(
            label="Table size (Small / Medium / High)",
            placeholder="Small, Medium, or High",
            max_length=16,
        )

        def __init__(self, cog: "PokerCog"):
            super().__init__()
            self.cog = cog

        async def on_submit(self, interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=False)
            await self.cog._apply_table_size(interaction, self.size.value)

    async def _apply_table_size(self, interaction: discord.Interaction, size: str):
        # `size` is free text (not a fixed choice list) specifically so the
        # hidden Chaos preset can be reached by typing "soahc" — it never
        # shows up as a suggestion, and now that this whole value comes
        # through a modal rather than a visible slash-command parameter, it
        # never appears in Discord's public "[Manager] used /pokerset table"
        # invocation indicator either (modal submissions aren't part of
        # that — only the bare command name is).
        raw = (size or "").strip().lower()
        is_chaos = raw == chaos.UNLOCK_PHRASE

        if raw == "small":
            sb, bb, min_b, max_b = 5, 10, 50, 1000
            preset_name = "Small Table"
        elif raw == "medium":
            sb, bb, min_b, max_b = 15, 30, 150, 3000
            preset_name = "Medium Table"
        elif raw == "high":
            sb, bb, min_b, max_b = 25, 50, 250, 5000
            preset_name = "High Table"
        elif is_chaos:
            base = chaos.CHAOS_BASE_SETTINGS
            sb, bb, min_b, max_b = base["small_blind"], base["big_blind"], base["min_wallet"], base["max_wallet"]
            preset_name = "🎲 Chaos Table"
        else:
            await interaction.followup.send("❌ Not a valid table size. Choose Small, Medium, or High.")
            return

        await db.set_settings(
            interaction.guild_id,
            small_blind=sb,
            big_blind=bb,
            min_wallet=min_b,
            max_wallet=max_b,
            table_mode="chaos" if is_chaos else "normal",
        )

        # 4. Confirmation
        if is_chaos:
            await interaction.followup.send(
                f"✅ Applied: **🎲 Chaos Table**\n"
                f"Blinds: {sb}/{bb} | Buy-in: {min_b} to {max_b}\n"
            )
        else:
            await interaction.followup.send(
                f"✅Applied: **{preset_name}**\n"
                f"Blinds: {sb}/{bb} | Buy-in: {min_b} to {max_b}"
            )

    @pokerset.command(name="table", description="[Manager] Apply a global Stakes & Buy-in preset")
    async def set_preset(self, interaction: discord.Interaction):
        # 1. Manager check — must happen BEFORE the modal, since a modal has
        # to be the interaction's first response.
        if not await is_manager(interaction):
            await interaction.response.send_message("❌ Poker Managers only.", ephemeral=True)
            return
        await interaction.response.send_modal(self._TableSizeModal(self))

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
        await interaction.response.defer(ephemeral=False)
        if not (
                interaction.user.guild_permissions.administrator
                or interaction.user.id in self.DEV_USER_IDS
                or interaction.guild.get_role(1010238899320270999) in interaction.user.roles
        ):
            await interaction.response.send_message("❌ Server Administrators only.", ephemeral=True);
            return

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
        await interaction.response.defer(ephemeral=False)

        if not (
                interaction.user.guild_permissions.administrator
                or interaction.user.id in self.DEV_USER_IDS
                or interaction.guild.get_role(1010238899320270999) in interaction.user.roles
        ):
            await interaction.followup.send("❌ Server Administrators only.", ephemeral=True)
            return

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
        from . import database as db
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

        # Get live jackpot values
        jp, egirl_cut, rf_cut, sf_cut, quads_cut = (
            await jackpot.get_jackpot_display_cuts()
        )

        # Generate the finished jackpot image off the event loop
        image_data = await asyncio.to_thread(
            generate_jackpot_image,
            jp,
            quads_cut,
            sf_cut,
            rf_cut,
            egirl_cut,
        )

        file = discord.File(image_data, filename="jackpot.png")

        # Components V2 layout
        view = discord.ui.LayoutView()

        container = discord.ui.Container(
            discord.ui.MediaGallery().add_item(
                media="attachment://jackpot.png"
            ),
        )

        view.add_item(container)

        await interaction.followup.send(
            view=view,
            file=file,
        )

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

    @poker.command(name="cosmetics", description="View and equip your unlocked titles and win messages")
    async def titles_cmd(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        cosmetics = await db.get_cosmetics(interaction.user.id)
        embed, view = _build_cosmetics_embed_and_view(interaction.user.id, cosmetics)
        view.message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    @poker.command(name="titles", description="View and equip your unlocked titles and win messages")
    async def titles_alias(self, interaction: discord.Interaction):
        await self.titles_cmd.callback(self, interaction)


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

    @poker.command(name="equipskin", description="Equip one of your unlocked card skins")
    @app_commands.describe(skin_id="Your unlocked card skin — pick from the list")
    @app_commands.autocomplete(skin_id=_autocomplete_skin)
    async def equipskin(self, interaction: discord.Interaction, skin_id: str):
        if skin_id == "none":
            await db.set_active_skin(interaction.user.id, None)
            await interaction.followup.send("✅ Card skin removed.", ephemeral=True)
            return

        if skin_id not in db.SKINS:
            await interaction.followup.send("❌ Unknown card skin. Use `/poker titles` to see your options.",
                                            ephemeral=True)
            return

        ok = await db.set_active_skin(interaction.user.id, skin_id)
        if ok:
            info = db.SKINS[skin_id]
            await interaction.followup.send(f"✅ Card skin set to **{db.SKINS[skin_id]['display']}**!", ephemeral=True)

    @pokeradmin.command(name="grant_cosmetic", description="[Admin] Grant a title, win message, or card skin to any player")
    @app_commands.describe(user="The player to receive the cosmetic", kind="Type of cosmetic",
                           cosmetic_id="Search for the cosmetic")
    @app_commands.choices(kind=[
        app_commands.Choice(name="Title", value="title"),
        app_commands.Choice(name="Win Message", value="winmsg"),
        app_commands.Choice(name="Card Skin", value="skin"),
    ])
    @app_commands.autocomplete(cosmetic_id=_autocomplete_grant_cosmetic)
    async def grant_cosmetic(self, interaction: discord.Interaction, user: discord.Member, kind: str, cosmetic_id: str):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in self.DEV_USER_IDS):
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        k = kind.strip().lower()
        if k not in ("title", "winmsg", "skin"):
            await interaction.followup.send("❌ `kind` must be `title`, `winmsg`, or `skin`.", ephemeral=True);
            return
        catalog = db.catalog_for_kind(k)
        cid = cosmetic_id.strip().lower()
        if cid not in catalog:
            valid = ", ".join(f"`{x}`" for x in catalog)
            await interaction.followup.send(f"❌ Unknown ID `{cid}`.\nValid: {valid}", ephemeral=True); return
        newly = await db.unlock_cosmetic(user.id, k, cid)
        display = catalog[cid]["display"]
        cmd = {"title": "equiptitle", "winmsg": "equipwinmsg", "skin": "equipskin"}[k]
        if newly:
            await interaction.followup.send(
                f"✅ Granted **{display}** to {user.mention}.\nThey can equip it with `/poker {cmd}`", ephemeral=True)
        else:
            await interaction.followup.send(f"ℹ️ {user.mention} already owns **{display}**.", ephemeral=True)

    @pokeradmin.command(name="makecustom", description="[Admin] Create a custom title or win message")
    @app_commands.describe(
        kind="'title', 'winmsg', or 'border'",
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

    @poker.command(name="drawcards", description="Draw cards from a 52 card deck")
    @app_commands.describe(number="Number of cards to draw",
                           infinite="Use a fresh deck for every card (Default: False)",
                           shiny_chance="Chance for Ace of Spades to be shiny (0 to 1, Default: 0)", name="X drew",
                           sort="Sort cards by rank or suit (Default: rank)")
    @app_commands.choices(
        sort=[app_commands.Choice(name="Rank", value="rank"), app_commands.Choice(name="Suit", value="suit")])
    async def draw_cards(self, interaction: discord.Interaction, number: app_commands.Range[int, 1, 10],
                         infinite: bool = False, shiny_chance: app_commands.Range[float, 0.0, 1.0] = 0.0,
                         name: str = "", sort: str = "rank"):
        await interaction.response.defer(ephemeral=False)

        from treys import Deck, Card
        import random

        if infinite:
            cards = []
            for _ in range(number):
                deck = Deck()
                cards.extend(deck.draw(1))
        else:
            deck = Deck()
            cards = deck.draw(number)

        suit_order = {"s": 0, "d": 1, "c": 2, "h": 3}
        rank_order = {r: i for i, r in enumerate("23456789TJQKA")}

        if sort == "rank":
            cards.sort(key=lambda card: (rank_order[Card.int_to_str(card)[0]], suit_order[Card.int_to_str(card)[1]]))
        else:
            cards.sort(key=lambda card: (suit_order[Card.int_to_str(card)[1]], rank_order[Card.int_to_str(card)[0]]))

        ace_of_spades = Card.new("As")
        shiny = ace_of_spades in cards and random.random() < shiny_chance

        file = await asyncio.to_thread(card_images.make_strip, cards, 0, True, shiny)

        await interaction.followup.send(
            f"🃏 {name} drew **{number}** card{'s' if number != 1 else ''}"
            f"{' (infinite deck)' if infinite else ''}: "
            f"\n {hand_str(cards)}{' ✨' if shiny else ''}",
            file=file
        )

    @poker.command(name="currencylog", description="View recent chip transactions")
    @app_commands.describe(minimum="Only show transactions with this many chips or more", user="Player to check (Admins/Devs only, leave blank for yourself)")
    async def currencylog(self, interaction: discord.Interaction, user: discord.Member = None, minimum: int = None):
        target = user or interaction.user

        # 🚨 Permission Check
        if target.id != interaction.user.id:
            if not await is_manager(interaction):
                await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
                return

        await interaction.response.defer(ephemeral=True)
        logs = await db.get_currency_logs(target.id)

        # Pass the caller (to verify button clicks) AND the target (for the embed profile)
        view = CurrencyLogView(caller=interaction.user, target=target, logs=logs, minimum=minimum)
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

    # Moved from `poker` to `pokerset` — Discord caps a command group at 25
    # subcommands, and `poker` was at 26. `preferences` is genuinely a
    # settings command ("Configure poker settings" is pokerset's own
    # description), so this is the natural one to relocate rather than
    # spinning up a brand new subgroup. Command becomes /pokerset preferences.
    @pokerset.command(name="preferences", description="Configure your auto-rebuy, auto-showdown, and confirmation settings")
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
            conn = await db._get_db()

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

        from . import database as db
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
            await _handle_post_action(interaction.guild, interaction.channel, t)

    @pokermgr.command(name="dealerhours", description="[Manager] Show minutes hosted per dealer between two UTC dates")
    @app_commands.describe(start="Start date (required)",
                           end="End date (optional, defaults to today)")
    @app_commands.autocomplete(
        start=date_autocomplete,
        end=date_autocomplete,
    )
    async def dealer_hours(self, interaction: discord.Interaction, start: str, end: str = None):
        await interaction.response.defer(ephemeral=False)
        if not await is_manager(interaction):
            await interaction.followup.send("❌ Poker Managers only.", ephemeral=True)
            return

        try:
            start = parse_date(start).strftime(DATE_FORMAT)

            if end:
                end = parse_date(end).strftime(DATE_FORMAT)
            else:
                end = date.today().strftime(DATE_FORMAT)

        except ValueError:
            await interaction.followup.send(
                "❌ Invalid date.",
                ephemeral=True,
            )
            return
        rows = await db.get_dealer_minutes(start, end)

        if not rows:
            label = start if not end else f"{start} → {end}"
            await interaction.followup.send(f"No dealer sessions logged for `{label}`.", ephemeral=True)
            return

        label = start if end == start else f"{start} → {end}"
        lines = [f"**Dealer Hours — {label}**"]
        for r in rows:
            lines.append(f"• **{r['dealer_name']}** — {r['minutes']} min")

        await interaction.followup.send("\n".join(lines))

    @app_commands.command(name="changelog",description="View the poker bot's Git changelog",)
    @app_commands.describe(search="Only show commits matching this term",)
    async def changelog(self,interaction: discord.Interaction,search: str | None = None,):
        await interaction.response.defer(ephemeral=True)

        commits = await asyncio.to_thread(get_git_changelog)

        if not commits:
            await interaction.followup.send("❌ Unable to read the Git changelog.",ephemeral=True,)
            return

        view = ChangelogView(caller=interaction.user,commits=commits,search=search,)

        await interaction.followup.send(view=view,ephemeral=True,)

    # ── User-install top-level aliases ─────────────────────────────────────

    @app_commands.command(name="stats", description="View your poker stats")
    @app_commands.allowed_installs(guilds=False, users=True)
    @app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
    @app_commands.describe(hidden="Hide the stats message from others? (Default: False)")
    async def user_stats(self, interaction: discord.Interaction, hidden: bool = False):
        await self.stats.callback(self, interaction, hidden)

    @app_commands.command(name="leaderboard", description="Top poker players by net chips")
    @app_commands.allowed_installs(guilds=False, users=True)
    @app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
    async def user_leaderboard(self, interaction: discord.Interaction):
        await self.leaderboard.callback(self, interaction)

    @app_commands.command(name="jackpot", description="View the current casino jackpot!")
    @app_commands.allowed_installs(guilds=False, users=True)
    @app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
    async def user_jackpot(self, interaction: discord.Interaction):
        await self.jackpot_cmd.callback(self, interaction)

    @app_commands.command(name="drawcards", description="Draw cards from a 52 card deck.")
    @app_commands.allowed_installs(guilds=True, users=True)
    @app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
    @app_commands.describe(number="Number of cards to draw",
                           infinite="Use a fresh deck for every card (Default: False)",
                           shiny_chance="Chance for Ace of Spades to be shiny (0 to 1, Default: 0)", name="X drew",
                           sort="Sort cards by rank or suit (Default: rank)")
    @app_commands.choices(
        sort=[app_commands.Choice(name="Rank", value="rank"), app_commands.Choice(name="Suit", value="suit")])
    async def user_drawcards(self, interaction: discord.Interaction, number: app_commands.Range[int, 1, 10],
                             infinite: bool = False, shiny_chance: app_commands.Range[float, 0.0, 1.0] = 0.0,
                             name: str = "", sort: str = "rank"):
        await self.draw_cards.callback(self, interaction, number, infinite, shiny_chance, name, sort)

    @app_commands.command(name="myactivity", description="Check your poker activity status")
    @app_commands.allowed_installs(guilds=False, users=True)
    @app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
    async def user_myactivity(self, interaction: discord.Interaction):
        await self.myactivity.callback(self, interaction)


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
    def __init__(self, caller: discord.User | discord.Member, target: discord.User | discord.Member, logs: list[dict], minimum=None):
        super().__init__(timeout=120)
        self.caller = caller   # The person clicking the buttons
        self.target = target   # The person whose logs we are viewing
        self.all_logs = logs
        self.minimum = minimum

        if minimum is not None:
            logs = [
                log for log in logs
                if abs(log["amount"]) >= minimum
            ]

        self.logs = logs
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


class ChangelogView(discord.ui.LayoutView):
    PER_PAGE = 5
    MAX_FILES_SHOWN = 8

    def __init__(
        self,
        caller: discord.User | discord.Member,
        commits: list[dict],
        search: str | None = None,
    ):
        super().__init__(timeout=300)

        self.caller = caller
        self.all_commits = commits
        self.search = search.strip() if search and search.strip() else None
        self.page = 0

        self._build()

    # =========================================================
    # DATA / PAGINATION
    # =========================================================

    def _filtered_commits(self) -> list[dict]:
        if not self.search:
            return self.all_commits

        term = self.search.casefold()

        return [
            commit
            for commit in self.all_commits
            if (
                term in commit["subject"].casefold()
                or any(
                    term in file["path"].casefold()
                    for file in commit["files"]
                )
            )
        ]

    def _page_count(self) -> int:
        count = len(self._filtered_commits())

        return max(
            1,
            math.ceil(count / self.PER_PAGE),
        )

    def _current_commits(self) -> list[dict]:
        commits = self._filtered_commits()

        start = self.page * self.PER_PAGE
        end = start + self.PER_PAGE

        return commits[start:end]

    # =========================================================
    # SANITIZATION
    # =========================================================

    @staticmethod
    def _sanitize_text(
        text: str,
        max_length: int = 500,
    ) -> str:
        """
        Prevent accidental Discord mentions and excessive text.
        """

        text = str(text)
        text = text.replace("@", "@\u200b")
        text = text.replace("`", "'")

        return text[:max_length]

    @staticmethod
    def _sanitize_subject(
        subject: str,
    ) -> str:
        return ChangelogView._sanitize_text(
            subject,
            500,
        )

    @staticmethod
    def _sanitize_path(
        path: str,
    ) -> str:
        """
        Git gives us repository-relative paths.

        Only display the relative path. Never expose the local
        repository directory or machine username.
        """

        path = str(path)

        # Extra safety in case Git ever returns an absolute path.
        path = path.replace("\\", "/")

        if path.startswith("/"):
            path = path.lstrip("/")

        path = path.replace("@", "@\u200b")
        path = path.replace("`", "'")

        return path[:200]

    # =========================================================
    # COMPONENTS V2 RENDERING
    # =========================================================

    def _build(self):
        self.clear_items()

        commits = self._filtered_commits()
        page_count = self._page_count()

        # Keep the page valid.
        if self.page >= page_count:
            self.page = page_count - 1

        if self.page < 0:
            self.page = 0

        page_commits = self._current_commits()

        container = discord.ui.Container(
            accent_colour=discord.Colour(0x36393F)
        )

        # -----------------------------------------------------
        # HEADER
        # -----------------------------------------------------

        title = "# 📝 Poker Bot Changelog"

        if self.search:
            safe_search = self._sanitize_text(
                self.search,
                100,
            )

            title += f"\n-# Search: `{safe_search}`"

        container.add_item(
            discord.ui.TextDisplay(title)
        )

        container.add_item(
            discord.ui.Separator(
                spacing=discord.SeparatorSpacing.small
            )
        )

        # -----------------------------------------------------
        # COMMITS
        # -----------------------------------------------------

        if not page_commits:

            container.add_item(
                discord.ui.TextDisplay(
                    "No commits found matching that search."
                )
            )

        else:

            for commit_index, commit in enumerate(page_commits):

                short_hash = commit["hash"][:7]

                subject = self._sanitize_subject(
                    commit["subject"]
                )

                timestamp = commit["timestamp"]

                files = commit.get("files", [])

                added = commit.get("added", 0)
                deleted = commit.get("deleted", 0)

                file_count = len(files)

                # ---------------------------------------------
                # Commit header
                # ---------------------------------------------

                commit_text = (
                    f"### `{short_hash}` {subject}\n"
                    f"-# <t:{timestamp}:F> · "
                    f"<t:{timestamp}:R>\n"
                    f"📁 **{file_count} "
                    f"file{'s' if file_count != 1 else ''}** · "
                    f"**+{added:,} −{deleted:,}**"
                )

                container.add_item(
                    discord.ui.TextDisplay(
                        commit_text
                    )
                )

                # ---------------------------------------------
                # Changed files
                # ---------------------------------------------

                if files:

                    file_lines = []

                    shown_files = files[
                        :self.MAX_FILES_SHOWN
                    ]

                    for file in shown_files:

                        path = self._sanitize_path(
                            file.get("path", "unknown")
                        )

                        file_added = file.get(
                            "added",
                            0,
                        )

                        file_deleted = file.get(
                            "deleted",
                            0,
                        )

                        file_lines.append(
                            f"`{path}` "
                            f"`+{file_added:,}` "
                            f"`−{file_deleted:,}`"
                        )

                    remaining = (
                        len(files)
                        - len(shown_files)
                    )

                    if remaining > 0:

                        file_lines.append(
                            f"-# + {remaining} more "
                            f"file"
                            f"{'s' if remaining != 1 else ''}"
                        )

                    container.add_item(
                        discord.ui.TextDisplay(
                            "\n".join(file_lines)
                        )
                    )

                # ---------------------------------------------
                # Separator between commits
                # ---------------------------------------------

                if commit_index < len(page_commits) - 1:

                    container.add_item(
                        discord.ui.Separator(
                            spacing=discord.SeparatorSpacing.small
                        )
                    )

        # -----------------------------------------------------
        # FOOTER
        # -----------------------------------------------------

        container.add_item(
            discord.ui.Separator(
                spacing=discord.SeparatorSpacing.small
            )
        )

        footer = (
            f"-# Page {self.page + 1} of {page_count}"
            f" · {len(commits)} commit"
            f"{'s' if len(commits) != 1 else ''}"
        )

        container.add_item(
            discord.ui.TextDisplay(
                footer
            )
        )

        # -----------------------------------------------------
        # PAGINATION
        # -----------------------------------------------------

        row = discord.ui.ActionRow()

        first = discord.ui.Button(
            emoji="⏪",
            style=discord.ButtonStyle.secondary,
            disabled=self.page <= 0,
        )
        first.callback = self._first_page

        previous = discord.ui.Button(
            emoji="◀️",
            style=discord.ButtonStyle.secondary,
            disabled=self.page <= 0,
        )
        previous.callback = self._previous_page

        next_button = discord.ui.Button(
            emoji="▶️",
            style=discord.ButtonStyle.secondary,
            disabled=self.page >= page_count - 1,
        )
        next_button.callback = self._next_page

        last = discord.ui.Button(
            emoji="⏩",
            style=discord.ButtonStyle.secondary,
            disabled=self.page >= page_count - 1,
        )
        last.callback = self._last_page

        row.add_item(first)
        row.add_item(previous)
        row.add_item(next_button)
        row.add_item(last)

        container.add_item(row)

        # Add the complete Components V2 container.
        self.add_item(container)

    # =========================================================
    # INTERACTION
    # =========================================================

    async def _check_caller(
        self,
        interaction: discord.Interaction,
    ) -> bool:

        if interaction.user.id != self.caller.id:

            await interaction.response.send_message(
                "❌ This isn't your changelog menu.",
                ephemeral=True,
            )

            return False

        return True

    async def _refresh(
        self,
        interaction: discord.Interaction,
    ):
        self._build()

        await interaction.response.edit_message(
            view=self
        )

    # =========================================================
    # PAGINATION CALLBACKS
    # =========================================================

    async def _first_page(
        self,
        interaction: discord.Interaction,
    ):
        if not await self._check_caller(interaction):
            return

        self.page = 0

        await self._refresh(interaction)

    async def _previous_page(
        self,
        interaction: discord.Interaction,
    ):
        if not await self._check_caller(interaction):
            return

        self.page = max(
            0,
            self.page - 1,
        )

        await self._refresh(interaction)

    async def _next_page(
        self,
        interaction: discord.Interaction,
    ):
        if not await self._check_caller(interaction):
            return

        self.page = min(
            self._page_count() - 1,
            self.page + 1,
        )

        await self._refresh(interaction)

    async def _last_page(
        self,
        interaction: discord.Interaction,
    ):
        if not await self._check_caller(interaction):
            return

        self.page = self._page_count() - 1

        await self._refresh(interaction)

    async def on_timeout(self):
        self.stop()

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


def get_git_changelog() -> list[dict]:
    """
    Read Git history for the public changelog.

    Exposes only:
      - commit hash
      - commit timestamp
      - commit subject
      - changed file paths
      - additions
      - deletions

    Never retrieves Git author/committer names or emails.
    """

    repo_dir = os.path.dirname(os.path.abspath(__file__))

    # ---------------------------------------------------------
    # Get commits
    # ---------------------------------------------------------

    try:
        result = subprocess.run(
            [
                "git",
                "-C",
                repo_dir,
                "log",
                "--format=%H%x1f%ct%x1f%s%x1e",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
    except (subprocess.SubprocessError, OSError) as e:
        print(f"[changelog] git log failed: {e}")
        return []

    commits = []

    for raw in result.stdout.split("\x1e"):
        raw = raw.strip()

        if not raw:
            continue

        parts = raw.split("\x1f", 2)

        if len(parts) != 3:
            continue

        commit_hash, timestamp_raw, subject = parts

        try:
            timestamp = int(timestamp_raw)
        except ValueError:
            continue

        commits.append(
            {
                "hash": commit_hash,
                "timestamp": timestamp,
                "subject": subject,
                "files": [],
                "added": 0,
                "deleted": 0,
            }
        )

    # ---------------------------------------------------------
    # Get changed files for every commit
    # ---------------------------------------------------------

    for commit in commits:
        try:
            result = subprocess.run(
                [
                    "git",
                    "-C",
                    repo_dir,
                    "show",
                    "--format=",
                    "--numstat",
                    "--root",
                    "--no-renames",
                    commit["hash"],
                    "--",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=True,
            )
        except (subprocess.SubprocessError, OSError) as e:
            print(
                f"[changelog] git show failed for "
                f"{commit['hash'][:7]}: {e}"
            )
            continue

        for line in result.stdout.splitlines():
            line = line.strip()

            if not line:
                continue

            parts = line.split("\t", 2)

            if len(parts) != 3:
                continue

            added_raw, deleted_raw, path = parts

            # Binary files are represented by "-"
            added = int(added_raw) if added_raw.isdigit() else 0
            deleted = int(deleted_raw) if deleted_raw.isdigit() else 0

            path = path.strip()

            if not path:
                continue

            commit["files"].append(
                {
                    "path": path,
                    "added": added,
                    "deleted": deleted,
                }
            )

            commit["added"] += added
            commit["deleted"] += deleted

    print(
        f"[changelog] Loaded {len(commits)} commits "
        f"with file statistics"
    )

    return commits

async def setup(bot):
    if not hasattr(bot, "poker_tables"):
        bot.poker_tables = tables
    if not hasattr(bot, "poker_global_locks"):
        bot.poker_global_locks = global_rejoin_locks
    asyncio.create_task(_migrate_active_tables(bot))
    await bot.add_cog(PokerCog(bot))