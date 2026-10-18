"""
uno_cog.py
Discord-facing session layer for UNO, built on top of the pure uno_engine.

Per current scope: NO per-card button/emoji UI is hooked up yet — every
card is entered through a modal (PlayCardModal). This also means the
"force modal when hand > 25 or a button fails" requirement is already
satisfied by construction right now; when the fancy per-card button hand
(from unodemo.py) gets wired in later, that flow should try the button
path and fall back to send_modal(PlayCardModal(...)) in the except branch,
and should check len(hand) > MAX_BUTTONS_TOTAL up front to skip straight
to the modal.

Features implemented here:
  - Persistent table (like a poker table). /uno open posts a join panel;
    joining = placing a bet, which is moved from the wallet onto the table
    (chips_in_play) and stays there. Host/staff press Start for round 1;
    after that rounds follow each other on their own: a round ends on the
    first player to empty their hand, the win/loss is applied straight to
    the bets on the table (the winner collects from each opponent at most
    their OWN bet, so no side pots), a bet_window-second panel pings
    everyone seated to change their bet or join, then the next round
    deals. Leftover chips carry over as the next bet; under the minimum
    -> removed from the table. Leave queues until the round ends,
    /uno quit leaves instantly and forfeits the bet. Moderator powers
    (kick/end/start) belong to event staff (see is_event_staff) — and the
    host can Start their own round 1.
  - All tunables (min/max bet, turn timer, bet window, resend threshold,
    max players, decks, log channel, manager role) live in the guild's
    settings — /unoset settings.
  - The per-turn table message is Components V2 (TableView(LayoutView)),
    not an embed — deliberately, so @mentions in it (e.g. "it's <@id>'s
    turn") actually trigger a ping. Discord does not send notifications
    for mentions inside embed fields/descriptions, only for mentions in
    real message content, and Components V2 TextDisplay content counts
    as real content. Styled to look like an embed anyway via Container's
    accent_colour + a Section/Thumbnail for the top card image. The
    ephemeral "Table" turn-order popup stays a normal embed (no ping
    concern there since only the clicker sees it) but also uses mentions
    instead of display names for consistency/clickability.
  - Table view buttons: Play / Hand / Draw / Table(turn order) / Callout.
    Draw and Callout double as Accept/Challenge for a pending Wild Draw
    Four decision (see below) when the clicking user is the target.
  - Wild Draw Four follows the real official rule: it can always be
    played, but the targeted player may challenge it instead of just
    accepting the 4-card penalty. If the WD4 player actually had a card
    matching the prior color, the challenge succeeds and THEY draw 4
    instead; if the challenge is wrong, the target draws 6 total. Turn
    order pauses on the target's decision — the AFK timer auto-accepts on
    their behalf if they don't respond in time, rather than misfiring
    against the WD4 player (who already acted).
  - Turn timer: the guild's turn_timer setting per turn; on expiry, auto-draws for
    the AFK player. MAX_MISSED_TURNS (from the engine) consecutive
    timeouts in a row -> that player is removed from the game.
  - Table/panel auto-resends every resend_after channel
    messages, or on manual "Table" button / /uno table command. (Note:
    the "Table" *button* was repurposed into the turn-order popup above —
    manual full-table refresh is the /uno refresh slash command instead.)
  - on_member_remove / on_member_ban: auto-removes that player from any
    table they're seated at (mid-round that forfeits their bet).
  - Callout (UNO challenge) is hardened: only active players can call
    someone out, self-callouts are blocked, and the engine itself already
    prevents repeat-farming the same target (a successful challenge takes
    their hand off exactly 1 card, so a second challenge against them
    fails until they're legitimately back down to 1 card).
  - /uno kick <user> — event staff can remove a player from a table.
"""

import asyncio
import os
import io
import json
import math
import tempfile
import time
import random
import collections
import weakref
import zipfile
import uuid
import discord
import logging
import config
from pathlib import Path
from datetime import datetime, timedelta, timezone as _tz
from discord import app_commands
from discord.ext import commands, tasks
from discord.ui import (
    Container, TextDisplay, Section, Thumbnail, Separator, ActionRow, Button, MediaGallery,
)
from PIL import Image

from . import uno_engine as ue
from . import cards as card_assets
from .hand_render import compose_hand_image, prepare_card_image
from . import database as db
from .unodemo import (
    sort_hand,
    display_label,
    build_rows_with_fallback,
    sanitize_emoji_name,
    FALLBACK_EMOJI_BY_COLOR,
    MAX_BUTTONS_TOTAL,
)

from poker.leaderboard_image import generate_leaderboard_image

log = logging.getLogger("uno")


def _parse_chips(value: str) -> int | None:
    """Parse chip amounts like 500, 2k, 1.5k, 2000."""
    from decimal import Decimal, InvalidOperation
    try:
        v = value.strip().lower().replace(",", "")
        n = Decimal(v[:-1]) * 1000 if v.endswith("k") else Decimal(v)
        if not n.is_finite() or n != n.to_integral_value():
            return None  # e.g. "1.9" — reject instead of silently truncating
        return int(n)
    except (InvalidOperation, ValueError, TypeError, OverflowError):
        return None

# Where in-progress games get checkpointed so a bot crash/restart mid-game
# doesn't wipe out everyone's hands. One JSON file per active channel,
# named by channel id. See UnoGame._save_session / resume_cmd.
SAVE_DIR = Path(__file__).parent / "uno_saves"

STAFF_ROLE_IDS = [1010238899320270999,833456633689669681]

async def is_event_staff(interaction: discord.Interaction) -> bool:
    """Checks if the user is a Dev, Admin, or has a specific staff role."""

    if isinstance(interaction.user, discord.Member):
        # 1. Admin Bypass
        if interaction.user.guild_permissions.administrator:
            return True

        # 2. Check for specific roles
        for role_id in STAFF_ROLE_IDS:
            if interaction.user.get_role(role_id) is not None:
                return True

    # 3. Dev Bypass
    if interaction.user.id in getattr(config, 'DEV_USER_IDS', []):
        return True

    return False


async def is_uno_manager(interaction: discord.Interaction) -> bool:
    """Gates /unomgr (economy) commands — a per-guild configurable role
    (set via /unoset managerrole) or Administrator. Same shape as poker's
    is_manager, kept deliberately separate from is_event_staff: game-
    moderation (kick/cancel/timeout) and economy management (addchips,
    ban, cashouts) are different responsibilities in poker too."""
    settings = await db.get_settings(interaction.guild_id)
    role_id = settings.get("manager_role_id")
    if role_id:
        role = interaction.guild.get_role(int(role_id))
        if role and role in interaction.user.roles:
            return True
    return interaction.user.guild_permissions.administrator

_BG_TASKS: set = set()


def safe_task(coro, name: str | None = None) -> asyncio.Task:
    """
    Wraps a fire-and-forget coroutine so an unexpected exception gets
    logged with a full traceback instead of only reaching asyncio's
    default (easy-to-miss) "Task exception was never retrieved" handler.
    Use this for anything scheduled with create_task() that nobody
    awaits/checks later — right now that's just the per-turn timer loop.
    """
    async def _wrapper():
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Background task %r crashed", name or coro)
    task = asyncio.create_task(_wrapper(), name=name)
    _BG_TASKS.add(task)  # strong ref: the loop only keeps weak ones, so a fire-and-forget task could be GC'd mid-flight
    task.add_done_callback(_BG_TASKS.discard)
    return task


async def report_component_error(interaction: discord.Interaction, error: Exception, item, source: str):
    """
    Shared error handler for every interactive View/LayoutView/Modal below.
    Logs the real exception (through the standard `logging` module, same
    as the rest of the bot — not print()) so it shows up properly in
    console/log files instead of either flooding stdout with discord.py's
    default "Ignoring exception in view" dump or being silently swallowed
    with no trace at all. Always follows up with a quiet ephemeral message
    to the user regardless of what already happened with the interaction.
    """
    log.error("Error in %s (item=%s): %r", source, getattr(item, "custom_id", item), error, exc_info=error)
    try:
        if interaction.response.is_done():
            await interaction.followup.send("⚠️ Something went wrong with that action.", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ Something went wrong with that action.", ephemeral=True)
    except discord.HTTPException:
        pass


# MAX_BUTTONS_TOTAL is imported from unodemo (both are Discord's 5x5 = 25 button ceiling)

COLOR_TO_DISCORD = {
    "Red": discord.Color.red(),
    "Yellow": discord.Color.gold(),
    "Green": discord.Color.green(),
    "Blue": discord.Color.blue(),
}

PLACEMENT_EMOJI = config.PLACEMENT_EMOJI

MIN_PLAYERS_LIMIT, MAX_PLAYERS_LIMIT = 2, 20
MIN_DECKS, MAX_DECKS = 1, 6
MIN_TURN_TIMEOUT, MAX_TURN_TIMEOUT = 30, 600


def _mention(player_id: int | None) -> str:
    return f"<@{player_id}>" if player_id is not None else "Someone"


def _plain_name(session: "GameSession", player_id: int | None) -> str:
    """
    Non-pinging reference to a player — bold display name, not a real
    mention. Used for anything past-tense/non-actionable (who just played,
    who got skipped, who was drawn on) so the table message doesn't ping
    everyone it happens to talk about. Real <@id> mentions are reserved
    for the one or two lines that are an actual call to action (it's your
    turn / you must accept-or-challenge this Wild Draw Four).
    """
    if player_id is None:
        return "Someone"
    engine = session.engine
    name = engine.all_names.get(player_id, str(player_id)) if engine else str(player_id)
    return f"**{name}**"


def describe_last_action(session: "GameSession") -> str:
    """
    Builds the headline for the table message from whatever the last
    action was. Everyone mentioned here is referenced by plain name, not
    a real ping — see _plain_name. The one exception is the "must accept
    or challenge" WD4 note, which IS a real ping since it's an actual
    call to action, not a recap. Never reveals what card someone drew
    (that's hidden info) — only that they drew, and how many for forced
    draws (already public since everyone sees the discard pile).
    """
    action = session.last_action
    if not action:
        engine = session.engine
        total = engine.num_decks * 108 if engine else 0
        return f"Round {session.round_num} started! ({engine.num_decks if engine else '?'} deck{'s' if engine and engine.num_decks != 1 else ''}, {total} cards)"

    t = action["type"]
    actor = _plain_name(session, action.get("actor_id"))

    if t == "play":
        pretty = action["card"].replace("_", " ")
        effects = action.get("effects", {})
        verb = "drew and played" if action.get("drew_first") else "played"
        parts = [f"{actor} {verb} **{pretty}**."]
        if effects.get("reversed"):
            parts.append("<:uno_reversal:1536288323268780123> Direction reversed!")
        if effects.get("wd4_pending"):
            parts.append(f"\n⚠️ {_mention(effects.get('target_id'))} must **Draw** (accept) or **Callout** (challenge) the Wild Draw Four.")
        drew = effects.get("drew")
        if drew:
            parts.append(f"{_plain_name(session, drew['player_id'])} drew {drew['n']} card{'s' if drew['n'] != 1 else ''} and was skipped!")
        elif effects.get("skipped"):
            parts.append(f"{_plain_name(session, effects['skipped'])} was skipped!")
        return " ".join(parts)

    if t == "color":
        effects = action.get("effects", {})
        pretty = effects.get("card", "Wild").replace("_", " ")
        parts = [f"{actor} played **{pretty}** and chose **{action['color']}**."]
        if effects.get("wd4_pending"):
            parts.append(f"⚠️ {_mention(effects.get('target_id'))} must **Draw** (accept) or **Callout** (challenge) the Wild Draw Four.")
        drew = effects.get("drew")
        if drew:
            parts.append(f"{_plain_name(session, drew['player_id'])} drew {drew['n']} cards and was skipped!")
        return " ".join(parts)

    if t == "wd4_accept":
        return f"{actor} accepted the Wild Draw Four and drew 4 cards."

    if t == "wd4_timeout_accept":
        return f"<a:bay_alarm:1536288829248512030> {actor} didn't respond in time — auto-accepted the Wild Draw Four and drew 4 cards."

    if t == "wd4_challenge_win":
        return f"❗ {actor} challenged the Wild Draw Four and **won** — {_plain_name(session, action.get('penalty_id'))} drew 4 cards instead!"

    if t == "wd4_challenge_lose":
        return f"❗ {actor} challenged the Wild Draw Four and **lost** — drew 6 cards!"

    if t == "draw":
        return f"{actor} drew a card."

    if t == "timeout_draw":
        return f"<a:bay_alarm:1536288829248512030> {actor} was AFK and auto-drew a card."

    if t == "timeout_kick":
        return f"<a:bay_alarm:1536288829248512030>👢 {actor} was AFK too long (2 missed turns) and was removed."

    if t == "kick":
        return f"👢 {_plain_name(session, action.get('target_id'))} was removed by a moderator."

    if t == "leave":
        return f"🚪 {_plain_name(session, action.get('target_id'))} left the game."

    if t == "callout":
        caught_names = ", ".join(_plain_name(session, pid) for pid in action.get("caught", []))
        n = action.get("n", 2)
        return f"❗ {actor} called it out — {caught_names} forgot UNO and drew {n} penalty cards each!"

    if t == "callout_miss":
        return f"❗ {actor} called out the table but nobody was vulnerable — drew {action.get('n', 2)} penalty cards for the false callout."

    return "..."


def _build_table_text(session: "GameSession") -> str:
    """
    Builds the full multi-line body text for the per-turn table message.
    This is plain text meant for a Components V2 TextDisplay/Section (not
    an embed) specifically so @mentions inside it actually ping — Discord
    does not send push notifications for mentions inside embed
    fields/descriptions, only for mentions in real message content.
    """
    engine = session.engine
    lines = []

    lines.append(describe_last_action(session))
    lines.append("")

    if engine.players:
        current = engine.current_player()
        if engine.pending_wild:
            lines.append(f"<a:uno_timer:1536289544566087720> Waiting on {_mention(current.player_id)} to choose a color.")
        elif engine.pending_wd4_challenge:
            pass  # already covered by the wd4_pending note in describe_last_action
        else:
            lines.append(f"<:uno_arrow:1536135499566157956> It's {_mention(current.player_id)}'s turn.")
    else:
        lines.append("Game over.")

    lines.append("")
    # Was "Decks: N (X cards) | Remaining: Y | Discarded: Z" — deck count
    # never changes turn to turn (it's a lobby setting), so it now only
    # appears once, in the "Round N started!" headline above. What's left
    # here is just the two numbers that actually change every turn, kept
    # short enough to survive mobile wrapping, and marked as Discord
    # markdown subtext ("-# ") so it visually reads as a footnote rather
    # than a stat line competing with the turn info above it.
    lines.append(f"-# <:uno_back:1536699499295285260> {len(engine.draw_pile)} · <:uno_discard:1536699045970579536> {len(engine.discard_pile)}")

    if engine.finishers:
        placed = ", ".join(
            f"{PLACEMENT_EMOJI.get(i, f'{i+1}.')} {_plain_name(session, pid)}"
            for i, pid in enumerate(engine.finishers)
        )
        lines.append(f"<:uno_trophy:1536135332863676548> Finished: {placed}")

    if engine.bets:
        pot_total = sum(engine.bets.values())
        lines.append(f"Round #{session.round_num} · Pot: **{pot_total}** {config.UNO_CHIP_EMOJI}")

    lines.append("")
    if session.turn_started_at:
        expire_ts = int(session.turn_started_at + session.turn_timeout)
    else:
        expire_ts = int(time.time()) + session.turn_timeout
    lines.append(f"-# <t:{expire_ts}:R> – {session.turn_timeout}s Turn Time")

    return "\n".join(lines)


class Seat:
    """One player's place at the table. `bet` is everything they currently
    have on it (always equal to their chips_in_play row): it's what they
    risk each round, and after a round the win/loss is applied straight to
    it, so whatever is left simply carries over as their next bet.

    in_round      - dealt into the round currently being played
    leave_queued  - clicked Leave mid-round; cashed out when the round ends
    quit          - removed from the round already (quit/kicked/AFK/left the
                    server) and just waiting for the round to settle
    missed_turns  - consecutive AFK turns, carried across rounds so being
                    away for several rounds still gets you removed
    """
    __slots__ = ("name", "bet", "in_round", "leave_queued", "quit", "missed_turns", "exit_reason")

    def __init__(self, name: str, bet: int, in_round: bool = False, leave_queued: bool = False,
                 quit: bool = False, missed_turns: int = 0, exit_reason: str | None = None):
        self.name = name
        self.bet = bet
        self.in_round = in_round
        self.leave_queued = leave_queued
        self.quit = quit
        self.missed_turns = missed_turns
        self.exit_reason = exit_reason

    def to_dict(self):
        return {"name": self.name, "bet": self.bet, "in_round": self.in_round,
                "leave_queued": self.leave_queued, "quit": self.quit, "missed_turns": self.missed_turns}

    @classmethod
    def from_dict(cls, d):
        return cls(d["name"], int(d["bet"]), d.get("in_round", False), d.get("leave_queued", False),
                   d.get("quit", False), d.get("missed_turns", 0))


class GameSession:
    """A persistent table, like a poker table: seats come and go between
    rounds while the table (and each seat's chips on it) carries on.
    `engine` is only ever the round currently being played.

    state: "lobby"   - before round 1, host/staff presses Start
           "window"  - between rounds, bet_window seconds to join / change bet
           "waiting" - fewer than 2 seats; idle until someone joins
           "playing" - a round is in progress"""

    def __init__(self, guild_id: int, channel_id: int, host_id: int, host_name: str, settings: dict | None = None):
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.host_id = host_id
        self.host_name = host_name
        # The dealer is who /uno tip pays — defaults to whoever opened the
        # table, /unomgr setdealer changes it (same as poker).
        self.dealer_id = host_id
        self.dealer_name = host_name
        self.table_id = uuid.uuid4().hex[:8]
        self.round_num = 0
        self.state = "lobby"
        self.closed = False  # set by close_table so stale modals/buttons can tell the table is gone
        # Host does NOT auto-join — a pid only ever appears in `seats` once
        # it has put a bet on the table.
        self.seats: dict[int, Seat] = {}
        self.engine: ue.GameState | None = None
        self.table_message: discord.Message | None = None
        self.lobby_message: discord.Message | None = None  # the join/bet panel
        self.timer_task: asyncio.Task | None = None
        self.window_task: asyncio.Task | None = None
        self.window_generation = 0
        self.window_ends_at = 0.0
        self.msg_count = 0
        self.last_action: dict | None = None  # feeds describe_last_action() for the table message
        self.turn_started_at: float = 0.0  # unix time the current turn's timer started, for the <t:...> display
        self.callout_balance: dict[int, int] = {}  # player_id -> false-callouts remaining (missing = 1, default)
        self._last_turn_player_id: int | None = None  # tracks turn changes so callout_balance refreshes correctly
        # Set while a player holds a drawn, playable card and owes a Play/No decision:
        # {"player_id", "card", "engine"}. Only that card may be played, and the
        # decision resolves exactly once under session.lock.
        self.pending_draw: dict | None = None
        # Set by /uno close mid-round: the table closes (everyone refunded)
        # once the round in progress has settled.
        self.close_queued = False

        # Serializes state-changing interaction handlers (play/draw/pass/
        # wild color/WD4 accept/challenge/timeout/kick/leave/UNO toggle/
        # bet changes/round transitions) so two near-simultaneous actions
        # on the same session can't both read the pre-mutation state and
        # then both mutate it. Not persisted — a resumed session just gets
        # a fresh lock.
        self.lock = asyncio.Lock()
        # Bumped every time a new turn starts (see restart_timer). A timer
        # task captures the generation it was started with and checks it
        # again after waking up from sleep — if the number has moved on,
        # some other action already resolved this turn, so the stale timer
        # does nothing instead of double-acting on a turn that's already over.
        self.turn_generation = 0

        self.apply_settings(settings or db._setting_defaults())

    def apply_settings(self, s: dict):
        """Copies the guild's current settings onto the table. Re-run at
        every window/round start (and by /unoset settings) so changes take
        effect without reopening the table."""
        self.min_bet = int(s["min_bet"])
        self.max_bet = int(s["max_bet"])  # 0 = no limit
        self.turn_timeout = int(s["turn_timeout"])
        self.bet_window = int(s["bet_window"])
        self.resend_after = int(s["resend_after_msgs"])
        self.max_players = int(s["max_players"])
        self.num_decks = int(s["num_decks"])

    @property
    def started(self) -> bool:
        """True while a round is being played (the engine exists)."""
        return self.state == "playing"

    def cancel_timer(self):
        # Guard against self-cancellation: _timer_loop calls
        # announce_and_cleanup() on itself when a round ends on an AFK
        # timeout, which calls this. session.timer_task at that point is
        # STILL the currently-running task (nothing reassigns it before
        # this call), so cancelling it here would throw CancelledError
        # into this same coroutine at its next `await` — aborting
        # announce_and_cleanup before it ever settles the round, with
        # no error logged (safe_task's wrapper deliberately re-raises
        # CancelledError rather than logging it). Cross-task cancels
        # (e.g. from /uno kick or /uno quit, which run on the
        # interaction-handler task, not the timer task) are unaffected
        # and still cancel normally.
        if self.timer_task and not self.timer_task.done() and self.timer_task is not asyncio.current_task():
            self.timer_task.cancel()
        self.timer_task = None

    def cancel_window(self):
        """Cancels the between-rounds countdown / idle-close timer. Same
        self-cancel guard as cancel_timer: the window task calls
        begin_round/close_table, which call this, while it's still the
        current task."""
        self.window_generation += 1
        if self.window_task and not self.window_task.done() and self.window_task is not asyncio.current_task():
            self.window_task.cancel()
        self.window_task = None

    # ---------- persistence ----------
    # The whole table is saved, not just a round in progress — seats'
    # chips stay locked in chips_in_play for as long as the table exists,
    # so a crash between rounds has to be resumable too.
    # table_message/lobby_message (discord.Message objects) and the timer
    # tasks (asyncio.Tasks) aren't JSON-serializable and don't need to be —
    # restore just posts a fresh table/panel and restarts the timers.

    def to_dict(self):
        return {
            "guild_id": self.guild_id,
            "channel_id": self.channel_id,
            "host_id": self.host_id,
            "host_name": self.host_name,
            "dealer_id": self.dealer_id,
            "dealer_name": self.dealer_name,
            "table_id": self.table_id,
            "round_num": self.round_num,
            "state": self.state,
            "seats": {str(pid): seat.to_dict() for pid, seat in self.seats.items()},
            "engine": self.engine.to_dict() if self.engine else None,
            "msg_count": self.msg_count,
            "last_action": self.last_action,
            "turn_started_at": self.turn_started_at,
            "callout_balance": {str(k): v for k, v in self.callout_balance.items()},
            "_last_turn_player_id": self._last_turn_player_id,
            "close_queued": self.close_queued,
        }

    @classmethod
    def from_dict(cls, d, settings: dict | None = None):
        self = cls.__new__(cls)
        self.guild_id = d["guild_id"]
        self.channel_id = d["channel_id"]
        self.host_id = d["host_id"]
        self.host_name = d["host_name"]
        self.dealer_id = d.get("dealer_id", self.host_id)
        self.dealer_name = d.get("dealer_name", self.host_name)
        self.table_id = d.get("table_id") or uuid.uuid4().hex[:8]
        self.engine = ue.GameState.from_dict(d["engine"]) if d.get("engine") else None
        if "seats" in d:
            self.seats = {int(k): Seat.from_dict(v) for k, v in d["seats"].items()}
            self.state = d.get("state", "playing" if self.engine else "window")
            self.round_num = d.get("round_num", 0)
        else:
            # Save written before tables persisted across rounds (one
            # round per table, bets only on the engine): rebuild the seats
            # from the engine's bets so that round can still be finished.
            self.seats = {}
            if self.engine:
                for pid, bet in self.engine.bets.items():
                    self.seats[pid] = Seat(self.engine.all_names.get(pid, str(pid)), bet, in_round=True,
                                           quit=pid in self.engine.left_early)
            self.state = "playing"
            self.round_num = 1
        self.closed = False
        self.table_message = None
        self.lobby_message = None
        self.timer_task = None
        self.window_task = None
        self.window_generation = 0
        self.window_ends_at = 0.0
        self.msg_count = d.get("msg_count", 0)
        self.last_action = d.get("last_action")
        self.turn_started_at = d.get("turn_started_at", 0.0)
        self.callout_balance = {int(k): v for k, v in d.get("callout_balance", {}).items()}
        self._last_turn_player_id = d.get("_last_turn_player_id")
        self.pending_draw = None  # not persisted; resumed turns start without a pending draw decision
        self.close_queued = bool(d.get("close_queued", False))
        # Fresh lock/generation counter for the resumed session — see the
        # comments on these fields in __init__.
        self.lock = asyncio.Lock()
        self.turn_generation = 0
        self.apply_settings(settings or db._setting_defaults())
        return self


class UnoConfirmResetView1(discord.ui.View):
    def __init__(self, admin_id: int):
        super().__init__(timeout=30)
        self.admin_id = admin_id

    @discord.ui.button(label="Yes, I'm sure", style=discord.ButtonStyle.red)
    async def step1(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.admin_id:
            await interaction.response.send_message("❌ Not your button.", ephemeral=True)
            return
        view = UnoConfirmResetView2(self.admin_id)
        await interaction.response.edit_message(
            content="⚠️ **Final confirmation.** This CANNOT be undone.", view=view)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Cancelled.", view=None)


class UnoConfirmResetView2(discord.ui.View):
    def __init__(self, admin_id: int):
        super().__init__(timeout=30)
        self.admin_id = admin_id

    @discord.ui.button(label="WIPE EVERYTHING", style=discord.ButtonStyle.red)
    async def step2(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.admin_id:
            await interaction.response.send_message("❌ Not your button.", ephemeral=True)
            return
        await interaction.response.defer()
        await db.reset_database(interaction.user.id, interaction.user.display_name)
        await interaction.edit_original_response(
            content=f"✅ UNO database wiped by **{interaction.user.display_name}**.", view=None)


class UnoRecoverChipsView(discord.ui.View):
    def __init__(self, admin_id: int):
        super().__init__(timeout=60)
        self.admin_id = admin_id

    @discord.ui.button(label="Refund & Clear", style=discord.ButtonStyle.red)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.admin_id:
            await interaction.response.send_message("❌ Not your button.", ephemeral=True)
            return
        await interaction.response.defer()
        recovered = await db.recover_chips_in_play()
        if not recovered:
            await interaction.edit_original_response(
                content="✅ Nothing left to recover — someone beat you to it, or it resolved on its own.",
                view=None)
            return
        lines = "\n".join(f"• **{r['username']}**: +{r['amount']}{config.UNO_CHIP_EMOJI}" for r in recovered[:20])
        await interaction.edit_original_response(
            content=f"✅ Refunded and cleared **{len(recovered)}** player(s):\n{lines}", view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.admin_id:
            await interaction.response.send_message("❌ Not your button.", ephemeral=True)
            return
        await interaction.response.edit_message(content="Cancelled.", view=None)


class RawSQLPaginationView(discord.ui.View):
    def __init__(self, columns: list, rows: list, title: str, items_per_page: int = 15, max_pages_limit: int = 20):
        super().__init__(timeout=300)
        self.columns = columns
        self.rows = rows
        self.title = title
        self.items_per_page = items_per_page
        self.current_page = 0

        calculated_pages = math.ceil(len(rows) / items_per_page) if rows else 1
        self.max_pages = min(calculated_pages, max_pages_limit)
        self.update_buttons()

    def update_buttons(self):
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
            row_str = " | ".join(str(r[col])[:40] for col in self.columns)
            lines.append(row_str)

        description = "\n" + "\n".join(lines) + "\n"
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


class UnoCurrencyLogView(discord.ui.View):
    def __init__(self, caller: discord.User | discord.Member, target: discord.User | discord.Member,
                 logs: list[dict], minimum=None):
        super().__init__(timeout=120)
        self.caller = caller
        self.target = target
        self.all_logs = logs
        self.minimum = minimum

        if minimum is not None:
            logs = [log for log in logs if abs(log["amount"]) >= minimum]
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
        embed = discord.Embed(title="UNO Currency Log", color=0xF1C40F)
        embed.set_author(name=self.target.display_name, icon_url=self.target.display_avatar.url)

        if not self.logs:
            embed.description = "No transactions found for this filter."
            return embed

        start = self.page * self.per_page
        page_logs = self.logs[start:start + self.per_page]

        desc_lines = []
        for entry in page_logs:
            dt = datetime.fromisoformat(entry["ts"]).replace(tzinfo=_tz.utc)
            unix_ts = int(dt.timestamp())
            sign = "+" if entry["amount"] > 0 else ""
            desc_lines.append(f"**{entry['description']}**")
            desc_lines.append(f"└ <t:{unix_ts}:R>")
            desc_lines.append(f"└ {sign}{entry['amount']:,} {config.UNO_CHIP_EMOJI}")
            desc_lines.append("\u200b")

        embed.description = "\n".join(desc_lines)
        max_pages = max(1, math.ceil(len(self.logs) / self.per_page))
        embed.set_footer(text=f"Page {self.page + 1} of {max_pages}  •  Filter: {self.filter}")
        return embed

    @discord.ui.select(
        placeholder="Filter by type...",
        options=[
            discord.SelectOption(label="All", value="All", emoji="📋"),
            discord.SelectOption(label="Rounds", value="Round", emoji="🃏"),
            discord.SelectOption(label="Cash Ins", value="Cash In", emoji="📥"),
            discord.SelectOption(label="Cash Outs", value="Cash Out", emoji="📤"),
            discord.SelectOption(label="Wipes", value="Wipe", emoji="🧹"),
        ],
        row=0
    )
    async def filter_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        if interaction.user.id != self.caller.id:
            await interaction.response.send_message("❌ This is not your menu.", ephemeral=True)
            return
        self.filter = select.values[0]
        self.logs = self.all_logs if self.filter == "All" else [
            log for log in self.all_logs if log["event_type"] == self.filter
        ]
        self.page = 0
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(emoji="⏪", style=discord.ButtonStyle.blurple, row=1)
    async def btn_first(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id:
            return
        self.page = 0
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.blurple, row=1)
    async def btn_prev(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id:
            return
        self.page = max(0, self.page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.blurple, row=1)
    async def btn_next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id:
            return
        max_pages = max(1, math.ceil(len(self.logs) / self.per_page))
        self.page = min(max_pages - 1, self.page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(emoji="⏩", style=discord.ButtonStyle.blurple, row=1)
    async def btn_last(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id:
            return
        max_pages = max(1, math.ceil(len(self.logs) / self.per_page))
        self.page = max_pages - 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


def _seat_lines(session: "GameSession") -> str:
    lines = []
    for pid, seat in session.seats.items():
        flags = []
        if session.state == "playing" and not seat.in_round:
            flags.append("⏳")
        if seat.leave_queued:
            flags.append("leaving")
        if session.state in ("window", "waiting") and seat.bet < session.min_bet:
            flags.append("⚠️")
        extra = f" _({', '.join(flags)})_" if flags else ""
        lines.append(f"<@{pid}> — **{seat.bet}** {config.UNO_CHIP_EMOJI}{extra}")
    return "\n".join(lines) or "_nobody yet_"


def build_panel_embed(session: "GameSession") -> discord.Embed:
    """The join / change-bet panel — the lobby before round 1, the
    countdown between rounds, and the 'waiting for players' state."""
    n = session.round_num
    if session.state == "lobby":
        title = "<:uno:1536135137232953451> UNO — Round 1"
        desc = "Place a bet to take a seat. The host or staff presses **Start** once everyone's in."
    elif session.state == "window":
        title = f"<:uno:1536135137232953451> UNO — Round {n + 1} next"
        desc = (f"Round {n + 1} starts <t:{int(session.window_ends_at)}:R>. Join with a bet, or change yours — "
                f"whatever you have left on the table carries over as your next bet "
                f"(minimum {session.min_bet}, or you'll be removed).")
    elif session.state == "waiting":
        title = "<:uno:1536135137232953451> UNO — waiting for players"
        desc = (f"The next round starts "
                f"{session.bet_window}s after the second seat fills.")
    else:
        title = f"<:uno:1536135137232953451> UNO — Round {n} in progress"
        desc = "Place a bet to take a seat — you'll be dealt in next round."

    e = discord.Embed(title=title, description=desc, color=discord.Color.blurple())
    e.add_field(name="<:uno_host:1536135395933159556> Dealer", value=f"<@{session.dealer_id}>", inline=True)
    e.add_field(name="👥 Players", value=f"{len(session.seats)}/{session.max_players}", inline=True)
    e.add_field(name="<:uno_deck:1536135257789829120> Decks", value=str(session.num_decks), inline=True)
    limits = f"{session.min_bet}" + (f" – {session.max_bet}" if session.max_bet else "+")
    e.add_field(name=f"Bet — {limits}", value=_seat_lines(session), inline=False)
    e.set_footer(text=f"Table {session.table_id}")
    e.set_thumbnail(url='https://media.discordapp.net/attachments/1525465986541555813/1536293002975117362/wdmbzh1.jpg?ex=6a7adfda&is=6a798e5a&hm=4bbf5ffcf806948fecbd377781edfc95f3c5c06a78cd859895930d4f7057226b&=&format=webp')
    return e


def build_settings_embed(s: dict) -> discord.Embed:
    """Current UNO settings for a guild — shown by /unoset settings."""
    e = discord.Embed(title="⚙️ UNO Settings", color=discord.Color.blurple())
    e.add_field(name="Min bet", value=f"{s['min_bet']} {config.UNO_CHIP_EMOJI}", inline=True)
    e.add_field(name="Max bet", value=(f"{s['max_bet']} {config.UNO_CHIP_EMOJI}" if s["max_bet"] else "No limit"), inline=True)
    e.add_field(name="Turn timer", value=f"{s['turn_timeout']}s", inline=True)
    e.add_field(name="Bet window", value=f"{s['bet_window']}s", inline=True)
    e.add_field(name="Resend after", value=f"{s['resend_after_msgs']} messages", inline=True)
    e.add_field(name="Max players", value=str(s["max_players"]), inline=True)
    e.add_field(name="Decks", value=str(s["num_decks"]), inline=True)
    e.add_field(name="Log channel", value=(f"<#{s['log_channel_id']}>" if s.get("log_channel_id") else "Not set"), inline=True)
    e.add_field(name="Manager role", value=(f"<@&{s['manager_role_id']}>" if s.get("manager_role_id") else "Not set"), inline=True)
    return e


class BetModal(discord.ui.Modal):
    """Join (place a first bet) or change the bet already on the table.
    Either way the number entered is the TOTAL on the table afterward —
    raising it pulls the difference from the wallet, lowering it returns
    the difference, both atomically (db.table_set_amount)."""

    def __init__(self, cog: "UnoGame", session: "GameSession", balance: int, mode: str = "join",
                 current_bet: int = 0):
        super().__init__(title="UNO — Place Your Bet" if mode == "join"
        else f"UNO — Change Bet (current {current_bet:,})")
        self.cog = cog
        self.session = session
        self.balance = balance
        self.mode = mode
        self.current_bet = current_bet
        # A change re-sets the TOTAL bet, so what's available is the wallet PLUS what's already on the table.
        available = balance + current_bet
        if mode == "join":
            label = "How many chips to bet?"
            placeholder = f"Total available chips: {available:,} (min {session.min_bet})"
        else:
            label = "New total bet"
            placeholder = f"Total available chips: {available:,}"
        self.bet_input = discord.ui.TextInput(label=label, placeholder=placeholder[:100], min_length=1, max_length=8)
        self.add_item(self.bet_input)

    async def on_submit(self, interaction: discord.Interaction):
        s = self.session
        uid = interaction.user.id
        async with s.lock:
            def deny(msg):
                return interaction.response.send_message(msg, ephemeral=True)

            if s.closed:
                await deny("⚠️ This table has closed.")
                return
            seat = s.seats.get(uid)
            if self.mode == "join":
                if seat:
                    await deny("You're already seated — use **Change bet** instead.")
                    return
                if await db.is_banned(interaction.guild_id, uid):
                    await deny("🚫 You're banned from UNO in this server.")
                    return
                if len(s.seats) >= s.max_players:
                    await deny("The table is full.")
                    return
            else:
                if not seat:
                    await deny("You're not seated — hit **Join** first.")
                    return
                if s.state == "playing" and seat.in_round:
                    await deny("Your bet is locked in until this round ends.")
                    return

            bet = _parse_chips(self.bet_input.value)
            if bet is None:
                await deny("⚠️ Enter a valid amount (e.g. 500, 2k).")
                return
            if bet < s.min_bet:
                await deny(f"⚠️ Minimum bet is **{s.min_bet}** {config.UNO_CHIP_EMOJI}.")
                return
            if s.max_bet and bet > s.max_bet:
                await deny(f"⚠️ Maximum bet is **{s.max_bet}** {config.UNO_CHIP_EMOJI}.")
                return

            await db.upsert_wallet_name(uid, interaction.user.name)
            ok, old = await db.table_set_amount(uid, interaction.user.display_name, bet)
            if not ok:
                bal = await db.get_balance(uid)
                await deny(f"⚠️ Not enough chips — you have **{bal + old:,}** {config.UNO_CHIP_EMOJI} available "
                           f"in total (wallet **{bal:,}** + **{old:,}** already bet).")
                return

            if seat:
                seat.bet = bet
                seat.name = interaction.user.display_name
                await self.cog._log_seat(s, uid, seat.name, "change_bet", bet, f"from {old}")
            else:
                s.seats[uid] = Seat(interaction.user.display_name, bet)
                await self.cog._log_seat(s, uid, interaction.user.display_name, "join", bet)

            await interaction.response.edit_message(embed=build_panel_embed(s))
            clicked = getattr(interaction, "message", None)
            if s.lobby_message and clicked is not None and s.lobby_message.id != clicked.id:
                await self.cog.refresh_panel(s, create=False)  # they used an older panel — keep the live one current too
            # Second seat just filled an idle table -> start counting down to a round
            if s.state == "waiting" and len(s.seats) >= 2:
                await self.cog.open_window(s, interaction.channel)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await report_component_error(interaction, error, None, "BetModal")


class TablePanelView(discord.ui.View):
    def __init__(self, cog: "UnoGame", session: "GameSession"):
        super().__init__(timeout=1800)
        self.cog = cog
        self.session = session
        if session.state != "lobby":
            # Start / Cancel only exist before round 1 — after that rounds
            # start themselves and staff use /uno close.
            self.remove_item(self.start)
            self.remove_item(self.cancel)
        cog._track_view(self)

    async def on_timeout(self):
        # Only the pre-round-1 lobby needs cleaning up when its panel times
        # out — a running table has its own timers and replaces its panel
        # every round.
        s = self.session
        if s.state == "lobby" and not s.closed:
            async with s.lock:
                if s.state == "lobby" and not s.closed:
                    await self.cog.close_table(s, "Lobby expired — all bets were refunded.")
        self.stop()

    @discord.ui.button(label="Join", style=discord.ButtonStyle.success, row=0)
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        s = self.session
        if s.closed:
            await interaction.response.send_message("This table has closed.", ephemeral=True)
            return
        if interaction.user.id in s.seats:
            await interaction.response.send_message("You're already seated — use **Change bet**.", ephemeral=True)
            return
        if len(s.seats) >= s.max_players:
            await interaction.response.send_message("The table is full.", ephemeral=True)
            return
        balance = await db.get_balance(interaction.user.id)
        await interaction.response.send_modal(BetModal(self.cog, s, balance, "join"))

    @discord.ui.button(label="Change bet", style=discord.ButtonStyle.primary, row=0)
    async def change_bet(self, interaction: discord.Interaction, button: discord.ui.Button):
        s = self.session
        seat = s.seats.get(interaction.user.id)
        if s.closed:
            await interaction.response.send_message("This table has closed.", ephemeral=True)
            return
        if not seat:
            await interaction.response.send_message("You're not seated — hit **Join** first.", ephemeral=True)
            return
        if s.state == "playing" and seat.in_round:
            await interaction.response.send_message("Your bet is locked in until this round ends.", ephemeral=True)
            return
        balance = await db.get_balance(interaction.user.id)
        await interaction.response.send_modal(BetModal(self.cog, s, balance, "change", seat.bet))

    @discord.ui.button(label="Leave", style=discord.ButtonStyle.danger, row=0)
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_leave(interaction)

    @discord.ui.button(label="Rules", style=discord.ButtonStyle.secondary, row=0)
    async def rules(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.rules_cmd.callback(self.cog, interaction)

    @discord.ui.button(label="Start", style=discord.ButtonStyle.primary, row=1)
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        s = self.session
        if not (interaction.user.id == s.host_id or await is_event_staff(interaction)):
            await interaction.response.send_message("Only the host or event staff can start the game.", ephemeral=True)
            return
        async with s.lock:
            if s.closed or s.state != "lobby":
                await interaction.response.send_message("This game has already started.", ephemeral=True)
                return
            if len(s.seats) < 2:
                await interaction.response.send_message("Need at least 2 players to start.", ephemeral=True)
                return
            await interaction.response.defer()
            await self.cog.begin_round(s, interaction.channel)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, row=1)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await is_event_staff(interaction):
            await interaction.response.send_message("Only event staff can cancel the lobby.", ephemeral=True)
            return
        s = self.session
        async with s.lock:
            if s.closed or s.state != "lobby":
                await interaction.response.send_message("This game has already started.", ephemeral=True)
                return
            await interaction.response.defer()
            await self.cog.close_table(s, "Lobby cancelled — all bets were refunded.")

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        await report_component_error(interaction, error, item, "TablePanelView")


# ---------------------------------------------------------------------------
# Modals (the current, base card-entry method)
# ---------------------------------------------------------------------------

class DrawFollowupView(discord.ui.View):
    """
    Shown after drawing a card that CAN legally be played, instead of the
    draw always ending the turn outright. "Play this Card" plays it
    immediately (turn continues through the normal play flow, including
    any wild color follow-up); "No" keeps it and ends the turn, same as
    a timeout (see _keep_and_pass) — keeping an already-drawn card is a
    normal, penalty-free outcome either way, never treated as an AFK
    strike and never counted toward missed_turns.
    """

    def __init__(self, cog: "UnoGame", session: GameSession, card: str, player_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.session = session
        self.card = card
        self.player_id = player_id
        cog._track_view(self)

    async def _keep_and_pass(self, player_id: int):
        """Ends the turn, keeping the already-drawn card. Shared by the
        'No' button and on_timeout so the two paths can't diverge later.
        Deliberately does NOT call engine.record_timeout() or touch
        missed_turns — this is a normal turn-ending action, not an AFK
        strike, even when reached via the timeout path."""
        async with self.session.lock:
            engine = self.session.engine
            # Resolve the pending decision exactly once: if it was already
            # settled (card played / declined / superseded) this is a no-op.
            if not self.cog._pending_draw_for(self.session, player_id):
                return
            self.session.pending_draw = None
            if not engine or engine.finished or engine.current_player().player_id != player_id:
                return
            engine.pass_turn(player_id)
            self.session.last_action = {"type": "draw", "actor_id": player_id}
            self.cog.restart_timer(self.session)
            await self.cog.refresh_table(self.session)

    @discord.ui.button(label="Play this Card", style=discord.ButtonStyle.success)
    async def play_it(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        await self.cog.handle_play_card(interaction, self.session, self.card, drew_first=True)

    @discord.ui.button(label="No", style=discord.ButtonStyle.secondary)
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        engine = self.session.engine
        if not engine or engine.finished or engine.current_player().player_id != interaction.user.id:
            await interaction.response.send_message("⚠️ Too late for that.", ephemeral=True)
            return
        self.stop()
        await interaction.response.edit_message(content=f"You drew **{self.card}** and kept it.", view=None)
        await self._keep_and_pass(interaction.user.id)

    async def on_timeout(self):
        await self._keep_and_pass(self.player_id)

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        await report_component_error(interaction, error, item, "DrawFollowupView")


class ColorButtonsView(discord.ui.View):
    """
    Ephemeral 4-button color picker shown after any Wild is played —
    replaces the old text-entry modal since 4 fixed options fit buttons
    perfectly and there's no parsing to get wrong. If nobody picks within
    the timeout, on_timeout() randomly picks a color via the same
    engine.choose_color() path as a manual pick, so all the Wild
    resolution state stays centralized in the engine rather than being
    special-cased here.
    """

    def __init__(self, cog: "UnoGame", session: GameSession, interaction: discord.Interaction | None = None):
        super().__init__(timeout=60)
        self.cog = cog
        self.session = session
        # Stashed so on_timeout can edit the original ephemeral response —
        # ephemeral messages don't give us a discord.Message to edit later,
        # but the interaction token stays valid long enough for this.
        self.interaction = interaction
        cog._track_view(self)

    async def _choose(self, interaction: discord.Interaction, color: str):
        async with self.session.lock:
            engine = self.session.engine
            try:
                effects = engine.choose_color(interaction.user.id, color)
            except ue.UnoError as e:
                await interaction.response.send_message(f"⚠️ {e}", ephemeral=True)
                return

            self.session.last_action = {"type": "color", "actor_id": interaction.user.id,
                                         "color": color, "effects": effects}
            self.stop()
            await interaction.response.edit_message(content=f"Color set to **{color}**.", view=None)
            await self.cog.refresh_table(self.session)
            self.cog.restart_timer(self.session)
            if engine.finished:
                await self.cog.announce_and_cleanup(self.session)

    @discord.ui.button(label="Red", style=discord.ButtonStyle.danger, emoji="🟥")
    async def red(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, "Red")

    @discord.ui.button(label="Yellow", style=discord.ButtonStyle.secondary, emoji="🟨")
    async def yellow(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, "Yellow")

    @discord.ui.button(label="Green", style=discord.ButtonStyle.success, emoji="🟩")
    async def green(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, "Green")

    @discord.ui.button(label="Blue", style=discord.ButtonStyle.primary, emoji="🟦")
    async def blue(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, "Blue")

    async def on_timeout(self):
        async with self.session.lock:
            engine = self.session.engine
            if not engine or engine.finished or not engine.pending_wild:
                return  # already resolved manually in the meantime

            player_id = engine.current_player().player_id
            color = random.choice(ue.COLORS)
            try:
                effects = engine.choose_color(player_id, color)
            except ue.UnoError:
                return

            self.session.last_action = {"type": "color", "actor_id": player_id,
                                         "color": color, "effects": effects}
            self.stop()
            for child in self.children:
                child.disabled = True
            if self.interaction is not None:
                try:
                    await self.interaction.edit_original_response(
                        content=f"<a:bay_alarm:1536288829248512030> No color chosen in time — randomly set to **{color}**.", view=None,
                    )
                except discord.HTTPException:
                    pass
            await self.cog.refresh_table(self.session)
            self.cog.restart_timer(self.session)
            if engine.finished:
                await self.cog.announce_and_cleanup(self.session)

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        await report_component_error(interaction, error, item, "ColorButtonsView")


class CallUnoToggleView(discord.ui.View):
    """
    Single button shown alongside the Hand view when a player has 1 or 2
    cards left — its meaning depends on which:

      2 cards: arm/disarm toggle. Default color is neutral (secondary/
        gray); toggling it on switches it to green (success) to show it's
        armed. Arming it doesn't announce anything by itself — the "UNO!"
        channel message fires later, automatically, the moment they play
        down to 1 card while armed (see handle_play_card).
      1 card: immediate declaration — pressing it calls UNO right now and
        sends the public announcement. Nothing gets "armed" at 1 card;
        there's no future checkpoint left to arm for.
    """

    def __init__(self, cog: "UnoGame", session: GameSession, player_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.session = session
        self.player_id = player_id
        cog._track_view(self)

        engine = session.engine
        player = engine.player_by_id(player_id) if engine else None
        hand_len = len(player.hand) if player else 2

        if hand_len == 1:
            label, style = "Call UNO", discord.ButtonStyle.success
        else:
            armed = bool(player and player.armed_uno)
            label = "UNO Armed" if armed else "Call UNO"
            style = discord.ButtonStyle.success if armed else discord.ButtonStyle.secondary

        button = discord.ui.Button(label=label, style=style)
        button.callback = self._toggle
        self.add_item(button)

    async def _toggle(self, interaction: discord.Interaction):
        await self.cog.handle_toggle_arm_uno(interaction, self.session)

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        await report_component_error(interaction, error, item, "CallUnoToggleView")


class PlayCardModal(discord.ui.Modal, title="Play a card"):
    """Text-entry fallback — used automatically when a hand is too big for
    a button grid (> MAX_BUTTONS_TOTAL), and always available as a manual
    option via /uno play for anyone who'd rather type than tap."""
    card_input = discord.ui.TextInput(
        label="Card (e.g. Red_4, Blue Skip, Wild Draw 4)",
        placeholder="Red_4",
        max_length=20,
    )

    def __init__(self, cog: "UnoGame", session: GameSession):
        super().__init__()
        self.cog = cog
        self.session = session

    async def on_submit(self, interaction: discord.Interaction):
        card = ue.parse_card_input(self.card_input.value)
        if card is None:
            await interaction.response.send_message(
                f"⚠️ Couldn't understand `{self.card_input.value}`. Try formats like `Red_4`, `Blue Skip`, `Wild Draw 4`.",
                ephemeral=True,
            )
            return
        await self.cog.handle_play_card(interaction, self.session, card)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await report_component_error(interaction, error, None, "PlayCardModal")


class HandPlayView(discord.ui.LayoutView):
    """
    The real per-card button hand — stitched hand image up top, color-
    grouped rows of buttons below, each one playing that exact card when
    tapped. Only used when the hand fits in Discord's 25-button ceiling;
    Play falls back to PlayCardModal automatically above that.
    """

    def __init__(self, cog: "UnoGame", session: GameSession, player_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.session = session
        self.player_id = player_id
        self.file: discord.File | None = None
        cog._track_view(self)
        # NOTE: construction is two-phase — __init__ can't be async, but
        # building the hand image involves a synchronous PNG encode that's
        # expensive enough to matter on the event loop. Use the `create()`
        # classmethod below instead of calling HandPlayView(...) directly;
        # it awaits _build() (which offloads the PNG encode to a thread)
        # before handing back a fully-populated view.

    @classmethod
    async def create(cls, cog: "UnoGame", session: GameSession, player_id: int) -> "HandPlayView":
        self = cls(cog, session, player_id)
        await self._build()
        return self

    async def _build(self):
        engine = self.session.engine
        player = engine.player_by_id(self.player_id)
        hand = sort_hand(list(player.hand))

        container = Container(accent_colour=discord.Color.blurple())
        container.add_item(TextDisplay(f"Your hand — {len(hand)} card{'s' if len(hand) != 1 else ''}. Tap one to play it."))

        try:
            png = await self.cog.hand_png(hand)
            filename = "hand.png"
            self.file = discord.File(io.BytesIO(png), filename=filename)
            container.add_item(MediaGallery(discord.MediaGalleryItem(f"attachment://{filename}")))
        except (FileNotFoundError, ValueError):
            pass  # missing card art for something in hand — buttons still work fine without the image

        container.add_item(Separator(spacing=discord.SeparatorSpacing.small))

        rows, used_fallback = build_rows_with_fallback(hand)

        for row_cards in rows:
            row = ActionRow()
            for card_name in row_cards:
                btn = Button(
                    label=display_label(card_name),
                    emoji=self.cog.emoji_for(card_name),
                    style=discord.ButtonStyle.secondary,
                )
                btn.callback = self._make_play_callback(card_name)
                row.add_item(btn)
            container.add_item(row)

        if len(hand) in (1, 2):
            uno_row = ActionRow()
            if len(hand) == 1:
                uno_label, uno_style = "Call UNO", discord.ButtonStyle.success
            else:
                uno_label = "UNO Armed" if player.armed_uno else "Call UNO"
                uno_style = discord.ButtonStyle.success if player.armed_uno else discord.ButtonStyle.secondary
            uno_btn = Button(label=uno_label, style=uno_style)
            uno_btn.callback = self._toggle_uno
            uno_row.add_item(uno_btn)
            container.add_item(uno_row)

        self.add_item(container)

    def _make_play_callback(self, card_name: str):
        async def _callback(interaction: discord.Interaction):
            await self.cog.handle_play_card(interaction, self.session, card_name)
        return _callback

    async def _toggle_uno(self, interaction: discord.Interaction):
        await self.cog.handle_toggle_arm_uno(interaction, self.session)

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        await report_component_error(interaction, error, item, "HandPlayView")


# ---------------------------------------------------------------------------
# Table view
# ---------------------------------------------------------------------------

class TableView(discord.ui.LayoutView):
    """
    The main per-turn game message. Built as Components V2 (LayoutView)
    instead of an embed specifically so @mentions inside it actually ping
    people — Discord does not send push notifications for mentions inside
    embed fields/descriptions, only for mentions in real message content,
    and TextDisplay content counts as real content. Styled to look like
    an embed (colored accent bar via Container, thumbnail via Section)
    even though it technically isn't one.
    """

    def __init__(self, cog: "UnoGame", session: GameSession):
        # Old TableViews used to live forever (timeout=None), so every past
        # turn's View stayed registered with discord.py indefinitely — a
        # slow, unbounded memory leak over a long game. A bounded timeout
        # lets discord.py release old View registrations once they're no
        # longer useful, with zero visible change: old table messages and
        # their buttons still look exactly the same, they just eventually
        # stop responding. 720s (12 min) comfortably covers the max 600s
        # turn timeout plus a buffer, so the CURRENT table's buttons are
        # never at risk of expiring mid-turn — only genuinely old ones do.
        super().__init__(timeout=720)
        self.cog = cog
        self.session = session
        self.file: discord.File | None = None
        cog._track_view(self)
        # NOTE: construction is two-phase, same reasoning as HandPlayView —
        # __init__ can't be async, but building the top-card thumbnail can
        # involve a synchronous PNG encode. Use the `create()` classmethod
        # below instead of calling TableView(...) directly.

    @classmethod
    async def create(cls, cog: "UnoGame", session: GameSession) -> "TableView":
        self = cls(cog, session)
        await self._build()
        return self

    async def on_timeout(self):
        # View.timeout already stops the View on its own; calling stop()
        # explicitly here is harmless belt-and-suspenders, not essential.
        self.stop()

    async def _build(self):
        engine = self.session.engine
        color = COLOR_TO_DISCORD.get(engine.current_color, discord.Color.dark_grey()) if engine else discord.Color.blurple()

        container = Container(accent_colour=color)

        text = _build_table_text(self.session)
        self.file = await self.cog._top_card_file(engine)
        if self.file:
            container.add_item(Section(text, accessory=Thumbnail(f"attachment://{self.file.filename}")))
        else:
            container.add_item(TextDisplay(text))

        container.add_item(Separator(spacing=discord.SeparatorSpacing.small))

        # All 5 buttons fit in a single ActionRow (Discord's cap is 5 per
        # row) — no reason to split them across two.
        row = ActionRow()
        play_btn = Button(label="Play", style=discord.ButtonStyle.primary, custom_id="uno_play")
        play_btn.callback = self._play
        row.add_item(play_btn)
        draw_btn = Button(label="Draw", style=discord.ButtonStyle.secondary, custom_id="uno_draw")
        draw_btn.callback = self._draw
        row.add_item(draw_btn)
        callout_btn = Button(label="Callout", style=discord.ButtonStyle.danger, custom_id="uno_callout")
        callout_btn.callback = self._callout
        row.add_item(callout_btn)
        hand_btn = Button(label="Hand", style=discord.ButtonStyle.secondary, custom_id="uno_hand")
        hand_btn.callback = self._hand
        row.add_item(hand_btn)
        table_btn = Button(label="Table", style=discord.ButtonStyle.secondary, custom_id="uno_table")
        table_btn.callback = self._table
        row.add_item(table_btn)
        container.add_item(row)

        self.add_item(container)

    def _guard(self) -> GameSession | None:
        """Returns the session if it's still a live, unfinished game — None
        (and safe to bail) otherwise. Every button routes through this
        first so clicking a stale button on an old finished game's
        message (which we never delete) can't throw and spam the console."""
        session = self.session
        if not session or not session.engine or session.engine.finished:
            return None
        if not self.cog._is_live(session):
            # /uno close (or anything else that pops the channel out of
            # self.sessions) doesn't touch this session/engine object —
            # old TableViews kept a direct reference and every check
            # above still passed, so their buttons kept working even
            # after the game was "ended". Catch that here too.
            return None
        return session

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        # Logs the real error (see report_component_error) instead of
        # either flooding console with discord.py's default dump or
        # silently swallowing it with no trace at all.
        await report_component_error(interaction, error, item, "TableView")

    async def _play(self, interaction: discord.Interaction):
        session = self._guard()
        if not session:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        engine = session.engine
        player = engine.player_by_id(interaction.user.id)
        if not player:
            await interaction.response.send_message("You're not in this game.", ephemeral=True)
            return

        if len(player.hand) > MAX_BUTTONS_TOTAL:
            # Too many cards for a button grid — force the text modal.
            # A modal MUST be the raw, undeferred initial response, so this
            # branch can't defer — it's engine-only work (no image/network
            # I/O) up to this point, so it's already fast enough.
            try:
                await interaction.response.send_modal(PlayCardModal(self.cog, session))
            except discord.InteractionResponded:
                log.warning("uno_play: interaction already responded before send_modal (double-click?)")
            return

        # Below this point we're doing real I/O (application-emoji fetch on
        # first use, PIL image composition) that risks blowing past
        # Discord's ~3s ack window. Rather than defer up front (which would
        # burn the modal fallback — a modal must be the raw, un-deferred
        # first response), give the BUILD a short budget: if it finishes in
        # time, send it directly and the modal option was never needed; if
        # it's slow OR raises, fall back to the modal, which is always fast
        # since it's just opening a text field.
        #
        # Crucially, the timeout only ever wraps the local/CPU build (image
        # composition, emoji cache), never the actual
        # interaction.response.send_message() call to Discord. Wrapping the
        # live network call in wait_for() is what caused the 40060
        # "already acknowledged" bug: cancelling that call locally on
        # timeout doesn't mean the request didn't land — discord.py only
        # marks the interaction responded AFTER the call returns, so a
        # request that was cancelled mid-flight (already delivered to
        # Discord, just still waiting on the response body) leaves us
        # thinking we can still send_modal when Discord already considers
        # the interaction acknowledged. Keeping the network call outside
        # any wait_for/cancellation removes that race entirely.
        try:
            await self.cog.load_emoji_cache()

            try:
                view = await asyncio.wait_for(
                    HandPlayView.create(self.cog, session, interaction.user.id),
                    timeout=2.0,
                )
            except Exception:
                log.exception("HandPlayView build failed or was too slow for user %s", interaction.user.id)
                await interaction.response.send_modal(PlayCardModal(self.cog, session))
                return

            if view.file:
                await interaction.response.send_message(view=view, file=view.file, ephemeral=True)
            else:
                await interaction.response.send_message(view=view, ephemeral=True)
        except discord.HTTPException:
            log.exception("uno_play: send_message/send_modal failed for user %s", interaction.user.id)

    async def _hand(self, interaction: discord.Interaction):
        session = self._guard()
        if not session:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        await self.cog.send_hand_view(interaction, session, interaction.user.id)

    async def _draw(self, interaction: discord.Interaction):
        session = self._guard()
        if not session:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        await self.cog.handle_draw_card(interaction, session)

    async def _table(self, interaction: discord.Interaction):
        """
        Shows turn order top-to-bottom as a fixed seat list with an arrow
        on whoever's turn it is. The list itself flips when the direction
        is reversed, so "top-to-bottom" always reads as "the order play
        will actually go in" rather than needing a separate ↻/↺ symbol.
        This stays a plain embed (ephemeral, no ping concern) per request.
        """
        session = self._guard()
        if not session:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return

        engine = session.engine
        order = list(engine.players)
        if engine.direction == -1:
            order = list(reversed(order))

        current_id = engine.current_player().player_id if engine.players else None
        lines = []
        for p in order:
            marker = "<:uno_arrow:1536135499566157956>" if p.player_id == current_id else "・"
            uno_tag = " <:uno:1536135137232953451>" if len(p.hand) == 1 and p.said_uno else ""
            lines.append(f"{marker} {_mention(p.player_id)} — {len(p.hand)} card{'s' if len(p.hand) != 1 else ''}{uno_tag}")

        if engine.finishers:
            placed = ", ".join(
                f"{PLACEMENT_EMOJI.get(i, f'{i+1}.')} {_mention(pid)}"
                for i, pid in enumerate(engine.finishers)
            )
            lines.append(f"\n<:uno_trophy:1536135332863676548> Finished: {placed}")

        embed = discord.Embed(
            title="Table",
            description="\n".join(lines) or "_no active players_",
            color=discord.Color.blurple(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def _callout(self, interaction: discord.Interaction):
        session = self._guard()
        if not session:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        engine = session.engine

        # If a Wild Draw Four is pending a decision, Callout doubles as "challenge it".
        if engine.pending_wd4_challenge:
            async with session.lock:
                pc = engine.pending_wd4_challenge
                if not pc or interaction.user.id != pc["target_id"]:
                    await interaction.response.send_message(
                        f"⚠️ Waiting on {_mention(pc['target_id']) if pc else 'someone'} to accept or challenge the Wild Draw Four.",
                        ephemeral=True,
                    )
                    return
                result = engine.challenge_wild_draw4(interaction.user.id)
                if result["success"]:
                    session.last_action = {"type": "wd4_challenge_win", "actor_id": interaction.user.id,
                                            "penalty_id": result["penalty_to"]}
                    await interaction.response.send_message(
                        "✅ Challenge succeeded! They had a matching card, they draw 4 instead of you.", ephemeral=False
                    )
                else:
                    session.last_action = {"type": "wd4_challenge_lose", "actor_id": interaction.user.id}
                    await interaction.response.send_message(
                        "❌ Challenge failed, they really didn't have a match. You draw 6.", ephemeral=False
                    )
                self.cog.restart_timer(session)
                await self.cog.refresh_table(session)
            return

        await self.cog.handle_callout(interaction, session)


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------

class UnoGame(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessions: dict[int, GameSession] = {}  # channel_id -> GameSession
        self._emoji_cache: dict[str, discord.PartialEmoji] = {}
        self._emoji_cache_loaded = False

        # Every View/LayoutView this cog instance creates registers itself
        # here (see _track_view) so cog_unload can stop them all on
        # reload — a WeakSet so a view that's already been garbage
        # collected (message deleted, or discord.py's own timeout cleanup)
        # doesn't need to be explicitly removed first.
        self._active_views: "weakref.WeakSet" = weakref.WeakSet()

        # Guards the check-then-insert in /uno start so two near-
        # simultaneous invocations in the same channel can't both see "no
        # session yet" and both create one. One lock per channel, created
        # lazily; cheap to keep around for the lifetime of the process.
        self._start_locks: dict[int, asyncio.Lock] = collections.defaultdict(asyncio.Lock)

        # Card art, loaded into memory ONCE here instead of being re-read
        # from disk on every hand/thumbnail render. Hand images and top-
        # card thumbnails are then composed/padded purely in memory and
        # attached via BytesIO — nothing gets written to disk for these
        # anymore (the old ./_uno_hand_*.png, ./_uno_hand_view_*.png, and
        # ./_uno_thumb_cache/ files were exactly this: a full disk write
        # on every single hand/table render, for every player, forever).
        self._card_image_cache: dict[str, Image.Image] = {}
        # sorted-hand tuple -> finished PNG bytes (LRU). Players re-open the same
        # hand repeatedly while waiting; a hit skips composing AND encoding.
        self._hand_png_cache: "collections.OrderedDict[tuple, bytes]" = collections.OrderedDict()
        self._load_card_image_cache()
        self._thumb_bytes_cache: dict[str, bytes] = {}  # card_name -> pre-encoded padded PNG bytes

        self._flush_turn_log.start()

    def _track_view(self, view: discord.ui.View):
        self._active_views.add(view)

    @staticmethod
    def _pending_draw_for(session: GameSession, user_id: int) -> dict | None:
        pd = session.pending_draw
        if pd and pd["player_id"] == user_id and pd["engine"] is session.engine:
            return pd
        return None

    def _is_live(self, session: GameSession) -> bool:
        """True iff `session` is still THE registered session for its
        channel. /uno close (and any other path that pops a channel out of
        self.sessions) doesn't touch the session object itself or its
        engine — old Views (TableView, HandPlayView) and modals
        (PlayCardModal, DrawFollowupView) all hold a direct reference to
        that same session object in their closures, so after /uno close
        `session.engine` is still non-None/unfinished and every
        engine-level check on it still passes. Without this identity
        check, those stale components kept working — the game "kept
        going" through old buttons even after /uno close said it ended.
        Every shared action handler below checks this first."""
        return self.sessions.get(session.channel_id) is session

    def _stop_session_views(self, session: GameSession):
        """Stops every tracked View belonging to this specific session
        (its buttons show as disabled/dead on next interaction) — used by
        /uno close so old table/hand messages don't just silently no-op,
        they visibly stop responding. Narrower than cog_unload's
        stop-everything sweep, which is for a full cog reload."""
        for view in list(self._active_views):
            if getattr(view, "session", None) is session:
                try:
                    view.stop()
                except Exception:
                    pass

    async def cog_unload(self):
        """
        Called by discord.py right before this cog is removed — e.g. a
        manual reload. Without this, a reload leaves the OLD cog instance's
        timer tasks still running and its Views' buttons still registered
        and responding, side-by-side with the brand new cog instance that
        replaces it — two instances racing over the same channels' state,
        neither of which is `self.sessions` on the other.

        Cancels every session's timer task (so no stale AFK timer from the
        old instance can fire), checkpoints active games to disk one last
        time (belt-and-suspenders — post_table already saves after every
        turn, but this catches anything since the last one), stops every
        View this instance created (so old buttons stop responding and
        discord.py can release them), and clears this instance's session
        table. Deliberately does NOT try to hand sessions off to the new
        cog instance in memory — the new instance starts empty, and each
        channel's game gets explicitly picked back up with the existing
        manual /uno resume flow, same as a crash/restart recovery.
        """
        for session in list(self.sessions.values()):
            session.cancel_timer()
            session.cancel_window()
            if not session.closed:
                try:
                    await asyncio.to_thread(self._save_session, session)
                except Exception:
                    log.exception("Failed to checkpoint channel %s during cog_unload", session.channel_id)

        self._flush_turn_log.cancel()
        # One last drain so events since the final periodic flush aren't
        # silently dropped on a reload — same reasoning as the checkpoint
        # save just above.
        all_events = []
        for session in list(self.sessions.values()):
            if session.started and session.engine:
                all_events.extend(session.engine.pop_turn_events())
        if all_events:
            try:
                await db.log_turn_events_bulk(all_events)
            except Exception:
                log.exception("Failed to flush final UNO turn log batch during cog_unload")

        for view in list(self._active_views):
            try:
                view.stop()
            except Exception:
                pass

        self.sessions.clear()
        log.info("UnoGame cog unloaded — timers cancelled, views stopped, sessions checkpointed and cleared.")

    @tasks.loop(seconds=5)
    async def _flush_turn_log(self):
        """The actual batching: instead of a DB write (and disk fsync) per
        play/draw/pass across however many tables are live, drain every
        active engine's buffered turn_events every 5 seconds and write
        them all in ONE transaction. A round's own settlement also does a
        final drain (see announce_and_cleanup) so nothing waits a full
        cycle to land once the round's actually over."""
        all_events = []
        for session in list(self.sessions.values()):
            if session.started and session.engine:
                all_events.extend(session.engine.pop_turn_events())
        if all_events:
            try:
                await db.log_turn_events_bulk(all_events)
            except Exception:
                log.exception("Failed to flush UNO turn log batch (%d events)", len(all_events))

    @_flush_turn_log.before_loop
    async def _before_flush_turn_log(self):
        await self.bot.wait_until_ready()

    def _render_hand_png(self, hand: list[str]) -> bytes:
        img = compose_hand_image(hand, self._card_image_cache)
        buf = io.BytesIO()
        img.save(buf, format="PNG", compress_level=3)  # faster encode, slightly larger file
        return buf.getvalue()

    async def hand_png(self, hand: list[str]) -> bytes:
        """Hand image as PNG bytes, LRU-cached. Compose + encode run together in
        ONE worker-thread call. Raises FileNotFoundError / ValueError like
        compose_hand_image."""
        key = tuple(hand)
        data = self._hand_png_cache.get(key)
        if data is not None:
            self._hand_png_cache.move_to_end(key)
            return data
        data = await asyncio.to_thread(self._render_hand_png, hand)
        self._hand_png_cache[key] = data
        while len(self._hand_png_cache) > 48:
            self._hand_png_cache.popitem(last=False)
        return data

    def _load_card_image_cache(self):
        if not os.path.isdir(card_assets.CARDS_DIR):
            print(f"⚠️ UNO cards folder not found: {card_assets.CARDS_DIR}")
            return
        for fname in os.listdir(card_assets.CARDS_DIR):
            if not fname.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
                continue
            name = os.path.splitext(fname)[0]
            try:
                path = os.path.join(card_assets.CARDS_DIR, fname)
                with Image.open(path) as im:
                    # stored at render height (300px) — less RAM, and no per-render resizing
                    self._card_image_cache[name] = prepare_card_image(im)
            except Exception:
                log.exception("Failed to load card image %s", fname)
        log.info("Cached %d card image(s) in memory", len(self._card_image_cache))
        print(f"✅ Cached {len(self._card_image_cache)} UNO cards in memory")

    uno = app_commands.Group(name="uno", description="UNO game commands", guild_ids=[config.GUILD_ID])
    unomgr = app_commands.Group(name="unomgr", description="UNO manager commands (economy)", guild_ids=[config.GUILD_ID])
    unoadmin = app_commands.Group(name="unoadmin", description="UNO admin/dev commands", guild_ids=[config.GUILD_ID])
    unoset = app_commands.Group(name="unoset", description="Configure UNO settings", guild_ids=[config.GUILD_ID])

    # ---------------- persistence ----------------

    def _save_path(self, channel_id: int) -> Path:
        return SAVE_DIR / f"{channel_id}.json"

    def _save_session(self, session: GameSession):
        """
        Checkpoints a started game to disk. Best-effort: a disk/permission
        problem here should never take down the actual game, so failures
        are logged and swallowed rather than propagated. Writes to a temp
        file and os.replace()s it into place so a crash mid-write can't
        leave a half-written, unreadable save behind.
        """
        if session.closed:
            return
        try:
            SAVE_DIR.mkdir(parents=True, exist_ok=True)
            data = json.dumps(session.to_dict())
            fd, tmp_path = tempfile.mkstemp(dir=SAVE_DIR, suffix=".tmp")
            try:
                with os.fdopen(fd, "w") as f:
                    f.write(data)
                os.replace(tmp_path, self._save_path(session.channel_id))
            except Exception:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                raise
        except Exception:
            log.exception("Failed to save game state for channel %s", session.channel_id)

    def _delete_save(self, channel_id: int):
        try:
            self._save_path(channel_id).unlink(missing_ok=True)
        except Exception:
            log.exception("Failed to delete save file for channel %s", channel_id)

    @uno.command(name="resume", description="Resume this channel's table from its last saved checkpoint")
    async def resume_cmd(self, interaction: discord.Interaction):
        """
        Deliberately manual, not automatic — the whole table (seats, bets,
        and the round in progress, if any) is checkpointed to disk, so if
        the bot crashed or a bug corrupted state, the save file is the
        recovery point: hand-fix the JSON if needed, then run this to pick
        back up from it. No auto-restore-on-boot, on purpose — that would
        resume every saved table unconditionally on every restart, with no
        chance to inspect or fix a save first if the crash was caused by
        bad state. The chips stay locked on the table (chips_in_play) the
        whole time, which is what makes resuming safe.
        """
        if interaction.channel_id in self.sessions:
            await interaction.response.send_message(
                "There's already a table open in this channel — end it first.", ephemeral=True)
            return

        path = self._save_path(interaction.channel_id)
        if not path.exists():
            await interaction.response.send_message("No saved table found for this channel.", ephemeral=True)
            return

        try:
            with open(path, "r") as f:
                data = json.load(f)
            session = GameSession.from_dict(data, await db.get_settings(data["guild_id"]))
        except Exception:
            log.exception("Failed to parse save file for channel %s", interaction.channel_id)
            await interaction.response.send_message(
                "⚠️ That save file didn't load — check the logs for the exact error "
                "(bad/edited JSON, wrong field, etc.), fix it, and try again.", ephemeral=True)
            return

        if not await is_event_staff(interaction):
            await interaction.response.send_message("Only event staff can resume this save.", ephemeral=True)
            return

        engine = session.engine
        if engine:
            # A round in progress (or finished but not yet settled) has its
            # bets locked in chips_in_play. If they're gone — e.g.
            # /unoadmin recoverchips refunded them — resuming would pay the
            # round out on top of chips already returned to wallets. The
            # one exception is a round whose settlement already committed
            # (a crash right after it): replaying that is a harmless no-op.
            already_settled = engine.finished and await db.round_exists(engine.round_uuid)
            if not already_settled:
                missing = [pid for pid in engine.bets if await db.get_table_amount(pid) <= 0]
                if missing:
                    await interaction.response.send_message(
                        "⚠️ Can't resume safely: " + ", ".join(f"<@{pid}>" for pid in missing) +
                        " no longer have chips on the table (probably refunded with `/unoadmin recoverchips`), "
                        "so resuming would pay them twice. Use `/uno close force:True` instead.",
                        ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                    return
        else:
            # Between rounds the DB is the source of truth for what's on the table
            for pid, seat in list(session.seats.items()):
                amount = await db.get_table_amount(pid)
                if amount <= 0 and seat.bet > 0:
                    del session.seats[pid]  # refunded out from under the save
                else:
                    seat.bet = amount if amount > 0 else seat.bet

        self.sessions[interaction.channel_id] = session
        try:
            await interaction.response.send_message("🔄 Resuming saved table — posted below.", ephemeral=True)
            async with session.lock:
                if engine and engine.finished:
                    await self.announce_and_cleanup(session)
                elif engine:
                    session.state = "playing"
                    await self.post_table(session, interaction.channel)
                    self.restart_timer(session)
                elif session.state == "lobby":
                    await self.post_panel(session, interaction.channel)
                else:
                    await self.open_window(session, interaction.channel)
        except Exception:
            log.exception("Failed to resume table for channel %s", interaction.channel_id)
            self.sessions.pop(interaction.channel_id, None)
            # Deliberately NOT deleting the save here — a failed resume
            # (e.g. a bad emoji/image render) shouldn't destroy the one
            # copy of the table state; leave it on disk so /uno resume can
            # just be retried after whatever broke gets fixed.
            await interaction.followup.send(
                "⚠️ Resume failed partway through — the save file is untouched, you can try again.", ephemeral=True)

    # ---------------- card emoji (shared with the button-grid hand UI) ----------------

    async def load_emoji_cache(self):
        if self._emoji_cache_loaded:
            return
        try:
            app_emojis = await self.bot.fetch_application_emojis()
            for e in app_emojis:
                self._emoji_cache[e.name] = e
        except Exception:
            # Was a bare `except: pass` — silently leaves every card falling
            # back to its plain-color emoji with zero trace of why. Not
            # fatal (fallback emoji still works), so we don't re-raise, but
            # it needs to be visible in logs.
            log.exception("Failed to load application emojis — falling back to plain-color emoji for all cards")
        self._emoji_cache_loaded = True

    def emoji_for(self, card_name: str):
        sanitized = sanitize_emoji_name(card_name)
        if sanitized in self._emoji_cache:
            return self._emoji_cache[sanitized]
        color = card_name.split("_")[0]
        return FALLBACK_EMOJI_BY_COLOR.get(color, "🃏")

    # ---------------- table rendering ----------------

    async def _square_pad_thumbnail_bytes(self, card_name: str) -> bytes | None:
        """
        Card art is tall (~2:3), but Components V2's Thumbnail crops non-
        square images to fit its roughly-square slot — cutting into the
        card face. Padding it onto a square transparent canvas first
        means only the padding gets cropped, never the actual card art.
        Built once per card name from the in-memory cache, then cached
        as PNG bytes (never touches disk) — so the synchronous PIL/PNG
        work below only actually runs once per unique card name ever;
        every other call is just a cache hit.
        """
        cached = self._thumb_bytes_cache.get(card_name)
        if cached is not None:
            return cached

        img = self._card_image_cache.get(card_name)
        if img is None:
            return None

        # Offload the actual pad+encode to a thread so a first-ever draw of
        # some obscure card doesn't stall the event loop (and every other
        # channel's interactions) while it renders.
        data = await asyncio.to_thread(self._render_square_thumbnail, img)
        self._thumb_bytes_cache[card_name] = data
        return data

    @staticmethod
    def _render_square_thumbnail(img: Image.Image) -> bytes:
        side = max(img.width, img.height)
        canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
        canvas.paste(img, ((side - img.width) // 2, (side - img.height) // 2), img)
        buf = io.BytesIO()
        canvas.save(buf, format="PNG")
        return buf.getvalue()

    async def _top_card_file(self, engine: ue.GameState | None) -> discord.File | None:
        if not engine or not engine.discard_pile:
            return None
        card_name = engine.top_card()
        data = await self._square_pad_thumbnail_bytes(card_name)
        if data is None:
            return None
        return discord.File(io.BytesIO(data), filename=f"{card_name}.png")

    async def post_table(self, session: GameSession, channel: discord.abc.Messageable):
        """
        Sends a BRAND NEW table message every time — never edits, never
        deletes. Discord never sends a notification for a mention that's
        added via message edit, so the only way for the next player to
        actually get pinged each turn is a fresh message; keeping every
        previous message around (rather than deleting them) is by design
        so the channel keeps a full visible history of the game.
        """
        view = await TableView.create(self, session)
        if view.file:
            msg = await channel.send(view=view, file=view.file)
        else:
            msg = await channel.send(view=view)

        session.table_message = msg
        session.msg_count = 0

        # Every action that changes the table ends up here (see
        # refresh_table + the direct callers below it) — this is the one
        # choke point that covers every turn of an in-progress game, so
        # it's the natural spot to checkpoint to disk. Offloaded to a
        # thread since the JSON encode + file write is blocking I/O; a
        # save failure here must never break the game itself, hence the
        # broad catch inside _save_session.
        await asyncio.to_thread(self._save_session, session)

    async def refresh_table(self, session: GameSession):
        """
        Resolves the right channel and delegates to post_table — kept as
        the name every action call site already uses, but it now always
        sends a new message rather than editing (see post_table's
        docstring for why editing can't ping).
        """
        channel = session.table_message.channel if session.table_message is not None else None
        if channel is None:
            channel = self.bot.get_channel(session.channel_id)
        if channel is None:
            return
        await self.post_table(session, channel)

    # ---------------- table lifecycle ----------------
    #
    # A table outlives its rounds: lobby -> (round -> window -> round ...) with
    # chips staying on the table (chips_in_play) between rounds. Every method
    # below that mutates the table expects the caller to hold session.lock —
    # the interaction handlers, the turn timer and the window timer all take
    # it before calling in — so none of them acquire it themselves.

    async def _reload_settings(self, session: GameSession):
        session.apply_settings(await db.get_settings(session.guild_id))

    def _save_session_now(self, session: GameSession):
        self._save_session(session)

    async def _log_seat(self, session: GameSession, uid: int, name: str, event: str,
                        amount: int | None = None, detail: str | None = None):
        """round_num = round in progress, or the last finished one between rounds.
        Audit only — values are captured now and the DB write runs in the
        background so gameplay never waits on it. Never breaks gameplay."""
        args = (session.table_id, session.round_num,
                session.engine.round_uuid if session.engine else None,
                uid, name, event, amount, detail)

        async def _write():
            try:
                await db.log_seat_event(*args)
            except Exception:
                log.exception("seat_log write failed (%s, table %s)", event, args[0])

        safe_task(_write(), name="uno-seat-log")

    async def _log_table(self, session: GameSession, event: str, actor_id: int | None = None,
                         detail: str | None = None):
        args = (session.guild_id, session.channel_id, session.table_id, event,
                session.round_num, actor_id, detail)

        async def _write():
            try:
                await db.log_table_event(*args)
            except Exception:
                log.exception("table_log write failed (%s, table %s)", event, args[2])

        safe_task(_write(), name="uno-table-log")

    async def _retire_panel(self, session: GameSession):
        old, session.lobby_message = session.lobby_message, None
        if old:
            try:
                await old.edit(view=None)
            except discord.HTTPException:
                pass

    async def post_panel(self, session: GameSession, channel: discord.abc.Messageable, ping: bool = False):
        """Posts a fresh join / change-bet panel at the bottom of the
        channel, retiring the previous one. ping=True also @s everyone
        seated (start of every between-rounds window)."""
        await self._retire_panel(session)
        content = None
        if ping and session.seats:
            content = f"🃏 **Round {session.round_num + 1}** — " + " ".join(f"<@{pid}>" for pid in session.seats)
        session.lobby_message = await channel.send(
            content=content, embed=build_panel_embed(session), view=TablePanelView(self, session),
            allowed_mentions=discord.AllowedMentions(users=True),
        )
        session.msg_count = 0

    async def refresh_panel(self, session: GameSession, create: bool = True):
        """Updates the existing panel in place after a seat/bet change; posts a new one if it's gone."""
        if session.lobby_message:
            try:
                await session.lobby_message.edit(embed=build_panel_embed(session))
                return
            except discord.HTTPException:
                session.lobby_message = None
        if create:
            channel = self.bot.get_channel(session.channel_id)
            if channel:
                await self.post_panel(session, channel)

    async def open_window(self, session: GameSession, channel: discord.abc.Messageable):
        """Between rounds: ping everyone seated, post the join / change-bet
        panel and count down to the next round — or, with fewer than 2
        seats, wait for players instead."""
        session.cancel_timer()
        session.cancel_window()
        await self._reload_settings(session)
        if len(session.seats) < 2:
            await self._enter_waiting(session, channel)
            return
        session.state = "window"
        session.window_ends_at = time.time() + session.bet_window
        await self.post_panel(session, channel, ping=True)
        session.window_task = safe_task(self._window_timer(session, session.window_generation),
                                        name=f"uno-window-{session.channel_id}")
        self._save_session(session)

    async def _enter_waiting(self, session: GameSession, channel: discord.abc.Messageable):
        session.cancel_window()
        session.state = "waiting"
        session.window_ends_at = 0.0
        await self.post_panel(session, channel)
        session.window_task = safe_task(self._idle_timer(session, session.window_generation),
                                        name=f"uno-idle-{session.channel_id}")
        self._save_session(session)

    async def _window_timer(self, session: GameSession, generation: int):
        await asyncio.sleep(max(0.0, session.window_ends_at - time.time()))
        async with session.lock:
            if session.closed or generation != session.window_generation or session.state != "window":
                return
            channel = self.bot.get_channel(session.channel_id)
            if not channel:
                await self.close_table(session, None)
                return
            await self.begin_round(session, channel)

    async def _idle_timer(self, session: GameSession, generation: int):
        await asyncio.sleep(config.UNO_IDLE_CLOSE_SECONDS)
        async with session.lock:
            if session.closed or generation != session.window_generation or session.state != "waiting":
                return
            await self.close_table(session, "Table closed — not enough players for a while. "
                                            "Everyone's chips were returned to their wallets.")

    async def begin_round(self, session: GameSession, channel: discord.abc.Messageable):
        """Deals the next round to everyone seated with a valid bet."""
        session.cancel_window()
        await self._reload_settings(session)

        # Anyone still under the minimum when the window closes (their
        # leftover didn't cover it and they didn't top up) is removed and
        # their leftover goes back to their wallet.
        removed = []
        for pid, seat in list(session.seats.items()):
            if seat.bet < session.min_bet:
                amount = await db.table_cash_out(pid, seat.name)
                del session.seats[pid]
                await self._log_seat(session, pid, seat.name, "below_min", amount,
                                     f"below the {session.min_bet} minimum — returned to wallet")
                removed.append(f"<@{pid}> — **{amount}** {config.UNO_CHIP_EMOJI} returned to wallet")
        if removed:
            await channel.send("🚪**Removed from the table (below the {} minimum bet):**\n{}".format(
                session.min_bet, "\n".join(removed)), allowed_mentions=discord.AllowedMentions(users=True))

        if len(session.seats) < 2:
            await self._enter_waiting(session, channel)
            return

        players = [(pid, seat.name) for pid, seat in session.seats.items()]
        random.shuffle(players)  # turn order shouldn't depend on join order
        try:
            engine = ue.GameState(players, num_decks=session.num_decks,
                                  bets={pid: seat.bet for pid, seat in session.seats.items()},
                                  table_id=session.table_id, round_num=session.round_num + 1)
        except ue.UnoError as e:
            await channel.send(f"⚠️ Couldn't deal the round: {e}")
            await self._enter_waiting(session, channel)
            return

        for pid, seat in session.seats.items():
            seat.in_round = True
            seat.leave_queued = False
            seat.quit = False
            player = engine.player_by_id(pid)
            if player:
                player.missed_turns = seat.missed_turns  # AFK streaks carry across rounds

        session.engine = engine
        session.state = "playing"
        session.round_num += 1
        session.last_action = None
        session.callout_balance = {}
        session._last_turn_player_id = None
        await self._retire_panel(session)
        await self.post_table(session, channel)  # also checkpoints the table
        self.restart_timer(session)

    async def close_table(self, session: GameSession, message: str | None):
        """Tears a table down and gives every seat everything it has on the
        table back (lobby cancelled/expired, /uno close, idle close...).
        Mid-round that voids the round — bets only change when a round is
        settled, so what's on the table IS what each player put in."""
        if session.closed:
            return
        session.closed = True
        session.cancel_timer()
        session.cancel_window()
        if session.engine:
            try:
                await db.log_turn_events_bulk(session.engine.pop_turn_events())
            except Exception:
                log.exception("Failed to flush turn log while closing channel %s", session.channel_id)
        refunds = []
        for pid, seat in list(session.seats.items()):
            amount = await db.table_cash_out(pid, seat.name)
            await self._log_seat(session, pid, seat.name, "closed", amount, "table closed — bets returned to wallet")
            refunds.append(f"<@{pid}> — **{amount}** {config.UNO_CHIP_EMOJI}")
        await self._log_table(session, "close", detail=(message or "")[:200])
        session.seats.clear()
        if self.sessions.get(session.channel_id) is session:
            self.sessions.pop(session.channel_id, None)
        # Old table/hand messages' buttons hold a direct reference to this
        # session object — stopping the Views makes them visibly dead
        # immediately instead of only failing silently on next click.
        self._stop_session_views(session)
        await self._retire_panel(session)
        await asyncio.to_thread(self._delete_save, session.channel_id)
        channel = self.bot.get_channel(session.channel_id)
        if channel and message:
            text = message + ("\n" + "\n".join(refunds) if refunds else "")
            try:
                await channel.send(text, allowed_mentions=discord.AllowedMentions(users=True))
            except discord.HTTPException:
                pass

    async def _after_seat_change(self, session: GameSession, channel: discord.abc.Messageable):
        """Called after a seat is removed outside a round: an emptied
        running table closes, a window left with <2 seats falls back to
        waiting, otherwise just refresh the panel."""
        if not session.seats and session.state in ("window", "waiting"):
            await self.close_table(session, "Everyone left — table closed.")
        elif session.state == "window" and len(session.seats) < 2:
            await self._enter_waiting(session, channel)
        else:
            await self.refresh_panel(session, create=False)
            self._save_session(session)

    async def handle_leave(self, interaction: discord.Interaction):
        """Leave button / /uno leave. Mid-round this QUEUES the leave until
        the round ends (the bet is already in play); anywhere else it takes
        effect immediately and the player's chips go back to their wallet."""
        session = self.sessions.get(interaction.channel_id)
        uid = interaction.user.id
        if not session or uid not in session.seats:
            await interaction.response.send_message("You're not seated at a table here.", ephemeral=True)
            return
        async with session.lock:
            seat = session.seats.get(uid)
            if not seat:
                await interaction.response.send_message("You're not seated at a table here.", ephemeral=True)
                return
            if session.state == "playing" and seat.in_round:
                if seat.quit:
                    await interaction.response.send_message("You're already out of this round.", ephemeral=True)
                    return
                seat.leave_queued = not seat.leave_queued
                await self._log_seat(session, uid, seat.name,
                                     "leave_queued" if seat.leave_queued else "leave_cancelled", seat.bet)
                self._save_session(session)
                await interaction.response.send_message(
                    "🚪 You'll leave when this round ends and your chips go back to your wallet. "
                    "(`/uno quit` leaves right now and forfeits your bet.)" if seat.leave_queued
                    else "You'll stay at the table.", ephemeral=True)
                return
            amount = await db.table_cash_out(uid, seat.name)
            del session.seats[uid]
            await self._log_seat(session, uid, seat.name, "leave", amount, "returned to wallet")
            await interaction.response.send_message(
                f"You left the table — **{amount}** {config.UNO_CHIP_EMOJI} returned to your wallet.", ephemeral=True)
            await self._after_seat_change(session, interaction.channel)

    async def _drop_from_round(self, session: GameSession, pid: int, reason: str) -> bool:
        """Removes a player from the round in progress (quit / kicked / left
        the server). Their seat stays until the round settles — their bet
        is forfeited like any other loss — then settlement cashes it out.
        Returns False if they weren't an active player. Caller holds the lock."""
        engine = session.engine
        seat = session.seats.get(pid)
        if not (engine and seat and seat.in_round and not engine.finished and engine.player_by_id(pid)):
            return False
        engine.remove_player(pid, reason=reason)
        seat.quit = True
        seat.exit_reason = reason
        await self._log_seat(session, pid, seat.name, {"kicked": "kick"}.get(reason, reason), seat.bet,
                             "bet forfeited, seat cashed out when the round settles")
        return True

    async def announce_and_cleanup(self, session: GameSession):
        """Called once a round has ended: settles the money, posts the
        result, then opens the next window — the table keeps going."""
        channel = self.bot.get_channel(session.channel_id)
        engine = session.engine
        session.cancel_timer()

        if not channel or not engine:
            await self.close_table(session, None)
            return

        try:
            embed = await self._settle_round(session, channel, engine)
        except Exception:
            # Deliberately do NOT close the table or delete the save file
            # here — if settlement itself blew up, nothing was applied
            # (it's one DB transaction), and the table plus its save are
            # the only record of that round. After fixing whatever broke,
            # restarting the bot and running /uno resume replays the
            # finished round (the save holds it), or /uno close force:True voids it
            # and refunds everyone's bets.
            log.exception("UNO settlement failed for channel %s — table and save file were NOT cleared, "
                           "so this needs manual attention", session.channel_id)
            try:
                await channel.send(
                    "⚠️ Something went wrong finishing this round — nothing was applied and no chips were lost. "
                    "Check the bot's console for the error — after it's fixed, restart the bot and `/uno resume` "
                    "replays this round, or `/uno close force:True` voids it and refunds everyone."
                )
            except discord.HTTPException:
                pass
            return

        try:
            await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        except discord.HTTPException:
            log.exception("Failed to post the round result for channel %s", session.channel_id)

        session.engine = None
        session.last_action = None
        if session.close_queued:
            await self.close_table(session, "🔒 Table closed — the round ended and closing was queued by staff. "
                                            "Everything on the table was returned to wallets:")
            return
        if not session.seats:
            await self.close_table(session, "Everyone left — table closed.")
            return
        await self.open_window(session, channel)

    async def _settle_round(self, session: GameSession, channel, engine: ue.GameState) -> discord.Embed:
        """Applies the finished round (one DB transaction, idempotent on
        round_uuid), updates the seats from the result, cashes out anyone
        who quit / queued a leave, posts the log line and returns the
        round-result embed."""
        results = engine.compute_settlement(config.UNO_WINNERS_TAX_RATE)
        channel_name = getattr(channel, "name", str(session.channel_id))
        winner = engine.winner
        n = session.round_num
        total_tax = sum(r["tax"] for r in results.values())
        collected = results[winner]["change"] if winner in results else 0

        players = [{
            "user_id": pid, "username": engine.all_names.get(pid, str(pid)), "bet": r["bet"],
            "change": r["change"], "tax": r["tax"], "net": r["net"], "new_bet": r["new_bet"],
            "placement": 1 if r["winner"] else None, "won": r["winner"],
        } for pid, r in results.items()]

        # A round nobody won (every player left) changes no balances and
        # isn't recorded. apply_round_settlement returning None means this
        # exact round was already applied — a resume replaying a round
        # that had in fact committed — so the DB is already right and the
        # seats below simply re-sync from it.
        if winner is not None:
            await db.apply_round_settlement(engine.round_uuid, session.guild_id, session.channel_id,
                                            session.table_id, n, engine.num_decks, winner,
                                            engine.all_names.get(winner), players, channel_name,
                                            dealer_id=session.dealer_id, dealer_name=session.dealer_name,
                                            tax_rate=config.UNO_WINNERS_TAX_RATE)
        try:
            await db.log_turn_events_bulk(engine.pop_turn_events())
        except Exception:
            log.exception("Failed to flush turn log for channel %s", session.channel_id)

        leaving: set[int] = set()
        for pid in engine.bets:
            seat = session.seats.get(pid)
            if not seat:
                continue
            seat.in_round = False
            seat.bet = await db.get_table_amount(pid)
            player = engine.player_by_id(pid)
            seat.missed_turns = player.missed_turns if player else 0
            if seat.quit or seat.leave_queued or pid in engine.left_early:
                leaving.add(pid)
        for pid in leaving:
            seat = session.seats.pop(pid, None)
            if seat:
                returned = await db.table_cash_out(pid, seat.name)
                if seat.exit_reason:  # quit / kicked / left_guild
                    why = seat.exit_reason
                elif pid in engine.left_early:  # removed by the AFK timer
                    why = "afk_kick"
                else:  # queued Leave
                    why = "leave"
                await self._log_seat(session, pid, seat.name, "cashed_out", returned, why)
        self._save_session(session)

        settings = await db.get_settings(session.guild_id)
        log_channel_id = settings.get("log_channel_id")
        if log_channel_id and winner is not None:
            log_channel = self.bot.get_channel(int(log_channel_id))
            if log_channel:
                header = (f"Round #{n} | ({session.table_id}) | "
                          f"Pot: {collected} | Tax: {total_tax}")
                rows = "\n".join(
                    f"  {p['username']} ({p['user_id']}): Bet: {p['bet']}  Net: {'+' if p['net'] >= 0 else ''}{p['net']}"
                    for p in players
                )
                try:
                    await log_channel.send(f"```\n{header}\n{rows}\n```")
                except discord.HTTPException:
                    pass

        if winner is None:
            return discord.Embed(title=f"Round {n} voided",
                                 description="Everyone left — no bets changed hands.", color=discord.Color.light_grey())
        embed = discord.Embed(title=f"<:uno_trophy:1536135332863676548> Round {n} complete", color=discord.Color.gold())
        ordered = sorted(results.items(), key=lambda kv: (not kv[1]["winner"], -kv[1]["net"]))
        lines = []
        for pid, r in ordered:
            medal = f"{PLACEMENT_EMOJI.get(0, '')} " if r["winner"] else ""
            sign = "+" if r["net"] >= 0 else ""
            where = "returned to wallet" if pid in leaving else "on the table"
            lines.append(f"{medal}{_mention(pid)} **{sign}{r['net']}** {config.UNO_CHIP_EMOJI} → "
                         f"**{r['new_bet']}** {where}")
        embed.add_field(name="Net", value="\n".join(lines), inline=False)
        embed.set_footer(text=f"Table {session.table_id} · Round {n} · Dealer {session.dealer_name}")
        return embed


    # ---------------- shared action handlers (used by buttons AND slash commands) ----------------

    async def handle_play_card(self, interaction: discord.Interaction, session: GameSession, card: str,
                                drew_first: bool = False):
        """Shared by the button grid, the modal fallback, /uno play, and the
        DrawFollowupView's "Play this Card" button — one code path so all
        four behave identically. drew_first just changes the wording to
        make clear this was a drew-then-played turn, not a plain play."""
        if not self._is_live(session):
            await interaction.response.send_message("This game has ended.", ephemeral=True)
            return

        engine = session.engine
        if not engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return

        async with session.lock:
            if not self._is_live(session) or session.engine is not engine or engine.finished:
                await interaction.response.send_message("This game has ended.", ephemeral=True)
                return
            if engine.current_player().player_id != interaction.user.id:
                await interaction.response.send_message("⚠️ It's not your turn.", ephemeral=True)
                return

            pd = self._pending_draw_for(session, interaction.user.id)
            if pd and card != pd["card"]:
                await interaction.response.send_message(
                    f"⚠️ You drew **{pd['card']}** — you can only play that card or keep it.", ephemeral=True)
                return

            try:
                effects = engine.play_card(interaction.user.id, card)
            except ue.IllegalCard as e:
                await interaction.response.send_message(f"⚠️ {e}", ephemeral=True)
                return
            except ue.NotYourTurn:
                await interaction.response.send_message("⚠️ It's not your turn.", ephemeral=True)
                return

            if pd:
                session.pending_draw = None
            engine.record_action(interaction.user.id)

            # Consume the Call UNO pre-arm (set via the Hand toggle at 2 cards):
            # if it was armed and this play brought them down to exactly 1
            # card, auto-declare it and fire the "UNO!" channel message. Either
            # way the arm is spent once they're past the 2-card checkpoint.
            player = engine.player_by_id(interaction.user.id)
            fired_uno = False
            if player and player.armed_uno:
                if len(player.hand) == 1:
                    engine.call_uno(interaction.user.id)
                    fired_uno = True
                player.armed_uno = False

            if effects.get("color_pending"):
                session.cancel_timer()
                view = ColorButtonsView(self, session, interaction)
                msg = "You drew and played a Wild — choose a color:" if drew_first else "Choose a color for your Wild:"
                await interaction.response.send_message(msg, view=view, ephemeral=True)
                if fired_uno:
                    await self._send_to_game_channel(session, f"{_mention(interaction.user.id)}: UNO!")
                return

            session.last_action = {"type": "play", "actor_id": interaction.user.id, "card": card,
                                    "effects": effects, "drew_first": drew_first}
            confirm = f"You drew and played **{card}**!" if drew_first else f"You played **{card}**."
            await interaction.response.send_message(confirm, ephemeral=True)
            self.restart_timer(session)
            await self.refresh_table(session)
            if fired_uno:
                await self._send_to_game_channel(session, f"{_mention(interaction.user.id)}: UNO!")
            if engine.finished:
                await self.announce_and_cleanup(session)

    async def handle_draw_card(self, interaction: discord.Interaction, session: GameSession):
        """Shared by the Draw button and /uno draw. If the drawn card can
        legally be played, offers a Play-this-card/No follow-up instead of
        always ending the turn outright."""
        if not self._is_live(session):
            await interaction.response.send_message("This game has ended.", ephemeral=True)
            return

        engine = session.engine
        if not engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return

        async with session.lock:
            if not self._is_live(session) or session.engine is not engine or engine.finished:
                await interaction.response.send_message("This game has ended.", ephemeral=True)
                return
            if engine.pending_wd4_challenge:
                pc = engine.pending_wd4_challenge
                if interaction.user.id != pc["target_id"]:
                    await interaction.response.send_message(
                        f"⚠️ Waiting on {_mention(pc['target_id'])} to accept or challenge the Wild Draw Four.",
                        ephemeral=True,
                    )
                    return
                engine.accept_wild_draw4(interaction.user.id)
                session.last_action = {"type": "wd4_accept", "actor_id": interaction.user.id}
                await interaction.response.send_message("You accepted the Wild Draw Four and drew 4 cards.", ephemeral=True)
                self.restart_timer(session)
                await self.refresh_table(session)
                return

            if engine.current_player().player_id != interaction.user.id:
                await interaction.response.send_message("⚠️ It's not your turn.", ephemeral=True)
                return

            if self._pending_draw_for(session, interaction.user.id):
                await interaction.response.send_message(
                    "⚠️ Decide on the card you just drew first — play it or keep it.", ephemeral=True)
                return

            result = engine.draw_card(interaction.user.id)
            drawn = result["drawn"]
            # engine.draw_card() already clears armed_uno itself when the
            # draw moves the hand off exactly 2 cards — see the
            # armed_uno invariant note in uno_engine.py.

            if result["still_playable"]:
                session.pending_draw = {"player_id": interaction.user.id, "card": drawn, "engine": engine}
                session.cancel_timer()
                view = DrawFollowupView(self, session, drawn, interaction.user.id)
                await interaction.response.send_message(f"You drew **{drawn}**. Play it?", view=view, ephemeral=True)
                return  # turn stays open until they pick Play/No — no refresh/restart yet

            engine.pass_turn(interaction.user.id)
            session.last_action = {"type": "draw", "actor_id": interaction.user.id}
            await interaction.response.send_message(f"You drew **{drawn}**.", ephemeral=True)
            self.restart_timer(session)
            await self.refresh_table(session)

    async def handle_callout(self, interaction: discord.Interaction, session: GameSession):
        """No-target callout: catches everyone currently vulnerable at once,
        or penalizes the accuser if nobody qualifies. Just posts a plain
        chat message — doesn't send a new turn/table message.

        Each player gets 1 false-callout "charge" that refreshes when it
        becomes their turn again (see restart_timer) — only a FALSE
        callout spends it; successfully catching someone is free and
        doesn't touch the balance. Once spent, further callout attempts
        are blocked outright until their turn comes back around, so
        repeatedly mashing Callout can't keep drawing cards/spamming
        messages.
        """
        if not self._is_live(session):
            await interaction.response.send_message("This game has ended.", ephemeral=True)
            return

        engine = session.engine
        if not engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        if engine.player_by_id(interaction.user.id) is None:
            await interaction.response.send_message("Only active players can call someone out.", ephemeral=True)
            return

        async with session.lock:
            if not self._is_live(session) or session.engine is not engine or engine.finished:
                await interaction.response.send_message("This game has ended.", ephemeral=True)
                return
            balance = session.callout_balance.get(interaction.user.id, 1)
            if balance <= 0:
                await interaction.response.send_message(
                    "⚠️ You're out of callout attempts until your turn comes back around.", ephemeral=True
                )
                return

            try:
                result = engine.challenge_uno_auto(interaction.user.id)
            except ue.UnoError as e:
                await interaction.response.send_message(f"⚠️ {e}", ephemeral=True)
                return

            if result["penalty_to_accuser"]:
                session.callout_balance[interaction.user.id] = balance - 1
                # Not setting session.last_action here — this already gets its
                # own public message right below, and (unlike kick/leave)
                # handle_callout deliberately doesn't repost the table, so the
                # old "callout_miss" last_action used to just sit there and
                # surface later, out of context, on whatever refresh_table call
                # happened to come next (e.g. the periodic chat-activity
                # refresh) — restating a callout that already scrolled by.
                await interaction.response.send_message(
                    f"{_mention(interaction.user.id)} draws {result['n']} cards for a false callout.",
                    ephemeral=False,
                )
            else:
                caught_mentions = ", ".join(_mention(pid) for pid in result["caught"])
                await interaction.response.send_message(
                    f"{caught_mentions} forgot to call UNO and draw {result['n']} cards each.",
                    ephemeral=False,
                )

    async def handle_toggle_arm_uno(self, interaction: discord.Interaction, session: GameSession):
        """
        Handles the Call UNO button/command at both checkpoints:

          2 cards: arm/disarm toggle. Arming it here just sets a flag; the
            actual "UNO!" announcement fires later, as its own plain
            channel message, once they play down to 1 card with it armed
            (see handle_play_card).
          1 card: declares immediately — no arming involved, since there's
            no future checkpoint left to arm for. Sends the public "UNO!"
            announcement right now.
        """
        if not self._is_live(session):
            await interaction.response.send_message("This game has ended.", ephemeral=True)
            return

        engine = session.engine
        if not engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return

        async with session.lock:
            if not self._is_live(session) or session.engine is not engine or engine.finished:
                await interaction.response.send_message("This game has ended.", ephemeral=True)
                return
            player = engine.player_by_id(interaction.user.id)
            if not player or len(player.hand) not in (1, 2):
                await interaction.response.send_message("⚠️ You don't have 1 or 2 cards left.", ephemeral=True)
                return

            if len(player.hand) == 1:
                if player.said_uno:
                    await interaction.response.send_message("You've already called UNO.", ephemeral=True)
                    return
                engine.call_uno(interaction.user.id)
                await interaction.response.send_message("You called UNO!", ephemeral=True)
                await self._send_to_game_channel(session, f"{_mention(interaction.user.id)}: UNO!")
                return

            player.armed_uno = not player.armed_uno
            if player.armed_uno:
                await interaction.response.send_message(
                    "✅ Armed — you'll auto-call UNO when you play down to 1 card.", ephemeral=True
                )
            else:
                await interaction.response.send_message("❌ Un-armed.", ephemeral=True)

    async def send_hand_view(self, interaction: discord.Interaction, session: GameSession, player_id: int):
        """View-only hand: stitched card images plus a plain text list as
        backup. No buttons here on purpose — this is just for looking,
        Play is where the actual tap-to-play buttons live. EXCEPT: at
        exactly 2 cards, the Call UNO arm/disarm toggle shows up here."""
        engine = session.engine
        player = engine.player_by_id(player_id) if engine else None
        if not player:
            await interaction.response.send_message("You're not in this game.", ephemeral=True)
            return

        hand = sort_hand(list(player.hand))
        text_list = ", ".join(hand) or "_empty_"
        content = f"**Your hand ({len(hand)}):**\n{text_list}"

        view = CallUnoToggleView(self, session, player_id) if len(hand) in (1, 2) else None

        # Image composition is real work (PIL, potentially large hands) —
        # defer first so it can never blow the ack window, then follow up.
        try:
            await interaction.response.defer(ephemeral=True)
        except discord.InteractionResponded:
            log.warning("uno_hand: interaction already responded before defer (double-click?)")
            return

        kwargs = {"content": content, "ephemeral": True}
        if view is not None:
            kwargs["view"] = view

        try:
            kwargs["file"] = discord.File(io.BytesIO(await self.hand_png(hand)), filename="hand.png")
        except FileNotFoundError:
            pass  # missing card art for something in hand — text list still works fine
        except ValueError:
            pass  # empty hand — compose_hand_image can't build an image for zero cards
        except Exception:
            # Anything else (corrupt image data, PIL error, etc.) — log it
            # rather than letting the whole /uno hand call die silently;
            # the text list is still a perfectly usable fallback.
            log.exception("compose_hand_image failed for user %s", player_id)

        try:
            await interaction.followup.send(**kwargs)
        except discord.HTTPException:
            log.exception("Failed to send hand view followup for user %s", player_id)

    # ---------------- turn timer ----------------

    def restart_timer(self, session: GameSession):
        session.cancel_timer()
        # Bump the generation unconditionally (even if the game just ended)
        # so any timer task still in flight from a previous turn recognizes
        # itself as stale and no-ops instead of double-acting.
        session.turn_generation += 1
        if session.engine and not session.engine.finished:
            session.turn_started_at = time.time()

            # Refresh the new current player's callout balance back to 1 —
            # this is what "1 false callout, refreshes on your own turn"
            # means: it only resets when it actually becomes their turn
            # again, not on every action in general.
            if session.engine.players:
                current_id = session.engine.current_player().player_id
                if current_id != session._last_turn_player_id:
                    session.callout_balance[current_id] = 1
                    session._last_turn_player_id = current_id

            generation = session.turn_generation
            session.timer_task = safe_task(
                self._timer_loop(session, generation), name=f"uno-timer-{session.channel_id}"
            )

    async def _send_to_game_channel(self, session: GameSession, content: str):
        """
        Sends a plain message to the SESSION's actual game channel — not
        wherever an interaction happened to come from. Matters once a
        button click can originate from a DM'd play-menu copy rather than
        the in-server table message.
        """
        channel = self.bot.get_channel(session.channel_id)
        if channel:
            try:
                await channel.send(content)
            except discord.HTTPException:
                pass

    async def _timer_loop(self, session: GameSession, generation: int):
        """
        Per-turn AFK timer. `generation` is the value session.turn_generation
        held when this timer was scheduled (see restart_timer) — any manual
        action (play/draw/pass/kick/etc) bumps the counter, so if it's moved
        on by the time this wakes up, some other action already resolved
        this turn and this timer does nothing instead of double-acting.

        Also resilient by construction: an unexpected error while
        auto-acting on the AFK player is logged and still leads to a fresh
        timer being scheduled (previously such an error would just kill
        this task permanently, with nothing left to ever re-trigger a
        timeout in this channel again), and finished-game cleanup always
        happens even if the table re-render that follows it fails.
        """
        try:
            await asyncio.sleep(session.turn_timeout)
        except asyncio.CancelledError:
            return

        if session.channel_id not in self.sessions:
            return

        engine = session.engine
        if not engine or engine.finished or session.turn_generation != generation:
            return

        finished = False
        try:
            async with session.lock:
                if session.turn_generation != generation or engine.finished:
                    return  # resolved by something else while we waited on the lock

                if engine.pending_wd4_challenge:
                    # Special case: the "current player" right now is nominally
                    # still the person who played the Wild Draw Four — the actual
                    # pending decision belongs to the target. If they go AFK on
                    # that decision, default to auto-accepting on their behalf
                    # rather than running the normal timeout/kick logic against
                    # the wrong person.
                    target_id = engine.pending_wd4_challenge["target_id"]
                    engine.accept_wild_draw4(target_id)
                    session.last_action = {"type": "wd4_timeout_accept", "actor_id": target_id}
                else:
                    result = engine.record_timeout()
                    if result["kicked"]:
                        session.last_action = {"type": "timeout_kick", "actor_id": result["player_id"]}
                    else:
                        session.last_action = {"type": "timeout_draw", "actor_id": result["player_id"]}

                finished = engine.finished
        except Exception:
            log.exception("Turn timeout auto-action failed for channel %s", session.channel_id)
            if session.channel_id in self.sessions and engine and not engine.finished:
                self.restart_timer(session)
            return

        if finished:
            # Cleanup MUST happen regardless of whether this render succeeds
            # — a finished game should never sit stuck in self.sessions just
            # because a Discord API call failed.
            try:
                await self.refresh_table(session)
            except Exception:
                log.exception("Failed to render final table for channel %s", session.channel_id)
            await self.announce_and_cleanup(session)
            return

        self.restart_timer(session)
        try:
            await self.refresh_table(session)
        except Exception:
            log.exception("Post-timeout table refresh failed for channel %s", session.channel_id)

    # ---------------- commands ----------------

    @uno.command(name="open", description="Open a new UNO table in this channel")
    async def start(self, interaction: discord.Interaction):
        # Guards the check-then-insert below — without this, two /uno open
        # calls landing in the same channel within the same event-loop tick
        # (e.g. two people double-clicking, or a client retry) can both
        # read self.sessions before either has inserted into it, and both
        # go on to create a table. Whichever one loses the lock's ordering
        # will re-check afterward and see the other's session already
        # there.
        async with self._start_locks[interaction.channel_id]:
            if interaction.channel_id in self.sessions:
                await interaction.response.send_message("There's already a table open in this channel.", ephemeral=True)
                return

            settings = await db.get_settings(interaction.guild_id)
            session = GameSession(interaction.guild_id, interaction.channel_id, interaction.user.id,
                                  interaction.user.display_name, settings)
            self.sessions[interaction.channel_id] = session
            await self._log_table(session, "open", interaction.user.id,
                                  f"min_bet={session.min_bet} max_players={session.max_players} decks={session.num_decks}")

            await interaction.response.send_message(embed=build_panel_embed(session), view=TablePanelView(self, session))
            msg = await interaction.original_response()
            try:
                # A plain channel message rather than the interaction's
                # webhook message — those can only be edited for 15 minutes
                # and this panel gets edited for as long as the lobby is open.
                session.lobby_message = await interaction.channel.fetch_message(msg.id)
            except discord.HTTPException:
                session.lobby_message = msg

    @uno.command(name="refresh", description="Repost the table and join panel at the bottom of the channel")
    async def refresh_cmd(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        if not session or session.closed:
            await interaction.response.send_message("No table open here.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with session.lock:
            # Always the join / bet panel too (mid-round it lets new players
            # take a seat for the next round) — same as poker's /poker table
            # putting the whole table back at the bottom.
            await self.post_panel(session, interaction.channel)
            if session.state == "playing" and session.engine:
                await self.post_table(session, interaction.channel)
        await interaction.followup.send("Table refreshed.", ephemeral=True)

    @uno.command(name="table", description="Show turn order — who's up next, top to bottom")
    async def table_cmd(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        if not session or not session.engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return

        engine = session.engine
        order = list(engine.players)
        if engine.direction == -1:
            order = list(reversed(order))

        current_id = engine.current_player().player_id if engine.players else None
        lines = []
        for p in order:
            marker = "<:uno_arrow:1536135499566157956>" if p.player_id == current_id else "・"
            uno_tag = " <:uno:1536135137232953451>" if len(p.hand) == 1 and p.said_uno else ""
            lines.append(f"{marker} {_mention(p.player_id)} — {len(p.hand)} card{'s' if len(p.hand) != 1 else ''}{uno_tag}")

        if engine.finishers:
            placed = ", ".join(
                f"{PLACEMENT_EMOJI.get(i, f'{i+1}.')} {_mention(pid)}"
                for i, pid in enumerate(engine.finishers)
            )
            lines.append(f"\n<:uno_trophy:1536135332863676548> Finished: {placed}")

        embed = discord.Embed(
            title="Turn Order",
            description="\n".join(lines) or "_no active players_",
            color=discord.Color.blurple(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @uno.command(name="play", description="Play a card by name, e.g. Red_4, Blue Skip, Wild Draw 4")
    @app_commands.describe(card="The card to play", color="Color to declare if this is a Wild (optional)")
    async def play_cmd(self, interaction: discord.Interaction, card: str, color: str | None = None):
        session = self.sessions.get(interaction.channel_id)
        if not session or not session.engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return

        parsed = ue.parse_card_input(card)
        if parsed is None:
            await interaction.response.send_message(
                f"⚠️ Couldn't understand `{card}`. Try formats like `Red_4`, `Blue Skip`, `Wild Draw 4`.", ephemeral=True
            )
            return

        if ue.is_wild(parsed) and color:
            engine = session.engine
            color_norm = color.strip().capitalize()
            if color_norm not in ue.COLORS:
                await interaction.response.send_message("⚠️ Color must be Red/Yellow/Green/Blue.", ephemeral=True)
                return
            async with session.lock:
                if engine.current_player().player_id != interaction.user.id:
                    await interaction.response.send_message("⚠️ It's not your turn.", ephemeral=True)
                    return
                try:
                    effects = engine.play_card(interaction.user.id, parsed, chosen_color=color_norm)
                except ue.IllegalCard as e:
                    await interaction.response.send_message(f"⚠️ {e}", ephemeral=True)
                    return
                except ue.NotYourTurn:
                    await interaction.response.send_message("⚠️ It's not your turn.", ephemeral=True)
                    return
                engine.record_action(interaction.user.id)

                player = engine.player_by_id(interaction.user.id)
                fired_uno = False
                if player and player.armed_uno:
                    if len(player.hand) == 1:
                        engine.call_uno(interaction.user.id)
                        fired_uno = True
                    player.armed_uno = False

                session.last_action = {"type": "play", "actor_id": interaction.user.id, "card": parsed, "effects": effects}
                await interaction.response.send_message(f"You played **{parsed}** and chose **{color_norm}**.", ephemeral=True)
                self.restart_timer(session)
                await self.refresh_table(session)
                if fired_uno:
                    await self._send_to_game_channel(session, f"{_mention(interaction.user.id)}: UNO!")
                if engine.finished:
                    await self.announce_and_cleanup(session)
            return

        await self.handle_play_card(interaction, session, parsed)

    @uno.command(name="hand", description="View your hand (images + text list)")
    async def hand_cmd(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        if not session or not session.engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        await self.send_hand_view(interaction, session, interaction.user.id)

    @uno.command(name="draw", description="Draw a card on your turn")
    async def draw_cmd(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        if not session or not session.engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        await self.handle_draw_card(interaction, session)

    @uno.command(name="callout", description="Call out anyone at 1 card who hasn't said UNO (2-card penalty on you if nobody's caught)")
    async def callout_cmd(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        if not session or not session.engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        engine = session.engine

        if engine.pending_wd4_challenge:
            async with session.lock:
                pc = engine.pending_wd4_challenge
                if not pc or interaction.user.id != pc["target_id"]:
                    await interaction.response.send_message(
                        f"⚠️ Waiting on {_mention(pc['target_id']) if pc else 'someone'} to accept or challenge the Wild Draw Four.",
                        ephemeral=True,
                    )
                    return
                result = engine.challenge_wild_draw4(interaction.user.id)
                if result["success"]:
                    session.last_action = {"type": "wd4_challenge_win", "actor_id": interaction.user.id,
                                            "penalty_id": result["penalty_to"]}
                    await interaction.response.send_message(
                        "✅ Challenge succeeded! They had a matching card — they draw 4 instead of you.", ephemeral=False
                    )
                else:
                    session.last_action = {"type": "wd4_challenge_lose", "actor_id": interaction.user.id}
                    await interaction.response.send_message(
                        "❌ Challenge failed — they really didn't have a match. You draw 6.", ephemeral=False
                    )
                self.restart_timer(session)
                await self.refresh_table(session)
            return

        await self.handle_callout(interaction, session)

    @uno.command(name="calluno", description="Call UNO — declares immediately at 1 card, arms/disarms at 2 cards")
    async def calluno_cmd(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        if not session or not session.engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        await self.handle_toggle_arm_uno(interaction, session)

    @uno.command(name="close", description="Close the table after the current round ends (event staff only)")
    @app_commands.describe(force="Close RIGHT NOW, voiding the round in progress and refunding every bet")
    async def close_cmd(self, interaction: discord.Interaction, force: bool = False):
        session = self.sessions.get(interaction.channel_id)
        if not session or session.closed:
            await interaction.response.send_message("No table open here.", ephemeral=True)
            return
        if not await is_event_staff(interaction):
            await interaction.response.send_message("Only event staff can close the table.", ephemeral=True)
            return
        async with session.lock:
            if session.closed or self.sessions.get(interaction.channel_id) is not session:
                await interaction.response.send_message("No table open here.", ephemeral=True)
                return
            engine = session.engine
            round_live = session.state == "playing" and engine is not None and not engine.finished
            # Nothing is at risk outside a live round (or when the round is
            # stuck unsettled), and `force` is the explicit emergency exit.
            if force or not round_live:
                await interaction.response.defer(ephemeral=True)
                await self.close_table(session, "Table closed by staff — everything on the table was refunded:")
                await interaction.followup.send("Table closed.", ephemeral=True)
                return
            session.close_queued = not session.close_queued
            self._save_session(session)
            queued = session.close_queued
            await interaction.response.send_message(
                "🔒 Table will close when the current round ends — everyone's chips go back to their wallets. "
                "Run `/uno close` again to cancel." if queued else
                "🔓 Close cancelled — the table will keep running.", ephemeral=True)
        channel = interaction.channel
        if channel:
            try:
                await channel.send(
                    "🔒 **This table will close after the current round.**" if queued
                    else "🔓 **Table close cancelled — play continues.**")
            except discord.HTTPException:
                pass

    @uno.command(name="kick", description="Remove a player from the table (event staff only)")
    @app_commands.describe(user="The user to remove")
    async def kick(self, interaction: discord.Interaction, user: discord.User):
        session = self.sessions.get(interaction.channel_id)
        if not session:
            await interaction.response.send_message("No table open here.", ephemeral=True)
            return
        if not await is_event_staff(interaction):
            await interaction.response.send_message("Only event staff can kick players.", ephemeral=True)
            return

        target_id = user.id
        async with session.lock:
            seat = session.seats.get(target_id)
            if not seat:
                await interaction.response.send_message("That user isn't seated at this table.", ephemeral=True)
                return
            name = seat.name
            if session.state == "playing" and seat.in_round:
                engine = session.engine
                if not await self._drop_from_round(session, target_id, reason="kicked"):
                    await interaction.response.send_message("That user isn't an active player.", ephemeral=True)
                    return
                # Not setting session.last_action — the send_message below
                # is already the public announcement.
                await interaction.response.send_message(f"Removed **{name}** from the game — their bet is forfeited.")
                if engine.finished:
                    await self.announce_and_cleanup(session)
                else:
                    # Always restart the timer here, not just "if was_current" —
                    # remove_player's turn-advancement can change who's current in
                    # ways a simple pre-check can't detect (e.g. kicking the
                    # target of a pending Wild Draw Four while the WD4 player was
                    # still nominally "current"). Restarting unconditionally is
                    # cheap and always correct.
                    self.restart_timer(session)
                    await self.refresh_table(session)
                return
            amount = await db.table_cash_out(target_id, name)
            del session.seats[target_id]
            await interaction.response.send_message(
                f"Removed **{name}** from the table — **{amount}** {config.UNO_CHIP_EMOJI} returned to their wallet.")
            await self._log_seat(session, target_id, name, "kick", amount, "removed between rounds — returned to wallet")
            await self._after_seat_change(session, interaction.channel)

    @uno.command(name="leave", description="Leave the table after the current round ends")
    async def leave_cmd(self, interaction: discord.Interaction):
        await self.handle_leave(interaction)

    @uno.command(name="quit", description="Leave the table right now — forfeits your bet if a round is in progress")
    async def quit_cmd(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        uid = interaction.user.id
        if not session or uid not in session.seats:
            await interaction.response.send_message("You're not seated at a table here.", ephemeral=True)
            return
        async with session.lock:
            seat = session.seats.get(uid)
            if not seat:
                await interaction.response.send_message("You're not seated at a table here.", ephemeral=True)
                return
            if session.state == "playing" and seat.in_round:
                if seat.quit:
                    await interaction.response.send_message("You're already out of this round.", ephemeral=True)
                    return
                engine = session.engine
                if not await self._drop_from_round(session, uid, reason="quit"):
                    await interaction.response.send_message("This round has already ended.", ephemeral=True)
                    return
                await interaction.response.send_message(
                    f"{_mention(uid)} quit the table — their bet is forfeited.",
                    allowed_mentions=discord.AllowedMentions.none())
                if engine.finished:
                    await self.announce_and_cleanup(session)
                else:
                    self.restart_timer(session)
                    await self.refresh_table(session)
                return
            # Nothing is at risk between rounds, so quitting is just leaving.
            amount = await db.table_cash_out(uid, seat.name)
            del session.seats[uid]
            await self._log_seat(session, uid, seat.name, "quit", amount, "between rounds — returned to wallet")
            await interaction.response.send_message(
                f"You left the table — **{amount}** {config.UNO_CHIP_EMOJI} returned to your wallet.", ephemeral=True)
            await self._after_seat_change(session, interaction.channel)

    @uno.command(name="tip", description="Tip the dealer")
    @app_commands.describe(amount="How many chips to tip? (e.g. 50, 1k)")
    async def tip_cmd(self, interaction: discord.Interaction, amount: str):
        await interaction.response.defer(ephemeral=False)
        tip = _parse_chips(amount)
        if tip is None or tip <= 0:
            await interaction.followup.send("❌ Enter a valid amount greater than 0.", ephemeral=True)
            return
        session = self.sessions.get(interaction.channel_id)
        if not session or session.closed:
            await interaction.followup.send("❌ No table in this channel.", ephemeral=True)
            return
        uid = interaction.user.id
        if uid == session.dealer_id:
            await interaction.followup.send("❌ You can't tip yourself.", ephemeral=True)
            return

        async with session.lock:
            seat = session.seats.get(uid)
            wallet_bal = await db.get_balance(uid)
            # Wallet first, then (between rounds only) the bet on the table —
            # same order poker uses.
            from_table = tip - min(tip, wallet_bal)
            pulled = False
            if from_table > 0:
                if not seat:
                    await interaction.followup.send(f"❌ Not enough chips. Wallet: **{wallet_bal}**.", ephemeral=True)
                    return
                if session.state == "playing" and seat.in_round:
                    await interaction.followup.send(
                        "❌ You can't tip chips from the table while a round is in progress. Wait for it to finish.",
                        ephemeral=True)
                    return
                if seat.bet - from_table < session.min_bet:
                    await interaction.followup.send(
                        f"❌ Not enough chips — tipping that much from the table would drop your bet below the "
                        f"**{session.min_bet}** minimum. Wallet: **{wallet_bal}**, table: **{seat.bet}**.",
                        ephemeral=True)
                    return
                pulled = True

            dealer_name = session.dealer_name
            ok, table_amt = await db.tip_transfer(
                uid, interaction.user.display_name, session.dealer_id, dealer_name, tip,
                from_table=from_table, table_id=session.table_id, round_num=session.round_num)
            if not ok:
                if pulled:  # resync to the DB's authoritative table amount (nothing was applied)
                    seat.bet = table_amt
                await interaction.followup.send("❌ Not enough chips (balance changed). Try again.", ephemeral=True)
                return
            if pulled:
                seat.bet = table_amt
                self._save_session(session)
                await self._log_seat(session, uid, interaction.user.display_name, "change_bet", seat.bet,
                                     f"tip pulled {from_table} from the table")
                await self.refresh_panel(session, create=False)

        await interaction.followup.send(
            f"💸 **{interaction.user.display_name}** tipped **{tip}** {config.UNO_CHIP_EMOJI} to **{dealer_name}**!")

    @uno.command(name="tipleaders", description="Top generous players by total chips tipped")
    async def tipleaders(self, interaction: discord.Interaction):
        await interaction.response.defer()
        rows = await db.get_tip_leaderboard(10)
        if not rows:
            await interaction.followup.send("No tips recorded yet! Be the first to tip the dealer!", ephemeral=True)
            return
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        lines = ["```", f"{'':4}{'Player':<16} {'Tipped':>7}", "─" * 34]
        for i, r in enumerate(rows, start=1):
            you = " ◀" if r["user_id"] == interaction.user.id else ""
            lines.append(f"{medals.get(i, f'{i}. '):<4}{r['username'][:16]:<16} {r['total_tipped']:>7,}{you}")
        lines.append("```")
        embed = discord.Embed(title="💸 Top Tippers Leaderboard", description="\n".join(lines), color=0xF1C40F)
        await interaction.followup.send(embed=embed)

    @unomgr.command(name="setdealer", description="[Manager] Change the dealer (who receives tips) for this table")
    @app_commands.describe(user="The new dealer")
    async def set_dealer(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.defer(ephemeral=False)
        if not await is_uno_manager(interaction):
            await interaction.followup.send("❌ UNO Managers only.", ephemeral=True)
            return
        session = self.sessions.get(interaction.channel_id)
        if not session or session.closed:
            await interaction.followup.send("❌ No table in this channel.", ephemeral=True)
            return
        old_dealer = session.dealer_id
        session.dealer_id = user.id
        session.dealer_name = user.name
        await self._log_table(session, "dealer", interaction.user.id, f"{old_dealer} -> {user.id}")
        self._save_session(session)
        await self.refresh_panel(session, create=False)
        await interaction.followup.send(f"🔄 **{user.mention}** has taken over as the dealer! All new tips will go to them.")

    @unomgr.command(name="addchips", description="[Manager] Add chips to a player's UNO wallet")
    @app_commands.describe(user="Player", amount="Chips to add", note="Optional reason")
    async def addchips(self, interaction: discord.Interaction, user: discord.Member, amount: int, note: str = ""):
        await interaction.response.defer(ephemeral=False)
        if not await is_uno_manager(interaction):
            await interaction.followup.send("❌ UNO Managers only.", ephemeral=True)
            return
        if config.UNO_ADD_CHIPS_CHANNELS and interaction.channel_id not in config.UNO_ADD_CHIPS_CHANNELS:
            await interaction.followup.send("❌ This command can't be used in this channel.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)
            return

        new_bal = await db.add_chips(interaction.user.id, interaction.user.display_name,
                                      user.id, user.display_name, amount, note)
        desc = f"Staff Add: {note}" if note else "Staff Add"
        await db.log_currency_event(user.id, "Cash In", amount, desc)

        await interaction.followup.send(
            f"✅ **+{amount}** chips → **{user.mention}** | Balance: **{new_bal}** {config.UNO_CHIP_EMOJI}"
            + (f"\n> {note}" if note else ""), allowed_mentions=discord.AllowedMentions(users=True))

    @unomgr.command(name="removechips", description="[Manager] Remove chips from a player's UNO wallet")
    @app_commands.describe(user="Player", amount="Chips to remove", note="Optional reason")
    async def removechips(self, interaction: discord.Interaction, user: discord.Member, amount: int, note: str = ""):
        await interaction.response.defer(ephemeral=False)
        if not await is_uno_manager(interaction):
            await interaction.followup.send("❌ UNO Managers only.", ephemeral=True)
            return
        if config.UNO_REMOVE_CHIPS_CHANNELS and interaction.channel_id not in config.UNO_REMOVE_CHIPS_CHANNELS:
            await interaction.followup.send("❌ This command can't be used in this channel.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)
            return

        bal_before = await db.get_balance(user.id)
        if amount > bal_before:
            await interaction.followup.send(
                f"❌ **{user.display_name}** only has **{bal_before}** {config.UNO_CHIP_EMOJI} — cannot remove **{amount}**.",
                ephemeral=True)
            return

        new_bal = await db.add_chips(interaction.user.id, interaction.user.display_name,
                                      user.id, user.display_name, -amount, note)
        desc = f"Staff Remove: {note}" if note else "Staff Remove"
        await db.log_currency_event(user.id, "Cash Out", -amount, desc)

        await interaction.followup.send(
            f"✅ **-{amount}** chips from **{user.mention}** | Balance: **{new_bal}** {config.UNO_CHIP_EMOJI}"
            + (f"\n> {note}" if note else ""), allowed_mentions=discord.AllowedMentions(users=True))

    @unomgr.command(name="ban", description="[Manager] Ban a user from joining UNO tables in this server")
    @app_commands.describe(user="Player to ban", reason="Optional reason")
    async def ban_cmd(self, interaction: discord.Interaction, user: discord.Member, reason: str = ""):
        if not await is_uno_manager(interaction):
            await interaction.response.send_message("❌ UNO Managers only.", ephemeral=True)
            return
        ok = await db.ban_player(interaction.guild_id, user.id, user.display_name, interaction.user.id)
        if not ok:
            await interaction.response.send_message(f"**{user.display_name}** is already banned.", ephemeral=True)
            return
        await interaction.response.send_message(
            f"🚫 Banned **{user.mention}** from UNO." + (f"\n> {reason}" if reason else ""),
            allowed_mentions=discord.AllowedMentions(users=True))

    @unomgr.command(name="unban", description="[Manager] Unban a user from UNO")
    @app_commands.describe(user="Player to unban")
    async def unban_cmd(self, interaction: discord.Interaction, user: discord.Member):
        if not await is_uno_manager(interaction):
            await interaction.response.send_message("❌ UNO Managers only.", ephemeral=True)
            return
        existed = await db.unban_player(interaction.guild_id, user.id)
        if not existed:
            await interaction.response.send_message(f"**{user.display_name}** wasn't banned.", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Unbanned **{user.mention}**.",
                                                  allowed_mentions=discord.AllowedMentions(users=True))

    @unomgr.command(name="bans", description="[Manager] List all currently banned UNO players")
    async def bans_cmd(self, interaction: discord.Interaction):
        if not await is_uno_manager(interaction):
            await interaction.response.send_message("❌ UNO Managers only.", ephemeral=True)
            return
        bans = await db.get_all_bans(interaction.guild_id)
        if not bans:
            await interaction.response.send_message("No one is currently banned from UNO.", ephemeral=True)
            return
        lines = [f"• <@{b['user_id']}> — banned by <@{b['banned_by']}> ({b['ts']})" for b in bans[:25]]
        await interaction.response.send_message(
            "**UNO bans:**\n" + "\n".join(lines), ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none())

    @unomgr.command(name="removestats", description="[Manager] Remove a player from the UNO leaderboard")
    @app_commands.describe(user="Player to remove from leaderboard")
    async def removestats(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.defer(ephemeral=False)
        if not await is_uno_manager(interaction):
            await interaction.followup.send("❌ UNO Managers only.", ephemeral=True)
            return
        removed = await db.delete_player_stats(user.id)
        if removed:
            await interaction.followup.send(f"✅ Removed **{user.name}** ({user.id}) from the UNO leaderboard.")
        else:
            await interaction.followup.send(f"ℹ️ **{user.name}** has no UNO stats on record.", ephemeral=True)

    @unomgr.command(name="pay_cashout", description="[Manager] Deduct a paid cashout from a player's pending balance")
    @app_commands.describe(user="Player who was paid", amount="Amount of chips paid")
    async def pay_cashout(self, interaction: discord.Interaction, user: discord.Member, amount: int):
        await interaction.response.defer(ephemeral=False)
        if not await is_uno_manager(interaction):
            await interaction.followup.send("❌ UNO Managers only.", ephemeral=True)
            return
        if config.UNO_CASHOUT_CHANNEL_ID and interaction.channel_id != config.UNO_CASHOUT_CHANNEL_ID:
            await interaction.followup.send(f"❌ This can only be used in <#{config.UNO_CASHOUT_CHANNEL_ID}>.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)
            return

        ok = await db.pay_cashout(user.id, amount)
        if not ok:
            _, pending = await db.get_wallet(user.id)
            await interaction.followup.send(
                f"❌ **{user.display_name}** only has **{pending}** {config.UNO_CHIP_EMOJI} pending — cannot deduct {amount}.",
                ephemeral=True)
            return

        await interaction.followup.send(
            f"✅ Deducted **{amount}** {config.UNO_CHIP_EMOJI} from **{user.mention}**'s pending cashouts.",
            allowed_mentions=discord.AllowedMentions(users=True))

    @unoset.command(name="settings", description="[Manager] View or change UNO settings — only the options you fill in change")
    @app_commands.describe(
        min_bet="Minimum bet per round",
        max_bet="Maximum bet per round (0 = no limit)",
        turn_timer="Seconds each player gets per turn (30-600)",
        bet_window="Seconds between rounds to join / change bet (10-120)",
        resend_after="Chat messages before the table/panel is reposted at the bottom (1-50)",
        max_players="Seats per table (2-20)",
        decks="Number of decks (1-6)",
        log_channel="Channel for round-by-round summaries",
        manager_role="Role that gets UNO manager access (admin only)",
    )
    async def settings_cmd(
        self, interaction: discord.Interaction,
        min_bet: app_commands.Range[int, 1, 1_000_000] | None = None,
        max_bet: app_commands.Range[int, 0, 100_000_000] | None = None,
        turn_timer: app_commands.Range[int, MIN_TURN_TIMEOUT, MAX_TURN_TIMEOUT] | None = None,
        bet_window: app_commands.Range[int, 10, 120] | None = None,
        resend_after: app_commands.Range[int, 1, 50] | None = None,
        max_players: app_commands.Range[int, MIN_PLAYERS_LIMIT, MAX_PLAYERS_LIMIT] | None = None,
        decks: app_commands.Range[int, MIN_DECKS, MAX_DECKS] | None = None,
        log_channel: discord.TextChannel | None = None,
        manager_role: discord.Role | None = None,
    ):
        if not await is_uno_manager(interaction):
            await interaction.response.send_message("❌ UNO Managers only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=False)

        current = await db.get_settings(interaction.guild_id)
        changes: dict = {}
        applied: list[str] = []

        def want(key, value, label, fmt=str):
            if value is not None and value != current.get(key):
                changes[key] = value
                applied.append(f"{label} → **{fmt(value)}**")

        want("min_bet", min_bet, "Min bet")
        want("max_bet", max_bet, "Max bet", lambda v: str(v) if v else "no limit")
        want("turn_timeout", turn_timer, "Turn timer", lambda v: f"{v}s")
        want("bet_window", bet_window, "Bet window", lambda v: f"{v}s")
        want("resend_after_msgs", resend_after, "Resend after", lambda v: f"{v} messages")
        want("max_players", max_players, "Max players")
        want("num_decks", decks, "Decks")
        if log_channel is not None:
            want("log_channel_id", log_channel.id, "Log channel", lambda v: f"<#{v}>")
        if manager_role is not None:
            if not interaction.user.guild_permissions.administrator:
                await interaction.followup.send("❌ Only a server Administrator can change the manager role.", ephemeral=True)
                return
            want("manager_role_id", manager_role.id, "Manager role", lambda v: f"<@&{v}>")

        merged = {**current, **changes}
        problem = None
        if merged["max_bet"] and merged["min_bet"] > merged["max_bet"]:
            problem = f"Min bet ({merged['min_bet']}) can't be higher than max bet ({merged['max_bet']})."
        elif merged["max_players"] * 7 + 1 > merged["num_decks"] * 108:
            problem = (f"{merged['max_players']} players need more than {merged['num_decks']} deck(s) can deal "
                       f"— raise the decks or lower max players.")
        if problem:
            await interaction.followup.send(f"❌ {problem} Nothing was changed.", ephemeral=True)
            return

        if changes:
            await db.set_settings(interaction.guild_id, **changes)
            current = await db.get_settings(interaction.guild_id)
            # Live tables pick the new values up immediately (a turn already
            # running keeps its current timer; the next one uses the new length)
            for s in list(self.sessions.values()):
                if s.guild_id == interaction.guild_id and not s.closed:
                    s.apply_settings(current)
                    await self._log_table(s, "settings", interaction.user.id, json.dumps(changes))
                    if s.state in ("lobby", "window", "waiting"):
                        await self.refresh_panel(s, create=False)
        text = ("✅ **Applied:** " + ", ".join(applied)) if applied else None
        await interaction.followup.send(content=text, embed=build_settings_embed(current))

    # ── Admin/dev economy commands ──────────────────────────────────────────

    @unoadmin.command(name="economy", description="[Admin] View total UNO chips in circulation")
    async def economy(self, interaction: discord.Interaction):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in config.DEV_USER_IDS):
            await interaction.response.send_message("❌ Server Administrators only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=False)

        avail, pending = await db.get_economy_totals()
        in_play = await db.get_total_chips_in_play()
        total = avail + pending + in_play

        embed = discord.Embed(title="🃏 UNO Economy Dashboard", color=0x2ecc71)
        embed.add_field(name="Available in Wallets", value=f"{avail:,} {config.UNO_CHIP_EMOJI}", inline=False)
        embed.add_field(name="Locked Pending Cashouts", value=f"{pending:,} {config.UNO_CHIP_EMOJI}", inline=False)
        embed.add_field(name="Currently at Tables", value=f"{in_play:,} {config.UNO_CHIP_EMOJI}", inline=False)
        embed.add_field(name="Total Circulation", value=f"**{total:,}** {config.UNO_CHIP_EMOJI}", inline=False)

        await interaction.followup.send(embed=embed)

    @unoadmin.command(name="revenue", description="[Admin] View projected UNO house profits")
    async def revenue(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False)
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in config.DEV_USER_IDS):
            await interaction.followup.send("❌ Server Administrators only.", ephemeral=True)
            return

        stats = await db.get_revenue_stats()

        embed = discord.Embed(title="📈 UNO House Revenue (Inactivity Tax)", color=0xf1c40f)
        embed.add_field(name="Past 24 Hours", value=f"{stats['daily']:,} {config.UNO_CHIP_EMOJI}", inline=True)
        embed.add_field(name="Past 7 Days", value=f"{stats['weekly']:,} {config.UNO_CHIP_EMOJI}", inline=True)
        embed.add_field(name="Past 30 Days", value=f"{stats['monthly']:,} {config.UNO_CHIP_EMOJI}", inline=True)
        embed.add_field(name="All-Time Profit", value=f"**{stats['all_time']:,} **{config.UNO_CHIP_EMOJI}", inline=False)

        await interaction.followup.send(embed=embed)

    @unoadmin.command(name="adjustrevenue", description="[Admin] Manually adjust the all-time UNO revenue tracker")
    @app_commands.describe(amount="Amount to add (or negative to subtract)")
    async def adjustrevenue(self, interaction: discord.Interaction, amount: int):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in config.DEV_USER_IDS):
            await interaction.response.send_message("❌ Server Administrators only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=False)
        await db.log_house_revenue(amount, source="adjustment")
        await interaction.followup.send(f"✅ Adjusted all-time UNO revenue by **{amount:+,}** {config.UNO_CHIP_EMOJI}.")

    @unoadmin.command(name="salt", description="[Admin] View daily UNO house profits in a monthly calendar layout")
    @app_commands.describe(month="YYYY-MM format (e.g. 2026-07) - defaults to current month")
    async def salt(self, interaction: discord.Interaction, month: str = None):
        await interaction.response.defer(ephemeral=False)
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in config.DEV_USER_IDS):
            await interaction.followup.send("❌ Server Administrators only.", ephemeral=True)
            return

        import re
        if month:
            if not re.match(r"^\d{4}-\d{2}$", month):
                await interaction.followup.send("❌ Invalid month format. Use YYYY-MM (e.g. 2026-07).")
                return
            target_year_month = month
        else:
            target_year_month = datetime.utcnow().strftime("%Y-%m")

        year_str, month_str = target_year_month.split("-")
        y_val, m_val = int(year_str), int(month_str)

        db_conn = await db._get_db()
        daily_totals = {}
        try:
            async with db_conn.execute(
                "SELECT ts, amount FROM house_revenue WHERE ts LIKE ?", (f"{target_year_month}%",)
            ) as c:
                rows = await c.fetchall()
                for ts_str, amt in rows:
                    try:
                        day = int(ts_str.split("T")[0].split("-")[2])
                        daily_totals[day] = daily_totals.get(day, 0) + amt
                    except Exception:
                        pass
        except Exception as e:
            log.exception(f"Error querying UNO house revenue: {e}")

        import calendar
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
                return f"{int(val_m)}M" if val_m == int(val_m) else f"{val_m:.1f}M"
            elif val >= 100_000:
                val_k = val / 1_000
                return f"{int(val_k)}K" if val_k == int(val_k) else f"{val_k:.1f}K"
            return str(val)

        bg_color = (19, 19, 26)
        card_bg = (33, 33, 47)
        empty_card_bg = (24, 24, 33)
        header_color = (255, 255, 255)
        text_muted = (130, 130, 160)
        cyan_color = (56, 189, 248)
        green_color = (74, 222, 128)
        grey_color = (110, 120, 140)

        base_dir = os.path.dirname(os.path.abspath(__file__))
        font_path_bold = os.path.join(base_dir, "..", "poker", "assets", "Roboto-Bold.ttf")
        font_path_medium = os.path.join(base_dir, "..", "poker", "assets", "Roboto-Medium.ttf")
        try:
            font_title = ImageFont.truetype(font_path_bold, 34)
            font_header = ImageFont.truetype(font_path_bold, 22)
            font_date = ImageFont.truetype(font_path_medium, 18)
            font_rev = ImageFont.truetype(font_path_bold, 24)
            font_total = ImageFont.truetype(font_path_bold, 26)
        except Exception:
            font_title = font_header = font_date = font_rev = font_total = ImageFont.load_default()

        num_weeks = len(weeks)
        padding, card_w, card_h, gap = 20, 115, 95, 10
        header_h, weekdays_h = 90, 45
        grid_h = card_h * num_weeks + gap * (num_weeks - 1)
        footer_h = 70

        img_w = padding * 2 + card_w * 7 + gap * 6
        img_h = padding + header_h + weekdays_h + grid_h + footer_h + padding

        img = Image.new("RGB", (img_w, img_h), bg_color)
        draw = ImageDraw.Draw(img)

        draw.text((padding, padding + 15), f"UNO Revenue — {month_name} {y_val}", font=font_title, fill=header_color)

        weekdays = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"]
        start_y = padding + header_h
        for idx, day_lbl in enumerate(weekdays):
            x = padding + idx * (card_w + gap)
            try:
                bbox = draw.textbbox((0, 0), day_lbl, font=font_header)
                text_w = bbox[2] - bbox[0]
            except Exception:
                text_w = len(day_lbl) * 12
            draw.text((x + (card_w - text_w) // 2, start_y), day_lbl, font=font_header, fill=text_muted)

        start_grid_y = start_y + weekdays_h
        for row_idx, week in enumerate(weeks):
            y = start_grid_y + row_idx * (card_h + gap)
            for col_idx, day in enumerate(week):
                x = padding + col_idx * (card_w + gap)
                if day == 0:
                    draw.rounded_rectangle([x, y, x + card_w, y + card_h], radius=6, fill=empty_card_bg)
                else:
                    draw.rounded_rectangle([x, y, x + card_w, y + card_h], radius=6, fill=card_bg)
                    draw.text((x + 10, y + 8), f"{day:02d}", font=font_date, fill=cyan_color)
                    rev_val = daily_totals.get(day, 0)
                    rev_str = format_revenue(rev_val)
                    color = green_color if rev_val > 0 else grey_color
                    try:
                        bbox = draw.textbbox((0, 0), rev_str, font=font_rev)
                        text_w = bbox[2] - bbox[0]
                    except Exception:
                        text_w = len(rev_str) * 12
                    draw.text((x + (card_w - text_w) // 2, y + 48), rev_str, font=font_rev, fill=color)

        total_rev = sum(daily_totals.values())
        footer_y = start_grid_y + grid_h + 20
        draw.line([padding, footer_y, img_w - padding, footer_y], fill=(40, 40, 60), width=1)
        draw.text((padding, footer_y + 20), f"Total Monthly Revenue: {total_rev:,} UNO Chips", font=font_total, fill=green_color)

        temp_img_path = f"uno_revenue_{target_year_month}.png"
        img.save(temp_img_path)
        try:
            file = discord.File(temp_img_path, filename=f"uno_revenue_{target_year_month}.png")
            await interaction.followup.send(file=file)
        finally:
            if os.path.exists(temp_img_path):
                os.remove(temp_img_path)

    @unoadmin.command(name="sql", description="[Dev] Run a read-only SQL query against the UNO database")
    @app_commands.describe(query="SELECT statement only")
    async def sql_query(self, interaction: discord.Interaction, query: str):
        if interaction.user.id not in config.DEV_USER_IDS:
            await interaction.response.send_message("❌ Restricted to the bot developer.", ephemeral=True)
            return
        stripped = query.strip().rstrip(";")
        if not stripped.lower().startswith("select"):
            await interaction.response.send_message("❌ Only SELECT queries are allowed.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)
        try:
            conn = await db._get_db()
            async with conn.execute(stripped) as c:
                rows = await c.fetchall()
                columns = [d[0] for d in c.description] if c.description else []
        except Exception as e:
            await interaction.followup.send(f"❌ Query error: {e}", ephemeral=True)
            return

        row_dicts = [dict(r) for r in rows]
        if not row_dicts:
            await interaction.followup.send("✅ Query ran successfully — 0 rows returned.")
            return

        view = RawSQLPaginationView(columns, row_dicts, title=f"UNO SQL: {stripped[:60]}")
        await interaction.followup.send(embed=view.format_page(), view=view)

    @unoadmin.command(name="setstat", description="[Dev] Modify a player's UNO statistics")
    @app_commands.describe(user="The player whose stats you want to change",
                            stat="The specific statistic to modify",
                            value="The new integer value for this stat")
    @app_commands.choices(stat=[
        app_commands.Choice(name="Rounds Played", value="rounds_played"),
        app_commands.Choice(name="Rounds Won", value="rounds_won"),
        app_commands.Choice(name="Chips Won", value="chips_won"),
        app_commands.Choice(name="Chips Lost", value="chips_lost"),
        app_commands.Choice(name="Times Wiped (Inactivity)", value="times_wiped"),
    ])
    async def setstat(self, interaction: discord.Interaction, user: discord.Member,
                       stat: app_commands.Choice[str], value: int):
        if interaction.user.id not in config.DEV_USER_IDS:
            await interaction.response.send_message("❌ Access Denied.", ephemeral=True)
            return
        try:
            conn = await db._get_db()
            async with db._write_lock:
                await conn.execute(f"UPDATE stats SET {stat.value} = ? WHERE user_id = ?", (value, user.id))
                await conn.commit()
            await interaction.response.send_message(
                f"✅ Successfully updated **{stat.name}** to `{value:,}` for **{user.display_name}**!")
        except Exception as e:
            log.exception("setstat failed")
            await interaction.response.send_message(f"❌ Database error: {e}", ephemeral=True)

    async def _send_uno_backup(self, user: discord.User):
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
        clean_zip_name = f"uno_data_backup_{date_str}.zip"
        zip_path = clean_zip_name
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "uno")

        try:
            async with db._write_lock:
                conn = await db._get_db()
                await conn.execute("PRAGMA wal_checkpoint(RESTART)")

            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(data_dir):
                    for file in files:
                        full_path = os.path.join(root, file)
                        zipf.write(full_path, arcname=os.path.relpath(full_path, data_dir))

            with open(zip_path, 'rb') as f:
                await user.send(f"📦 **UNO Database Backup** ({date_str})", file=discord.File(f, filename=clean_zip_name))
        finally:
            if os.path.exists(zip_path):
                os.remove(zip_path)

    @unoadmin.command(name="backup", description="[Dev] Force a UNO database backup to your DMs")
    async def force_backup(self, interaction: discord.Interaction):
        if interaction.user.id not in config.DEV_USER_IDS:
            await interaction.response.send_message("❌ This command is restricted to the bot developer.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            await self._send_uno_backup(interaction.user)
            await interaction.followup.send("✅ Backup sent directly to your DMs!", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send(
                "❌ Failed to send DM. Please check your Discord privacy settings to allow messages from server members.",
                ephemeral=True)
        except Exception as e:
            log.exception("UNO backup failed")
            await interaction.followup.send(f"❌ Backup failed: {e}", ephemeral=True)

    @unoadmin.command(name="resetdb", description="[Admin] Wipe ALL UNO data from the database")
    async def reset_db(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Server Administrator only.", ephemeral=True)
            return
        view = UnoConfirmResetView1(interaction.user.id)
        await interaction.response.send_message(
            "⚠️ **This will permanently delete all UNO wallets, stats, logs and settings.**\nAre you sure?",
            view=view, ephemeral=True)

    @unoadmin.command(name="set_activity", description="[Dev] Change a player's UNO last_activity timestamp")
    @app_commands.describe(user="The player to modify",
                            timestamp="Discord timestamp (e.g. <t:1716388800:R>) or raw Unix epoch")
    async def set_activity(self, interaction: discord.Interaction, user: discord.Member, timestamp: str):
        if interaction.user.id not in config.DEV_USER_IDS:
            await interaction.response.send_message("❌ Access Denied. Devs only.", ephemeral=True)
            return

        import re
        match = re.search(r"<t:(\d+)", timestamp)
        if match:
            unix_ts = int(match.group(1))
        elif timestamp.isdigit():
            unix_ts = int(timestamp)
        else:
            await interaction.response.send_message(
                "❌ Invalid format. Use a Discord timestamp like `<t:1716388800>` or a raw Unix epoch.",
                ephemeral=True)
            return

        try:
            new_iso = datetime.utcfromtimestamp(unix_ts).isoformat()
        except (ValueError, OSError, OverflowError) as e:
            await interaction.response.send_message(f"❌ Failed to parse date: {e}", ephemeral=True)
            return

        conn = await db._get_db()
        try:
            async with db._write_lock:
                await conn.execute("UPDATE wallets SET last_activity = ? WHERE user_id = ?", (new_iso, user.id))
                await conn.commit()
            await interaction.response.send_message(
                f"✅ Backdated **{user.display_name}**'s UNO activity to <t:{unix_ts}:F>!\n"
                f"*(Saved as:* `{new_iso}`*)*", ephemeral=True)
        except Exception as e:
            log.exception("set_activity failed")
            await interaction.response.send_message(f"❌ Database error: {e}", ephemeral=True)

    @unoadmin.command(name="check_inactive", description="[Admin] Check who will be wiped soon")
    async def check_inactive(self, interaction: discord.Interaction):
        if not (interaction.user.guild_permissions.administrator or interaction.user.id in config.DEV_USER_IDS):
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=False)

        at_risk = await db.get_players_at_risk()
        inactive = await db.get_inactive_players()

        embed = discord.Embed(title="🔍 UNO Inactivity Report", color=0xe74c3c)

        if at_risk:
            risk_lines = [
                f"• **{p['username']}**: {p['balance']} chips ({p.get('days_inactive', 0)}d ago, {p['recent_rounds']} rounds)"
                for p in at_risk[:15]
            ]
            embed.add_field(name=f"⚠️ At Risk - Wiping in <24h ({len(at_risk)} players)",
                             value="\n".join(risk_lines), inline=False)

        if inactive:
            inactive_lines = [
                f"• **{p['username']}**: {p['balance']} chips ({p.get('days_inactive', 0)}d ago, {p['recent_rounds']} rounds)"
                for p in inactive[:10]
            ]
            embed.add_field(name=f"💀 Will Be Wiped Next Run ({len(inactive)} players)",
                             value="\n".join(inactive_lines), inline=False)

        if not at_risk and not inactive:
            embed.description = "✅ All players are active! No chips will be wiped."

        await interaction.followup.send(embed=embed)

    @unoadmin.command(name="force_wipe_inactive_players", description="[Admin] Manually trigger UNO inactivity wipe NOW")
    async def force_wipe(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=False)

        wiped = await db.wipe_inactive_players()
        if not wiped:
            await interaction.followup.send("✅ No inactive players found. Nothing to wipe!")
            return

        summary = "\n".join(
            f"• **{w['username']}**: {w['amount_wiped']} chips "
            f"(rounds: {w['recent_rounds']}, wagered: {w['recent_chips_wagered']})"
            for w in wiped[:20]
        )
        await interaction.followup.send(f"🧹 **Wiped {len(wiped)} inactive UNO player(s):**\n{summary}")

    @unoadmin.command(name="recoverchips",
                       description="[Admin] Manually refund + clear chips_in_play (only for tables that can't be resumed)")
    async def recover_chips(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Administrators only.", ephemeral=True)
            return

        detail = await db.get_chips_in_play_detail()
        if not detail:
            await interaction.response.send_message("✅ Nothing locked in chips_in_play right now.", ephemeral=True)
            return

        lines = "\n".join(f"• **{r['username']}**: {r['amount']}{config.UNO_CHIP_EMOJI}" for r in detail[:20])
        active_channels = ", ".join(f"<#{cid}>" for cid in self.sessions) or "none"
        view = UnoRecoverChipsView(interaction.user.id)
        await interaction.response.send_message(
            f"⚠️ This refunds and clears **every** locked UNO bet below, all at once — there's no way to "
            f"target one channel. **Check `/uno resume` works in every channel that should keep its game "
            f"first** — running this on a table that gets resumed afterward double-credits whoever wins it.\n\n"
            f"Currently active UNO tables in this server: {active_channels}\n\n"
            f"**Will refund:**\n{lines}",
            view=view, ephemeral=True,
        )

    # ── Player-facing stats/leaderboard ─────────────────────────────────────
    # Note: poker's /poker leaderboard renders a PIL/Twemoji graphic
    # (poker/leaderboard_image.py) that depends on bundled font + emoji PNG
    # assets. Those assets aren't part of this port, so these render as
    # plain embeds instead — same underlying data/ranking, simpler display.

    @uno.command(name="wallet", description="Check your UNO chip wallet balance")
    @app_commands.describe(user="Player to check (leave blank for yourself)")
    async def wallet(self, interaction: discord.Interaction, user: discord.Member = None):
        await interaction.response.defer(ephemeral=False)
        target = user or interaction.user
        bal, pending = await db.get_wallet(target.id)

        session = self.sessions.get(interaction.channel_id)
        table_str = ""
        if session and target.id in session.seats:
            table_str = f"\n**At table:** {session.seats[target.id].bet} {config.UNO_CHIP_EMOJI}"
        pending_str = f"\n**Pending Cashout:** 🔒 {pending} {config.UNO_CHIP_EMOJI}" if pending > 0 else ""

        label = f"**{target.display_name}'s UNO Wallet**" if user else "**Your UNO Wallet**"
        await interaction.followup.send(f"{label}: {bal} {config.UNO_CHIP_EMOJI}{table_str}{pending_str}", ephemeral=False)

    @uno.command(name="myactivity", description="Check your UNO activity status and wipe risk")
    @app_commands.describe(user="Player to check (Admins/Devs only, leave blank for yourself)")
    async def myactivity(self, interaction: discord.Interaction, user: discord.Member = None):
        target = user or interaction.user
        if target.id != interaction.user.id:
            if not (interaction.user.guild_permissions.administrator or interaction.user.id in config.DEV_USER_IDS):
                await interaction.response.send_message(
                    "❌ Only Administrators or Devs can check other players' activity.", ephemeral=True)
                return

        await interaction.response.defer(ephemeral=True)
        stats = await db.get_player_activity_stats(target.id)
        if not stats:
            name_str = "You don't" if target.id == interaction.user.id else f"**{target.display_name}** doesn't"
            await interaction.followup.send(f"❌ {name_str} have a UNO wallet yet!", ephemeral=True)
            return

        last_active = datetime.fromisoformat(stats["last_activity"]).replace(tzinfo=_tz.utc)
        exact_expiration = last_active + timedelta(days=config.UNO_INACTIVITY_DAYS)
        # Snap to the next scheduled UNO wipe run (03:45 UTC)
        if exact_expiration.hour < 3 or (exact_expiration.hour == 3 and exact_expiration.minute <= 45):
            wipe_date = exact_expiration.replace(hour=3, minute=45, second=0, microsecond=0)
        else:
            wipe_date = (exact_expiration + timedelta(days=1)).replace(hour=3, minute=45, second=0, microsecond=0)
        wipe_timestamp = int(wipe_date.timestamp())

        embed = discord.Embed(title=f"📊 UNO Activity Status: {stats['username']}", color=0x3498db)
        total_chips = stats["balance"] + stats["pending_cashout"]
        embed.add_field(name="💰 Total Chips", value=f"{total_chips:,} chips", inline=True)
        embed.add_field(name="📅 Last Active", value=f"<t:{int(last_active.timestamp())}:R>", inline=True)
        if stats["days_until_wipe"] > 0:
            embed.add_field(name="⏰ Chips Wiped", value=f"<t:{wipe_timestamp}:R>", inline=True)
        else:
            embed.add_field(name="⏰ Chips Wiped", value="**Next cleanup run!**", inline=True)

        def progress_bar(current: int, required: int, length: int = 10) -> str:
            filled = min(int((current / max(required, 1)) * length), length)
            done = "🟩" * filled
            empty = "⬜" * (length - filled)
            pct = min(int((current / max(required, 1)) * 100), 100)
            return f"{done}{empty}  **{current}/{required}** ({pct}%)"

        rounds_bar = progress_bar(stats["recent_rounds"], config.UNO_MIN_ROUNDS_PER_PERIOD)
        rounds_status = "✅" if stats["meets_rounds_requirement"] else "❌"
        embed.add_field(name=f"🃏 Rounds Played {rounds_status}", value=rounds_bar, inline=False)

        if config.UNO_MIN_CHIPS_WAGERED > 0:
            wager_bar = progress_bar(stats["recent_chips_wagered"], config.UNO_MIN_CHIPS_WAGERED)
            wager_status = "✅" if stats["meets_wager_requirement"] else "❌"
            embed.add_field(name=f"💵 Chips Wagered {wager_status}", value=wager_bar, inline=False)

        await interaction.followup.send(embed=embed, ephemeral=True)

    @uno.command(name="stats", description="View your UNO stats")
    @app_commands.describe(hidden="Hide the stats message from others? (Default: False)")
    async def stats(self, interaction: discord.Interaction, hidden: bool = False):
        await interaction.response.defer(ephemeral=hidden)
        row = await db.get_player_stats(interaction.user.id)
        if not row:
            await interaction.followup.send("No UNO stats yet!", ephemeral=hidden)
            return

        rank = await db.get_player_rank(interaction.user.id)
        rank_str = f"#{rank}" if rank else "Unranked"

        net = row["net_chips"]
        wp = f"{row['rounds_won'] / row['rounds_played'] * 100:.1f}%" if row["rounds_played"] else "—"

        embed = discord.Embed(
            title=f"UNO Stats — {row['username']}",
            color=0x2ecc71 if net > 0 else (0xe74c3c if net < 0 else 0xFFFFFF),
        )
        embed.add_field(name="Rank", value=rank_str, inline=True)
        embed.add_field(name="Rounds Played", value=str(row["rounds_played"]), inline=True)
        embed.add_field(name="Win %", value=wp, inline=True)
        embed.add_field(name="Net Chips", value=f"{'+' if net >= 0 else ''}{net:,} {config.UNO_CHIP_EMOJI}", inline=True)
        embed.add_field(name="Wallet Balance", value=f"{row['wallet']:,} {config.UNO_CHIP_EMOJI}", inline=True)
        if row.get("times_wiped", 0) > 0:
            embed.add_field(name="Times Wiped", value=str(row["times_wiped"]), inline=True)

        await interaction.followup.send(embed=embed, ephemeral=hidden)

    @uno.command(name="leaderboard", description="Top UNO players by net chips")
    async def leaderboard(self, interaction: discord.Interaction):
        await interaction.response.defer()
        rows = await db.get_leaderboard(10)
        if not rows:
            await interaction.followup.send("No UNO stats yet!", ephemeral=True)
            return

        caller_row = await db.get_player_stats(interaction.user.id)
        caller_rank = await db.get_player_rank(interaction.user.id) if caller_row else None

        # Offload the image generation to a background thread to prevent blocking
        image_bytes = await asyncio.to_thread(
            generate_leaderboard_image,
            rows,
            interaction.user.id,
            caller_row,
            caller_rank,
            title="UNO Leaderboard"
        )

        file = discord.File(image_bytes, filename="uno_leaderboard.png")

        container = discord.ui.Container(
            discord.ui.MediaGallery(
                discord.MediaGalleryItem(
                    "attachment://uno_leaderboard.png"
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

    @uno.command(name="request_cashout", description="Lock UNO chips for withdrawal and notify staff")
    @app_commands.describe(amount="Chips to cash out (e.g. 500, 2k)", note="Optional notes")
    async def request_cashout(self, interaction: discord.Interaction, amount: str,
                               note: app_commands.Range[str, 0, 50] = ""):
        await interaction.response.defer(ephemeral=True)
        chips = _parse_chips(amount)
        if chips is None or chips <= 0:
            await interaction.followup.send("❌ Enter a valid amount (e.g. 500, 2k).", ephemeral=True)
            return

        bal, _ = await db.get_wallet(interaction.user.id)
        if chips > bal:
            await interaction.followup.send(
                f"❌ You only have **{bal} **{config.UNO_CHIP_EMOJI} available. "
                f"(Chips currently on a table aren't cashable until it settles.)", ephemeral=True)
            return

        ok = await db.request_cashout(interaction.user.id, chips)
        if not ok:
            await interaction.followup.send("❌ Failed to process cashout.", ephemeral=True)
            return

        desc = f"Requested Cashout: {note}" if note else "Requested Cashout"
        await db.log_currency_event(interaction.user.id, "Cash Out", -chips, desc)

        if config.UNO_CASHOUT_CHANNEL_ID:
            try:
                ch = interaction.guild.get_channel(config.UNO_CASHOUT_CHANNEL_ID)
                if ch:
                    # Format mirrors poker's cashout ticket exactly — same
                    # regex-based ✅-reaction payout handler in bot.py reads
                    # both, keyed only by which channel the reaction fired in.
                    ticket_msg = f"**Username:** {interaction.user.mention}\n**Amount:** {chips} {config.UNO_CHIP_EMOJI}"
                    if note:
                        ticket_msg += f"\n**Notes:** {note}"
                    ticket = await ch.send(ticket_msg)
            except Exception:
                log.exception("Failed to post UNO cashout ticket")

        await interaction.followup.send(
            f"✅ Locked **{chips}** {config.UNO_CHIP_EMOJI} for cashout. Staff have been notified""", ephemeral=True)

    @uno.command(name="currencylog", description="View recent UNO chip transactions")
    @app_commands.describe(user="Player to check (managers only, leave blank for yourself)",
                            minimum="Only show transactions with this many chips or more")
    async def currencylog(self, interaction: discord.Interaction, user: discord.Member = None, minimum: int = None):
        target = user or interaction.user
        if target.id != interaction.user.id and not await is_uno_manager(interaction):
            await interaction.response.send_message("❌ UNO Managers only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        logs = await db.get_currency_logs(target.id)
        view = UnoCurrencyLogView(caller=interaction.user, target=target, logs=logs, minimum=minimum)
        await interaction.followup.send(embed=view.build_embed(), view=view, ephemeral=True)

    @uno.command(name="rules", description="View the UNO rules and bot-specific rules.")
    async def rules_cmd(self, interaction: discord.Interaction):
        container = Container(
            accent_colour=discord.Colour(0x2020D5)
        )

        container.add_item(
            TextDisplay(
                "# <:uno:1536135137232953451> UNO Rules\n"
                "The bot follows standard UNO rules except those explicitly stated below.\n"
                "[View Standard UNO Rules](https://en.wikipedia.org/wiki/Uno_(card_game)#Official_rules)"
            )
        )

        container.add_item(Separator())

        container.add_item(
            TextDisplay(
                "### <:uno_draw_2:1536919114247839815> Draw 2 Stacking\n"
                "Each consecutive Draw 2 increases the penalty by 2.\n"
                "Example: `+2 → +4 → +6 → +8`.\n"
                "Playing anything other than a Draw 2, or drawing normally, resets the chain."
            )
        )

        container.add_item(Separator())

        container.add_item(
            TextDisplay(
                "### <:Wild_Draw_4:1536130674908594256> Wild Draw 4 Challenge\n"
                "A Wild Draw 4 may be played regardless of your hand. "
                "The targeted player may **Accept** or **Challenge**.\n\n"
                "A **Challenge** checks whether the Wild Draw 4 player had a card "
                "matching the previous color.\n\n"
                "**Successful challenge:** Wild Draw 4 player draws 4.\n"
                "**Failed challenge:** Challenger draws 6.\n"
                "**No response:** Automatically accepted; challenger draws 4."
            )
        )

        container.add_item(Separator())

        container.add_item(
            TextDisplay(
                "### <:uno_call:1536919891075014787> Calling UNO\n"
                "**At 2 cards:** You may toggle UNO in your play menu. "
                "If you subsequently play down to 1 card, the bot automatically calls UNO for you.\n\n"
                "**At 1 card:** You can immediately call UNO using the button or slash command."
            )
        )

        container.add_item(Separator())

        container.add_item(
            TextDisplay(
                "### ❗ Callouts\n"
                "**Successful:** All players who did not call UNO at 1 card draw 2.\n"
                "**Unsuccessful:** You draw 2 cards. You cannot call out again until it is your turn. "
                "Staff may also take action if this is abused."
            )
        )

        container.add_item(Separator())

        container.add_item(
            TextDisplay(
                "### <a:bay_alarm:1536288829248512030>️ Turn Timer / AFK\n"
                "Each turn has a configurable timeout. **45 seconds by default.**\n"
                "If the timer expires, the bot automatically makes you draw a card.\n"
                "Consecutive AFK timeouts can result in removal from the game."
            )
        )

        container.add_item(Separator())

        container.add_item(
            TextDisplay(
                f"### {config.UNO_CHIP_EMOJI} Betting & Rounds\n"
                "The first player to empty their hand "
                "wins the round and collects from each opponent up to **their own bet**. Anything "
                "you bet above the winner's bet stays yours. Your winnings or losses are applied to the "
                "bet on the table and the table goes straight into the next round.\n\n"
                "Between rounds you get a short window to **Change bet** or join. Do nothing and whatever "
                "you have left carries over as your next bet, if you below the minimum and you're removed "
                "\n\n"
                "**Leave** takes effect after the current round. **/uno quit** leaves immediately and "
                "forfeits your bet for the round."
            )
        )

        view = discord.ui.LayoutView()
        view.add_item(container)

        await interaction.response.send_message(
            view=view,
            ephemeral=True,
        )

    # ---------------- listeners ----------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        session = self.sessions.get(message.channel.id)
        if not session or session.closed:
            return
        session.msg_count += 1
        if session.msg_count >= session.resend_after:
            async with session.lock:
                if session.closed:
                    return
                # Repost whichever message people are actually using right
                # now so it doesn't get buried: the live table mid-round,
                # the join / bet panel otherwise.
                if session.state == "playing" and session.engine and not session.engine.finished:
                    await self.post_table(session, message.channel)
                elif session.state in ("lobby", "window", "waiting"):
                    await self.post_panel(session, message.channel)

    async def _remove_member_everywhere(self, guild_id: int, user_id: int, reason: str):
        for session in list(self.sessions.values()):
            if session.guild_id != guild_id or user_id not in session.seats:
                continue
            async with session.lock:
                seat = session.seats.get(user_id)
                if not seat or session.closed:
                    continue
                channel = self.bot.get_channel(session.channel_id)
                if session.state == "playing" and seat.in_round:
                    engine = session.engine
                    if await self._drop_from_round(session, user_id, reason="left_guild"):
                        if channel:
                            await channel.send(f"👋 A player {reason} and was removed from the game — their bet is forfeited.")
                        if engine.finished:
                            await self.announce_and_cleanup(session)
                        else:
                            # Always restart, not just "if was_current" — see the
                            # matching comment in the /uno kick handler for why.
                            self.restart_timer(session)
                            await self.refresh_table(session)
                    continue
                amount = await db.table_cash_out(user_id, seat.name)
                await self._log_seat(session, user_id, seat.name, "left_guild", amount,
                                     "between rounds — returned to wallet")
                del session.seats[user_id]
                if channel:
                    await self._after_seat_change(session, channel)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        await self._remove_member_everywhere(member.guild.id, member.id, "left the server")

    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        await self._remove_member_everywhere(guild.id, user.id, "was banned")


    # ---------------- global slash-command error handler ----------------

    async def cog_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        """
        Catches anything raised inside any /uno command that wasn't already
        handled locally (e.g. a stray UnoError/DeckExhausted, an AttributeError
        from a coding bug, etc). Without this, discord.py's default behavior
        is to log to console and leave the interaction un-acked entirely —
        which is exactly what shows up client-side as "This interaction
        failed". Every View/Modal already has its own on_error routed
        through report_component_error; this is the equivalent for slash
        commands specifically.
        """
        original = getattr(error, "original", error)
        log.error("Slash command '%s' failed for user %s", getattr(interaction.command, "qualified_name", "?"),
                  interaction.user.id, exc_info=original)
        try:
            if interaction.response.is_done():
                await interaction.followup.send("⚠️ Something went wrong running that command.", ephemeral=True)
            else:
                await interaction.response.send_message("⚠️ Something went wrong running that command.", ephemeral=True)
        except discord.HTTPException:
            pass


async def setup(bot: commands.Bot):
    # Refuse to load with an absurd tax rate. It's applied as
    # ceil(profit * rate), so a percent typed where a fraction belongs
    # (5 instead of 0.05) would tax every win at 500% — fail loudly at
    # startup instead of silently eating payouts.
    rate = config.UNO_WINNERS_TAX_RATE
    if not isinstance(rate, (int, float)) or not 0 <= rate <= 0.5:
        raise ValueError(f"config.UNO_WINNERS_TAX_RATE must be a fraction between 0 and 0.5 (0.05 = 5%), got {rate!r}")
    await bot.add_cog(UnoGame(bot))