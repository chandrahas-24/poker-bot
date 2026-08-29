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
  - Lobby (Join / Leave / Settings / Start / Cancel). The host is auto-
    added as a player by default. Moderator powers (settings/kick/start/
    cancel) belong to event staff (see is_event_staff: admins, configured
    staff roles, or developer IDs) — NOT to the host specifically. A host
    who also happens to be event staff can Leave and still run/start the
    game; a host who isn't staff has no special powers here.
  - "Winners" setting = how many players can finish (empty their hand,
    placing 1st/2nd/3rd/...) before the game ends — not a match-restart.
    The engine keeps finished players out of turn rotation and keeps the
    remaining players going until that many have placed.
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
  - Turn timer: TURN_TIMEOUT_SECONDS per turn; on expiry, auto-draws for
    the AFK player. MAX_MISSED_TURNS (from the engine) consecutive
    timeouts in a row -> that player is removed from the game.
  - Table auto-resends every TABLE_REFRESH_EVERY_MESSAGES channel
    messages, or on manual "Table" button / /uno table command. (Note:
    the "Table" *button* was repurposed into the turn-order popup above —
    manual full-table refresh is the /uno table slash command instead.)
  - on_member_remove / on_member_ban: auto-removes that player from any
    session (lobby or active game) they're in.
  - Callout (UNO challenge) is hardened: only active players can call
    someone out, self-callouts are blocked, and the engine itself already
    prevents repeat-farming the same target (a successful challenge takes
    their hand off exactly 1 card, so a second challenge against them
    fails until they're legitimately back down to 1 card).
  - /uno kick <user> — event staff can remove a player from lobby or game.
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
from .hand_render import compose_hand_image
from . import database as db
from .unodemo import (
    sort_hand,
    display_label,
    build_rows_with_fallback,
    sanitize_emoji_name,
    FALLBACK_EMOJI_BY_COLOR,
    MAX_BUTTONS_TOTAL,
)

log = logging.getLogger("uno")


def _parse_chips(value: str) -> int | None:
    """Parse chip amounts like 500, 2k, 1.5k, 2000."""
    try:
        v = value.strip().lower().replace(",", "")
        if v.endswith("k"):
            return int(float(v[:-1]) * 1000)
        return int(float(v))
    except (ValueError, TypeError):
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
    return asyncio.create_task(_wrapper(), name=name)


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


TURN_TIMEOUT_SECONDS = 45
TABLE_REFRESH_EVERY_MESSAGES = 7
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
MIN_TARGET_WINNERS, MAX_TARGET_WINNERS = 1, 19
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
        return f"Game started! ({engine.num_decks if engine else '?'} deck{'s' if engine and engine.num_decks != 1 else ''}, {total} cards)"

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
    # appears once, in the "Game started!" headline above. What's left
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
        if engine.bets:
            # target_winners isn't what ends an economy round — pots
            # settling is — so drop the "/N" that implies a fixed target.
            lines.append(f"<:uno_trophy:1536135332863676548> Finished: {placed}")
        else:
            lines.append(f"<:uno_trophy:1536135332863676548> Finished ({len(engine.finishers)}/{engine.target_winners}): {placed}")

    if engine.bets:
        pot_total = sum(engine.bets.values())
        lines.append(f"Pot: **{pot_total}** {config.UNO_CHIP_EMOJI}")

    lines.append("")
    if session.turn_started_at:
        expire_ts = int(session.turn_started_at + session.turn_timeout)
    else:
        expire_ts = int(time.time()) + session.turn_timeout
    lines.append(f"-# <t:{expire_ts}:R> – {session.turn_timeout}s Turn Time")

    return "\n".join(lines)


class GameSession:
    def __init__(self, guild_id: int, channel_id: int, host_id: int, host_name: str):
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.host_id = host_id
        self.host_name = host_name
        # Host does NOT auto-join — they show up in the lobby list only
        # once they click Join and submit a bet, exactly like anyone else.
        # host_id/host_name are kept purely for the "Host" embed field and
        # don't imply they're playing.
        self.lobby_players: dict[int, str] = {}
        self.lobby_bets: dict[int, int] = {}  # pid -> chips wagered — a pid only ever appears in lobby_players once it's also here
        self.min_bet = config.UNO_MIN_BET
        self.started = False
        self.engine: ue.GameState | None = None
        self.table_message: discord.Message | None = None
        self.lobby_message: discord.Message | None = None
        self.timer_task: asyncio.Task | None = None
        self.msg_count = 0
        self.last_action: dict | None = None  # feeds describe_last_action() for the table message
        self.turn_started_at: float = 0.0  # unix time the current turn's timer started, for the <t:...> display
        self.callout_balance: dict[int, int] = {}  # player_id -> false-callouts remaining (missing = 1, default)
        self._last_turn_player_id: int | None = None  # tracks turn changes so callout_balance refreshes correctly

        # Serializes state-changing interaction handlers (play/draw/pass/
        # wild color/WD4 accept/challenge/timeout/kick/leave/UNO toggle) so
        # two near-simultaneous actions on the same session can't both read
        # the pre-mutation engine state and then both mutate it. Not
        # persisted — a resumed session just gets a fresh lock.
        self.lock = asyncio.Lock()
        # Bumped every time a new turn starts (see restart_timer). A timer
        # task captures the generation it was started with and checks it
        # again after waking up from sleep — if the number has moved on,
        # some other action already resolved this turn (a manual play/draw/
        # etc, or another timer), so the stale timer does nothing instead
        # of double-acting on a turn that's already over.
        self.turn_generation = 0

        # configurable via the Settings button, event-staff-only
        self.max_players = 6
        self.num_decks = 4
        self.turn_timeout = TURN_TIMEOUT_SECONDS
        # NOT configurable — economy rounds end dynamically once every side
        # pot already has a determined winner (see GameState._pots_resolved),
        # not at a fixed "N players finished" target. Passed to GameState
        # purely for its non-economy fallback code path, which UNO never
        # actually takes now that every table requires bets to join.
        self.target_winners = 1

    def cancel_timer(self):
        # Guard against self-cancellation: _timer_loop calls
        # announce_and_cleanup() on itself when the game ends on an AFK
        # timeout, which calls this. session.timer_task at that point is
        # STILL the currently-running task (nothing reassigns it before
        # this call), so cancelling it here would throw CancelledError
        # into this same coroutine at its next `await` — aborting
        # announce_and_cleanup before it ever sends the win message, with
        # no error logged (safe_task's wrapper deliberately re-raises
        # CancelledError rather than logging it). Cross-task cancels
        # (e.g. from /uno kick or /uno leave, which run on the
        # interaction-handler task, not the timer task) are unaffected
        # and still cancel normally.
        if self.timer_task and not self.timer_task.done() and self.timer_task is not asyncio.current_task():
            self.timer_task.cancel()
        self.timer_task = None

    # ---------- persistence ----------
    # Only ever called for started games (see UnoGame.post_table) — an
    # un-started lobby is cheap to just recreate with /uno start, so it's
    # not worth the complexity of saving/restoring LobbyView state too.
    # table_message/lobby_message (discord.Message objects) and timer_task
    # (an asyncio.Task) aren't JSON-serializable and don't need to be —
    # restore just posts a fresh table message and restarts the timer.

    def to_dict(self):
        return {
            "guild_id": self.guild_id,
            "channel_id": self.channel_id,
            "host_id": self.host_id,
            "host_name": self.host_name,
            "lobby_players": {str(k): v for k, v in self.lobby_players.items()},
            "started": self.started,
            "engine": self.engine.to_dict() if self.engine else None,
            "msg_count": self.msg_count,
            "last_action": self.last_action,
            "turn_started_at": self.turn_started_at,
            "callout_balance": {str(k): v for k, v in self.callout_balance.items()},
            "_last_turn_player_id": self._last_turn_player_id,
            "max_players": self.max_players,
            "num_decks": self.num_decks,
            "target_winners": self.target_winners,
            "turn_timeout": self.turn_timeout,
        }

    @classmethod
    def from_dict(cls, d):
        self = cls.__new__(cls)
        self.guild_id = d["guild_id"]
        self.channel_id = d["channel_id"]
        self.host_id = d["host_id"]
        self.host_name = d["host_name"]
        self.lobby_players = {int(k): v for k, v in d.get("lobby_players", {}).items()}
        self.started = d.get("started", True)
        self.engine = ue.GameState.from_dict(d["engine"]) if d.get("engine") else None
        self.table_message = None
        self.lobby_message = None
        self.timer_task = None
        self.msg_count = d.get("msg_count", 0)
        self.last_action = d.get("last_action")
        self.turn_started_at = d.get("turn_started_at", 0.0)
        self.callout_balance = {int(k): v for k, v in d.get("callout_balance", {}).items()}
        self._last_turn_player_id = d.get("_last_turn_player_id")
        self.max_players = d.get("max_players", 10)
        self.num_decks = d.get("num_decks", 1)
        self.target_winners = d.get("target_winners", 1)
        self.turn_timeout = d.get("turn_timeout", TURN_TIMEOUT_SECONDS)
        # Fresh lock/generation counter for the resumed session — see the
        # comments on these fields in __init__.
        self.lock = asyncio.Lock()
        self.turn_generation = 0
        return self


# ---------------------------------------------------------------------------
# Lobby view
# ---------------------------------------------------------------------------

class LobbySettingsModal(discord.ui.Modal, title="UNO — Lobby Settings"):
    max_players_input = discord.ui.TextInput(label="Max players (2–20)", max_length=2)
    decks_input = discord.ui.TextInput(label="Decks (1–6)", max_length=1)
    turn_timeout_input = discord.ui.TextInput(label="Turn timeout seconds (30–600)", max_length=3)

    def __init__(self, cog: "UnoGame", session: GameSession):
        super().__init__()
        self.cog = cog
        self.session = session
        self.max_players_input.default = str(session.max_players)
        self.decks_input.default = str(session.num_decks)
        self.turn_timeout_input.default = str(session.turn_timeout)

    async def on_submit(self, interaction: discord.Interaction):
        async with self.session.lock:
            if self.session.started:
                await interaction.response.send_message(
                    "⚠️ The game has already started.", ephemeral=True,
                )
                return

            def parse(field, lo, hi, current):
                try:
                    v = int(field.value.strip())
                except ValueError:
                    return current, False
                if not (lo <= v <= hi):
                    return current, False
                return v, True

            max_players, ok1 = parse(self.max_players_input, MIN_PLAYERS_LIMIT, MAX_PLAYERS_LIMIT, self.session.max_players)
            decks, ok2 = parse(self.decks_input, MIN_DECKS, MAX_DECKS, self.session.num_decks)
            turn_timeout, ok3 = parse(self.turn_timeout_input, MIN_TURN_TIMEOUT, MAX_TURN_TIMEOUT, self.session.turn_timeout)

            if not (ok1 and ok2 and ok3):
                await interaction.response.send_message(
                    "⚠️ One or more values were out of range — settings left unchanged.", ephemeral=True
                )
                return

            if max_players < len(self.session.lobby_players):
                await interaction.response.send_message(
                    f"⚠️ Can't set max players below the {len(self.session.lobby_players)} already in the lobby.",
                    ephemeral=True,
                )
                return

            self.session.max_players = max_players
            self.session.num_decks = decks
            self.session.turn_timeout = turn_timeout

            await interaction.response.send_message("✅ Settings updated.", ephemeral=True)
            if self.session.lobby_message:
                await self.session.lobby_message.edit(embed=LobbyView.build_embed(self.session))

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await report_component_error(interaction, error, None, "LobbySettingsModal")


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


class BetModal(discord.ui.Modal, title="UNO — Place Your Bet"):
    def __init__(self, cog: "UnoGame", session: GameSession, balance: int):
        super().__init__()
        self.cog = cog
        self.session = session
        self.balance = balance
        self.bet_input = discord.ui.TextInput(
            label="How many chips to bet?",
            placeholder=f"min {session.min_bet}  (wallet: {balance} {config.UNO_CHIP_EMOJI})",
            min_length=1, max_length=8,
        )
        self.add_item(self.bet_input)

    async def on_submit(self, interaction: discord.Interaction):
        async with self.session.lock:
            if self.session.started:
                await interaction.response.send_message("⚠️ The game has already started.", ephemeral=True)
                return
            if interaction.user.id in self.session.lobby_bets:
                await interaction.response.send_message("You're already in with a bet placed.", ephemeral=True)
                return
            if await db.is_banned(interaction.guild_id, interaction.user.id):
                await interaction.response.send_message("🚫 You're banned from UNO in this server.", ephemeral=True)
                return
            if len(self.session.lobby_players) >= self.session.max_players:
                await interaction.response.send_message("Lobby is full.", ephemeral=True)
                return

            bet = _parse_chips(self.bet_input.value)
            if bet is None:
                await interaction.response.send_message("⚠️ Enter a valid amount (e.g. 500, 2k).", ephemeral=True)
                return
            if bet < self.session.min_bet:
                await interaction.response.send_message(
                    f"⚠️ Minimum bet is **{self.session.min_bet}** {config.UNO_CHIP_EMOJI}.", ephemeral=True
                )
                return
            if bet > self.balance:
                await interaction.response.send_message(
                    f"⚠️ You only have **{self.balance}** {config.UNO_CHIP_EMOJI} available.", ephemeral=True
                )
                return

            await db.upsert_wallet_name(interaction.user.id, interaction.user.name)
            ok = await db.deduct_chips(interaction.user.id, bet)
            if not ok:
                # balance moved between opening the modal and submitting it — the
                # cached self.balance we validated against above is now stale
                await interaction.response.send_message(
                    "⚠️ Insufficient balance for that bet — your wallet may have changed since you opened this. "
                    "Hit Join again to try with your current balance.", ephemeral=True
                )
                return
            await db.mark_chips_in_play(interaction.user.id, interaction.user.display_name, bet)

            self.session.lobby_players[interaction.user.id] = interaction.user.display_name
            self.session.lobby_bets[interaction.user.id] = bet

            await interaction.response.edit_message(embed=LobbyView.build_embed(self.session))

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await report_component_error(interaction, error, None, "BetModal")


class LobbyView(discord.ui.View):
    def __init__(self, cog: "UnoGame", session: GameSession):
        super().__init__(timeout=1800)
        self.cog = cog
        self.session = session
        cog._track_view(self)

    async def on_timeout(self):
        # The lobby message itself just goes stale/uninteractable when a
        # View times out — but the session was still sitting in
        # self.cog.sessions, permanently blocking /uno start in this
        # channel until someone noticed and ran /uno end. Only clean up if
        # the game never actually started (a started game has its own
        # lifecycle/timer and must not be touched here).
        if not self.session.started:
            self.cog.sessions.pop(self.session.channel_id, None)
            for pid, bet in self.session.lobby_bets.items():
                await db.return_chips(pid, bet)
                await db.clear_chips_in_play(pid)
            if self.session.lobby_message:
                try:
                    await self.session.lobby_message.edit(
                        content="Lobby expired. Any bets placed have been refunded.", embed=None, view=None
                    )
                except discord.HTTPException:
                    pass
        self.stop()

    @staticmethod
    def build_embed(session: GameSession) -> discord.Embed:
        host_mention = f"<@{session.host_id}>"
        names = "\n".join(
            f"<@{pid}> — **{session.lobby_bets[pid]}** {config.UNO_CHIP_EMOJI}"
            for pid in session.lobby_players
        ) or "_nobody yet_"

        e = discord.Embed(
            title="<:uno:1536135137232953451> UNO",
            description="A new game is starting in this channel — hit **Join** to grab a seat.",
            color=discord.Color.blurple(),
        )
        e.add_field(name="<:uno_host:1536135395933159556> Host", value=f"  {host_mention}", inline=False)
        e.add_field(
            name="👥 Players",
            value=f"{len(session.lobby_players)}/{session.max_players}",
            inline=True
        )
        e.add_field(
            name="<:uno_deck:1536135257789829120>  Decks",
            value=f"\u200b\u200b{session.num_decks}",
            inline=True
        )
        e.add_field(name=f"Minimum bet - {session.min_bet} {config.UNO_CHIP_EMOJI}", value=names, inline=False)
        e.set_thumbnail(url='https://media.discordapp.net/attachments/1525465986541555813/1536293002975117362/wdmbzh1.jpg?ex=6a7adfda&is=6a798e5a&hm=4bbf5ffcf806948fecbd377781edfc95f3c5c06a78cd859895930d4f7057226b&=&format=webp')
        return e

    async def _refresh(self, interaction: discord.Interaction):
        await interaction.response.edit_message(embed=self.build_embed(self.session), view=self)

    @discord.ui.button(label="Join", style=discord.ButtonStyle.success, custom_id="uno_lobby_join", row=0)
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.session.lock:
            if interaction.user.id in self.session.lobby_bets:
                await interaction.response.send_message("You're already in with a bet placed.", ephemeral=True)
                return
            if len(self.session.lobby_players) >= self.session.max_players:
                await interaction.response.send_message("Lobby is full.", ephemeral=True)
                return
        balance = await db.get_balance(interaction.user.id)
        await interaction.response.send_modal(BetModal(self.cog, self.session, balance))

    @discord.ui.button(label="Leave", style=discord.ButtonStyle.danger, custom_id="uno_lobby_leave", row=0)
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.session.lock:
            if interaction.user.id not in self.session.lobby_players:
                await interaction.response.send_message("You're not in this lobby.", ephemeral=True)
                return
            del self.session.lobby_players[interaction.user.id]
            bet = self.session.lobby_bets.pop(interaction.user.id, None)
            if bet is not None:
                await db.return_chips(interaction.user.id, bet)
                await db.clear_chips_in_play(interaction.user.id)
            await self._refresh(interaction)

    @discord.ui.button(label="Rules",style=discord.ButtonStyle.secondary,custom_id="uno_lobby_rules",row=0)
    async def rules(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.rules_cmd.callback(self.cog, interaction)

    @discord.ui.button(label="Settings", style=discord.ButtonStyle.secondary, emoji="⚙️", custom_id="uno_lobby_settings", row=1)
    async def settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await is_event_staff(interaction):
            await interaction.response.send_message("Only event staff can change settings.", ephemeral=True)
            return
        # Only the modal-open itself needs no lock (nothing is mutated yet);
        # LobbySettingsModal.on_submit acquires session.lock around its own
        # mutation, and separately re-checks session.started to guard
        # against a stale modal being submitted after the game starts.
        await interaction.response.send_modal(LobbySettingsModal(self.cog, self.session))

    @discord.ui.button(label="Start", style=discord.ButtonStyle.primary, custom_id="uno_lobby_start", row=1)
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await is_event_staff(interaction):
            await interaction.response.send_message("Only event staff can start the game.", ephemeral=True)
            return

        async with self.session.lock:
            if self.session.started:
                await interaction.response.send_message("This game has already started.", ephemeral=True)
                return

            players = list(self.session.lobby_players.items())
            if len(players) < 2:
                await interaction.response.send_message("Need at least 2 players to start.", ephemeral=True)
                return
            # No unbetted-player check needed here: a pid only ever enters
            # lobby_players once BetModal has already collected a bet for
            # it (see the Join button below) — there's no path to a lobby
            # member without one.

            required_cards = len(players) * 7 + 1  # hand_size is always 7 here
            available_cards = self.session.num_decks * 108
            if required_cards > available_cards:
                await interaction.response.send_message(
                    f"⚠️ Not enough cards for {len(players)} players with "
                    f"{self.session.num_decks} deck(s).",
                    ephemeral=True,
                )
                return

            random.shuffle(players)  # turn order shouldn't depend on join order
            try:
                self.session.engine = ue.GameState(players, num_decks=self.session.num_decks,
                                                    target_winners=self.session.target_winners,
                                                    bets=dict(self.session.lobby_bets))
            except ue.UnoError as e:
                await interaction.response.send_message(f"⚠️ {e}", ephemeral=True)
                return

            # Refund anyone whose buy-in was uncontested from the start
            # (see GameState._trim_uncontested_excess) — right back to
            # their wallet now, before a single card is played.
            refund_lines = []
            for pid, excess in self.session.engine.starting_refunds.items():
                await db.return_chips(pid, excess)
                await db.update_chips_in_play(pid, self.session.engine.bets[pid])
                await db.log_currency_event(pid, "Cash In", excess, "Uncontested Buy-In Refund")
                refund_lines.append(
                    f"<@{pid}> — **{excess}** {config.UNO_CHIP_EMOJI} refunded "
                    f"(no one else covered that much, bet trimmed to **{self.session.engine.bets[pid]}**)"
                )

            self.session.started = True
            self.stop()

            # Leave the original lobby embed exactly as it is — don't edit or clear
            # it. Just disable its buttons (so Join/Leave/etc can't be clicked on a
            # stale lobby) and post the table as a brand-new message.
            for child in self.children:
                child.disabled = True
            await interaction.response.edit_message(view=self)

            if refund_lines:
                await interaction.channel.send(
                    "💰 **Uncontested buy-in refunded:**\n" + "\n".join(refund_lines),
                    allowed_mentions=discord.AllowedMentions(users=True),
                )

            await self.cog.post_table(self.session, interaction.channel)
            self.cog.restart_timer(self.session)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, custom_id="uno_lobby_cancel",row=1)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await is_event_staff(interaction):
            await interaction.response.send_message("Only event staff can cancel the lobby.", ephemeral=True)
            return
        async with self.session.lock:
            self.cog.sessions.pop(self.session.channel_id, None)
            for pid, bet in self.session.lobby_bets.items():
                await db.return_chips(pid, bet)
                await db.clear_chips_in_play(pid)
            self.stop()
            await interaction.response.edit_message(content="Lobby cancelled.", embed=None, view=None)

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        await report_component_error(interaction, error, item, "LobbyView")


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
            img = compose_hand_image(hand, self.cog._card_image_cache)
            buf = io.BytesIO()
            # PNG encoding is synchronous CPU work — offload it so it
            # doesn't block the event loop (and every other channel's
            # interactions) while it runs.
            await asyncio.to_thread(img.save, buf, format="PNG")
            buf.seek(0)
            filename = "hand.png"
            self.file = discord.File(buf, filename=filename)
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
            # /uno end (or anything else that pops the channel out of
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
        self._load_card_image_cache()
        self._thumb_bytes_cache: dict[str, bytes] = {}  # card_name -> pre-encoded padded PNG bytes

        self._flush_turn_log.start()

    def _track_view(self, view: discord.ui.View):
        self._active_views.add(view)

    def _is_live(self, session: GameSession) -> bool:
        """True iff `session` is still THE registered session for its
        channel. /uno end (and any other path that pops a channel out of
        self.sessions) doesn't touch the session object itself or its
        engine — old Views (TableView, HandPlayView) and modals
        (PlayCardModal, DrawFollowupView) all hold a direct reference to
        that same session object in their closures, so after /uno end
        `session.engine` is still non-None/unfinished and every
        engine-level check on it still passes. Without this identity
        check, those stale components kept working — the game "kept
        going" through old buttons even after /uno end said it ended.
        Every shared action handler below checks this first."""
        return self.sessions.get(session.channel_id) is session

    def _stop_session_views(self, session: GameSession):
        """Stops every tracked View belonging to this specific session
        (its buttons show as disabled/dead on next interaction) — used by
        /uno end so old table/hand messages don't just silently no-op,
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
            if session.started and session.engine:
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

    def _load_card_image_cache(self):
        if not os.path.isdir(card_assets.CARDS_DIR):
            return
        for fname in os.listdir(card_assets.CARDS_DIR):
            if not fname.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
                continue
            name = os.path.splitext(fname)[0]
            try:
                path = os.path.join(card_assets.CARDS_DIR, fname)
                self._card_image_cache[name] = Image.open(path).convert("RGBA")
            except Exception:
                log.exception("Failed to load card image %s", fname)
        log.info("Cached %d card image(s) in memory", len(self._card_image_cache))

    uno = app_commands.Group(name="uno", description="UNO game commands")
    unomgr = app_commands.Group(name="unomgr", description="UNO manager commands (economy)")
    unoadmin = app_commands.Group(name="unoadmin", description="UNO admin/dev commands")
    unoset = app_commands.Group(name="unoset", description="Configure UNO settings")

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
        if not session.engine:
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

    @uno.command(name="resume", description="Resume this channel's game from its last saved checkpoint")
    async def resume_cmd(self, interaction: discord.Interaction):
        """
        Deliberately manual, not automatic — a save on disk is a checkpoint
        taken after every turn (see post_table), so if the bot crashed or a
        bug corrupted state mid-game, the save file is the recovery point:
        hand-fix the JSON if needed, then run this to pick back up from it.
        No auto-restore-on-boot, on purpose — that would resume every saved
        game unconditionally on every restart, with no chance to inspect or
        fix a save first if the crash was caused by bad game state.
        """
        if interaction.channel_id in self.sessions:
            await interaction.response.send_message(
                "There's already a game or lobby active in this channel — end it first.", ephemeral=True
            )
            return

        path = self._save_path(interaction.channel_id)
        if not path.exists():
            await interaction.response.send_message("No saved game found for this channel.", ephemeral=True)
            return

        try:
            with open(path, "r") as f:
                data = json.load(f)
            session = GameSession.from_dict(data)
        except Exception:
            log.exception("Failed to parse save file for channel %s", interaction.channel_id)
            await interaction.response.send_message(
                "⚠️ That save file didn't load — check the logs for the exact error "
                "(bad/edited JSON, wrong field, etc.), fix it, and try again.",
                ephemeral=True,
            )
            return

        if not session.engine:
            await interaction.response.send_message("⚠️ That save has no game state to resume.", ephemeral=True)
            return

        if not await is_event_staff(interaction):
            await interaction.response.send_message(
                "Only event staff can resume this save.", ephemeral=True
            )
            return

        self.sessions[interaction.channel_id] = session
        try:
            await interaction.response.send_message("🔄 Resuming saved game — table posted below.", ephemeral=True)
            await self.post_table(session, interaction.channel)
            self.restart_timer(session)
        except Exception:
            log.exception("Failed to resume game for channel %s", interaction.channel_id)
            self.sessions.pop(interaction.channel_id, None)
            # Deliberately NOT deleting the save here — a failed resume
            # (e.g. a bad emoji/image render) shouldn't destroy the one
            # copy of the game state; leave it on disk so /uno resume can
            # just be retried after whatever broke gets fixed.
            await interaction.followup.send(
                "⚠️ Resume failed partway through — the save file is untouched, you can try again.", ephemeral=True
            )

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

    async def announce_and_cleanup(self, session: GameSession):
        """Called once the game has fully ended — posts final placements and tears the session down."""
        channel = self.bot.get_channel(session.channel_id)
        engine = session.engine
        session.cancel_timer()
        self.sessions.pop(session.channel_id, None)
        await asyncio.to_thread(self._delete_save, session.channel_id)

        if not channel or not engine:
            return

        net_lines = []
        if engine.bets:
            # Every original bettor's buy-in is done being "in play" the
            # moment the round ends, win or lose. Nothing gets credited to
            # a wallet until we know each player's NET result for the whole
            # round (gross winnings across every pot they took, minus their
            # own bet) — same order poker's engine.py applies its tax in
            # (_showdown/_advance): distribute the raw pots first, then tax
            # each player once off their total net profit for the hand/round,
            # only if that net is actually positive.
            gross_payout: dict[int, int] = collections.defaultdict(int)
            for sp in engine.side_pots:
                payouts = sp.get("payouts") or {}
                if payouts:
                    for pid, amount in payouts.items():
                        gross_payout[pid] += amount
                else:
                    # Shouldn't normally happen (_pots_resolved() guarantees
                    # a winner before the round ends) — defensive fallback
                    # for the one edge case that can still reach here (every
                    # remaining player got removed simultaneously). Split
                    # the pot evenly across its eligible contributors rather
                    # than returning each of them their FULL bet (which
                    # would double-pay anyone eligible for more than one pot).
                    eligible = sp["eligible"] or list(engine.bets)
                    share, remainder = divmod(sp["amount"], len(eligible))
                    for i, pid in enumerate(eligible):
                        amt = share + (remainder if i == 0 else 0)
                        if amt:
                            gross_payout[pid] += amt

            tax_rate = config.UNO_WINNERS_TAX_RATE
            placements = {pid: i + 1 for i, pid in enumerate(engine.finishers)}
            round_players = []
            total_tax = 0
            for pid in engine.bets:
                name = engine.all_names.get(pid, str(pid))
                gross = gross_payout.get(pid, 0)
                bet = engine.bets[pid]
                net = gross - bet
                tax = math.ceil(net * tax_rate) if net > 0 else 0
                credited = gross - tax

                if credited > 0:
                    await db.add_chips(
                        self.bot.user.id, "UNO Pot", pid, name,
                        credited, note="UNO pot win" if net > 0 else "UNO pot refund",
                    )
                await db.clear_chips_in_play(pid)

                net_after_tax = net - tax
                await db.record_round_result(
                    pid, name, won=net_after_tax > 0,
                    net_chips=net_after_tax, chips_wagered=bet,
                )
                await db.log_currency_event(
                    pid, "Round", net_after_tax,
                    f"UNO round in #{channel.name if hasattr(channel, 'name') else session.channel_id}"
                )
                if tax > 0:
                    await db.log_house_revenue(tax, source="winners_tax")
                    total_tax += tax

                round_players.append({
                    "user_id": pid, "username": name, "bet": bet, "gross": gross,
                    "tax": tax, "net": net_after_tax, "placement": placements.get(pid),
                    "won": net_after_tax > 0,
                })

                sign = "+" if net_after_tax >= 0 else ""
                net_lines.append(f"{_mention(pid)} **{sign}{net_after_tax}** {config.UNO_CHIP_EMOJI}")

            winner_pid = engine.finishers[0] if engine.finishers else None
            await db.log_round(
                engine.round_uuid, session.guild_id, session.channel_id, len(engine.bets), engine.num_decks,
                sum(engine.bets.values()), total_tax, winner_pid,
                engine.all_names.get(winner_pid) if winner_pid else None, round_players,
            )
            # Catch anything played since the last periodic flush so the
            # full turn-by-turn log is complete before the session (and
            # its engine, along with any unflushed events still sitting on
            # it) gets torn down a few lines below.
            await db.log_turn_events_bulk(engine.pop_turn_events())

            settings = await db.get_settings(session.guild_id)
            log_channel_id = settings.get("log_channel_id")
            if log_channel_id:
                log_channel = self.bot.get_channel(int(log_channel_id))
                if log_channel:
                    rows = "\n".join(
                        f"{p['username'][:14]:<14} bet:{p['bet']:<6} gross:{p['gross']:<6} "
                        f"tax:{p['tax']:<4} net:{'+' if p['net'] >= 0 else ''}{p['net']}"
                        for p in round_players
                    )
                    try:
                        await log_channel.send(
                            f"**UNO round** — #{getattr(channel, 'name', session.channel_id)} — "
                            f"{len(round_players)}p, pot {sum(engine.bets.values())}{config.UNO_CHIP_EMOJI}, "
                            f"tax {total_tax}\n```\n{rows}\n```"
                        )
                    except discord.HTTPException:
                        pass

        if not engine.finishers:
            await channel.send(
                "Game ended — no one finished (not enough players remaining)."
                + ("\n" + "\n".join(net_lines) if net_lines else "")
            )
            return

        winner_id = engine.finishers[0]
        embed = discord.Embed(
            title="<:uno_trophy:1536135332863676548> Game Over!",
            description=f"<:uno_1:1536286536218443837> {_mention(winner_id)} takes the win!",
            color=discord.Color.gold(),
        )

        placements = "\n".join(
            f"{PLACEMENT_EMOJI.get(i, f'`#{i+1}`')} {_mention(pid)}"
            for i, pid in enumerate(engine.finishers)
        )
        embed.add_field(name="Placements", value=placements, inline=False)

        if engine.players:
            still_in = ", ".join(_mention(p.player_id) for p in engine.players)
            embed.add_field(name="Still had cards left", value=still_in, inline=False)

        if net_lines:
            embed.add_field(name="Net", value="\n".join(net_lines), inline=False)

        embed.set_footer(text=f"{len(engine.finishers)} finisher(s) • {engine.num_decks} deck(s)")
        await channel.send(embed=embed)

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
            if engine.current_player().player_id != interaction.user.id:
                await interaction.response.send_message("⚠️ It's not your turn.", ephemeral=True)
                return

            try:
                effects = engine.play_card(interaction.user.id, card)
            except ue.IllegalCard as e:
                await interaction.response.send_message(f"⚠️ {e}", ephemeral=True)
                return
            except ue.NotYourTurn:
                await interaction.response.send_message("⚠️ It's not your turn.", ephemeral=True)
                return

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

            result = engine.draw_card(interaction.user.id)
            drawn = result["drawn"]
            # engine.draw_card() already clears armed_uno itself when the
            # draw moves the hand off exactly 2 cards — see the
            # armed_uno invariant note in uno_engine.py.

            if result["still_playable"]:
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
            img = compose_hand_image(hand, self._card_image_cache)
            buf = io.BytesIO()
            # PNG encoding is synchronous CPU work — offload it off the
            # event loop rather than blocking every other channel's
            # interactions while this one hand gets encoded.
            await asyncio.to_thread(img.save, buf, format="PNG")
            buf.seek(0)
            kwargs["file"] = discord.File(buf, filename="hand.png")
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

    @uno.command(name="start", description="Start a new UNO lobby in this channel")
    async def start(self, interaction: discord.Interaction):
        # Guards the check-then-insert below — without this, two /uno start
        # calls landing in the same channel within the same event-loop tick
        # (e.g. two people double-clicking, or a client retry) can both
        # read self.sessions before either has inserted into it, and both
        # go on to create a lobby. Whichever one loses the lock's ordering
        # will re-check afterward and see the other's session already
        # there.
        async with self._start_locks[interaction.channel_id]:
            if interaction.channel_id in self.sessions:
                await interaction.response.send_message("There's already a game or lobby active in this channel.", ephemeral=True)
                return

            session = GameSession(interaction.guild_id, interaction.channel_id, interaction.user.id, interaction.user.display_name)
            self.sessions[interaction.channel_id] = session

            view = LobbyView(self, session)
            await interaction.response.send_message(embed=LobbyView.build_embed(session), view=view)
            session.lobby_message = await interaction.original_response()

    @uno.command(name="refresh", description="Manually resend the full table for the game in this channel")
    async def refresh_cmd(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        if not session or not session.engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
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

    @uno.command(name="end", description="End/cancel the game or lobby in this channel (event staff only)")
    async def end(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        if not session:
            await interaction.response.send_message("No active game or lobby here.", ephemeral=True)
            return
        if not await is_event_staff(interaction):
            await interaction.response.send_message("Only event staff can end this.", ephemeral=True)
            return
        session.cancel_timer()
        self.sessions.pop(interaction.channel_id, None)
        # Old table/hand messages' buttons hold a direct reference to this
        # session object, not a fresh lookup — popping it out of
        # self.sessions above is what _is_live()/_guard() now check, but
        # stopping the Views too makes it visible immediately (buttons
        # show as dead) instead of only failing silently on next click.
        self._stop_session_views(session)
        await asyncio.to_thread(self._delete_save, interaction.channel_id)
        await interaction.response.send_message("Game ended.")

    @uno.command(name="kick", description="Remove a player from the lobby or game (event staff only)")
    @app_commands.describe(user="The user to remove")
    async def kick(self, interaction: discord.Interaction, user: discord.User):
        session = self.sessions.get(interaction.channel_id)
        if not session:
            await interaction.response.send_message("No active game or lobby here.", ephemeral=True)
            return
        if not await is_event_staff(interaction):
            await interaction.response.send_message("Only event staff can kick players.", ephemeral=True)
            return

        target_id = user.id

        if not session.started:
            # Lobby phase
            async with session.lock:
                if target_id in session.lobby_players:
                    name = session.lobby_players.pop(target_id)
                    bet = session.lobby_bets.pop(target_id, None)
                    if bet is not None:
                        await db.return_chips(target_id, bet)
                        await db.clear_chips_in_play(target_id)
                    await interaction.response.send_message(f"Removed **{name}** from the lobby.", ephemeral=False)
                    if session.lobby_message:
                        await session.lobby_message.edit(embed=LobbyView.build_embed(session))
                else:
                    await interaction.response.send_message("That user isn't in the lobby.", ephemeral=True)
            return

        # Active game phase
        engine = session.engine
        if not engine or engine.player_by_id(target_id) is None:
            await interaction.response.send_message("That user isn't an active player.", ephemeral=True)
            return

        async with session.lock:
            player = engine.player_by_id(target_id)
            if player is None:
                await interaction.response.send_message("That user isn't an active player.", ephemeral=True)
                return
            name = player.name
            engine.remove_player(target_id, reason="kicked")
            # Not setting session.last_action — the send_message right below
            # is already the public announcement, and refresh_table (called
            # a few lines down) used to repost a whole new table whose
            # headline said the exact same thing a second time.
            await interaction.response.send_message(f"Removed **{name}** from the game.", ephemeral=False)

            if engine.finished:
                await self.announce_and_cleanup(session)
            else:
                # Always restart the timer here, not just "if was_current" —
                # remove_player's turn-advancement can change who's current in
                # ways this simple pre-check can't detect (e.g. kicking the
                # target of a pending Wild Draw Four while the WD4 player was
                # still nominally "current"). Restarting unconditionally is
                # cheap and always correct.
                self.restart_timer(session)
                await self.refresh_table(session)

    @uno.command(name="leave", description="Leave the game or lobby in this channel")
    async def leave_cmd(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        if not session:
            await interaction.response.send_message("No active game or lobby here.", ephemeral=True)
            return

        target_id = interaction.user.id

        if not session.started:
            async with session.lock:
                if target_id in session.lobby_players:
                    session.lobby_players.pop(target_id)
                    bet = session.lobby_bets.pop(target_id, None)
                    if bet is not None:
                        await db.return_chips(target_id, bet)
                        await db.clear_chips_in_play(target_id)
                    await interaction.response.send_message("You left the lobby.", ephemeral=True)
                    if session.lobby_message:
                        await session.lobby_message.edit(embed=LobbyView.build_embed(session))
                else:
                    await interaction.response.send_message("You're not in this lobby.", ephemeral=True)
            return

        engine = session.engine
        if not engine or engine.player_by_id(target_id) is None:
            await interaction.response.send_message("You're not an active player in this game.", ephemeral=True)
            return

        async with session.lock:
            if engine.player_by_id(target_id) is None:
                await interaction.response.send_message("You're not an active player in this game.", ephemeral=True)
                return
            engine.remove_player(target_id, reason="left")
            # Same reasoning as /uno kick — don't duplicate the message below
            # as the next table headline too.
            await interaction.response.send_message(f"{_mention(target_id)} left the game.", ephemeral=False)

            if engine.finished:
                await self.announce_and_cleanup(session)
            else:
                self.restart_timer(session)
                await self.refresh_table(session)

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

    @unoset.command(name="managerrole", description="[Admin] Set the UNO Manager role")
    @app_commands.describe(role="Role that gets UNO manager access (addchips, ban, cashouts, etc.)")
    async def set_manager_role(self, interaction: discord.Interaction, role: discord.Role):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Server Administrator only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=False)
        await db.set_settings(interaction.guild_id, manager_role_id=role.id)
        await interaction.followup.send(f"✅ UNO Manager role: **{role.name}**")

    @unoset.command(name="logchannel", description="[Admin] Set where UNO round summaries get posted")
    @app_commands.describe(channel="Channel for round-by-round summaries (also always saved to the DB regardless)")
    async def set_log_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Server Administrator only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=False)
        await db.set_settings(interaction.guild_id, log_channel_id=channel.id)
        await interaction.followup.send(f"✅ UNO round log channel: {channel.mention}")

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
        if session and session.started and session.engine and target.id in session.engine.bets:
            table_str = f"\n**At table:** {session.engine.bets[target.id]} {config.UNO_CHIP_EMOJI}"
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

        lines = []
        for i, r in enumerate(rows):
            net = r["net_chips"]
            lines.append(
                f"{PLACEMENT_EMOJI.get(i, f'`#{i+1}`')} <@{r['user_id']}> — "
                f"**{'+' if net >= 0 else ''}{net:,} **{config.UNO_CHIP_EMOJI}"
            )
        embed = discord.Embed(title="🃏 UNO Leaderboard", description="\n".join(lines), color=0xF1C40F)
        if caller_row and caller_rank and caller_rank > 10:
            net = caller_row["net_chips"]
            embed.set_footer(text=f"Your rank: #{caller_rank} ({'+' if net >= 0 else ''}{net:,} net)")

        await interaction.followup.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

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
                    await ticket.add_reaction("✅")
            except Exception:
                log.exception("Failed to post UNO cashout ticket")

        await interaction.followup.send(
            f"✅ Locked **{chips}** {config.UNO_CHIP_EMOJI} for cashout. Staff have been notified — "
            f"react ✅ on the ticket to mark it paid.", ephemeral=True)

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

    @uno.command(
        name="timeout",
        description="Change the turn timeout for the current UNO game (event staff only)",
    )
    @app_commands.describe(seconds="Turn timeout in seconds (30–600)")
    async def timeout_cmd(self, interaction: discord.Interaction, seconds: int):
        session = self.sessions.get(interaction.channel_id)

        if not session or not session.engine:
            await interaction.response.send_message(
                "No active game here.", ephemeral=True
            )
            return

        if not await is_event_staff(interaction):
            await interaction.response.send_message(
                "Only event staff can change the turn timeout.", ephemeral=True
            )
            return

        if not MIN_TURN_TIMEOUT <= seconds <= MAX_TURN_TIMEOUT:
            await interaction.response.send_message(
                f"Turn timeout must be between {MIN_TURN_TIMEOUT} and {MAX_TURN_TIMEOUT} seconds.",
                ephemeral=True,
            )
            return

        async with session.lock:
            if not self._is_live(session):
                await interaction.response.send_message(
                    "This game has ended.", ephemeral=True
                )
                return

            old_timeout = session.turn_timeout
            session.turn_timeout = seconds

        await interaction.response.send_message(
            f"✅ Turn timeout changed from **{old_timeout}s** to **{seconds}s**. "
            f"It will apply starting with the next turn.",
            ephemeral=True,
        )

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
        if not session or not session.engine:
            return
        session.msg_count += 1
        if session.msg_count >= TABLE_REFRESH_EVERY_MESSAGES:
            await self.post_table(session, message.channel)

    async def _remove_member_everywhere(self, guild_id: int, user_id: int, reason: str):
        for session in list(self.sessions.values()):
            if session.guild_id != guild_id:
                continue

            if not session.started:
                # lobby phase — just drop them from the joined list (host slot
                # stays put; a departed/banned host still just loses moderator
                # powers implicitly since they're gone, nothing else to do)
                async with session.lock:
                    if user_id in session.lobby_players:
                        del session.lobby_players[user_id]
                        bet = session.lobby_bets.pop(user_id, None)
                        if bet is not None:
                            await db.return_chips(user_id, bet)
                            await db.clear_chips_in_play(user_id)
                        if session.lobby_message:
                            try:
                                await session.lobby_message.edit(embed=LobbyView.build_embed(session))
                            except discord.NotFound:
                                pass
                continue

            if not session.engine or session.engine.player_by_id(user_id) is None:
                continue

            async with session.lock:
                if session.engine.player_by_id(user_id) is None:
                    continue
                session.engine.remove_player(user_id, reason="left_guild")

                channel = self.bot.get_channel(session.channel_id)
                if channel:
                    await channel.send(f"👋 A player {reason} and was removed from the game.")

                if session.engine.finished:
                    await self.announce_and_cleanup(session)
                else:
                    # Always restart, not just "if was_current" — see the
                    # matching comment in the /uno kick handler for why.
                    self.restart_timer(session)
                    await self.refresh_table(session)

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
    await bot.add_cog(UnoGame(bot))