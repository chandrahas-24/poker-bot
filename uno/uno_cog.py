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
import tempfile
import time
import random
import collections
import weakref
import discord
import logging
import config
from pathlib import Path
from discord import app_commands
from discord.ext import commands
from discord.ui import (
    Container, TextDisplay, Section, Thumbnail, Separator, ActionRow, Button, MediaGallery,
)
from PIL import Image

from . import uno_engine as ue
from . import cards as card_assets
from .hand_render import compose_hand_image
from .unodemo import (
    sort_hand,
    display_label,
    build_rows_with_fallback,
    sanitize_emoji_name,
    FALLBACK_EMOJI_BY_COLOR,
    MAX_BUTTONS_TOTAL,
)

log = logging.getLogger("uno")

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


TURN_TIMEOUT_SECONDS = 120
TABLE_REFRESH_EVERY_MESSAGES = 7
# MAX_BUTTONS_TOTAL is imported from unodemo (both are Discord's 5x5 = 25 button ceiling)

COLOR_TO_DISCORD = {
    "Red": discord.Color.red(),
    "Yellow": discord.Color.gold(),
    "Green": discord.Color.green(),
    "Blue": discord.Color.blue(),
}

PLACEMENT_EMOJI = {0: "<:uno_1:1536286536218443837>", 1: "<:uno_2:1536286641768108065>", 2: "<:uno_3:1536286708331446282>"}

MIN_PLAYERS_LIMIT, MAX_PLAYERS_LIMIT = 2, 20
MIN_DECKS, MAX_DECKS = 1, 6
MIN_TARGET_WINNERS, MAX_TARGET_WINNERS = 1, 19
MIN_TURN_TIMEOUT, MAX_TURN_TIMEOUT = 60, 600


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
        lines.append(f"<:uno_trophy:1536135332863676548> Finished ({len(engine.finishers)}/{engine.target_winners}): {placed}")

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
        # host is auto-added as a player (joinee) by default. Event-staff
        # permissions are checked independently of host/player status, so
        # staff can manage the lobby/game even if they're not playing.
        self.lobby_players: dict[int, str] = {host_id: host_name}
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
        self.max_players = 10
        self.num_decks = 1
        self.target_winners = 1  # how many players can finish before the game ends
        self.turn_timeout = TURN_TIMEOUT_SECONDS

    def cancel_timer(self):
        if self.timer_task and not self.timer_task.done():
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
    target_winners_input = discord.ui.TextInput(label="Winners before game ends (1–19)", max_length=2)
    turn_timeout_input = discord.ui.TextInput(label="Turn timeout seconds (60–600)", max_length=3)

    def __init__(self, cog: "UnoGame", session: GameSession):
        super().__init__()
        self.cog = cog
        self.session = session
        self.max_players_input.default = str(session.max_players)
        self.decks_input.default = str(session.num_decks)
        self.target_winners_input.default = str(session.target_winners)
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
            target_winners, ok3 = parse(self.target_winners_input, MIN_TARGET_WINNERS, MAX_TARGET_WINNERS, self.session.target_winners)
            turn_timeout, ok4 = parse(self.turn_timeout_input, MIN_TURN_TIMEOUT, MAX_TURN_TIMEOUT, self.session.turn_timeout)

            if not (ok1 and ok2 and ok3 and ok4):
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
            if target_winners >= max_players:
                await interaction.response.send_message(
                    "⚠️ Winners-before-game-ends must be less than max players (someone has to be left playing).",
                    ephemeral=True,
                )
                return

            self.session.max_players = max_players
            self.session.num_decks = decks
            self.session.target_winners = target_winners
            self.session.turn_timeout = turn_timeout

            await interaction.response.send_message("✅ Settings updated.", ephemeral=True)
            if self.session.lobby_message:
                await self.session.lobby_message.edit(embed=LobbyView.build_embed(self.session))

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await report_component_error(interaction, error, None, "LobbySettingsModal")


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
            if self.session.lobby_message:
                try:
                    await self.session.lobby_message.edit(
                        content="Lobby expired.", embed=None, view=None
                    )
                except discord.HTTPException:
                    pass
        self.stop()

    @staticmethod
    def build_embed(session: GameSession) -> discord.Embed:
        host_mention = f"<@{session.host_id}>"
        names = ", ".join(f"<@{pid}>" for pid in session.lobby_players) or "_nobody yet_"

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
        e.add_field(
            name="<:uno_trophy:1536135332863676548>  Winners",
            value=f"\u200b\u200b{session.target_winners}",
            inline=True
        )
        e.add_field(name="Joined", value=names, inline=False)
        e.set_thumbnail(url='https://media.discordapp.net/attachments/1525465986541555813/1536293002975117362/wdmbzh1.jpg?ex=6a7adfda&is=6a798e5a&hm=4bbf5ffcf806948fecbd377781edfc95f3c5c06a78cd859895930d4f7057226b&=&format=webp')
        return e

    async def _refresh(self, interaction: discord.Interaction):
        await interaction.response.edit_message(embed=self.build_embed(self.session), view=self)

    @discord.ui.button(label="Join", style=discord.ButtonStyle.success, custom_id="uno_lobby_join", row=0)
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.session.lock:
            if interaction.user.id in self.session.lobby_players:
                await interaction.response.send_message("You're already in.", ephemeral=True)
                return
            if len(self.session.lobby_players) >= self.session.max_players:
                await interaction.response.send_message("Lobby is full.", ephemeral=True)
                return
            self.session.lobby_players[interaction.user.id] = interaction.user.display_name
            await self._refresh(interaction)

    @discord.ui.button(label="Leave", style=discord.ButtonStyle.danger, custom_id="uno_lobby_leave", row=0)
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.session.lock:
            if interaction.user.id not in self.session.lobby_players:
                await interaction.response.send_message("You're not in this lobby.", ephemeral=True)
                return
            del self.session.lobby_players[interaction.user.id]
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
            if self.session.target_winners >= len(players):
                await interaction.response.send_message(
                    "⚠️ Winners-before-game-ends must be less than the player count — adjust Settings first.",
                    ephemeral=True,
                )
                return

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
                                                    target_winners=self.session.target_winners)
            except ue.UnoError as e:
                await interaction.response.send_message(f"⚠️ {e}", ephemeral=True)
                return
            self.session.started = True
            self.stop()

            # Leave the original lobby embed exactly as it is — don't edit or clear
            # it. Just disable its buttons (so Join/Leave/etc can't be clicked on a
            # stale lobby) and post the table as a brand-new message.
            for child in self.children:
                child.disabled = True
            await interaction.response.edit_message(view=self)

            await self.cog.post_table(self.session, interaction.channel)
            self.cog.restart_timer(self.session)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, custom_id="uno_lobby_cancel",row=1)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await is_event_staff(interaction):
            await interaction.response.send_message("Only event staff can cancel the lobby.", ephemeral=True)
            return
        async with self.session.lock:
            self.cog.sessions.pop(self.session.channel_id, None)
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
        hand_btn = Button(label="Hand", style=discord.ButtonStyle.secondary, custom_id="uno_hand")
        hand_btn.callback = self._hand
        row.add_item(hand_btn)
        draw_btn = Button(label="Draw", style=discord.ButtonStyle.secondary, custom_id="uno_draw")
        draw_btn.callback = self._draw
        row.add_item(draw_btn)
        table_btn = Button(label="Table", style=discord.ButtonStyle.secondary, custom_id="uno_table")
        table_btn.callback = self._table
        row.add_item(table_btn)
        callout_btn = Button(label="Callout", style=discord.ButtonStyle.danger, custom_id="uno_callout")
        callout_btn.callback = self._callout
        row.add_item(callout_btn)
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
        # first response), give the build a short budget on the RAW
        # interaction: if it finishes in time, send it directly and the
        # modal option was never needed; if it's slow OR raises, fall back
        # to the modal, which is always fast since it's just opening a text
        # field. 2s leaves a buffer under the 3s cutoff for the fallback
        # send_modal call itself.
        try:
            await self.cog.load_emoji_cache()

            async def _build_and_send():
                view = await HandPlayView.create(self.cog, session, interaction.user.id)
                if view.file:
                    await interaction.response.send_message(view=view, file=view.file, ephemeral=True)
                else:
                    await interaction.response.send_message(view=view, ephemeral=True)

            await asyncio.wait_for(_build_and_send(), timeout=2.0)
        except Exception:
            # Covers build failures (missing card art, PIL errors, etc.) AND
            # asyncio.TimeoutError from a slow build/send — in both cases we
            # haven't responded yet, so the modal is still available.
            log.exception("HandPlayView build/send failed or was too slow for user %s", interaction.user.id)
            try:
                await interaction.response.send_modal(PlayCardModal(self.cog, session))
            except discord.InteractionResponded:
                # The send_message from _send() actually landed right as
                # the timeout fired (a real race, not just slow) — nothing
                # left to do, the player already got their hand view.
                log.warning("uno_play: send succeeded right as the timeout fired for user %s", interaction.user.id)
            except discord.HTTPException:
                log.exception("uno_play: send_modal fallback also failed for user %s", interaction.user.id)

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
            uno_tag = " <:uno:1536135137232953451>" if len(p.hand) == 1 and not p.said_uno else ""
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

    def _track_view(self, view: discord.ui.View):
        self._active_views.add(view)

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

        for view in list(self._active_views):
            try:
                view.stop()
            except Exception:
                pass

        self.sessions.clear()
        log.info("UnoGame cog unloaded — timers cancelled, views stopped, sessions checkpointed and cleared.")

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

        if not engine.finishers:
            await channel.send("Game ended — no one finished (not enough players remaining).")
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

        embed.set_footer(text=f"{len(engine.finishers)} finisher(s) • {engine.num_decks} deck(s)")
        await channel.send(embed=embed)

    # ---------------- shared action handlers (used by buttons AND slash commands) ----------------

    async def handle_play_card(self, interaction: discord.Interaction, session: GameSession, card: str,
                                drew_first: bool = False):
        """Shared by the button grid, the modal fallback, /uno play, and the
        DrawFollowupView's "Play this Card" button — one code path so all
        four behave identically. drew_first just changes the wording to
        make clear this was a drew-then-played turn, not a plain play."""
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
            uno_tag = " <:uno:1536135137232953451>" if len(p.hand) == 1 and not p.said_uno else ""
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
            engine.remove_player(target_id)
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
            engine.remove_player(target_id)
            # Same reasoning as /uno kick — don't duplicate the message below
            # as the next table headline too.
            await interaction.response.send_message(f"{_mention(target_id)} left the game.", ephemeral=False)

            if engine.finished:
                await self.announce_and_cleanup(session)
            else:
                self.restart_timer(session)
                await self.refresh_table(session)

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
                "Each turn has a configurable timeout. **120 seconds by default.**\n"
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
                session.engine.remove_player(user_id)

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