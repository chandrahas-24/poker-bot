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
    added as a player by default, but retains moderator powers
    (settings/kick/start) independent of that — they can Leave and still
    run/start the game.
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
  - /uno kick <user> — host/mod can remove a player from lobby or game.
"""

import asyncio
import os
import io
import time
import random
import discord
import logging
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


def is_moderator(session: "GameSession", user: discord.Member | discord.User) -> bool:
    if user.id == session.host_id:
        return True
    perms = getattr(user, "guild_permissions", None)
    return bool(perms and (perms.manage_guild or perms.kick_members))


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
        return "Game started!"

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

    total_cards = engine.num_decks * 108
    lines.append("")
    lines.append(f"Decks: {engine.num_decks} ({total_cards} cards) | Remaining: {len(engine.draw_pile)} | Discarded: {len(engine.discard_pile)}")

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
        # host is auto-added as a player (joinee) by default, but retains
        # moderator powers (settings/kick/start) independent of whether
        # they're actually in lobby_players — they can Leave and still
        # run/start the game.
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

        # configurable via the Settings button, host-only
        self.max_players = 10
        self.num_decks = 1
        self.target_winners = 1  # how many players can finish before the game ends
        self.turn_timeout = TURN_TIMEOUT_SECONDS

    def cancel_timer(self):
        if self.timer_task and not self.timer_task.done():
            self.timer_task.cancel()
        self.timer_task = None


# ---------------------------------------------------------------------------
# Lobby view
# ---------------------------------------------------------------------------

class LobbySettingsModal(discord.ui.Modal, title="UNO — Lobby Settings"):
    max_players_input = discord.ui.TextInput(label="Max players (2–20)", max_length=2)
    decks_input = discord.ui.TextInput(label="Decks (1–6)", max_length=1)
    target_winners_input = discord.ui.TextInput(label="Winners before game ends (1–19)", max_length=2)
    turn_timeout_input = discord.ui.TextInput(label="Turn timeout seconds (15–600)", max_length=3)

    def __init__(self, cog: "UnoGame", session: GameSession):
        super().__init__()
        self.cog = cog
        self.session = session
        self.max_players_input.default = str(session.max_players)
        self.decks_input.default = str(session.num_decks)
        self.target_winners_input.default = str(session.target_winners)
        self.turn_timeout_input.default = str(session.turn_timeout)

    async def on_submit(self, interaction: discord.Interaction):
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

    @discord.ui.button(label="Join", style=discord.ButtonStyle.success, custom_id="uno_lobby_join")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id in self.session.lobby_players:
            await interaction.response.send_message("You're already in.", ephemeral=True)
            return
        if len(self.session.lobby_players) >= self.session.max_players:
            await interaction.response.send_message("Lobby is full.", ephemeral=True)
            return
        self.session.lobby_players[interaction.user.id] = interaction.user.display_name
        await self._refresh(interaction)

    @discord.ui.button(label="Leave", style=discord.ButtonStyle.secondary, custom_id="uno_lobby_leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id not in self.session.lobby_players:
            await interaction.response.send_message("You're not in this lobby.", ephemeral=True)
            return
        del self.session.lobby_players[interaction.user.id]
        await self._refresh(interaction)

    @discord.ui.button(label="Settings", style=discord.ButtonStyle.secondary, emoji="⚙️", custom_id="uno_lobby_settings")
    async def settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_moderator(self.session, interaction.user):
            await interaction.response.send_message("Only the host or a mod can change settings.", ephemeral=True)
            return
        await interaction.response.send_modal(LobbySettingsModal(self.cog, self.session))

    @discord.ui.button(label="Start Game", style=discord.ButtonStyle.primary, custom_id="uno_lobby_start")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_moderator(self.session, interaction.user):
            await interaction.response.send_message("Only the host or a mod can start the game.", ephemeral=True)
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

        random.shuffle(players)  # turn order shouldn't depend on join order
        self.session.engine = ue.GameState(players, num_decks=self.session.num_decks,
                                            target_winners=self.session.target_winners)
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

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, custom_id="uno_lobby_cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_moderator(self.session, interaction.user):
            await interaction.response.send_message("Only the host or a mod can cancel the lobby.", ephemeral=True)
            return
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
    before.
    """

    def __init__(self, cog: "UnoGame", session: GameSession, card: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.session = session
        self.card = card

    @discord.ui.button(label="Play this Card", style=discord.ButtonStyle.success)
    async def play_it(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_play_card(interaction, self.session, self.card, drew_first=True)

    @discord.ui.button(label="No", style=discord.ButtonStyle.secondary)
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        engine = self.session.engine
        if not engine or engine.finished or engine.current_player().player_id != interaction.user.id:
            await interaction.response.send_message("⚠️ Too late for that.", ephemeral=True)
            return
        engine.pass_turn(interaction.user.id)
        self.session.last_action = {"type": "draw", "actor_id": interaction.user.id}
        await interaction.response.edit_message(content=f"You drew **{self.card}** and kept it.", view=None)
        self.cog.restart_timer(self.session)
        await self.cog.refresh_table(self.session)

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        await report_component_error(interaction, error, item, "DrawFollowupView")


class ColorButtonsView(discord.ui.View):
    """
    Ephemeral 4-button color picker shown after any Wild is played —
    replaces the old text-entry modal since 4 fixed options fit buttons
    perfectly and there's no parsing to get wrong.
    """

    def __init__(self, cog: "UnoGame", session: GameSession):
        super().__init__(timeout=60)
        self.cog = cog
        self.session = session

    async def _choose(self, interaction: discord.Interaction, color: str):
        engine = self.session.engine
        try:
            effects = engine.choose_color(interaction.user.id, color)
        except ue.UnoError as e:
            await interaction.response.send_message(f"⚠️ {e}", ephemeral=True)
            return

        self.session.last_action = {"type": "color", "actor_id": interaction.user.id,
                                     "color": color, "effects": effects}
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

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        await report_component_error(interaction, error, item, "ColorButtonsView")


class CallUnoToggleView(discord.ui.View):
    """
    Single toggle button shown alongside the Hand view when a player has
    exactly 2 cards. Default color is neutral (secondary/gray); toggling
    it on switches it to green (success) to show it's armed. Arming it
    doesn't announce anything by itself — the "UNO!" channel message
    fires later, automatically, the moment they play down to 1 card
    while armed (see handle_play_card).
    """

    def __init__(self, cog: "UnoGame", session: GameSession, player_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.session = session
        self.player_id = player_id

        engine = session.engine
        player = engine.player_by_id(player_id) if engine else None
        armed = bool(player and player.armed_uno)

        button = discord.ui.Button(
            label="UNO Armed" if armed else "Call UNO",
            style=discord.ButtonStyle.success if armed else discord.ButtonStyle.secondary,
        )
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
        self._build()

    def _build(self):
        engine = self.session.engine
        player = engine.player_by_id(self.player_id)
        hand = sort_hand(list(player.hand))

        container = Container(accent_colour=discord.Color.blurple())
        container.add_item(TextDisplay(f"Your hand — {len(hand)} card{'s' if len(hand) != 1 else ''}. Tap one to play it."))

        try:
            img = compose_hand_image(hand, self.cog._card_image_cache)
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            buf.seek(0)
            filename = "hand.png"
            self.file = discord.File(buf, filename=filename)
            container.add_item(MediaGallery(discord.MediaGalleryItem(f"attachment://{filename}")))
        except (FileNotFoundError, ValueError):
            pass  # missing card art for something in hand — buttons still work fine without the image

        container.add_item(Separator(spacing=discord.SeparatorSpacing.small))

        rows, used_fallback = build_rows_with_fallback(hand)
        if used_fallback:
            container.add_item(TextDisplay("_(flat layout — color grouping didn't fit in 5 rows)_"))

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

        if len(hand) == 2:
            uno_row = ActionRow()
            uno_btn = Button(
                label="UNO Armed" if player.armed_uno else "Call UNO",
                style=discord.ButtonStyle.success if player.armed_uno else discord.ButtonStyle.secondary,
            )
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
        super().__init__(timeout=None)
        self.cog = cog
        self.session = session
        self.file: discord.File | None = None
        self._build()

    def _build(self):
        engine = self.session.engine
        color = COLOR_TO_DISCORD.get(engine.current_color, discord.Color.dark_grey()) if engine else discord.Color.blurple()

        container = Container(accent_colour=color)

        text = _build_table_text(self.session)
        self.file = self.cog._top_card_file(engine)
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

        try:
            if len(player.hand) > MAX_BUTTONS_TOTAL:
                # Too many cards for a button grid — force the text modal.
                await interaction.response.send_modal(PlayCardModal(self.cog, session))
            else:
                await self.cog.load_emoji_cache()
                view = HandPlayView(self.cog, session, interaction.user.id)
                if view.file:
                    await interaction.response.send_message(view=view, file=view.file, ephemeral=True)
                else:
                    await interaction.response.send_message(view=view, ephemeral=True)
        except Exception as e:
            # Anything goes wrong building the button grid -> fall back to
            # the modal, but LOG what actually broke instead of hiding it —
            # a silent fallback here means the button grid is broken and
            # nobody would ever find out why.
            import traceback
            print(f"[uno] HandPlayView build/send failed, falling back to modal: {e!r}")
            traceback.print_exc()
            try:
                await interaction.response.send_modal(PlayCardModal(self.cog, session))
            except discord.InteractionResponded:
                pass

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
            pc = engine.pending_wd4_challenge
            if interaction.user.id != pc["target_id"]:
                await interaction.response.send_message(
                    f"⚠️ Waiting on {_mention(pc['target_id'])} to accept or challenge the Wild Draw Four.",
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
            except Exception as e:
                print(f"[uno] failed to load card image {fname}: {e}")
        print(f"[uno] cached {len(self._card_image_cache)} card image(s) in memory")

    uno = app_commands.Group(name="uno", description="UNO game commands")

    # ---------------- card emoji (shared with the button-grid hand UI) ----------------

    async def load_emoji_cache(self):
        if self._emoji_cache_loaded:
            return
        try:
            app_emojis = await self.bot.fetch_application_emojis()
            for e in app_emojis:
                self._emoji_cache[e.name] = e
        except Exception:
            pass
        self._emoji_cache_loaded = True

    def emoji_for(self, card_name: str):
        sanitized = sanitize_emoji_name(card_name)
        if sanitized in self._emoji_cache:
            return self._emoji_cache[sanitized]
        color = card_name.split("_")[0]
        return FALLBACK_EMOJI_BY_COLOR.get(color, "🃏")

    # ---------------- table rendering ----------------

    def _square_pad_thumbnail_bytes(self, card_name: str) -> bytes | None:
        """
        Card art is tall (~2:3), but Components V2's Thumbnail crops non-
        square images to fit its roughly-square slot — cutting into the
        card face. Padding it onto a square transparent canvas first
        means only the padding gets cropped, never the actual card art.
        Built once per card name from the in-memory cache, then cached
        as PNG bytes (never touches disk).
        """
        cached = self._thumb_bytes_cache.get(card_name)
        if cached is not None:
            return cached

        img = self._card_image_cache.get(card_name)
        if img is None:
            return None

        side = max(img.width, img.height)
        canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
        canvas.paste(img, ((side - img.width) // 2, (side - img.height) // 2), img)

        buf = io.BytesIO()
        canvas.save(buf, format="PNG")
        data = buf.getvalue()
        self._thumb_bytes_cache[card_name] = data
        return data

    def _top_card_file(self, engine: ue.GameState | None) -> discord.File | None:
        if not engine or not engine.discard_pile:
            return None
        card_name = engine.top_card()
        data = self._square_pad_thumbnail_bytes(card_name)
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
        view = TableView(self, session)
        if view.file:
            msg = await channel.send(view=view, file=view.file)
        else:
            msg = await channel.send(view=view)

        session.table_message = msg
        session.msg_count = 0

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
            view = ColorButtonsView(self, session)
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

        # Drawing moves hand size away from 2 either way -> any pre-armed
        # Call UNO no longer applies to this checkpoint.
        player = engine.player_by_id(interaction.user.id)
        if player:
            player.armed_uno = False

        if result["still_playable"]:
            view = DrawFollowupView(self, session, drawn)
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
            session.last_action = {"type": "callout_miss", "actor_id": interaction.user.id, "n": result["n"]}
            await interaction.response.send_message(
                f"{_mention(interaction.user.id)} draws {result['n']} cards for a false callout.",
                ephemeral=False,
            )
        else:
            session.last_action = {"type": "callout", "actor_id": interaction.user.id,
                                    "caught": result["caught"], "n": result["n"]}
            caught_mentions = ", ".join(_mention(pid) for pid in result["caught"])
            await interaction.response.send_message(
                f"{caught_mentions} forgot to call UNO and draw {result['n']} cards each.",
                ephemeral=False,
            )

    async def handle_toggle_arm_uno(self, interaction: discord.Interaction, session: GameSession):
        """
        The Call UNO toggle now lives in the Hand view at 2 cards (not a
        table button) — arming it here just sets a flag; the actual "UNO!"
        announcement fires later, as its own plain channel message, once
        they play down to 1 card with it armed (see handle_play_card).
        """
        engine = session.engine
        if not engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        player = engine.player_by_id(interaction.user.id)
        if not player or len(player.hand) != 2:
            await interaction.response.send_message("⚠️ You don't have exactly 2 cards left.", ephemeral=True)
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

        view = CallUnoToggleView(self, session, player_id) if len(hand) == 2 else None

        try:
            img = compose_hand_image(hand, self._card_image_cache)
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            buf.seek(0)
            file = discord.File(buf, filename="hand.png")
            kwargs = {
                "content": content,
                "file": file,
                "ephemeral": True,
            }
            if view is not None:
                kwargs["view"] = view

            await interaction.response.send_message(**kwargs)
        except FileNotFoundError:
            kwargs = {
                "content": content,
                "ephemeral": True,
            }
            if view is not None:
                kwargs["view"] = view

            await interaction.response.send_message(**kwargs)
        except ValueError:
            # empty hand — compose_hand_image can't build an image for zero cards
            kwargs = {
                "content": content,
                "ephemeral": True,
            }
            if view is not None:
                kwargs["view"] = view

            await interaction.response.send_message(**kwargs)

    # ---------------- turn timer ----------------

    def restart_timer(self, session: GameSession):
        session.cancel_timer()
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

            session.timer_task = asyncio.create_task(self._timer_loop(session))

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

    async def _timer_loop(self, session: GameSession):
        try:
            await asyncio.sleep(session.turn_timeout)
        except asyncio.CancelledError:
            return

        if session.channel_id not in self.sessions or not session.engine or session.engine.finished:
            return

        engine = session.engine

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

        if engine.finished:
            await self.refresh_table(session)
            await self.announce_and_cleanup(session)
        else:
            self.restart_timer(session)
            await self.refresh_table(session)

    # ---------------- commands ----------------

    @uno.command(name="start", description="Start a new UNO lobby in this channel")
    async def start(self, interaction: discord.Interaction):
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
            if engine.current_player().player_id != interaction.user.id:
                await interaction.response.send_message("⚠️ It's not your turn.", ephemeral=True)
                return
            color_norm = color.strip().capitalize()
            if color_norm not in ue.COLORS:
                await interaction.response.send_message("⚠️ Color must be Red/Yellow/Green/Blue.", ephemeral=True)
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
            pc = engine.pending_wd4_challenge
            if interaction.user.id != pc["target_id"]:
                await interaction.response.send_message(
                    f"⚠️ Waiting on {_mention(pc['target_id'])} to accept or challenge the Wild Draw Four.", ephemeral=True
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

    @uno.command(name="calluno", description="Arm/disarm Call UNO while you're at 2 cards")
    async def calluno_cmd(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        if not session or not session.engine:
            await interaction.response.send_message("No active game here.", ephemeral=True)
            return
        await self.handle_toggle_arm_uno(interaction, session)

    @uno.command(name="end", description="End/cancel the game or lobby in this channel (host only)")
    async def end(self, interaction: discord.Interaction):
        session = self.sessions.get(interaction.channel_id)
        if not session:
            await interaction.response.send_message("No active game or lobby here.", ephemeral=True)
            return
        if not is_moderator(session, interaction.user):
            await interaction.response.send_message("Only the host (or a mod) can end this.", ephemeral=True)
            return
        session.cancel_timer()
        self.sessions.pop(interaction.channel_id, None)
        await interaction.response.send_message("Game ended.")

    @uno.command(name="kick", description="Remove a player from the lobby or game (host/mod only)")
    @app_commands.describe(user="The user to remove")
    async def kick(self, interaction: discord.Interaction, user: discord.User):
        session = self.sessions.get(interaction.channel_id)
        if not session:
            await interaction.response.send_message("No active game or lobby here.", ephemeral=True)
            return
        if not is_moderator(session, interaction.user):
            await interaction.response.send_message("Only the host or a mod can kick players.", ephemeral=True)
            return

        target_id = user.id

        if not session.started:
            # Lobby phase
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

        name = engine.player_by_id(target_id).name
        engine.remove_player(target_id)
        session.last_action = {"type": "kick", "actor_id": interaction.user.id, "target_id": target_id}
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

        engine.remove_player(target_id)
        session.last_action = {"type": "leave", "actor_id": target_id, "target_id": target_id}
        await interaction.response.send_message(f"{_mention(target_id)} left the game.", ephemeral=False)

        if engine.finished:
            await self.announce_and_cleanup(session)
        else:
            self.restart_timer(session)
            await self.refresh_table(session)

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


async def setup(bot: commands.Bot):
    await bot.add_cog(UnoGame(bot))