"""
uno_engine.py
Pure game logic for UNO — no Discord imports, fully unit-testable on its own.

Card naming matches the project's asset convention:
    "<Color>_<Value>"   e.g. "Red_4", "Blue_Skip", "Green_Draw_2", "Yellow_Reverse"
    "Wild"
    "Wild_Draw_4"

Rules follow the standard/official UNO ruleset
(https://en.wikipedia.org/wiki/Uno_(card_game)#Official_rules).
This is an original implementation written for this project.
"""

from __future__ import annotations
import random
import re
from dataclasses import dataclass, field
from typing import Optional

COLORS = ["Red", "Yellow", "Green", "Blue"]
NUMBER_VALUES = [str(n) for n in range(10)]
ACTION_VALUES = ["Skip", "Reverse", "Draw_2"]
WILD_CARDS = ["Wild", "Wild_Draw_4"]


class UnoError(Exception):
    """Base exception for illegal moves / invalid engine calls."""


class NotYourTurn(UnoError):
    pass


class IllegalCard(UnoError):
    pass


class GameOver(UnoError):
    pass


class DeckExhausted(UnoError):
    """Raised only in the extreme edge case where there is truly nothing
    left to draw or reshuffle — should not happen in a normal game."""
    pass


# ---------------------------------------------------------------------------
# Card helpers
# ---------------------------------------------------------------------------

def build_deck(num_decks: int = 1) -> list[str]:
    """Standard 108-card deck, repeated `num_decks` times and shuffled."""
    deck: list[str] = []
    for _ in range(num_decks):
        for color in COLORS:
            deck.append(f"{color}_0")
            for value in NUMBER_VALUES[1:]:
                deck.append(f"{color}_{value}")
                deck.append(f"{color}_{value}")
            for value in ACTION_VALUES:
                deck.append(f"{color}_{value}")
                deck.append(f"{color}_{value}")
        for _ in range(4):
            deck.append("Wild")
            deck.append("Wild_Draw_4")
    random.shuffle(deck)
    return deck


def card_color(card: str) -> Optional[str]:
    if card in WILD_CARDS:
        return None
    return card.split("_", 1)[0]


def card_value(card: str) -> str:
    if card in WILD_CARDS:
        return card
    return card.split("_", 1)[1]


def is_wild(card: str) -> bool:
    return card in WILD_CARDS


_VALUE_ALIASES = {
    "skip": "Skip", "s": "Skip",
    "reverse": "Reverse", "r": "Reverse", "rev": "Reverse",
    "draw2": "Draw_2", "draw_2": "Draw_2", "+2": "Draw_2", "drawtwo": "Draw_2", "draw_two": "Draw_2",
}

_COLOR_ALIASES = {
    "red": "Red", "r": "Red",
    "yellow": "Yellow", "y": "Yellow",
    "green": "Green", "g": "Green",
    "blue": "Blue", "b": "Blue",
}


def parse_card_input(text: str) -> Optional[str]:
    """
    Best-effort parse of free-text modal input into a canonical card id.
    Accepts things like "red 4", "Red_4", "r 4", "wild +4", "wild draw 4",
    "wild4", "blue skip", "b skip", "blue draw two". Returns None if it
    can't confidently parse.
    """
    if not text:
        return None
    tokens = [t for t in re.split(r"[\s_]+", text.strip()) if t]
    if not tokens:
        return None

    if tokens[0].lower() == "wild":
        if len(tokens) == 1:
            return "Wild"
        rest = "".join(tokens[1:]).lower().replace("+", "")
        if rest in ("draw4", "draw_4", "4", "drawfour", "draw_four"):
            return "Wild_Draw_4"
        return None

    if len(tokens) < 2:
        return None
    color = _COLOR_ALIASES.get(tokens[0].lower())
    if color is None:
        return None

    value_raw = "".join(tokens[1:]).lower().replace("+", "")
    if value_raw in _VALUE_ALIASES:
        value = _VALUE_ALIASES[value_raw]
    elif value_raw.isdigit() and 0 <= int(value_raw) <= 9:
        value = value_raw
    else:
        return None
    return f"{color}_{value}"


# ---------------------------------------------------------------------------
# Player
# ---------------------------------------------------------------------------

@dataclass
class Player:
    player_id: int
    name: str
    hand: list[str] = field(default_factory=list)
    connected: bool = True
    missed_turns: int = 0          # consecutive auto-draw timeouts
    said_uno: bool = False         # true once they've called uno at 1 card
    armed_uno: bool = False        # pre-armed via the Hand toggle at 2 cards; consumed on their next play

    def to_dict(self):
        return {
            "player_id": self.player_id,
            "name": self.name,
            "hand": list(self.hand),
            "connected": self.connected,
            "missed_turns": self.missed_turns,
            "said_uno": self.said_uno,
            "armed_uno": self.armed_uno,
        }

    @classmethod
    def from_dict(cls, d):
        p = cls(player_id=d["player_id"], name=d["name"], hand=list(d["hand"]))
        p.connected = d.get("connected", True)
        p.missed_turns = d.get("missed_turns", 0)
        p.said_uno = d.get("said_uno", False)
        p.armed_uno = d.get("armed_uno", False)
        return p


# ---------------------------------------------------------------------------
# Game state
# ---------------------------------------------------------------------------

class GameState:
    """
    One running UNO game. player_id is whatever id the caller uses
    (Discord user id in practice) — nothing Discord-specific lives here.
    """

    MAX_MISSED_TURNS = 2  # consecutive auto-draws before a kick

    def __init__(self, players: list[tuple[int, str]], num_decks: int = 1,
                 hand_size: int = 7, wild_draw4_challenge: bool = True,
                 target_winners: int = 1):
        if len(players) < 2:
            raise UnoError("Need at least 2 players")

        self.num_decks = num_decks
        self.wild_draw4_challenge = wild_draw4_challenge
        self.target_winners = max(1, min(target_winners, len(players) - 1))

        self.players: list[Player] = [Player(pid, name) for pid, name in players]
        self.all_names: dict[int, str] = {pid: name for pid, name in players}  # survives players leaving/finishing
        self.finishers: list[int] = []  # player_ids in the order they emptied their hand (placement order)
        self.draw_pile: list[str] = build_deck(num_decks)
        self.discard_pile: list[str] = []

        for p in self.players:
            for _ in range(hand_size):
                p.hand.append(self._draw_raw())

        first = self._draw_raw()
        while first == "Wild_Draw_4":
            self.draw_pile.append(first)
            random.shuffle(self.draw_pile)
            first = self._draw_raw()
        self.discard_pile.append(first)

        self.current_color: str = card_color(first) or random.choice(COLORS)
        self.current_index: int = 0
        self.direction: int = 1
        self.pending_wild: bool = False
        self.pending_wild_card: Optional[str] = None
        self.pending_wd4_challenge: Optional[dict] = None  # {"wd4_player_id","target_id","prior_color"}
        self.last_wd4_by: Optional[int] = None
        self.draw2_chain_count: int = 0  # consecutive Draw_2 plays in the current unbroken chain
        self.winner: Optional[int] = None
        self.finished: bool = False
        self.callout_window: Optional[int] = None
        self.last_summary: str = f"Game started. Top card: {first}."

        if card_value(first) == "Reverse":
            self.direction = -1
        elif card_value(first) == "Skip":
            self.current_index = self._offset(1)
        elif card_value(first) == "Draw_2":
            self._force_draw(self.players[self._offset(1)], 2)
            self.current_index = self._offset(2)
            self.draw2_chain_count = 1  # so an immediate follow-up +2 correctly continues the chain at 4, not restarts at 2

    # ---------- persistence ----------

    def to_dict(self):
        return {
            "players": [p.to_dict() for p in self.players],
            "all_names": dict(self.all_names),
            "finishers": list(self.finishers),
            "target_winners": self.target_winners,
            "draw_pile": list(self.draw_pile),
            "discard_pile": list(self.discard_pile),
            "current_color": self.current_color,
            "current_index": self.current_index,
            "direction": self.direction,
            "pending_wild": self.pending_wild,
            "pending_wild_card": self.pending_wild_card,
            "pending_wd4_challenge": dict(self.pending_wd4_challenge) if self.pending_wd4_challenge else None,
            "last_wd4_by": self.last_wd4_by,
            "draw2_chain_count": self.draw2_chain_count,
            "winner": self.winner,
            "finished": self.finished,
            "callout_window": self.callout_window,
            "num_decks": self.num_decks,
            "wild_draw4_challenge": self.wild_draw4_challenge,
            "last_summary": self.last_summary,
        }

    @classmethod
    def from_dict(cls, d):
        """
        Rebuilds a GameState from a saved dict. Validates structural
        invariants instead of trusting the save blindly — a corrupted or
        hand-edited save shouldn't be able to produce a current_index that
        crashes current_player(), or a game with no players.
        """
        self = cls.__new__(cls)
        try:
            self.players = [Player.from_dict(p) for p in d["players"]]
            self.draw_pile = list(d["draw_pile"])
            self.discard_pile = list(d["discard_pile"])
            self.current_color = d["current_color"]
            self.current_index = d["current_index"]
            self.direction = d["direction"]
            self.finished = d["finished"]
        except (KeyError, TypeError) as e:
            raise UnoError(f"Corrupt save data — missing or malformed field: {e}")

        if not self.finished:
            if not self.players:
                raise UnoError("Corrupt save data — no players in an unfinished game.")
            if not isinstance(self.current_index, int) or not (0 <= self.current_index < len(self.players)):
                # repair rather than crash later on current_player()
                self.current_index = 0
        if self.direction not in (1, -1):
            self.direction = 1
        if self.current_color not in COLORS:
            self.current_color = COLORS[0]
        if not self.discard_pile:
            raise UnoError("Corrupt save data — empty discard pile.")

        self.all_names = dict(d.get("all_names", {p.player_id: p.name for p in self.players}))
        self.finishers = list(d.get("finishers", []))
        self.target_winners = d.get("target_winners", 1)
        self.pending_wild = d.get("pending_wild", False)
        self.pending_wild_card = d.get("pending_wild_card")
        self.pending_wd4_challenge = d.get("pending_wd4_challenge")
        self.last_wd4_by = d.get("last_wd4_by")
        self.draw2_chain_count = d.get("draw2_chain_count", 0)
        self.winner = d.get("winner")
        self.callout_window = d.get("callout_window")
        self.num_decks = d.get("num_decks", 1)
        self.wild_draw4_challenge = d.get("wild_draw4_challenge", True)
        self.last_summary = d.get("last_summary", "")
        return self

    # ---------- helpers ----------

    def _offset(self, steps: int) -> int:
        return (self.current_index + self.direction * steps) % len(self.players)

    def _reshuffle_if_needed(self):
        """
        Recycles the discard pile (minus its top card) back into the draw
        pile when the draw pile runs dry. Deliberately does NOT fabricate
        a brand-new deck when both piles are (nearly) empty — doing so
        would inject extra cards beyond what num_decks specifies, breaking
        card-count integrity. If there's truly nothing left to recycle,
        drawing raises DeckExhausted instead (this is an extremely rare
        edge case — it can only happen with very small custom deck/hand
        configurations, essentially never in a real game).
        """
        if not self.draw_pile:
            if len(self.discard_pile) <= 1:
                raise DeckExhausted("No cards left to draw and nothing left to reshuffle.")
            top = self.discard_pile.pop()
            self.draw_pile = self.discard_pile
            random.shuffle(self.draw_pile)
            self.discard_pile = [top]

    def _draw_raw(self) -> str:
        self._reshuffle_if_needed()
        return self.draw_pile.pop()

    def _force_draw(self, player: Player, n: int):
        for _ in range(n):
            player.hand.append(self._draw_raw())
        player.said_uno = False

    def current_player(self) -> Player:
        return self.players[self.current_index]

    def top_card(self) -> str:
        return self.discard_pile[-1]

    def player_by_id(self, player_id: int) -> Optional[Player]:
        for p in self.players:
            if p.player_id == player_id:
                return p
        return None

    def is_legal(self, player: Player, card: str) -> bool:
        if card not in player.hand:
            return False
        if is_wild(card):
            return True
        return card_color(card) == self.current_color or card_value(card) == card_value(self.top_card())

    def legal_moves(self, player: Player) -> list[str]:
        return [c for c in player.hand if self.is_legal(player, c)]

    # ---------- turn/timeout bookkeeping ----------

    def record_action(self, player_id: int):
        p = self.player_by_id(player_id)
        if p:
            p.missed_turns = 0

    def record_timeout(self) -> dict:
        """
        Called by the session layer when the current player's timer expires.
        Auto-draws one card for them, ends their turn, and kicks them if
        this is their MAX_MISSED_TURNS-th consecutive timeout in a row.
        """
        player = self.current_player()
        drawn = self._draw_raw()
        player.hand.append(drawn)
        player.said_uno = False
        player.missed_turns += 1
        self.draw2_chain_count = 0  # AFK auto-draw doesn't continue a Draw_2 chain

        kicked = player.missed_turns >= self.MAX_MISSED_TURNS
        result = {"player_id": player.player_id, "name": player.name,
                   "drawn": drawn, "kicked": kicked}

        if kicked:
            self.remove_player(player.player_id)
            self.last_summary = f"{player.name} was AFK too long and was removed from the game."
        else:
            self.current_index = self._offset(1) % max(len(self.players), 1)
            self.last_summary = f"{player.name} was AFK and auto-drew a card."

        self._check_win()
        return result

    # ---------- player join/leave ----------

    def remove_player(self, player_id: int):
        """
        Removes a player entirely (AFK kick, left guild, banned, etc).
        Their hand is shuffled back into the draw pile.

        Turn order is resolved by identity, not index arithmetic: before
        mutating the list we figure out WHICH player should hold the turn
        next (honoring the current direction), then relocate that player's
        new index afterward. A pure "shift the index down by one" approach
        only happens to give the right answer in the forward-direction
        case — in reverse direction it grabs the wrong neighbor when the
        removed player was the current one, since "next" means idx-1 there,
        not idx+1.
        """
        idx = next((i for i, p in enumerate(self.players) if p.player_id == player_id), None)
        if idx is None:
            return

        leaving = self.players[idx]
        self.draw_pile.extend(leaving.hand)
        random.shuffle(self.draw_pile)

        was_current = idx == self.current_index
        wd4_involved = self.pending_wd4_challenge is not None and player_id in (
            self.pending_wd4_challenge["wd4_player_id"], self.pending_wd4_challenge["target_id"]
        )

        if wd4_involved:
            # A pending WD4 decision involves exactly two people: the
            # player who played it (still nominally "current" — turn
            # intentionally doesn't advance until this resolves) and the
            # target who owes accept/challenge. Whichever of them just
            # left, the naive was_current logic below gets this wrong:
            # if the TARGET leaves, was_current is False (current_index
            # still points at the WD4 player), so the generic "not
            # current" branch would hand the turn right back to the WD4
            # player — who already played — leaving the game stuck with
            # no one able to act. Resolve it explicitly instead.
            wd4_info = self.pending_wd4_challenge
            if player_id == wd4_info["wd4_player_id"]:
                # WD4 player left -> no one left to enforce the penalty
                # from; the target just gets a normal turn.
                next_player_id = wd4_info["target_id"]
            else:
                # target left -> skip past them, same as if they'd
                # accepted the card and been skipped.
                next_player_id = self.players[self._offset(2)].player_id if len(self.players) > 2 else None
            self.pending_wd4_challenge = None
        elif was_current:
            next_player_id = self.players[self._offset(1)].player_id if len(self.players) > 1 else None
        else:
            next_player_id = self.current_player().player_id

        del self.players[idx]

        if not self.players:
            self.finished = True
            self.winner = None
            return

        if next_player_id is not None:
            new_idx = next((i for i, p in enumerate(self.players) if p.player_id == next_player_id), None)
            self.current_index = new_idx if new_idx is not None else 0
        else:
            self.current_index %= len(self.players)

        if self.pending_wild:
            # the departing player owed a color choice — clear the pending state
            self.pending_wild = False
            self.pending_wild_card = None

        self._check_win()

    # ---------- core actions ----------

    def play_card(self, player_id: int, card: str, chosen_color: Optional[str] = None,
                   declare_uno: bool = False) -> dict:
        if self.finished:
            raise GameOver("Game already finished")

        player = self.current_player()
        if player.player_id != player_id:
            raise NotYourTurn(f"It is not player {player_id}'s turn")
        if self.pending_wild:
            raise IllegalCard("Waiting on a color choice for the last wild card")
        if self.pending_wd4_challenge:
            raise IllegalCard("Waiting on the targeted player to accept or challenge the Wild Draw Four")
        if not self.is_legal(player, card):
            raise IllegalCard(f"{card} is not a legal play right now")

        player.hand.remove(card)
        self.discard_pile.append(card)

        effects = {"card": card, "player_id": player_id, "skipped": None,
                   "drew": None, "reversed": False, "color_pending": False}

        # If this was their last card and it's a Wild, auto-resolve the color
        # (keep the current color) instead of leaving the game waiting on a
        # choice from a player who's about to be removed from rotation.
        if is_wild(card) and chosen_color is None and len(player.hand) == 0:
            chosen_color = self.current_color if self.current_color in COLORS else random.choice(COLORS)

        if is_wild(card):
            if chosen_color is None:
                self.pending_wild = True
                self.pending_wild_card = card
                effects["color_pending"] = True
                self._post_play_uno_check(player, declare_uno)
                self._check_win()
                self.last_summary = f"{player.name} played {card} — waiting on color choice."
                return effects
            self._apply_wild_color(card, chosen_color, player, effects)
        else:
            self.current_color = card_color(card)
            self._apply_value_effects(card, player, effects)

        self._post_play_uno_check(player, declare_uno)
        self._check_win()
        self.last_summary = f"{player.name} played {card}."
        return effects

    def choose_color(self, player_id: int, color: str) -> dict:
        if not self.pending_wild:
            raise UnoError("No wild card is waiting on a color choice")
        color = color.strip().capitalize()
        if color not in COLORS:
            raise UnoError(f"Invalid color: {color}")

        player = self.player_by_id(player_id)
        if player is None or player.player_id != self.current_player().player_id:
            raise NotYourTurn("Only the player who played the wild can choose its color")

        card = self.pending_wild_card
        effects = {"card": card, "player_id": player_id, "skipped": None,
                   "drew": None, "reversed": False, "color_pending": False}
        self.pending_wild = False
        self.pending_wild_card = None
        self._apply_wild_color(card, color, player, effects)
        self._check_win()
        self.last_summary = f"{player.name} chose {color}."
        return effects

    def _apply_wild_color(self, card: str, color: str, player: Player, effects: dict):
        self.draw2_chain_count = 0  # any wild (plain or +4) breaks a Draw_2 chain
        prior_color = self.current_color  # color that was in effect before this wild
        self.current_color = color
        if card == "Wild_Draw_4":
            self.last_wd4_by = player.player_id
            target = self.players[self._offset(1)]
            # Skip the challenge window entirely if challenges are disabled,
            # or if this was the player's last card (they're about to be
            # removed from rotation as a finisher — nothing meaningful to
            # challenge against a hand that's about to disappear).
            if self.wild_draw4_challenge and len(player.hand) > 0:
                # Real official rule: WD4 can always be played, but the
                # target may challenge whether it was legal (i.e. whether
                # the player had a card matching prior_color) instead of
                # just accepting the 4-card penalty. Turn intentionally
                # does NOT advance until the target resolves this via
                # accept_wild_draw4() or challenge_wild_draw4().
                self.pending_wd4_challenge = {
                    "wd4_player_id": player.player_id,
                    "target_id": target.player_id,
                    "prior_color": prior_color,
                }
                effects["wd4_pending"] = True
                effects["target_id"] = target.player_id
            else:
                self._force_draw(target, 4)
                effects["drew"] = {"player_id": target.player_id, "n": 4}
                effects["skipped"] = target.player_id
                self.current_index = self._offset(2)
        else:
            self.current_index = self._offset(1)

    def accept_wild_draw4(self, target_id: int) -> dict:
        """The targeted player accepts the Wild Draw Four at face value: draws 4, loses their turn."""
        if not self.pending_wd4_challenge:
            raise UnoError("No Wild Draw Four is pending a decision")
        pc = self.pending_wd4_challenge
        if target_id != pc["target_id"]:
            raise NotYourTurn("Only the targeted player can accept or challenge this Wild Draw Four")

        target = self.player_by_id(target_id)
        self._force_draw(target, 4)
        self.pending_wd4_challenge = None
        self.current_index = self._offset(2)  # skip the target, move to the player after them
        self.last_summary = f"{target.name} accepted the Wild Draw Four and drew 4 cards."
        self._check_win()
        return {"target_id": target_id, "n": 4, "success": None}

    def challenge_wild_draw4(self, target_id: int) -> dict:
        """
        The targeted player challenges instead of accepting: if the player
        who played the Wild Draw Four actually had a card matching the
        color that was current beforehand, the challenge succeeds and THEY
        draw 4 instead (target's turn proceeds normally — no draw, no
        skip). If the challenge is wrong, the target draws 6 total (a
        2-card penalty on top of the original 4) and loses their turn.
        """
        if not self.pending_wd4_challenge:
            raise UnoError("No Wild Draw Four is pending a decision")
        pc = self.pending_wd4_challenge
        if target_id != pc["target_id"]:
            raise NotYourTurn("Only the targeted player can accept or challenge this Wild Draw Four")

        wd4_player = self.player_by_id(pc["wd4_player_id"])
        target = self.player_by_id(target_id)
        prior_color = pc["prior_color"]
        self.pending_wd4_challenge = None

        had_matching = wd4_player is not None and any(
            card_color(c) == prior_color for c in wd4_player.hand if not is_wild(c)
        )

        if had_matching:
            self._force_draw(wd4_player, 4)
            self.current_index = self._offset(1)  # becomes the target's turn, no penalty to them
            self.last_summary = f"{target.name} challenged successfully — {wd4_player.name} drew 4 cards instead!"
            result = {"target_id": target_id, "success": True, "penalty_to": wd4_player.player_id, "n": 4}
        else:
            self._force_draw(target, 6)
            self.current_index = self._offset(2)
            self.last_summary = f"{target.name} challenged and lost — drew 6 cards!"
            result = {"target_id": target_id, "success": False, "penalty_to": target_id, "n": 6}

        self._check_win()
        return result

    def _apply_value_effects(self, card: str, player: Player, effects: dict):
        value = card_value(card)
        if value == "Reverse":
            self.draw2_chain_count = 0
            if len(self.players) == 2:
                effects["skipped"] = self.players[self._offset(1)].player_id
                self.current_index = self._offset(2)
            else:
                self.direction *= -1
                effects["reversed"] = True
                self.current_index = self._offset(1)
        elif value == "Skip":
            self.draw2_chain_count = 0
            target = self.players[self._offset(1)]
            effects["skipped"] = target.player_id
            self.current_index = self._offset(2)
        elif value == "Draw_2":
            # Chained Draw_2: each one played back-to-back (no other card
            # type in between) increases the forced draw by another 2 —
            # 1st = 2, 2nd = 4, 3rd = 6, etc. Applied immediately to
            # whoever's next (not deferred/stackable-by-choice), and the
            # chain only resets once something other than a Draw_2 gets
            # played, or a player draws normally instead of continuing it.
            self.draw2_chain_count += 1
            amount = 2 * self.draw2_chain_count
            target = self.players[self._offset(1)]
            self._force_draw(target, amount)
            effects["drew"] = {"player_id": target.player_id, "n": amount}
            effects["skipped"] = target.player_id
            self.current_index = self._offset(2)
        else:
            self.draw2_chain_count = 0
            self.current_index = self._offset(1)

    def draw_card(self, player_id: int) -> dict:
        """
        Draws a card without ending the turn by itself — callers decide
        what happens next via pass_turn() (drew, kept it, turn over) or
        play_card() with the same card if still_playable is True (the cog
        offers this as a "Play this Card?" follow-up rather than always
        ending the turn on a draw).
        """
        if self.finished:
            raise GameOver("Game already finished")
        player = self.current_player()
        if player.player_id != player_id:
            raise NotYourTurn(f"It is not player {player_id}'s turn")
        if self.pending_wild:
            raise IllegalCard("Waiting on a color choice for the last wild card")
        if self.pending_wd4_challenge:
            raise IllegalCard("Waiting on the targeted player to accept or challenge the Wild Draw Four")

        card = self._draw_raw()
        player.hand.append(card)
        player.said_uno = False
        self.record_action(player_id)
        self.draw2_chain_count = 0  # voluntarily drawing instead of playing a Draw_2 breaks the chain
        self.last_summary = f"{player.name} drew a card."

        return {"player_id": player_id, "drawn": card, "still_playable": self.is_legal(player, card)}

    def pass_turn(self, player_id: int):
        player = self.current_player()
        if player.player_id != player_id:
            raise NotYourTurn(f"It is not player {player_id}'s turn")
        if self.pending_wd4_challenge:
            raise IllegalCard("Waiting on the targeted player to accept or challenge the Wild Draw Four")
        self.current_index = self._offset(1)

    # ---------- uno callout ----------

    def call_uno(self, player_id: int):
        player = self.player_by_id(player_id)
        if player is None:
            raise UnoError("Unknown player")
        if len(player.hand) == 1:
            player.said_uno = True
            self.callout_window = None

    def uncall_uno(self, player_id: int):
        """Reverses call_uno — lets a player un-toggle their UNO declaration
        while still at 1 card, restoring their normal callout vulnerability."""
        player = self.player_by_id(player_id)
        if player is None:
            raise UnoError("Unknown player")
        if len(player.hand) == 1:
            player.said_uno = False
            self.callout_window = player.player_id

    def _post_play_uno_check(self, player: Player, declared: bool):
        if len(player.hand) == 1:
            player.said_uno = declared
            self.callout_window = player.player_id if not declared else None
        else:
            player.said_uno = False
            if self.callout_window == player.player_id:
                self.callout_window = None

    def challenge_uno(self, accuser_id: int, target_id: int, penalty: int = 2) -> bool:
        """
        Returns True if the challenge succeeded (target draws penalty cards).
        Hardened against abuse:
          - accuser must be an active player in this game (no spectators/
            outsiders calling people out)
          - can't call yourself out
          - target must actually be at exactly 1 card with no UNO declared
            (this alone already blocks repeat-farming: after a successful
            challenge the target's hand size is no longer 1, so a second
            challenge_uno against the same target fails until they're
            legitimately back down to 1 card again)
        """
        if self.finished:
            return False
        if accuser_id == target_id:
            return False
        if self.player_by_id(accuser_id) is None:
            return False  # not a participant in this game

        target = self.player_by_id(target_id)
        if target is None:
            return False
        if len(target.hand) == 1 and not target.said_uno:
            self._force_draw(target, penalty)
            self.callout_window = None
            return True
        return False

    def challenge_uno_auto(self, accuser_id: int, penalty: int = 2) -> dict:
        """
        No-target callout: catches EVERYONE currently vulnerable (at
        exactly 1 card, hasn't declared UNO) in one shot rather than
        requiring the accuser to name a specific person. If nobody
        qualifies, the accuser eats the penalty themselves instead —
        this keeps mashing Callout on the off-chance of catching someone
        from being free.
        """
        if self.finished:
            raise GameOver("Game already finished")
        if self.player_by_id(accuser_id) is None:
            raise UnoError("Not a participant in this game")

        caught = [
            p for p in self.players
            if p.player_id != accuser_id and len(p.hand) == 1 and not p.said_uno
        ]

        if caught:
            for p in caught:
                self._force_draw(p, penalty)
            self.callout_window = None
            return {"caught": [p.player_id for p in caught], "penalty_to_accuser": False, "n": penalty}

        accuser = self.player_by_id(accuser_id)
        self._force_draw(accuser, penalty)
        return {"caught": [], "penalty_to_accuser": True, "n": penalty}

    # ---------- win check ----------

    def _finish_player(self, player_id: int):
        """
        Removes a player from active rotation because they emptied their
        hand (they're placing, not leaving) — separate from remove_player,
        which is for AFK-kicks/departures. Does not touch finishers/hand;
        caller is responsible for that.

        Uses the same identity-based relocation as remove_player rather
        than index-shift arithmetic, so it stays correct regardless of
        direction even though in practice the finishing player is never
        the current player at the point this runs (their turn has already
        advanced past them by the time _check_win calls this).
        """
        idx = next((i for i, p in enumerate(self.players) if p.player_id == player_id), None)
        if idx is None:
            return

        was_current = idx == self.current_index
        if was_current:
            next_player_id = self.players[self._offset(1)].player_id if len(self.players) > 1 else None
        else:
            next_player_id = self.current_player().player_id

        del self.players[idx]
        if not self.players:
            return

        if next_player_id is not None:
            new_idx = next((i for i, p in enumerate(self.players) if p.player_id == next_player_id), None)
            self.current_index = new_idx if new_idx is not None else 0
        else:
            self.current_index %= len(self.players)

    def _check_win(self):
        if self.finished:
            return

        # a player may have just emptied their hand — move them from active
        # rotation into the finishers/placement list. Loop defensively in
        # case more than one hits zero in the same pass.
        progressed = True
        while progressed:
            progressed = False
            for p in list(self.players):
                if len(p.hand) == 0 and p.player_id not in self.finishers:
                    self.finishers.append(p.player_id)
                    self._finish_player(p.player_id)
                    progressed = True
                    break

        if len(self.finishers) >= self.target_winners:
            self.finished = True
        elif len(self.players) <= 1:
            # everyone else finished or left — whoever's left "wins" by attrition
            if len(self.players) == 1 and self.players[0].player_id not in self.finishers:
                self.finishers.append(self.players[0].player_id)
            self.finished = True

        if self.finished:
            self.winner = self.finishers[0] if self.finishers else None
            if self.finishers:
                names = [self.all_names.get(pid, str(pid)) for pid in self.finishers]
                self.last_summary = "Game over! Placements: " + ", ".join(
                    f"{i+1}. {n}" for i, n in enumerate(names)
                )
            else:
                self.last_summary = "Game over."