import random
from treys import Card, Deck, Evaluator
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum, auto

evaluator = Evaluator()

SUIT_EMOJI = {"s": "♠️", "h": "♥️", "d": "♦️", "c": "♣️"}

def card_str(card: int) -> str:
    s = Card.int_to_str(card)
    return f"{s[0]}{SUIT_EMOJI.get(s[1], s[1])}"

def hand_str(cards: list[int]) -> str:
    return "  ".join(card_str(c) for c in cards)

class Street(Enum):
    WAITING  = auto()
    PREFLOP  = auto()
    FLOP     = auto()
    TURN     = auto()
    RIVER    = auto()
    SHOWDOWN = auto()

@dataclass
class PokerPlayer:
    user_id:      int
    display_name: str
    chips:        int  = 0
    hole_cards:   list = field(default_factory=list)
    bet:          int  = 0       # amount bet THIS street
    total_bet:    int  = 0       # total bet this hand
    folded:       bool = False
    all_in:       bool = False
    acted:        bool = False   # has acted at least once this street
    sitting_out:  bool = False

    def reset_for_hand(self):
        self.hole_cards  = []
        self.bet         = 0
        self.total_bet   = 0
        self.folded      = False
        self.all_in      = False
        self.acted       = False
        self.sitting_out = False

    def reset_for_street(self):
        self.bet   = 0
        self.acted = False

@dataclass
class HandResult:
    winners:     list
    pot:         int
    summary:     str
    chip_deltas: dict
    is_over:     bool = False

class PokerGame:
    SMALL_BLIND = 25
    BIG_BLIND   = 50
    MIN_BUYIN   = 50

    def __init__(self):
        self.players:           list[PokerPlayer] = []
        self.pending_joins:     list[PokerPlayer] = []
        self.pending_leaves:    list[int]         = []
        self.street:            Street            = Street.WAITING
        self.pot:               int               = 0
        self.community:         list[int]         = []
        self.deck:              Optional[Deck]    = None
        self.dealer_idx:        int               = 0
        self.current_idx:       int               = 0
        self.current_bet:       int               = 0
        self.last_raiser:       Optional[int]     = None
        self.hand_num:          int               = 0
        self._hand_result:      Optional[HandResult] = None

    # ── Lobby ──────────────────────────────────────────────────────────────

    def add_player(self, user_id: int, display_name: str, chips: int) -> str:
        if any(p.user_id == user_id for p in self.players):
            return "❌ You're already at the table."
        if any(p.user_id == user_id for p in self.pending_joins):
            return "❌ You're already waiting to join."
        if chips < self.MIN_BUYIN:
            return f"❌ Minimum buy-in is {self.MIN_BUYIN} chips."
        p = PokerPlayer(user_id, display_name, chips)
        if self.street == Street.WAITING:
            self.players.append(p)
            return f"✅ **{display_name}** joined with **{chips}** chips. ({len(self.players)} seated)"
        else:
            p.sitting_out = True
            self.pending_joins.append(p)
            return f"✅ **{display_name}** will join next hand with **{chips}** chips."

    def remove_player(self, user_id: int) -> tuple[int, str]:
        for p in self.pending_joins:
            if p.user_id == user_id:
                self.pending_joins.remove(p)
                return p.chips, f"👋 **{p.display_name}** left before joining."
        p = self.get_player(user_id)
        if not p:
            return 0, "❌ You're not at the table."
        if self.street != Street.WAITING:
            if user_id not in self.pending_leaves:
                self.pending_leaves.append(user_id)
            return 0, f"👋 **{p.display_name}** will leave after this hand."
        self.players.remove(p)
        return p.chips, f"👋 **{p.display_name}** cashed out **{p.chips}** chips."

    def _process_pending(self):
        for uid in self.pending_leaves:
            p = self.get_player(uid)
            if p:
                self.players.remove(p)
        self.pending_leaves.clear()
        for p in self.pending_joins:
            p.sitting_out = False
            self.players.append(p)
        self.pending_joins.clear()

    # ── Hand lifecycle ─────────────────────────────────────────────────────

    def start_hand(self) -> tuple[bool, str]:
        self._process_pending()
        active = [p for p in self.players if p.chips > 0]
        if len(active) < 2:
            return False, "❌ Need at least 2 players with chips to start."
        self.players = active
        for p in self.players:
            p.reset_for_hand()

        self.deck         = Deck()
        self.deck.shuffle()
        self.community    = []
        self.pot          = 0
        self.current_bet  = 0
        self.last_raiser  = None
        self._hand_result = None
        self.hand_num    += 1

        n      = len(self.players)
        sb_idx = (self.dealer_idx + 1) % n
        bb_idx = (self.dealer_idx + 2) % n

        self._post_blind(sb_idx, self.SMALL_BLIND)
        self._post_blind(bb_idx, self.BIG_BLIND)
        self.current_bet = self.BIG_BLIND

        # BB is considered to have "acted" for preflop purposes (they've put money in)
        # but they still get an option if no one raises — handled by acted flag
        self.players[bb_idx].acted = False  # BB gets their option

        for p in self.players:
            p.hole_cards = self.deck.draw(2)

        # Action starts left of BB
        self.current_idx = (bb_idx + 1) % n
        # Skip anyone who is all-in from blinds
        self.current_idx = self._next_active_idx(self.current_idx)

        self.street = Street.PREFLOP

        dealer = self.players[self.dealer_idx].display_name
        sb     = self.players[sb_idx].display_name
        bb     = self.players[bb_idx].display_name
        return True, (
            f"🃏 **Hand #{self.hand_num}** — Dealer: {dealer} | "
            f"SB: {sb} ({self.SMALL_BLIND}) | BB: {bb} ({self.BIG_BLIND})"
        )

    def _post_blind(self, idx: int, amount: int):
        p = self.players[idx]
        actual = min(amount, p.chips)
        p.chips     -= actual
        p.bet       += actual
        p.total_bet += actual
        p.acted      = True   # blinds count as acted so they don't get skipped
        self.pot    += actual
        if p.chips == 0:
            p.all_in = True

    # ── Actions ────────────────────────────────────────────────────────────

    def get_player(self, user_id: int) -> Optional[PokerPlayer]:
        return next((p for p in self.players if p.user_id == user_id), None)

    @property
    def active_players(self) -> list[PokerPlayer]:
        """Players who can still act (not folded, not all-in)."""
        return [p for p in self.players if not p.folded and not p.all_in]

    @property
    def players_in_hand(self) -> list[PokerPlayer]:
        return [p for p in self.players if not p.folded]

    def current_player(self) -> Optional[PokerPlayer]:
        if self.street in (Street.WAITING, Street.SHOWDOWN):
            return None
        return self.players[self.current_idx]

    def is_turn(self, user_id: int) -> bool:
        cp = self.current_player()
        return cp is not None and cp.user_id == user_id

    def call_amount(self, player: PokerPlayer) -> int:
        return min(self.current_bet - player.bet, player.chips)

    def fold(self, user_id: int) -> tuple[bool, str]:
        p = self.get_player(user_id)
        if not p or not self.is_turn(user_id):
            return False, "❌ It's not your turn."
        p.folded = True
        p.acted  = True
        msg = f"🏳️ **{p.display_name}** folds."
        end = self._advance()
        return True, msg + ("\n" + end if end else "")

    def check_or_call(self, user_id: int) -> tuple[bool, str]:
        p = self.get_player(user_id)
        if not p or not self.is_turn(user_id):
            return False, "❌ It's not your turn."
        amount = self.call_amount(p)
        if amount == 0:
            msg = f"✅ **{p.display_name}** checks."
        else:
            p.chips     -= amount
            p.bet       += amount
            p.total_bet += amount
            self.pot    += amount
            if p.chips == 0:
                p.all_in = True
            msg = f"📞 **{p.display_name}** calls {amount}. (Pot: {self.pot})"
        p.acted = True
        end = self._advance()
        return True, msg + ("\n" + end if end else "")

    def raise_bet(self, user_id: int, amount: int) -> tuple[bool, str]:
        p = self.get_player(user_id)
        if not p or not self.is_turn(user_id):
            return False, "❌ It's not your turn."
        total_needed = self.current_bet - p.bet + amount
        if total_needed > p.chips:
            total_needed = p.chips
        if amount < self.BIG_BLIND and total_needed < p.chips:
            return False, f"❌ Minimum raise is {self.BIG_BLIND}."
        p.chips     -= total_needed
        p.bet       += total_needed
        p.total_bet += total_needed
        self.pot    += total_needed
        self.current_bet  = p.bet
        self.last_raiser  = user_id
        if p.chips == 0:
            p.all_in = True
        # A raise re-opens action: everyone else needs to act again
        for other in self.active_players:
            if other.user_id != user_id:
                other.acted = False
        p.acted = True
        msg = f"📈 **{p.display_name}** raises to {self.current_bet}. (Pot: {self.pot})"
        end = self._advance()
        return True, msg + ("\n" + end if end else "")

    # ── Advancement ────────────────────────────────────────────────────────

    def _advance(self) -> str:
        # Only one player left — they win
        alive = self.players_in_hand
        if len(alive) == 1:
            winner = alive[0]
            winner.chips += self.pot
            msg = f"🏆 **{winner.display_name}** wins **{self.pot}** chips (all others folded)!"
            self._hand_result = self._build_result([winner])
            self._end_hand()
            return msg

        if self._betting_closed():
            return self._next_street()

        # Move to the next player who can act
        self.current_idx = self._next_active_idx((self.current_idx + 1) % len(self.players))
        return ""

    def _betting_closed(self) -> bool:
        """
        Betting is closed when every player who can act has:
        1. Acted at least once this street, AND
        2. Their bet matches the current bet (or they're all-in)
        """
        active = self.active_players  # not folded, not all-in
        if not active:
            return True   # everyone is all-in or folded — run out the board

        for p in active:
            if not p.acted:
                return False          # hasn't acted yet this street
            if p.bet < self.current_bet:
                return False          # hasn't matched the bet

        return True

    def _next_active_idx(self, start_idx: int) -> int:
        """Find the next index (from start_idx) of a player who can act."""
        n = len(self.players)
        for i in range(n):
            idx = (start_idx + i) % n
            p   = self.players[idx]
            if not p.folded and not p.all_in:
                return idx
        # All remaining are all-in — return current (won't be asked to act)
        return self.current_idx

    def _next_street(self) -> str:
        for p in self.players:
            p.reset_for_street()
        self.current_bet  = 0
        self.last_raiser  = None

        # First to act post-flop: first active player left of dealer
        n = len(self.players)
        start = (self.dealer_idx + 1) % n
        self.current_idx = self._next_active_idx(start)

        if self.street == Street.PREFLOP:
            self.street     = Street.FLOP
            self.community += self.deck.draw(3)
            return f"🌊 **Flop:** {hand_str(self.community)}  |  Pot: {self.pot}"
        elif self.street == Street.FLOP:
            self.street     = Street.TURN
            self.community += self.deck.draw(1)
            return f"↩️ **Turn:** {hand_str(self.community)}  |  Pot: {self.pot}"
        elif self.street == Street.TURN:
            self.street     = Street.RIVER
            self.community += self.deck.draw(1)
            return f"🏁 **River:** {hand_str(self.community)}  |  Pot: {self.pot}"
        elif self.street == Street.RIVER:
            return self._showdown()
        return ""

    def _showdown(self) -> str:
        self.street  = Street.SHOWDOWN
        alive        = self.players_in_hand
        scores       = {p.user_id: evaluator.evaluate(p.hole_cards, self.community) for p in alive}
        best         = min(scores.values())
        winners      = [p for p in alive if scores[p.user_id] == best]
        split        = self.pot // len(winners)
        for w in winners:
            w.chips += split

        lines = ["🃏 **Showdown!**", f"Board: {hand_str(self.community)}"]
        for p in alive:
            rank_str = evaluator.class_to_string(evaluator.get_rank_class(scores[p.user_id]))
            lines.append(f"  **{p.display_name}**: {hand_str(p.hole_cards)} → *{rank_str}*")
        if len(winners) == 1:
            lines.append(f"\n🏆 **{winners[0].display_name}** wins **{self.pot}** chips!")
        else:
            names = ", ".join(w.display_name for w in winners)
            lines.append(f"\n🤝 Split pot! **{names}** each win **{split}** chips.")

        self._hand_result = self._build_result(winners, scores)
        self._end_hand()
        return "\n".join(lines)

    def _build_result(self, winners: list, scores: dict = None) -> HandResult:
        deltas = {}
        for p in self.players:
            if any(w.user_id == p.user_id for w in winners):
                deltas[p.user_id] = (self.pot // len(winners)) - p.total_bet
            else:
                deltas[p.user_id] = -p.total_bet
        lines = [f"Hand #{self.hand_num} | Pot: {self.pot}"]
        lines.append("Winners: " + ", ".join(w.display_name for w in winners))
        for p in self.players:
            sign = "+" if deltas[p.user_id] >= 0 else ""
            lines.append(f"  {p.display_name}: {sign}{deltas[p.user_id]}")
        return HandResult(winners=winners, pot=self.pot,
                          summary="\n".join(lines), chip_deltas=deltas)

    def _end_hand(self):
        self.street = Street.WAITING
        if self.players:
            self.dealer_idx = (self.dealer_idx + 1) % len(self.players)
        self.community = []
        self.pot       = 0