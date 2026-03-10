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
    user_id:       int
    display_name:  str
    chips:         int  = 0
    hole_cards:    list = field(default_factory=list)
    bet:           int  = 0
    total_bet:     int  = 0
    folded:        bool = False
    all_in:        bool = False
    acted:         bool = False
    sitting_out:   bool = False
    pending_rebuy: int  = 0

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
class SidePot:
    amount:   int
    eligible: list

@dataclass
class HandResult:
    winners:      list        # all unique winners across all pots
    pot:          int
    summary:      str
    chip_deltas:  dict        # {user_id: net gain/loss}
    community:    list = None # board cards at time of result
    winner_ranks: dict = None # {user_id: "Flush"} etc
    pot_results: list = None  # [(amount, [winner,...]), ...] one entry per side pot
    showdown_players: list = None  # snapshot of all non-folded players at showdown
    is_over: bool = False

class PokerGame:
    SMALL_BLIND = 25
    BIG_BLIND   = 50
    MIN_BUYIN   = 50
    MAX_PLAYERS = 8

    def __init__(self):
        self.players:        list[PokerPlayer] = []
        self.pending_joins:  list[PokerPlayer] = []
        self.pending_leaves: list[int]         = []
        self.street:         Street            = Street.WAITING
        self.pot:            int               = 0
        self.community:      list[int]         = []
        self.deck:           Optional[Deck]    = None
        self.dealer_idx:     int               = 0
        self.current_idx:    int               = 0
        self.current_bet:    int               = 0
        self.last_raiser:    Optional[int]     = None
        self.last_raise_size: int             = 0
        self.hand_num:       int               = 0
        self._hand_result:   Optional[HandResult] = None
        self.side_pots:      list[SidePot]     = []
        self.banned_users:   list[int]         = []  # global table ban
        self.kicked_users:   list[int]         = []  # pending kick (force leave after hand)

    # Lobby

    def add_player(self, user_id: int, display_name: str, chips: int) -> str:
        total = len(self.players) + len(self.pending_joins)
        if total >= self.MAX_PLAYERS:
            return f"❌ Table is full ({self.MAX_PLAYERS} players max)."
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
        total = p.chips + p.pending_rebuy
        self.players.remove(p)
        return total, f"👋 **{p.display_name}** cashed out **{total}** chips."

    def queue_rebuy(self, user_id: int, amount: int) -> str:
        p = self.get_player(user_id)
        if p:
            p.pending_rebuy += amount
            return f"✅ **{p.display_name}** queued **{amount}** chips for the next hand. (Pending: **{p.pending_rebuy}**)"
        for pj in self.pending_joins:
            if pj.user_id == user_id:
                pj.chips += amount
                return f"✅ **{pj.display_name}** added **{amount}** chips. Stack at join: **{pj.chips}**."
        return "❌ You're not at the table."

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

    # Hand lifecycle

    def start_hand(self) -> tuple[bool, str]:
        for p in self.players:
            if p.pending_rebuy > 0:
                p.chips += p.pending_rebuy
                p.pending_rebuy = 0

        self._process_pending()
        active = [p for p in self.players if p.chips > 0]
        if len(active) < 2:
            return False, "❌ Need at least 2 players with chips to start."
        self.players   = active
        self.side_pots = []

        # Rotate dealer now, after _process_pending has trimmed the player list.
        # Doing it in _end_hand uses the stale pre-leave list and can corrupt the index.
        if self.players:
            self.dealer_idx = (self.dealer_idx % len(self.players) + 1) % len(self.players)

        for p in self.players:
            p.reset_for_hand()

        self.deck         = Deck()
        self.deck.shuffle()
        self.community    = []
        self.pot          = 0
        self.current_bet  = 0
        self.last_raiser  = None
        self.last_raise_size = 0
        self._hand_result = None
        self.hand_num    += 1

        n      = len(self.players)
        sb_idx = (self.dealer_idx + 1) % n
        bb_idx = (self.dealer_idx + 2) % n

        self._post_blind(sb_idx, self.SMALL_BLIND)
        self._post_blind(bb_idx, self.BIG_BLIND)
        self.current_bet           = self.BIG_BLIND
        self.players[bb_idx].acted = False

        for p in self.players:
            p.hole_cards = self.deck.draw(2)

        start            = (bb_idx + 1) % n
        self.current_idx = self._next_active_idx(start)
        self.street      = Street.PREFLOP

        dealer = self.players[self.dealer_idx].display_name
        sb     = self.players[sb_idx].display_name
        bb     = self.players[bb_idx].display_name
        return True, (
            f"🃏 **Hand #{self.hand_num}** — Dealer: {dealer} | "
            f"SB: {sb} ({self.SMALL_BLIND}) | BB: {bb} ({self.BIG_BLIND})"
        )

    def _post_blind(self, idx: int, amount: int):
        p            = self.players[idx]
        actual       = min(amount, p.chips)
        p.chips     -= actual
        p.bet       += actual
        p.total_bet += actual
        p.acted      = True
        self.pot    += actual
        if p.chips == 0:
            p.all_in = True

    # Helpers

    def get_player(self, user_id: int) -> Optional[PokerPlayer]:
        return next((p for p in self.players if p.user_id == user_id), None)

    @property
    def active_players(self) -> list[PokerPlayer]:
        return [p for p in self.players if not p.folded and not p.all_in]

    @property
    def players_in_hand(self) -> list[PokerPlayer]:
        return [p for p in self.players if not p.folded]

    @property
    def all_in_run_out(self) -> bool:
        """Zero active players — board runs itself."""
        return len(self.active_players) == 0

    def current_player(self) -> Optional[PokerPlayer]:
        if self.street in (Street.WAITING, Street.SHOWDOWN):
            return None
        if self.all_in_run_out:
            return None
        cp = self.players[self.current_idx]
        if cp.folded or cp.all_in:
            self.current_idx = self._next_active_idx(self.current_idx)
            cp = self.players[self.current_idx]
            if cp.folded or cp.all_in:
                return None
        return cp

    def is_turn(self, user_id: int) -> bool:
        cp = self.current_player()
        return cp is not None and cp.user_id == user_id

    def call_amount(self, player: PokerPlayer) -> int:
        return max(0,min(self.current_bet - player.bet, player.chips))

    # Actions

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
        others_can_call = any(
            not o.folded and not o.all_in and o.user_id != user_id
            for o in self.players
        )
        if not others_can_call:
            return False, "❌ Everyone else is all-in. You can only call or fold."
        if amount <= 0:
            return False, "❌ Raise amount must be greater than 0."

        # Minimum raise = size of the last raise, or big blind if no raise yet.
        min_raise = self.last_raise_size if self.last_raise_size > 0 else self.BIG_BLIND
        call_needed  = self.current_bet - p.bet
        total_needed = call_needed + amount
        going_all_in = total_needed >= p.chips

        # Only enforce min-raise if the player has enough chips to meet it.
        if not going_all_in and amount < min_raise:
            return False, f"❌ Minimum raise is **{min_raise}** chips. (Use All-In to go all-in for less.)"

        if total_needed > p.chips:
            total_needed = p.chips

        new_bet = p.bet + total_needed
        actual_raise = new_bet - self.current_bet
        # Track raise size for future min-raise enforcement (only if a full raise)
        if actual_raise >= min_raise:
            self.last_raise_size = actual_raise

        p.chips -= total_needed
        p.bet += total_needed
        p.total_bet += total_needed
        self.pot += total_needed

        # FIX: Only update the table's bet if they actually raised it!
        if p.bet > self.current_bet:
            self.current_bet = p.bet
            self.last_raiser = user_id

        if p.chips == 0:
            p.all_in = True
        for other in self.active_players:
            if other.user_id != user_id:
                other.acted = False
        p.acted = True
        msg = f"📈 **{p.display_name}** raises to {self.current_bet}. (Pot: {self.pot})"
        end = self._advance()
        return True, msg + ("\n" + end if end else "")

    # Advancement

    def _advance(self) -> str:
        alive = self.players_in_hand
        if len(alive) == 1:
            winner = alive[0]
            winner.chips += self.pot
            self._hand_result = self._build_fold_result(winner)

            # Capture the pot value BEFORE the hand is cleared
            pot_won = self.pot

            self._end_hand()
            return f"🏆 **{winner.display_name}** wins **{pot_won}** chips (all others folded)!"

        if self._betting_closed():
            return self._next_street()

        self.current_idx = self._next_active_idx((self.current_idx + 1) % len(self.players))
        return ""

    def _betting_closed(self) -> bool:
        """
        Betting closes when all active players have acted and matched current_bet.
        When the last active player calls all-in, they become all_in=True,
        so active_players becomes empty and this returns True immediately.
        """
        active = self.active_players
        if not active:
            return True
        for p in active:
            if not p.acted:
                return False
            if p.bet < self.current_bet and p.chips > 0:
                return False
        return True

    def _next_active_idx(self, start_idx: int) -> int:
        n = len(self.players)
        for i in range(n):
            idx = (start_idx + i) % n
            p   = self.players[idx]
            if not p.folded and not p.all_in:
                return idx
        return self.current_idx

    def _next_street(self) -> str:
        for p in self.players:
            p.reset_for_street()
        self.current_bet = 0
        self.last_raiser = None
        self.last_raise_size = 0
        n     = len(self.players)
        start = (self.dealer_idx + 1) % n
        self.current_idx = self._next_active_idx(start)

        if self.street == Street.PREFLOP:
            self.street     = Street.FLOP
            self.community += self.deck.draw(3)
            msg = f"🌊 **Flop:** {hand_str(self.community)}  |  Pot: {self.pot}"
        elif self.street == Street.FLOP:
            self.street     = Street.TURN
            self.community += self.deck.draw(1)
            msg = f"↩️ **Turn:** {hand_str(self.community)}  |  Pot: {self.pot}"
        elif self.street == Street.TURN:
            self.street     = Street.RIVER
            self.community += self.deck.draw(1)
            msg = f"🏁 **River:** {hand_str(self.community)}  |  Pot: {self.pot}"
        elif self.street == Street.RIVER:
            return self._showdown()
        else:
            return ""

        # After dealing: if 0 active players, run board automatically.
        # If 1 active player, they have no one left to bet against —
        # mark them acted and immediately advance (streets auto-run without prompting).
        active = self.active_players
        if len(active) == 0:
            tail = self._next_street()
            return msg + ("\n" + tail if tail else "")
        if len(active) == 1:
            active[0].acted = True
            tail = self._next_street()
            return msg + ("\n" + tail if tail else "")

        return msg

    # Side pots

    def _compute_side_pots(self) -> list[SidePot]:
        in_hand = self.players_in_hand
        all_p   = self.players
        levels  = sorted(set(p.total_bet for p in in_hand if p.total_bet > 0))
        pots: list[SidePot] = []
        prev = 0
        for level in levels:
            amount   = sum(min(p.total_bet, level) - min(p.total_bet, prev) for p in all_p)
            eligible = [p for p in in_hand if p.total_bet >= level]
            if amount > 0 and eligible:
                pots.append(SidePot(amount=amount, eligible=eligible))
            prev = level
        leftover = self.pot - sum(sp.amount for sp in pots)
        if leftover > 0 and pots:
            pots[-1].amount += leftover
        elif leftover > 0 and in_hand:
            pots.append(SidePot(amount=leftover, eligible=in_hand))
        return pots

    # Showdown

    def _showdown(self) -> str:
        self.street = Street.SHOWDOWN
        alive       = self.players_in_hand
        scores      = {p.user_id: evaluator.evaluate(p.hole_cards, self.community)
                       for p in alive}

        pots        = self._compute_side_pots()
        chip_deltas = {p.user_id: -p.total_bet for p in self.players}
        pot_results = []

        for sp in pots:
            best      = min(scores[p.user_id] for p in sp.eligible)
            winners   = [p for p in sp.eligible if scores[p.user_id] == best]
            each      = sp.amount // len(winners)
            remainder = sp.amount - each * len(winners)
            for i, w in enumerate(winners):
                award = each + (remainder if i == 0 else 0)
                w.chips               += award
                chip_deltas[w.user_id] += award
            pot_results.append((sp.amount, winners))

        lines = ["🃏 **Showdown!**", f"Board: {hand_str(self.community)}"]
        for p in alive:
            rank_str = evaluator.class_to_string(evaluator.get_rank_class(scores[p.user_id]))
            lines.append(f"  **{p.display_name}**: {hand_str(p.hole_cards)} → *{rank_str}*")
        lines.append("")

        if len(pots) == 1:
            amt, winners = pot_results[0]
            if len(winners) == 1:
                lines.append(f"🏆 **{winners[0].display_name}** wins **{amt}** chips!")
            else:
                each  = amt // len(winners)
                names = ", ".join(w.display_name for w in winners)
                lines.append(f"🤝 Split — **{names}** each win **{each}** chips.")
        else:
            for i, (amt, winners) in enumerate(pot_results):
                label = "Main pot" if i == 0 else f"Side pot {i}"
                each  = amt // len(winners)
                if len(winners) == 1:
                    lines.append(f"🏆 **{label}** ({amt}🪙): **{winners[0].display_name}**")
                else:
                    names = ", ".join(w.display_name for w in winners)
                    lines.append(f"🤝 **{label}** ({amt}🪙): **{names}** ({each}🪙 each)")

        seen        = set()
        all_winners = []
        for _, ws in pot_results:
            for w in ws:
                if w.user_id not in seen:
                    seen.add(w.user_id)
                    all_winners.append(w)

        winner_ranks = {
            w.user_id: evaluator.class_to_string(evaluator.get_rank_class(scores[w.user_id]))
            for w in all_winners
        }

        self.side_pots    = pots
        self._hand_result = HandResult(
            winners=all_winners,
            pot=self.pot,
            summary="\n".join(lines),
            chip_deltas=chip_deltas,
            community=list(self.community),
            winner_ranks=winner_ranks,
            pot_results=pot_results,
            showdown_players=list(alive),  # snapshot before _end_hand clears state
        )
        self._end_hand()
        return "\n".join(lines)

    def force_fold(self, user_id: int) -> tuple[bool, str]:
        """Force a player to fold regardless of turn (for kick/admin)."""
        p = self.get_player(user_id)
        if not p:
            return False, "Player not found."
        if p.folded:
            return False, "Already folded."

        # 1. Capture turn status BEFORE setting them to folded
        was_turn = self.is_turn(user_id)

        p.folded = True
        p.acted = True
        msg = f"🏳️ **{p.display_name}** was force-folded."

        # 2. Check if this leaves only 1 active player
        if len(self.players_in_hand) == 1:
            end = self._advance()
            return True, msg + ("\n" + end if end else "")

        # 3. Check if folding them closes betting
        if self._betting_closed():
            end = self._next_street()
            return True, msg + ("\n" + end if end else "")

        # 4. If it was their turn, simply checking current_player() naturally glides
        # the turn index to the next active player without double-skipping them!
        if was_turn:
            self.current_player()

        return True, msg

    def _build_fold_result(self, winner: "PokerPlayer") -> HandResult:
        deltas = {p.user_id: -p.total_bet for p in self.players}
        deltas[winner.user_id] += self.pot
        lines = [f"Hand #{self.hand_num} | Pot: {self.pot}",
                 f"Winner: {winner.display_name} (all folded)"]
        for p in self.players:
            d = deltas[p.user_id]
            lines.append(f"  {p.display_name}: {'+' if d >= 0 else ''}{d}")
        return HandResult(winners=[winner], pot=self.pot,
                          summary="\n".join(lines), chip_deltas=deltas,
                          community=list(self.community),
                          showdown_players=[winner])

    def _end_hand(self):
        self.street = Street.WAITING
        # NOTE: dealer_idx is intentionally NOT rotated here.
        # Rotation happens at the top of start_hand(), AFTER _process_pending()
        # has removed any pending-leave players. Rotating here would use the
        # pre-leave player list, causing the index to point at the wrong player
        # or go out-of-bounds once leavers are removed.
        self.community = []
        self.pot       = 0
        for p in self.players:
            p.bet       = 0
            p.total_bet = 0
            p.folded    = False
            p.all_in    = False