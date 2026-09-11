from treys import Card, Deck, Evaluator
from dataclasses import dataclass, field
from typing import Optional, Callable
from enum import Enum, auto
from math import ceil
import random
import sys
import config
from . import taxation
from . import shiny_cards
from . import chaos
from . import hand_eval
import datetime

evaluator = Evaluator()

SUIT_EMOJI = {"s": "♠️", "h": "♥️", "d": "♦️", "c": "♣️"}

# Chaos: Uno Reverse — the 4 wild-card colors (images: reverse_<color>.png)
UNO_COLORS = ["red", "blue", "green", "yellow"]
UNO_COLOR_EMOJI = {"red": "🔴", "blue": "🔵", "green": "🟢", "yellow": "🟡"}

def card_str(card: int) -> str:
    s = Card.int_to_str(card)
    return f"{s[0]}{SUIT_EMOJI.get(s[1], s[1])}"

def hand_str(cards: list[int]) -> str:
    return "  ".join(card_str(c) for c in cards)

# Chaos: Blindness fog marker — matches the one card_images.py uses for the
# smudged card ART, so the text announcements below stay consistent with it.
BLIND_FOG = "🌫️❓"

def masked_hand_str(cards: list[int], blind_idx: set[int]) -> str:
    """Like hand_str(), but any index in `blind_idx` is rendered as the fog
    marker instead of the real card — used for community-card announcement
    text so a card Chaos: Blindness smudged in the board image doesn't leak
    its identity in the accompanying chat text."""
    return "  ".join(BLIND_FOG if i in blind_idx else card_str(c) for i, c in enumerate(cards))

_old_engine = sys.modules.get('poker.engine')
if _old_engine and hasattr(_old_engine, 'Street'):
    Street = _old_engine.Street
else:
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
    seat:          int  = -1
    hole_cards:    list = field(default_factory=list)
    bet:           int  = 0
    total_bet:     int  = 0
    folded:        bool = False
    all_in:        bool = False
    acted:         bool = False
    sitting_out:   bool = False
    pending_rebuy: int  = 0
    shiny_ids:     list = field(default_factory=list)  # ids of shiny cards held this hand
    uno_color:     str | None = None  # Chaos: Uno Reverse — "red"/"blue"/"green"/"yellow" if dealt one this hand
    premove:       dict | None = None
    vpip: bool = False
    # chaos card skin's batman card art rolled once per hand & kept across players hand, reset every hand
    skin_batman_rolled: bool = False
    skin_batman_card: int | None = None

    @property
    def egirl_saro(self) -> bool:
        """Back-compat shorthand: True if this player is currently holding ANY shiny card."""
        return bool(self.shiny_ids)

    def reset_for_hand(self):
        self.hole_cards  = []
        self.bet         = 0
        self.total_bet   = 0
        self.folded      = False
        self.all_in      = False
        self.acted       = False
        self.sitting_out = False
        self.shiny_ids   = []
        self.skin_batman_rolled = False
        self.skin_batman_card = None
        self.uno_color   = None
        self.premove     = None
        self.vpip        = False

    def reset_for_street(self):
        self.bet   = 0
        self.acted = False
        self.premove = None

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
    winner_ranks: dict = None # {user_id: "Flush"} etc — board 1 (or the single board, when Double Board isn't active)
    pot_results: list = None  # [(amount, [winner,...]), ...] one entry per side pot
    pot_result_meta: list = None
    showdown_players: list = None  # all players not just showdown now
    is_over: bool = False
    tax: int = 0
    allin_user_ids: set = None
    action_history: list = None
    vpip_ids: set = None
    wagers: dict = None  # {user_id: total_bet} snapshot before _end_hand zeroes it
    ragebait_target: int | None = None  # user_id cursed this hand (revealed regardless of outcome)
    ragebait_jackpot: int = 0           # chips redirected to the jackpot by Ragebait this hand
    bounty_targets: dict = None   # {hunter_uid: target_uid} — this hand's private per-player Bounty assignments
    bounty_results: list = None   # [(hunter_uid, target_uid, amount_paid), ...] — claims actually paid out this hand
    community2: list | None = None      # Double Board: the second board, only set when active
    winner_ranks2: dict = None          # Double Board: {user_id: "Flush"} etc, against community2 specifically —
                                         # kept SEPARATE from winner_ranks rather than folded into one "best hand"
                                         # dict, since a player can win board 1 with one hand class and board 2
                                         # with a completely different one in the same showdown.

class PokerGame:
    SMALL_BLIND = config.DEFAULT_SMALL_BLIND
    BIG_BLIND   = config.DEFAULT_BIG_BLIND
    MIN_BUYIN   = config.DEFAULT_MIN_BUYIN
    MAX_PLAYERS = config.MAX_PLAYERS

    def __init__(self):
        self.players:        list[PokerPlayer] = []
        self.pending_joins:  list[PokerPlayer] = []
        self.pending_leaves: list[int]         = []
        self.street:         Street            = Street.WAITING
        self.pot:            int               = 0
        self.community:      list[int]         = []
        self.deck:           Optional[Deck]    = None
        self.current_idx:    int               = 0
        self.current_bet:    int               = 0
        self.last_raiser:    Optional[int]     = None
        self.last_raise_size: int             = 0
        self.hand_num:       int               = 0
        self._hand_result:   Optional[HandResult] = None
        self.side_pots:      list[SidePot]     = []
        self.banned_users:   list[int]         = []  # global table ban
        self.kicked_users:   list[int]         = []  # pending kick (force leave after hand)
        self.shiny_holders: dict[str, set[int]] = {}   # shiny_id -> user_ids dealt it this hand
        self.chaos_modifiers: list[str] = []            # active chaos modifier ids for THIS hand
        self.max_bet_per_street: int | None = None       # set by Baby Table, else None (unlimited)
        self.pending_pot_boost: int = 0                 # Leaky Jackpot: chips to add to the pot at hand start
        self.ragebait_target: int | None = None          # Ragebait: user_id cursed this hand
        self._ragebait_jackpot_total: int = 0            # accumulated across pots/uncontested win
        self.bounty_targets: dict[int, int] = {}          # Bounty: hunter_uid -> target_uid, private per-player assignments this hand
        self.bounty_claims: list[tuple[int, int, int]] = []  # Bounty: (hunter_uid, target_uid, amount) — folds claimed so far this hand
        self.blinded_community_idx: set[int] = set()       # Blindness: indices in self.community that are smudged
        self.blinded_hole_idx: dict[int, int] = {}         # Blindness: user_id -> index in their hole_cards that's smudged
        self.community2: list[int] = []                    # Double Board: the second community board
        self.blinded_community2_idx: set[int] = set()      # Blindness: same, but for board 2
        self.community_auction_done: bool = False           # Community Auction: only ever fires once per hand
        self.uno_reverse_reminder_pending: bool = False      # Uno Reverse: True right as TURN opens with the modifier
                                                              # active, so poker.py can send its one-time reminder —
                                                              # see _next_street() and poker.py's _handle_post_action
        self.awaiting_runout_showdown: bool = False          # Community Auction × run-out (natural all-in, or "All In!"):
                                                              # True when _next_street()'s run-out cascade has been
                                                              # paused with the board fully dealt so the auction can
                                                              # run — see _next_street() and resume_runout_showdown()
        self.runout_pause_pending: bool = False              # dramatic run-out reveal: True when _next_street() has
                                                              # dealt one street of a run-out and stopped, with at
                                                              # least one more street still to come — poker.py sleeps
                                                              # a beat, shows the board as it stands, then calls
                                                              # continue_runout() to deal the next one. See both.
        self.is_chaos_hand: bool = False                 # set by poker.py: True if this table is in chaos mode this hand
        # Random events: trigger-point names reached (and independently
        # rolled a "yes") during this hand's play, not yet consumed by
        # poker.py. See chaos.py's "Timing model" notes for what each
        # trigger point means and when it's queued.
        self.pending_random_event_triggers: list[str] = []
        self.was_runout_hand: bool = False               # True once the board started running out with no more betting stops this hand (early all-in, "All In!", etc.) — poker.py uses this to fire "after_winners" instead of separate per-street triggers
        self._runout_first_reveal_queued: bool = False    # internal: has the run-out's one "first_reveal" trigger already been queued this hand
        self.action_history: list = []
        self.tax_rate, _ = taxation.get_tax_config()
        self.is_tournament = False
        self.tax_exempt = False
        self.vpip = False

        # ── AFK / decision-timeout hook ─────────────────────────────────────
        self.on_player_acted: Optional[Callable[[int], None]] = None
        # While set to a user_id, _notify_acted() will NOT fire the callback
        # for that specific user_id. Used by the AFK system so its own
        # automatic check/fold (which goes through the normal action methods)
        # doesn't get mistaken for a real decision and reset the player's own
        # consecutive-AFK counter. Any OTHER player's action (e.g. a premove
        # that fires as a result of this auto-action) is unaffected.
        self._afk_auto_user_id: Optional[int] = None

    def _notify_acted(self, user_id: int):
        """Fire on_player_acted for a genuine decision, unless this exact
        user_id is currently being auto-acted for by the AFK system."""
        if user_id == getattr(self, '_afk_auto_user_id', None):
            return
        callback = getattr(self, 'on_player_acted', None)
        if callback:
            try:
                callback(user_id)
            except Exception:
                pass

    @property
    def chip_emoji(self) -> str:
        if getattr(self, "is_tournament", False):
            return config.TOURNAMENT_CHIP_EMOJI
        return config.POKER_CHIP_EMOJI

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
            return f"❌ Minimum buy-in is {self.MIN_BUYIN} {self.chip_emoji}."

        occupied_seats = {p.seat for p in self.players + self.pending_joins}
        available_seats = [s for s in range(self.MAX_PLAYERS) if s not in occupied_seats]
        assigned_seat = available_seats[0]

        p = PokerPlayer(user_id, display_name, chips, seat=assigned_seat)

        if self.street == Street.WAITING:
            self.players.append(p)
            return f"✅ **{display_name}** joined with **{chips}** {self.chip_emoji}. ({len(self.players)} seated)"
        else:
            p.sitting_out = True
            self.pending_joins.append(p)
            return f"✅ **{display_name}** will join next hand with **{chips}** {self.chip_emoji}."

        # Fallback if somehow no one is found (shouldn't happen if game logic holds)
        return 0, self.players[0]

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
        return total, f"👋 **{p.display_name}** cashed out **{total}** {self.chip_emoji}."

    def queue_rebuy(self, user_id: int, amount: int, emoji: str = None) -> str:
        if emoji is None:
            emoji = self.chip_emoji
        p = self.get_player(user_id)
        if p:
            p.pending_rebuy += amount
            return f"✅ **{p.display_name}** queued **{amount}** {emoji} for the next hand. (Pending: **{p.pending_rebuy}**)"
        for pj in self.pending_joins:
            if pj.user_id == user_id:
                pj.chips += amount
                return f"✅ **{pj.display_name}** added **{amount}** {emoji}. Stack at join: **{pj.chips}**."
        return "❌ You're not at the table."

    # Hand lifecycle

    def start_hand(self) -> tuple[bool, str]:
        for p in self.players:
            if p.pending_rebuy > 0:
                p.chips += p.pending_rebuy
                p.pending_rebuy = 0

        # 1. Process pending leaves
        for uid in self.pending_leaves:
            p = self.get_player(uid)
            if p:
                self.players.remove(p)
        self.pending_leaves.clear()

        # 2. Append pending joins (they go to the back of the line)
        for p in self.pending_joins:
            p.sitting_out = False
            self.players.append(p)
        self.pending_joins.clear()

        # 3. Rotate the array (Button moves to the next physical chair)
        if self.hand_num > 0 and len(self.players) > 0:
            self.players.append(self.players.pop(0))

        # 4. Filter out broke or sitting out players
        self.players = [p for p in self.players if p.chips > 0 and not p.sitting_out]

        num_players = len(self.players)
        if num_players < 2:
            self.street = Street.WAITING
            return False, "❌ Need at least 2 players with chips to start."

        self.side_pots = []

        for p in self.players:
            p.reset_for_hand()

        self.deck = Deck()
        self.deck.shuffle()
        self.community = []
        self.community2 = []
        self.community_auction_done = False
        self.uno_reverse_reminder_pending = False
        self.awaiting_runout_showdown = False
        self.runout_pause_pending = False
        self.current_bet = 0
        self.last_raiser = None
        self.last_raise_size = 0

        # ── Chaos: Leaky Jackpot — its cut was already withdrawn from the DB
        # jackpot by poker.py (an async call this sync function can't make);
        # here we just add the pre-computed amount straight into the pot.
        self.pot += self.pending_pot_boost
        self.pending_pot_boost = 0

        # ── Chaos: Ragebait — curse one random player for this hand ──
        self._ragebait_jackpot_total = 0
        if "ragebait" in self.chaos_modifiers and self.players:
            self.ragebait_target = random.choice(self.players).user_id
        else:
            self.ragebait_target = None

        # ── Chaos: Bounty — every player gets their OWN private target to
        # pressure into folding: a full derangement over the seated players
        # (a single rotation of a shuffled order), so everyone hunts
        # exactly one other player and is hunted by exactly one other
        # player — no two players ever share a target, and nobody's ever
        # assigned to themselves. Kept entirely private; see poker.py for
        # where each player's own assignment gets surfaced to them.
        self._bounty_results: list[tuple[int, int, int]] = []
        self.bounty_claims = []
        if "bounty" in self.chaos_modifiers and len(self.players) >= 2:
            shuffled = self.players[:]
            random.shuffle(shuffled)
            n = len(shuffled)
            self.bounty_targets = {
                shuffled[i].user_id: shuffled[(i + 1) % n].user_id for i in range(n)
            }
        else:
            self.bounty_targets = {}

        # ── Random events: trigger points are queued live as they're
        # reached during the hand (see _maybe_queue_random_event_trigger
        # and _next_street below) rather than rolled once up front — just
        # reset the per-hand bookkeeping here. ──
        self.pending_random_event_triggers = []
        self.was_runout_hand = False
        self._runout_first_reveal_queued = False
        self._hand_result = None
        self.hand_num += 1

        self.action_history = []
        self.action_history.append({"type": "street", "name": "PREFLOP"})

        # ── Rigging Check ──
        # Fetch rigged data injected by tutorial_cog
        rigged_hands = getattr(self, "_rigged_hands", {})
        rigged_community = getattr(self, "_rigged_community", [])

        # Remove rigged community cards from deck to prevent duplicates
        if rigged_community:
            for c in rigged_community:
                if c in self.deck.cards:
                    self.deck.cards.remove(c)

        # ── BLIND LOGIC (Index 0 is ALWAYS Dealer) ──
        dealer_idx = 0

        if num_players == 2:
            sb_idx, bb_idx = 0, 1
            start_idx = 0
        else:
            sb_idx = 1
            bb_idx = 2 % num_players
            start_idx = 3 % num_players

        # ── Chaos: Baby Table overrides blinds + a per-street bet cap for this hand ──
        self.max_bet_per_street = None
        if "baby_table" in self.chaos_modifiers:
            baby = chaos.get("baby_table").params
            self.SMALL_BLIND = baby["small_blind"]
            self.BIG_BLIND = baby["big_blind"]
            self.max_bet_per_street = baby["max_bet_per_street"]

        # ── Chaos: Double Board doubles the blinds (applied AFTER Baby Table,
        # so if both are active it doubles Baby Table's 5/10 to 10/20 rather
        # than the two fighting over which wins) ──
        if "double_board" in self.chaos_modifiers:
            self.SMALL_BLIND *= 2
            self.BIG_BLIND *= 2

        self._post_blind(sb_idx, self.SMALL_BLIND)
        self._post_blind(bb_idx, self.BIG_BLIND)
        self.current_bet = self.BIG_BLIND
        self.players[bb_idx].acted = False

        # ── Chaos: Rising Tide sets an escalating per-street minimum contribution ──
        if "rising_tide" in self.chaos_modifiers:
            floor = chaos.get("rising_tide").params["PREFLOP"]
            self.current_bet = max(self.current_bet, floor)

        self.current_idx = self._next_active_idx(start_idx)
        self.street = Street.PREFLOP

        self.shiny_holders.clear()
        hole_count, want_pair, is_high_table_saturday, frenzy_chance = self._chaos_deal_flags()

        for p in self.players:
            self._deal_hole_cards(p, hole_count, want_pair, rigged_hands)
            self._roll_shiny_for_player(p, is_high_table_saturday, frenzy_chance)

        # ── Chaos: Uno Reverse — 30% independent chance per player to also
        # get a wild card on top of their normal hand. Not re-rolled on
        # Reshuffle — it's a separate item, not tied to hole-card identity.
        if "uno_reverse" in self.chaos_modifiers:
            uno_chance = chaos.get("uno_reverse").params["chance"]
            for p in self.players:
                if random.random() < uno_chance:
                    p.uno_color = random.choice(UNO_COLORS)

        # ── Chaos: Blindness — exactly one hole card per player is always
        # smudged (this one isn't a coin flip, unlike community cards below).
        self.blinded_community_idx = set()
        if "blindness" in self.chaos_modifiers:
            self.blinded_hole_idx = {p.user_id: random.randrange(len(p.hole_cards)) for p in self.players}
        else:
            self.blinded_hole_idx = {}

        dealer_name = self.players[dealer_idx].display_name
        sb_name = self.players[sb_idx].display_name
        bb_name = self.players[bb_idx].display_name

        return True, (
            f"🃏 **Hand #{self.hand_num}** — Button: {self.players[0].display_name} | "
            f"SB: {sb_name} ({self.SMALL_BLIND}) | BB: {bb_name} ({self.BIG_BLIND})"
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

        self.action_history.append({
            "type": "action", "player_id": p.user_id,
            "action": "post_blind", "amount": actual, "street": "PREFLOP"
        })

    def _draw_pairmageddon_hand(self, n: int) -> list[int]:
        """
        Chaos: Pairmageddon. Draws a hole-card hand guaranteed to contain a
        pocket pair (two cards of the same rank). Any additional cards
        (n > 2, i.e. Triple Threat is also active) are drawn at random.
        """
        by_rank: dict[int, list[int]] = {}
        for c in self.deck.cards:
            by_rank.setdefault(Card.get_rank_int(c), []).append(c)
        eligible_ranks = [r for r, cs in by_rank.items() if len(cs) >= 2]
        if not eligible_ranks:
            # Deck is almost exhausted (shouldn't realistically happen with
            # a 52-card deck and normal table sizes) — fall back gracefully.
            return self.deck.draw(n)

        rank = random.choice(eligible_ranks)
        pair = random.sample(by_rank[rank], 2)
        for c in pair:
            self.deck.cards.remove(c)

        hand = list(pair)
        if n > 2:
            hand += self.deck.draw(n - 2)
        return hand

    def _chaos_deal_flags(self) -> tuple[int, bool, bool, float | None]:
        """
        Reads this hand's chaos_modifiers and returns the settings that
        govern dealing: (hole_count, want_pair, is_high_table_saturday,
        frenzy_chance). Shared by the initial deal in start_hand() and by
        the Reshuffle modifier's mid-hand re-deal, so both always apply
        Triple Threat / Pairmageddon / Shiny Frenzy identically.
        """
        hole_count = 3 if "triple_hole" in self.chaos_modifiers else 2
        want_pair = "pairmageddon" in self.chaos_modifiers
        is_high_table_saturday = self.MIN_BUYIN >= 250 and datetime.datetime.now().weekday() == 5
        frenzy_active = ("shiny_frenzy" in self.chaos_modifiers
                          and len(self.players) >= chaos.get("shiny_frenzy").min_players)
        frenzy_chance = chaos.get("shiny_frenzy").params["chance"] if frenzy_active else None
        return hole_count, want_pair, is_high_table_saturday, frenzy_chance

    def _deal_hole_cards(self, p: "PokerPlayer", hole_count: int, want_pair: bool,
                          rigged_hands: dict | None = None):
        """Deals hole_count cards to a single player, honoring rigged hands
        (tutorial only) and Pairmageddon's guaranteed-pair requirement."""
        if rigged_hands and p.user_id in rigged_hands:
            p.hole_cards = rigged_hands[p.user_id]
            for c in p.hole_cards:
                if c in self.deck.cards:
                    self.deck.cards.remove(c)
        elif want_pair:
            p.hole_cards = self._draw_pairmageddon_hand(hole_count)
        else:
            p.hole_cards = self.deck.draw(hole_count)

    def _roll_shiny_for_player(self, p: "PokerPlayer", is_high_table_saturday: bool,
                                frenzy_chance: float | None):
        """Rolls shiny-card eligibility for whatever p.hole_cards currently holds."""
        for shiny in shiny_cards.SHINY_CARDS:
            if shiny.card_int not in p.hole_cards:
                continue
            if frenzy_chance is not None:
                chance = frenzy_chance
            else:
                chance = shiny.chance
                if is_high_table_saturday and shiny.saturday_chance is not None:
                    chance = shiny.saturday_chance
            if random.random() < chance:
                p.shiny_ids.append(shiny.id)
                self.shiny_holders.setdefault(shiny.id, set()).add(p.user_id)

    def _reshuffle_hands(self) -> str:
        """
        Chaos: Reshuffle. Mucks every non-folded player's hole cards back
        into the deck, shuffles, and deals fresh ones — respecting Triple
        Threat / Pairmageddon / Shiny Frenzy exactly like the original deal.
        Folded players are untouched (their cards no longer matter).
        """
        hole_count, want_pair, is_high_table_saturday, frenzy_chance = self._chaos_deal_flags()
        active = self.players_in_hand

        for p in active:
            # Drop any stale shiny bookkeeping for cards about to be mucked.
            for shiny_id in p.shiny_ids:
                self.shiny_holders.get(shiny_id, set()).discard(p.user_id)
            self.deck.cards.extend(p.hole_cards)
            p.hole_cards = []
            p.shiny_ids = []

        # NOTE: deliberately NOT self.deck.shuffle() — treys' Deck.shuffle()
        # resets self.cards to a brand-new FULL 52-card deck rather than
        # shuffling the cards already in it, which would silently make
        # already-dealt community/hole cards eligible to be drawn again
        # (duplicate cards in play). random.shuffle() shuffles the actual
        # remaining-cards list in place instead.
        random.shuffle(self.deck.cards)

        for p in active:
            self._deal_hole_cards(p, hole_count, want_pair, rigged_hands=None)
            self._roll_shiny_for_player(p, is_high_table_saturday, frenzy_chance)
            if "blindness" in self.chaos_modifiers:
                self.blinded_hole_idx[p.user_id] = random.randrange(len(p.hole_cards))

        return "🔀 The deck reshuffles — new hole cards have been dealt!"

    def swap_hands(self, user_id: int) -> tuple[bool, str, "PokerPlayer | None"]:
        """
        Chaos: Uno Reverse. Consumes the player's uno_color and swaps their
        hole cards (and any shiny status attached to those specific cards —
        shiny follows the card, not the player) with a random OTHER
        non-folded player. Valid from the moment cards are dealt up to (but
        not including) the river — "right before the last community card
        is shown", per the spec.

        Returns (success, message, target_player_or_None).
        """
        p = self.get_player(user_id)
        if not p:
            return False, "❌ You're not at this table.", None
        if not p.uno_color:
            return False, "❌ You don't have an Uno Reverse card right now.", None
        if self.street not in (Street.PREFLOP, Street.FLOP, Street.TURN):
            # Wording matches what's ACTUALLY already out — under Reverse
            # this cutoff lands the instant the real 3-card flop drops
            # (dealt on the River-named street — see the TURN reminder
            # above), not a single river card.
            what = "flop" if "reverse" in self.chaos_modifiers else "river"
            return False, f"❌ Too late — the {what}'s already out.", None

        candidates = [q for q in self.players if q.user_id != user_id and not q.folded and q.hole_cards]
        if not candidates:
            return False, "❌ No one else is available to swap with right now.", None

        target = random.choice(candidates)
        p.hole_cards, target.hole_cards = target.hole_cards, p.hole_cards
        p.shiny_ids, target.shiny_ids = target.shiny_ids, p.shiny_ids
        p.uno_color = None  # consumed

        self.action_history.append({
            "type": "action", "player_id": user_id,
            "action": "uno_swap", "target_id": target.user_id, "street": self.street.name
        })
        return True, f"🔄 **{p.display_name}** has swapped hands with **{target.display_name}**!", target

    def _find_card_location(self, card_int: int):
        """
        Chaos: Chameleon helper. Returns (owner, setter) for wherever
        card_int currently lives among either community board or any
        player's hole cards — owner is None for a community card, or the
        PokerPlayer for a hole card. Returns (None, None) if the card isn't
        live anywhere (i.e. it's sitting in the deck).
        """
        for idx, c in enumerate(self.community):
            if c == card_int:
                def setter(new_val, idx=idx):
                    self.community[idx] = new_val
                return None, setter
        for idx, c in enumerate(self.community2):
            if c == card_int:
                def setter(new_val, idx=idx):
                    self.community2[idx] = new_val
                return None, setter
        for p in self.players:
            for idx, c in enumerate(p.hole_cards):
                if c == card_int:
                    def setter(new_val, p=p, idx=idx):
                        p.hole_cards[idx] = new_val
                    return p, setter
        return None, None

    def _chameleon_swap_pass(self) -> bool:
        """
        Chaos: Chameleon. Called once per community-card reveal. Every live
        card (every player's hole cards + every community card dealt so
        far) independently has a chance to swap suits. A currently-active
        shiny card is immune, both as the card being rolled AND as a
        candidate swap target (swapping into a shiny slot would silently
        destroy someone's shiny without any of the normal shiny handling).

        Swap resolution for a triggered card:
          - If the same rank+alternate-suit card is still in the deck,
            swap directly with the deck (no duplicate ranks/suits result).
          - If that card is already live elsewhere (another hand or the
            board), the two cards trade places instead — unless the other
            slot is an active shiny, in which case a different alternate
            suit is tried.
        Returns True if at least one swap happened (used to decide whether
        to send a "cards shifted" notice).
        """
        chance = chaos.get("chameleon").params["chance"]
        ALL_SUITS = [1, 2, 4, 8]

        def is_active_shiny(card_int, owner) -> bool:
            if owner is None:
                return False
            shiny = shiny_cards.get_by_card_int(card_int)
            return shiny is not None and shiny.id in owner.shiny_ids

        # Snapshot which cards are eligible to be rolled and who (if anyone)
        # owns each, based on state at the START of this pass — a card that
        # gets swapped INTO a slot mid-pass doesn't get a second roll here.
        roll_candidates = list(self.community) + list(self.community2)
        owner_of = {c: None for c in roll_candidates}
        for p in self.players:
            for c in p.hole_cards:
                roll_candidates.append(c)
                owner_of[c] = p

        swapped_any = False
        for card_int in roll_candidates:
            if is_active_shiny(card_int, owner_of[card_int]):
                continue
            if random.random() >= chance:
                continue

            # It may have already moved from an earlier swap this same pass —
            # find where it actually is right now, not where it started.
            _, setter = self._find_card_location(card_int)
            if setter is None:
                continue  # already swapped away into the deck this pass

            rank = Card.get_rank_int(card_int)
            cur_suit = Card.get_suit_int(card_int)
            alt_suits = [s for s in ALL_SUITS if s != cur_suit]
            random.shuffle(alt_suits)

            for target_suit in alt_suits:
                target_int = Card.new(Card.STR_RANKS[rank] + Card.INT_SUIT_TO_CHAR_SUIT[target_suit])

                if target_int in self.deck.cards:
                    self.deck.cards.remove(target_int)
                    self.deck.cards.append(card_int)
                    setter(target_int)
                    swapped_any = True
                    break

                target_owner, target_setter = self._find_card_location(target_int)
                if target_setter is None:
                    continue  # defensive: not in deck and not live -- skip
                if is_active_shiny(target_int, target_owner):
                    continue  # protected -- try the next alternate suit

                setter(target_int)
                target_setter(card_int)
                swapped_any = True
                break

        return swapped_any

    # Helpers

    def get_player(self, user_id: int) -> Optional[PokerPlayer]:
        return next((p for p in self.players if p.user_id == user_id), None)

    def _pot_display(self) -> str:
        """Chaos: Hidden Pot — the string to show in place of the exact pot
        total in any public-facing message. Raise-to amounts still show
        (players need them to call correctly, per Hidden Pot's own
        design), but the running pot total itself stays masked."""
        if "hidden_pot" in self.chaos_modifiers:
            return "🙈 hidden"
        return str(self.pot)

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

        # ── Chaos: Bounty — every player privately hunts exactly one other
        # player. If the person who just folded is someone's target, and
        # the last raiser THIS street is that specific hunter (i.e. this
        # hunter is who pressured them into folding), the hunter claims a
        # bounty worth what the target had put in. Multiple claims can
        # accumulate across a single hand — each target only ever pays out
        # to their OWN assigned hunter, never anyone else's.
        if "bounty" in self.chaos_modifiers and self.last_raiser is not None and self.last_raiser != user_id:
            hunter_uid = next((h for h, t in self.bounty_targets.items() if t == user_id), None)
            if hunter_uid is not None and hunter_uid == self.last_raiser:
                self.bounty_claims.append((hunter_uid, user_id, p.total_bet))

        self.action_history.append({
            "type": "action", "player_id": user_id,
            "action": "fold", "amount": 0, "street": self.street.name
        })
        msg = f"🏳️ **{p.display_name}** folds."
        self._notify_acted(user_id)
        end = self._advance()
        return True, msg + ("\n" + end if end else "")

    def check_or_call(self, user_id: int) -> tuple[bool, str]:
        p = self.get_player(user_id)
        if not p or not self.is_turn(user_id):
            return False, "❌ It's not your turn."
        amount = self.call_amount(p)
        if amount == 0:
            # Chaos: All In! — no free checks preflop. If nothing to call,
            # you have to raise (or fold) instead of passing the action.
            if "all_in_showdown" in self.chaos_modifiers and self.street == Street.PREFLOP:
                return False, "❌ No checking under **All In!** — raise or fold."
            msg = f"✅ **{p.display_name}** checks."
        else:
            p.chips     -= amount
            p.bet       += amount
            p.total_bet += amount
            self.pot    += amount
            if p.chips == 0:
                p.all_in = True
            msg = f"📞 **{p.display_name}** calls {amount}. (Pot: {self.pot})"
            if self.street == Street.PREFLOP and amount > 0:
                p.vpip = True
        self.action_history.append({
            "type": "action", "player_id": user_id,
            "action": "check" if amount == 0 else "call",
            "amount": amount, "street": self.street.name
        })
        p.acted = True
        self._notify_acted(user_id)
        end = self._advance()
        return True, msg + ("\n" + end if end else "")

    def raise_bet(self, user_id: int, amount: int) -> tuple[bool, str]:
        p = self.get_player(user_id)
        if not p or not self.is_turn(user_id):
            return False, "❌ It's not your turn."
        if "bomb_pot" in self.chaos_modifiers and self.street == Street.PREFLOP:
            return False, "❌ No raising preflop under **Bomb Pot** — call or fold."
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

        # ── Chaos: Baby Table caps each player's total bet-this-street ──
        if self.max_bet_per_street is not None:
            room = self.max_bet_per_street - p.bet
            if room <= 0:
                return False, f"❌ This hand's {self.max_bet_per_street}-chip street cap is already maxed out."
            if total_needed > room:
                total_needed = room
                amount = max(0, total_needed - call_needed)

        going_all_in = total_needed >= p.chips

        # Only enforce min-raise if the player has enough chips to meet it,
        # and isn't being capped below it by Baby Table's street cap.
        capped_below_min = self.max_bet_per_street is not None and total_needed == (self.max_bet_per_street - p.bet) and total_needed < call_needed + min_raise
        if not going_all_in and not capped_below_min and amount < min_raise:
            return False, f"❌ Minimum raise is **{min_raise}** {self.chip_emoji}. (Use All-In to go all-in for less.)"

        if total_needed > p.chips:
            total_needed = p.chips

        new_bet = p.bet + total_needed
        actual_raise = new_bet - self.current_bet
        # Track raise size for future min-raise enforcement (only if a full raise)
        if actual_raise >= min_raise:
            self.last_raise_size = actual_raise

        if self.street == Street.PREFLOP and amount > 0:
            p.vpip = True

        self.action_history.append({
            "type": "action", "player_id": user_id,
            "action": "raise", "amount": total_needed, "street": self.street.name
        })

        p.chips -= total_needed
        p.bet += total_needed
        p.total_bet += total_needed
        self.pot += total_needed

        if p.bet > self.current_bet:
            self.current_bet = p.bet
            self.last_raiser = user_id
            for other in self.active_players:
                if other.user_id != user_id:
                    other.acted = False

        if p.chips == 0:
            p.all_in = True
        p.acted = True
        msg = f"📈 **{p.display_name}** raises to {self.current_bet}. (Pot: {self._pot_display()})"
        self._notify_acted(user_id)
        end = self._advance()
        return True, msg + ("\n" + end if end else "")

    # Advancement

    def _advance(self) -> str:
        alive = self.players_in_hand
        if len(alive) == 1:
            winner = alive[0]

            # REFUND UNCALLED OVERBET FOR FOLDS
            self._refund_uncalled_bet()

            # ── Chaos: Bounty — pay out every claim where the hunter ISN'T
            # the winner (if they are, their bounty's already folded into
            # their normal winnings, so no separate carve-out is needed).
            # Each claim is capped to whatever's left in the pot after any
            # earlier claims this same hand already took their cut.
            bounty_results: list[tuple[int, int, int]] = []
            if "bounty" in self.chaos_modifiers and self.bounty_claims:
                for hunter_uid, target_uid, amount in self.bounty_claims:
                    if hunter_uid == winner.user_id or self.pot <= 0:
                        continue
                    hunter_p = self.get_player(hunter_uid)
                    if not hunter_p:
                        continue
                    paid = min(amount, self.pot)
                    hunter_p.chips += paid
                    self.pot -= paid
                    bounty_results.append((hunter_uid, target_uid, paid))
            self._bounty_results = bounty_results

            profit = self.pot - winner.total_bet
            ragebait_active = ("ragebait" in self.chaos_modifiers
                                and self.ragebait_target == winner.user_id and profit > 0)

            if ragebait_active:
                # Chaos: Ragebait — redirect the cursed winner's take instead
                # of the normal 5% tax (their own "tax" IS this redirect).
                # Applied to PROFIT only (self.pot minus their own
                # total_bet), never the gross pot — the gross pot includes
                # the winner's OWN chips coming back to them, and taking a
                # cut of that guaranteed a net loss on nearly every win
                # (e.g. bet 100 into a 200 pot: 15% of the 200 gross is
                # only 30, so they'd net -70 despite winning). Now their
                # own contribution always comes back intact; only the
                # profit on top of it gets redirected.
                rb = chaos.get("ragebait").params
                tax = ceil(profit * rb["tax_pct"])
                player_keep = ceil(profit * rb["player_pct"])
                jackpot_cut = max(0, profit - tax - player_keep)
                self._ragebait_jackpot_total += jackpot_cut
                self.pot = winner.total_bet + player_keep
            else:
                # 🚨 5% TAX ON NET WINNINGS ONLY
                tax = 0
                if not getattr(self, 'tax_exempt', False):
                    if profit > 0:
                        tax = ceil(profit * self.tax_rate)
                self.pot -= tax

            winner.chips += self.pot
            self._hand_result = self._build_fold_result(winner, tax)

            # Capture the pot value BEFORE the hand is cleared
            self._end_hand()
            return "🏳️ All other players folded."

        if self._betting_closed():
            return self._next_street()

        self.current_idx = self._next_active_idx((self.current_idx + 1) % len(self.players))
        # Check if next player has a premove queued
        cp = self.current_player()
        if cp and cp.premove:
            fired, premove_msg = self._try_fire_premove(cp)
            if fired:
                return premove_msg
        return ""

    def _try_fire_premove(self, p: PokerPlayer) -> tuple[bool, str]:
        """
        Attempts to fire a queued premove for the given player.
        Returns True if an action was taken, False if conditions weren't met.
        Clears the premove regardless.
        """
        moves = p.premove if isinstance(p.premove, list) else ([p.premove] if p.premove else [])
        p.premove = None  # always clear, one-shot only

        for move in moves:
            if move is None:
                continue

            action = move["action"]
            call_amt = self.call_amount(p)
            success, msg = False, ""

            if action == "check" and call_amt == 0:
                success, msg = self.check_or_call(p.user_id)
            elif action == "call_any":
                success, msg = self.check_or_call(p.user_id)
            elif action == "call_upto":
                if call_amt <= move["amount"]:
                    success, msg = self.check_or_call(p.user_id)
            elif action == "fold_any":
                if call_amt == 0:
                    success, msg = self.check_or_call(p.user_id)
                else:
                    success, msg = self.fold(p.user_id)
            elif action == "fold_if_gt":
                if call_amt > move["amount"]:
                    success, msg = self.fold(p.user_id)
            elif action == "raise_all_in":
                call_needed = self.current_bet - p.bet
                raise_on_top = p.chips - call_needed
                if raise_on_top <= 0:
                    success, msg = self.check_or_call(p.user_id)
                else:
                    success, msg = self.raise_bet(p.user_id, raise_on_top)
            elif action == "raise_to":
                target_total = move["amount"]
                relative_raise = target_total - self.current_bet
                if relative_raise > 0:
                    min_raise = self.last_raise_size if self.last_raise_size > 0 else self.BIG_BLIND
                    call_needed = self.current_bet - p.bet
                    total_needed = call_needed + relative_raise
                    if total_needed >= p.chips or relative_raise >= min_raise:
                        success, msg = self.raise_bet(p.user_id, relative_raise)
            elif action == "raise_by":
                relative_raise = move["amount"]
                min_raise = self.last_raise_size if self.last_raise_size > 0 else self.BIG_BLIND
                call_needed = self.current_bet - p.bet
                total_needed = call_needed + relative_raise
                if total_needed >= p.chips or relative_raise >= min_raise:
                    success, msg = self.raise_bet(p.user_id, relative_raise)

            if success:
                parts = msg.split("\n")
                dec_parts = []
                for part in parts:
                    if part.strip():
                        if p.display_name in part:
                            dec_parts.append(f"⚡ {part.strip()}")
                        else:
                            dec_parts.append(part.strip())
                return True, "\n".join(dec_parts)

        return False, ""

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

    def _maybe_queue_random_event_trigger(self, point: str) -> None:
        """
        Random events: independently rolls chaos.RANDOM_EVENT_CHANCE for
        this specific trigger point and, on a hit, queues it onto
        pending_random_event_triggers for poker.py to drain and dispatch.
        No-op on non-chaos hands. See chaos.py's "Timing model" notes for
        the full list of trigger points and why they're fixed points
        instead of a random mid-hand moment.
        """
        if not self.is_chaos_hand:
            return
        if chaos.should_random_event_fire():
            self.pending_random_event_triggers.append(point)

    def _next_street(self) -> str:
        for p in self.players:
            p.reset_for_street()
        self.current_bet = 0
        self.last_raiser = None
        self.last_raise_size = 0

        self.current_idx = self._next_active_idx(1)

        prev_community_len = len(self.community)  # for Chaos: Blindness, below

        # Random events: is the board about to run itself out with no more
        # betting stops from here (≤1 player left who can still act)? Used
        # below to decide whether this street's reveal gets its own trigger
        # point or folds into a single "first_reveal" for the whole cascade
        # — see chaos.py's "Timing model" notes.
        entering_runout = len(self.active_players) <= 1

        # Fetch rigged community list
        rigged_comm = getattr(self, "_rigged_community", [])
        # "Reverse" is a chaos-table-only modifier and tutorials never run chaos
        # modifiers, so this never has to coexist with a rigged tutorial deck.
        reverse_active = "reverse" in self.chaos_modifiers
        tide_floors = chaos.get("rising_tide").params if "rising_tide" in self.chaos_modifiers else None

        # `label` is the announcement's lead-in text only. The actual card
        # text is rendered further down (after Chaos: Blindness has had a
        # chance to smudge any newly revealed card), so a blinded card never
        # leaks into this message — same treatment the board image already
        # gets. Do not build `msg` with hand_str() directly in the branches
        # below; set `label` and let the block after Blindness's roll do it.
        label = None

        if self.street == Street.PREFLOP:
            if "bomb_pot" in self.chaos_modifiers:
                # Chaos: Bomb Pot — skip the flop as its own street entirely;
                # flop + turn drop together, then play resumes normally from
                # the turn's betting round onward. (Excluded from combining
                # with Reverse — see chaos.py's compatibility notes: Reverse's
                # river-transition branch would otherwise try to draw 3 more
                # "flop" cards that this branch already dealt.)
                self.street = Street.TURN
                self.action_history.append({"type": "street", "name": "TURN"})
                self.community += self.deck.draw(4)
                label = "💣 **Bomb Pot!** Flop and turn drop together:"
                if tide_floors:
                    self.current_bet = max(self.current_bet, tide_floors["TURN"])
                if "all_in_showdown" in self.chaos_modifiers:
                    for p in self.players_in_hand:
                        p.all_in = True

            else:
                self.street = Street.FLOP
                self.action_history.append({"type": "street", "name": "FLOP"})
                if rigged_comm:
                    # Take first 3 cards from the rigged list
                    self.community = rigged_comm[0:3]
                    label = "🌊 **Flop:**"
                elif reverse_active:
                    # Chaos: Reverse — start from the 5th card
                    self.community += self.deck.draw(1)
                    label = "⏪ **Card revealed (river slot):**"
                else:
                    self.community += self.deck.draw(3)
                    label = "🌊 **Flop:**"
                if tide_floors:
                    self.current_bet = max(self.current_bet, tide_floors["FLOP"])

                # ── Chaos: All In! — preflop was the only betting round. Mark
                # every remaining player as "all-in" (even though their stack
                # may not literally be zero) so the existing all-in run-out
                # cascade below carries the board straight through to showdown
                # with no further betting — "just for the visual", as intended.
                if "all_in_showdown" in self.chaos_modifiers:
                    for p in self.players_in_hand:
                        p.all_in = True

        elif self.street == Street.FLOP:
            self.street = Street.TURN
            self.action_history.append({"type": "street", "name": "TURN"})
            if rigged_comm:
                # Take the 4th card (index 3)
                self.community.append(rigged_comm[3])
                label = "↩️ **Turn:**"
            elif reverse_active:
                # Chaos: Reverse — then the 4th card
                self.community += self.deck.draw(1)
                label = "⏪ **Card revealed (turn slot):**"
            else:
                self.community += self.deck.draw(1)
                label = "↩️ **Turn:**"
            if tide_floors:
                self.current_bet = max(self.current_bet, tide_floors["TURN"])

        elif self.street == Street.TURN:
            self.street = Street.RIVER
            self.action_history.append({"type": "street", "name": "RIVER"})
            if rigged_comm:
                # Take the 5th card (index 4)
                self.community.append(rigged_comm[4])
                label = "🏁 **River:**"
            elif reverse_active:
                # Chaos: Reverse — then the first 3, all together
                self.community += self.deck.draw(3)
                label = "⏪ **Final 3 cards revealed:**"
            else:
                self.community += self.deck.draw(1)
                label = "🏁 **River:**"
            if tide_floors:
                self.current_bet = max(self.current_bet, tide_floors["RIVER"])

        elif self.street == Street.RIVER:
            return self._showdown()
        else:
            return ""

        # ── Random events: queue this reveal's trigger point. A run-out
        # only ever queues ONE trigger, on whichever street starts it —
        # further streets that cascade automatically in the same run-out
        # (e.g. turn+river with nobody left to act) don't each get their
        # own roll, since they'd all land within the same second or two.
        if entering_runout:
            if not self._runout_first_reveal_queued:
                self.was_runout_hand = True
                self._runout_first_reveal_queued = True
                self._maybe_queue_random_event_trigger("first_reveal")
        else:
            self._maybe_queue_random_event_trigger(self.street.name.lower())

        # ── Chaos: Double Board — deal the same number of new cards to the
        # second board as were just dealt to the first, this same street ──
        double_board_active = "double_board" in self.chaos_modifiers
        prev_community2_len = len(self.community2)
        if double_board_active:
            n_new = len(self.community) - prev_community_len
            if n_new > 0:
                self.community2 += self.deck.draw(n_new)

        # ── Chaos: Blindness — each newly revealed community card independently
        # has a chance to come out smudged. Rolled BEFORE the announcement
        # text below is built, so a blinded card is masked in the text the
        # same way it's masked in the board image — never shown in the clear.
        if "blindness" in self.chaos_modifiers:
            blind_chance = chaos.get("blindness").params["chance"]
            for idx in range(prev_community_len, len(self.community)):
                if random.random() < blind_chance:
                    self.blinded_community_idx.add(idx)
            for idx in range(prev_community2_len, len(self.community2)):
                if random.random() < blind_chance:
                    self.blinded_community2_idx.add(idx)

        msg = f"{label} {masked_hand_str(self.community, self.blinded_community_idx)}  |  Pot: {self._pot_display()}"
        if double_board_active:
            msg += f"\n*(Board 2: {masked_hand_str(self.community2, self.blinded_community2_idx)})*"

        # ── Chaos: Uno Reverse — last-call reminder. Fires exactly once,
        # right as the TURN street opens: the last street where swap_hands()
        # still allows a swap (it gates on `self.street in (PREFLOP, FLOP,
        # TURN)` — see PokerGame.swap_hands). That gate checks the Street
        # enum name only, so this reminder lands correctly whether or not
        # "reverse" is also active — Reverse changes WHICH physical cards
        # land in which board slot and in what order they're revealed, but
        # it does not rename or reorder the PREFLOP→FLOP→TURN→RIVER street
        # sequence itself, so "the street right before RIVER" is still
        # unambiguous either way.
        #
        # This only sets a flag — it does NOT build any text here, and
        # specifically does NOT list which players actually hold a card.
        # The old version appended holder mentions straight into this
        # street's announcement, which meant merely reading who got pinged
        # told you who had a card. poker.py's _handle_post_action reads
        # this flag and sends its own standalone, generic message instead
        # — pinging EVERYONE still in the hand regardless of whether they
        # actually hold a card, and never naming holders specifically —
        # so nothing about who has one leaks either way. Set unconditionally
        # whenever the modifier is active, independent of whether anyone
        # actually got dealt a card this hand, for the same reason: firing
        # only when a holder truly exists would itself be a leak.
        if self.street == Street.TURN and "uno_reverse" in self.chaos_modifiers:
            self.uno_reverse_reminder_pending = True

        # ── Chaos: Reshuffle — a chance every street to muck & redeal hands ──
        if "reshuffle" in self.chaos_modifiers and random.random() < chaos.get("reshuffle").params["chance"]:
            msg += "\n" + self._reshuffle_hands()

        # ── Chaos: Chameleon — every live card gets a shot at swapping suits ──
        # (runs after Reshuffle so a reshuffled street's fresh cards are what
        # get scanned, never stale pre-reshuffle identities)
        if "chameleon" in self.chaos_modifiers and self._chameleon_swap_pass():
            msg += "\n🦎 The cards shimmer — some suits have changed!"

        active = self.active_players

        # ── Dramatic run-out reveal / Community Auction × run-out — a
        # run-out (natural, or the "All In!" modifier) that reaches the
        # river with nobody left to act would otherwise recurse itself
        # straight through to _showdown() with no pause at all: the
        # river's own card would get dealt and showdown would resolve in
        # this same instant, so it would never actually be SHOWN on its
        # own before winners are announced — exactly the gap the pre-river
        # streets below already close for every earlier street. So this
        # pauses here unconditionally (not just when Community Auction
        # happens to be active): control goes back to poker.py with the
        # board complete (final card dealt) and `_hand_result` still
        # unset — indistinguishable, from poker.py's side, from an
        # ordinary paused river. From there poker.py does its own
        # dramatic-reveal pause and, if Community Auction is ALSO active
        # this hand, runs the auction too (auction's own window already
        # doubles as that pause) — either way it finishes by calling
        # resume_runout_showdown() to pick up exactly where this left off.
        # The `not self.awaiting_runout_showdown` guard is this branch's
        # own one-shot latch (mirrors `community_auction_done`'s role
        # below) — resume_runout_showdown() clears the flag right before
        # calling _showdown(), so this can't re-fire for the same pause.
        if self.street == Street.RIVER and len(active) <= 1 and not self.awaiting_runout_showdown:
            if len(active) == 1:
                active[0].acted = True
            self.awaiting_runout_showdown = True
            return msg

        # ── Dramatic run-out reveal — pause between successive community
        # cards instead of dealing the whole run-out in one instantaneous
        # burst, same idea as the Community Auction pause just above (and
        # composes cleanly with it: this only ever fires when there's a
        # FURTHER street left to deal, so the river branch above — or the
        # plain non-auction river-to-showdown recursion just below — is
        # always what actually finishes a run-out, never this). Only
        # during a run-out (active ≤ 1): an ordinary hand where players
        # are still betting each street already paces itself naturally
        # through their own actions, so this never touches those.
        if len(active) <= 1 and self.street != Street.RIVER:
            if len(active) == 1:
                active[0].acted = True
            self.runout_pause_pending = True
            return msg

        if len(active) == 0:
            tail = self._next_street()
            return msg + ("\n" + tail if tail else "")
        if len(active) == 1:
            active[0].acted = True
            tail = self._next_street()
            return msg + ("\n" + tail if tail else "")

        return msg

    def resume_runout_showdown(self) -> str:
        """
        Chaos: Community Auction × run-out. Finishes what _next_street()
        deliberately paused right before (see `awaiting_runout_showdown`
        above) once poker.py's post-river auction window has closed.
        self.street is already RIVER and self.community already has its
        final 5 cards — possibly with one auction-replaced — by this
        point, exactly as if this were an ordinary non-runout river that
        had just finished its (nonexistent, for a run-out) betting round.
        """
        self.awaiting_runout_showdown = False
        return self._showdown()

    def continue_runout(self) -> str:
        """
        Dramatic run-out reveal. Resumes exactly where _next_street()
        paused itself (see `runout_pause_pending` above) to deal the NEXT
        street's card(s) — this method itself does no waiting; poker.py
        sleeps for effect, updates the board display, then calls this.
        May set runout_pause_pending again (another street still to come),
        or reach the river and hand off to Community Auction
        (awaiting_runout_showdown) or straight to showdown — exactly like
        an uninterrupted run-out always has. Only the pacing changed.
        """
        self.runout_pause_pending = False
        return self._next_street()

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

    def _pot_winning_score(self, scores: dict, eligible: list) -> int:
        """
        Returns the score that WINS a pot among the given eligible players —
        normally the LOWEST treys score (best hand), but the HIGHEST
        (worst hand) under the Losers' Hand chaos modifier. Centralized so
        every place a pot's winner gets picked (main distribution, Double
        Board's per-board split, Bounty's "did the claimant already win"
        check) flips consistently instead of only some of them.
        """
        if "losers_hand" in self.chaos_modifiers:
            return max(scores[p.user_id] for p in eligible)
        return min(scores[p.user_id] for p in eligible)

    def _showdown(self) -> str:
        self.street = Street.SHOWDOWN
        alive       = self.players_in_hand

        # 1. REFUND UNCALLED OVERBETS BEFORE BUILDING POTS
        self._refund_uncalled_bet()

        scores = {p.user_id: hand_eval.evaluate_any(evaluator, p.hole_cards, self.community)
                  for p in alive}

        # ── Chaos: Double Board — a second independent set of scores against
        # the second board. Each pot splits 50/50 between the two boards'
        # winners (any odd chip goes to board 2). Bounty is excluded from
        # this modifier (see chaos.py) specifically to avoid the ambiguity
        # of carving a bounty out of a pot that's about to be split two ways.
        double_board_active = "double_board" in self.chaos_modifiers
        scores2 = {}
        if double_board_active:
            scores2 = {p.user_id: hand_eval.evaluate_any(evaluator, p.hole_cards, self.community2)
                       for p in alive}

        pots = self._compute_side_pots()
        chip_deltas = {p.user_id: -p.total_bet for p in self.players}
        pot_results = []
        pot_result_meta = []

        # ── Chaos: Bounty — carve each claim out of the main pot first
        # (tax-exempt side payment), unless that hunter already wins the
        # main pot outright — then they just keep their normal winnings,
        # no separate redirect for that one. Multiple claims are processed
        # in the order they happened, each capped to whatever's left in
        # the main pot after any earlier claims already took their cut.
        bounty_results: list[tuple[int, int, int]] = []
        if "bounty" in self.chaos_modifiers and self.bounty_claims and pots:
            main_pot = pots[0]
            for hunter_uid, target_uid, amount in self.bounty_claims:
                if main_pot.amount <= 0:
                    break
                hunter_p = self.get_player(hunter_uid)
                if not hunter_p:
                    continue
                hunter_eligible = any(pl.user_id == hunter_uid for pl in main_pot.eligible)
                already_wins = False
                if hunter_eligible:
                    main_best = self._pot_winning_score(scores, main_pot.eligible)
                    already_wins = scores[hunter_uid] == main_best
                if already_wins:
                    continue
                paid = min(amount, main_pot.amount)
                hunter_p.chips += paid
                chip_deltas[hunter_uid] += paid
                main_pot.amount -= paid
                self.pot -= paid  # keep self.pot in sync — it's read directly below
                bounty_results.append((hunter_uid, target_uid, paid))
        self._bounty_results = bounty_results

        # 1. Distribute the raw pots normally — Ragebait's redirect is
        # applied AFTERWARD, on final net profit (see step 2 below), not
        # per-award here. A player can win multiple pots in one hand (main
        # + side pots, or both board halves under Double Board), and the
        # old version took its cut of each GROSS award independently —
        # since a gross award already includes the winner's own
        # contribution coming back to them, that meant even a completely
        # ordinary win could leave the cursed player net negative overall.
        ragebait_active = "ragebait" in self.chaos_modifiers

        def _award(w, award):
            w.chips += award
            chip_deltas[w.user_id] += award

        for pot_idx, sp in enumerate(pots):
            if double_board_active and sp.eligible:
                half1 = sp.amount // 2
                half2 = sp.amount - half1
                for board_num, (half_amount, sc) in enumerate(((half1, scores), (half2, scores2)), start=1):
                    if half_amount <= 0:
                        continue
                    best = self._pot_winning_score(sc, sp.eligible)
                    winners = [p for p in sp.eligible if sc[p.user_id] == best]
                    each = half_amount // len(winners)
                    remainder = half_amount - each * len(winners)
                    for i, w in enumerate(winners):
                        _award(w, each + (remainder if i == 0 else 0))
                    pot_results.append((half_amount, winners))
                    pot_result_meta.append((pot_idx, board_num))
            else:
                best = self._pot_winning_score(scores, sp.eligible)
                winners = [p for p in sp.eligible if scores[p.user_id] == best]
                each = sp.amount // len(winners)
                remainder = sp.amount - each * len(winners)
                for i, w in enumerate(winners):
                    _award(w, each + (remainder if i == 0 else 0))
                pot_results.append((sp.amount, winners))
                pot_result_meta.append((pot_idx, 0))

        # 2. Chaos: Ragebait — redirect the cursed player's cut, applied to
        # their FINAL NET PROFIT across the whole hand (chip_deltas already
        # starts at -total_bet for everyone, so a positive value here means
        # they're genuinely ahead once all pots are counted) — never a
        # gross per-pot amount. Their own contribution always comes back
        # intact; only the profit sitting on top of it gets redirected. If
        # they didn't actually finish the hand in profit (e.g. they only
        # won back less than they put in across multiple pots), there's
        # nothing to redirect and they're taxed normally like anyone else.
        ragebait_tax_collected = 0
        if ragebait_active and self.ragebait_target is not None:
            rb_target = self.get_player(self.ragebait_target)
            rt_profit = chip_deltas.get(self.ragebait_target, 0)
            if rb_target and rt_profit > 0:
                rb = chaos.get("ragebait").params
                rb_tax = ceil(rt_profit * rb["tax_pct"])
                rb_player_keep = ceil(rt_profit * rb["player_pct"])
                rb_jackpot = max(0, rt_profit - rb_tax - rb_player_keep)
                reduction = rt_profit - rb_player_keep
                rb_target.chips -= reduction
                chip_deltas[self.ragebait_target] -= reduction
                ragebait_tax_collected = rb_tax
                self._ragebait_jackpot_total += rb_jackpot

        # 🚨 3. APPLY 5% TAX ONLY TO PLAYERS WITH NET PROFIT
        # (skip the Ragebait target if their redirect above actually fired —
        # their "tax" was already folded into that redirect, at the exact
        # same rate; if it DIDN'T fire — no net profit — they're taxed
        # normally like everyone else, same as the `continue` below implies
        # only when ragebait_tax_collected > 0 for them specifically)
        total_tax = ragebait_tax_collected
        if not getattr(self, 'tax_exempt', False):
            for p in self.players:
                if ragebait_active and p.user_id == self.ragebait_target and ragebait_tax_collected > 0:
                    continue
                if chip_deltas[p.user_id] > 0:  # Only tax them if they actually made a profit!
                    profit_tax = ceil(chip_deltas[p.user_id] * self.tax_rate)
                    if profit_tax > 0:
                        p.chips -= profit_tax
                        chip_deltas[p.user_id] -= profit_tax
                        total_tax += profit_tax

        lines = ["🃏 **Showdown!**",
                 f"Board 1: {hand_str(self.community)}" if double_board_active else f"Board: {hand_str(self.community)}"]
        if double_board_active:
            lines.append(f"Board 2: {hand_str(self.community2)}")

        if double_board_active:
            # pot_results holds (board1_half, board2_half) pairs per side pot
            for pot_idx in range(len(pots)):
                label_prefix = "Main pot" if pot_idx == 0 else f"Side pot {pot_idx}"
                for board_num in (1, 2):
                    ridx = pot_idx * 2 + (board_num - 1)
                    if ridx >= len(pot_results):
                        continue
                    amt, winners = pot_results[ridx]
                    if amt <= 0 or not winners:
                        continue
                    each = amt // len(winners)
                    board_label = f"{label_prefix} — Board {board_num}"
                    if len(winners) == 1:
                        lines.append(f"🏆 **{board_label}** ({amt}{self.chip_emoji}): **{winners[0].display_name}**")
                    else:
                        names = ", ".join(w.display_name for w in winners)
                        lines.append(f"🤝 **{board_label}** ({amt}{self.chip_emoji}): **{names}** ({each}{self.chip_emoji} each)")
        elif len(pots) == 1:
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
                    lines.append(f"🏆 **{label}** ({amt}{self.chip_emoji} ): **{winners[0].display_name}**")
                else:
                    names = ", ".join(w.display_name for w in winners)
                    lines.append(f"🤝 **{label}** ({amt}{self.chip_emoji} ): **{names}** ({each}{self.chip_emoji}  each)")

        seen        = set()
        all_winners = []
        for _, ws in pot_results:
            for w in ws:
                if w.user_id not in seen:
                    seen.add(w.user_id)
                    all_winners.append(w)

        winner_ranks = {}
        winner_ranks2 = {}
        for w in all_winners:
            # Board 1 (or the single board): rank strictly reflects THIS
            # board's own score for this player — never mixed with board 2.
            if w.user_id in scores:
                winner_ranks[w.user_id] = evaluator.class_to_string(evaluator.get_rank_class(scores[w.user_id]))
            if double_board_active and w.user_id in scores2:
                # Board 2 gets its own independent entry. Previously this
                # collapsed to whichever of the two boards gave the player
                # their single "best" hand overall, which meant a player
                # who won board 1 with a pair but board 2 with a flush got
                # reported as "Flush" on BOTH boards. Each board's rank now
                # only ever describes that board.
                winner_ranks2[w.user_id] = evaluator.class_to_string(evaluator.get_rank_class(scores2[w.user_id]))

        self.side_pots    = pots
        self._hand_result = HandResult(
            winners=all_winners,
            pot=self.pot - total_tax,
            summary="\n".join(lines),
            chip_deltas=chip_deltas,
            community=list(self.community),
            winner_ranks=winner_ranks,
            winner_ranks2=winner_ranks2 if double_board_active else None,
            pot_results=pot_results,
            pot_result_meta=pot_result_meta,
            showdown_players=list(self.players),
            allin_user_ids={p.user_id for p in alive if p.all_in},
            tax=total_tax,# snapshot before _end_hand clears state
            action_history = list(self.action_history),
            wagers = {p.user_id: p.total_bet for p in self.players if p.total_bet > 0},
            ragebait_target=self.ragebait_target,
            ragebait_jackpot=self._ragebait_jackpot_total,
            bounty_targets=dict(self.bounty_targets),
            bounty_results=self._bounty_results,
            community2=list(self.community2) if double_board_active else None,
        )

        self._hand_result.folded_ids = {p.user_id for p in self.players if p.folded}
        self._hand_result.vpip_ids = {p.user_id for p in self.players if p.vpip}  # 🛠️ Exported
        self._end_hand()
        return "🃏 **Showdown!**"

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

    def _build_fold_result(self, winner: "PokerPlayer", tax: int = 0) -> HandResult:
        deltas = {p.user_id: -p.total_bet for p in self.players}
        deltas[winner.user_id] += self.pot
        bounty_results = getattr(self, "_bounty_results", [])
        for hunter_uid, target_uid, paid in bounty_results:
            if hunter_uid in deltas:
                deltas[hunter_uid] += paid
        lines = [f"Hand #{self.hand_num} | Pot: {self.pot}",
                 f"Winner: {winner.display_name} (all folded)"]
        for p in self.players:
            d = deltas[p.user_id]
            lines.append(f"  {p.display_name}: {'+' if d >= 0 else ''}{d}")
        res = HandResult(winners=[winner], pot=self.pot,
                          summary="\n".join(lines), chip_deltas=deltas,
                          community=list(self.community),
                          showdown_players=list(self.players), tax=tax,
                          allin_user_ids={winner.user_id} if winner.all_in else set(),
                          action_history=list(self.action_history),
                         wagers={p.user_id: p.total_bet for p in self.players if p.total_bet > 0},
                         ragebait_target=self.ragebait_target,
                         ragebait_jackpot=self._ragebait_jackpot_total,
                         bounty_targets=dict(self.bounty_targets),
                         bounty_results=bounty_results)


        res.folded_ids = {p.user_id for p in self.players if p.folded}
        res.vpip_ids = {p.user_id for p in self.players if getattr(p, 'vpip', False)}  # 🛠️ Exported
        return res

    def _refund_uncalled_bet(self):
        max_bet = max((p.total_bet for p in self.players), default=0)
        max_betters = [p for p in self.players if p.total_bet == max_bet]
        if len(max_betters) == 1:
            p = max_betters[0]
            second_max = max((o.total_bet for o in self.players if o != p), default=0)
            uncalled = max_bet - second_max
            if uncalled > 0:
                p.chips += uncalled
                p.total_bet -= uncalled
                p.bet -= uncalled
                self.pot -= uncalled

    def _end_hand(self):
        self.street = Street.WAITING
        self.community = []
        self.pot       = 0
        for p in self.players:
            p.bet       = 0
            p.total_bet = 0
            p.folded    = False
            p.all_in    = False