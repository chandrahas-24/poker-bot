"""
chaos.py



excluded combos

    double_board + execution
    
    double_board + bounty
        excluded for now
        
    reshuffle + all_in_showdown

    rising_tide + all_in_showdown
    
    bomb_pot + all_in_showdown
    
    bomb_pot + reverse
        excluded for now
"""

from __future__ import annotations

import itertools
import random
from dataclasses import dataclass, field

import discord


# tableset 
# same numbers as medium table
CHAOS_BASE_SETTINGS = {
    "small_blind": 15,
    "big_blind": 30,
    "min_wallet": 150,
    "max_wallet": 3000,
}

# closing chaos table reset to medium
MEDIUM_RESET_SETTINGS = dict(CHAOS_BASE_SETTINGS)

UNLOCK_PHRASE = "acosh"


# Modifiers
@dataclass(frozen=True)
class ChaosModifier:
    id: str
    name: str
    emoji: str
    description: str          # show per hand embed
    weight: float = 1.0
    min_players: int = 2
    implemented: bool = False
    params: dict = field(default_factory=dict)


MODIFIERS: list[ChaosModifier] = [
    ChaosModifier(
        id="triple_hole",
        name="Triple Threat",
        emoji="➕🃏",
        description="Everyone gets **3 hole cards** instead of 2.",
        weight=1.0,
        implemented=True,
    ),
    ChaosModifier(
        id="pairmageddon",
        name="Pairmageddon",
        emoji="👯",
        description="Every player is dealt a **pocket pair**",
        weight=1.0,
        implemented=True,
    ),
    ChaosModifier(
        id="double_board",
        name="Double Trouble",
        emoji="😈😈",
        description="**Two separate community boards** at once",
        weight=1.0,
        implemented=True,
    ),
    ChaosModifier(
        id="uno_reverse",
        name="Uno Reverse",
        emoji="🔄",
        description="Some hands come with a wild **Uno Reverse card**. Can be consumed "
                     "to randomly swap hands with another player any time before the river.",
        weight=1.0,
        implemented=True,
        params={"chance": 0.40},
    ),
    ChaosModifier(
        id="blindness",
        name="Blindness",
        emoji="🙈",
        description="One card in everyone's hand is unreadable, "
                     "along with each community card at a **50% chance**",
        weight=1.0,
        implemented=True,
        params={"chance": 0.50},
    ),
    ChaosModifier(
        id="execution",
        name="Execution",
        emoji="💀",
        description="The **worst hand** has a **60% chance** of getting "
                     "kicked from the table.",
        weight=0.6,
        min_players=4,
        implemented=True,
        params={"kick_chance": 0.6, "ban_seconds": 600},
    ),
    ChaosModifier(
        id="baby_table",
        name="Baby Table",
        emoji="👶",
        description="Blinds drop to **5/10** and bets are capped at **200 chips** "
                     "per street for this hand.",
        weight=0.6,
        implemented=True,
        params={"small_blind": 5, "big_blind": 10, "max_bet_per_street": 200},
    ),
    ChaosModifier(
        id="classified_report",
        name="Classified Report",
        emoji="🕵️",
        description="You can see the **next player's** hole cards, but not your own.",
        weight=1.0,
        implemented=True,
    ),
    ChaosModifier(
        id="bounty",
        name="Bounty",
        emoji="🎯💰",
        description="Everyone gets their own **private target** (check My Cards). Get them to "
                     "fold and claim the chips they put in -- but only if you don't win the pot.",
        weight=0.7,
        min_players=3,
        implemented=True,
    ),
    ChaosModifier(
        id="community_auction",
        name="Card Auction",
        emoji="🔨",
        description="Right after the river, bid your stack to **replace a community "
                     "card** of your choice. Only winners' bid gets taken",
        weight=1.0,
        implemented=True,
    ),
    ChaosModifier(
        id="reshuffle",
        name="Reshuffle",
        emoji="🔀",
        description="On the flop, turn, and river, there's a **40% chance** every hand "
                     "gets redealt.",
        weight=1.0,
        implemented=True,
        params={"chance": 0.40},
    ),
    ChaosModifier(
        id="all_in_showdown",
        name="All In!",
        emoji="🎰",
        description="Call, raise, or fold preflop, then straight to showdown.",
        weight=0.7,
        implemented=True,
    ),
    ChaosModifier(
        id="rising_tide",
        name="Rising Tide",
        emoji="🌊",
        description="Minimum bets escalate: **30 / 60 / 90 / 120** chips.",
        weight=1.0,
        implemented=True,
        params={"PREFLOP": 30, "FLOP": 60, "TURN": 90, "RIVER": 120},
    ),
    ChaosModifier(
        id="shiny_frenzy",
        name="Shiny Frenzy",
        emoji="✨",
        description="Shiny-card odds jump to **25%**, but you need **2+ shinies in "
                     "your own hand** to actually cash in the jackpot or cosmetics.",
        weight=0.75,
        min_players=6,
        implemented=True,
        params={"chance": 0.5, "min_shinies": 2},
    ),
    ChaosModifier(
        id="gamble_the_gamble",
        name="Gamble the Gamble",
        emoji="🎲",
        description="Before cards are dealt, bet up to **2000 chips** on a rank and suit "
                     "for a side-payout.",
        weight=1.0,
        implemented=True,
        params={"max_bet": 2000, "exact_mult": 10, "split_mult": 3, "rank_mult": 2, "suit_mult": 0.5},
    ),
    ChaosModifier(
        id="reverse",
        name="Reverse!",
        emoji="⏪",
        description="The board comes out **backwards**: the river first, then the "
                     "turn, then the first three cards together.",
        weight=1.0,
        implemented=True,
    ),
    ChaosModifier(
        id="leaky_jackpot",
        name="Leaky Jackpot",
        emoji="💧",
        description="The jackpot springs a leak... **2.5%** drains straight into "
                     "this hand's pot.",
        weight=0.3,
        implemented=True,
        params={"leak_pct": 0.025},
    ),
    ChaosModifier(
        id="chameleon",
        name="Chameleon",
        emoji="🦎",
        description="Every time a new community card is revealed, **every card in "
                     "play** has a **15% chance** to swap suits.",
        weight=1.0,
        implemented=True,
        params={"chance": 0.15},
    ),
    ChaosModifier(
        id="hidden_pot",
        name="Hidden Pot",
        emoji="🙊",
        description="You can't see the pot size or who's folded or checked but "
                     "raise amounts stay visible",
        weight=1.0,
        implemented=True,
    ),
    ChaosModifier(
        id="ragebait",
        name="Ragebait",
        emoji="😤",
        description="One player is secretly **cursed** (revealed only at the end "
                     "of the hand). If they win, they only keep **15%** of it: **5%** "
                     "goes to tax and **80%** floods the jackpot.",
        weight=1.0,
        implemented=True,
        params={"tax_pct": 0.05, "player_pct": 0.15},
    ),
    ChaosModifier(
        id="bomb_pot",
        name="Bomb Pot",
        emoji="💣",
        description="No raising preflop",
        weight=0.8,
        implemented=True,
    ),
    ChaosModifier(
        id="cute_mode",
        name="Cute Mode",
        emoji="💕",
        description="Purely visual: every card gets a cute overlay.",
        weight=1.0,
        implemented=True,
    ),
    ChaosModifier(
        id="losers_hand",
        name="Losers' Hand",
        emoji="🙃",
        description="The **worst hand wins**!",
        weight=1.0,
        implemented=True,
    ),
]

BY_ID: dict[str, ChaosModifier] = {m.id: m for m in MODIFIERS}


def get(modifier_id: str) -> ChaosModifier | None:
    return BY_ID.get(modifier_id)


# mod exclusions
EXCLUSION_PAIRS: list[tuple[str, str]] = [
    ("double_board", "execution"),
    ("double_board", "bounty"),
    ("reshuffle", "all_in_showdown"),
    ("rising_tide", "all_in_showdown"),
    ("bomb_pot", "all_in_showdown"),
    ("bomb_pot", "reverse"),
]

INCOMPATIBLE: dict[str, set[str]] = {m.id: set() for m in MODIFIERS}
for _a, _b in EXCLUSION_PAIRS:
    INCOMPATIBLE[_a].add(_b)
    INCOMPATIBLE[_b].add(_a)


# Modifier selection

# number of modifiers, weights
_COUNT_WEIGHTS = {1: 0.15, 2: 0.40, 3: 0.30, 4: 0.15}


def pick_modifiers(player_count: int, rng: random.Random | None = None) -> list[str]:
    # min_players requirements
    # no EXCLUSION_PAIRS combos
    rng = rng or random
    pool = [m for m in MODIFIERS if m.implemented and player_count >= m.min_players]
    if not pool:
        return []

    target_k = min(len(pool), rng.choices(
        list(_COUNT_WEIGHTS.keys()), weights=list(_COUNT_WEIGHTS.values()), k=1
    )[0])

    chosen: list[str] = []
    remaining = list(pool)
    while remaining and len(chosen) < target_k:
        weights = [m.weight for m in remaining]
        pick = rng.choices(remaining, weights=weights, k=1)[0]
        chosen.append(pick.id)
        blocked = INCOMPATIBLE.get(pick.id, set())
        remaining = [m for m in remaining if m.id != pick.id and m.id not in blocked]

    return chosen


# Random events 
@dataclass(frozen=True)
class ChaosEvent:
    id: str
    name: str
    emoji: str
    description: str
    weight: float = 1.0
    implemented: bool = False
    params: dict = field(default_factory=dict)


EVENTS: list[ChaosEvent] = [
    ChaosEvent(
        id="egirl_lover",
        name="egirl lover",
        emoji="🗣️",
        implemented=True,
        description="First 2 people who say:\n> <@412651268142792704> is my favorite egirl",
        params={
            "phrase": "<@412651268142792704> is my favorite egirl",
            "winners_needed": 2,
            "cosmetic_pool": [
                ("title", "egirl_simp"),
                ("title", "kissing_cat"),
                ("title", "rawr"),
                ("winmsg", "ara_ara"),
                ("winmsg", "whos_a_good_egirl"),
                ("winmsg", "egirl_services"),
            ],
            "consolation_chips": 10,
        },
    ),
    ChaosEvent(
        id="number_guess",
        name="Guess the Number",
        emoji="💭",
        implemented=True,
        description="Guess the secret number between 1-25",
        # logs as negative revenue
        params={"min_prize": 25, "max_prize": 75},
    ),
    ChaosEvent(
        id="think_fast",
        name="Think Fast",
        emoji="⚡",
        implemented=True,
        description="First person to click the button gets between -100 and +200 chips.",
        params={"min_delta": -50, "max_delta": 100, "min_wallet": 50, "unlucky_title_id": "wilted_flower"},
    ),
    ChaosEvent(
        id="giveaway_react",
        name="Giveaway",
        emoji="🎁",
        implemented=True,
        description="React within 10 seconds for a shot at the giveaway "
                     "70% chance of **chips**, 30% odds of **90 minute ban** ",
        params={"kick_chance": 0.3, "ban_seconds": 5400, "min_prize": 10, "max_prize": 200,
                "react_emoji": "🤑", "kick_title_id": "foot"},
    ),
    ChaosEvent(
        id="redistribution",
        name="Redistribution",
        emoji="💸",
        implemented=True,
        description="After 15 seconds, the pool gets randomly redistributed among everyone who entered.",
        params={"buy_in_options": [25, 50, 100, 500], "window_seconds": 15},
    ),
    ChaosEvent(
        id="chip_shower",
        name="Chip Shower",
        emoji="🌧️",
        implemented=True,
        description="For 5 seconds, up to 20 people can grab a random shower payout of 25-50 chips.",
        # logs as negative revenue
        params={"window_seconds": 5, "min_prize": 25, "max_prize": 50, "max_claimers": 20},
    ),
    ChaosEvent(
        id="lottery",
        name="Lottery",
        emoji="🎟️",
        implemented=True,
        description="Classic lottery with 10% tax. The more entries, the higher your chances!",
        # The explicit split is the revenue logging contract: tax_pct -> log_house_revenue,
        # jackpot_pct -> db.adjust_jackpot, remainder -> the winner.
        params={"tax_pct": 0.08, "jackpot_pct": 0.02},
    ),
    ChaosEvent(
        id="double_or_nothing",
        name="Double or Nothing",
        emoji="🤸",
        implemented=True,
        description="Buy in for 20 chips, then 50/50 to double or lose it all each round! "
                     "30 seconds to buy in.",
        params={"window_seconds": 30, "seed_chips": 20, "double_chance": 0.5, "max_rounds": 6},
    ),
]

EVENTS_BY_ID: dict[str, ChaosEvent] = {e.id: e for e in EVENTS}

RANDOM_EVENT_CHANCE = 0.15  # independent chance, PER TRIGGER POINT (see below)

def should_random_event_fire(rng: random.Random | None = None) -> bool:
    # call once per trigger point
    rng = rng or random
    return rng.random() < RANDOM_EVENT_CHANCE


def pick_event_id(rng: random.Random | None = None) -> str | None:
    # picks event
    rng = rng or random
    pool = [e for e in EVENTS if e.implemented]
    if not pool:
        return None
    return rng.choices(pool, weights=[e.weight for e in pool], k=1)[0].id


# announcement embeds 

def build_announcement_embed(modifier_ids: list[str], hand_num: int | None = None) -> discord.Embed:
    # embed sent before each hand
    title = "🎲 Chaos Table"
    if hand_num:
        title += f" — Hand #{hand_num}"
    embed = discord.Embed(title=title, color=0xE91E63)
    if not modifier_ids:
        embed.description = "No modifiers this hand — just a normal deal. Enjoy the calm."
        return embed
    lines = []
    for mid in modifier_ids:
        m = get(mid)
        if not m:
            continue
        lines.append(f"**{m.emoji} {m.name}**\n{m.description}")
    embed.description = "\n\n".join(lines)
    return embed


def modifiers_summary_line(modifier_ids: list[str]) -> str:
    # compact 1line for consistent embed
    if not modifier_ids:
        return "—"
    parts = []
    for mid in modifier_ids:
        m = get(mid)
        if m:
            parts.append(f"{m.emoji} {m.name}")
    return "  ·  ".join(parts) if parts else "—"


RANDOM_EVENT_COLOR = 0xF1C40F  # gold

TRIGGER_POINT_LABELS: dict[str, str] = {
    "flop":          "",
    "turn":          "",
    "river":         "",
    "first_reveal":  "",
    "after_winners": "",
    "intermission":  "",
}


RANDOM_EVENT_RESULT_COLOR = 0x2ECC71  # green


def build_event_result_embed(event_id: str, description: str,
                              no_result: bool = False) -> discord.Embed:
    """
    `no_result=True` swaps the title + embed color gold.
    """
    e = EVENTS_BY_ID.get(event_id)
    name = f"{e.emoji} {e.name}" if e else "🎉 Random Event"
    if no_result:
        return discord.Embed(title=f"{name} — Results", description=description, color=RANDOM_EVENT_COLOR)
    return discord.Embed(title=f"{name} — Results 🏆", description=description, color=RANDOM_EVENT_RESULT_COLOR)


def build_event_embed(event_id: str, trigger_point: str | None = None,
                       description: str | None = None) -> discord.Embed:
    e = EVENTS_BY_ID.get(event_id)
    if not e:
        return discord.Embed(title="🎉 Random Event", description=description or "Something's happening...",
                              color=RANDOM_EVENT_COLOR)
    embed = discord.Embed(title=f"{e.emoji} {e.name}", description=description or e.description,
                           color=RANDOM_EVENT_COLOR)
    embed.set_author(name="🎲 Random Event!")
    footer = TRIGGER_POINT_LABELS.get(trigger_point)
    if footer:
        embed.set_footer(text=footer)
    return embed


# Execution helper
def pick_execution_victim(evaluator, hand_eval_module, result, chaos_modifiers=()):
    community = result.community or []
    candidates = [
        p for p in (result.showdown_players or [])
        if p.hole_cards and len(p.hole_cards) + len(community) >= 5
    ]
    if len(candidates) < 2:
        return None  # need at least 2 real hands for "worst" to mean anything
 
    losers_hand_active = "losers_hand" in chaos_modifiers
 
    worst_player = None
    worst_score = -1 if not losers_hand_active else 7463  # treys scores run 1(best)-7462(worst)
    for p in candidates:
        score = hand_eval_module.evaluate_any(evaluator, list(p.hole_cards), list(community))
        if score is None:
            continue
        is_worse = score < worst_score if losers_hand_active else score > worst_score
        if is_worse:
            worst_score = score
            worst_player = p
    return worst_player
