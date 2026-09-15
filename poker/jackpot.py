import math
from . import database as db
from . import shiny_cards
from . import chaos
from . import hand_eval
from treys import Evaluator, Card

evaluator = Evaluator()

def evaluate_jackpot_tiers(player, community: list) -> tuple[bool, bool, bool]:
    """
    Evaluates a player's hand against the community board to determine if they
    hit Quads, a Straight Flush, or a Royal Flush, using strict casino rules.
    Returns: (is_quads, is_sf, is_rf)
    """
    if not player.hole_cards or not community or len(community) < 3:
        return False, False, False

    score = hand_eval.evaluate_any(evaluator, player.hole_cards, community)
    rank_str = evaluator.class_to_string(evaluator.get_rank_class(score))

    board_score = None
    board_rank_str = ""

    # Evaluate the board to ensure the player actually beat it with their hole cards
    if len(community) == 5:
        board_score = evaluator.evaluate(community[:2], community[2:])
        board_rank_str = evaluator.class_to_string(evaluator.get_rank_class(board_score))
    elif len(community) == 4:
        ranks_on_board = [Card.get_rank_int(c) for c in community]
        if len(set(ranks_on_board)) == 1:
            board_score = score
            board_rank_str = "Four of a Kind"

    is_quads = False
    is_sf = False
    is_rf = False

    if rank_str == "Four of a Kind":
        if board_rank_str != "Four of a Kind":
            is_quads = True
    elif rank_str in ["Straight Flush", "Royal Flush"]:
        played_board = (board_score is not None) and (score >= board_score)
        if not played_board:
            is_sf = True
            is_rf = (score == 1)

    return is_quads, is_sf, is_rf

async def process_jackpot_hits(players: list, community: list, folded_ids: set,
                                winner_ids: set = frozenset(), frenzy: bool = False) -> tuple[list[tuple], list]:
    """
    Takes all eligible players (who didn't fold), checks for triggers, pays them, and returns receipts.

    `winner_ids` — the user_ids who won this hand's pot. Needed so a shiny-card
    holder can be paid their card's win_pct (if they won) or loss_pct (if they
    reached showdown but lost) — see shiny_cards.py.

    `frenzy` — True when the Shiny Frenzy chaos modifier was active this hand.
    Under Frenzy, a shiny hit only pays out (and only unlocks cosmetics,
    handled separately in poker.py's _handle_shiny_cards) if that SAME
    player pulled 2+ shinies in their own hand — a single shiny during
    Frenzy doesn't qualify.

    Returns: (jackpot_hits, frenzy_misses)
        jackpot_hits — list of (user_id, jp_tier_label, actual_paid_amount, new_jackpot_total)
        frenzy_misses — PokerPlayers who pulled exactly one shiny during a
            Frenzy hand and therefore didn't qualify for anything; poker.py
            uses this to print the "so close!" message.
    """
    jackpot_hits = []
    frenzy_misses = []
    try:
        jackpot_now = await db.get_jackpot()
        if jackpot_now <= 0 or not players:
            return jackpot_hits, frenzy_misses

        min_shinies = chaos.get("shiny_frenzy").params["min_shinies"] if frenzy else 1

        # ── Shiny cards always take priority and block the natural-hand tiers ──
        shiny_events = []  # list of (label, pct, player)
        for p in players:
            if p.user_id in folded_ids:
                continue
            shiny_ids = getattr(p, "shiny_ids", None) or []
            if not shiny_ids:
                continue
            if frenzy and len(shiny_ids) < min_shinies:
                frenzy_misses.append(p)
                continue
            for sid in shiny_ids:
                shiny = shiny_cards.get_by_id(sid)
                if not shiny:
                    continue
                is_winner = p.user_id in winner_ids
                pct = shiny.win_pct if is_winner else shiny.loss_pct
                label = f"{shiny.emoji} {shiny.display_name}"
                shiny_events.append((label, pct, p))

        if shiny_events:
            for label, pct, p in shiny_events:
                current_jp = await db.get_jackpot()
                payout = math.ceil(current_jp * pct)
                actual = await db.pay_jackpot(p.user_id, p.display_name, payout, label)

                if actual > 0:
                    new_jp = await db.get_jackpot()
                    await db.log_currency_event(p.user_id, "Jackpot", actual, f"Won {label}!")
                    jackpot_hits.append((p.user_id, label, actual, new_jp))

            return jackpot_hits, frenzy_misses
        rf_players = []
        sf_players = []
        quads_players = []

        for p in players:
            if p.user_id in folded_ids:
                continue
            is_quads, is_sf, is_rf = evaluate_jackpot_tiers(p, community)
            if is_rf:
                rf_players.append(p)
            elif is_sf:
                sf_players.append(p)
            elif is_quads:
                quads_players.append(p)

        tiers = []
        if rf_players:
            tiers.append(("👑 Royal Flush", 0.60, rf_players))
        if sf_players:
            tiers.append(("🔥 Straight Flush", 0.20, sf_players))
        if quads_players:
            tiers.append(("🃏 Four of a Kind", 0.05, quads_players))

        if not tiers:
            return jackpot_hits, frenzy_misses

        # Pay out each tier that triggered (multiple tiers CAN trigger sequentially)
        for jp_tier, jp_pct, tier_winners in tiers:
            current_jp = await db.get_jackpot()
            each_pct = jp_pct / len(tier_winners)

            for p in tier_winners:
                payout = math.ceil(current_jp * each_pct)
                actual = await db.pay_jackpot(p.user_id, p.display_name, payout, jp_tier)

                if actual > 0:
                    new_jp = await db.get_jackpot()
                    await db.log_currency_event(p.user_id, "Jackpot", actual, f"Won {jp_tier}!")
                    jackpot_hits.append((p.user_id, jp_tier, actual, new_jp))

    except Exception:
        import traceback
        traceback.print_exc()

    return jackpot_hits, frenzy_misses

async def get_jackpot_display_cuts() -> tuple[int, int, int, int, int]:
    """
    Returns (total_jp, shiny_cut, rf_cut, sf_cut, quads_cut).

    `shiny_cut` reflects a WIN with a shiny card. The jackpot image template
    only has one generic "SHINY CARD WIN" slot, so if you ever give different
    shiny cards different win_pct values this number represents the highest
    one currently configured in shiny_cards.py.
    """
    jp = await db.get_jackpot()
    shiny_win_pct = max((c.win_pct for c in shiny_cards.SHINY_CARDS), default=0.80)
    shiny_cut = max(2000, math.ceil(jp * shiny_win_pct))
    rf_cut = math.ceil(jp * 0.60)
    sf_cut = math.ceil(jp * 0.20)
    quads_cut = math.ceil(jp * 0.05)
    return jp, shiny_cut, rf_cut, sf_cut, quads_cut