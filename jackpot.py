import math
import database as db
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

    score = evaluator.evaluate(player.hole_cards, community)
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

async def process_jackpot_hits(players: list, community: list, folded_ids: set) -> list[tuple]:
    """
    Takes all eligible players (who didn't fold), checks for triggers, pays them, and returns receipts.
    Returns: list of (user_id, jp_tier, actual_paid_amount, new_jackpot_total)
    """
    jackpot_hits = []
    try:
        jackpot_now = await db.get_jackpot()
        if jackpot_now <= 0 or not players:
            return jackpot_hits

        egirl_players = []
        rf_players = []
        sf_players = []
        quads_players = []

        # Evaluate all eligible players sent from the engine
        for p in players:

            if p.user_id in folded_ids:
                continue

            if p.egirl_saro:
                egirl_players.append(p)
            else:
                is_quads, is_sf, is_rf = evaluate_jackpot_tiers(p, community)
                if is_rf:
                    rf_players.append(p)
                elif is_sf:
                    sf_players.append(p)
                elif is_quads:
                    quads_players.append(p)

        tiers = []
        if egirl_players:
            # Shiny completely blocks all other payouts for the hand
            tiers = [("✨ E-girl Saroshi Ace", 0.80, egirl_players)]
        else:
            if rf_players:
                tiers.append(("👑 Royal Flush", 0.60, rf_players))
            if sf_players:
                tiers.append(("🔥 Straight Flush", 0.20, sf_players))
            if quads_players:
                tiers.append(("🃏 Four of a Kind", 0.05, quads_players))

        if not tiers:
            return jackpot_hits

        # Pay out each tier that triggered (Multiple tiers CAN trigger sequentially if no shiny)
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

    except Exception as e:
        print(f"[jackpot] processing error: {e}")

    return jackpot_hits

async def get_jackpot_display_cuts() -> tuple[int, int, int, int, int]:
    """Returns (total_jp, egirl_cut, rf_cut, sf_cut, quads_cut)"""
    jp = await db.get_jackpot()
    egirl_cut = max(2000, math.ceil(jp * 0.8))
    rf_cut = math.ceil(jp * 0.60)
    sf_cut = math.ceil(jp * 0.20)
    quads_cut = math.ceil(jp * 0.05)
    return jp, egirl_cut, rf_cut, sf_cut, quads_cut