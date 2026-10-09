import math
import re
from datetime import datetime, timedelta, timezone
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


# ── Stats (read side) ─────────────────────────────────────────────────────────

# audit_log.detail written by database.pay_jackpot:
#   "{label}: {amt:,} chips paid out (jackpot was {n:,})"
SHINY_MIN_AMT = 2000  # stored units; 5000 displays as 5,000,000,000
_PAYOUT_RE = re.compile(r"^(?P<label>.+?): (?P<amt>[\d,]+) chips paid out")
_TIER_KINDS = (
    ("Royal Flush", "rf", "Royal Flush"),
    ("Straight Flush", "sf", "Straight Flush"),
    ("Four of a Kind", "quads", "Four of a Kind"),
)
_EMOJI_JUNK = re.compile(r"<a?:\w+:\d+>|[^\w\s'.\-]")


def _clean_shiny_name(label: str) -> str:
    for c in shiny_cards.SHINY_CARDS:
        if c.display_name in label:
            return c.display_name
    return _EMOJI_JUNK.sub("", label).strip() or "Shiny"


def parse_payout(row: tuple) -> dict | None:
    """(id, ts, user_id, user_name, detail) -> {uid, amt, kind, tier, ts} or None if unparseable."""
    _id, ts, uid, _name, detail = row
    m = _PAYOUT_RE.match(detail or "")
    if not m:
        return None
    label = m.group("label")
    amt = int(m.group("amt").replace(",", ""))
    kind, tier = "shiny", None
    for needle, k, shown in _TIER_KINDS:
        if needle in label:
            kind, tier = k, shown
            break
    if kind == "shiny":
        tier = f"Shiny ({_clean_shiny_name(label)})"
    try:
        unix = int(datetime.strptime(ts, "%Y-%m-%d %H:%M UTC").replace(tzinfo=timezone.utc).timestamp())
    except (TypeError, ValueError):
        unix = 0
    return {"uid": uid, "amt": amt, "kind": kind, "tier": tier, "ts": unix}


def build_jackpot_stats(rows: list[tuple], top_n: int = 5, window_days: int = 30) -> dict:
    """
    rows: newest-first output of db.get_jackpot_payouts().
    Returns {"window_total", "window_count", "quads", "sf", "rf", "shiny", "top"};
    each list entry is a parse_payout() dict. Manual chip_log grants are not included.
    """
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=window_days)).timestamp())
    out = {"window_total": 0, "window_count": 0, "all_total": 0, "all_count": 0,
           "quads": [], "sf": [], "rf": [], "shiny": [], "top": []}
    parsed = []
    for row in rows:
        e = parse_payout(row)
        if e is None:
            continue
        parsed.append(e)
        out["all_total"] += e["amt"]
        out["all_count"] += 1
        if e["ts"] >= cutoff:
            out["window_total"] += e["amt"]
            out["window_count"] += 1
        if e["kind"] == "shiny" and e["amt"] < SHINY_MIN_AMT:
            continue
        bucket = out[e["kind"]]
        if len(bucket) < top_n:
            bucket.append(e)
    out["top"] = sorted(parsed, key=lambda e: e["amt"], reverse=True)[:top_n]
    return out
