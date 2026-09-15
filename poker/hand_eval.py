"""
hand_eval.py 

supports 3 hole cards + 5 card board
otherwise passes through
"""

import itertools


def evaluate_any(evaluator, hole: list[int], board: list[int]) -> int | None:
    """
    Returns None if there are fewer than 5 total cards
    """
    all_cards = hole + board
    n = len(all_cards)
    if n < 5:
        return None

    handler = evaluator.hand_size_map.get(n)
    if handler is not None:
        return handler(all_cards)

    # n == 8 (or, in principle, any n > 7): brute-force best-of-C(n,5).
    best = None
    for combo in itertools.combinations(all_cards, 5):
        score = evaluator._five(combo)
        if best is None or score < best:
            best = score
    return best
