import os
from pathlib import Path

CARDS_DIR = Path(__file__).parent / "uno_cards"

COLORS = {"Blue", "Green", "Red", "Yellow"}
ACTIONS = {"Draw_2", "Reverse", "Skip", "Wild", "Wild_Draw_4"}


def parse_card_name(filename: str):
    """
    'Blue_4'        -> color='Blue',  value='4'
    'Blue_Draw_2'   -> color='Blue',  value='Draw_2'
    'Blue_Reverse'  -> color='Blue',  value='Reverse'
    'Blue_Skip'     -> color='Blue',  value='Skip'
    'Wild'          -> color=None,    value='Wild'
    'Wild_Draw_4'   -> color=None,    value='Wild_Draw_4'
    """
    name = os.path.splitext(filename)[0]
    parts = name.split("_")

    if parts[0] == "Wild":
        color = None
        value = "_".join(parts) if len(parts) > 1 else "Wild"
    else:
        color = parts[0]
        value = "_".join(parts[1:]) if len(parts) > 1 else parts[0]

    return {"name": name, "color": color, "value": value}


def load_all_cards():
    cards = {}
    if not os.path.isdir(CARDS_DIR):
        return cards
    for fname in os.listdir(CARDS_DIR):
        if not fname.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
            continue
        info = parse_card_name(fname)
        info["path"] = os.path.join(CARDS_DIR, fname)
        cards[info["name"]] = info
    return cards


if __name__ == "__main__":
    for name, info in load_all_cards().items():
        print(info)