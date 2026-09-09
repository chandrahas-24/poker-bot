# shiny_cards.py
'''
To add a new shiny card
1. Drop the art into poker/cards/<image>.png
2. Add a ShinyCard entry to SHINY_CARDS below with a unique `id`.
3. Add matching entries to db.TITLES / db.WIN_MESSAGES using the same `title_id` / `winmsg_ids`
   If not found, unlock_cosmetic() will still record the unlock, it just won't have display text yet
'''

from dataclasses import dataclass, field
from treys import Card


@dataclass(frozen=True)
class ShinyCard:
    id: str                            # internal id, also the cosmetic-id prefix
    card: str                          # treys short form, "As", "6h", "9s", "Kc"
    image: str                         # filename inside poker/cards/
    display_name: str                  # shown in announcements
    chance: float                      # base per-player, per-hand hit chance
    saturday_chance: float | None = None  # overrides `chance` on High Table Saturdays
    win_pct: float = 0.80              # % jp for win w/ shiny
    loss_pct: float = 0.30             # % jp for loss w/ shiny (no fold)
    title_id: str | None = None        # cosmetic id unlocked in db.TITLES (None = placeholder)
    winmsg_ids: tuple[str, ...] = ()   # cosmetic ids unlocked in db.WIN_MESSAGES
    emoji: str = "✨"                  # inline marker shown next to the card in text

    @property
    def card_int(self) -> int:
        return Card.new(self.card)


SHINY_CARDS: list[ShinyCard] = [
    ShinyCard(
        id="egirl_saro",
        card="As",
        image="egirl_ace_of_spades.png",
        display_name="e-girl Saroshi",
        chance=0.0001
        saturday_chance=0.0001625,
        win_pct=0.80,
        loss_pct=0.30,
        title_id="sarosmommy",
        winmsg_ids=("egirl_ace_winmsg", "noo"),
    ),
    ShinyCard(
        id="princess_bay",
        card="6h",
        image="princess_bay.png",
        display_name="Princess Bay",
        chance=0.0001
        saturday_chance=0.0001625,
        win_pct=0.80,
        loss_pct=0.30,
        title_id="pwincess",
        winmsg_ids=("princess_bay_winmsg",),    # STILL NEED
    ),
    ShinyCard(
        id="draug",
        card="9s",
        image="draug.png",
        display_name="draug",
        chance=0.0001,
        saturday_chance=0.0001625,
        win_pct=0.80,
        loss_pct=0.30,
        title_id="meat", 
        winmsg_ids=("undefeated",),
    ),
    ShinyCard(
        id="makima_sap",
        card="Kc",
        image="makima_sap.png", 
        display_name="Makima",
        chance=0.0001,
        saturday_chance=0.0001625,
        win_pct=0.80,
        loss_pct=0.30,
        title_id="goth_mommy", 
        winmsg_ids=("thighs",), 
    ),
]

# Fast lookups
BY_ID: dict[str, ShinyCard] = {c.id: c for c in SHINY_CARDS}
BY_CARD_INT: dict[int, ShinyCard] = {c.card_int: c for c in SHINY_CARDS}


def get_by_id(shiny_id: str) -> ShinyCard | None:
    return BY_ID.get(shiny_id)


def get_by_card_int(card_int: int) -> ShinyCard | None:
    return BY_CARD_INT.get(card_int)
