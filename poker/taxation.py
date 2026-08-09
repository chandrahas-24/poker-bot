from datetime import datetime
import config
from . import database as db
import math


def get_tax_config() -> tuple[float, bool]:
    """
    Checks if today is the special tax day.
    Returns: (tax_rate, is_special_day)
    """
    now = datetime.utcnow()
    # Check if today matches the day in config (e.g., "Friday")
    is_special = now.strftime("%A").lower() == config.TAX_FREE_DAY.lower()

    rate = config.SPECIAL_TAX_RATE if is_special else config.TAX_RATE
    return rate, is_special


async def process_and_log_tax(tax_amount: int):
    """
    Splits the collected tax between the House Revenue and the Jackpot.
    On normal days: 80% House / 20% Jackpot.
    On special days: 100% Jackpot.
    """
    if tax_amount <= 0:
        return

    rate, is_special = get_tax_config()

    if is_special:
        # On Jackpot Friday, 100% of the tax goes straight into the Jackpot
        await db.adjust_jackpot(tax_amount)
    else:
        # Standard 80/20 split
        jackpot_cut = math.ceil(tax_amount*0.2)
        house_cut = tax_amount - jackpot_cut

        await db.log_house_revenue(house_cut)
        if jackpot_cut > 0:
            await db.adjust_jackpot(jackpot_cut)