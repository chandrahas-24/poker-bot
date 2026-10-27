import os
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).parent

DONATION_CONVERSION_ENABLED = True

# Load the secret tokens from .env
load_dotenv()

DISCORD_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = BASE_DIR / "data" / "poker" / "poker.db"
TOURNAMENT_DB_PATH = BASE_DIR / "data" / "tournament" / "tournament.db"
TUTORIAL_DB_PATH = BASE_DIR / "data" / "tutorial" / "tutorial.db"
EVENTLOG_DB_PATH = BASE_DIR / "data" / "eventlog" / "eventlog_database.db"
NET_CHIPS_HISTORY_CSV_PATH = BASE_DIR / "historical_net_chips.csv"   # one-time seed for the history db
POKER_NET_HISTORY_DB_PATH = BASE_DIR / "data" / "poker" / "net_history.db"
UNO_DB_PATH = BASE_DIR / "data" / "uno" / "uno.db"
UNO_STATS_DB_PATH = BASE_DIR / "data" / "uno" / "uno_stats.db"
TOURNAMENT_REGISTER_CHANNEL_ID = 1509457673014481006
TOURNAMENT_STARTING_CHIPS = 5000

# ── SERVER & CHANNELS ──
GUILD_ID = 799685121582039141
CASHOUT_CHANNEL_ID = 1479793953090830460
INACTIVITY_CHANNEL_ID = 1479667673804701707
ADD_CHIPS_CHANNELS = {1479792931505180845, 1479667673804701707}
REMOVE_CHIPS_CHANNELS = {1479792931505180845, 1479667673804701707}
PAYOUT_MANAGER_ROLE = 1019909875913998388

LOCKDOWN_CHANNELS = [799685122701000754]

RESTRICTED_CHANNELS = {
    1508645866712465479: ["poker start", "poker open", "poker rebuy", "poker close", "poker gamble", "tourney wallet", "poker table", "pokerset table"],  #allowed exceptions
    1513018821982814270: ["poker start", "poker open", "poker rebuy", "poker close", "poker gamble", "tourney wallet", "poker table", "pokerset table"],
}

# ── ECONOMY & TAX ──
TAX_FREE_DAY = "Friday"
TAX_RATE = 0.05
REJOIN_FEE_MULTIPLIER = 2
SPECIAL_TAX_RATE = 0.05

# ── TUTORIAL SETTINGS ──
TUTORIAL_CHANNEL_ID = 1492843653108989952
TUTORIAL_REWARD_CHANNEL = 1014509165357584434
TUTORIAL_BUY_IN_CHANNEL = 1479792931505180845
TUTORIAL_PLAY_CHANNEL = 1479666976975491153

# ── REJOIN COOLDOWN SETTINGS (in seconds) ──
REGULAR_REJOIN_COOLDOWN = 600
TOURNAMENT_REJOIN_COOLDOWN = 120

# ── AFK / DECISION TIMEOUT ──
LARGE_POT_THRESHOLD   = 1000
LARGE_POT_EXTRA_TIME  = 15
DAILY_AFK_LIMIT        = 5
CONSECUTIVE_AFK_LIMIT  = 2

# ── GLOBAL CASINO LIMITS ──
TURN_TIMEOUT_DEFAULT    = 300
NEXT_HAND_DELAY_DEFAULT = 30
TABLE_RESEND_MSGS       = 10
INACTIVITY_DAYS         = 2
MIN_HANDS_PER_PERIOD    = 10
MIN_CHIPS_WAGERED       = 500
MAX_PLAYERS = 12
PLAYERS_PER_FIELD = 6

# defaults
GRACE_PERIOD_DAYS = 0
DEFAULT_SMALL_BLIND = 25
DEFAULT_BIG_BLIND = 50
DEFAULT_MIN_BUYIN = 50

# ── INACTIVITY WIPE TAX ──
WIPE_TAX_RATE = 0.20  # 20%

# ── UNO ECONOMY (separate wallets/db from poker — see uno/database.py) ──
UNO_MIN_BET = 50
UNO_WINNERS_TAX_RATE = 0.05
UNO_WIPE_TAX_RATE = 0.20       # same tax rate poker uses
UNO_INACTIVITY_DAYS = 2        # same grace window poker uses
UNO_MIN_ROUNDS_PER_PERIOD = 2  # UNO rounds run longer than poker hands, so a lower bar than MIN_HANDS_PER_PERIOD
UNO_MIN_CHIPS_WAGERED = 100    # a single min-bet round should count as activity

# Defaults for the per-guild settings (/unoset settings) — a guild that's never
# changed a setting follows these; changing one here changes it for them too.
UNO_MAX_BET = 0                # 0 = no limit
UNO_TURN_TIMEOUT = 45          # seconds per turn
UNO_BET_WINDOW = 25            # seconds between rounds to join / change bet
UNO_RESEND_AFTER_MSGS = 7      # chat messages before the table/panel is reposted at the bottom
UNO_MAX_PLAYERS = 6
UNO_NUM_DECKS = 4
UNO_IDLE_CLOSE_SECONDS = 600   # a table with fewer than 2 players this long closes and refunds everyone

# ── UNO SERVER & CHANNELS
UNO_CASHOUT_CHANNEL_ID = 1554549964741287986
UNO_INACTIVITY_CHANNEL_ID = 1554675031966220360
UNO_DONATION_CHANNELS = {1554550010962518086}
UNO_ADD_CHIPS_CHANNELS = {1554550010962518086,1554675031966220360}
UNO_REMOVE_CHIPS_CHANNELS = {1554550010962518086,1554675031966220360}
UNO_PAYOUT_MANAGER_ROLE = 1019909875913998388

DEV_USER_IDS = (
    1339935869598961728, # baymax for backups
    804762802451382283,  # real baymax
)

# ── CUSTOM EMOJIS ──
POKER_CHIP_EMOJI = "<:poker_chip:1490458259855773707>"
TOURNAMENT_CHIP_EMOJI = "<:tourney_chip:1508826107246415924>"
UNO_CHIP_EMOJI = "<:uno_chip:1543047662977417286>"
PLACEMENT_EMOJI = {0: "<:uno_1:1536286536218443837>", 1: "<:uno_2:1536286641768108065>", 2: "<:uno_3:1536286708331446282>"}
