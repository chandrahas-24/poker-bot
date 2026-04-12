import os
import random
import asyncio
import discord
from discord import app_commands
from discord.ext import commands
from treys import Card as TreysCard, Evaluator
from engine import PokerGame, Street, hand_str, PokerPlayer
import tutorial_db as db
import card_images

_evaluator = Evaluator()
USE_IMAGES = card_images.cards_available()

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

BOT_IDS   = [-1, -2, -3]
BOT_NAMES = ["Baymax 🤖", "guard 🤖", "Saroshi 🤖"]

TUTORIAL_CHIPS  = 1_000
TUTORIAL_SB     = 10
TUTORIAL_BB     = 20
TUTORIAL_REWARD = 10
TOTAL_HANDS     = 3

ENV_REWARD_CH = "TUTORIAL_REWARD_CHANNEL"
ENV_BUYIN_CH  = "TUTORIAL_BUY_IN_CHANNEL"
ENV_PLAY_CH   = "TUTORIAL_PLAY_CHANNEL"

# Rigged card configs.  Community order: flop(3) · turn(1) · river(1).
# All strings are treys format: rank + suit (e.g. "Kc" = King of clubs).
HAND_CONFIGS: dict[int, dict] = {
    # Hand 1 — strong hand (pocket kings → trips on flop)
    1: {
        "player":    ["Kc", "Kd"],
        "community": ["Ks", "7h", "2d", "9c", "As"],
    },
    # Hand 2 — bad hand (7-2 offsuit); bot goes all-in on turn
    2: {
        "player":    ["7c", "2d"],
        "community": ["Qh", "Jd", "Ts", "Kc", "5h"],
    },
    # Hand 3 — bluff hand; scary board (A-K-Q-J by turn), player misses
    3: {
        "player":    ["6c", "8d"],
        "community": ["Kh", "Qc", "Jc", "As", "2h"],
    },
}

RANKINGS_POPUP = (
    "📊 **Hand Rankings** — Best → Worst\n"
    "```\n"
    "👑  Royal Flush      A K Q J 10, same suit          ← BEST\n"
    "🔥  Straight Flush   5 in a row, same suit\n"
    "🃏  Four of a Kind   All 4 of one rank\n"
    "🏠  Full House        Three of a Kind + Pair\n"
    "♠️  Flush             Any 5 cards of the same suit\n"
    "📈  Straight          5 in a row, any suits\n"
    "3️⃣  Three of a Kind   3 cards of the same rank\n"
    "👫  Two Pair           Two different pairs\n"
    "🎴  One Pair           Two cards of the same rank\n"
    "🔼  High Card          Your single highest card      ← WORST\n"
    "```\n"
    "Now you know what beats what — make your move! 🎯"
)

STREET_ADVANCE_KWS = ("🌊", "↩️", "🏁", "Showdown", "🏆")

# Active tutorial sessions: {user_id: TutorialSession}
tutorial_sessions: dict[int, "TutorialSession"] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Session
# ─────────────────────────────────────────────────────────────────────────────

class TutorialSession:
    """All state for one user's tutorial run.  No real chip DB ops are performed."""

    def __init__(self, user_id: int, username: str):
        self.user_id       = user_id
        self.username      = username
        self.game          = PokerGame()
        self.game.SMALL_BLIND = TUTORIAL_SB
        self.game.BIG_BLIND   = TUTORIAL_BB

        # Progress counters
        self.current_hand  = 0 # 1/2/3 while in progress
        self.hands_done    = 0 # increments on hand completion

        # State machine: "intro" → "playing" → "between" → ... → "complete"
        self.phase         = "intro"

        # Per-hand flags
        self.rankings_acked  = False    # Hand 1 flop: must view rankings to unlock buttons
        self.bot_went_allin  = False    # Hand 2 turn: tracks the scripted all-in
        self.player_raised   = False    # Hand 3: did the player attempt a bluff-raise?
        self.player_won_h3   = False    # Hand 3: did the player win?

        self.street_log: list[str] = []
        self.result_text = ""

        # For storing the board image file
        self.board_file: discord.File | None = None

        # Tracks the currently active View so it can be stopped when session is reopened
        self.active_view: discord.ui.View | None = None

    # ── Helpers ────────────────────────────────────────────────────────────

    def slog(self, text: str):
        if text:
            self.street_log.append(text)

    def slog_clear(self):
        self.street_log = []

    def get_player(self) -> PokerPlayer | None:
        return self.game.get_player(self.user_id)

    # ── Script ────────────────────────────────────────────────────

    def _bot_act_one(self, bot_id: int) -> str:
        """
        Per-hand scripted bot behaviour:
          Hand 1 — Call/check with occasional small raises (~12 %).
          Hand 2 — Pre-flop/flop: call only.  Turn: first bot goes all-in,
                   subsequent bots fold to the all-in.
          Hand 3 — Fold to any raise above the big blind (facilitates bluffing).
        """
        g = self.game
        p = g.get_player(bot_id)
        if not p or p.folded or p.all_in:
            return ""

        call_amt = g.call_amount(p)

        # ── Hand 1: passive call/check, occasional small raise ───────────
        if self.current_hand == 1:
            if call_amt == 0:
                if random.random() < 0.12 and p.chips > g.BIG_BLIND * 4:
                    ok, msg = g.raise_bet(bot_id, g.BIG_BLIND)
                    if ok:
                        return msg
                ok, msg = g.check_or_call(bot_id)
                return msg if ok else ""
            ok, msg = g.check_or_call(bot_id)
            return msg if ok else ""

        # ── Hand 2: scripted all-in on the turn ─────────────────────────
        elif self.current_hand == 2:
            if g.street == Street.TURN:
                if not self.bot_went_allin:
                    # First bot to act on the turn goes all-in
                    self.bot_went_allin = True
                    allin_raise = max(1, p.chips - call_amt)
                    ok, msg = g.raise_bet(bot_id, allin_raise)
                    if ok:
                        return msg
                    # Fallback if raise fails (e.g. stack already ≤ call)
                    ok, msg = g.check_or_call(bot_id)
                    return msg if ok else ""
                else:
                    # Subsequent bots fold to the all-in
                    ok, msg = g.fold(bot_id)
                    return msg if ok else ""
            # Pre-flop / flop: just call
            ok, msg = g.check_or_call(bot_id)
            return msg if ok else ""

        # ── Hand 3: fold to any meaningful raise (teaches bluffing) ─────
        elif self.current_hand == 3:
            if call_amt > g.BIG_BLIND:
                ok, msg = g.fold(bot_id)
                return msg if ok else ""
            ok, msg = g.check_or_call(bot_id)
            return msg if ok else ""

        # Fallback
        ok, msg = g.check_or_call(bot_id)
        return msg if ok else ""

    # ── Player/bot list setup ──────────────────────────────────────────────

    def setup_or_reset(self):
        """
        Ensure all bots and the human are seated with adequate chips.
        Safe to call multiple times — tops up stacks, re-adds removed players.

        For hands 2 and 3 the dealer index is set one before the human player
        so that start_hand() rotates it to the human, making the human dealer
        and therefore LAST to act on every post-flop street.  This guarantees
        at least one bot acts on the turn before the human sees it.
        """
        g = self.game
        existing = {p.user_id: p for p in g.players}

        # Add / top-up bots
        for bid, bname in zip(BOT_IDS, BOT_NAMES):
            if bid in existing:
                if existing[bid].chips < g.BIG_BLIND * 2:
                    existing[bid].chips = TUTORIAL_CHIPS
            else:
                g.players.append(PokerPlayer(bid, bname, TUTORIAL_CHIPS))

        # Add / top-up human
        if self.user_id in existing:
            if existing[self.user_id].chips < g.BIG_BLIND * 2:
                existing[self.user_id].chips = TUTORIAL_CHIPS
        else:
            g.players.append(PokerPlayer(self.user_id, self.username, TUTORIAL_CHIPS))

        g.pending_joins.clear()
        g.pending_leaves.clear()

        # Hand 2+: make human the dealer so they act LAST post-flop
        if self.current_hand >= 2:
            n = len(g.players)
            player_idx = next(
                (i for i, p in enumerate(g.players) if p.user_id == self.user_id),
                n - 1,
            )
            # start_hand() rotates +1, so set one position before the human
            g.dealer_idx = (player_idx - 1) % n

    # ── Bot runner ─────────────────────────────────────────────────────────

    def run_bots_until_player_turn(self) -> bool:
        """
        Runs bot actions until either the hand ends or it is the human's turn.

        Returns True  — hand is over (hand_result set, or street is WAITING).
                False — it is now the human player's turn.
        """
        g = self.game
        for _ in range(500):   # safety cap
            if g._hand_result or g.street in (Street.WAITING, Street.SHOWDOWN):
                return True

            cp = g.current_player()

            if cp is None:
                # All-in run-out: no active players, advance the board automatically
                if g.street not in (Street.WAITING, Street.SHOWDOWN):
                    msg = g._next_street()
                    if msg:
                        if any(kw in msg for kw in STREET_ADVANCE_KWS):
                            self.slog_clear()
                        self.slog(msg)
                    continue
                return True

            if cp.user_id == self.user_id:
                return False   # human's turn — stop and let the View render

            msg = self._bot_act_one(cp.user_id)
            if msg:
                if any(kw in msg for kw in STREET_ADVANCE_KWS):
                    self.slog_clear()
                self.slog(msg)

        return True   # safety fallback


# ─────────────────────────────────────────────────────────────────────────────
# Guidance text
# ─────────────────────────────────────────────────────────────────────────────

def _get_hand_name(s: TutorialSession) -> str:
    """Evaluate the player's best hand on the current board (needs ≥ 3 community cards)."""
    p = s.get_player()
    g = s.game
    if not p or not p.hole_cards or len(g.community) < 3:
        return ""
    try:
        score = _evaluator.evaluate(p.hole_cards, g.community)
        return _evaluator.class_to_string(_evaluator.get_rank_class(score))
    except Exception:
        return ""


def _get_guidance(s: TutorialSession) -> str:
    """Return the contextual tip/guidance string for the current tutorial moment."""
    g = s.game

    # ── Hand 1: teach basics ───────────────────────────────────────────────
    if s.current_hand == 1:

        if g.street == Street.PREFLOP:
            return (
                "📚 **Pre-Flop — Your First Hand!**\n"
                "You've been dealt **2 private hole cards** (only you can see them).\n\n"
                "The **Small Blind** (SB) and **Big Blind** (BB) are forced bets that seed the pot. "
                "As the BB, you've already put chips in!\n\n"
                "💡 **Click Call or Check** to match any bet and see the first 3 shared cards — the **Flop**."
            )

        elif g.street == Street.FLOP:
            hand_name = _get_hand_name(s)
            strength_blurb = {
                "Three of a Kind": "🎉 **Three of a Kind — three Kings!** That's a very strong hand!",
                "Two Pair":        "🎉 **Two Pair!** A solid hand at this stage.",
                "Pair":            "🎉 **One Pair!** Not bad — let's see the next cards.",
                "Flush":           "🎉 **Flush!** Very strong — same suit five times.",
                "Straight":        "🎉 **Straight!** Five in a row — very strong!",
                "Full House":      "🎉 **Full House!** Incredibly strong!",
                "Four of a Kind":  "🎉 **Four of a Kind!** Nearly unbeatable!",
            }.get(hand_name, f"🎉 **You have {hand_name}!**" if hand_name else "")

            if not s.rankings_acked:
                return (
                    "📚 **The Flop** — 3 shared community cards hit the board!\n\n"
                    + (strength_blurb + "\n\n" if strength_blurb else "")
                    + "👆 **Before you act — click the 📊 Rankings button to see which hands beat which!**\n"
                    "*(Game buttons will unlock after you do.)*"
                )
            else:
                return (
                    (strength_blurb + "\n\n" if strength_blurb else "")
                    + "Everyone combines their 2 hole cards with the 5 community cards to make the **best 5-card hand**.\n\n"
                    "💡 With a strong hand, **Raise** to build the pot and win more chips, "
                    "or **Check** to see the next card for free."
                )

        elif g.street == Street.TURN:
            return (
                "📚 **The Turn** — A 4th community card!\n\n"
                "You now have 6 cards to work with (your 2 + 4 community). "
                "Your final hand will use the **best 5 of them** — the engine picks automatically.\n\n"
                "Keep betting strong, or check to see the River for free. 💪"
            )

        elif g.street == Street.RIVER:
            return (
                "📚 **The River** — The 5th and final community card!\n\n"
                "Last round of betting before the **Showdown**, where everyone reveals their hand "
                "and the best one wins the pot.\n\n"
                "**Check or Call** to get to Showdown! 🏆"
            )

    # ── Hand 2: teach folding ──────────────────────────────────────────────
    elif s.current_hand == 2:

        if g.street == Street.PREFLOP:
            return (
                "📚 **Pre-Flop — Hand 2: Knowing When to Fold**\n\n"
                "You've been dealt **7♣ 2♦** — the worst starting hand in poker!\n"
                "These cards rarely make a good hand on the board.\n\n"
                "That's okay — **not every hand is a winner.** "
                "The skill is knowing when to walk away. "
                "For now, **Check or Call** cheaply to see the Flop."
            )

        elif g.street == Street.FLOP:
            return (
                "📚 **Flop** — Your hand didn't connect with the board.\n\n"
                "When you have nothing, the smart move is usually to **Check** — "
                "you see the next card for *free*, risking nothing extra.\n\n"
                "Don't throw chips away chasing a weak hand! **Check** to see the Turn. 👀"
            )

        elif g.street == Street.TURN:
            if s.bot_went_allin:
                # Identify which bot went all-in for a personal touch
                allin_name = next(
                    (p.display_name for p in s.game.players if p.all_in and p.user_id in BOT_IDS),
                    "An opponent",
                )
                return (
                    f"⚠️ **{allin_name} just went ALL-IN!**\n\n"
                    "You're holding **7♣ 2♦** — this is not a great hand in poker. "
                    "Calling would mean risking most of your stack on a hand you're very likely to lose.\n\n"
                    "🏳️ **Fold here — it's the smart play!**\n"
                    "Protecting your stack by folding bad hands is one of the most important skills in poker."
                )
            else:
                return (
                    "📚 **The Turn** — Watch out, an opponent might make a big move...\n\n"
                    "Remember: you have **7♣ 2♦**, which hasn't connected. "
                    "If someone bets big, be ready to fold."
                )

        elif g.street == Street.RIVER:
            return (
                "📚 **River** — Almost there!\n\n"
                "**Check or Call** to reach the Showdown."
            )

    # ── Hand 3: teach bluffing ─────────────────────────────────────────────
    elif s.current_hand == 3:

        if g.street == Street.PREFLOP:
            return (
                "📚 **Pre-Flop — Hand 3: The Bluff! 🎭**\n\n"
                "In poker you don't always need the **best** hand to win. "
                "If you bet or raise confidently, opponents may think *you* have something strong "
                "and fold — this is called a **bluff**!\n\n"
                "**Call or Check** to see the Flop, then look for a chance to make a bold move."
            )

        elif g.street == Street.FLOP:
            return (
                "📚 **Flop** — Look at this board: **K♥ Q♣ J♣**!\n\n"
                "This is a *scary* board — it's possible someone has part of a **straight** "
                "(K-Q-J needs a 10 and a 9) or is chasing a **club flush**. "
                "Even if you don't have those, opponents can't know that!\n\n"
                "🎭 **Try clicking Raise (Bluff!)** — you might make the bots fold right here! "
                "Or Check to see the Turn for free."
            )

        elif g.street == Street.TURN:
            return (
                "📚 **Turn** — An Ace! The board now reads **A♠ K♥ Q♣ J♣**.\n\n"
                "Anyone with a **10** has a straight. This board is *terrifying* for most opponents!\n\n"
                "🎭 **Now's a great bluffing spot — try Raise (Bluff!)!** "
                "Representing the straight here is very believable. "
                "Confident bets often make everyone else fold."
            )

        elif g.street == Street.RIVER:
            return (
                "📚 **River** — Last chance to bluff!\n\n"
                "The board has **A-K-Q-J** on it. A big raise here says "
                "*\"I have the 10 for the straight\"* — your opponents don't know you're bluffing!\n\n"
                "🎭 **Go all-in to complete the bluff!** Click Raise (Bluff!), then go All In 🚀 to push everyone out."
            )

    return "📚 Good luck!"


def _build_guidance_embed(s: TutorialSession) -> discord.Embed | None:
    """Return a guidance Embed for the current tutorial moment, or None if not applicable."""
    guidance = _get_guidance(s)
    if not guidance:
        return None
    return discord.Embed(description=guidance, color=0x5865F2)


def _build_embeds(s: TutorialSession) -> list[discord.Embed]:
    """Build [main_embed] or [main_embed, guidance_embed] for the tutorial message."""
    main = build_tutorial_embed(s)
    guidance = _build_guidance_embed(s)
    return [main, guidance] if guidance else [main]


# ─────────────────────────────────────────────────────────────────────────────
# Button state logic
# ─────────────────────────────────────────────────────────────────────────────

def _get_button_states(s: TutorialSession) -> dict:
    """
    Return which game-action buttons should be enabled/disabled,
    plus any label / style overrides.
    """
    g = s.game
    p = s.get_player()

    is_my_turn = (
        s.phase == "playing"
        and p is not None
        and not p.folded
        and not p.all_in
        and g.is_turn(s.user_id)
    )

    # Defaults
    states = {
        "fold":        is_my_turn,
        "check":       is_my_turn,
        "raise":       is_my_turn and bool(p and p.chips > 0),
        "check_label": "Check",
        "check_style": discord.ButtonStyle.blurple,
        "call_label":  "Call",
        "call_style":  discord.ButtonStyle.green,
        "raise_label": "Raise",
        "raise_style": discord.ButtonStyle.green,
    }

    if not is_my_turn:
        return states

    # Dynamic check/call label
    if p:
        call_amt = g.call_amount(p)
        if call_amt:
            states["call_label"] = f"Call"
            states["call_style"] = discord.ButtonStyle.green
        else:
            states["check_label"] = "Check"
            states["check_style"] = discord.ButtonStyle.blurple

    # ── Hand 1 Pre-Flop: guide to Call only ─────────────────────────────
    # Fold and Raise are disabled — the lesson is to call and see the flop.
    if s.current_hand == 1 and g.street == Street.PREFLOP:
        states["fold"]  = False
        states["raise"] = False

    # ── Hand 1 Flop: Rankings button must be acknowledged first ─────────
    if s.current_hand == 1 and g.street == Street.FLOP and not s.rankings_acked:
        states["fold"]  = False
        states["check"] = False
        states["raise"] = False

    # ── Hand 2: no fold/raise until bot goes all-in ──────────────────────
    if s.current_hand == 2:
        if not s.bot_went_allin:
            states["fold"] = False
            states["raise"] = False
        # After bot goes all-in, only Fold is available
        elif g.street == Street.TURN and s.bot_went_allin:
            states["check"] = False
            states["raise"] = False
            states["fold"]  = True

    # ── Hand 3: bluff restrictions ────────────────────────────────────────
    if s.current_hand == 3:
        # Grey out fold always
        states["fold"] = False

        if g.street == Street.PREFLOP:
            # Pre-flop: only check/call
            states["raise"] = False
        elif g.street in (Street.FLOP, Street.TURN):
            # Flop/Turn: check/call and raise (bluff)
            states["raise_label"] = "Raise (Bluff!)"
            states["raise_style"] = discord.ButtonStyle.red
        elif g.street == Street.RIVER:
            # River: only raise all-in (must bluff)
            states["check"] = False
            states["raise_label"] = "Raise (Bluff!)"
            states["raise_style"] = discord.ButtonStyle.red

    return states


# ─────────────────────────────────────────────────────────────────────────────
# Board image update
# ─────────────────────────────────────────────────────────────────────────────

async def update_board(s: TutorialSession):
    """Generate card strip File object for the community board."""
    g = s.game
    if not USE_IMAGES or g.street in (Street.WAITING, Street.PREFLOP) or not g.community:
        s.board_file = None
        return
    backs = max(0, 5 - len(g.community))
    # Push image generation to a background thread
    s.board_file = await asyncio.to_thread(card_images.make_strip, g.community, backs)


# ─────────────────────────────────────────────────────────────────────────────
# Embed builder
# ─────────────────────────────────────────────────────────────────────────────

def build_tutorial_embed(s: TutorialSession) -> discord.Embed:
    g = s.game
    p = s.get_player()

    # ── Intro ────────────────────────────────────────────────────────────
    if s.phase == "intro":
        return discord.Embed(
            title="🃏 Poker Tutorial — 3 Guided Hands",
            description=(
                "Welcome! You'll play **3 practice hands** against bots using **fake chips**.\n"
                "Your real wallet balance is **never touched**.\n\n"
                "**What you'll learn:**\n"
                "1️⃣  **Hand 1** — The basics: cards, blinds, betting & hand rankings\n"
                "2️⃣  **Hand 2** — When to fold and protect your stack\n"
                "3️⃣  **Hand 3** — How to bluff and win without the best hand\n\n"
                "Click **▶ Start** to deal Hand 1!"
            ),
            color=0x5865F2,
        )

    # ── Complete ─────────────────────────────────────────────────────────
    if s.phase == "complete":
        buy_ch  = int(os.getenv(ENV_BUYIN_CH, "0"))
        play_ch = int(os.getenv(ENV_PLAY_CH,  "0"))
        buy_m   = f"<#{buy_ch}>"  if buy_ch  else "#economy"
        play_m  = f"<#{play_ch}>" if play_ch else "#poker"

        if s.player_won_h3 and s.player_raised:
            bluff_note = "\n\n🎭 **Incredible bluff!** You won Hand 3 without the best cards — that's the power of a well-timed raise!"
        elif s.player_raised and not s.player_won_h3:
            bluff_note = "\n\n💡 Your bluff didn't work out this time, but that's poker! Timing and reading opponents improves with practice."
        elif not s.player_raised:
            bluff_note = "\n\n💡 **Tip:** In Hand 3 you can try clicking **Raise (Bluff!)** to win the pot without a good hand — replay the tutorial to try it!"
        else:
            bluff_note = ""

        return discord.Embed(
            title="🎉 Tutorial Complete! You're Ready!",
            description=(
                "**Here's what you learned:**\n"
                "✅  Hand Rankings — Royal Flush is best, High Card is worst\n"
                "✅  Folding — saving chips on a bad hand!\n"
                "✅  Bluffing — play mind games on other players and win the pot without the best hand!"
                f"{bluff_note}\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"**Okay, you're ready for the real game!**\n"
                f"Head to {buy_m} to buy chips, then {play_m} to sit down and play! Remember to cash out if you're not actively playing poker using </poker request_cashout:1479797495528685578>!"
            ),
            color=0x2ECC71,
        )

    # ── Between hands ─────────────────────────────────────────────────────
    if s.phase == "between":
        next_titles = {2: "Hand 2: When to Fold", 3: "Hand 3: The Bluff 🎭"}
        next_label  = next_titles.get(s.hands_done + 1, f"Hand {s.hands_done + 1}")
        embed = discord.Embed(
            title=f"✅ Hand {s.hands_done}/{TOTAL_HANDS} Done!",
            color=0x5865F2,
        )
        if s.result_text:
            embed.add_field(name="📋 Result", value=s.result_text, inline=False)
        embed.add_field(
            name=f"⬆ Up Next: {next_label}",
            value="Click **Next Hand ▶** when you're ready!",
            inline=False,
        )
        embed.set_footer(text="Tutorial — fake chips only · your wallet is untouched")
        return embed

    # ── Playing ────────────────────────────────────────────────────────────
    STREET_COLOR = {
        Street.PREFLOP:  0x36393F,
        Street.FLOP:     0x1F8B4C,
        Street.TURN:     0xE67E22,
        Street.RIVER:    0xE74C3C,
        Street.SHOWDOWN: 0xF1C40F,
    }
    STREET_LABEL = {
        Street.PREFLOP:  "Pre-Flop",
        Street.FLOP:     "Flop",
        Street.TURN:     "Turn",
        Street.RIVER:    "River",
        Street.SHOWDOWN: "Showdown",
    }

    embed = discord.Embed(
        title=(
            f"🃏 Tutorial — Hand {s.current_hand}/{TOTAL_HANDS}"
            + (f"  ·  {STREET_LABEL.get(g.street, '')}" if g.street != Street.WAITING else "")
        ),
        color=STREET_COLOR.get(g.street, 0x5865F2),
    )

    # Hole cards (ephemeral — only this player sees this message)
    if p and p.hole_cards:
        embed.add_field(
            name="🂠 Your Hole Cards",
            value=f"**{hand_str(p.hole_cards)}**",
            inline=False,
        )

    # Community board (shown as image if available, else text with placeholders)
    if g.community:
        if USE_IMAGES:
            # Render the card strip inside the embed itself
            embed.add_field(name="🌊 Community Board", value="\u200b", inline=False)
            embed.set_image(url="attachment://cards.png")
        else:
            # Fallback to text
            backs = max(0, 5 - len(g.community))
            board = hand_str(g.community) + ("  🂠" * backs)
            embed.add_field(name="🌊 Community Board", value=board, inline=False)

    # Player table
    cp   = g.current_player()
    lines = []
    for pl in g.players:
        is_you = pl.user_id == s.user_id
        name   = "**You**" if is_you else pl.display_name
        if pl.folded:
            status = "~~folded~~"
        elif pl.all_in:
            status = f"ALL-IN 🚀  ({pl.chips} chips behind)"
        elif cp and cp.user_id == pl.user_id:
            status = "🎯 **your turn**" if is_you else "🎯 thinking…"
        elif pl.bet:
            status = f"bet {pl.bet}"
        else:
            status = "—"
        lines.append(f"{name}  **{pl.chips}** chips — {status}")

    pot_header = f"**Pot: {g.pot}**"
    if g.current_bet and p and not (p.folded or p.all_in):
        call_amt = g.call_amount(p)
        if call_amt:
            pot_header += f"  ·  **To call: {call_amt}**"
    embed.add_field(name=pot_header, value="\n".join(lines), inline=False)

    # Recent action log (last 5 lines)
    if s.street_log:
        embed.add_field(
            name="📜 Action",
            value="\n".join(s.street_log[-5:]),
            inline=False,
        )

    embed.set_footer(text=f"Hand {s.current_hand}/{TOTAL_HANDS}  ·  Tutorial (fake chips) — wallet untouched")
    return embed


# ─────────────────────────────────────────────────────────────────────────────
# View
# ─────────────────────────────────────────────────────────────────────────────

class TutorialView(discord.ui.View):
    def __init__(self, session: TutorialSession, channel: discord.abc.Messageable):
        super().__init__(timeout=600)
        self.session = session
        self.channel = channel
        # Register this view as the active one so it can be stopped on reopen
        session.active_view = self
        self._sync_buttons()

    # ── Button state sync ─────────────────────────────────────────────────

    def _sync_buttons(self):
        s      = self.session
        states = _get_button_states(s)

        # Row 0 — lifecycle buttons (only show in intro/between/complete)
        show_lifecycle = s.phase in ("intro", "between", "complete")
        self.btn_start.disabled = s.phase != "intro" or not show_lifecycle
        self.btn_next.disabled = s.phase != "between" or not show_lifecycle

        # Remove these buttons during playing
        if s.phase == "playing":
            self.remove_item(self.btn_start)
            self.remove_item(self.btn_next)

        # Row 0 — game setup (greyed out in tutorial)
        self.btn_join.disabled = True
        self.btn_leave.disabled = True

        # Row 0/1 — game actions
        self.btn_call.disabled = not states["check"]  # Call uses check logic
        self.btn_check.disabled = not states["check"]
        self.btn_raise.disabled = not states["raise"]
        self.btn_fold.disabled = not states["fold"]

        # Labels and styles
        if s.get_player():
            call_amt = s.game.call_amount(s.get_player())
            if call_amt:
                self.btn_call.label = f"Call {call_amt}"
                self.btn_call.style = discord.ButtonStyle.green
            else:
                self.btn_call.label = "Call"
                self.btn_call.style = discord.ButtonStyle.green

        self.btn_check.label = states["check_label"]
        self.btn_check.style = states["check_style"]
        self.btn_raise.label = states["raise_label"]
        self.btn_raise.style = states["raise_style"]

        # Row 1 — utilities
        # Highlight Rankings when it's blocking game buttons (hand 1 flop)
        if s.current_hand == 1 and s.phase == "playing":
            g = s.game
            if g.street == Street.FLOP and not s.rankings_acked:
                self.btn_rankings.style = discord.ButtonStyle.green  # stands out
            else:
                self.btn_rankings.style = discord.ButtonStyle.grey
        else:
            self.btn_rankings.style = discord.ButtonStyle.grey

        self.btn_wallet.disabled = True  # Always greyed

    # ── Timeout cleanup ───────────────────────────────────────────────────

    async def on_timeout(self):
        tutorial_sessions.pop(self.session.user_id, None)

    # ── Guard ─────────────────────────────────────────────────────────────

    def _wrong_user(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id != self.session.user_id

    # ─────────────────────────────────────────────────────────────────────
    # Row 0: lifecycle + game setup
    # ─────────────────────────────────────────────────────────────────────

    @discord.ui.button(label="▶ Start", style=discord.ButtonStyle.green, row=0)
    async def btn_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True)
            return
        await interaction.response.defer()
        await _start_hand(interaction, self.session, self.channel)

    @discord.ui.button(label="Next Hand ▶", style=discord.ButtonStyle.blurple, row=0)
    async def btn_next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True)
            return
        await interaction.response.defer()
        await _start_hand(interaction, self.session, self.channel)

    @discord.ui.button(label="Join", style=discord.ButtonStyle.green, row=1, disabled=True)
    async def btn_join(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ This is a tutorial — Join is disabled.", ephemeral=True)

    @discord.ui.button(label="Leave", style=discord.ButtonStyle.red, row=1, disabled=True)
    async def btn_leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ This is a tutorial — Leave is disabled.", ephemeral=True)

    @discord.ui.button(label="Call", style=discord.ButtonStyle.green, row=2)
    async def btn_call(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True)
            return
        await interaction.response.defer()
        s = self.session
        if not s.game.is_turn(s.user_id):
            return
        ok, msg = s.game.check_or_call(s.user_id)
        if ok:
            _maybe_slog_clear(s, msg)
            s.slog(msg)
        await _after_action(interaction, s, self.channel)

    @discord.ui.button(label="Check", style=discord.ButtonStyle.blurple, row=2)
    async def btn_check(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True)
            return
        await interaction.response.defer()
        s = self.session
        if not s.game.is_turn(s.user_id):
            return
        # Check that there's nothing to call
        p = s.get_player()
        if p and s.game.call_amount(p) > 0:
            await interaction.followup.send(
                f"❌ There's **{s.game.call_amount(p)}** to call. Use Call button.", ephemeral=True)
            return
        ok, msg = s.game.check_or_call(s.user_id)
        if ok:
            _maybe_slog_clear(s, msg)
            s.slog(msg)
        await _after_action(interaction, s, self.channel)

    @discord.ui.button(label="Raise", style=discord.ButtonStyle.green, row=2)
    async def btn_raise(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True)
            return
        s = self.session
        if not s.game.is_turn(s.user_id):
            await interaction.response.send_message("❌ Not your turn.", ephemeral=True)
            return

        await interaction.response.defer()

        g = s.game
        p = g.get_player(s.user_id)
        call_amt = g.call_amount(p) if p else 0

        # Build the raise picker view — hand 3 shows only All In; hand 1 hides All In
        picker = TutorialRaisePickerView(s, self.channel)

        # Build the info line shown above the picker buttons
        if s.current_hand == 3:
            info = "🎭 **It's bluff time!** Commit fully — go All In 🚀 to make the bots fold!"
        else:
            pot_third  = max(call_amt, g.pot // 3)  if p else 0
            pot_half   = max(call_amt, g.pot // 2)  if p else 0
            stack_half = max(call_amt, p.chips // 2) if p else 0
            info = (
                f"**Raise** — Pot: **{g.pot}**  ·  To call: **{call_amt}**  ·  Stack: **{p.chips if p else '?'}**\n"
                f"1/3 Pot = +{pot_third}  ·  1/2 Pot = +{pot_half}  ·  1/2 Stack = +{stack_half}"
            )

        # Replace the current view on the main embed with the picker
        main_embed = build_tutorial_embed(s)
        picker_guidance = discord.Embed(description=info, color=0x2ECC71)
        await update_board(s)
        files = [s.board_file] if s.board_file else []
        await interaction.edit_original_response(
            embeds=[main_embed, picker_guidance], view=picker, attachments=files
        )

    @discord.ui.button(label="Fold", style=discord.ButtonStyle.red, row=2)
    async def btn_fold(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True)
            return
        await interaction.response.defer()
        s = self.session
        if not s.game.is_turn(s.user_id):
            return
        ok, msg = s.game.fold(s.user_id)
        if ok:
            _maybe_slog_clear(s, msg)
            s.slog(msg)
        await _after_action(interaction, s, self.channel)

    @discord.ui.button(label="My Cards", style=discord.ButtonStyle.grey, row=3)
    async def btn_hole(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True)
            return
        p = self.session.get_player()
        if not p or not p.hole_cards:
            await interaction.response.send_message("❌ No cards right now.", ephemeral=True)
            return

        caption = f"Your hole cards — {p.chips} chips at table\n**Cards:** {hand_str(p.hole_cards)}"
        await interaction.response.send_message(caption, ephemeral=True)

        # Add image if available
        if USE_IMAGES:
            try:
                file = await asyncio.to_thread(card_images.make_strip, p.hole_cards, 0, True, False)
                await interaction.edit_original_response(attachments=[file])
            except Exception:
                pass

    @discord.ui.button(label="Rankings", style=discord.ButtonStyle.grey, row=3)
    async def btn_rankings(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True)
            return
        s = self.session

        # Acknowledge rankings (unlocks game buttons on hand 1 flop)
        was_blocked = (
            s.current_hand == 1
            and s.phase == "playing"
            and s.game.street == Street.FLOP
            and not s.rankings_acked
        )
        s.rankings_acked = True

        # Respond with the rankings popup (visible only to this user)
        await interaction.response.defer()
        await interaction.followup.send(RANKINGS_POPUP, ephemeral=True)

        # If this just unblocked the game buttons, refresh the embed to show them
        if was_blocked:
            view = TutorialView(s, self.channel)
            await update_board(s)
            files = [s.board_file] if s.board_file else []
            await interaction.edit_original_response(
                embeds=_build_embeds(s), view=view, attachments=files
            )

    @discord.ui.button(label="Wallet", style=discord.ButtonStyle.grey, row=3, disabled=True)
    async def btn_wallet(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ This is a tutorial — Wallet is disabled.", ephemeral=True)


# ─────────────────────────────────────────────────────────────────────────────
# Raise picker view  (mirrors poker.py's RaisePickerView, adapted for tutorial)
# ─────────────────────────────────────────────────────────────────────────────

class TutorialRaisePickerView(discord.ui.View):
    """
    Shown in-place (on the main tutorial embed) when the player clicks Raise.
    Hand 1 — hides All In (lesson is proportional raises).
    Hand 3 — shows ONLY All In (the bluff must be a full commitment).
    Includes a Cancel button to restore the normal TutorialView.
    """

    def __init__(self, session: TutorialSession, channel: discord.abc.Messageable):
        super().__init__(timeout=120)
        self.session = session
        self.channel = channel

        if session.current_hand == 3:
            # Bluff hand: only All In is allowed — remove everything else
            self.remove_item(self.third_pot)
            self.remove_item(self.half_pot)
            self.remove_item(self.half_stack)
            self.remove_item(self.custom_btn)
        elif session.current_hand == 1:
            # Teaching hand: no all-in allowed
            self.remove_item(self.all_in_btn)

    # ── Timeout: expire session (consistent with TutorialView) ────────────

    async def on_timeout(self):
        tutorial_sessions.pop(self.session.user_id, None)

    # ── Guard ─────────────────────────────────────────────────────────────

    def _wrong_user(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id != self.session.user_id

    # ── Shared raise executor ─────────────────────────────────────────────

    async def _execute_raise(self, interaction: discord.Interaction, raise_amount: int):
        """Perform the raise, then return to the normal TutorialView."""
        await interaction.response.defer()
        s = self.session

        if not s.game.is_turn(s.user_id):
            await interaction.followup.send("❌ Too slow — it's no longer your turn.", ephemeral=True)
            # Restore the normal view anyway
            view = TutorialView(s, self.channel)
            await update_board(s)
            files = [s.board_file] if s.board_file else []
            await interaction.edit_original_response(
                embeds=_build_embeds(s), view=view, attachments=files
            )
            return

        ok, msg = s.game.raise_bet(s.user_id, raise_amount)
        if not ok:
            await interaction.followup.send(f"❌ {msg}", ephemeral=True)
            return

        if s.current_hand == 3:
            s.player_raised = True
        _maybe_slog_clear(s, msg)
        s.slog(msg)
        await _after_action(interaction, s, self.channel)

    # ── Buttons ───────────────────────────────────────────────────────────

    @discord.ui.button(label="1/3 Pot", style=discord.ButtonStyle.green, row=0)
    async def third_pot(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True); return
        s = self.session
        g = s.game
        p = g.get_player(s.user_id)
        if not p:
            await interaction.response.send_message("❌ Not in game.", ephemeral=True); return
        call_amt = g.call_amount(p)
        amount = max(call_amt, g.pot // 3)
        # Hand 1: guard against accidental all-in
        if s.current_hand == 1 and (call_amt + amount) >= p.chips:
            await interaction.response.send_message(
                f"❌ That would put you all-in! Try a smaller raise instead (e.g. {TUTORIAL_BB * 2}–{TUTORIAL_BB * 4} chips).",
                ephemeral=True,
            ); return
        await self._execute_raise(interaction, amount)

    @discord.ui.button(label="1/2 Pot", style=discord.ButtonStyle.green, row=0)
    async def half_pot(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True); return
        s = self.session
        g = s.game
        p = g.get_player(s.user_id)
        if not p:
            await interaction.response.send_message("❌ Not in game.", ephemeral=True); return
        call_amt = g.call_amount(p)
        amount = max(call_amt, g.pot // 2)
        if s.current_hand == 1 and (call_amt + amount) >= p.chips:
            await interaction.response.send_message(
                f"❌ That would put you all-in! Try a smaller raise instead (e.g. {TUTORIAL_BB * 2}–{TUTORIAL_BB * 4} chips).",
                ephemeral=True,
            ); return
        await self._execute_raise(interaction, amount)

    @discord.ui.button(label="1/2 Stack", style=discord.ButtonStyle.blurple, row=0)
    async def half_stack(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True); return
        s = self.session
        g = s.game
        p = g.get_player(s.user_id)
        if not p:
            await interaction.response.send_message("❌ Not in game.", ephemeral=True); return
        call_amt = g.call_amount(p)
        amount = max(call_amt, p.chips // 2)
        if s.current_hand == 1 and (call_amt + amount) >= p.chips:
            await interaction.response.send_message(
                f"❌ That would put you all-in! Try a smaller raise instead (e.g. {TUTORIAL_BB * 2}–{TUTORIAL_BB * 4} chips).",
                ephemeral=True,
            ); return
        await self._execute_raise(interaction, amount)

    @discord.ui.button(label="All In 🚀", style=discord.ButtonStyle.red, row=0)
    async def all_in_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True); return
        await interaction.response.defer()
        s = self.session
        g = s.game
        p = g.get_player(s.user_id)
        if not p:
            await interaction.followup.send("❌ Not in game.", ephemeral=True); return
        call_needed  = g.call_amount(p)
        raise_on_top = p.chips - call_needed
        if raise_on_top <= 0:
            ok, msg = g.check_or_call(s.user_id)
        else:
            ok, msg = g.raise_bet(s.user_id, raise_on_top)
        if not ok:
            await interaction.followup.send(f"❌ {msg}", ephemeral=True); return
        if s.current_hand == 3:
            s.player_raised = True
        _maybe_slog_clear(s, msg)
        s.slog(msg)
        await _after_action(interaction, s, self.channel)

    @discord.ui.button(label="Custom…", style=discord.ButtonStyle.grey, row=0)
    async def custom_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True); return
        await interaction.response.send_modal(TutorialRaiseCustomModal(self.session, self.channel))

    @discord.ui.button(label="↩ Cancel", style=discord.ButtonStyle.grey, row=1)
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._wrong_user(interaction):
            await interaction.response.send_message("❌ This is not your tutorial.", ephemeral=True); return
        await interaction.response.defer()
        s = self.session
        view = TutorialView(s, self.channel)
        await update_board(s)
        files = [s.board_file] if s.board_file else []
        await interaction.edit_original_response(
            embeds=_build_embeds(s), view=view, attachments=files
        )


# ─────────────────────────────────────────────────────────────────────────────
# Custom raise modal  (reached via Custom… button in TutorialRaisePickerView)
# ─────────────────────────────────────────────────────────────────────────────

class TutorialRaiseCustomModal(discord.ui.Modal, title="Custom Raise"):
    amount = discord.ui.TextInput(
        label="Raise BY how many chips?",
        placeholder=f"e.g. {TUTORIAL_BB * 3}  (minimum is the big blind: {TUTORIAL_BB})",
        min_length=1,
        max_length=6,
    )

    def __init__(self, session: TutorialSession, channel: discord.abc.Messageable):
        super().__init__()
        self.session = session
        self.channel = channel

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        s = self.session
        try:
            amt = int(self.amount.value.strip())
        except ValueError:
            await interaction.followup.send("❌ Enter a whole number.", ephemeral=True)
            return
        if not s.game.is_turn(s.user_id):
            await interaction.followup.send("❌ It's not your turn.", ephemeral=True)
            return

        # Hand 1: prevent all-in or raises that would spend all chips
        if s.current_hand == 1:
            p = s.get_player()
            if p:
                call_amt = s.game.call_amount(p)
                total_bet = call_amt + amt
                if total_bet >= p.chips:
                    await interaction.followup.send(
                        "❌ Don't go all-in yet! Try a smaller raise to build the pot gradually. "
                        f"Raise by {TUTORIAL_BB * 2} to {TUTORIAL_BB * 4} chips instead.",
                        ephemeral=True,
                    )
                    return

        ok, msg = s.game.raise_bet(s.user_id, amt)
        if not ok:
            await interaction.followup.send(f"❌ {msg}", ephemeral=True)
            return
        # Track raise attempt for hand 3 bluff lesson
        if s.current_hand == 3:
            s.player_raised = True
        _maybe_slog_clear(s, msg)
        s.slog(msg)
        await _after_action(interaction, s, self.channel)


# ─────────────────────────────────────────────────────────────────────────────
# Game-flow helpers
# ─────────────────────────────────────────────────────────────────────────────

def _maybe_slog_clear(s: TutorialSession, msg: str):
    if any(kw in msg for kw in STREET_ADVANCE_KWS):
        s.slog_clear()


async def _start_hand(
    interaction: discord.Interaction,
    session: TutorialSession,
    channel: discord.abc.Messageable,
):
    """Set up players, rig cards, deal the hand, advance bots, update embed."""
    s = session
    s.slog_clear()
    s.result_text    = ""
    s.bot_went_allin = False
    s.player_raised  = False
    s.rankings_acked = False
    s.current_hand  += 1

    s.setup_or_reset()

    # Rig hole cards and community cards for this hand
    cfg = HAND_CONFIGS.get(s.current_hand, {})
    if cfg.get("player"):
        s.game._rigged_hands     = {s.user_id: [TreysCard.new(c) for c in cfg["player"]]}
    if cfg.get("community"):
        s.game._rigged_community = [TreysCard.new(c) for c in cfg["community"]]

    ok, msg = s.game.start_hand()
    if not ok:
        await interaction.followup.send(f"❌ Couldn't start hand: {msg}", ephemeral=True)
        tutorial_sessions.pop(s.user_id, None)
        return

    s.slog(msg)
    s.phase = "playing"

    hand_ended = s.run_bots_until_player_turn()
    view = TutorialView(s, channel)

    # Update board image
    await update_board(s)
    files = [s.board_file] if s.board_file else []

    if hand_ended:
        await _finish_hand(interaction, s, channel, view)
    else:
        await interaction.edit_original_response(
            embeds=_build_embeds(s), view=view, attachments=files
        )


async def _after_action(
    interaction: discord.Interaction,
    session: TutorialSession,
    channel: discord.abc.Messageable,
):
    """Called after the human takes any game action.  Runs bots, updates embed."""
    s = session
    g = s.game

    if g._hand_result or g.street in (Street.WAITING, Street.SHOWDOWN):
        await _finish_hand(interaction, s, channel, TutorialView(s, channel))
        return

    hand_ended = s.run_bots_until_player_turn()
    view = TutorialView(s, channel)

    # Update board image
    await update_board(s)
    files = [s.board_file] if s.board_file else []

    if hand_ended:
        await _finish_hand(interaction, s, channel, view)
    else:
        await interaction.edit_original_response(
            embeds=_build_embeds(s), view=view, attachments=files
        )


async def _finish_hand(
    interaction: discord.Interaction,
    session: TutorialSession,
    channel: discord.abc.Messageable,
    _view: TutorialView,
):
    """Wrap up a hand: record outcome, advance state, post reward if first-ever completion."""
    s      = session
    g      = s.game
    result = g._hand_result

    # Build a human-readable result summary
    lines = []
    player_won = False
    if result:
        player_won = s.user_id in [w.user_id for w in result.winners]
        if result.winners:
            wnames = ", ".join(
                "**You** 🏆" if w.user_id == s.user_id else w.display_name
                for w in result.winners
            )
            lines.append(f"🏆 Winner: {wnames}")
        if result.community:
            lines.append(f"Board: {hand_str(result.community)}")
        for uid, delta in result.chip_deltas.items():
            pl   = g.get_player(uid)
            name = "You" if uid == s.user_id else (pl.display_name if pl else "Bot")
            sign = "+" if delta >= 0 else ""
            lines.append(f"  {name}: {sign}{delta}")
    else:
        lines.append("Hand complete.")

    # Track hand 3 outcome for the completion embed
    if s.current_hand == 3:
        s.player_won_h3 = player_won

    s.result_text   = "\n".join(lines)
    g._hand_result  = None
    s.hands_done   += 1

    # ── Tutorial finished ────────────────────────────────────────────────
    if s.hands_done >= TOTAL_HANDS:
        s.phase = "complete"
        tutorial_sessions.pop(s.user_id, None)   # allow re-run later

        is_first = await db.mark_tutorial_complete(s.user_id)
        final_view = TutorialView(s, channel)
        await interaction.edit_original_response(
            embeds=[build_tutorial_embed(s)], view=final_view, attachments=[]
        )

        if is_first:
            reward_ch  = int(os.getenv(ENV_REWARD_CH, "0"))
            ch_mention = f"<#{reward_ch}>" if reward_ch else "#payouts"
            try:
                await channel.send(
                    f"🎉 **{interaction.user.mention}** just completed the poker tutorial "
                    f"for the first time!\n"
                    f"**Congratulations! You earned {TUTORIAL_REWARD}m DMC.**  "
                    f"Link this message and claim in {ch_mention}",
                    allowed_mentions=discord.AllowedMentions(users=True),
                )
            except Exception as e:
                print(f"[tutorial] reward message error: {e}")

    # ── Between hands ────────────────────────────────────────────────────
    else:
        s.phase = "between"
        between_view = TutorialView(s, channel)
        await interaction.edit_original_response(
            embeds=[build_tutorial_embed(s)], view=between_view, attachments=[]
        )


# ─────────────────────────────────────────────────────────────────────────────
# Cog
# ─────────────────────────────────────────────────────────────────────────────

class TutorialCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="tutorial",
        description=(
            "Learn Texas Hold'em with a guided 3-hand walkthrough "
            "(private · fake chips · wallet never touched)"
        ),
    )
    async def tutorial(self, interaction: discord.Interaction):

        allowed_channel_id = int(os.getenv("TUTORIAL_CHANNEL_ID", "0"))

        # Check if the command is being run in the correct channel
        if allowed_channel_id != 0 and interaction.channel_id != allowed_channel_id:
            await interaction.response.send_message(
                f"❌ The tutorial can only be played in <#{allowed_channel_id}>.",
                ephemeral=True
            )
            return

        uid = interaction.user.id

        # ── Resume an existing session ────────────────────────────────────
        if uid in tutorial_sessions:
            session = tutorial_sessions[uid]

            # Stop the old view so its buttons no longer respond
            if session.active_view:
                session.active_view.stop()

            await interaction.response.defer(ephemeral=True)

            # Regenerate board image in case the previous message is gone
            await update_board(session)
            files = [session.board_file] if session.board_file else []

            view = TutorialView(session, interaction.channel)
            await interaction.followup.send(
                embeds=_build_embeds(session),
                view=view,
                ephemeral=True,
                files=files,
            )
            return

        # ── Start a fresh session ─────────────────────────────────────────
        already_done = await db.has_completed_tutorial(uid)
        session = TutorialSession(uid, interaction.user.display_name)
        tutorial_sessions[uid] = session

        await interaction.response.defer(ephemeral=True)
        embed = build_tutorial_embed(session)
        if already_done:
            embed.set_footer(
                text=(
                    "You've already completed the tutorial — replaying for practice. "
                    "No chip reward on repeat runs."
                )
            )

        view = TutorialView(session, interaction.channel)
        await interaction.followup.send(embeds=[embed], view=view, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(TutorialCog(bot))