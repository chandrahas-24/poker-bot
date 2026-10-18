import discord
from discord import app_commands
from discord.ext import commands
import asyncio
import os
from dotenv import load_dotenv
import traceback

from poker.database import init_db, recover_chips_in_play, close_unclosed_dealer_sessions
from poker.poker import _as_id_set
from uno.database import init_db as init_uno_db
from poker import database as db
from uno import database as uno_db
from poker.tutorial_db import init_db as init_tutorial_db
from eventlog import eventlog_database
from discord.ext import tasks
import datetime
import subprocess
import config
import re

load_dotenv()



intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents, allowed_mentions=discord.AllowedMentions(users=True))
bot.startup_complete = False


# ── Global slash-command error handler ────────────────────────────────────────
# Without this, any raised error inside a command (including a stale/expired
# interaction that Discord already invalidated) prints as a raw
# CommandInvokeError traceback and looks like a crash. Most of the ones worth
# distinguishing here are interactions that expired before we could respond —
# nothing else can be sent back to Discord for those, so just log and move on.
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    original = error.__cause__ or error

    if isinstance(original, discord.NotFound) and original.code == 10062:
        # "Unknown interaction" — the interaction token was already invalid
        # by the time we tried to respond (bot restart replaying a backlog,
        # a gateway hiccup, etc). There's no interaction left to reply to.
        print(f"⚠️  Stale interaction for /{interaction.command.qualified_name if interaction.command else '?'} "
              f"(user {interaction.user.id}) — Discord had already expired it before we could respond.")
        return

    print(f"🚨 [Command Error] /{interaction.command.qualified_name if interaction.command else '?'}: {original!r}")
    traceback.print_exception(type(original), original, original.__traceback__)

    try:
        message = "❌ Something went wrong running that command."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.HTTPException:
        pass

# ── Donation listener config ──────────────────────────────────────────────────
DONATION_BOT_ID  = 270904126974590976
CHIPS_PER_COIN   = 1_000_000  # 1 chip per 1,000,000 donated coins


# ── Helper: send a DM, silently ignore if the user has DMs closed ─────────────
async def _try_dm(user_id: int, content: str = None, embed: discord.Embed = None) -> bool:
    """
    Attempt to DM a user by ID.  Returns True on success, False if blocked/closed.
    """
    try:
        user = await bot.fetch_user(user_id)
        await user.send(content=content, embed=embed)
        return True
    except (discord.Forbidden, discord.HTTPException):
        return False


# ── Daily inactivity wipe (03:30 UTC) ─────────────────────────────────────────
wipe_time = datetime.time(hour=3, minute=30, tzinfo=datetime.timezone.utc)

@tasks.loop(time=wipe_time)
async def daily_inactive_wipe():
    """
    1. DM players who are exactly 24 h away from a wipe (at-risk warning).
    2. Wipe inactive players: 20% tax → house revenue, 80% auto-queued cashout.
    3. DM every wiped player with their receipt.
    4. Post a summary to the inactivity channel.
    5. Send individual cashout tickets to the staff cashout channel.
    """

    # ── Step 1: 24-hour warning DMs ─────────────────────────────────────────
    try:
        at_risk = await db.get_players_at_risk()
        dm_warned = 0
        for player in at_risk:
            embed = discord.Embed(
                title="⚠️ Inactivity Warning — You're at risk of being wiped!",
                color=0xf39c12,
            )
            embed.description = (
                f"Hey **{player['username']}**! Your chips are scheduled to be wiped "
                f"in approximately **24 hours** because you haven't met the activity requirements.\n\n"
                f"**Your balance:** {player['balance']:,} chips\n"
                f"**Hands played:** {player['recent_hands']} / {db.MIN_HANDS_PER_PERIOD} required\n"
                f"**Chips wagered:** {player['recent_chips_wagered']:,} / {db.MIN_CHIPS_WAGERED:,} required\n\n"
                f"**Play {db.MIN_HANDS_PER_PERIOD - player['recent_hands']} more hand(s) "
                f"before the wipe to keep your chips!**"
            )
            embed.set_footer(text="Wipe runs daily at 03:30 UTC • Use /poker myactivity to check your status")
            if await _try_dm(player["user_id"], embed=embed):
                dm_warned += 1
        if at_risk:
            print(f"[Daily Wipe] Sent 24h warning DMs to {dm_warned}/{len(at_risk)} at-risk player(s)")
    except Exception:
        traceback.print_exc()

    # ── Step 2 & 3: Wipe inactive players + DM receipts ─────────────────────
    try:
        wiped = await db.wipe_inactive_players()

        dm_sent = 0
        for w in wiped:
            tax_pct = int(config.WIPE_TAX_RATE * 100)
            embed = discord.Embed(
                title="🧹 Your chips have been wiped due to inactivity",
                color=0xe74c3c,
            )
            embed.description = (
                f"Hey **{w['username']}**, you didn't meet the activity requirements "
                f"and your chips have been wiped.\n\n"
                f"**Chips wiped:** {w['amount_wiped']:,}\n"
                f"**Inactivity tax ({tax_pct}%):** -{w['tax_amount']:,} chips\n"
                f"**Auto-queued cashout (80%):** {w['cashout_amount']:,} chips\n\n"
                f"The **{w['cashout_amount']:,} chips** have been moved to your pending "
                f"cashout queue.\n\n"
                f"**Hands played:** {w['recent_hands']} / {db.MIN_HANDS_PER_PERIOD} required\n"
                f"**Chips wagered:** {w['recent_chips_wagered']:,} / {db.MIN_CHIPS_WAGERED:,} required"
            )
            embed.set_footer(
                text="Use /poker myactivity to check requirements • /poker request_cashout to manage your funds")
            if await _try_dm(w["user_id"], embed=embed):
                dm_sent += 1

        # ── Step 4: Channel summary ──────────────────────────────────────────
        if wiped:
            for channel_id in _as_id_set(config.INACTIVITY_CHANNEL_ID):
                try:
                    channel = await bot.fetch_channel(channel_id)
                    tax_total = sum(w["tax_amount"] for w in wiped)
                    cashout_total = sum(w["cashout_amount"] for w in wiped)
                    summary_lines = [
                        f"• **{w['username']}**: {w['amount_wiped']:,} wiped "
                        f"(tax: {w['tax_amount']:,} | cashout queued: {w['cashout_amount']:,}, "
                        f"{w['recent_hands']} hands)"
                        for w in wiped[:10]
                    ]
                    summary = "\n".join(summary_lines)
                    await channel.send(
                        f"🧹 **Wiped {len(wiped)} inactive player(s):**\n{summary}\n\n"
                        f"**Total tax collected:** {tax_total:,} chips\n"
                        f"**Total cashouts queued:** {cashout_total:,} chips\n"
                        f"*(DMs sent: {dm_sent}/{len(wiped)})*"
                    )
                except Exception:
                    traceback.print_exc()

            # ── Step 5: Staff Cashout Tickets ────────────────────────────────
            for cashout_channel_id in _as_id_set(getattr(config, "CASHOUT_CHANNEL_ID", None)):
                try:
                    cashout_channel = await bot.fetch_channel(cashout_channel_id)
                    for w in wiped:
                        if w.get('cashout_amount', 0) > 0:
                            ticket_msg = (
                                f"**Username:** <@{w['user_id']}>\n"
                                f"**Amount:** {w['cashout_amount']:,} <:poker_chip:1490458259855773707>\n"
                                f"**Notes:** Auto-Cashout (Inactivity Wipe)"
                            )
                            await cashout_channel.send(ticket_msg)
                except Exception:
                    traceback.print_exc()

            print(
                f"[Daily Wipe] Wiped {len(wiped)} player(s) | "
                f"tax: {sum(w['tax_amount'] for w in wiped):,} | "
                f"cashouts queued: {sum(w['cashout_amount'] for w in wiped):,} | "
                f"DMs sent: {dm_sent}/{len(wiped)}"
            )
    except Exception:
        traceback.print_exc()

# ── Daily UNO inactivity wipe (03:45 UTC — offset from poker's so they don't overlap) ──
uno_wipe_time = datetime.time(hour=3, minute=45, tzinfo=datetime.timezone.utc)

def _uno_todo(p: dict) -> str:
    """Unmet requirements reported independently (never negative)."""
    from uno import database as uno_db
    rounds_left = max(0, uno_db.MIN_ROUNDS_PER_PERIOD - p["recent_rounds"])
    wager_left = max(0, uno_db.MIN_CHIPS_WAGERED - p["recent_chips_wagered"])
    parts = []
    if rounds_left:
        parts.append(f"play **{rounds_left}** more round(s)")
    if wager_left:
        parts.append(f"wager **{wager_left:,}** more chips")
    if not parts:
        return "**You've met the requirements — keep playing to stay safe!**"
    return "**To keep your chips: " + " and ".join(parts) + " before the wipe!**"


@tasks.loop(time=uno_wipe_time)
async def daily_uno_inactive_wipe():
    """Same shape as daily_inactive_wipe, but against uno/database.py's
    separate wallets — a player's UNO activity/balance is tracked
    independently of their poker one."""
    from uno import database as uno_db

    try:
        at_risk = await uno_db.get_players_at_risk()
        dm_warned = 0
        for player in at_risk:
            embed = discord.Embed(
                title="⚠️ UNO Inactivity Warning — You're at risk of being wiped!",
                color=0xf39c12,
            )
            embed.description = (
                f"Hey **{player['username']}**! Your UNO chips are scheduled to be wiped "
                f"in approximately **24 hours** because you haven't met the activity requirements.\n\n"
                f"**Your balance:** {player['balance']:,} chips\n"
                f"**Rounds played:** {player['recent_rounds']} / {uno_db.MIN_ROUNDS_PER_PERIOD} required\n"
                f"**Chips wagered:** {player['recent_chips_wagered']:,} / {uno_db.MIN_CHIPS_WAGERED:,} required\n\n"
                f"{_uno_todo(player)}"
            )
            embed.set_footer(text="UNO wipe runs daily at 03:45 UTC")
            if await _try_dm(player["user_id"], embed=embed):
                dm_warned += 1
        if at_risk:
            print(f"[Daily UNO Wipe] Sent 24h warning DMs to {dm_warned}/{len(at_risk)} at-risk player(s)")
    except Exception:
        traceback.print_exc()

    try:
        wiped = await uno_db.wipe_inactive_players()
        dm_sent = 0
        for w in wiped:
            tax_pct = int(config.UNO_WIPE_TAX_RATE * 100)
            embed = discord.Embed(
                title="🧹 Your UNO chips have been wiped due to inactivity",
                color=0xe74c3c,
            )
            embed.description = (
                f"Hey **{w['username']}**, you didn't meet the activity requirements "
                f"and your UNO chips have been wiped.\n\n"
                f"**Chips wiped:** {w['amount_wiped']:,}\n"
                f"**Inactivity tax ({tax_pct}%):** -{w['tax_amount']:,} chips\n"
                f"**Auto-queued cashout ({100 - tax_pct}%):** {w['cashout_amount']:,} chips\n\n"
                f"**Rounds played:** {w['recent_rounds']} / {uno_db.MIN_ROUNDS_PER_PERIOD} required\n"
                f"**Chips wagered:** {w['recent_chips_wagered']:,} / {uno_db.MIN_CHIPS_WAGERED:,} required"
            )
            if await _try_dm(w["user_id"], embed=embed):
                dm_sent += 1

        if wiped:
            channel_id = config.UNO_INACTIVITY_CHANNEL_ID
            if channel_id:
                try:
                    channel = await bot.fetch_channel(channel_id)
                    tax_total = sum(w["tax_amount"] for w in wiped)
                    cashout_total = sum(w["cashout_amount"] for w in wiped)
                    summary_lines = [
                        f"• **{w['username']}**: {w['amount_wiped']:,} wiped "
                        f"(tax: {w['tax_amount']:,} | cashout queued: {w['cashout_amount']:,}, "
                        f"{w['recent_rounds']} rounds)"
                        for w in wiped[:10]
                    ]
                    summary = "\n".join(summary_lines)
                    await channel.send(
                        f"🧹 **[UNO] Wiped {len(wiped)} inactive player(s):**\n{summary}\n\n"
                        f"**Total tax collected:** {tax_total:,} chips\n"
                        f"**Total cashouts queued:** {cashout_total:,} chips\n"
                        f"*(DMs sent: {dm_sent}/{len(wiped)})*"
                    )
                except Exception:
                    traceback.print_exc()

            # Staff cashout tickets — same layout on_raw_reaction_add parses
            # (and the UNO chip emoji routes it to the UNO economy).
            for cashout_channel_id in _as_id_set(getattr(config, "UNO_CASHOUT_CHANNEL_ID", None)):
                try:
                    cashout_channel = await bot.fetch_channel(cashout_channel_id)
                    for w in wiped:
                        if w.get("cashout_amount", 0) > 0:
                            await cashout_channel.send(
                                f"**Username:** <@{w['user_id']}>\n"
                                f"**Amount:** {w['cashout_amount']:,} {config.UNO_CHIP_EMOJI}\n"
                                f"**Notes:** Auto-Cashout (Inactivity Wipe)"
                            )
                except Exception:
                    traceback.print_exc()

            print(
                f"[Daily UNO Wipe] Wiped {len(wiped)} player(s) | "
                f"tax: {sum(w['tax_amount'] for w in wiped):,} | "
                f"cashouts queued: {sum(w['cashout_amount'] for w in wiped):,} | "
                f"DMs sent: {dm_sent}/{len(wiped)}"
            )
    except Exception:
        traceback.print_exc()

_startup_lock = asyncio.Lock()


async def _load_ext(name: str):
    """Idempotent load so a retried startup doesn't trip over extensions
    that already loaded on the failed attempt."""
    if name not in bot.extensions:
        await bot.load_extension(name)


@bot.event
async def on_ready():
    if bot.startup_complete:
        print("🔄 Reconnected to Discord.")
        return

    async with _startup_lock:
        if bot.startup_complete:
            return
        await _run_startup()
        # Only after EVERY step succeeded — if anything above raised, the
        # next on_ready retries instead of being skipped for good.
        bot.startup_complete = True


async def _run_startup():
    await init_db()
    await init_uno_db()
    await init_tutorial_db()
    await eventlog_database.init_log_db()
    # await tournament_db.init_db()

    recovered = await recover_chips_in_play()
    await close_unclosed_dealer_sessions()
    if recovered:
        print(f"⚠️  Recovered chips for {len(recovered)} player(s) after restart:")
        for r in recovered:
            print(f"   {r['username']}: +{r['amount']} chips returned to wallet")

    uno_locked = await uno_db.get_total_chips_in_play()
    if uno_locked:
        print(f"⚠️  {uno_locked} UNO chip(s) still locked in chips_in_play after restart — "
              f"NOT auto-refunded. Check each channel with an open UNO table: /uno resume "
              f"first, or /unoadmin recoverchips once you've confirmed a game won't be resumed.")

    # tourney_recovered = await tournament_db.recover_chips_in_play()
    # if tourney_recovered:
    #     print(f"⚠️  Recovered tournament chips for {len(tourney_recovered)} player(s) after restart:")
    #     for r in tourney_recovered:
    #         print(f"   {r['username']}: +{r['amount']} tournament chips returned to wallet")

    await _load_ext("poker.poker")
    await _load_ext("poker.tutorial_cog")
    await _load_ext("eventlog.eventlog")
    await _load_ext("poker.pokerai")
    #await bot.load_extension("hotpotato")
    #await bot.load_extension("uno.unodemo")
    await _load_ext("uno.uno_cog")
    await _load_ext("highlight")
    # await bot.load_extension("tournament.tournament")


    YOUR_GUILD_ID = config.GUILD_ID
    if YOUR_GUILD_ID:
        guild = discord.Object(id=YOUR_GUILD_ID)
        await bot.tree.sync(guild=guild)
        print(f"✅ Synced commands to guild {YOUR_GUILD_ID}")

    await bot.tree.sync()
    print("✅ Synced global/user-install commands")

    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    if not daily_inactive_wipe.is_running():
        daily_inactive_wipe.start()
    if not daily_uno_inactive_wipe.is_running():
        daily_uno_inactive_wipe.start()

    if config.DONATION_CONVERSION_ENABLED and not clear_donation_cache.is_running():
        clear_donation_cache.start()


_processed_donations = set()

@tasks.loop(hours=2)
async def clear_donation_cache():
    """Periodically clear the memory set to prevent slow memory leaks."""
    _processed_donations.clear()

if config.DONATION_CONVERSION_ENABLED:
    @bot.event
    async def on_raw_message_edit(payload: discord.RawMessageUpdateEvent):
        # ── 0. Prevent concurrent duplicate triggers ──
        if payload.message_id in _processed_donations:
            return

        # ── 1. Channel filter — cheapest check, drop everything else immediately ──
        is_uno = payload.channel_id in config.UNO_DONATION_CHANNELS
        if payload.channel_id not in config.ADD_CHIPS_CHANNELS and not is_uno:
            return
        target_db = uno_db if is_uno else db
        chip_emoji = config.UNO_CHIP_EMOJI if is_uno else "<:poker_chip:1490458259855773707>"

        # ── 2. Author filter — check raw payload before fetching the full message ──
        author_id = int((payload.data.get("author") or {}).get("id", 0))
        if author_id and author_id != DONATION_BOT_ID:
            return  # Not Dank Memer, bail before any API call

        # ── 3. Fetch full message (needed for Components v2 text + interaction_metadata) ──
        channel = bot.get_channel(payload.channel_id)
        if not channel:
            return
        try:
            message = await channel.fetch_message(payload.message_id)
        except discord.NotFound:
            return

        # Double-check author on the fetched message (author may have been absent in delta)
        if message.author.id != DONATION_BOT_ID:
            return

        # ── 3.5. Ensure we haven't already processed this message (persistent check) ──
        for reaction in message.reactions:
            if reaction.me and str(reaction.emoji) == "✅":
                return

        # ── 4. Content filter — extract all text, look for the success phrase ──
        def _extract_component_text(components: list) -> str:
            """Recursively extract text from Components v2 nodes."""
            out = ""
            for c in components:
                out += " " + (c.get("content") or "")       # type 10 Text Display
                out += " " + (c.get("description") or "")   # type 9 Section
                out += _extract_component_text(c.get("components") or [])
                if c.get("accessory"):
                    out += _extract_component_text([c["accessory"]])
            return out

        try:
            raw = await bot.http.get_message(channel.id, message.id)
        except Exception:
            traceback.print_exc()
            return

        text = raw.get("content") or ""
        for e in raw.get("embeds") or []:
            text += " " + (e.get("title") or "")
            text += " " + (e.get("description") or "")
            for f in e.get("fields") or []:
                text += " " + (f.get("name") or "")
                text += " " + (f.get("value") or "")
            text += " " + ((e.get("footer") or {}).get("text") or "")
            text += " " + ((e.get("author") or {}).get("name") or "")
        text += _extract_component_text(raw.get("components") or [])

        if "Successfully donated" not in text:
            return

        # ── Lock the transaction IMMEDIATELY to prevent fast race conditions ──
        if payload.message_id in _processed_donations:
            return
        _processed_donations.add(payload.message_id)

        # ── 5. Parse amount ───────────────────────────────────────────────────────
        match = re.search(r"Successfully donated[^0-9]*([\d,]+)", text)
        if not match:
            print("[Donation] Regex found no amount — skipping")
            # Unlock if it was a false positive so it can try again
            _processed_donations.discard(payload.message_id)
            return

        donated = int(match.group(1).replace(",", ""))
        chips   = donated // CHIPS_PER_COIN  # floor division: 1.9M → 1 chip

        if chips <= 0:
            await message.reply(
                f"⚠️ Donation of ⏣{donated:,} is less than the minimum "
                f"⏣{CHIPS_PER_COIN:,} needed for 1 chip."
            )
            return

        # ── 6. Resolve donor ─────────────────────────────────────────────────────
        meta = getattr(message, "interaction_metadata", None)
        user = getattr(meta, "user", None)
        if user is None:
            print(f"[Donation] No interaction_metadata on message {message.id} — skipping")
            # Unlock if we couldn't resolve the user
            _processed_donations.discard(payload.message_id)
            return

        if message.guild:
            user = message.guild.get_member(user.id) or user

        # ── 7. Claim, then credit ─────────────────────────────────────────────────
        # The persistent ✅ reaction is the claim marker (step 3.5 checks it),
        # so it goes on BEFORE crediting: nothing after the credit can leave
        # a credited donation looking unprocessed.
        try:
            await message.add_reaction("✅")
        except Exception as e:
            print(f"[Donation Error] Couldn't claim message {message.id}: {e}")
            _processed_donations.discard(payload.message_id)
            return

        try:
            if is_uno:
                # dedupe record + credit + logs in ONE DB transaction, keyed by message id
                new_bal = await target_db.credit_donation(
                    payload.message_id, bot.user.id, bot.user.display_name, user.id, user.name,
                    chips, "Dank Memer Donation Exchange")
                if new_bal is None:
                    print(f"[Donation/UNO] Message {message.id} was already credited — skipping")
                    return
            else:
                new_bal = await target_db.add_chips(
                    bot.user.id, bot.user.display_name, user.id, user.name, chips,
                    "Dank Memer Donation Exchange")
        except Exception as e:
            print(f"[Donation Error] Failed to process donation for {user.id}: {e}")
            try:
                await message.remove_reaction("✅", bot.user)  # nothing was credited — allow a retry
            except Exception:
                pass
            _processed_donations.discard(payload.message_id)
            return

        if not is_uno:
            try:
                await target_db.log_currency_event(user.id, "Cash In", chips, "Dank Memer Donation")
            except Exception:
                traceback.print_exc()  # chips are already credited; the audit row is best-effort

        print(f"[Donation{'/UNO' if is_uno else ''}] {user} donated ⏣{donated:,} → +{chips} chip(s) | new balance: {new_bal}")
        try:
            await message.reply(
                f"✅ **+{chips:,}** chip(s) → {user.mention} | Balance: **{new_bal:,}** {chip_emoji}"
            )
        except Exception:
            traceback.print_exc()


_processing_cashouts = set()

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    # 1. Ignore the bot's own reactions
    if payload.user_id == bot.user.id:
        return

    # 2. Restrict to a cashout channel — poker's and UNO's may be the same
    # channel, different channels, or (for poker) a set of several; either
    # way this is just "is it worth looking at all", not which economy.
    poker_channels = _as_id_set(getattr(config, "CASHOUT_CHANNEL_ID", None))
    uno_channels = _as_id_set(getattr(config, "UNO_CASHOUT_CHANNEL_ID", None))
    if payload.channel_id not in poker_channels and payload.channel_id not in uno_channels:
        return

    # 3. Trigger on standard ✅ OR any custom emoji with "check" or "tick" in the name
    emoji_name = str(payload.emoji.name).lower()
    if "check" not in emoji_name and "tick" not in emoji_name and payload.emoji.name != "✅":
        return

    # ── Concurrency Check ─────────────────────────────────────────────────────
    if payload.message_id in _processing_cashouts:
        return
    _processing_cashouts.add(payload.message_id)

    try:
        # 4. Fetch the actual message from Discord
        channel = bot.get_channel(payload.channel_id)
        if not channel:
            return
        try:
            message = await channel.fetch_message(payload.message_id)
        except discord.NotFound:
            return

        # 5. Ensure we don't double-pay a ticket that's already processed
        if "**PAID**" in message.content:
            return

        # 6. Which economy this ticket belongs to has to come from the
        # message itself, not the channel, now that both can post to the
        # same one — /uno request_cashout always embeds
        # config.UNO_CHIP_EMOJI verbatim in its ticket text; anything
        # posted to a cashout channel without it is a poker ticket. This
        # works whether the two channels are configured the same or
        # different — channel identity was never a reliable enough signal
        # once they can overlap.
        is_uno = config.UNO_CHIP_EMOJI in message.content
        target_db = uno_db if is_uno else db

        # 7. Security Check: Admin/Dev always allowed; otherwise gated by
        # that economy's own payout-manager role specifically — a poker
        # payout manager reacting to a UNO ticket (or vice versa) doesn't
        # count, same as if the channels were still separate.
        payout_manager_role_id = config.UNO_PAYOUT_MANAGER_ROLE if is_uno else config.PAYOUT_MANAGER_ROLE

        is_admin = payload.member.guild_permissions.administrator
        is_dev = payload.user_id in config.DEV_USER_IDS
        is_payout_manager = False

        if payout_manager_role_id:
            role = payload.member.guild.get_role(int(payout_manager_role_id))
            if role and role in payload.member.roles:
                is_payout_manager = True

        if not (is_admin or is_dev or is_payout_manager):
            return  # Unauthorized person reacted, ignore silently

        # 8. Extract the User ID and Amount from the ticket format (same
        # shape for poker and UNO — /uno request_cashout posts identically)
        user_match = re.search(r"\*\*Username:\*\* <@!?(\d+)>", message.content)
        amount_match = re.search(r"\*\*Amount:\*\* ([\d,]+)", message.content)

        if not user_match or not amount_match:
            return  # Message doesn't match the cashout ticket layout, ignore

        target_user_id = int(user_match.group(1))
        amount = int(amount_match.group(1).replace(",", ""))

        # 9. Pay exactly once.
        paid_line = f"\n\n-# **PAID** by <@{payload.user_id}>"
        fail_msg = (f"❌ <@{payload.user_id}>, failed to process cashout for <@{target_user_id}>. "
                    f"They might not have **{amount:,}** chips in their pending queue anymore.")

        if is_uno:
            # Idempotent by ticket (message id), recorded in the same DB
            # transaction as the deduction. The message edit is display only.
            if await target_db.is_ticket_paid(payload.message_id):
                try:
                    await message.edit(content=message.content + paid_line)  # repair the display
                except Exception:
                    traceback.print_exc()
                return
            if not await target_db.pay_cashout(target_user_id, amount, ticket_id=payload.message_id):
                await channel.send(fail_msg, delete_after=15)
                return
            try:
                await message.edit(content=message.content + paid_line)
            except Exception:
                traceback.print_exc()  # payment is recorded; a retry just repairs the display
        else:
            # poker's pay_cashout keeps no per-ticket record, so mark the
            # ticket FIRST and pay second: if the edit fails nothing is paid,
            # and if the payment fails the mark is reverted.
            original = message.content
            try:
                await message.edit(content=original + paid_line)
            except Exception:
                traceback.print_exc()
                await channel.send(f"❌ <@{payload.user_id}>, couldn't mark the ticket as paid — nothing was paid out.",
                                   delete_after=15)
                return
            try:
                ok = await target_db.pay_cashout(target_user_id, amount)
            except Exception:
                traceback.print_exc()
                ok = False
            if not ok:
                try:
                    await message.edit(content=original)
                except Exception:
                    traceback.print_exc()
                await channel.send(fail_msg, delete_after=15)
                return
    finally:
        _processing_cashouts.discard(payload.message_id)


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    # Hook into poker cog for embed resend counter
    await bot.process_commands(message)

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, discord.ext.commands.CommandNotFound):
        return


AUTHORIZED_ADMINS = config.DEV_USER_IDS

@bot.command(aliases=["deploy","kiloyeeters"])
async def restart(ctx):
    if ctx.author.id not in AUTHORIZED_ADMINS:
        await ctx.send("**Access Denied.** This command is restricted.")
        return

    await ctx.send("**Initiating Deployment...**")

    try:
        pull_output = subprocess.check_output(["git", "pull"]).decode("utf-8")
        await ctx.send(f"**Git Pull Success:**\n```\n{pull_output}\n```")
        await ctx.send("**Restarting bot service...**")
        os._exit(0)

    except Exception as e:
        traceback.print_exc()
        await ctx.send(f"**Deployment Failed:**\n```python\n{e}\n```")


@bot.command(aliases=["hotreload"])
async def reload(ctx, cog_name: str = None):
    global AUTHORIZED_ADMINS
    if ctx.author.id not in AUTHORIZED_ADMINS:
        await ctx.send("**Access Denied.** This command is restricted.")
        return

    await ctx.send("**Initiating Code Hot-Reload...**")

    # 1. Run git pull inside the repository root
    try:
        repo_dir = os.path.dirname(os.path.abspath(__file__))
        pull_output = subprocess.check_output(["git", "pull"], cwd=repo_dir).decode("utf-8")
        await ctx.send(f"**Git Pull Success:**\n```\n{pull_output}\n```")
    except Exception as e:
        await ctx.send(f"⚠️ **Git Pull failed (continuing reload anyway):**\n```python\n{e}\n```")

    import sys
    import importlib

    helpers_to_reload = []
    cogs_to_reload = []
    UNO_HELPERS = ["config", "uno.cards", "uno.hand_render", "uno.uno_engine", "uno.database", "uno.unodemo"]

    def resolve_target(name_lower, name_raw):
        """Returns (helpers, cogs) for a single target token."""
        if name_lower == "poker":
            return (["config", "poker.database", "poker.engine", "poker.card_images", "poker.jackpot", "poker.taxation", "poker.leaderboard_image"],
                     ["poker.poker"])
        elif name_lower == "eventlog":
            return (["config", "eventlog.eventlog_database"], ["eventlog.eventlog"])
        elif name_lower in ("tutorial", "tutorial_cog"):
            return (["config", "poker.tutorial_db"], ["poker.tutorial_cog"])
        elif name_lower in ("ai", "pokerai"):
            return (["config"], ["poker.pokerai"])
        elif name_lower in ("uno",):
            return (UNO_HELPERS, ["uno.uno_cog"])
        elif name_lower in ("highlight", "hl"):
            return (["config"], ["highlight"])
        else:
            # Generic fallback: any dotted path works here, not just the named
            # targets above — no need to pre-register a module in this file.
            ext_name = name_raw if name_raw in bot.extensions else (f"cogs.{name_raw}" if f"cogs.{name_raw}" in bot.extensions else None)
            if ext_name:
                # It's a registered cog/extension — reload it as one.
                return ([], [name_raw])
            elif name_raw in sys.modules:
                # It's a plain helper module already imported somewhere (e.g. poker.chaos) —
                # reload it in place. Anything doing `from . import <mod>` elsewhere picks
                # up the change automatically since it holds a reference to this same module object.
                return ([name_raw], [])
            else:
                # Never imported yet under this name — try a fresh import so a brand-new
                # file can still be picked up without a restart.
                importlib.import_module(name_raw)  # raises if truly not found
                return ([name_raw], [])

    if cog_name is None:
        helpers_to_reload = ["config", "poker.database", "poker.engine", "poker.card_images", "poker.jackpot", "poker.taxation", "eventlog.eventlog_database", "tournament.tournament_db", "poker.tutorial_db", "poker.leaderboard_image"] + [h for h in UNO_HELPERS if h != "config"]
        cogs_to_reload = ["poker.poker", "eventlog.eventlog", "poker.tutorial_cog", "poker.pokerai", "uno.uno_cog", "highlight"]
    else:
        # Comma-separated list of targets — aliases, dotted helper modules, or cogs, any mix.
        for token in [t.strip() for t in cog_name.split(",") if t.strip()]:
            try:
                h, c = resolve_target(token.lower(), token)
            except Exception as e:
                await ctx.send(f"❌ Unknown cog/module `{token}`:\n```python\n{e}\n```")
                return
            for x in h:
                if x not in helpers_to_reload:
                    helpers_to_reload.append(x)
            for x in c:
                if x not in cogs_to_reload:
                    cogs_to_reload.append(x)

    # 2. Close database connections for helpers being reloaded to prevent locks/leaks
    #    Generic: any helper module currently holding an open `_db` handle, not just
    #    the historically-known db module names.
    dbs_to_close = [h for h in helpers_to_reload if h in sys.modules and getattr(sys.modules[h], "_db", None) is not None]
    closed_dbs = []
    for db_name in dbs_to_close:
        if db_name in sys.modules:
            db_mod = sys.modules[db_name]
            if getattr(db_mod, "_db", None) is not None:
                try:
                    await db_mod._db.close()
                    db_mod._db = None
                    closed_dbs.append(db_name)
                except Exception as e:
                    await ctx.send(f"⚠️ Failed to close database connection for `{db_name}`:\n```python\n{e}\n```")

    # 3. Reload helper modules
    reloaded_helpers = []
    for helper in helpers_to_reload:
        if helper in sys.modules:
            try:
                importlib.reload(sys.modules[helper])
                reloaded_helpers.append(helper)
            except Exception as e:
                await ctx.send(f"❌ Failed to reload helper `{helper}`:\n```python\n{e}\n```")
                return

    if "poker.database" in reloaded_helpers:
        await sys.modules["poker.database"].load_custom_cosmetics()

    # 4. Reload cogs
    reloaded_cogs = []
    for cog in cogs_to_reload:
        ext_name = cog if cog in bot.extensions else (f"cogs.{cog}" if f"cogs.{cog}" in bot.extensions else None)
        if ext_name:
            try:
                await bot.reload_extension(ext_name)
                reloaded_cogs.append(cog)
            except Exception as e:
                await ctx.send(f"❌ Failed to reload cog `{cog}`:\n```python\n{e}\n```")
                return

    # 5. Reassign global/static config dependencies in bot.py if config was reloaded
    if "config" in reloaded_helpers:
        AUTHORIZED_ADMINS = config.DEV_USER_IDS

    # 5.5 Re-sync Slash Commands Tree with Discord
    sync_msg = ""
    try:
        YOUR_GUILD_ID = config.GUILD_ID
        sync_lines = []

        if YOUR_GUILD_ID:
            guild = discord.Object(id=YOUR_GUILD_ID)
            await bot.tree.sync(guild=guild)
            sync_lines.append(f"Synced commands to guild {YOUR_GUILD_ID}")

        await bot.tree.sync()
        sync_lines.append("Synced global/user-install commands")

        sync_msg = "\n".join(sync_lines)

    except Exception as e:
        sync_msg = f"⚠️ **Command Sync failed:** {e}"

    # 6. Report success
    msg = "✅ **Hot-Reload Complete!**\n"
    if closed_dbs:
        msg += f"**Closed DB Connections:** {', '.join(closed_dbs)}\n"
    if reloaded_helpers:
        msg += f"**Reloaded Helpers:** {', '.join(reloaded_helpers)}\n"
    if reloaded_cogs:
        msg += f"**Reloaded Cogs:** {', '.join(reloaded_cogs)}\n"
    if sync_msg:
        msg += f"{sync_msg}\n"
    await ctx.send(msg)


async def global_channel_restriction(interaction: discord.Interaction) -> bool:
    if not interaction.command:
        return True

    # Several commands (equiptitle, equipwinmsg, equipskin, grant_cosmetic,
    # dealerhours) have autocomplete-enabled options. Discord fires an
    # autocomplete-type interaction on every keystroke in those fields, and
    # this interaction_check runs for those too — but an autocomplete
    # interaction can ONLY be answered with an autocomplete result (Discord
    # response type 8), never a regular message. Trying to send_message()
    # a rejection on one throws "In type: Value must be one of {8}" — which
    # is exactly the flood of errors this was causing in any restricted or
    # lockdown channel. Autocomplete doesn't execute anything by itself
    # (the real command invocation gets its own interaction_check call
    # afterward), so it's safe to just let it through untouched here.
    if interaction.type is discord.InteractionType.autocomplete:
        return True

    LOCKDOWN_CHANNELS = config.LOCKDOWN_CHANNELS
    RESTRICTED_CHANNELS = config.RESTRICTED_CHANNELS


    # qualified_name captures the full path: e.g., "poker start" or "tourneymgr setcycle"
    command_name = (interaction.command.qualified_name
        if interaction.command
        else ""
    )
    channel_id = interaction.channel_id

    USER_INSTALL_COMMANDS = {
        "stats",
        "leaderboard",
        "jackpot",
        "drawcards",
        "myactivity",
        "changelog",
        "responsible-gambling"
    }

    allowed_guild = config.GUILD_ID
    if (allowed_guild and interaction.guild_id != allowed_guild and command_name not in USER_INSTALL_COMMANDS):
        await interaction.response.send_message(
            "❌ This bot is exclusively configured for another server.",
            ephemeral=True
        )
        return False

    # ─── ABSOLUTE LOCKDOWN CHANNEL ────────────────────────────────────
    if channel_id in LOCKDOWN_CHANNELS:
        await interaction.response.send_message(
            "❌ Nuh uh no commands here",
            ephemeral=True
        )
        return False

    # ─── WHITELIST CHANNELS (MGR + EXCEPTIONS ONLY) ───────────────────
    if channel_id in RESTRICTED_CHANNELS:
        # Automatically allow ANY command inside the tourneymgr slash group
        is_manager_cmd = command_name.startswith("tourneymgr") or command_name.startswith("pokermgr")

        # Check if the exact subcommand path is in your allowed exceptions
        is_allowed_exception = command_name in RESTRICTED_CHANNELS[channel_id]

        if not (is_manager_cmd or is_allowed_exception):
            await interaction.response.send_message(
                f"❌ The command `{command_name}` cannot be used in this channel.",
                ephemeral=True
            )
            return False

    return True

bot.tree.interaction_check = global_channel_restriction


if __name__ == "__main__":
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise ValueError("BOT_TOKEN not set in .env")
    bot.run(token)
