import discord
from discord.ext import commands
import os
from dotenv import load_dotenv
import traceback

from poker.database import init_db, recover_chips_in_play, close_unclosed_dealer_sessions
from poker import database as db
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
            channel_id = config.INACTIVITY_CHANNEL_ID
            if channel_id:
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
            if hasattr(config, "CASHOUT_CHANNEL_ID") and config.CASHOUT_CHANNEL_ID:
                try:
                    cashout_channel = await bot.fetch_channel(config.CASHOUT_CHANNEL_ID)
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

@bot.event
async def on_ready():
    if bot.startup_complete:
        print("🔄 Reconnected to Discord.")
        return

    bot.startup_complete = True
    await init_db()
    await init_tutorial_db()
    await eventlog_database.init_log_db()
    # await tournament_db.init_db()

    recovered = await recover_chips_in_play()
    await close_unclosed_dealer_sessions()
    if recovered:
        print(f"⚠️  Recovered chips for {len(recovered)} player(s) after restart:")
        for r in recovered:
            print(f"   {r['username']}: +{r['amount']} chips returned to wallet")

    # tourney_recovered = await tournament_db.recover_chips_in_play()
    # if tourney_recovered:
    #     print(f"⚠️  Recovered tournament chips for {len(tourney_recovered)} player(s) after restart:")
    #     for r in tourney_recovered:
    #         print(f"   {r['username']}: +{r['amount']} tournament chips returned to wallet")

    await bot.load_extension("poker.poker")
    await bot.load_extension("poker.tutorial_cog")
    await bot.load_extension("eventlog.eventlog")
    await bot.load_extension("poker.pokerai")
    #await bot.load_extension("uno.unodemo")
    #await bot.load_extension("uno.uno_cog")
    # await bot.load_extension("highlight")
    # await bot.load_extension("tournament.tournament")


    YOUR_GUILD_ID = config.GUILD_ID
    if YOUR_GUILD_ID:
        guild = discord.Object(id=YOUR_GUILD_ID)
        bot.tree.copy_global_to(guild=guild)
        await bot.tree.sync(guild=guild)
        print(f"✅ Synced commands to guild {YOUR_GUILD_ID}")
    else:
        await bot.tree.sync()
        print("✅ Synced commands globally")

    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    daily_inactive_wipe.start()
    clear_donation_cache.start()


_processed_donations = set()

@tasks.loop(hours=2)
async def clear_donation_cache():
    """Periodically clear the memory set to prevent slow memory leaks."""
    _processed_donations.clear()

@bot.event
async def on_raw_message_edit(payload: discord.RawMessageUpdateEvent):
    # ── 0. Prevent concurrent duplicate triggers ──
    if payload.message_id in _processed_donations:
        return

    # ── 1. Channel filter — cheapest check, drop everything else immediately ──
    if payload.channel_id not in config.ADD_CHIPS_CHANNELS:
        return

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

    # ── 7. Credit chips ───────────────────────────────────────────────────────
    try:
        new_bal = await db.add_chips(
            bot.user.id,
            bot.user.display_name,
            user.id,
            user.name,
            chips,
            "Dank Memer Donation Exchange",
        )
        await db.log_currency_event(user.id, "Cash In", chips, "Dank Memer Donation")

        print(f"[Donation] {user} donated ⏣{donated:,} → +{chips} chip(s) | new balance: {new_bal}")

        await message.reply(
            f"✅ **+{chips:,}** chip(s) → {user.mention} | Balance: **{new_bal:,}** <:poker_chip:1490458259855773707>"
        )
        await message.add_reaction("✅")
    except Exception as e:
        print(f"[Donation Error] Failed to process donation for {user.id}: {e}")


_processing_cashouts = set()

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    # 1. Ignore the bot's own reactions
    if payload.user_id == bot.user.id:
        return

    # 2. Restrict to the Cashout channel
    if payload.channel_id != getattr(config, "CASHOUT_CHANNEL_ID", 0):
        return

    # 3. Trigger on standard ✅ OR any custom emoji with "check" or "tick" in the name
    emoji_name = str(payload.emoji.name).lower()
    if "check" not in emoji_name and "tick" not in emoji_name and payload.emoji.name != "✅":
        return

    # 4. Security Check: Must be Admin, Dev, or Payout Manager
    settings = await db.get_settings(payload.guild_id)
    payout_manager_role_id = config.PAYOUT_MANAGER_ROLE

    is_admin = payload.member.guild_permissions.administrator
    is_dev = payload.user_id in config.DEV_USER_IDS
    is_payout_manager = False

    if payout_manager_role_id:
        role = payload.member.guild.get_role(int(payout_manager_role_id))
        if role and role in payload.member.roles:
            is_payout_manager = True

    if not (is_admin or is_dev or is_payout_manager):
        return  # Unauthorized person reacted, ignore silently

    # ── Concurrency Check ─────────────────────────────────────────────────────
    if payload.message_id in _processing_cashouts:
        return
    _processing_cashouts.add(payload.message_id)

    try:
        # 5. Fetch the actual message from Discord
        channel = bot.get_channel(payload.channel_id)
        if not channel:
            return
        try:
            message = await channel.fetch_message(payload.message_id)
        except discord.NotFound:
            return

        # 6. Ensure we don't double-pay a ticket that's already processed
        if "**PAID**" in message.content:
            return

        # 7. Extract the User ID and Amount from your exact ticket format
        user_match = re.search(r"\*\*Username:\*\* <@!?(\d+)>", message.content)
        amount_match = re.search(r"\*\*Amount:\*\* ([\d,]+)", message.content)

        if not user_match or not amount_match:
            return  # Message doesn't match the cashout ticket layout, ignore

        target_user_id = int(user_match.group(1))
        amount = int(amount_match.group(1).replace(",", ""))

        # 8. Attempt to process the payment in the database
        ok = await db.pay_cashout(target_user_id, amount)
        if not ok:
            await channel.send(
                f"❌ <@{payload.user_id}>, failed to process cashout for <@{target_user_id}>. "
                f"They might not have **{amount:,}** chips in their pending queue anymore.",
                delete_after=15
            )
            return

        # 9. Update the ticket visually so staff know it's done
        new_content = message.content + f"\n\n-# **PAID** by <@{payload.user_id}>"
        await message.edit(content=new_content)
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

    cog_name_lower = cog_name.lower() if cog_name else None
    helpers_to_reload = []
    cogs_to_reload = []

    # Map target cog/helpers to reload
    if cog_name_lower == "poker":
        helpers_to_reload = ["config", "database", "engine", "card_images", "jackpot", "taxation"]
        cogs_to_reload = ["poker"]
    elif cog_name_lower == "eventlog":
        helpers_to_reload = ["config", "eventlog_database"]
        cogs_to_reload = ["eventlog"]
    elif cog_name_lower in ("tutorial", "tutorial_cog"):
        helpers_to_reload = ["config", "tutorial_db"]
        cogs_to_reload = ["tutorial_cog"]
    elif cog_name_lower in ("ai", "pokerai"):
        helpers_to_reload = ["config"]
        cogs_to_reload = ["pokerai"]
    elif cog_name_lower in ("hotpotato", "potato"):
        helpers_to_reload = ["config"]
        cogs_to_reload = ["hotpotato"]
    elif cog_name_lower is None:
        helpers_to_reload = ["config", "database", "engine", "card_images", "jackpot", "taxation", "eventlog_database", "tournament_db", "tutorial_db"]
        cogs_to_reload = ["poker", "eventlog", "tutorial_cog", "pokerai", "hotpotato"]
    else:
        # Check if it is a loaded extension
        ext_name = cog_name if cog_name in bot.extensions else (f"cogs.{cog_name}" if f"cogs.{cog_name}" in bot.extensions else None)
        if ext_name:
            cogs_to_reload = [cog_name]
        else:
            await ctx.send(f"❌ Unknown cog/extension `{cog_name}`.")
            return

    # 2. Close database connections for helpers being reloaded to prevent locks/leaks
    dbs_to_close = [h for h in helpers_to_reload if h in ("database", "tournament_db", "tutorial_db")]
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

    if "database" in reloaded_helpers:
        await sys.modules["database"].load_custom_cosmetics()

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
        if YOUR_GUILD_ID:
            guild = discord.Object(id=YOUR_GUILD_ID)
            bot.tree.copy_global_to(guild=guild)
            await bot.tree.sync(guild=guild)
            sync_msg = f"Synced commands to guild {YOUR_GUILD_ID}"
        else:
            await bot.tree.sync()
            sync_msg = "Synced commands globally"
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

    LOCKDOWN_CHANNELS = config.LOCKDOWN_CHANNELS
    RESTRICTED_CHANNELS = config.RESTRICTED_CHANNELS

    # qualified_name captures the full path: e.g., "poker start" or "tourneymgr setcycle"
    cmd_name = interaction.command.qualified_name
    channel_id = interaction.channel_id

    allowed_guild = config.GUILD_ID
    if allowed_guild and interaction.guild_id != allowed_guild:
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
        is_manager_cmd = cmd_name.startswith("tourneymgr") or cmd_name.startswith("pokermgr")

        # Check if the exact subcommand path is in your allowed exceptions
        is_allowed_exception = cmd_name in RESTRICTED_CHANNELS[channel_id]

        if not (is_manager_cmd or is_allowed_exception):
            await interaction.response.send_message(
                f"❌ The command `{cmd_name}` cannot be used in this channel.",
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
