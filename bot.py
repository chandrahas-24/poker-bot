import discord
from discord.ext import commands
import os
from dotenv import load_dotenv

from database import init_db, recover_chips_in_play
import database as db
from tutorial_db import init_db as init_tutorial_db
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

# ── Donation listener config ──────────────────────────────────────────────────
DONATION_BOT_ID  = 270904126974590976
CHIPS_PER_COIN   = 1_000_000  # 1 chip per 1,000,000 donated coins


wipe_time = datetime.time(hour=3, minute=30, tzinfo=datetime.timezone.utc)
@tasks.loop(time=wipe_time)
async def daily_inactive_wipe():
    from database import wipe_inactive_players
    try:
        wiped = await wipe_inactive_players()
        if wiped:
            channel_id = int(os.getenv("INACTIVITY_CHANNEL_ID", "0"))
            if channel_id:
                try:
                    channel = await bot.fetch_channel(channel_id)
                    summary = "\n".join([
                        f"• {w['username']}: {w['amount_wiped']} chips ({w['recent_hands']} hands)"
                        for w in wiped[:10]
                    ])
                    await channel.send(
                        f"🧹 **Wiped {len(wiped)} inactive player(s):**\n{summary}"
                    )
                except Exception as e:
                    print(f"[Daily Wipe] Failed to send to Discord channel: {e}")

            print(f"[Daily Wipe] Wiped {len(wiped)} inactive players")
    except Exception as e:
        print(f"[Daily Wipe] Error: {e}")


@bot.event
async def on_ready():
    await init_db()
    await init_tutorial_db()

    recovered = await recover_chips_in_play()
    if recovered:
        print(f"⚠️  Recovered chips for {len(recovered)} player(s) after restart:")
        for r in recovered:
            print(f"   {r['username']}: +{r['amount']} chips returned to wallet")

    await bot.load_extension("poker")
    await bot.load_extension("tutorial_cog")

    YOUR_GUILD_ID = int(os.getenv("GUILD_ID", "0"))
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


async def _handle_donation(message: discord.Message, before: discord.Message | None = None):
    """
    Credit chips when Dank Memer confirms a /serverevents donate.

    `before` is passed from on_message_edit so we can retrieve interaction_metadata
    from the cached version — Discord's MESSAGE_UPDATE payload omits unchanged fields
    like interaction_metadata, so after.interaction_metadata is often None.
    """
    print(f"[Donation] _handle_donation called — channel={message.channel.id}, embeds={len(message.embeds)}")

    # Only act in the configured chip channels
    if message.channel.id not in config.ADD_CHIPS_CHANNELS:
        print(f"[Donation] Wrong channel ({message.channel.id} not in {config.ADD_CHIPS_CHANNELS}) — skipping")
        return

    # Resolve donor from slash command metadata.
    # Prefer `before` (fully cached) since the MESSAGE_UPDATE payload often
    # omits interaction_metadata because it didn't change.
    metadata = (
        getattr(before, "interaction_metadata", None)
        or getattr(message, "interaction_metadata", None)
    )
    if not metadata:
        print(f"[Donation] No interaction_metadata on message {message.id} — skipping")
        return

    user = getattr(metadata, "user", None)
    if not user:
        print(f"[Donation] Could not resolve user from interaction_metadata — skipping")
        return

    # Build the text to search from content + all embed fields
    text = message.content or ""
    for embed in message.embeds:
        text += " " + (embed.title or "")
        text += " " + (embed.description or "")

    print(f"[Donation] Text to search: {text!r}")

    if "Successfully donated" not in text:
        print(f"[Donation] 'Successfully donated' not found in text — skipping")
        return

    match = re.search(r"Successfully donated.*?([\d,]+)", text)
    if not match:
        print(f"[Donation] Regex found no amount — skipping")
        return

    donated = int(match.group(1).replace(",", ""))
    chips   = donated // CHIPS_PER_COIN
    print(f"[Donation] Parsed: donated={donated}, chips={chips}")

    if chips <= 0:
        await message.reply(
            f"⚠️ Donation of ⏣{donated:,} is less than the minimum "
            f"⏣{CHIPS_PER_COIN:,} needed for 1 chip."
        )
        return

    new_bal = await db.add_chips(
        bot.user.id,
        bot.user.display_name,
        user.id,
        user.name,
        chips,
        "Dank Memer Donation Exchange",
    )

    await db.log_currency_event(user.id, "Cash In", chips, "Dank Memer Donation")

    await message.reply(
        f"✅ **+{chips:,}** chips → {user.mention} | Balance: **{new_bal:,}** <:poker_chip:1490458259855773707>"
    )
    await message.add_reaction("✅")

    print(f"[Donation] ✅ {user} donated ⏣{donated:,} → +{chips} chip(s) (balance: {new_bal})")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    await bot.process_commands(message)


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    # Dank Memer flow: pending confirmation embed → edited to "Successfully donated ____"
    # Pass `before` so _handle_donation can pull interaction_metadata from the
    # cached version (the MESSAGE_UPDATE payload omits unchanged fields).
    if after.author.id != DONATION_BOT_ID:
        return
    await _handle_donation(after, before=before)


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
        await ctx.send(f"**Deployment Failed:**\n```python\n{e}\n```")


if __name__ == "__main__":
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise ValueError("BOT_TOKEN not set in .env")
    bot.run(token)
