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
                    # 🚨 FIXED: Use fetch_channel instead of get_channel
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
    daily_inactive_wipe.start()  # Start the daily task


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


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    # 0. Restrict to specific channel IDs
    ALLOWED_CHANNEL_ID = config.ADD_CHIPS_CHANNELS
    if after.channel.id not in ALLOWED_CHANNEL_ID:
        return

    # 1. Ignore if the author isn't Dank Memer
    if after.author.id != 270904126974590976:
        return

    # 2. Use the new interaction_metadata
    if not after.interaction_metadata:
        return

    # 3. Check the command name inside the metadata
    if "serverevents" not in after.interaction_metadata.name.lower():
        return

    # 4. Dank Memer uses embeds. Combine content and embed text to search.
    text_to_search = after.content
    for embed in after.embeds:
        if embed.description:
            text_to_search += " " + embed.description
        if embed.title:
            text_to_search += " " + embed.title

    # 5. Look for the exact success trigger
    if "Successfully donated" in text_to_search:
        # 6. Extract the number
        match = re.search(r"Successfully donated.*?([\d,]+)", text_to_search)
        if match:
            amount_str = match.group(1).replace(",", "")
            amount = int(amount_str)//1000000

            # Pull the user from the new metadata object
            user = after.interaction_metadata.user

            # 7. Credit their poker wallet AND capture the new balance
            new_bal = await db.add_chips(
                bot.user.id,
                bot.user.display_name,
                user.id,
                user.name,
                amount,
                "Dank Memer Donation Exchange"
            )

            await db.log_currency_event(user.id, "Cash In", amount, "Dank Memer Donation")

            # Reply directly to Dank Memer's message
            await after.reply(
                f"✅ **+{amount:,}** chips → {user.mention} | Balance: **{new_bal:,}** <:poker_chip:1490458259855773707>"
            )

            await after.add_reaction("✅")

AUTHORIZED_ADMINS = config.DEV_USER_IDS

@bot.command(aliases=["deploy","kiloyeeters"])
async def restart(ctx):
    if ctx.author.id not in AUTHORIZED_ADMINS:
        await ctx.send("**Access Denied.** This command is restricted.")
        return

    await ctx.send("**Initiating Deployment...**")

    try:
        # Pull latest changes from Git
        pull_output = subprocess.check_output(["git", "pull"]).decode("utf-8")
        await ctx.send(f"**Git Pull Success:**\n```\n{pull_output}\n```")

        # Kill the bot. Systemd will restart it immediately.
        await ctx.send("**Restarting bot service...**")
        os._exit(0)

    except Exception as e:
        await ctx.send(f"**Deployment Failed:**\n```python\n{e}\n```")


if __name__ == "__main__":
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise ValueError("BOT_TOKEN not set in .env")
    bot.run(token)

# comment to test
