import discord
from discord.ext import commands
import os
from dotenv import load_dotenv
from database import init_db, recover_chips_in_play
from tutorial_db import init_db as init_tutorial_db
from discord.ext import tasks
import datetime

load_dotenv()

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents)

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


if __name__ == "__main__":
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise ValueError("BOT_TOKEN not set in .env")
    bot.run(token)