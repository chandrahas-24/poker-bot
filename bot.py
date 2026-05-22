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


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    await bot.process_commands(message)


@bot.event
async def on_raw_message_edit(payload: discord.RawMessageUpdateEvent):
    # Use raw event so we catch ALL edits, not just ones where the
    # before-state is in discord.py's cache.
    if payload.channel_id not in config.ADD_CHIPS_CHANNELS:
        return

    data = payload.data
    author_id = int(data.get("author", {}).get("id", 0))
    if author_id != DONATION_BOT_ID:
        return

    # Debug: dump every embed field so we can see exactly where the text lives
    embeds_raw = data.get("embeds", [])
    print(f"[DEBUG raw_edit] msg={payload.message_id} embeds={len(embeds_raw)}")
    for i, e in enumerate(embeds_raw):
        print(f"[DEBUG raw_edit]   embed[{i}] title={e.get('title')!r} desc={e.get('description')!r}")
        for j, f in enumerate(e.get("fields", [])):
            print(f"[DEBUG raw_edit]   embed[{i}].field[{j}] name={f.get('name')!r} value={f.get('value')!r}")
        footer = e.get("footer", {})
        if footer: print(f"[DEBUG raw_edit]   embed[{i}].footer={footer.get('text')!r}")

    # Build searchable text from every embed text surface
    text = data.get("content", "") or ""
    for e in embeds_raw:
        text += " " + (e.get("title") or "")
        text += " " + (e.get("description") or "")
        for f in e.get("fields", []):
            text += " " + (f.get("name") or "")
            text += " " + (f.get("value") or "")
        text += " " + (e.get("footer", {}).get("text") or "")
        text += " " + (e.get("author", {}).get("name") or "")

    print(f"[DEBUG raw_edit] text={text!r}")

    if "Successfully donated" not in text:
        return

    # Get the amount
    match = re.search(r"Successfully donated.*?([\d,]+)", text)
    if not match:
        print("[Donation] Regex found no amount — skipping")
        return

    donated = int(match.group(1).replace(",", ""))
    chips   = donated // CHIPS_PER_COIN
    print(f"[Donation] Parsed: donated={donated}, chips={chips}")

    if chips <= 0:
        channel = bot.get_channel(payload.channel_id)
        if channel:
            await channel.send(
                f"⚠️ Donation of ⏣{donated:,} is less than the minimum "
                f"⏣{CHIPS_PER_COIN:,} needed for 1 chip."
            )
        return

    # Get the user from interaction_metadata — prefer the cached message
    # since raw payload may omit unchanged fields
    user = None
    if payload.cached_message:
        meta = getattr(payload.cached_message, "interaction_metadata", None)
        user = getattr(meta, "user", None)
    if user is None:
        # Fall back to raw interaction data in the payload
        interaction_data = data.get("interaction_metadata") or data.get("interaction")
        if interaction_data:
            user_data = interaction_data.get("user")
            if user_data:
                user_id   = int(user_data["id"])
                user_name = user_data.get("username", "Unknown")
                guild = bot.get_guild(int(data["guild_id"])) if "guild_id" in data else None
                user  = (guild.get_member(user_id) if guild else None) or await bot.fetch_user(user_id)

    if user is None:
        print("[Donation] Could not resolve user — skipping")
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

    channel = bot.get_channel(payload.channel_id)
    if channel:
        await channel.send(
            f"✅ **+{chips:,}** chips → {user.mention} | Balance: **{new_bal:,}** <:poker_chip:1490458259855773707>"
        )

    print(f"[Donation] ✅ {user} donated ⏣{donated:,} → +{chips} chip(s) (balance: {new_bal})")


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
