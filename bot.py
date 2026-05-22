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
    if payload.channel_id not in config.ADD_CHIPS_CHANNELS:
        return

    # The MESSAGE_UPDATE payload only includes changed fields, so "Successfully
    # donated" may not appear if it was set in an earlier edit. Fetch the full
    # message from the API to get the complete current state.
    channel = bot.get_channel(payload.channel_id)
    if not channel:
        return
    try:
        message = await channel.fetch_message(payload.message_id)
    except discord.NotFound:
        return

    if message.author.id != DONATION_BOT_ID:
        return

    print(f"[DEBUG raw_edit] fetched msg={message.id} content={message.content!r} embeds={len(message.embeds)} components={len(message.components)}")

    def _extract_component_text(components: list) -> str:
        """Recursively extract text from all component types."""
        out = ""
        for c in components:
            # type 10 = Text Display, content field holds the text
            out += " " + (c.get("content") or "")
            # type 9 = Section, has a description and optional components
            out += " " + (c.get("description") or "")
            # recurse into nested components (Container=17, Section=9, ActionRow=1, etc.)
            out += _extract_component_text(c.get("components") or [])
            # some types nest under "accessory"
            if c.get("accessory"):
                out += _extract_component_text([c["accessory"]])
        return out

    # Fetch the raw message dict so we can read Components v2 fields
    # (Text Display type=10) which don't appear in message.embeds or content
    try:
        raw = await bot.http.get_message(channel.id, message.id)
    except Exception as e:
        print(f"[Donation] Failed to fetch raw message: {e}")
        return

    # Build searchable text from every possible surface
    text = (raw.get("content") or "")
    for e in raw.get("embeds") or []:
        text += " " + (e.get("title") or "")
        text += " " + (e.get("description") or "")
        for f in e.get("fields") or []:
            text += " " + (f.get("name") or "")
            text += " " + (f.get("value") or "")
        text += " " + ((e.get("footer") or {}).get("text") or "")
        text += " " + ((e.get("author") or {}).get("name") or "")
    text += _extract_component_text(raw.get("components") or [])

    print(f"[DEBUG raw_edit] full text={text!r}")

    if "Successfully donated" not in text:
        return

    match = re.search(r"Successfully donated.*?([\d,]+)", text)
    if not match:
        print("[Donation] Regex found no amount — skipping")
        return

    donated = int(match.group(1).replace(",", ""))
    chips   = donated // CHIPS_PER_COIN
    print(f"[Donation] Parsed: donated={donated}, chips={chips}")

    if chips <= 0:
        await channel.send(
            f"⚠️ Donation of ⏣{donated:,} is less than the minimum "
            f"⏣{CHIPS_PER_COIN:,} needed for 1 chip."
        )
        return

    # Get the donor from interaction_metadata on the fetched message
    meta = getattr(message, "interaction_metadata", None)
    user = getattr(meta, "user", None)
    if user is None:
        print(f"[Donation] No interaction_metadata on fetched message — skipping")
        return

    # Resolve to guild Member so mention works correctly
    if message.guild:
        user = message.guild.get_member(user.id) or user

    new_bal = await db.add_chips(
        bot.user.id,
        bot.user.display_name,
        user.id,
        user.name,
        chips,
        "Dank Memer Donation Exchange",
    )

    await db.log_currency_event(user.id, "Cash In", chips, "Dank Memer Donation")

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
