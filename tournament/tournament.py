import time
import discord
from discord.ext import commands
from discord.ext import tasks
import datetime
from discord import app_commands
import asyncio
import traceback

import config
from poker import database as db
from . import tournament_db as tdb
from poker.poker import TableState, tables, refresh, slog, slog_clear, schedule_next_hand, Street, parse_chips, RawSQLPaginationView, GameView, get_tourney_lock
from poker.leaderboard_image import generate_leaderboard_image

# -- Tournament Join ----------------------------------------------------------
# Tournament joins always take the player's ENTIRE wallet (no modal, no max).

async def tourney_join_execute(interaction: discord.Interaction, t: TableState, amt: int):
    """Seat the player with `amt` chips. Caller must already have deferred (ephemeral)."""
    uid = interaction.user.id
    success = await tdb.deduct_chips(uid, amt)
    if not success:
        await interaction.followup.send(f"Failed to deduct {config.TOURNAMENT_CHIP_EMOJI}. Try again.", ephemeral=True)
        return

    await tdb.refresh_player_name(uid, interaction.user.name)

    name = interaction.user.name
    msg = t.game.add_player(uid, name, amt)
    if not msg.startswith("✅"):
        await tdb.return_chips(uid, amt)
        await interaction.followup.send(msg, ephemeral=True)
        return

    await tdb.mark_chips_in_play(uid, name, amt)

    await interaction.channel.send(
        f"**✅ {interaction.user.display_name}** joined the tournament table with **{amt}** {config.TOURNAMENT_CHIP_EMOJI}!")
    await interaction.followup.send("✅ Successfully joined!", ephemeral=True)

    if t.game.street == Street.WAITING:
        await refresh(interaction.channel, t, cosmetics_cache=t.cosmetics_cache)


# -- Tournament Game View -----------------------------------------------------

class TournamentGameView(GameView):
    @discord.ui.button(label="Wallet", style=discord.ButtonStyle.grey, row=2)
    async def btn_wallet(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        uid = interaction.user.id
        stats = await tdb.get_player_stats(uid)
        if not stats:
            await interaction.followup.send("Not registered for tournament.", ephemeral=True)
            return
        bal = stats['balance']
        p = self.t.game.get_player(uid)
        table_str = f"  |  At table: {p.chips} {config.TOURNAMENT_CHIP_EMOJI}" if p else ""
        await interaction.followup.send(
            f"Tournament Wallet: {bal} {config.TOURNAMENT_CHIP_EMOJI}{table_str}", ephemeral=True)

    @discord.ui.button(label="Join", style=discord.ButtonStyle.green, row=0)
    async def btn_join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.t.closing:
            await interaction.response.send_message("This table is closing.", ephemeral=True)
            return
        uid = interaction.user.id

        # Prevent multi-tabling across the entire bot
        for other_t in tables.values():
            if any(p.user_id == uid for p in other_t.game.players + other_t.game.pending_joins):
                if other_t is not self.t: # If they are at a DIFFERENT table
                    await interaction.response.send_message("❌ You are already seated at another table! You can only play at one table at a time.", ephemeral=True)
                    return

        if any(p.user_id == uid for p in self.t.game.players) or any(pj.user_id == uid for pj in self.t.game.pending_joins):
            await interaction.response.send_message("❌ You are already at this table or waiting to join.", ephemeral=True)
            return

        if not await tdb.is_registered(uid):
            await interaction.response.send_message(
                "You are not registered for the tournament. Use /tourney register first.",
                ephemeral=True)
            return

        if uid in self.t.game.kicked_users:
            await interaction.response.send_message(
                "❌ You have been kicked and will be removed after this hand.", ephemeral=True)
            return

        # Rejoin cooldown (separate tournament lock pool, no bypass)
        expiry = get_tourney_lock(uid)
        if expiry:
            await interaction.response.send_message(
                f"⏳ You recently left the table. You can rejoin <t:{int(expiry)}:R>.",
                ephemeral=True)
            return

        min_w = getattr(self.t.game, "MIN_BUYIN", 50)
        bal   = await tdb.get_balance(uid)

        if bal < min_w:
            await interaction.response.send_message(
                f"Need at least {min_w} {config.TOURNAMENT_CHIP_EMOJI} to join. Wallet: {bal} {config.TOURNAMENT_CHIP_EMOJI}.", ephemeral=True)
            return

        # Full-stack rule: always join with the entire wallet
        await interaction.response.defer(ephemeral=True)
        await tourney_join_execute(interaction, self.t, bal)


# -- Stats View ---------------------------------------------------------------

class TournamentStatsView:
    """Plain stats embed builder (no buttons)."""
    def __init__(self, caller: discord.User | discord.Member, target: discord.User | discord.Member, stats: dict):
        self.caller = caller
        self.target = target
        self.stats = stats

    def build_personal_embed(self) -> discord.Embed:
        net = self.stats['total_chips'] - config.TOURNAMENT_STARTING_CHIPS
        embed = discord.Embed(title=f"Tournament Stats — {self.target.display_name}", color=0x2ecc71 if net >= 0 else 0xe74c3c)

        rank_str = f"#{self.stats['rank']}" if self.stats['rank'] else "Unranked"
        wp = f"{self.stats['win_rate']:.1f}%" if self.stats['hands_played'] > 0 else "—"
        vpip_str = f"{self.stats['vpip_rate']:.1f}%" if self.stats.get('hands_played', 0) > 0 else "—"

        embed.add_field(name="Rank", value=rank_str, inline=True)
        embed.add_field(name="Hands Played", value=f"{self.stats['hands_played']:,}", inline=True)
        embed.add_field(name="Win %", value=wp, inline=True)

        # Career Net, Wallet Balance, and Chips in Play
        embed.add_field(name="Net Chips", value=f"{'+' if net >= 0 else ''}{net:,} {config.TOURNAMENT_CHIP_EMOJI}", inline=True)
        embed.add_field(name="Wallet Balance", value=f"{self.stats['balance']:,} {config.TOURNAMENT_CHIP_EMOJI}", inline=True)
        embed.add_field(name="VPIP %", value=vpip_str, inline=True)
        return embed


# -- Schedule helpers ---------------------------------------------------------

_UTC = datetime.timezone.utc


def _shift_tod(t: datetime.time, minutes: int) -> datetime.time:
    """Shift a time-of-day by `minutes` (wraps past midnight) and tag it UTC."""
    dt = datetime.datetime.combine(datetime.date(2000, 1, 1), t) + datetime.timedelta(minutes=minutes)
    return dt.time().replace(tzinfo=_UTC)


# Daily wipe = first session minus the lead. Reminders = each session minus the reminder lead.
WIPE_TIME = _shift_tod(config.TOURNAMENT_SESSION_STARTS[0], -config.TOURNAMENT_WIPE_LEAD_MIN)
REMINDER_TIMES = [_shift_tod(t, -config.TOURNAMENT_REMINDER_BEFORE_SESSION_MIN)
                  for t in config.TOURNAMENT_SESSION_STARTS]


def _now() -> datetime.datetime:
    return datetime.datetime.now(_UTC)


def _next_wipe(now: datetime.datetime) -> datetime.datetime:
    cand = now.replace(hour=WIPE_TIME.hour, minute=WIPE_TIME.minute, second=0, microsecond=0)
    if cand <= now:
        cand += datetime.timedelta(days=1)
    return cand


def _ts(dt: datetime.datetime) -> int:
    return int(dt.timestamp())


_QUOTA_PCT = int(config.TOURNAMENT_ACTIVITY_QUOTA * 100)
_PENALTY_PCT = int(config.TOURNAMENT_PENALTY_PCT * 100)


# -- Cog ----------------------------------------------------------------------

class TournamentCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.tourney_daily_enforcer.start()
        self.tourney_reminders.start()

    def cog_unload(self):
        self.tourney_daily_enforcer.cancel()
        self.tourney_reminders.cancel()

    async def _try_dm(self, user_id: int, embed: discord.Embed) -> bool:
        """Safely attempts to DM a user, catching closed DMs/blocks without crashing."""
        try:
            user = await self.bot.fetch_user(user_id)
            await user.send(embed=embed)
            return True
        except (discord.Forbidden, discord.HTTPException):
            return False

    async def _apply_stack_penalty(self, uid: int, amount: int):
        """Take `amount` from the player's table stack (queued to hand end if a hand is running)."""
        if amount <= 0:
            return
        for t in tables.values():
            if not getattr(t, 'is_tournament', False):
                continue
            g = t.game
            p = g.get_player(uid)
            pj = next((x for x in g.pending_joins if x.user_id == uid), None)
            if not p and not pj:
                continue
            if not p:                                  # waiting to be seated
                pj.chips = max(0, pj.chips - amount)
                await tdb.reduce_chips_in_play(uid, amount)
            elif g.street == Street.WAITING:           # between hands: apply now
                p.chips = max(0, p.chips - amount)
                await tdb.sync_chips_in_play({uid: p.chips + p.pending_rebuy})
            else:                                      # hand running: applied in the engine's _end_hand
                g.pending_penalties[uid] = g.pending_penalties.get(uid, 0) + amount
            return
        # Not at any live table: take it from the recorded stack
        await tdb.reduce_chips_in_play(uid, amount)

    async def _execute_wipe(self, channel, dry_run: bool = False):
        """Runs the daily activity check, applies penalties, DMs and posts the summary."""
        penalties, checked = await tdb.process_daily_tourney_check(dry_run=dry_run)
        prefix = "🧪 **DRY RUN — nothing was applied.**\n" if dry_run else ""

        if not dry_run:
            for p in penalties:
                await self._apply_stack_penalty(p['user_id'], p['stack_penalty'])

        if not penalties:
            if channel:
                await channel.send(
                    f"{prefix}🕒 **Daily Wipe:** all {checked} checked top-10 players met their {_QUOTA_PCT}% quota! "
                    f"Trackers reset.")
            return

        embed = discord.Embed(
            title="🚨 Tournament Activity Penalties Applied!" if not dry_run else "🧪 Activity Penalties (dry run)",
            description=f"These top-10 players wagered less than {_QUOTA_PCT}% of their chips in the last day "
                        f"and lost {_PENALTY_PCT}% of their total chips:",
            color=0xE74C3C)
        dm_sent = 0
        for p in penalties:
            embed.add_field(
                name=p['username'],
                value=(f"**Target:** {p['target']:,}\n**Wagered:** {p['actual']:,}\n"
                       f"**Penalty:** -{p['penalty']:,} {config.TOURNAMENT_CHIP_EMOJI}"),
                inline=False)
            if dry_run:
                continue
            dm = discord.Embed(title="📉 Tournament Activity Penalty Applied", color=0xe74c3c)
            dm.description = (
                f"Hey **{p['username']}**, you didn't wager {_QUOTA_PCT}% of your chips in the last day.\n\n"
                f"**Target:** {p['target']:,}\n**Wagered:** {p['actual']:,}\n"
                f"**Penalty ({_PENALTY_PCT}% of total):** -{p['penalty']:,} {config.TOURNAMENT_CHIP_EMOJI}\n"
                f"**New total:** {p['total_after']:,} {config.TOURNAMENT_CHIP_EMOJI}")
            if await self._try_dm(p['user_id'], dm):
                dm_sent += 1
        embed.set_footer(text="The daily wager tracker has been reset for all players.")
        if channel:
            await channel.send(
                content=f"{prefix}🚨 **Activity penalties applied!** (DMs sent: {dm_sent}/{len(penalties)})",
                embed=embed)

    @tasks.loop(time=WIPE_TIME)
    async def tourney_daily_enforcer(self):
        now = _now()
        # Enforce only after the tournament starts, up to and including the end
        if not (config.TOURNAMENT_START < now <= config.TOURNAMENT_END):
            return
        channel = self.bot.get_channel(config.TOURNAMENT_REGISTER_CHANNEL_ID)
        await self._execute_wipe(channel)

    @tourney_daily_enforcer.before_loop
    async def before_enforcer(self):
        await self.bot.wait_until_ready()

    @tasks.loop(time=REMINDER_TIMES)
    async def tourney_reminders(self):
        now = _now()
        if now < config.TOURNAMENT_START - datetime.timedelta(hours=1):
            return
        next_wipe = _next_wipe(now)
        if next_wipe > config.TOURNAMENT_END:      # no more wipes -> nothing left to remind about
            return

        # Which reminder fired? The one whose time-of-day is closest to now.
        mins = now.hour * 60 + now.minute
        def _dist(t: datetime.time) -> int:
            d = abs(mins - (t.hour * 60 + t.minute))
            return min(d, 1440 - d)
        idx = min(range(len(REMINDER_TIMES)), key=lambda i: _dist(REMINDER_TIMES[i]))
        is_final = idx == len(REMINDER_TIMES) - 1 and len(REMINDER_TIMES) > 1

        players = await tdb.get_players_below_quota(next_wipe.replace(tzinfo=None))
        failed = []
        for w in players:
            embed = discord.Embed(
                title="⏰ Final call — quota check at the next wipe" if is_final else "📅 New daily window — quota target",
                color=0xf39c12 if is_final else 0x3498DB)
            embed.description = (
                f"Hey **{w['username']}**! As a top-10 player you must wager **{_QUOTA_PCT}%** of your chips "
                f"before the next wipe (<t:{_ts(next_wipe)}:R>) or lose **{_PENALTY_PCT}%** of your total chips.\n\n"
                f"**Total chips:** {w['total_chips']:,} {config.TOURNAMENT_CHIP_EMOJI}\n"
                f"**Target:** {w['target']:,}\n"
                f"**Wagered so far:** {w['wagered']:,}\n"
                f"**Still to wager:** {w['shortfall']:,}\n"
                f"**Penalty if missed:** -{w['penalty']:,} {config.TOURNAMENT_CHIP_EMOJI}"
                + ("\n\nThis is your last session before the check." if is_final else ""))
            embed.set_footer(text="Use /tourney myactivity to check your status")
            if not await self._try_dm(w['user_id'], embed):
                failed.append(w['user_id'])

        # DMs closed: make sure they still hear about the final call
        if is_final and failed:
            channel = self.bot.get_channel(config.TOURNAMENT_REGISTER_CHANNEL_ID)
            if channel:
                mentions = " ".join(f"<@{u}>" for u in failed)
                await channel.send(
                    f"⏰ **Final call:** {mentions} — you're short of your {_QUOTA_PCT}% wager quota. "
                    f"Check `/tourney myactivity` before <t:{_ts(next_wipe)}:R>.")

    @tourney_reminders.before_loop
    async def before_reminders(self):
        await self.bot.wait_until_ready()

    tourney      = app_commands.Group(name="tourney",      description="Tournament player commands", guild_ids=[config.GUILD_ID])
    tourneymgr   = app_commands.Group(name="tourneymgr",   description="Tournament manager commands", guild_ids=[config.GUILD_ID])

    async def is_manager(self, interaction: discord.Interaction) -> bool:
        settings = await db.get_settings(interaction.guild_id)
        role_id  = settings.get("manager_role_id")
        if role_id:
            role = interaction.guild.get_role(int(role_id))
            if role and role in interaction.user.roles:
                return True
        return interaction.user.guild_permissions.administrator

    async def is_admin(self, interaction: discord.Interaction) -> bool:
        return interaction.user.guild_permissions.administrator

    # -- Manager: Table -------------------------------------------------------

    @tourney.command(name="open", description="[Manager] Open a new tournament table")
    @app_commands.describe(name="Table name")
    async def open_table(self, interaction: discord.Interaction, name: str):
        # 1. Defer the response first
        await interaction.response.defer()

        # 2. Use followup.send for everything after a defer
        if not await self.is_manager(interaction):
            await interaction.followup.send("Managers only.", ephemeral=True);
            return

        key = (interaction.guild_id, interaction.channel_id)
        if key in tables:
            await interaction.followup.send("A table is already open in this channel.", ephemeral=True);
            return

        t = TableState(name, interaction.user.id, interaction.user.name)
        t.is_tournament = True

        # 3. Deep-Stack Tournament Limits
        small_blind, big_blind = await tdb.get_blinds()
        t.game.SMALL_BLIND = small_blind
        t.game.BIG_BLIND = big_blind
        t.game.MIN_BUYIN = 250
        t.game.MAX_PLAYERS = 9
        t.game.tax_rate = 0
        t.game.tax_exempt = True

        # Load and apply guild settings
        settings = await db.get_settings(interaction.guild_id)
        from poker.poker import TABLE_RESEND_MSGS
        t.resend_threshold = settings.get("resend_after_msgs", TABLE_RESEND_MSGS)
        tables[key] = t

        # 4. Use followup.send for the final confirmation!
        await interaction.followup.send(
            f"Tournament Table Open!\nBlind: {t.game.SMALL_BLIND}/{t.game.BIG_BLIND} | Min: {t.game.MIN_BUYIN} | Full-stack buy-in | {t.game.MAX_PLAYERS}-max")
        await refresh(interaction.channel, t)

    # -- Aliases for /poker start and /poker close ----------------------------

    async def _run_poker_cmd(self, name: str, interaction: discord.Interaction):
        poker_cog = self.bot.get_cog("PokerCog")
        cmd = poker_cog.poker.get_command(name) if poker_cog else None
        if not cmd:
            await interaction.response.send_message("❌ Poker cog isn't loaded.", ephemeral=True)
            return
        await cmd.callback(poker_cog, interaction)

    @tourney.command(name="start", description="[Manager] Deal the first hand")
    async def mgr_start(self, interaction: discord.Interaction):
        await self._run_poker_cmd("start", interaction)

    @tourney.command(name="close", description="[Manager] Close table after current hand")
    async def mgr_close(self, interaction: discord.Interaction):
        await self._run_poker_cmd("close", interaction)

    @tourneymgr.command(name="blinds", description="[Manager] Set the tournament blinds (saved; applies from the next hand)")
    @app_commands.describe(small_blind="New small blind", big_blind="New big blind")
    async def set_blinds(self, interaction: discord.Interaction, small_blind: int, big_blind: int):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True); return

        if small_blind <= 0 or big_blind <= small_blind:
            await interaction.response.send_message("❌ Blinds must satisfy 0 < small < big.", ephemeral=True); return

        await tdb.set_blinds(small_blind, big_blind)

        applied_now = queued = 0
        for key, t in list(tables.items()):
            if not getattr(t, 'is_tournament', False):
                continue
            g = t.game
            if g.street == Street.WAITING:
                g.SMALL_BLIND, g.BIG_BLIND = small_blind, big_blind
                g.pending_blinds = None
                applied_now += 1
            else:
                # Hand in progress: min-raise/blind math reads BIG_BLIND mid-hand, so queue it
                g.pending_blinds = (small_blind, big_blind)
                queued += 1

        await interaction.response.send_message(
            f"📈 Tournament blinds set to **{small_blind}/{big_blind}** (saved for future tables). "
            f"Applied now: {applied_now} table(s), after current hand: {queued}.")

    @tourneymgr.command(name="forcefold", description="Force fold a player")
    async def forcefold(self, interaction: discord.Interaction, user: discord.Member):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True); return

        key = (interaction.guild_id, interaction.channel_id)
        t   = tables.get(key)
        if not t or not t.is_tournament:
            await interaction.response.send_message("No tournament table here.", ephemeral=True); return

        ok, msg = t.game.force_fold(user.id)
        if ok:
            await interaction.response.send_message(f"{user.display_name} was force-folded.")
            parts = msg.split("\n")
            if any(m in msg for m in ["Showdown", "wins", "folded"]):
                slog_clear(t)
            for part in parts:
                if part.strip():
                    slog(t, part)
            from poker import _process_result
            if t.game._hand_result:
                await _process_result(interaction.guild, interaction.channel, t)
            else:
                await refresh(interaction.channel, t, cosmetics_cache=t.cosmetics_cache)
        else:
            await interaction.response.send_message(f"Error: {msg}", ephemeral=True)

    @tourneymgr.command(name="kick", description="Kick a player from the table")
    async def kick(self, interaction: discord.Interaction, user: discord.Member):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True); return

        key = (interaction.guild_id, interaction.channel_id)
        t   = tables.get(key)
        if not t or not t.is_tournament:
            await interaction.response.send_message("No tournament table here.", ephemeral=True); return

        p = t.game.get_player(user.id)
        pj = next((x for x in t.game.pending_joins if x.user_id == user.id), None)

        if not p and not pj:
            await interaction.response.send_message(f"❌ **{user.display_name}** is not at the table.", ephemeral=True)
            return

        # Kick from waiting list
        if pj:
            t.game.pending_joins.remove(pj)
            total_to_return = pj.chips + pj.pending_rebuy
            if total_to_return > 0:
                await tdb.return_chips(user.id, total_to_return)
            await tdb.clear_chips_in_play(user.id)
            await interaction.response.send_message(f"🦵 **{user.display_name}** has been kicked from the waiting list.")
            return

        # Kick from table instantly if waiting
        if t.game.street == Street.WAITING and not t.game._hand_result:
            t.game.players.remove(p)
            total_to_return = p.chips + p.pending_rebuy
            if total_to_return > 0:
                await tdb.return_chips(user.id, total_to_return)
            await tdb.clear_chips_in_play(user.id)
            await interaction.response.send_message(f"🦵 **{user.display_name}** has been kicked and removed from the table.")
            await refresh(interaction.channel, t)
            return

        if user.id not in t.game.kicked_users:
            t.game.kicked_users.append(user.id)
        if user.id not in t.game.pending_leaves:
            t.game.pending_leaves.append(user.id)

        if not p.folded:
            ok, fold_msg = t.game.force_fold(user.id)
            if ok:
                parts = fold_msg.split("\n")
                if any(m in fold_msg for m in ["Showdown", "wins", "folded"]):
                    slog_clear(t)
                for part in parts:
                    if part.strip():
                        slog(t, part)

        await interaction.response.send_message(
            f"🦵 **{user.display_name}** has been kicked — force folded and will be removed after this hand.")

        from poker.poker import _process_result
        if t.game._hand_result:
            await _process_result(interaction.guild, interaction.channel, t)
        else:
            await refresh(interaction.channel, t, cosmetics_cache=t.cosmetics_cache)

    @tourneymgr.command(name="addchips", description="Give tournament chips to a player")
    async def addchips(self, interaction: discord.Interaction, user: discord.Member, amount: int):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True); return

        if config.ADD_CHIPS_CHANNELS and interaction.channel_id not in config.ADD_CHIPS_CHANNELS:
            mentions = ", ".join(f"<#{cid}>" for cid in config.ADD_CHIPS_CHANNELS)
            await interaction.response.send_message(f"❌ This command is restricted to: {mentions}", ephemeral=True)
            return

        if amount <= 0:
            await interaction.response.send_message("❌ Amount must be positive.", ephemeral=True)
            return

        if not await tdb.is_registered(user.id):
            await interaction.response.send_message("Player not registered.", ephemeral=True); return

        new_bal = await tdb.add_chips(user.id, user.name, amount)
        await interaction.response.send_message(
            f"Added {amount} {config.TOURNAMENT_CHIP_EMOJI} to {user.display_name}. New balance: {new_bal} {config.TOURNAMENT_CHIP_EMOJI}")

    @tourneymgr.command(name="removechips", description="Remove tournament chips from a player")
    async def removechips(self, interaction: discord.Interaction, user: discord.Member, amount: int):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True); return

        if config.REMOVE_CHIPS_CHANNELS and interaction.channel_id not in config.REMOVE_CHIPS_CHANNELS:
            mentions = ", ".join(f"<#{cid}>" for cid in config.REMOVE_CHIPS_CHANNELS)
            await interaction.response.send_message(f"❌ This command is restricted to: {mentions}", ephemeral=True)
            return

        if amount <= 0:
            await interaction.response.send_message("❌ Amount must be positive.", ephemeral=True)
            return

        success = await tdb.deduct_chips(user.id, amount)
        if success:
            new_bal = await tdb.get_balance(user.id)
            await interaction.response.send_message(
                f"Removed {amount} {config.TOURNAMENT_CHIP_EMOJI} from {user.display_name}. New balance: {new_bal} {config.TOURNAMENT_CHIP_EMOJI}")
        else:
            await interaction.response.send_message(f"Failed - player may not have enough {config.TOURNAMENT_CHIP_EMOJI}.", ephemeral=True)

    # -- Player ---------------------------------------------------------------

    @tourney.command(name="register", description="Register for the tournament")
    async def register(self, interaction: discord.Interaction):

        #await interaction.response.send_message("Registration is closed now.", ephemeral=True)
        #return

        if interaction.channel_id != config.TOURNAMENT_REGISTER_CHANNEL_ID:
            await interaction.response.send_message(
                f"❌ You can only register for the tournament in <#{config.TOURNAMENT_REGISTER_CHANNEL_ID}>.",
                ephemeral=True
            )
            return

        now = _now()
        if now < config.TOURNAMENT_REGISTRATION_OPEN:
            await interaction.response.send_message(
                f"⏳ Registration opens <t:{_ts(config.TOURNAMENT_REGISTRATION_OPEN)}:F> (<t:{_ts(config.TOURNAMENT_REGISTRATION_OPEN)}:R>).",
                ephemeral=True)
            return
        if now > config.TOURNAMENT_REGISTRATION_CLOSE:
            await interaction.response.send_message(
                f"🔒 Registration closed <t:{_ts(config.TOURNAMENT_REGISTRATION_CLOSE)}:R>.", ephemeral=True)
            return

        success = await tdb.register_player(interaction.user.id, interaction.user.name)
        if success:
            await interaction.response.send_message(
                f"You are registered! Starting balance: {config.TOURNAMENT_STARTING_CHIPS} {config.TOURNAMENT_CHIP_EMOJI}.")
        else:
            await interaction.response.send_message("You are already registered.", ephemeral=True)

    @tourney.command(name="wallet", description="View your tournament wallet balance")
    @app_commands.describe(user="Player to check (leave blank for yourself)")
    async def wallet(self, interaction: discord.Interaction, user: discord.Member = None):
        await interaction.response.defer(ephemeral=False)
        target = user or interaction.user
        stats = await tdb.get_player_stats(target.id)

        if not stats:
            await interaction.followup.send("❌ Player not registered for the tournament. Use `/tourney register` first.", ephemeral=True)
            return

        bal = stats['balance']
        in_play = stats['chips_in_play']
        table_str = f"\n**At table:** {in_play:,} {config.TOURNAMENT_CHIP_EMOJI}" if in_play > 0 else ""
        label = f"**{target.display_name}'s Tournament Wallet**" if user else "**Your Tournament Wallet**"
        await interaction.followup.send(f"{label}: {bal:,} {config.TOURNAMENT_CHIP_EMOJI}{table_str}", ephemeral=False)

    @tourney.command(name="stats", description="View tournament stats")
    @app_commands.describe(
        user="Player to view (leave blank for yourself)",
        hidden="Hide the stats message from others? (Default: False)"
    )
    async def stats(self, interaction: discord.Interaction, user: discord.Member = None, hidden: bool = False):
        await interaction.response.defer(ephemeral=hidden)
        target = user or interaction.user
        await tdb.refresh_player_name(interaction.user.id, interaction.user.name)
        if user:
            await tdb.refresh_player_name(user.id, user.name)
        stats = await tdb.get_player_stats(target.id)
        if not stats:
            await interaction.followup.send("❌ Player not registered. Use `/tourney register` first.", ephemeral=True)
            return

        view = TournamentStatsView(interaction.user, target, stats)
        await interaction.followup.send(embed=view.build_personal_embed(), ephemeral=hidden)

    @tourney.command(name="leaderboard", description="View the tournament leaderboard")
    async def leaderboard(self, interaction: discord.Interaction):
        await interaction.response.defer()
        rows = await tdb.get_individual_leaderboard(10)
        if not rows:
            await interaction.followup.send("No players yet!", ephemeral=True)
            return

        caller_id = interaction.user.id
        caller_row = await tdb.get_player_stats(caller_id)
        caller_rank = await tdb.get_player_rank(caller_id) if caller_row else None
        image_bytes = await asyncio.to_thread(
            generate_leaderboard_image,
            rows,
            caller_id,
            caller_row,
            caller_rank,
            title="Tournament Leaderboard",
        )

        file = discord.File(image_bytes, filename="tournament_leaderboard.png")

        container = discord.ui.Container(
            discord.ui.MediaGallery(
                discord.MediaGalleryItem("attachment://tournament_leaderboard.png")
            ),
            accent_color=0xF1C40F,
        )
        view = discord.ui.LayoutView()
        view.add_item(container)

        await interaction.followup.send(file=file, view=view)

    @tourney.command(name="myactivity", description="Check your daily wager quota and activity")
    async def myactivity(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        stats = await tdb.get_player_stats(interaction.user.id)

        if not stats:
            await interaction.followup.send("❌ You are not registered for the tournament.", ephemeral=True)
            return

        total_chips = stats['total_chips']

        db = await tdb._get_db()
        async with db.execute(
                "SELECT period_wagered, last_activity, target_wager, registered_at FROM players WHERE user_id=?",
                (interaction.user.id,)) as c:
            row = await c.fetchone()
            wagered = row[0] if row and row[0] else 0
            last_act = row[1] if row else None
            target = row[2] if row and row[2] is not None else int(total_chips * config.TOURNAMENT_ACTIVITY_QUOTA)
            reg_str = row[3] if row else None

        shortfall = max(0, target - wagered)
        percent_complete = min(100.0, (wagered / target * 100)) if target > 0 else 100.0
        penalty = int(total_chips * config.TOURNAMENT_PENALTY_PCT)

        now = _now()
        next_wipe = _next_wipe(now)
        start, end = config.TOURNAMENT_START, config.TOURNAMENT_END
        top_ids = await tdb.get_top_ids()
        in_top = interaction.user.id in top_ids
        in_grace = tdb._in_grace(reg_str, next_wipe.replace(tzinfo=None))

        embed = discord.Embed(title="📊 Your Daily Activity Tracker", color=0x3498DB)
        embed.add_field(name="Current Total Chips", value=f"{total_chips:,} {config.TOURNAMENT_CHIP_EMOJI}", inline=True)
        embed.add_field(name=f"Target ({_QUOTA_PCT}%)", value=f"{target:,}", inline=True)
        embed.add_field(name="Wagered So Far", value=f"{wagered:,}", inline=True)

        if last_act:
            dt = datetime.datetime.fromisoformat(last_act).replace(tzinfo=_UTC)
            embed.add_field(name="Last Active", value=f"<t:{_ts(dt)}:R>", inline=False)
        else:
            embed.add_field(name="Last Active", value="Never", inline=False)

        if now <= start:
            first_wipe = _next_wipe(start)
            embed.add_field(name="First wipe", value=f"<t:{_ts(first_wipe)}:R>", inline=False)
            embed.description = (f"⏳ **The tournament starts <t:{_ts(start)}:F>!**\n"
                                 f"Quotas and penalties are enforced from the first wipe after the start.")
        elif next_wipe > end:
            embed.description = "🏁 **The final wipe has passed.** No more activity checks."
        else:
            embed.add_field(name="Next wipe", value=f"<t:{_ts(next_wipe)}:R>", inline=False)
            if not in_top:
                status = (f"ℹ️ You're outside the top 10, so no penalty applies right now. "
                          f"If you enter it you'll be held to this quota.")
            elif in_grace:
                status = "🛡️ **Grace period!** You're skipped by the next wipe."
            elif shortfall == 0:
                status = f"✅ **You have met your {_QUOTA_PCT}% quota!** You're safe at the next wipe."
            else:
                status = (f"⚠️ **You are short!** Wager **{shortfall:,}** more chips before the wipe or lose "
                          f"**{penalty:,}** ({_PENALTY_PCT}% of your total).")
            filled = int(percent_complete / 10)
            bar = ("🟩" * filled) + ("⬛" * (10 - filled))
            embed.description = f"{status}\n\n**Progress:** {percent_complete:.1f}%\n{bar}"

        await interaction.followup.send(embed=embed, ephemeral=True)

    @tourneymgr.command(name="force_daily_check", description="[Manager] Manually run the daily activity wipe")
    @app_commands.describe(dry_run="Only report who would be penalized; change nothing (default: True)")
    async def force_daily_check(self, interaction: discord.Interaction, dry_run: bool = True):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True)
            return

        await interaction.response.defer()

        channel = self.bot.get_channel(config.TOURNAMENT_REGISTER_CHANNEL_ID)
        if not channel:
            await interaction.followup.send("❌ Error: Cannot find the tournament registration channel.")
            return

        await self._execute_wipe(channel, dry_run=dry_run)
        await interaction.followup.send(
            "✅ Dry run complete. Check the tournament channel for output." if dry_run
            else "✅ Forced the daily wipe. Check the tournament channel for output.")

    @tourneymgr.command(name="sql", description="[Dev] Run a read-only tournament database query")
    @app_commands.describe(query="The SELECT query to run")
    async def run_tourney_sql(self, interaction: discord.Interaction, query: str):
        # 1. Ironclad Security Check
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True);
            return

        # 2. String Check
        if not query.strip().upper().startswith("SELECT"):
            await interaction.response.send_message("❌ Only SELECT queries are allowed.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)

        try:
            import aiosqlite
            # Force SQLite into strict Read-Only mode
            db_uri = f"file:{config.TOURNAMENT_DB_PATH}?mode=ro"

            async with aiosqlite.connect(db_uri, uri=True) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(query) as cursor:
                    rows = await cursor.fetchall()

                    if not rows:
                        await interaction.followup.send("✅ Query executed successfully. No rows returned.",
                                                        ephemeral=False)
                        return

                    columns = list(rows[0].keys())

                    # Pass to Paginator (15 rows per page, Max 20 pages)
                    view = RawSQLPaginationView(columns=columns, rows=rows, title="🏆 Tournament DB Query",
                                                items_per_page=15, max_pages_limit=20)
                    await interaction.followup.send(embed=view.format_page(), view=view, ephemeral=False)

        except Exception as e:
            traceback.print_exc()
            await interaction.followup.send(f"❌ **SQL Error:**\n`{e}`", ephemeral=False)

    @tourneymgr.command(name="set_activity",
                        description="[Manager] Change a tournament player's last_activity timestamp")
    @app_commands.describe(
        user="The player to modify",
        timestamp="Discord timestamp (e.g. <t:1716388800:R>) or raw Unix epoch"
    )
    async def set_activity(self, interaction: discord.Interaction, user: discord.Member, timestamp: str):
        await interaction.response.defer(ephemeral=True)

        if not await self.is_manager(interaction):
            await interaction.followup.send("Managers only.", ephemeral=True)
            return

        import re
        from datetime import datetime

        # 1. Parse the Discord timestamp format (<t:1234567890> or <t:1234567890:R>)
        # Or accept a raw unix integer if you just type the numbers manually.
        match = re.search(r"<t:(\d+)", timestamp)
        if match:
            unix_ts = int(match.group(1))
        elif timestamp.isdigit():
            unix_ts = int(timestamp)
        else:
            await interaction.followup.send(
                "❌ Invalid format. Please use a Discord timestamp like `<t:1716388800>` or `<t:1716388800:R>`.",
                ephemeral=True
            )
            return

        # 2. Convert Unix Epoch -> UTC Datetime -> ISO 8601 string
        try:
            new_iso = datetime.utcfromtimestamp(unix_ts).isoformat()
        except (ValueError, OSError, OverflowError) as e:
            await interaction.followup.send(f"❌ Failed to parse date: {e}", ephemeral=True)
            return

        # 3. Update the tournament database directly
        db_conn = await tdb._get_db()
        try:
            async with tdb._write_lock:
                await db_conn.execute("UPDATE players SET last_activity = ? WHERE user_id = ?", (new_iso, user.id))
                await db_conn.commit()

            await interaction.followup.send(
                f"✅ Successfully backdated **{user.display_name}**'s tournament last activity to <t:{unix_ts}:F>!\n"
                f"*(Database saved exactly as:* `{new_iso}`*)*",
                ephemeral=True
            )
        except Exception as e:
            traceback.print_exc()
            await interaction.followup.send(f"❌ Database error: {e}", ephemeral=True)

async def setup(bot):
    await bot.add_cog(TournamentCog(bot))