import time
import discord
from discord.ext import commands
from discord.ext import tasks
import datetime
from discord import app_commands

import config
import database as db
import tournament_db as tdb
from poker import TableState, tables, refresh, slog, slog_clear, schedule_next_hand, Street, parse_chips
from engine import Street


# -- Tournament Join Modal ----------------------------------------------------

class TournamentJoinModal(discord.ui.Modal, title="Tournament Buy In"):
    amount = discord.ui.TextInput(label="Buy-in Amount", placeholder="e.g. 500", required=True)

    def __init__(self, t: TableState, balance: int, min_w: int, max_w: int):
        super().__init__()
        self.t = t
        self.balance = balance
        self.min_w = min_w
        self.max_w = max_w
        limit_str = f"{max_w}" if max_w > 0 else "None"
        self.amount.placeholder = f"min {min_w} - max {limit_str}  (wallet: {balance})"

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        # Prevent multi-tabling across the entire bot
        uid = interaction.user.id
        for other_t in tables.values():
            if any(p.user_id == uid for p in other_t.game.players + other_t.game.pending_joins):
                if other_t is not self.t: # If they are at a DIFFERENT table
                    await interaction.followup.send("❌ You are already seated at another table! You can only play at one table at a time.", ephemeral=True)
                    return

        # Double check cooldown
        expiry = self.t.rejoin_cooldowns.get(interaction.user.id)
        if expiry and time.time() < expiry:
            await interaction.followup.send("❌ You are currently on a rejoin cooldown.", ephemeral=True)
            return

        amt = parse_chips(self.amount.value)
        if amt is None or amt <= 0:
            await interaction.followup.send("❌ Enter a valid positive amount (e.g. 500, 2k).", ephemeral=True)
            return

        if amt < self.min_w:
            await interaction.followup.send(f"Minimum buy-in is {self.min_w} {config.TOURNAMENT_CHIP_EMOJI}.", ephemeral=True)
            return
        if self.max_w > 0 and amt > self.max_w:
            await interaction.followup.send(f"Maximum buy-in is {self.max_w} {config.TOURNAMENT_CHIP_EMOJI}.", ephemeral=True)
            return
        if amt > self.balance:
            await interaction.followup.send(f"You only have {self.balance} {config.TOURNAMENT_CHIP_EMOJI}.", ephemeral=True)
            return

        uid = interaction.user.id
        success = await tdb.deduct_chips(uid, amt)
        if not success:
            await interaction.followup.send(f"Failed to deduct {config.TOURNAMENT_CHIP_EMOJI}. Try again.", ephemeral=True)
            return

        name = interaction.user.display_name
        msg = self.t.game.add_player(uid, name, amt)
        if not msg.startswith("✅"):
            await tdb.return_chips(uid, amt)
            await interaction.followup.send(msg, ephemeral=True)
            return

        await tdb.mark_chips_in_play(uid, name, amt)

        # Team tags are removed from cosmetics as requested

        await interaction.channel.send(
            f"**✅ {interaction.user.display_name}** joined the tournament table with **{amt}** {config.TOURNAMENT_CHIP_EMOJI}!")
        await interaction.followup.send("✅ Successfully joined!", ephemeral=True)

        if self.t.game.street == Street.WAITING:
            await refresh(interaction.channel, self.t, cosmetics_cache=self.t.cosmetics_cache)


# -- Tournament Rebuy Modal ---------------------------------------------------

class TournamentRebuyModal(discord.ui.Modal, title="Add Tournament Chips"):
    amount = discord.ui.TextInput(label="Amount to Add", placeholder="e.g. 500", required=True)

    def __init__(self, t: TableState, balance: int, max_w: int, current_stack: int):
        super().__init__()
        self.t = t
        self.balance = balance
        self.max_w = max_w
        self.current_stack = current_stack

        if max_w > 0:
            allowed = max(0, max_w - current_stack)
            self.amount.placeholder = f"Max: {min(balance, allowed)}  (wallet: {balance})"
        else:
            self.amount.placeholder = f"Max: {balance}  (wallet: {balance})"

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        amt = parse_chips(self.amount.value)
        if amt is None or amt <= 0:
            await interaction.followup.send("❌ Enter a valid positive amount (e.g. 500, 2k).", ephemeral=True)
            return

        # Calculate live stack size right now
        p = self.t.game.get_player(interaction.user.id)
        pj = next((x for x in self.t.game.pending_joins if x.user_id == interaction.user.id), None)
        live_stack = (p.chips + p.pending_rebuy) if p else ((pj.chips + pj.pending_rebuy) if pj else 0)

        if self.max_w > 0 and (live_stack + amt) > self.max_w:
            await interaction.followup.send(
                f"Table max is {self.max_w} {config.TOURNAMENT_CHIP_EMOJI}. You can add up to {max(0, self.max_w - live_stack)} {config.TOURNAMENT_CHIP_EMOJI}.",
                ephemeral=True)
            return

        if amt > self.balance:
            await interaction.followup.send(f"You only have {self.balance} {config.TOURNAMENT_CHIP_EMOJI}.", ephemeral=True)
            return

        uid = interaction.user.id
        success = await tdb.deduct_chips(uid, amt)
        if not success:
            await interaction.followup.send(f"Failed to deduct {config.TOURNAMENT_CHIP_EMOJI}.", ephemeral=True)
            return

        msg = self.t.game.queue_rebuy(uid, amt, emoji=config.TOURNAMENT_CHIP_EMOJI)
        if msg.startswith("❌"):
            await tdb.return_chips(uid, amt)
            await interaction.followup.send(msg, ephemeral=True)
            return

        await tdb.mark_chips_in_play(uid, interaction.user.display_name, amt)

        await interaction.channel.send(
            f"**{interaction.user.display_name}** added **{amt}** {config.TOURNAMENT_CHIP_EMOJI} to their tournament stack for the next hand!")
        await interaction.followup.send(f"{config.TOURNAMENT_CHIP_EMOJI} queued for the next hand!", ephemeral=True)


# -- Tournament Game View -----------------------------------------------------

from poker import GameView

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

        expiry = self.t.rejoin_cooldowns.get(uid)
        if expiry and time.time() < expiry:
            await interaction.response.send_message(
                f"You recently left this table. You can rejoin <t:{int(expiry)}:R>.",
                ephemeral=True)
            return
        self.t.rejoin_cooldowns.pop(uid, None)

        min_w = getattr(self.t.game, "MIN_BUYIN", 50)
        max_w = getattr(self.t.game, "MAX_BUYIN", 0)
        bal   = await tdb.get_balance(uid)

        if bal < min_w:
            await interaction.response.send_message(
                f"Need at least {min_w} {config.TOURNAMENT_CHIP_EMOJI} to join. Wallet: {bal} {config.TOURNAMENT_CHIP_EMOJI}.", ephemeral=True)
            return

        await interaction.response.send_modal(TournamentJoinModal(self.t, bal, min_w, max_w))


# -- Between-Hands View -------------------------------------------------------

class TournamentBetweenHandsView(discord.ui.View):
    def __init__(self, t: TableState):
        super().__init__(timeout=None)
        self.t = t

    @discord.ui.button(label="Add Chips", style=discord.ButtonStyle.green)
    async def add_chips(self, interaction: discord.Interaction, button: discord.ui.Button):
        t = self.t
        if t.closing:
            await interaction.response.send_message("This table is closing.", ephemeral=True)
            return

        p  = t.game.get_player(interaction.user.id)
        pj = next((x for x in t.game.pending_joins if x.user_id == interaction.user.id), None)
        if not p and not pj:
            await interaction.response.send_message("You are not at the table.", ephemeral=True)
            return

        current_stack = (p.chips + p.pending_rebuy) if p else (pj.chips + pj.pending_rebuy)

        await interaction.response.defer(ephemeral=True)
        bal   = await tdb.get_balance(interaction.user.id)
        max_w = getattr(self.t.game, "MAX_BUYIN", 0)

        await interaction.followup.send(
            "Add tournament chips to your stack:",
            view=TournamentRebuyView(self.t, bal, max_w, current_stack),
            ephemeral=True)


class TournamentRebuyView(discord.ui.View):
    def __init__(self, t, bal, max_w, current_stack):
        super().__init__(timeout=60)
        self.t = t
        self.bal = bal
        self.max_w = max_w
        self.current_stack = current_stack

    @discord.ui.button(label="Open Rebuy Menu", style=discord.ButtonStyle.green)
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            TournamentRebuyModal(self.t, self.bal, self.max_w, self.current_stack))


# -- Leaderboard View ---------------------------------------------------------

class TournamentLeaderboardView(discord.ui.View):
    def __init__(self, caller_id: int, caller_row: dict | None, caller_rank: int | None, indiv: list[dict], teams: list[dict]):
        super().__init__(timeout=120)
        self.caller_id = caller_id
        self.caller_row = caller_row
        self.caller_rank = caller_rank
        self.indiv = indiv
        self.teams = teams
        self.show_teams = False

    def get_embed(self) -> discord.Embed:
        embed = discord.Embed(title="🏆 Tournament Leaderboard", color=0xF1C40F)
        if not self.show_teams:
            MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
            top_ids = {r['user_id'] for r in self.indiv}

            lines = []
            for i, r in enumerate(self.indiv):
                rank = i + 1
                total = r['total_chips']
                uname = r['username']  # No need to truncate names anymore!
                medal = MEDALS.get(rank, f"{rank}.")
                you_tag = " ◀" if r['user_id'] == self.caller_id else ""

                lines.append(f"{medal} **{uname}** - {total:,} {config.TOURNAMENT_CHIP_EMOJI}{you_tag}")

            embed.description = "\n".join(lines) if lines else "No players."

            # Caller's stats - shown at the bottom whether or not they're in the top 10
            if self.caller_row:
                in_top = self.caller_id in top_ids
                rank_str = f"#{self.caller_rank}" if self.caller_rank else "—"
                label = f"📊 Your Tournament Stats  ·  {rank_str}" + (" *(in top 10)*" if in_top else "")
                embed.add_field(
                    name=label,
                    value=(
                        f"Total **{self.caller_row['total_chips']:,}** {config.TOURNAMENT_CHIP_EMOJI}"
                    ),
                    inline=False
                )
            else:
                embed.add_field(name="📊 Your Tournament Stats", value="No hands played yet.", inline=False)
        else:
            text = "\n".join(
                f"{i + 1}. **{tm['name']}** - {tm['total_wins']} hands won"
                for i, tm in enumerate(self.teams)) or "No teams."
            embed.add_field(name="Team Top 10", value=text, inline=False)
        return embed

    @discord.ui.button(label="Switch to Teams", style=discord.ButtonStyle.blurple)
    async def toggle(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller_id:
            return await interaction.response.send_message("This is not your leaderboard.", ephemeral=True)
        self.show_teams = not self.show_teams
        button.label = "Switch to Individual" if self.show_teams else "Switch to Teams"
        await interaction.response.edit_message(embed=self.get_embed(), view=self)


# -- Stats View ---------------------------------------------------------------

class TournamentStatsView(discord.ui.View):
    def __init__(self, caller: discord.User | discord.Member, target: discord.User | discord.Member, stats: dict, roster=None, team_info=None):
        super().__init__(timeout=120)
        self.caller = caller
        self.target = target
        self.stats = stats
        self.roster = roster
        self.team_info = team_info
        self.show_team = False
        if not roster:
            self.toggle.disabled = True

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

        team_val = self.stats['team_name'] or "None"
        embed.add_field(name="Team", value=team_val, inline=False)
        return embed

    def build_team_embed(self) -> discord.Embed:
        embed = discord.Embed(title=f"Team Stats — {self.stats['team_name']}", color=0xE67E22)

        leader_id = self.team_info.get('leader_id') if self.team_info else None

        lines = []
        if self.roster:
            for i, p in enumerate(self.roster):
                is_leader = " 👑 *(Captain)*" if p['user_id'] == leader_id else ""
                lines.append(
                    f"{i + 1}. **{p['username']}**{is_leader} - {p['total_chips']:,} {config.TOURNAMENT_CHIP_EMOJI} ({p['hands_won']} wins)")

        text = "\n".join(lines) or "No players."
        embed.add_field(name="Player Contributions", value=text, inline=False)
        return embed

    @discord.ui.button(label="Switch to Team Stats", style=discord.ButtonStyle.blurple)
    async def toggle(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id:
            return await interaction.response.send_message("This is not your stats menu.", ephemeral=True)
        self.show_team = not self.show_team
        button.label = "Switch to My Stats" if self.show_team else "Switch to Team Stats"
        embed = self.build_team_embed() if self.show_team else self.build_personal_embed()
        await interaction.response.edit_message(embed=embed, view=self)


class TournamentTeamsView(discord.ui.View):
    def __init__(self, caller: discord.User | discord.Member, teams: list[dict]):
        super().__init__(timeout=120)
        self.caller = caller
        self.teams = teams
        self.page = 0
        self.update_buttons()

    def update_buttons(self):
        max_pages = len(self.teams)
        self.btn_first.disabled = self.page == 0
        self.btn_prev.disabled = self.page == 0
        self.btn_next.disabled = self.page >= max_pages - 1
        self.btn_last.disabled = self.page >= max_pages - 1

    async def build_embed(self) -> discord.Embed:
        team = self.teams[self.page]
        embed = discord.Embed(title=f"🛡️ Team: {team['name']}", color=0x3498DB)

        import tournament_db as tdb
        import config

        # Dynamically fetch the live roster for this specific team
        roster = await tdb.get_team_roster(team['id'])

        leader_id = team.get('leader_id')
        leader_str = f"👑 {team['leader_name']}" if team.get('leader_name') else "No Captain Set"

        embed.add_field(name="Captain", value=leader_str, inline=False)

        lines = []
        if roster:
            for i, p in enumerate(roster):
                is_leader = " 👑" if p['user_id'] == leader_id else ""
                lines.append(
                    f"{i + 1}. **{p['username']}**{is_leader} - {p['total_chips']:,} {config.TOURNAMENT_CHIP_EMOJI} ({p['hands_won']} wins)")

        text = "\n".join(lines) or "No players yet."

        # Dynamic status tag
        status = " *(Looking for players!)*" if len(roster) < 4 else " *(Full)*"
        embed.add_field(name=f"Roster ({len(roster)}/4){status}", value=text, inline=False)

        embed.set_footer(text=f"Page {self.page + 1} of {len(self.teams)}")
        return embed

    @discord.ui.button(emoji="⏪", style=discord.ButtonStyle.blurple, row=1)
    async def btn_first(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id:
            await interaction.response.send_message("❌ This is not your menu.", ephemeral=True)
            return
        self.page = 0
        self.update_buttons()
        await interaction.response.edit_message(embed=await self.build_embed(), view=self)

    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.blurple, row=1)
    async def btn_prev(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id:
            await interaction.response.send_message("❌ This is not your menu.", ephemeral=True)
            return
        self.page = max(0, self.page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=await self.build_embed(), view=self)

    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.blurple, row=1)
    async def btn_next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id:
            await interaction.response.send_message("❌ This is not your menu.", ephemeral=True)
            return
        self.page = min(len(self.teams) - 1, self.page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=await self.build_embed(), view=self)

    @discord.ui.button(emoji="⏩", style=discord.ButtonStyle.blurple, row=1)
    async def btn_last(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.caller.id:
            await interaction.response.send_message("❌ This is not your menu.", ephemeral=True)
            return
        self.page = len(self.teams) - 1
        self.update_buttons()
        await interaction.response.edit_message(embed=await self.build_embed(), view=self)


# -- Cog ----------------------------------------------------------------------

class TournamentCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.tourney_daily_enforcer.start()

    def cog_unload(self):
        self.tourney_daily_enforcer.cancel()

    async def _try_dm(self, user_id: int, embed: discord.Embed) -> bool:
        """Safely attempts to DM a user, catching closed DMs/blocks without crashing."""
        try:
            user = await self.bot.fetch_user(user_id)
            await user.send(embed=embed)
            return True
        except (discord.Forbidden, discord.HTTPException):
            return False
        except Exception as e:
            print(f"[Tournament DM] Error DMing {user_id}: {e}")
            return False

    tourney_wipe_time = datetime.time(hour=2, minute=00, tzinfo=datetime.timezone.utc)

    @tasks.loop(time=tourney_wipe_time)
    async def tourney_daily_enforcer(self):

        if datetime.datetime.utcnow() < datetime.datetime(2026, 6, 7):
            return

        channel = self.bot.get_channel(config.TOURNAMENT_REGISTER_CHANNEL_ID)
        if not channel: return

        import tournament_db as tdb
        cycle_day, warnings, penalties = await tdb.process_daily_tourney_check()

        if cycle_day == 1:
            dm_warned = 0
            if warnings:
                for w in warnings:
                    embed = discord.Embed(
                        title="⚠️ Tournament Coasting Warning — You're at risk of penalty!",
                        color=0xf39c12
                    )
                    embed.description = (
                        f"Hey **{w['username']}**! You are currently in the **Top 10** of the tournament.\n"
                        f"To prevent coasting, you must wager 25% of your stack every 2 days.\n\n"
                        f"**Your total chips:** {w['total_chips']:,} {config.TOURNAMENT_CHIP_EMOJI}\n"
                        f"**Target (25%):** {w['target']:,}\n"
                        f"**Wagered so far:** {w['wagered']:,}\n\n"
                        f"**Wager {w['shortfall']:,} more chip(s) in the next 24 hours to avoid a deduction penalty!**"
                    )
                    embed.set_footer(
                        text="Penalty runs daily at 03:30 UTC • Use /tourney myactivity to check your status")

                    # 🛠️ Uses safe DM helper and accurately tracks success
                    if await self._try_dm(w['user_id'], embed):
                        dm_warned += 1

                await channel.send(
                    f"⚠️ **24-Hour Coasting Warning:** {len(warnings)} players in the Top 10 are falling short of their 25% wager quota! (DMs sent: {dm_warned}/{len(warnings)})"
                )
        else:
            dm_sent = 0
            if not penalties:
                await channel.send(
                    "🕒 **48-Hour Tournament Checkpoint:** All Top 10 players met their 25% wager quota! The period tracker has been reset.")
            else:
                embed = discord.Embed(
                    title="🚨 Tournament Coasting Penalties Applied!",
                    description="The following Top 10 players failed to wager 25% of their stack in the last 48 hours and have been taxed their shortfall:",
                    color=0xE74C3C
                )
                for p in penalties:
                    embed.add_field(name=p['username'],
                                    value=f"**Target:** {p['target']:,}\n**Wagered:** {p['actual']:,}\n**Penalty:** -{p['shortfall']:,} {config.TOURNAMENT_CHIP_EMOJI}",
                                    inline=False)

                    dm = discord.Embed(
                        title="📉 Tournament Coasting Penalty Applied",
                        color=0xe74c3c
                    )
                    dm.description = (
                        f"Hey **{p['username']}**, you didn't meet the Top 10 activity requirements for the 48-hour period.\n\n"
                        f"**Target (25%):** {p['target']:,}\n"
                        f"**Wagered:** {p['actual']:,}\n"
                        f"**Shortfall Penalty:** -{p['shortfall']:,} {config.TOURNAMENT_CHIP_EMOJI}\n\n"
                        f"The shortfall amount has been deducted from your balance."
                    )

                    # 🛠️ Uses safe DM helper and accurately tracks success
                    if await self._try_dm(p['user_id'], dm):
                        dm_sent += 1

                embed.set_footer(text="The 48-hour wager tracker has been reset for all players.")

                # 🛠️ Standardized channel message tracking outside of the embed footer
                await channel.send(
                    content=f"🚨 **Coasting Penalties Applied!** (DMs sent: {dm_sent}/{len(penalties)})",
                    embed=embed
                )

    @tourney_daily_enforcer.before_loop
    async def before_enforcer(self):
        await self.bot.wait_until_ready()

    tourney      = app_commands.Group(name="tourney",      description="Tournament player commands")
    tourneymgr   = app_commands.Group(name="tourneymgr",   description="Tournament manager commands")

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

    @tourneymgr.command(name="open", description="[Manager] Open a new tournament table")
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

        t = TableState(name, interaction.user.id, interaction.user.display_name)
        t.is_tournament = True

        # 3. Deep-Stack Tournament Limits
        t.game.SMALL_BLIND = 50
        t.game.BIG_BLIND = 100
        t.game.MIN_BUYIN = 10000
        t.game.MAX_BUYIN = 10000
        t.game.tax_rate = 0
        t.game.tax_exempt = True

        # Load and apply guild settings
        settings = await db.get_settings(interaction.guild_id)
        from poker import TABLE_RESEND_MSGS
        t.resend_threshold = settings.get("resend_after_msgs", TABLE_RESEND_MSGS)
        tables[key] = t

        # 4. Use followup.send for the final confirmation!
        await interaction.followup.send(
            f"Tournament Table Open!\nBlind: {t.game.SMALL_BLIND}/{t.game.BIG_BLIND} | Min: {t.game.MIN_BUYIN} | Max: {t.game.MAX_BUYIN or 'None'}")
        await refresh(interaction.channel, t)

    @tourneymgr.command(name="blinds", description="Set blinds for this table")
    async def blinds(self, interaction: discord.Interaction, small_blind: int, big_blind: int):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True); return

        key = (interaction.guild_id, interaction.channel_id)
        t   = tables.get(key)
        if not t or not t.is_tournament:
            await interaction.response.send_message("No tournament table open here.", ephemeral=True); return

        t.game.SMALL_BLIND = small_blind
        t.game.BIG_BLIND   = big_blind
        await interaction.response.send_message(f"Blinds set to {small_blind}/{big_blind} for the next hand.")

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

        await interaction.response.send_message(f"🦵 **{user.display_name}** will be kicked after this hand.")

    @tourneymgr.command(name="addchips", description="Give tournament chips to a player")
    async def addchips(self, interaction: discord.Interaction, user: discord.Member, amount: int):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True); return

        if config.ADD_CHIPS_CHANNELS and interaction.channel_id not in config.ADD_CHIPS_CHANNELS:
            mentions = ", ".join(f"<#{cid}>" for cid in config.ADD_CHIPS_CHANNELS)
            await interaction.followup.send(f"❌ This command is restricted to: {mentions}", ephemeral=True)
            return

        if amount <= 0:
            await interaction.response.send_message("❌ Amount must be positive.", ephemeral=True)
            return

        if not await tdb.is_registered(user.id):
            await interaction.response.send_message("Player not registered.", ephemeral=True); return

        new_bal = await tdb.add_chips(user.id, user.display_name, amount)
        await interaction.response.send_message(
            f"Added {amount} {config.TOURNAMENT_CHIP_EMOJI} to {user.display_name}. New balance: {new_bal} {config.TOURNAMENT_CHIP_EMOJI}")

    @tourneymgr.command(name="removechips", description="Remove tournament chips from a player")
    async def removechips(self, interaction: discord.Interaction, user: discord.Member, amount: int):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True); return

        if config.REMOVE_CHIPS_CHANNELS and interaction.channel_id not in config.REMOVE_CHIPS_CHANNELS:
            mentions = ", ".join(f"<#{cid}>" for cid in config.REMOVE_CHIPS_CHANNELS)
            await interaction.followup.send(f"❌ This command is restricted to: {mentions}", ephemeral=True)
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

    @tourneymgr.command(name="createteam", description="Create a tournament team")
    async def createteam(self, interaction: discord.Interaction, name: str):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True); return

        success = await tdb.create_team(name, interaction.user.id)
        if success:
            await interaction.response.send_message(f"Team {name} created.")
        else:
            await interaction.response.send_message(f"Team {name} already exists.", ephemeral=True)

    @tourneymgr.command(name="addplayer", description="Add player to a team")
    async def addplayer(self, interaction: discord.Interaction, team_name: str, user: discord.Member):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True);
            return

        import tournament_db as tdb
        import datetime

        team = await tdb.get_team_by_name(team_name)
        if not team:
            await interaction.response.send_message("Team not found.", ephemeral=True);
            return

        stats = await tdb.get_player_stats(user.id)
        if not stats:
            await interaction.response.send_message("Player not registered.", ephemeral=True);
            return

        # 🛠️ FIXED: Prevent silent team swapping
        if stats.get('team_id') is not None:
            await interaction.response.send_message("❌ Player is already on a team. Remove them first.", ephemeral=True)
            return

        # --- ⏳ 24-HOUR DEADLINE CHECK ---
        reg_dt = datetime.datetime.fromisoformat(stats['registered_at']).replace(tzinfo=datetime.timezone.utc)
        now = datetime.datetime.now(datetime.timezone.utc)
        deadline = reg_dt + datetime.timedelta(days=1)

        #if now > deadline:
        #    await interaction.response.send_message(
        #        f"❌ **{user.display_name}** registered more than 24 hours ago. Players can only be added to a team within 24 hours of registering!",
        #        ephemeral=True
        #    )
        #    return
        # --------------------------------------------

        # 🛠️ FIXED: Rely on the DB's atomic lock to prevent TOCTOU overflow
        success = await tdb.add_player_to_team(user.id, team['id'])
        if not success:
            await interaction.response.send_message(f"❌ Team **{team_name}** is already full (Max 4 players).",
                                                    ephemeral=False)
            return

        await interaction.response.send_message(f"Added {user.display_name} to team {team_name}.")

    @tourneymgr.command(name="removeplayer", description="Remove player from their team")
    async def removeplayer(self, interaction: discord.Interaction, user: discord.Member):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True); return

        await tdb.remove_player_from_team(user.id)
        await interaction.response.send_message(f"Removed {user.display_name} from their team.")

    # -- Player ---------------------------------------------------------------

    @tourney.command(name="register", description="Register for the tournament")
    async def register(self, interaction: discord.Interaction):

        if interaction.channel_id != config.TOURNAMENT_REGISTER_CHANNEL_ID:
            await interaction.response.send_message(
                f"❌ You can only register for the tournament in <#{config.TOURNAMENT_REGISTER_CHANNEL_ID}>.",
                ephemeral=True
            )
            return

        success = await tdb.register_player(interaction.user.id, interaction.user.display_name)
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
        stats = await tdb.get_player_stats(target.id)

        if not stats:
            await interaction.followup.send("❌ Player not registered. Use `/tourney register` first.", ephemeral=True)
            return

        roster = None
        team_info = None
        if stats.get('team_id'):
            roster = await tdb.get_team_roster(stats['team_id'])
            team_info = await tdb.get_team_by_id(stats['team_id'])

        view = TournamentStatsView(interaction.user, target, stats, roster, team_info)
        await interaction.followup.send(embed=view.build_personal_embed(), view=view, ephemeral=hidden)

    @tourneymgr.command(name="setleader", description="[Manager] Set the captain/leader of a team")
    async def setleader(self, interaction: discord.Interaction, team_name: str, user: discord.Member):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True)
            return

        import tournament_db as tdb
        team = await tdb.get_team_by_name(team_name)
        if not team:
            await interaction.response.send_message("❌ Team not found.", ephemeral=True)
            return

        stats = await tdb.get_player_stats(user.id)
        if not stats or stats.get('team_id') != team['id']:
            await interaction.response.send_message(
                f"❌ **{user.display_name}** is not on team **{team_name}**. They must be on the team to be made captain.",
                ephemeral=True)
            return

        await tdb.set_team_leader(team['id'], user.id)
        await interaction.response.send_message(
            f"👑 **{user.display_name}** has been officially set as the captain of team **{team_name}**.")

    @tourney.command(name="leaderboard", description="View tournament leaderboards")
    async def leaderboard(self, interaction: discord.Interaction):
        await interaction.response.defer()
        indiv = await tdb.get_individual_leaderboard(10)

        for r in indiv:
            member = interaction.guild.get_member(r['user_id'])
            if member:
                r['username'] = member.name

        teams = await tdb.get_team_leaderboard(10)
        caller_id = interaction.user.id
        caller_row = await tdb.get_player_stats(caller_id)
        caller_rank = await tdb.get_player_rank(caller_id) if caller_row else None
        view = TournamentLeaderboardView(caller_id, caller_row, caller_rank, indiv, teams)
        await interaction.followup.send(embed=view.get_embed(), view=view)

    @tourney.command(name="myactivity", description="Check your dynamic 25% wager quota and activity")
    async def myactivity(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        import tournament_db as tdb
        stats = await tdb.get_player_stats(interaction.user.id)

        if not stats:
            await interaction.followup.send("❌ You are not registered for the tournament.", ephemeral=True)
            return

        total_chips = stats['total_chips']

        db = await tdb._get_db()
        # 🛠️ FIXED: Now fetching registered_at to verify the grace period
        async with db.execute(
                "SELECT period_wagered, last_activity, target_wager, registered_at FROM players WHERE user_id=?",
                (interaction.user.id,)) as c:
            row = await c.fetchone()
            wagered = row[0] if row and row[0] else 0
            last_act = row[1] if row else None
            target = row[2] if row and row[2] is not None else int(total_chips * 0.25)
            reg_str = row[3] if row and len(row) > 3 else None

        shortfall = max(0, target - wagered)
        percent_complete = min(100.0, (wagered / target * 100)) if target > 0 else 100.0
        is_top_10 = stats['rank'] is not None and stats['rank'] <= 10

        import datetime
        now = datetime.datetime.utcnow()

        # --- 🛡️ NEW PLAYER GRACE PERIOD CHECK ---
        is_grace = False
        if reg_str:
            try:
                reg_dt = datetime.datetime.fromisoformat(reg_str).replace(tzinfo=None)
                if now < reg_dt + datetime.timedelta(hours=48):
                    is_grace = True
            except Exception:
                pass

        if is_grace:
            status_msg = "🛡️ **New Player Grace Period!** You are safe from the tax for your first 48 hours."
        elif shortfall == 0:
            status_msg = "✅ **You have met your 25% quota!** You are safe from the tax."
        else:
            status_msg = f"⚠️ **You are short!** You need to wager **{shortfall:,}** more chips."

        warning = "\n🚨 *You are currently in the Top 10! You MUST meet this quota or the shortfall will be deducted from your balance.*" if is_top_10 and not is_grace else "\n*(Note: Penalties only apply to the Top 10 players, but it's good to stay active!)*"

        embed = discord.Embed(title="📊 Your 48-Hour Activity Tracker", color=0x3498DB)
        embed.add_field(name="Current Total Chips", value=f"{total_chips:,} {config.TOURNAMENT_CHIP_EMOJI}",
                        inline=True)
        embed.add_field(name="Target (25%)", value=f"{target:,}", inline=True)
        embed.add_field(name="Wagered So Far", value=f"{wagered:,}", inline=True)

        if last_act:
            dt = datetime.datetime.fromisoformat(last_act).replace(tzinfo=datetime.timezone.utc)
            embed.add_field(name="Last Active", value=f"<t:{int(dt.timestamp())}:R>", inline=False)
        else:
            embed.add_field(name="Last Active", value="Never", inline=False)

        async with db.execute("SELECT cycle_day FROM tourney_state WHERE id=1") as c:
            state_row = await c.fetchone()
            cycle_day = state_row[0] if state_row else 1

        # 🛠️ FIXED: Wipe time math now correctly snaps to exactly 02:30 UTC
        next_wipe = now.replace(hour=2, minute=30, second=0, microsecond=0)
        if now > next_wipe:
            next_wipe += datetime.timedelta(days=1)
        if cycle_day == 1:
            next_wipe += datetime.timedelta(days=1)

        # --- ⏳ PRE-TOURNAMENT OVERRIDE ---
        is_pre_tourney = now < datetime.datetime(2026, 6, 7)

        if is_pre_tourney:
            embed.add_field(name="Deadline", value="⏳ Starts June 6th", inline=False)
        else:
            wipe_timestamp = int(next_wipe.replace(tzinfo=datetime.timezone.utc).timestamp())
            embed.add_field(name="Deadline", value=f"<t:{wipe_timestamp}:R>", inline=False)

        filled = int(percent_complete / 10)
        bar = ("🟩" * filled) + ("⬛" * (10 - filled))

        if is_pre_tourney:
            embed.description = "⏳ **The tournament officially begins on June 6th!**\nTeam registration is open, but wager quotas and coasting penalties will not be enforced until the games begin."
        else:
            embed.description = f"{status_msg}{warning}\n\n**Progress:** {percent_complete:.1f}%\n{bar}"

        await interaction.followup.send(embed=embed, ephemeral=True)

    @tourneymgr.command(name="force_daily_check", description="[Admin] Manually trigger the daily tourney cycle")
    async def force_daily_check(self, interaction: discord.Interaction):
        if not await self.is_manager(interaction):
            await interaction.response.send_message("Managers only.", ephemeral=True);
            return

        await interaction.response.defer()

        # Bypass the date guard just for this manual trigger so you can test it!
        channel = self.bot.get_channel(config.TOURNAMENT_REGISTER_CHANNEL_ID)
        if not channel:
            await interaction.followup.send("❌ Error: Cannot find the tournament registration channel.")
            return

        import tournament_db as tdb
        cycle_day, warnings, penalties = await tdb.process_daily_tourney_check()

        await interaction.followup.send(
            f"✅ Forced the daily check. (Cycle Day {cycle_day} processed). Check the tournament channel for output.")

        # We manually call the logic here so we don't trip the June 6th block in the main task
        if cycle_day == 1:
            dm_warned = 0
            if warnings:
                for w in warnings:
                    embed = discord.Embed(title="⚠️ Tournament Coasting Warning", color=0xf39c12)
                    embed.description = f"Hey **{w['username']}**! You are falling behind on your 25% wager quota."
                    if await self._try_dm(w['user_id'], embed): dm_warned += 1
                await channel.send(
                    f"⚠️ **24-Hour Coasting Warning:** {len(warnings)} players in the Top 10 are falling short of their quota! (DMs sent: {dm_warned}/{len(warnings)})")
            else:
                await channel.send("✅ **Day 1 Check:** All Top 10 players are on track for their wager quotas.")
        else:
            dm_sent = 0
            if not penalties:
                await channel.send(
                    "🕒 **48-Hour Tournament Checkpoint:** All Top 10 players met their 25% wager quota! The period tracker has been reset.")
            else:
                embed = discord.Embed(title="🚨 Tournament Coasting Penalties Applied!", color=0xE74C3C)
                for p in penalties:
                    embed.add_field(name=p['username'], value=f"**Penalty:** -{p['shortfall']:,}", inline=False)
                    dm = discord.Embed(title="📉 Tournament Coasting Penalty Applied",
                                       description=f"You were penalized -{p['shortfall']:,} chips.", color=0xe74c3c)
                    if await self._try_dm(p['user_id'], dm): dm_sent += 1
                await channel.send(content=f"🚨 **Coasting Penalties Applied!** (DMs sent: {dm_sent}/{len(penalties)})",
                                   embed=embed)

    @tourney.command(name="teams", description="View all registered tournament teams")
    async def list_teams(self, interaction: discord.Interaction):
        await interaction.response.defer()
        import tournament_db as tdb
        teams = await tdb.get_all_teams_info()

        if not teams:
            await interaction.followup.send("No teams have been created yet.")
            return

        # Spawn the interactive paginated view
        view = TournamentTeamsView(interaction.user, teams)
        embed = await view.build_embed()

        await interaction.followup.send(embed=embed, view=view)

    @tourneymgr.command(name="sql", description="[Dev] Run a read-only tournament database query")
    @app_commands.describe(query="The SELECT query to run")
    async def run_sql(self, interaction: discord.Interaction, query: str):
        # 1. Ironclad Security Check
        if interaction.user.id not in config.DEV_USER_IDS:
            await interaction.response.send_message("❌ This command is restricted to bot developers.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)

        try:
            import aiosqlite
            # 2. Force SQLite into strict Read-Only mode using URI parameters
            # db.DB_PATH points to "poker.db" from your database.py file
            db_uri = f"file:{config.TOURNAMENT_DB_PATH}?mode=ro"

            async with aiosqlite.connect(db_uri, uri=True) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(query) as cursor:
                    rows = await cursor.fetchall()

                    if not rows:
                        await interaction.followup.send("✅ Query executed successfully. No rows returned.",
                                                        ephemeral=False)
                        return

                    # 3. Auto-format the columns and rows for Discord
                    columns = list(rows[0].keys())
                    lines = [f"**`{' | '.join(columns)}`**"]  # Header row

                    # Limit to 15 rows to prevent massive Discord spam
                    for row in rows[:15]:
                        lines.append(" | ".join(str(row[col]) for col in columns))

                    result_str = "\n".join(lines)

                    if len(rows) > 15:
                        result_str += f"\n\n*...and {len(rows) - 15} more rows.*"

                    # 4. Final safety clamp for Discord's 2000 character limit
                    if len(result_str) > 1900:
                        result_str = result_str[:1900] + "\n... [Output Truncated]"

                    await interaction.followup.send(f"**Query Results:**\n{result_str}", ephemeral=False)

        except Exception as e:
            # If they try to run an UPDATE or INSERT, this will catch the SQLite Read-Only error
            await interaction.followup.send(f"❌ **SQL Error:**\n`{e}`", ephemeral=False)

async def setup(bot):
    await bot.add_cog(TournamentCog(bot))
