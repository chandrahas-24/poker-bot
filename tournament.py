import time
import discord
from discord.ext import commands
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

            table_lines = ["```"]
            table_lines.append(f"{'':4}{'Player':<18} {'Win%':>5} {'Total':>9} {'Wins':>6}")
            table_lines.append("─" * 45)
            for i, r in enumerate(self.indiv):
                rank = i + 1
                wp = f"{r['hands_won'] / r['hands_played'] * 100:.0f}%" if r['hands_played'] else "—"
                total = r['total_chips']
                uname = r['username'][:17]
                medal = MEDALS.get(rank, f"{rank}. ")
                you_tag = " ◀" if r['user_id'] == self.caller_id else ""
                total_str = f"{total:,}"
                wins_str = f"{r['hands_won']:,}"
                table_lines.append(f"{medal:<4}{uname:<18} {wp:>5} {total_str:>9} {wins_str:>6}{you_tag}")
            table_lines.append("```")
            embed.description = "\n".join(table_lines)

            # Caller's stats - shown at the bottom whether or not they're in the top 10
            if self.caller_row:
                caller_wp = f"{self.caller_row['hands_won'] / self.caller_row['hands_played'] * 100:.1f}%" if self.caller_row['hands_played'] else "—"
                in_top = self.caller_id in top_ids
                rank_str = f"#{self.caller_rank}" if self.caller_rank else "—"
                label = f"📊 Your Tournament Stats  ·  {rank_str}" + (" *(in top 10)*" if in_top else "")
                embed.add_field(
                    name=label,
                    value=(
                        f"Win% **{caller_wp}**  ·  "
                        f"Total **{self.caller_row['total_chips']:,}** {config.TOURNAMENT_CHIP_EMOJI}  ·  "
                        f"Wins **{self.caller_row['hands_won']:,}**"
                    ),
                    inline=False
                )
            else:
                embed.add_field(name="📊 Your Tournament Stats", value="No hands played yet.", inline=False)
        else:
            text = "\n".join(
                f"{i+1}. **{tm['name']}** - {tm['total_wins']} hands won"
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
    def __init__(self, caller: discord.User | discord.Member, target: discord.User | discord.Member, stats: dict, roster=None):
        super().__init__(timeout=120)
        self.caller = caller
        self.target = target
        self.stats = stats
        self.roster = roster
        self.show_team = False
        if not roster:
            self.toggle.disabled = True

    def build_personal_embed(self) -> discord.Embed:
        net = self.stats['total_chips'] - config.TOURNAMENT_STARTING_CHIPS
        embed = discord.Embed(title=f"Tournament Stats — {self.target.display_name}", color=0x2ecc71 if net >= 0 else 0xe74c3c)

        rank_str = f"#{self.stats['rank']}" if self.stats['rank'] else "Unranked"
        wp = f"{self.stats['win_rate']:.1f}%" if self.stats['hands_played'] > 0 else "—"

        embed.add_field(name="Rank", value=rank_str, inline=True)
        embed.add_field(name="Hands Played", value=f"{self.stats['hands_played']:,}", inline=True)
        embed.add_field(name="Win %", value=wp, inline=True)

        # Career Net, Wallet Balance, and Chips in Play
        embed.add_field(name="Net Chips", value=f"{'+' if net >= 0 else ''}{net:,} {config.TOURNAMENT_CHIP_EMOJI}", inline=True)
        embed.add_field(name="Wallet Balance", value=f"{self.stats['balance']:,} {config.TOURNAMENT_CHIP_EMOJI}", inline=True)
        embed.add_field(name="Chips in Play", value=f"{self.stats['chips_in_play']:,} {config.TOURNAMENT_CHIP_EMOJI}", inline=True)

        team_val = self.stats['team_name'] or "None"
        embed.add_field(name="Team", value=team_val, inline=False)
        return embed

    def build_team_embed(self) -> discord.Embed:
        embed = discord.Embed(title=f"Team Stats — {self.stats['team_name']}", color=0xE67E22)
        text = "\n".join(
            f"{i+1}. **{p['username']}** - {p['total_chips']:,} {config.TOURNAMENT_CHIP_EMOJI} ({p['hands_won']} wins)"
            for i, p in enumerate(self.roster)) or "No players."
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


# -- Cog ----------------------------------------------------------------------

class TournamentCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

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
            await interaction.response.send_message("Managers only.", ephemeral=True); return

        team = await tdb.get_team_by_name(team_name)
        if not team:
            await interaction.response.send_message("Team not found.", ephemeral=True); return

        if not await tdb.is_registered(user.id):
            await interaction.response.send_message("Player not registered.", ephemeral=True); return

        roster = await tdb.get_team_roster(team['id'])
        if len(roster) >= 4:
            await interaction.response.send_message(f"❌ Team **{team_name}** is already full (Max 4 players).",
                                                    ephemeral=False)
            return

        await tdb.add_player_to_team(user.id, team['id'])
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
        if stats.get('team_id'):
            roster = await tdb.get_team_roster(stats['team_id'])

        view = TournamentStatsView(interaction.user, target, stats, roster)
        await interaction.followup.send(embed=view.build_personal_embed(), view=view, ephemeral=hidden)

    @tourney.command(name="leaderboard", description="View tournament leaderboards")
    async def leaderboard(self, interaction: discord.Interaction):
        await interaction.response.defer()
        indiv = await tdb.get_individual_leaderboard(10)
        teams = await tdb.get_team_leaderboard(10)
        caller_id = interaction.user.id
        caller_row = await tdb.get_player_stats(caller_id)
        caller_rank = await tdb.get_player_rank(caller_id) if caller_row else None
        view = TournamentLeaderboardView(caller_id, caller_row, caller_rank, indiv, teams)
        await interaction.followup.send(embed=view.get_embed(), view=view)

    @tourney.command(name="status", description="View active tournament tables")
    async def status(self, interaction: discord.Interaction):
        active = [t for t in tables.values() if t.is_tournament]

        if not active:
            await interaction.response.send_message("No active tournament tables.", ephemeral=True); return

        embed = discord.Embed(title="Active Tournament Tables", color=0x2ECC71)
        for t in active:
            players = len(t.game.players) + len(t.game.pending_joins)
            embed.add_field(
                name=t.name,
                value=f"Players: {players}/12 | Blinds: {t.game.SMALL_BLIND}/{t.game.BIG_BLIND}",
                inline=False)

        await interaction.response.send_message(embed=embed)


async def setup(bot):
    await bot.add_cog(TournamentCog(bot))
