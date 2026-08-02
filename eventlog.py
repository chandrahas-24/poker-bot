import discord
import zipfile
import io
import os
from discord import app_commands
from discord.ext import commands
import aiosqlite
import re
import traceback
import math
import datetime
import eventlog_database
import config

EVENT_LOG_CHANNELS = [982697197000876032, 1335375276879777812]
STAFF_ROLE_IDS = [1010238899320270999]

async def is_event_staff(interaction: discord.Interaction) -> bool:
    """Checks if the user is a Dev, Admin, or has a specific staff role."""

    if isinstance(interaction.user, discord.Member):
        # 1. Admin Bypass
        if interaction.user.guild_permissions.administrator:
            return True

        # 2. Check for specific roles
        for role_id in STAFF_ROLE_IDS:
            if interaction.user.get_role(role_id) is not None:
                return True

    # 3. Dev Bypass
    if interaction.user.id in getattr(config, 'DEV_USER_IDS', []):
        return True

    return False

class RawSQLPaginationView(discord.ui.View):
    def __init__(self, columns: list, rows: list, title: str, items_per_page: int = 15, max_pages_limit: int = 20):
        super().__init__(timeout=300)
        self.columns = columns
        self.rows = rows
        self.title = title
        self.items_per_page = items_per_page
        self.current_page = 0

        calculated_pages = math.ceil(len(rows) / items_per_page) if rows else 1
        self.max_pages = min(calculated_pages, max_pages_limit)

        self.update_buttons()

    def update_buttons(self):
        is_first_page = self.current_page == 0
        is_last_page = self.current_page >= self.max_pages - 1

        self.btn_first.disabled = is_first_page
        self.btn_prev.disabled = is_first_page
        self.btn_next.disabled = is_last_page
        self.btn_last.disabled = is_last_page

    def format_page(self):
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_rows = self.rows[start_idx:end_idx]

        embed = discord.Embed(title=self.title, color=discord.Color.dark_theme())

        header = " | ".join(str(c) for c in self.columns)
        separator = "-" * len(header)
        lines = [header, separator]

        for r in page_rows:
            row_str = " | ".join(str(r[col])[:40].replace('\n', ' ') for col in self.columns)
            lines.append(row_str)

        description = "\n".join(lines)

        if len(description) > 3900:
            description = description[:3900] + "\n...[Truncated]"

        embed.description = f"```\n{description}\n```"
        embed.set_footer(text=f"Page {self.current_page + 1} of {self.max_pages} | Total Rows: {len(self.rows)}")
        return embed

    @discord.ui.button(label="<<", style=discord.ButtonStyle.secondary)
    async def btn_first(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = 0
        self.update_buttons()
        await interaction.response.edit_message(embed=self.format_page(), view=self)

    @discord.ui.button(label="<", style=discord.ButtonStyle.primary)
    async def btn_prev(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.format_page(), view=self)

    @discord.ui.button(label=">", style=discord.ButtonStyle.primary)
    async def btn_next(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.format_page(), view=self)

    @discord.ui.button(label=">>", style=discord.ButtonStyle.secondary)
    async def btn_last(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = self.max_pages - 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.format_page(), view=self)


class EventLogsCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.processing_logs = set()

    eventlog_group = app_commands.Group(
        name="eventlog",
        description="Commands for managing and viewing event logs",
    )

    @eventlog_group.command(name="sql", description="[Dev] Run a read-only database query on the Event Logs")
    @app_commands.describe(query="The SELECT query to run")
    async def eventlog_sql(self, interaction: discord.Interaction, query: str):
        if not await is_event_staff(interaction):
            await interaction.response.send_message("❌ Missing required permissions.", ephemeral=True)
            return

        if not query.strip().upper().startswith("SELECT"):
            await interaction.response.send_message("❌ Only SELECT queries are allowed.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)

        try:
            db_uri = "file:eventlog_database.db?mode=ro"
            async with aiosqlite.connect(db_uri, uri=True) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(query) as cursor:
                    rows = await cursor.fetchall()

                    if not rows:
                        await interaction.followup.send("✅ Query executed successfully. No rows returned.",
                                                        ephemeral=False)
                        return

                    columns = list(rows[0].keys())
                    view = RawSQLPaginationView(
                        columns=columns,
                        rows=rows,
                        title="Event Logs DB Query",
                        items_per_page=15,
                        max_pages_limit=20
                    )
                    await interaction.followup.send(embed=view.format_page(), view=view, ephemeral=False)

        except Exception as e:
            traceback.print_exc()
            await interaction.followup.send(f"❌ **SQL Error:**\n`{e}`", ephemeral=False)

    @eventlog_group.command(name="hoststats", description="View total events hosted by staff since a specific date")
    @app_commands.describe(start_date="Start date in YYYY-MM-DD format (e.g., 2023-10-01)", end_date="End date in YYYY-MM-DD format (optional, defaults to today)")
    async def eventlog_hoststats(self, interaction: discord.Interaction, start_date: str, end_date: str = None):
        if not await is_event_staff(interaction):
            await interaction.response.send_message("❌ Missing required permissions.", ephemeral=True)
            return

        try:
            datetime.datetime.strptime(start_date, "%Y-%m-%d")
            if end_date:
                datetime.datetime.strptime(end_date, "%Y-%m-%d")
            else:
                end_date = datetime.date.today().strftime("%Y-%m-%d")
        except ValueError:
            await interaction.response.send_message("❌ Invalid date format. Please use **YYYY-MM-DD**.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)

        try:
            # Requires fetch_staff_stats to be present in eventlog_database.py
            records = await eventlog_database.fetch_staff_stats(start_date, end_date)

            if not records:
                await interaction.followup.send(f"❌ No events logged between **{start_date}** and **{end_date}**.")
                return

            embed = discord.Embed(
                title=f"📊 Staff Event Stats ({start_date} - {end_date})",
                color=0x3498db
            )

            # Putting the number first with an inline codeblock forces perfect alignment
            lines = ["**Hosted** ⸻ **Staff Member**"]
            for row in records:
                # Pad the number with a space if it's single digits (e.g., " 2" vs "15")
                count_str = str(row['total_events']).rjust(2, ' ')
                lines.append(f"` {count_str} ` ⸻ <@{row['staff_id']}>")

            description = "\n".join(lines)

            if len(description) > 4000:
                description = description[:4000] + "\n...[Truncated]"

            embed.description = description
            await interaction.followup.send(embed=embed)

        except Exception as e:
            traceback.print_exc()
            await interaction.followup.send(f"❌ **Error:** `{e}`")

    @eventlog_group.command(name="backup", description="[Dev] Zips and DMs you the database file.")
    async def eventlog_backup(self, interaction: discord.Interaction):
        # 1. Permission check
        if not await is_event_staff(interaction):
            await interaction.response.send_message("❌ Missing required permissions.", ephemeral=True)
            return

        # Defer response ephemerally so interaction doesn't time out during zipping/uploading
        await interaction.response.defer(ephemeral=True)

        db_path = "eventlog_database.db"

        if not os.path.exists(db_path):
            await interaction.followup.send("❌ Database file not found.", ephemeral=True)
            return

        try:
            # 2. Compress the DB file in memory (RAM) using BytesIO
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                # Add the database file into the zip
                zip_file.write(db_path, arcname=os.path.basename(db_path))

            # Reset stream position to the beginning before sending
            zip_buffer.seek(0)

            # 3. Create the Discord File object
            file = discord.File(fp=zip_buffer, filename="eventlog_database.zip")

            # 4. DM the user who triggered the command
            try:
                await interaction.user.send(
                    content="📦 **Here is your database backup:**",
                    file=file
                )
                await interaction.followup.send("✅ The database backup has been sent to your DMs!", ephemeral=True)
            except discord.Forbidden:
                # Triggers if the user's DMs are closed/blocked
                await interaction.followup.send(
                    "❌ Couldn't send you a DM! Please check your privacy settings and allow direct messages from server members.",
                    ephemeral=True
                )

        except Exception as e:
            traceback.print_exc()
            await interaction.followup.send(f"❌ An error occurred while creating the backup: `{e}`", ephemeral=True)

    @commands.Cog.listener("on_raw_reaction_add")
    async def process_embed_logs(self, payload: discord.RawReactionActionEvent):
        if payload.user_id == self.bot.user.id:
            return

        if payload.channel_id not in EVENT_LOG_CHANNELS:
            return

        if payload.emoji.name == "❌":
            return

        if payload.message_id in self.processing_logs:
            return
        self.processing_logs.add(payload.message_id)

        try:
            channel = self.bot.get_channel(payload.channel_id)
            if not channel:
                return

            try:
                message = await channel.fetch_message(payload.message_id)
                message_timestamp = (
                    message.created_at
                    .astimezone(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
                    .strftime("%Y-%m-%d %H:%M:%S")
                )
            except discord.NotFound:
                return

            if not message.embeds:
                return

            for reaction in message.reactions:
                if reaction.me and str(reaction.emoji) == "💾":
                    return

            embed = message.embeds[0]
            e_desc = embed.description or ""

            clean_desc = e_desc.replace("**", "")

            def extract_field(pattern, text):
                match = re.search(pattern, text, re.IGNORECASE)
                return match.group(1).strip() if match else "Unknown"

            donator = extract_field(r"Donator:\s*(.+)", clean_desc)
            event_type = extract_field(r"Event Type:\s*(.+)", clean_desc)
            prize = extract_field(r"Prize,\s*Message:\s*(.+)", clean_desc)

            if donator == "Unknown" and event_type == "Unknown":
                return

            old_footer = embed.footer.text or ""
            host_match = re.search(r"Host:\s*(.+?)\s*\((\d+)\)", old_footer)

            if host_match:
                host_name = host_match.group(1).strip()
                host_id = int(host_match.group(2))
            else:
                host_name = payload.member.name if payload.member else "Unknown"
                host_id = payload.user_id

            success = await eventlog_database.save_embed_log(
                staff_id=host_id,
                staff_username=host_name,
                message_id=payload.message_id,
                donator_username=donator,
                event_type=event_type,
                event_prize_msg=prize,
                timestamp = message_timestamp
            )

            if success:
                await message.add_reaction("💾")

        except Exception:
            traceback.print_exc()
        finally:
            self.processing_logs.discard(payload.message_id)


async def setup(bot):
    await bot.add_cog(EventLogsCog(bot))