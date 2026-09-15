import discord
from discord.ext import commands
from discord import app_commands
import json
import os
import re
import time
from collections import deque
import aiohttp
import asyncio
import config

DATA_FILE = "highlight_words.json"
CONFIG_FILE = "highlight_config.json"

DEFAULT_CONFIG = {
    "cooldown_seconds": 10,
    "active_context_seconds": 60,
    "allowed_users": [],
    "role_ids": [836210264873369630],
    "max_context_words": 1
}

def load_config():
    if not os.path.exists(CONFIG_FILE):
        config_data = DEFAULT_CONFIG.copy()
        save_config(config_data)
        return config_data

    try:
        with open(CONFIG_FILE, "r") as f:
            config_data = json.load(f)

        for key, value in DEFAULT_CONFIG.items():
            config_data.setdefault(key, value)

        return config_data
    except Exception as e:
        print(f"[Highlight] Error loading config: {e}")
        return DEFAULT_CONFIG.copy()

def save_config(config_data):
    with open(CONFIG_FILE, "w") as f:
        json.dump(config_data, f, indent=4)


HIGHLIGHT_CONFIG = load_config()
COOLDOWN_SECONDS = HIGHLIGHT_CONFIG["cooldown_seconds"]
ACTIVE_CONTEXT_SECONDS = HIGHLIGHT_CONFIG["active_context_seconds"]
hl_allowed = {int(user_id): True for user_id in HIGHLIGHT_CONFIG["allowed_users"]}
HIGHLIGHT_ROLE_IDS = HIGHLIGHT_CONFIG["role_ids"]

async def is_hl_authorized(interaction: discord.Interaction) -> bool:
    """Checks if the user is a Dev, Admin, has a specific role, or is whitelisted."""
    # 1. Dev Bypass
    if interaction.user.id in getattr(config, 'DEV_USER_IDS', []):
        return True

    # 2. Whitelisted Users Bypass
    if interaction.user.id in hl_allowed:
        return True

    if isinstance(interaction.user, discord.Member):
        # 3. Admin Bypass
        if interaction.user.guild_permissions.administrator:
            return True

        # 4. Role Check
        for role_id in HIGHLIGHT_ROLE_IDS:
            if interaction.user.get_role(role_id) is not None:
                return True

    return False


class HighlightCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.highlights = self.load_data()
        self.cooldowns = {}
        self.regex_cache = {}
        self.last_active = {}  # Tracks {(user_id, channel_id): timestamp} for highlight users ONLY
        self.channel_history = {}
        self.session = None    # Persistent aiohttp session

    async def cog_load(self):
        """Creates the session once when the cog starts."""
        self.session = aiohttp.ClientSession()

    async def cog_unload(self):
        """Cleanly closes the session when the cog unloads or reloads."""
        if self.session and not self.session.closed:
            await self.session.close()

    def load_data(self):
        if not os.path.exists(DATA_FILE):
            return {}

        try:
            with open(DATA_FILE, "r") as f:
                data = json.load(f)

            migrated_data = {}
            for user_id_str, user_data in data.items():
                if isinstance(user_data, list):
                    migrated_data[user_id_str] = {
                        "strict": set(user_data),
                        "general": set(),
                        "wildcard": set(),
                        "context_words": set(),
                        "blocked_channels": set(),
                        "blocked_users": set()
                    }
                else:
                    migrated_data[user_id_str] = {
                        "strict": set(user_data.get("strict", [])),
                        "general": set(user_data.get("general", [])),
                        "wildcard": set(user_data.get("wildcard", [])),
                        "context_words": set(
                            user_data.get(
                                "context_words",
                                [user_data["context_word"]] if user_data.get("context_word") else []
                            )
                        ),
                        "blocked_channels": set(user_data.get("blocked_channels", [])),
                        "blocked_users": set(user_data.get("blocked_users", []))
                    }
            return migrated_data
        except Exception as e:
            print(f"[Highlight] Error loading JSON data: {e}")
            return {}

    def save_data(self):
        with open(DATA_FILE, "w") as f:
            json_data = {}
            for user_id_str, data in self.highlights.items():
                json_data[str(user_id_str)] = {
                    "strict": list(data["strict"]),
                    "general": list(data["general"]),
                    "wildcard": list(data["wildcard"]),
                    "context_words": list(data.get("context_words", set())),
                    "blocked_channels": list(data["blocked_channels"]),
                    "blocked_users": list(data["blocked_users"])
                }
            json.dump(json_data, f, indent=4)

    def _ensure_user(self, user_id: str):
        if user_id not in self.highlights:
            self.highlights[user_id] = {
                "strict": set(), "general": set(), "wildcard": set(), "context_words": set(), "blocked_channels": set(),
                "blocked_users": set()
            }

    # ── UI Management Slash Commands ───────────────────────────────────────

    highlight_group = app_commands.Group(name="hl", description="Manage your tracked keywords and blocks", guild_ids=[config.GUILD_ID],)

    @highlight_group.command(name="add", description="Add a new keyword to track")
    @app_commands.describe(
        word="The exact word you want to track",
        match_type="Match strategy"
    )
    @app_commands.choices(match_type=[
        app_commands.Choice(name="Strict (Exact Word)", value="strict"),
        app_commands.Choice(name="General (Substring Anywhere)", value="general"),
        app_commands.Choice(name="Wildcard (Use * for variable string, ? for single char variable)", value="wildcard")
    ])
    async def add_word(self, interaction: discord.Interaction, word: str, match_type: str = "strict"):
        if not await is_hl_authorized(interaction):
            await interaction.response.send_message("❌ This command is restricted.", ephemeral=True)
            return

        word = word.lower().strip()
        user_id = str(interaction.user.id)

        if word == "*" and match_type == "wildcard":
            return await interaction.response.send_message("❌ You cannot track a standalone wildcard.", ephemeral=True)

        self._ensure_user(user_id)

        total_words = len(self.highlights[user_id]["strict"]) + len(self.highlights[user_id]["general"]) + len(
            self.highlights[user_id]["wildcard"])
        if total_words >= 100:
            embed = discord.Embed(title="❌ Limit Reached", description="You can only track up to 100 keywords total.",
                                  color=discord.Color.red())
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        for t in ["strict", "general", "wildcard"]:
            if t != match_type:
                self.highlights[user_id][t].discard(word)

        self.highlights[user_id][match_type].add(word)
        self.save_data()

        embed = discord.Embed(
            title="✅ Keyword Added",
            description=f"Now tracking **{word}** using **{match_type}** matching.",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @highlight_group.command(name="remove", description="Remove a tracked keyword")
    @app_commands.describe(word="The word you want to stop tracking")
    async def remove_word(self, interaction: discord.Interaction, word: str):
        if not await is_hl_authorized(interaction):
            await interaction.response.send_message("❌ This command is restricted.", ephemeral=True)
            return

        word = word.lower().strip()
        user_id = str(interaction.user.id)
        self._ensure_user(user_id)

        removed = False
        for t in ["strict", "general", "wildcard"]:
            if word in self.highlights[user_id][t]:
                self.highlights[user_id][t].remove(word)
                removed = True

        if removed:
            self.highlights[user_id]["context_words"].discard(word)

            self.save_data()
            embed = discord.Embed(title="🗑️ Keyword Removed", description=f"I have stopped tracking **{word}**.",
                                  color=discord.Color.orange())
        else:
            embed = discord.Embed(title="⚠️ Not Found", description=f"You are not tracking **{word}**.",
                                  color=discord.Color.red())

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @highlight_group.command(name="set_context", description="Add a context-aware highlight word.")
    @app_commands.describe(word="The exact word you want the AI to verify")
    async def setai(self, interaction: discord.Interaction, word: str):
        if not await is_hl_authorized(interaction):
            await interaction.response.send_message("❌ This command is restricted.", ephemeral=True)
            return

        clean_word = word.strip().lower()
        if not clean_word:
            return await interaction.response.send_message("❌ You must provide a word.", ephemeral=True)

        user_id = str(interaction.user.id)
        self._ensure_user(user_id)
        context_words = self.highlights[user_id]["context_words"]

        if clean_word not in context_words:
            max_context_words = HIGHLIGHT_CONFIG["max_context_words"]
            if len(context_words) >= max_context_words:
                return await interaction.response.send_message(
                    f"❌ You can only have **{max_context_words}** context word{'s' if max_context_words != 1 else ''}.",
                    ephemeral=True
                )
            context_words.add(clean_word)

        wildcard_version = f"*{clean_word}*"
        if wildcard_version not in self.highlights[user_id]["wildcard"]:
            self.highlights[user_id]["wildcard"].add(wildcard_version)

        self.save_data()
        await interaction.response.send_message(
            f"✅ **{clean_word}** is now a context-verified word.",
            ephemeral=True
        )

    @highlight_group.command(name="remove_context", description="Remove a context-specific highlight word.")
    @app_commands.describe(word="The context word to remove")
    async def removeai(self, interaction: discord.Interaction, word: str):
        if not await is_hl_authorized(interaction):
            await interaction.response.send_message("❌ This command is restricted.", ephemeral=True)
            return

        user_id = str(interaction.user.id)
        self._ensure_user(user_id)
        clean_word = word.strip().lower()
        context_words = self.highlights[user_id]["context_words"]

        if clean_word not in context_words:
            return await interaction.response.send_message(
                f"⚠️ **{clean_word}** is not a context word.", ephemeral=True
            )

        context_words.remove(clean_word)
        wildcard_version = f"*{clean_word}*"
        self.highlights[user_id]["wildcard"].discard(wildcard_version)
        self.save_data()

        await interaction.response.send_message(
            f"❌ Removed context word **{clean_word}**.", ephemeral=True
        )

    @highlight_group.command(name="context_list", description="View your context-verified words.")
    async def context_list(self, interaction: discord.Interaction):
        if not await is_hl_authorized(interaction):
            await interaction.response.send_message("❌ This command is restricted.", ephemeral=True)
            return

        user_id = str(interaction.user.id)
        self._ensure_user(user_id)
        context_words = self.highlights[user_id]["context_words"]
        max_context_words = HIGHLIGHT_CONFIG["max_context_words"]

        words = "\n".join(f"• `{word}`" for word in sorted(context_words)) or "None"
        embed = discord.Embed(title="🤖 Context-Verified Words", description=words, color=discord.Color.blue())
        embed.set_footer(text=f"{len(context_words)}/{max_context_words} slots used")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @highlight_group.command(name="config", description="Configure highlight system settings.")
    @app_commands.describe(
        setting="Setting or allowlist operation",
        value="New numeric value (for numeric settings)",
        user="User for allow/disallow",
        role="Role for addrole/removerole"
    )
    @app_commands.choices(setting=[
        app_commands.Choice(name="Cooldown Seconds", value="cooldown"),
        app_commands.Choice(name="Active Context Seconds", value="active_context"),
        app_commands.Choice(name="Max Context Words", value="max_context_words"),
        app_commands.Choice(name="Allow User", value="allow"),
        app_commands.Choice(name="Disallow User", value="disallow"),
        app_commands.Choice(name="Add Role", value="add_role"),
        app_commands.Choice(name="Remove Role", value="remove_role"),
    ])
    async def config_highlight(
        self,
        interaction: discord.Interaction,
        setting: str,
        value: int = None,
        user: discord.User = None,
        role: discord.Role = None
    ):
        if interaction.user.id not in getattr(config, "DEV_USER_IDS", []):
            await interaction.response.send_message("❌ Dev-only command.", ephemeral=True)
            return

        if setting == "cooldown":
            if value is None or value < 0:
                await interaction.response.send_message("❌ Provide a non-negative numeric value.", ephemeral=True)
                return
            HIGHLIGHT_CONFIG["cooldown_seconds"] = value
            globals()["COOLDOWN_SECONDS"] = value
            save_config()
            await interaction.response.send_message(f"✅ Cooldown set to `{value}s`.", ephemeral=True)

        elif setting == "active_context":
            if value is None or value < 0:
                await interaction.response.send_message("❌ Provide a non-negative numeric value.", ephemeral=True)
                return
            HIGHLIGHT_CONFIG["active_context_seconds"] = value
            globals()["ACTIVE_CONTEXT_SECONDS"] = value
            save_config()
            await interaction.response.send_message(f"✅ Active context set to `{value}s`.", ephemeral=True)

        elif setting == "max_context_words":
            if value is None or value < 1:
                await interaction.response.send_message("❌ Max context words must be at least 1.", ephemeral=True)
                return
            HIGHLIGHT_CONFIG["max_context_words"] = value
            save_config()
            await interaction.response.send_message(f"✅ Max context words set to `{value}`.", ephemeral=True)

        elif setting == "allow":
            if user is None:
                await interaction.response.send_message("❌ Select a user.", ephemeral=True)
                return
            hl_allowed[user.id] = True
            allowed_users = {int(uid) for uid in HIGHLIGHT_CONFIG.get("allowed_users", [])}
            allowed_users.add(user.id)
            HIGHLIGHT_CONFIG["allowed_users"] = sorted(allowed_users)
            save_config()
            await interaction.response.send_message(f"✅ {user.mention} is now allowlisted.", ephemeral=True)

        elif setting == "disallow":
            if user is None:
                await interaction.response.send_message("❌ Select a user.", ephemeral=True)
                return
            hl_allowed.pop(user.id, None)
            allowed_users = {int(uid) for uid in HIGHLIGHT_CONFIG.get("allowed_users", [])}
            allowed_users.discard(user.id)
            HIGHLIGHT_CONFIG["allowed_users"] = sorted(allowed_users)
            save_config()
            await interaction.response.send_message(f"✅ {user.mention} was removed from the allowlist.", ephemeral=True)

        elif setting == "add_role":
            if role is None:
                await interaction.response.send_message("❌ Select a role.", ephemeral=True)
                return
            role_ids = {int(role_id) for role_id in HIGHLIGHT_CONFIG.get("role_ids", [])}
            role_ids.add(role.id)
            HIGHLIGHT_CONFIG["role_ids"] = sorted(role_ids)
            if role.id not in HIGHLIGHT_ROLE_IDS:
                HIGHLIGHT_ROLE_IDS.append(role.id)
            save_config()
            await interaction.response.send_message(
                f"✅ Members with {role.mention} can now use highlights.",
                ephemeral=True
            )

        elif setting == "remove_role":
            if role is None:
                await interaction.response.send_message("❌ Select a role.", ephemeral=True)
                return
            role_ids = {int(role_id) for role_id in HIGHLIGHT_CONFIG.get("role_ids", [])}
            role_ids.discard(role.id)
            HIGHLIGHT_CONFIG["role_ids"] = sorted(role_ids)
            while role.id in HIGHLIGHT_ROLE_IDS:
                HIGHLIGHT_ROLE_IDS.remove(role.id)
            save_config()
            await interaction.response.send_message(
                f"✅ {role.mention} was removed from the highlight role allowlist.",
                ephemeral=True
            )

    @highlight_group.command(name="settings", description="Show the current highlight configuration.")
    async def highlight_settings(self, interaction: discord.Interaction):
        if interaction.user.id not in getattr(config, "DEV_USER_IDS", []):
            await interaction.response.send_message(
                "❌ Dev-only command.",
                ephemeral=True
            )
            return

        config_lines = []

        # Display every field currently present in highlight_config.json.
        for key, value in HIGHLIGHT_CONFIG.items():
            label = key.replace("_", " ").title()

            if key == "allowed_users":
                if value:
                    users = []
                    for user_id in value:
                        users.append(f"<@{int(user_id)}>")
                    display_value = "\n".join(f"• {user}" for user in users)
                else:
                    display_value = "None"

            elif key == "role_ids":
                if value:
                    roles = []
                    for role_id in value:
                        roles.append(f"<@&{int(role_id)}>")
                    display_value = "\n".join(f"• {role}" for role in roles)
                else:
                    display_value = "None"

            elif isinstance(value, list):
                display_value = ", ".join(str(item) for item in value) or "None"

            else:
                display_value = str(value)

            config_lines.append(f"**{label}:**\n{display_value}")

        embed = discord.Embed(
            title="⚙️ Highlight Configuration",
            description="\n\n".join(config_lines),
            color=discord.Color.blue()
        )
        embed.set_footer(text="Loaded from highlight_config.json")

        await interaction.response.send_message(
            embed=embed,
            ephemeral=True
        )

    @highlight_group.command(name="block", description="Block a channel or user from triggering your highlights")
    async def block_source(self, interaction: discord.Interaction, channel: discord.TextChannel = None,
                           user: discord.Member = None):
        if not await is_hl_authorized(interaction):
            await interaction.response.send_message("❌ This command is restricted.", ephemeral=True)
            return

        user_id = str(interaction.user.id)
        self._ensure_user(user_id)

        if not channel and not user:
            return await interaction.response.send_message("❌ You must specify either a channel or a user to block.",
                                                           ephemeral=True)

        description = ""
        if channel:
            self.highlights[user_id]["blocked_channels"].add(channel.id)
            description += f"Blocked channel: {channel.mention}\n"
        if user:
            self.highlights[user_id]["blocked_users"].add(user.id)
            description += f"Blocked user: {user.mention}\n"

        self.save_data()
        embed = discord.Embed(title="🛑 Source Blocked", description=description, color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @highlight_group.command(name="unblock", description="Unblock a channel or user")
    async def unblock_source(self, interaction: discord.Interaction, channel: discord.TextChannel = None,
                             user: discord.Member = None):
        if not await is_hl_authorized(interaction):
            await interaction.response.send_message("❌ This command is restricted.", ephemeral=True)
            return

        user_id = str(interaction.user.id)
        self._ensure_user(user_id)

        description = ""
        if channel and channel.id in self.highlights[user_id]["blocked_channels"]:
            self.highlights[user_id]["blocked_channels"].remove(channel.id)
            description += f"Unblocked channel: {channel.mention}\n"
        if user and user.id in self.highlights[user_id]["blocked_users"]:
            self.highlights[user_id]["blocked_users"].remove(user.id)
            description += f"Unblocked user: {user.mention}\n"

        if not description:
            return await interaction.response.send_message("⚠️ That channel/user is not currently blocked.",
                                                           ephemeral=True)

        self.save_data()
        embed = discord.Embed(title="✅ Source Unblocked", description=description, color=discord.Color.green())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @highlight_group.command(name="list", description="View your tracked keywords and blocklists")
    async def list_words(self, interaction: discord.Interaction):
        if not await is_hl_authorized(interaction):
            await interaction.response.send_message("❌ This command is restricted.", ephemeral=True)
            return

        user_id = str(interaction.user.id)
        self._ensure_user(user_id)

        data = self.highlights[user_id]
        embed = discord.Embed(title="📋 Your Highlight Settings", color=discord.Color.blue())

        context_words = data.get("context_words", set())
        ai_words = "\n".join(f"• `{word}`" for word in sorted(context_words)) or "None"
        embed.add_field(name="Context Words", value=ai_words, inline=False)

        strict_list = "\n".join([f"• `{w}`" for w in data["strict"]]) or "None"
        embed.add_field(name="Strict Matches", value=strict_list, inline=True)

        general_list = "\n".join([f"• `{w}`" for w in data["general"]]) or "None"
        embed.add_field(name="General Matches", value=general_list, inline=True)

        wildcard_list = "\n".join([f"• `{w}`" for w in data["wildcard"]]) or "None"
        embed.add_field(name="Wildcard Matches", value=wildcard_list, inline=False)

        blocked_c = "\n".join([f"• <#{c}>" for c in data["blocked_channels"]]) or "None"
        embed.add_field(name="Blocked Channels", value=blocked_c, inline=True)

        blocked_u = "\n".join([f"• <@{u}>" for u in data["blocked_users"]]) or "None"
        embed.add_field(name="Blocked Users", value=blocked_u, inline=True)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ── Event Processor ────────────────────────────────────────

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return

        if message.channel.id not in self.channel_history:
            self.channel_history[message.channel.id] = deque(maxlen=6)

        time_str = message.created_at.strftime('%H:%M:%S')
        clean_content = discord.utils.escape_markdown(message.content)
        self.channel_history[message.channel.id].append(f"**[{time_str}] {message.author.name}:** {clean_content}")

        current_time = time.time()
        author_id_str = str(message.author.id)

        if author_id_str in self.highlights:
            self.last_active[(author_id_str, message.channel.id)] = current_time

        content_lower = message.content.lower()
        message_words = None

        for user_id_str, prefs in self.highlights.items():
            if author_id_str == user_id_str:
                continue

            last_seen = self.last_active.get((user_id_str, message.channel.id), 0)
            if current_time - last_seen < ACTIVE_CONTEXT_SECONDS:
                continue

            member = message.guild.get_member(int(user_id_str))
            if not member:
                continue

            permissions = message.channel.permissions_for(member)
            if not permissions.view_channel:
                continue

            if message.channel.id in prefs["blocked_channels"]:
                continue
            if message.author.id in prefs["blocked_users"]:
                continue

            matches = set()

            if prefs["strict"]:
                if message_words is None:
                    message_words = set(re.findall(r'\b\w+\b', content_lower))
                matches.update(message_words.intersection(prefs["strict"]))

            for g_word in prefs["general"]:
                if g_word in content_lower:
                    matches.add(g_word)

            for w_pattern in prefs["wildcard"]:
                if w_pattern not in self.regex_cache:
                    escaped = re.escape(w_pattern)
                    regex_body = escaped.replace(r'\*', r'\w*').replace(r'\?', r'\w')
                    regex_str = r'(?i)\b' + regex_body + r'\b'
                    self.regex_cache[w_pattern] = re.compile(regex_str)

                if self.regex_cache[w_pattern].search(content_lower):
                    matches.add(w_pattern)

            for word in matches:
                cooldown_key = (user_id_str, word)
                last_sent = self.cooldowns.get(cooldown_key, 0)

                if current_time - last_sent > COOLDOWN_SECONDS:
                    # Only deliver highlights for channels the recipient can see.
                    member = message.guild.get_member(int(user_id_str)) if message.guild else None
                    if member is None and message.guild:
                        try:
                            member = await message.guild.fetch_member(int(user_id_str))
                        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                            member = None

                    if member is None or not message.channel.permissions_for(member).view_channel:
                        continue

                    self.cooldowns[cooldown_key] = current_time
                    self.bot.loop.create_task(self.send_alert(user_id_str, word, message))

    # ── AI Verification & DM Dispatch ─────────────────────────────────────

    async def verify_context_with_gemini(self, word: str, context_text: str) -> bool:
        api_key = os.getenv("API_KEY")

        if not api_key:
            print("[Groq] API key not found -> allowing highlight")
            return True

        model = "openai/gpt-oss-20b"
        url = "https://api.groq.com/openai/v1/chat/completions"
        base_word = word.replace('*', '')

        prompt = (
            f"You are deciding whether a Discord highlight should trigger.\n\n"
            f"Tracked term: '{base_word}'\n\n"
            f"Chat log:\n"
            f"{context_text}\n\n"
            f"Only the FINAL message is being evaluated. Earlier messages are "
            f"provided only as context to understand the final message.\n\n"
            f"Decide whether the occurrence of '{base_word}' in the final message "
            f"is a meaningful use of the tracked term, rather than an accidental "
            f"substring.\n\n"
            f"ALLOW (YES) when the occurrence is intentionally being used as:\n"
            f"- a username, nickname, alias, or player name\n"
            f"- a fictional character or other named entity\n"
            f"- a deliberately altered or stretched spelling of the tracked term\n"
            f"- another clear reference to the tracked term that a person would "
            f"reasonably want highlighted\n\n"
            f"REJECT (NO) when the occurrence is merely incidental, such as:\n"
            f"- normal grammatical or conversational usage of a common word "
            f"(for example, 'may' meaning possibility or permission)\n"
            f"- part of an ordinary dictionary word with an unrelated meaning\n"
            f"- part of a city, country, geographical location, or other place name\n"
            f"- part of an unrelated person's name or other entity\n"
            f"- an accidental substring with no meaningful connection to the "
            f"tracked term\n\n"
            f"Important:\n"
            f"- Judge the meaning and usage of the FINAL message, not merely whether "
            f"the characters match.\n"
            f"- Do not reject a match simply because it is not an exact standalone "
            f"word.\n"
            f"- A stretched or intentionally modified spelling can still be a valid "
            f"match.\n"
            f"- Use earlier messages only when they clearly help establish the "
            f"meaning of the final message.\n\n"
            f"Respond with exactly one word: YES or NO."
        )

        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_completion_tokens": 256,
            "reasoning_effort": "low",
            "include_reasoning": True
        }

        # Give all attempts a shared 60-second deadline rather than 60 seconds each.
        total_timeout = 60
        max_attempts = 3
        deadline = time.monotonic() + total_timeout

        session = self.session if (self.session and not self.session.closed) else aiohttp.ClientSession()

        for attempt in range(1, max_attempts + 1):
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                print("[Groq] 60s total timeout reached -> allowing highlight")
                return True

            try:
                timeout = aiohttp.ClientTimeout(total=remaining)

                async with session.post(
                    url,
                    json=payload,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json"
                    },
                    timeout=timeout
                ) as resp:
                    response_text = await resp.text()

                    if resp.status != 200:
                        if resp.status in (429, 500, 502, 503, 504) and attempt < max_attempts:
                            print(f"[Groq] HTTP {resp.status} -> retrying")
                            continue

                        print(f"[Groq] HTTP {resp.status} -> allowing highlight")
                        return True

                    try:
                        data = json.loads(response_text)
                    except json.JSONDecodeError:
                        print("[Groq] Invalid JSON response -> allowing highlight")
                        return True

                    choices = data.get("choices", [])

                    if not choices:
                        print("[Groq] No choices returned -> allowing highlight")
                        return True

                    content = choices[0].get("message", {}).get("content", "")
                    reply = content.strip().upper()

                    if reply == "NO":
                        return False

                    if reply == "YES":
                        return True

                    print("[Groq] Unexpected response -> allowing highlight")
                    return True

            except (aiohttp.ServerTimeoutError, asyncio.TimeoutError):
                remaining = deadline - time.monotonic()

                if remaining > 0 and attempt < max_attempts:
                    print(f"[Groq] Attempt {attempt} timed out -> retrying")
                    continue

                print("[Groq] 60s total timeout reached -> allowing highlight")
                return True

            except aiohttp.ClientError as e:
                remaining = deadline - time.monotonic()

                if remaining > 0 and attempt < max_attempts:
                    print(f"[Groq] Connection error -> retrying ({remaining:.1f}s left)")
                    continue

                print(f"[Groq] Connection failed: {type(e).__name__} -> allowing highlight")
                return True

            except Exception as e:
                print(f"[Groq] API error: {type(e).__name__}: {e} -> allowing highlight")
                return True

    async def send_alert(self, user_id_str: str, word: str, message: discord.Message):
        try:
            user = await self.bot.fetch_user(int(user_id_str))
            if not user:
                return

            context_lines = list(self.channel_history.get(message.channel.id, []))
            user_prefs = self.highlights.get(user_id_str, {})
            context_words = user_prefs.get("context_words", set())

            pattern = None
            if word in user_prefs.get("wildcard", []):
                regex_body = re.escape(word).replace(r'\*', r'\w*').replace(r'\?', r'\w')
                pattern = re.compile(r'(?i)\b' + regex_body + r'\b')
            elif word in user_prefs.get("strict", []):
                pattern = re.compile(r'(?i)\b' + re.escape(word) + r'\b')
            else:
                pattern = re.compile(r'(?i)' + re.escape(word))

            highlighted_lines = []
            for line in context_lines:
                parts = line.split(":** ", 1)
                if len(parts) == 2:
                    prefix = parts[0] + ":** "
                    content = parts[1]
                    if pattern:
                        content = pattern.sub(r'`\g<0>`', content)
                    highlighted_lines.append(prefix + content)
                else:
                    highlighted_lines.append(line)

            formatted_context = "\n".join(highlighted_lines)

            is_context_word = any(
                word.lower() == context_word.lower()
                or word.lower() == f"*{context_word.lower()}*"
                for context_word in context_words
            )

            if is_context_word:
                is_valid_target = await self.verify_context_with_gemini(word, formatted_context)
                if not is_valid_target:
                    return

            embed = discord.Embed(
                title=f"Highlight word \"{word}\"",
                description=formatted_context,
                color=discord.Color.gold(),
                timestamp=message.created_at
            )

            embed.add_field(name="Source message", value=f"[Jump to]({message.jump_url})", inline=False)
            embed.set_footer(text="Triggered")

            ping_content = f"{user.mention} | In **{message.guild.name}** › {message.channel.mention}"
            await user.send(content=ping_content, embed=embed)

        except discord.Forbidden:
            pass
        except Exception as e:
            print(f"[Highlight] Alert transmission failed: {e}")


async def setup(bot):
    await bot.add_cog(HighlightCog(bot))
