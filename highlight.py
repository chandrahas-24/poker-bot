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
COOLDOWN_SECONDS = 10        # No spam hl
ACTIVE_CONTEXT_SECONDS = 15  # User won't get pinged if they recently talked here

hl_allowed = {}          # User IDs
HIGHLIGHT_ROLE_IDS = [836210264873369630]  # Role IDs

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
                        "context_word": None,
                        "blocked_channels": set(),
                        "blocked_users": set()
                    }
                else:
                    migrated_data[user_id_str] = {
                        "strict": set(user_data.get("strict", [])),
                        "general": set(user_data.get("general", [])),
                        "wildcard": set(user_data.get("wildcard", [])),
                        "context_word": user_data.get("context_word", None),
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
                    "context_word": data.get("context_word"),
                    "blocked_channels": list(data["blocked_channels"]),
                    "blocked_users": list(data["blocked_users"])
                }
            json.dump(json_data, f, indent=4)

    def _ensure_user(self, user_id: str):
        if user_id not in self.highlights:
            self.highlights[user_id] = {
                "strict": set(), "general": set(), "wildcard": set(), "context_word": None, "blocked_channels": set(),
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
            if self.highlights[user_id].get("context_word") == word:
                self.highlights[user_id]["context_word"] = None

            self.save_data()
            embed = discord.Embed(title="🗑️ Keyword Removed", description=f"I have stopped tracking **{word}**.",
                                  color=discord.Color.orange())
        else:
            embed = discord.Embed(title="⚠️ Not Found", description=f"You are not tracking **{word}**.",
                                  color=discord.Color.red())

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @highlight_group.command(name="set_context", description="Set your ONE allowed context-aware highlight word.")
    @app_commands.describe(word="The exact word you want the AI to verify")
    async def setai(self, interaction: discord.Interaction, word: str):
        if not await is_hl_authorized(interaction):
            await interaction.response.send_message("❌ This command is restricted.", ephemeral=True)
            return

        clean_word = word.strip().lower()
        user_id = str(interaction.user.id)
        self._ensure_user(user_id)

        self.highlights[user_id]["context_word"] = clean_word

        wildcard_version = f"*{clean_word}*"
        if wildcard_version not in self.highlights[user_id]["wildcard"]:
            self.highlights[user_id]["wildcard"].add(wildcard_version)

        self.save_data()
        await interaction.response.send_message(
            f"✅ **{clean_word}** is now your context-verified word. (Added as `*{clean_word}*` to catch stretched text!)",
            ephemeral=True
        )

    @highlight_group.command(name="remove_context", description="Remove your context specific highlight word.")
    async def removeai(self, interaction: discord.Interaction):
        if not await is_hl_authorized(interaction):
            await interaction.response.send_message("❌ This command is restricted.", ephemeral=True)
            return

        user_id = str(interaction.user.id)
        self._ensure_user(user_id)

        old_word = self.highlights[user_id].get("context_word")
        if old_word:
            self.highlights[user_id]["context_word"] = None

            wildcard_version = f"*{old_word}*"
            if wildcard_version in self.highlights[user_id]["wildcard"]:
                self.highlights[user_id]["wildcard"].remove(wildcard_version)

            self.save_data()
            await interaction.response.send_message(f"❌ Removed your highlight word (**{old_word}**).", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ You don't have a context highlight word set.", ephemeral=True)

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

        ai_word = data.get("context_word") or "None"
        embed.add_field(name="Context Word", value=f"`{ai_word}`", inline=False)

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
            "include_reasoning": False
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
                print(f"[Groq] Attempt {attempt}/{max_attempts}")

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

                    print(f"[Groq] Response: {reply!r}")

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
            designated_ai_word = user_prefs.get("context_word")

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

            if designated_ai_word and (
                    word.lower() == designated_ai_word.lower() or word.lower() == f"*{designated_ai_word.lower()}*"):
                is_valid_target = await self.verify_context_with_gemini(word, formatted_context)
                if not is_valid_target:
                    return
            else:
                pass

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
