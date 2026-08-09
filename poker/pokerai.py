import discord
from discord import app_commands
from discord.ext import commands
import json
import asyncio
import time
import urllib.request
import urllib.error
from treys import Card, Evaluator

import config
from . import card_images

evaluator = Evaluator()

SUIT_EMOJI = {"s": "♠️", "h": "♥️", "d": "♦️", "c": "♣️"}
STREET_COLOR = {
    0: 0x36393F,  # Preflop
    1: 0x1F8B4C,  # Flop
    2: 0xE67E22,  # Turn
    3: 0xE74C3C,  # River
    4: 0xF1C40F,  # Showdown/Completed
}
STREET_LABEL = {
    0: "🃏 Pre-Flop",
    1: "🌊 Flop",
    2: "↩️ Turn",
    3: "🏁 River",
    4: "🏁 Completed",
}

def format_card_str(c: str) -> str:
    if not c or len(c) < 2:
        return c
    rank = c[0]
    suit = SUIT_EMOJI.get(c[1], c[1])
    return f"{rank}{suit}"

def format_cards_list(cards: list[str]) -> str:
    return "  ".join(format_card_str(c) for c in cards)


# ── SLUMBOT CLIENT ────────────────────────────────────────────────────────────
class SlumbotClient:
    HOST = "slumbot.com"
    
    @staticmethod
    def _post(url: str, payload: dict) -> dict:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status == 200:
                    return json.loads(response.read().decode("utf-8"))
                raise Exception(f"API request failed with status {response.status}")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8")
            raise Exception(f"HTTP Error {e.code}: {err_body}")
        except Exception as e:
            raise Exception(f"Connection error: {e}")

    @staticmethod
    def login(username, password) -> str:
        url = f"https://{SlumbotClient.HOST}/api/login"
        payload = {"username": username, "password": password}
        data = SlumbotClient._post(url, payload)
        if "error_msg" in data:
            raise Exception(data["error_msg"])
        token = data.get("token")
        if token:
            return token
        raise Exception("Login response did not contain a token")

    @staticmethod
    def new_hand(token: str) -> dict:
        url = f"https://{SlumbotClient.HOST}/api/new_hand"
        payload = {"token": token}
        res = SlumbotClient._post(url, payload)
        if "error_msg" in res:
            raise Exception(res["error_msg"])
        return res

    @staticmethod
    def act(token: str, action: str) -> dict:
        url = f"https://{SlumbotClient.HOST}/api/act"
        payload = {"token": token, "incr": action}
        res = SlumbotClient._post(url, payload)
        if "error_msg" in res:
            raise Exception(res["error_msg"])
        return res


# ── AI SESSION STATE ──────────────────────────────────────────────────────────
class AISession:
    def __init__(self, user_id: int, token: str, state: dict):
        self.user_id = user_id
        self.token = token
        self.hand_num = 1
        self.session_balance = 20000
        self.total_session_winnings = 0
        self.hand_msg = None
        self.last_activity = time.time()
        self.winnings = None
        self.eff_stack = 20000
        self.update_state(state)

    def update_state(self, state: dict):
        if self.winnings is None:
            self.eff_stack = min(self.session_balance, 40000 - self.session_balance)

        self.action_history = state.get("action", "")
        self.board = state.get("board", [])
        self.hole_cards = state.get("hole_cards", [])
        self.client_pos = state.get("client_pos", 0)
        
        new_winnings = state.get("winnings")
        if new_winnings is not None and self.winnings is None:
            capped_winnings = max(-self.eff_stack, min(self.eff_stack, new_winnings))
            self.session_balance += capped_winnings
            self.total_session_winnings += capped_winnings
            
        self.winnings = new_winnings
        self.won_pot = state.get("won_pot")
        self.bot_hole_cards = state.get("bot_hole_cards")
        
        new_token = state.get("token")
        if new_token:
            self.token = new_token
            
        self.last_activity = time.time()

    def parse_state(self) -> dict:
        st = 0
        total_pots = 0
        bb = min(100, self.eff_stack)
        sb = min(50, self.eff_stack)
        bets = {0: bb, 1: sb}
        spent = {0: bb, 1: sb}
        pos = 1  # Player 1 (Dealer/SB) acts first preflop
        
        parts = self.action_history.split('/')
        for idx, street_actions in enumerate(parts):
            if idx > 0:
                total_pots += bets[0] + bets[1]
                bets = {0: 0, 1: 0}
                st = idx
                pos = 0  # Player 0 acts first postflop
                
            i = 0
            while i < len(street_actions):
                c = street_actions[i]
                i += 1
                if c == 'k':
                    pos = 1 - pos
                elif c == 'c':
                    max_bet = max(bets[0], bets[1])
                    diff = max_bet - bets[pos]
                    diff = max(0, min(diff, self.eff_stack - spent[pos]))
                    spent[pos] += diff
                    bets[pos] += diff
                    pos = 1 - pos
                elif c == 'f':
                    pos = -1
                elif c == 'b':
                    j = i
                    while i < len(street_actions) and street_actions[i].isdigit():
                        i += 1
                    val = int(street_actions[j:i])
                    max_allowed = self.eff_stack - (spent[pos] - bets[pos])
                    val = max(0, min(val, max_allowed))
                    diff = val - bets[pos]
                    spent[pos] += diff
                    bets[pos] = val
                    pos = 1 - pos
                    
        return {
            "st": st,
            "bets": bets,
            "spent": spent,
            "total_pots": total_pots,
            "pos": pos,
        }

    def get_readable_log(self) -> list[str]:
        log_lines = []
        
        bb = min(100, self.eff_stack)
        sb = min(50, self.eff_stack)
        
        if self.client_pos == 0:
            log_lines.append(f"👤 You posted big blind ({bb})")
            log_lines.append(f"🤖 Skymax posted small blind ({sb})")
        else:
            log_lines.append(f"🤖 Skymax posted big blind ({bb})")
            log_lines.append(f"👤 You posted small blind ({sb})")

        st = 0
        bets = {0: bb, 1: sb}
        spent = {0: bb, 1: sb}
        pos = 1
        
        street_names = {1: "FLOP", 2: "TURN", 3: "RIVER"}
        parts = self.action_history.split('/')
        
        for idx, street_actions in enumerate(parts):
            if idx > 0:
                log_lines.append(f"─── {street_names.get(idx, 'NEXT STREET')} ───")
                bets = {0: 0, 1: 0}
                st = idx
                pos = 0
                
            i = 0
            while i < len(street_actions):
                c = street_actions[i]
                i += 1
                actor = "You" if pos == self.client_pos else "Skymax"
                actor_emoji = "👤" if pos == self.client_pos else "🤖"
                
                if c == 'k':
                    log_lines.append(f"{actor_emoji} {actor} checked")
                    pos = 1 - pos
                elif c == 'c':
                    max_bet = max(bets[0], bets[1])
                    diff = max_bet - bets[pos]
                    diff = max(0, min(diff, self.eff_stack - spent[pos]))
                    spent[pos] += diff
                    bets[pos] += diff
                    log_lines.append(f"{actor_emoji} {actor} called ({bets[pos]})")
                    pos = 1 - pos
                elif c == 'f':
                    log_lines.append(f"{actor_emoji} {actor} folded ❌")
                    pos = -1
                elif c == 'b':
                    j = i
                    while i < len(street_actions) and street_actions[i].isdigit():
                        i += 1
                    val = int(street_actions[j:i])
                    max_allowed = self.eff_stack - (spent[pos] - bets[pos])
                    val = max(0, min(val, max_allowed))
                    
                    other_bet = bets[1 - pos]
                    if other_bet == 0:
                        log_lines.append(f"{actor_emoji} {actor} bet {val}")
                    else:
                        log_lines.append(f"{actor_emoji} {actor} raised to {val}")
                    
                    diff = val - bets[pos]
                    spent[pos] += diff
                    bets[pos] = val
                    pos = 1 - pos
                    
        return log_lines

    def check_down_if_needed(self):
        p = self.parse_state()
        loop_count = 0
        while self.winnings is None and p["spent"][0] >= self.eff_stack and p["spent"][1] >= self.eff_stack:
            loop_count += 1
            if loop_count > 15:
                break
            last_char = self.action_history[-1] if self.action_history else ""
            auto_action = "c" if last_char.isdigit() else "k"
            state = SlumbotClient.act(self.token, auto_action)
            self.update_state(state)
            p = self.parse_state()


# ── DISCORD EMBED BUILDER ─────────────────────────────────────────────────────
async def build_ai_embed(session: AISession) -> tuple[discord.Embed, discord.File | None]:
    p = session.parse_state()
    st = p["st"]
    bets = p["bets"]
    spent = p["spent"]
    pos = p["pos"]
    
    emoji = config.TOURNAMENT_CHIP_EMOJI
    
    if session.winnings is not None:
        color = STREET_COLOR[4]
        label = STREET_LABEL[4]
    else:
        color = STREET_COLOR.get(st, 0x36393F)
        label = STREET_LABEL.get(st, "")

    title = f"🃏 Heads-Up vs Skymax  ·  Hand #{session.hand_num}"
    embed = discord.Embed(title=title, color=color)
    embed.set_footer(text=f"{label}")
    if session.winnings is None:
        embed.description = "💡 *Skymax plays GTO. Use standard sizing (e.g. 2x open-raise, 1/3 to 1/2 pot) to get more action!*"

    board_file = None
    if session.board:
        board_ints = [Card.new(c) for c in session.board]
        backs = max(0, 5 - len(board_ints))
        board_file = await asyncio.to_thread(card_images.make_strip, board_ints, backs)
        embed.set_image(url="attachment://cards.png")

    if session.winnings is not None:
        embed.description = "Hand completed!"
        
        card_text = f"**Your cards:** {format_cards_list(session.hole_cards)}"
        if session.bot_hole_cards:
            card_text += f"\n**Skymax's cards:** {format_cards_list(session.bot_hole_cards)}"
            if session.board:
                try:
                    p_cards = [Card.new(c) for c in session.hole_cards]
                    b_cards = [Card.new(c) for c in session.bot_hole_cards]
                    board_cards = [Card.new(c) for c in session.board]
                    
                    p_score = evaluator.evaluate(p_cards, board_cards)
                    b_score = evaluator.evaluate(b_cards, board_cards)
                    
                    p_rank = evaluator.class_to_string(evaluator.get_rank_class(p_score))
                    b_rank = evaluator.class_to_string(evaluator.get_rank_class(b_score))
                    
                    card_text += f"\n*(Your Hand: {p_rank} | Skymax Hand: {b_rank})*"
                except Exception:
                    pass
        else:
            card_text += f"\n**AI's cards:** Mucked/Folded 🗑️"
            
        embed.add_field(name="🃏 Cards Revealed", value=card_text, inline=False)
        
        pot = session.won_pot if session.won_pot is not None else (spent[0] + spent[1])
        pot = min(pot, 2 * session.eff_stack)
        winnings = session.winnings
        
        result_text = f"**Pot size:** {pot:,} {emoji}\n"
        if winnings > 0:
            result_text += f"🎉 **You won +{winnings:,} {emoji}!**"
        elif winnings < 0:
            result_text += f"💸 **Skymax won +{-winnings:,} {emoji}!**"
        else:
            result_text += f"🤝 **Split pot!**"
            
        result_text += f"\n**New Session Balance:** {session.session_balance:,} {emoji}"
        
        embed.add_field(name="🏆 Winnings", value=result_text, inline=False)
        if session.session_balance <= 0:
            embed.add_field(name="💀 Game Over", value="You went completely broke! Session ended.", inline=False)
        elif session.session_balance >= 40000:
            embed.add_field(name="🏆 Victory!", value="You won all the chips and defeated Skymax! Session ended.", inline=False)
    else:
        user_mention = f"<@{session.user_id}>"
        player_dealer = " 🔘" if session.client_pos == 1 else ""
        ai_dealer = " 🔘" if session.client_pos == 0 else ""
        
        player_stack = session.session_balance - spent[session.client_pos]
        ai_stack = (40000 - session.session_balance) - spent[1 - session.client_pos]
        
        if pos == session.client_pos:
            player_status = f"acting (bet {bets[session.client_pos]:,})" if bets[session.client_pos] else "acting"
            ai_status = f"bet {bets[1 - session.client_pos]:,}" if bets[1 - session.client_pos] else "—"
        else:
            player_status = f"bet {bets[session.client_pos]:,}" if bets[session.client_pos] else "—"
            ai_status = "acting"
            
        player_line = f"{user_mention} **{player_stack:,} {emoji}** — {player_status}{player_dealer}"
        ai_line = f"🤖 **Skymax** **{ai_stack:,} {emoji}** — {ai_status}{ai_dealer}"
        
        embed.add_field(name="Players", value=f"{player_line}\n{ai_line}", inline=False)

    log_lines = session.get_readable_log()
    if log_lines:
        embed.add_field(name="This round", value="\n".join(log_lines[-8:]), inline=False)

    if session.winnings is None:
        pot = spent[0] + spent[1]
        pot_info = f"**Pot:** {pot:,} {emoji}"
        if max(bets.values()) > 0:
            pot_info += f"  ·  **Bet:** {max(bets.values()):,}"
        if pos == session.client_pos:
            pot_info += f"\n⬅️ **Your turn**"
        else:
            pot_info += f"\n⬅️ **Skymax's turn**"
        embed.add_field(name="\u200b", value=pot_info, inline=False)

    return embed, board_file


# ── DISCORD UI VIEWS ──────────────────────────────────────────────────────────

class AIQuitConfirmView(discord.ui.View):
    def __init__(self, session: AISession, cog_sessions: dict, main_view: discord.ui.View):
        super().__init__(timeout=60)
        self.session = session
        self.cog_sessions = cog_sessions
        self.main_view = main_view

    @discord.ui.button(label="Yes, Quit", style=discord.ButtonStyle.red)
    async def btn_yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("❌ This is not your game.", ephemeral=True)
            return
            
        await interaction.response.defer()
        
        if self.session.user_id in self.cog_sessions:
            del self.cog_sessions[self.session.user_id]
            
        embed = discord.Embed(
            title="🏁 Session Ended",
            description=f"You retired from the match.\n\n"
                        f"**Hands played:** {self.session.hand_num}\n"
                        f"**Net Winnings:** {self.session.total_session_winnings:,} {config.TOURNAMENT_CHIP_EMOJI}\n"
                        f"**Final Balance:** {self.session.session_balance:,} {config.TOURNAMENT_CHIP_EMOJI}",
            color=0x95a5a6
        )
        self.main_view.stop()
        self.stop()
        
        try:
            await self.session.hand_msg.edit(embed=embed, view=None, attachments=[])
        except Exception:
            pass
            
        await interaction.edit_original_response(content="👋 Session quit successfully.", view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.grey)
    async def btn_no(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_original_response(content="Quit cancelled.", view=None)
        self.stop()


class AIGameView(discord.ui.View):
    def __init__(self, session: AISession, cog_sessions: dict):
        super().__init__(timeout=300)
        self.session = session
        self.cog_sessions = cog_sessions
        self.update_buttons()

    async def on_timeout(self):
        embed = discord.Embed(
            title="🏁 Session Ended (Inactivity)",
            description=f"This session expired due to inactivity.\n\n"
                        f"**Hands played:** {self.session.hand_num}\n"
                        f"**Net Winnings:** {self.session.total_session_winnings:,} {config.TOURNAMENT_CHIP_EMOJI}\n"
                        f"**Final Balance:** {self.session.session_balance:,} {config.TOURNAMENT_CHIP_EMOJI}",
            color=0x95a5a6
        )
        try:
            if self.session.hand_msg:
                await self.session.hand_msg.edit(embed=embed, view=None, attachments=[])
        except Exception:
            pass
        if self.session.user_id in self.cog_sessions:
            del self.cog_sessions[self.session.user_id]

    def update_buttons(self):
        p = self.session.parse_state()
        bets = p["bets"]
        spent = p["spent"]
        c_pos = self.session.client_pos
        
        call_amt = bets[1 - c_pos] - bets[c_pos]
        
        if call_amt > 0:
            self.btn_check_call.label = f"Call {call_amt}"
            self.btn_check_call.style = discord.ButtonStyle.green
        else:
            self.btn_check_call.label = "Check"
            self.btn_check_call.style = discord.ButtonStyle.blurple
            
        player_remaining = self.session.session_balance - (spent[c_pos] - bets[c_pos])
        ai_remaining = (40000 - self.session.session_balance) - (spent[1 - c_pos] - bets[1 - c_pos])
        max_raise_to = min(player_remaining, ai_remaining)
        
        if max_raise_to <= bets[1 - c_pos]:
            self.btn_raise.disabled = True
        else:
            self.btn_raise.disabled = False

    async def _act_and_update(self, interaction: discord.Interaction, action: str):
        is_main = (self.session.hand_msg and interaction.message and interaction.message.id == self.session.hand_msg.id)
        
        if is_main:
            await interaction.response.defer()
        else:
            await interaction.response.defer(ephemeral=True)
        
        try:
            state = SlumbotClient.act(self.session.token, action)
            self.session.update_state(state)
            self.session.check_down_if_needed()
            
            embed, file = await build_ai_embed(self.session)
            view = get_session_view(self.session, self.cog_sessions)
            
            if self.session.winnings is not None and (self.session.session_balance <= 0 or self.session.session_balance >= 40000):
                if self.session.user_id in self.cog_sessions:
                    del self.cog_sessions[self.session.user_id]
                
            self.stop()
            
            if is_main:
                msg = await interaction.edit_original_response(embed=embed, view=view, attachments=([file] if file else []))
            else:
                # Update the ephemeral picker to clear buttons
                await interaction.edit_original_response(content="✅ Raise submitted!", view=None)
                # Edit the main board message
                msg = await self.session.hand_msg.edit(embed=embed, view=view, attachments=([file] if file else []))
                
            try:
                self.session.hand_msg = await interaction.channel.fetch_message(msg.id)
            except Exception:
                self.session.hand_msg = msg
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error playing action: {e}", ephemeral=True)

    @discord.ui.button(label="Check", style=discord.ButtonStyle.blurple, row=0)
    async def btn_check_call(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("❌ This is not your game.", ephemeral=True)
            return
        
        p = self.session.parse_state()
        bets = p["bets"]
        c_pos = self.session.client_pos
        call_amt = bets[1 - c_pos] - bets[c_pos]
        
        action = "c" if call_amt > 0 else "k"
        await self._act_and_update(interaction, action)

    @discord.ui.button(label="Raise", style=discord.ButtonStyle.green, row=0)
    async def btn_raise(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("❌ This is not your game.", ephemeral=True)
            return
            
        picker = AIRaisePickerView(self.session, self)
        await interaction.response.send_message(
            content=f"Choose a raise sizing (Pot: {self.session.parse_state()['bets'][0]+self.session.parse_state()['bets'][1]+self.session.parse_state()['total_pots']:,} chips):",
            view=picker,
            ephemeral=True
        )

    @discord.ui.button(label="Fold", style=discord.ButtonStyle.red, row=0)
    async def btn_fold(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("❌ This is not your game.", ephemeral=True)
            return
        await self._act_and_update(interaction, "f")

    @discord.ui.button(label="My Cards", style=discord.ButtonStyle.grey, row=1)
    async def btn_cards(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("❌ This is not your game.", ephemeral=True)
            return
            
        await interaction.response.defer(ephemeral=True)
        try:
            hole_ints = [Card.new(c) for c in self.session.hole_cards]
            file = await asyncio.to_thread(card_images.make_strip, hole_ints, 0, True)
            await interaction.followup.send(content="Your hole cards:", file=file, ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Error displaying cards: {e}", ephemeral=True)

    @discord.ui.button(label="Quit", style=discord.ButtonStyle.grey, row=1)
    async def btn_quit(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("❌ This is not your game.", ephemeral=True)
            return
            
        confirm_view = AIQuitConfirmView(self.session, self.cog_sessions, self)
        await interaction.response.send_message(
            content="⚠️ **Are you sure you want to quit the poker session?** This will end the match immediately.",
            view=confirm_view,
            ephemeral=True
        )


# ── RAISE PICKER VIEW ─────────────────────────────────────────────────────────
class AIRaisePickerView(discord.ui.View):
    def __init__(self, session: AISession, game_view: AIGameView):
        super().__init__(timeout=120)
        self.session = session
        self.game_view = game_view
        self.calculate_options()

    def calculate_options(self):
        p = self.session.parse_state()
        bets = p["bets"]
        spent = p["spent"]
        c_pos = self.session.client_pos
        
        pot = spent[0] + spent[1]
        call_amt = bets[1 - c_pos] - bets[c_pos]
        call_pot = pot + call_amt
        
        self.min_raise_to = bets[1 - c_pos] + max(100, bets[1 - c_pos] - bets[c_pos])
        player_remaining = self.session.session_balance - (spent[c_pos] - bets[c_pos])
        ai_remaining = (40000 - self.session.session_balance) - (spent[1 - c_pos] - bets[1 - c_pos])
        self.max_raise_to = min(player_remaining, ai_remaining)
        
        self.min_raise_to = min(self.min_raise_to, self.max_raise_to)
        self.third_pot = max(self.min_raise_to, min(bets[1 - c_pos] + call_pot // 3, self.max_raise_to))
        self.half_pot = max(self.min_raise_to, min(bets[1 - c_pos] + call_pot // 2, self.max_raise_to))
        self.pot_size = max(self.min_raise_to, min(bets[1 - c_pos] + call_pot, self.max_raise_to))

    async def _do_raise(self, interaction: discord.Interaction, raise_to: int):
        if raise_to >= self.max_raise_to:
            raise_to = self.max_raise_to
        else:
            raise_to = max(self.min_raise_to, raise_to)
        action = f"b{raise_to}"
        await self.game_view._act_and_update(interaction, action)
        self.stop()

    @discord.ui.button(label="Min Raise", style=discord.ButtonStyle.green)
    async def btn_min(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._do_raise(interaction, self.min_raise_to)

    @discord.ui.button(label="1/3 Pot", style=discord.ButtonStyle.green)
    async def btn_third(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._do_raise(interaction, self.third_pot)

    @discord.ui.button(label="1/2 Pot", style=discord.ButtonStyle.green)
    async def btn_half(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._do_raise(interaction, self.half_pot)

    @discord.ui.button(label="Pot Size", style=discord.ButtonStyle.green)
    async def btn_pot(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._do_raise(interaction, self.pot_size)

    @discord.ui.button(label="All-In", style=discord.ButtonStyle.red)
    async def btn_all_in(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._do_raise(interaction, self.max_raise_to)

    @discord.ui.button(label="Custom...", style=discord.ButtonStyle.grey)
    async def btn_custom(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AICustomRaiseModal(self))


# ── CUSTOM RAISE MODAL ────────────────────────────────────────────────────────
class AICustomRaiseModal(discord.ui.Modal, title="Custom Raise"):
    amount = discord.ui.TextInput(label="Total bet size on this street?", placeholder="e.g. 500, 1.2k")

    def __init__(self, picker_view: AIRaisePickerView):
        super().__init__()
        self.picker_view = picker_view

    async def on_submit(self, interaction: discord.Interaction):
        import poker
        val = poker.parse_chips(self.amount.value)
        if val is None or val <= 0:
            await interaction.response.send_message("❌ Enter a valid chip amount (e.g. 500, 2k).", ephemeral=True)
            return
            
        min_to = self.picker_view.min_raise_to
        max_to = self.picker_view.max_raise_to
        
        if min_to > max_to:
            if val != max_to:
                await interaction.response.send_message(f"❌ You are short-stacked and can only raise to {max_to:,} chips (All-In).", ephemeral=True)
                return
        else:
            if val < min_to:
                await interaction.response.send_message(f"❌ Minimum raise-to amount is {min_to:,} chips.", ephemeral=True)
                return
            if val > max_to:
                await interaction.response.send_message(f"❌ Maximum raise-to amount is {max_to:,} chips (your stack).", ephemeral=True)
                return
            
        await self.picker_view._do_raise(interaction, val)


# ── GAME OVER VIEW ────────────────────────────────────────────────────────────
class AIGameOverView(discord.ui.View):
    def __init__(self, session: AISession, cog_sessions: dict):
        super().__init__(timeout=300)
        self.session = session
        self.cog_sessions = cog_sessions

    async def on_timeout(self):
        embed = discord.Embed(
            title="🏁 Session Ended (Inactivity)",
            description=f"This session expired due to inactivity.\n\n"
                        f"**Hands played:** {self.session.hand_num}\n"
                        f"**Net Winnings:** {self.session.total_session_winnings:,} {config.TOURNAMENT_CHIP_EMOJI}\n"
                        f"**Final Balance:** {self.session.session_balance:,} {config.TOURNAMENT_CHIP_EMOJI}",
            color=0x95a5a6
        )
        try:
            if self.session.hand_msg:
                await self.session.hand_msg.edit(embed=embed, view=None, attachments=[])
        except Exception:
            pass
        if self.session.user_id in self.cog_sessions:
            del self.cog_sessions[self.session.user_id]

    @discord.ui.button(label="Next Hand ➡️", style=discord.ButtonStyle.green)
    async def btn_next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("❌ This is not your game.", ephemeral=True)
            return
            
        await interaction.response.defer()
        try:
            state = SlumbotClient.new_hand(self.session.token)
            self.session.hand_num += 1
            self.session.winnings = None
            self.session.update_state(state)
            self.session.check_down_if_needed()
            
            embed, file = await build_ai_embed(self.session)
            view = get_session_view(self.session, self.cog_sessions)
            
            if self.session.winnings is not None and (self.session.session_balance <= 0 or self.session.session_balance >= 40000):
                if self.session.user_id in self.cog_sessions:
                    del self.cog_sessions[self.session.user_id]
            
            self.stop()
            msg = await interaction.edit_original_response(embed=embed, view=view, attachments=([file] if file else []))
            try:
                self.session.hand_msg = await interaction.channel.fetch_message(msg.id)
            except Exception:
                self.session.hand_msg = msg
        except Exception as e:
            await interaction.followup.send(f"❌ Error starting next hand: {e}", ephemeral=True)

    @discord.ui.button(label="Quit Session", style=discord.ButtonStyle.red)
    async def btn_quit(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("❌ This is not your game.", ephemeral=True)
            return
            
        confirm_view = AIQuitConfirmView(self.session, self.cog_sessions, self)
        await interaction.response.send_message(
            content="⚠️ **Are you sure you want to quit the poker session?** This will end the match immediately.",
            view=confirm_view,
            ephemeral=True
        )


def get_session_view(session: AISession, cog_sessions: dict) -> discord.ui.View | None:
    if session.winnings is not None:
        if session.session_balance <= 0 or session.session_balance >= 40000:
            return None
        return AIGameOverView(session, cog_sessions)
    return AIGameView(session, cog_sessions)


# ── COG IMPLEMENTATION ────────────────────────────────────────────────────────
class PokerAICog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        if hasattr(bot, "poker_ai_sessions"):
            self.sessions = bot.poker_ai_sessions
        else:
            self.sessions = {}
            bot.poker_ai_sessions = self.sessions

    advtut = app_commands.Group(name="advtut", description="Poker Heads-Up vs Skymax")

    @advtut.command(name="start", description="Start a Heads-Up poker game against Skymax")
    async def create(self, interaction: discord.Interaction):
        uid = interaction.user.id
        
        if uid in self.sessions:
            await interaction.response.send_message(
                f"❌ You already have an active game session! Play there or close it first.",
                ephemeral=True
            )
            return
            
        if len(self.sessions) >= 5:
            await interaction.response.send_message(
                "❌ All bot tables are currently full! (Limit: 5 active games). Please try again in a few minutes.",
                ephemeral=True
            )
            return
            
        await interaction.response.defer()
        
        try:
            token = SlumbotClient.login("cfr_abstraction", "cfr_abstraction")
            state = SlumbotClient.new_hand(token)
            
            session = AISession(uid, token, state)
            session.check_down_if_needed()
            self.sessions[uid] = session
            
            embed, file = await build_ai_embed(session)
            view = get_session_view(session, self.sessions)
            
            if session.winnings is not None and (session.session_balance <= 0 or session.session_balance >= 40000):
                if uid in self.sessions:
                    del self.sessions[uid]
                    
            kwargs = {"embed": embed, "view": view}
            if file:
                kwargs["file"] = file
            msg = await interaction.followup.send(**kwargs)
            try:
                session.hand_msg = await interaction.channel.fetch_message(msg.id)
            except Exception:
                session.hand_msg = msg
            
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to start game: {e}")

async def setup(bot):
    await bot.add_cog(PokerAICog(bot))
