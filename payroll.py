"""Weekly dealer payroll.

Every Monday 11:00 (config.PAYROLL_TZ, default Asia/Kolkata) the bot totals house revenue per game since the
previous run, takes DEALER_PAY_RATE of each game's revenue and splits it by that game's dealer minutes
(rounded UP per dealer). Poker and UNO pools are separate and paid in their own chips. The report goes to
config.PAYROLL_CHANNEL_ID with Approve / Adjust / Reject buttons. Approval credits poker wallets atomically
(poker.database.pay_payroll_run), then UNO wallets (uno.database.credit_payroll, idempotent + auto-retried).
"""
import asyncio
import calendar
import datetime as dt
import math
import re
import traceback
from fractions import Fraction
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands, tasks

import config
from poker import database as pdb
from uno import database as udb


def _rate() -> Fraction:
    return Fraction(str(getattr(config, "DEALER_PAY_RATE", 0.35)))


def _utcnow() -> dt.datetime:
    return dt.datetime.utcnow()


def last_boundary(now_utc: dt.datetime) -> dt.datetime:
    """Most recent Monday 11:00 in PAYROLL_TZ at or before now, as naive UTC."""
    tz = ZoneInfo(getattr(config, "PAYROLL_TZ", "Asia/Kolkata"))
    local = now_utc.replace(tzinfo=dt.timezone.utc).astimezone(tz)
    cand = (local - dt.timedelta(days=local.weekday())).replace(hour=11, minute=0, second=0, microsecond=0)
    if cand > local:
        cand -= dt.timedelta(days=7)
    return cand.astimezone(dt.timezone.utc).replace(tzinfo=None)


def _is_admin(user) -> bool:
    if user.id in config.DEV_USER_IDS:
        return True
    perms = getattr(user, "guild_permissions", None)
    if perms and perms.administrator:
        return True
    role_id = getattr(config, "PAYOUT_MANAGER_ROLE", None)
    return bool(role_id and any(r.id == int(role_id) for r in getattr(user, "roles", [])))


# ── Calculation ──────────────────────────────────────────────────────────────

async def compute(start: dt.datetime, end: dt.datetime) -> dict:
    p_rev = await pdb.get_revenue_between(start, end)
    u_rev = await udb.get_revenue_between(start, end)
    people: dict[int, dict] = {}
    for key, rows in (("p_sec", await pdb.get_dealer_minutes_between(start, end)),
                      ("u_sec", await udb.get_dealer_minutes_between(start, end))):
        for r in rows:
            e = people.setdefault(r["dealer_id"], {"user_id": r["dealer_id"], "name": r["dealer_name"],
                                                   "p_sec": 0, "u_sec": 0})
            e[key] += int(r["seconds"])
            e["name"] = r["dealer_name"]
    pool_p = _rate() * max(0, p_rev)
    pool_u = _rate() * max(0, u_rev)
    tot_p = sum(e["p_sec"] for e in people.values())
    tot_u = sum(e["u_sec"] for e in people.values())
    items = []
    for e in people.values():
        if e["p_sec"] + e["u_sec"] <= 0:
            continue
        p_base = math.ceil(pool_p * e["p_sec"] / tot_p) if tot_p > 0 and pool_p > 0 else 0
        u_base = math.ceil(pool_u * e["u_sec"] / tot_u) if tot_u > 0 and pool_u > 0 else 0
        items.append({**e, "p_base": p_base, "u_base": u_base, "p_adj": None, "u_adj": None})
    items.sort(key=lambda x: -(x["p_sec"] + x["u_sec"]))
    return {"p_rev": p_rev, "u_rev": u_rev, "pool_p": math.ceil(pool_p), "pool_u": math.ceil(pool_u),
            "rate": float(_rate()), "items": items}


def _norm_db_items(db_items: list[dict]) -> list[dict]:
    return [{"user_id": i["user_id"], "name": i["user_name"], "p_sec": i["poker_seconds"], "u_sec": i["uno_seconds"],
             "p_base": i["base_amount"], "p_adj": i["adjusted_amount"],
             "u_base": i["uno_amount"], "u_adj": i["adj_uno"]} for i in db_items]


async def settle_uno(run_id: int) -> bool:
    """Credit the UNO half of an approved run. Idempotent (per run+user in uno.db); safe to call repeatedly."""
    try:
        got = await pdb.get_payroll_run(run_id)
        if not got:
            return False
        run, db_items = got
        if run["status"] != "approved" or run["uno_paid"]:
            return True
        payees = []
        for it in _norm_db_items(db_items):
            amt = it["u_adj"] if it["u_adj"] is not None else it["u_base"]
            if amt > 0:
                payees.append({"user_id": it["user_id"], "name": it["name"], "amount": amt,
                               "note": pdb._pay_note(run_id, "UNO", it["u_sec"], it["u_base"], it["u_adj"])})
        await udb.credit_payroll(run_id, run["decided_by"], run["decided_name"] or "payroll", payees)
        await pdb.mark_uno_paid(run_id)
        return True
    except Exception:
        traceback.print_exc()
        return False


# ── Rendering ────────────────────────────────────────────────────────────────

def _ts(d: dt.datetime) -> str:
    return f"<t:{calendar.timegm(d.utctimetuple())}:f>"


def fmt_pay(base: int, adj) -> str:
    if adj is None or adj == base:
        return f"{base:,}"
    d = adj - base
    return f"{base:,}{'+' if d > 0 else '-'}{abs(d):,} -> {adj:,}"


def _section(items: list[dict], game: str, pool: int):
    sec, base_k, adj_k = ("p_sec", "p_base", "p_adj") if game == "poker" else ("u_sec", "u_base", "u_adj")
    lines, total = [], 0
    for it in items:
        if not (it[sec] or it[base_k] or it[adj_k] is not None):
            continue
        final = it[adj_k] if it[adj_k] is not None else it[base_k]
        total += final
        pct = f" ({final / pool * 100:.1f}%)" if pool > 0 else ""
        lines.append(f"<@{it['user_id']}> · {it[sec] / 60:.1f} min · **{fmt_pay(it[base_k], it[adj_k])}**{pct}")
    body = "\n".join(lines) if lines else "*No dealer minutes.*"
    if len(body) > 850:
        body = body[:850] + "\n…"
    return f"{body}\nTotal payout **{total:,}**"


def render(run_id, start, end, p_rev, u_rev, pool_p, pool_u, rate, items, status="pending", decided_by=None,
           dry=False, uno_paid=True) -> discord.Embed:
    colors = {"pending": 0xF1C40F, "approved": 0x2ECC71, "rejected": 0xE74C3C, "empty": 0x95A5A6}
    title = "Dealer Payroll" + (" — DRY RUN" if dry else f" — Run #{run_id}")
    e = discord.Embed(title=title, color=0x3498DB if dry else colors.get(status, 0x95A5A6))
    e.add_field(name="Window", value=f"{_ts(start)} → {_ts(end)}", inline=False)
    e.add_field(name=f"🃏 Poker — revenue {max(0, p_rev):,} → pool {pool_p:,} ({rate * 100:g}%)",
                value=_section(items, "poker", pool_p), inline=False)
    e.add_field(name=f"🎴 UNO — revenue {max(0, u_rev):,} → pool {pool_u:,} ({rate * 100:g}%)",
                value=_section(items, "uno", pool_u), inline=False)
    if dry:
        e.set_footer(text="Dry run — nothing saved or paid")
    elif status == "pending":
        e.add_field(name="Status", value="⏳ Pending approval", inline=False)
    elif status == "approved":
        tail = "poker + UNO wallets credited" if uno_paid else "poker credited · ⚠️ UNO credit pending, auto-retrying"
        e.add_field(name="Status", value=f"✅ Approved by <@{decided_by}> — {tail}", inline=False)
    elif status == "rejected":
        e.add_field(name="Status", value=f"❌ Rejected by <@{decided_by}> — nothing paid", inline=False)
    elif status == "empty":
        e.add_field(name="Status", value="Nothing to pay this window.", inline=False)
    return e


def render_run(run: dict, db_items: list[dict]) -> discord.Embed:
    return render(run["id"], dt.datetime.fromisoformat(run["window_start"]), dt.datetime.fromisoformat(run["window_end"]),
                  run["poker_revenue"], run["uno_revenue"], run["pool_poker"], run["pool_uno"], run["rate"],
                  _norm_db_items(db_items), status=run["status"], decided_by=run["decided_by"],
                  uno_paid=bool(run["uno_paid"]))


# ── Buttons (persistent: run is looked up by message id, so no per-run custom_ids) ──

async def _guard(interaction: discord.Interaction):
    if not _is_admin(interaction.user):
        await interaction.response.send_message("❌ Admins only.", ephemeral=True)
        return None
    got = await pdb.get_payroll_run_by_message(interaction.message.id)
    if not got:
        await interaction.response.send_message("❌ This payroll run wasn't found.", ephemeral=True)
        return None
    return got


class AdjustModal(discord.ui.Modal, title="Adjust payout"):
    game = discord.ui.TextInput(label="Game (poker / uno)", default="poker", max_length=5)
    user = discord.ui.TextInput(label="User (ID or @mention)", placeholder="123456789012345678", max_length=60)
    amount = discord.ui.TextInput(label="Final payout (that game's chips)", placeholder="250", max_length=12)

    def __init__(self, message: discord.Message):
        super().__init__()
        self.message = message

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        m = re.search(r"\d{15,20}", self.user.value)
        try:
            final = int(self.amount.value.replace(",", "").strip())
        except ValueError:
            final = -1
        g = self.game.value.strip().lower()
        game = "uno" if g.startswith("u") else "poker" if g.startswith("p") else None
        if not m or final < 0 or not game:
            await interaction.followup.send("❌ Need game (poker/uno), a user ID/@mention and a whole-number amount ≥ 0.", ephemeral=True)
            return
        uid = int(m.group())
        got = await pdb.get_payroll_run_by_message(self.message.id)
        if not got:
            await interaction.followup.send("❌ Run not found.", ephemeral=True)
            return
        run, items = got
        existing = next((i for i in items if i["user_id"] == uid), None)
        member = interaction.guild.get_member(uid)
        if member is None:
            try:
                member = await interaction.guild.fetch_member(uid)
            except discord.NotFound:
                member = None
        name = member.display_name if member else (existing["user_name"] if existing else None)
        if not name:
            await interaction.followup.send("❌ That user isn't in this server.", ephemeral=True)
            return
        ok, err = await pdb.set_payroll_adjustment(run["id"], game, uid, name, final, interaction.user.id, interaction.user.display_name)
        if not ok:
            await interaction.followup.send(f"❌ {err}", ephemeral=True)
            return
        run, items = await pdb.get_payroll_run(run["id"])
        await self.message.edit(embed=render_run(run, items), view=PayrollView())
        row = next((i for i in items if i["user_id"] == uid), None)
        if row:
            shown = fmt_pay(*((row["base_amount"], row["adjusted_amount"]) if game == "poker" else (row["uno_amount"], row["adj_uno"])))
        else:
            shown = "0"
        await interaction.followup.send(f"✅ {game.upper()} · <@{uid}>: {shown}", ephemeral=True)


class PayrollView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, custom_id="payroll:approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        got = await _guard(interaction)
        if not got:
            return
        run, _ = got
        await interaction.response.defer()
        try:
            ok, why = await pdb.pay_payroll_run(run["id"], interaction.user.id, interaction.user.display_name)
        except Exception:
            traceback.print_exc()
            await interaction.followup.send("❌ Payout failed and was rolled back — nobody was paid. Check logs.", ephemeral=True)
            return
        if ok or why == "approved":
            await settle_uno(run["id"])  # idempotent; also finishes a UNO credit that failed earlier
        run, items = await pdb.get_payroll_run(run["id"])
        await interaction.edit_original_response(embed=render_run(run, items), view=None if run["status"] != "pending" else self)
        if not ok:
            await interaction.followup.send(f"Already {why}; nothing paid again.", ephemeral=True)

    @discord.ui.button(label="Adjust", style=discord.ButtonStyle.primary, custom_id="payroll:adjust")
    async def adjust(self, interaction: discord.Interaction, button: discord.ui.Button):
        got = await _guard(interaction)
        if not got:
            return
        if got[0]["status"] != "pending":
            await interaction.response.send_message(f"Run is already {got[0]['status']}.", ephemeral=True)
            return
        await interaction.response.send_modal(AdjustModal(interaction.message))

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger, custom_id="payroll:reject")
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        got = await _guard(interaction)
        if not got:
            return
        run, _ = got
        await interaction.response.defer()
        await pdb.reject_payroll_run(run["id"], interaction.user.id, interaction.user.display_name)
        run, items = await pdb.get_payroll_run(run["id"])
        await interaction.edit_original_response(embed=render_run(run, items), view=None if run["status"] != "pending" else self)


# ── Cog ──────────────────────────────────────────────────────────────────────

class Payroll(commands.Cog):
    payroll = app_commands.Group(name="payroll", description="Weekly dealer payroll", guild_ids=[config.GUILD_ID])

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._lock = asyncio.Lock()

    async def cog_load(self):
        await pdb.init_payroll_tables()
        self.bot.add_view(PayrollView())
        if not self.scheduler.is_running():
            self.scheduler.start()

    async def cog_unload(self):
        self.scheduler.cancel()

    async def _channel(self):
        cid = getattr(config, "PAYROLL_CHANNEL_ID", None)
        if not cid:
            return None
        return self.bot.get_channel(cid) or await self.bot.fetch_channel(cid)

    async def _window(self, kind: str, now: dt.datetime):
        last_end = await pdb.get_last_payroll_end()
        boundary = last_boundary(now)
        if kind == "scheduled":
            if last_end and last_end >= boundary:
                return None
            return (last_end or boundary - dt.timedelta(days=7)), boundary
        now = now.replace(microsecond=0)
        start = last_end or boundary
        return (start, now) if now > start else None

    async def _post(self, channel, kind: str, start: dt.datetime, end: dt.datetime) -> bool:
        data = await compute(start, end)
        has_content = data["p_rev"] > 0 or data["u_rev"] > 0 or bool(data["items"])
        if kind == "force" and not has_content:
            return False  # don't consume the window for nothing
        status = "pending" if has_content else "empty"
        run_id = await pdb.create_payroll_run(start, end, kind, data["p_rev"], data["u_rev"], data["pool_p"],
                                              data["pool_u"], data["rate"], status, data["items"])
        embed = render(run_id, start, end, data["p_rev"], data["u_rev"], data["pool_p"], data["pool_u"], data["rate"],
                       data["items"], status=status)
        try:
            msg = await channel.send(embed=embed, view=PayrollView() if status == "pending" else None)
        except Exception:
            await pdb.delete_payroll_run(run_id)
            raise
        await pdb.set_payroll_message(run_id, msg.id, channel.id)
        return True

    async def _run_scheduled(self, channel) -> bool:
        w = await self._window("scheduled", _utcnow())
        if not w:
            return False
        await self._post(channel, "scheduled", *w)
        return True

    async def _retry_uno(self):
        for rid in await pdb.get_unsettled_uno_run_ids():
            if await settle_uno(rid):
                run, items = await pdb.get_payroll_run(rid)
                try:
                    ch = self.bot.get_channel(run["channel_id"]) or await self.bot.fetch_channel(run["channel_id"])
                    msg = await ch.fetch_message(run["message_id"])
                    await msg.edit(embed=render_run(run, items), view=None)
                except Exception:
                    traceback.print_exc()

    @tasks.loop(minutes=1)
    async def scheduler(self):
        try:
            await self._retry_uno()
            async with self._lock:
                ch = await self._channel()
                if ch is None:
                    return
                await self._run_scheduled(ch)
        except Exception:
            traceback.print_exc()

    @scheduler.before_loop
    async def _before(self):
        await self.bot.wait_until_ready()

    @payroll.command(name="dryrun", description="[Admin] Preview payroll from the last run to now (saves nothing)")
    async def dryrun(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            await interaction.response.send_message("❌ Admins only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        w = await self._window("force", _utcnow())
        if not w:
            await interaction.followup.send("Nothing to preview — window is empty.", ephemeral=True)
            return
        data = await compute(*w)
        embed = render(0, w[0], w[1], data["p_rev"], data["u_rev"], data["pool_p"], data["pool_u"], data["rate"], data["items"], dry=True)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @payroll.command(name="force", description="[Admin] Post a payroll report now for the last run → now")
    async def force(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            await interaction.response.send_message("❌ Admins only.", ephemeral=True)
            return
        ch = await self._channel()
        if ch is None:
            await interaction.response.send_message("❌ PAYROLL_CHANNEL_ID isn't set in config.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with self._lock:
            await self._run_scheduled(ch)  # settle any overdue Monday run first so windows stay contiguous
            w = await self._window("force", _utcnow())
            posted = bool(w) and await self._post(ch, "force", *w)
        await interaction.followup.send(f"✅ Posted in {ch.mention}." if posted else "Nothing to pay in this window.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Payroll(bot))