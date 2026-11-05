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

    for key, rows in (
        ("p_sec", await pdb.get_dealer_minutes_between(start, end)),
        ("u_sec", await udb.get_dealer_minutes_between(start, end)),
    ):
        for r in rows:
            e = people.setdefault(
                r["dealer_id"],
                {
                    "user_id": r["dealer_id"],
                    "name": r["dealer_name"],
                    "p_sec": 0,
                    "u_sec": 0,
                },
            )
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

        items.append({
            **e,
            "p_base": p_base,
            "u_base": u_base,
            "p_adj": None,
            "u_adj": None,
        })

    items.sort(key=lambda x: -(x["p_sec"] + x["u_sec"]))

    return {
        "p_rev": p_rev,
        "u_rev": u_rev,
        "pool_p": math.ceil(pool_p),
        "pool_u": math.ceil(pool_u),
        "rate": float(_rate()),
        "items": items,
    }


def _norm_db_items(db_items: list[dict]) -> list[dict]:
    return [
        {
            "user_id": i["user_id"],
            "name": i["user_name"],
            "p_sec": i["poker_seconds"],
            "u_sec": i["uno_seconds"],
            "p_base": i["base_amount"],
            "p_adj": i["adjusted_amount"],
            "u_base": i["uno_amount"],
            "u_adj": i["adj_uno"],
        }
        for i in db_items
    ]


async def settle_uno(run_id: int) -> bool:
    """Credit the UNO half of an approved run. Idempotent and safe to call repeatedly."""
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
                payees.append({
                    "user_id": it["user_id"],
                    "name": it["name"],
                    "amount": amt,
                    "note": pdb._pay_note(
                        run_id,
                        "UNO",
                        it["u_sec"],
                        it["u_base"],
                        it["u_adj"],
                    ),
                })

        await udb.credit_payroll(
            run_id,
            run["decided_by"],
            run["decided_name"] or "payroll",
            payees,
        )

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
    return f"{base:,} {'+' if d > 0 else '−'} {abs(d):,} → **{adj:,}**"


def _chip_emoji(game: str) -> str:
    """Use configured custom chip emotes; fall back to a readable coin symbol."""
    key = "POKER_CHIP_EMOJI" if game == "poker" else "UNO_CHIP_EMOJI"
    return (
        getattr(config, key, None)
        or getattr(config, "CHIP_EMOJI", None)
        or "🪙"
    )


def _section_text(items: list[dict], game: str, pool: int) -> str:
    sec, base_k, adj_k = (
        ("p_sec", "p_base", "p_adj")
        if game == "poker"
        else ("u_sec", "u_base", "u_adj")
    )

    chip = _chip_emoji(game)
    title = "<:saro_goodboy:1499330531685433411> **POKER**" if game == "poker" else "<:uno_title:1536081884843347988>  **UNO**"

    lines = [title]
    total = 0

    for it in items:
        if not (it[sec] or it[base_k] or it[adj_k] is not None):
            continue

        final = it[adj_k] if it[adj_k] is not None else it[base_k]
        total += final

        share = f" · {final / pool * 100:.1f}%" if pool > 0 else ""

        lines.append(
            f"<@{it['user_id']}>  ·  **{fmt_pay(it[base_k], it[adj_k])} {chip}"
            f"  ·  `{it[sec] / 60:.1f} min`{share}"
        )

    if len(lines) == 1:
        lines.append("_No dealer minutes._")

    lines.append(f"\n**Total payout:** **{total:,}** {chip}")

    body = "\n".join(lines)

    if len(body) > 3900:
        body = body[:3860].rsplit("\n", 1)[0] + "\n_… additional dealers omitted_"

    return body


def _status_text(status: str, decided_by=None, dry=False, uno_paid=True) -> str:
    if dry:
        return "🧪 **DRY RUN**　·　Preview only; nothing has been saved or paid."

    if status == "pending":
        return "**AWAITING APPROVAL**　·　Review the figures, then approve, adjust, or reject."

    if status == "approved":
        if uno_paid:
            return f"**APPROVED**　·　Paid by <@{decided_by}>. Poker and UNO wallets credited."

        return (
            f"**APPROVED**　·　Paid by <@{decided_by}>. "
            "Poker credited; UNO credit is pending and will be retried automatically."
        )

    if status == "rejected":
        return f"**REJECTED**　·　Rejected by <@{decided_by}>. No payouts were made."

    return "**NO PAYOUT**　·　There was nothing to pay in this window."


def _payroll_layout(
    run_id,
    start,
    end,
    p_rev,
    u_rev,
    pool_p,
    pool_u,
    rate,
    items,
    status="pending",
    decided_by=None,
    dry=False,
    uno_paid=True,
):
    title = "<a:money:1567798474613850143> PAYYYTIMEEEEE"
    window = f"**Period**　{_ts(start)}  →  {_ts(end)}"

    summary = (
        f"**Revenue:** **{max(0, p_rev):,}** {_chip_emoji('poker')} poker"
        f" · **{max(0, u_rev):,}** {_chip_emoji('uno')} UNO\n"
        f"**Pools:** **{pool_p:,}** {_chip_emoji('poker')}"
        f" · **{pool_u:,}** {_chip_emoji('uno')}"
    )

    return discord.ui.Container(
        discord.ui.TextDisplay(
            content=f"# {title}\n{window}\n\n{summary}"
        ),
        discord.ui.Separator(),
        discord.ui.TextDisplay(
            content=_section_text(items, "poker", pool_p)
        ),
        discord.ui.Separator(),
        discord.ui.TextDisplay(
            content=_section_text(items, "uno", pool_u)
        ),
        discord.ui.Separator(),
        discord.ui.TextDisplay(
            content=_status_text(status, decided_by, dry, uno_paid)
        ),
    )


def render(
    run_id,
    start,
    end,
    p_rev,
    u_rev,
    pool_p,
    pool_u,
    rate,
    items,
    status="pending",
    decided_by=None,
    dry=False,
    uno_paid=True,
):
    return PayrollView(
        report=_payroll_layout(
            run_id,
            start,
            end,
            p_rev,
            u_rev,
            pool_p,
            pool_u,
            rate,
            items,
            status=status,
            decided_by=decided_by,
            dry=dry,
            uno_paid=uno_paid,
        ),
        show_actions=(status == "pending" and not dry),
    )


def render_run(run: dict, db_items: list[dict]):
    return render(
        run["id"],
        dt.datetime.fromisoformat(run["window_start"]),
        dt.datetime.fromisoformat(run["window_end"]),
        run["poker_revenue"],
        run["uno_revenue"],
        run["pool_poker"],
        run["pool_uno"],
        run["rate"],
        _norm_db_items(db_items),
        status=run["status"],
        decided_by=run["decided_by"],
        uno_paid=bool(run["uno_paid"]),
    )


# ── Buttons ───────────────────────────────────────────────────────────────────

async def _guard(interaction: discord.Interaction):
    if not _is_admin(interaction.user):
        await interaction.response.send_message("❌ Admins only.", ephemeral=True)
        return None

    got = await pdb.get_payroll_run_by_message(interaction.message.id)

    if not got:
        await interaction.response.send_message(
            "❌ This payroll run wasn't found.",
            ephemeral=True,
        )
        return None

    return got


class AdjustModal(discord.ui.Modal, title="Adjust payout"):
    game = discord.ui.TextInput(
        label="Game (poker / uno)",
        default="poker",
        max_length=5,
    )

    user = discord.ui.TextInput(
        label="User (ID or @mention)",
        placeholder="123456789012345678",
        max_length=60,
    )

    amount = discord.ui.TextInput(
        label="Final payout (that game's chips)",
        placeholder="250",
        max_length=12,
    )

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
            await interaction.followup.send(
                "❌ Need game (poker/uno), a user ID/@mention and a whole-number amount ≥ 0.",
                ephemeral=True,
            )
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

        name = member.display_name if member else (
            existing["user_name"] if existing else None
        )

        if not name:
            await interaction.followup.send(
                "❌ That user isn't in this server.",
                ephemeral=True,
            )
            return

        ok, err = await pdb.set_payroll_adjustment(
            run["id"],
            game,
            uid,
            name,
            final,
            interaction.user.id,
            interaction.user.display_name,
        )

        if not ok:
            await interaction.followup.send(f"❌ {err}", ephemeral=True)
            return

        run, items = await pdb.get_payroll_run(run["id"])

        await self.message.edit(view=render_run(run, items))

        row = next((i for i in items if i["user_id"] == uid), None)

        if row:
            shown = fmt_pay(
                *(
                    (row["base_amount"], row["adjusted_amount"])
                    if game == "poker"
                    else (row["uno_amount"], row["adj_uno"])
                )
            )
        else:
            shown = "0"

        await interaction.followup.send(
            f"✅ {game.upper()} · <@{uid}>: {shown}",
            ephemeral=True,
        )


class _PayrollButton(discord.ui.Button):
    def __init__(self, label, style, custom_id, handler):
        super().__init__(
            label=label,
            style=style,
            custom_id=custom_id,
        )
        self.handler = handler

    async def callback(self, interaction: discord.Interaction):
        await self.handler(interaction)


class PayrollView(discord.ui.LayoutView):
    """Components V2 payroll report with persistent action buttons."""

    def __init__(self, report=None, show_actions=True):
        super().__init__(timeout=None)

        if report is not None:
            self.add_item(report)

        if show_actions:
            self.add_item(
                discord.ui.ActionRow(
                    _PayrollButton(
                        "Approve",
                        discord.ButtonStyle.success,
                        "payroll:approve",
                        self.approve,
                    ),
                    _PayrollButton(
                        "Adjust",
                        discord.ButtonStyle.primary,
                        "payroll:adjust",
                        self.adjust,
                    ),
                    _PayrollButton(
                        "Reject",
                        discord.ButtonStyle.danger,
                        "payroll:reject",
                        self.reject,
                    ),
                )
            )

    async def approve(self, interaction: discord.Interaction):
        got = await _guard(interaction)

        if not got:
            return

        run, _ = got

        await interaction.response.defer()

        try:
            ok, why = await pdb.pay_payroll_run(
                run["id"],
                interaction.user.id,
                interaction.user.display_name,
            )

        except Exception:
            traceback.print_exc()

            await interaction.followup.send(
                "❌ Payout failed and was rolled back — nobody was paid. Check logs.",
                ephemeral=True,
            )
            return

        if ok or why == "approved":
            await settle_uno(run["id"])

        run, items = await pdb.get_payroll_run(run["id"])

        await interaction.edit_original_response(
            view=render_run(run, items)
        )

        if not ok:
            await interaction.followup.send(
                f"Already {why}; nothing paid again.",
                ephemeral=True,
            )

    async def adjust(self, interaction: discord.Interaction):
        got = await _guard(interaction)

        if not got:
            return

        if got[0]["status"] != "pending":
            await interaction.response.send_message(
                f"Run is already {got[0]['status']}.",
                ephemeral=True,
            )
            return

        await interaction.response.send_modal(
            AdjustModal(interaction.message)
        )

    async def reject(self, interaction: discord.Interaction):
        got = await _guard(interaction)

        if not got:
            return

        run, _ = got

        await interaction.response.defer()

        await pdb.reject_payroll_run(
            run["id"],
            interaction.user.id,
            interaction.user.display_name,
        )

        run, items = await pdb.get_payroll_run(run["id"])

        await interaction.edit_original_response(
            view=render_run(run, items)
        )


# ── Cog ──────────────────────────────────────────────────────────────────────

class Payroll(commands.Cog):
    payroll = app_commands.Group(
        name="payroll",
        description="Weekly dealer payroll",
        guild_ids=[config.GUILD_ID],
    )

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

            return (
                last_end or boundary - dt.timedelta(days=7),
                boundary,
            )

        now = now.replace(microsecond=0)
        start = last_end or boundary

        return (start, now) if now > start else None

    async def _post(
        self,
        channel,
        kind: str,
        start: dt.datetime,
        end: dt.datetime,
    ) -> bool:
        data = await compute(start, end)

        has_content = (
            data["p_rev"] > 0
            or data["u_rev"] > 0
            or bool(data["items"])
        )

        if kind == "force" and not has_content:
            return False

        status = "pending" if has_content else "empty"

        run_id = await pdb.create_payroll_run(
            start,
            end,
            kind,
            data["p_rev"],
            data["u_rev"],
            data["pool_p"],
            data["pool_u"],
            data["rate"],
            status,
            data["items"],
        )

        report = render(
            run_id,
            start,
            end,
            data["p_rev"],
            data["u_rev"],
            data["pool_p"],
            data["pool_u"],
            data["rate"],
            data["items"],
            status=status,
        )

        try:
            msg = await channel.send(view=report)

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
                    ch = (
                        self.bot.get_channel(run["channel_id"])
                        or await self.bot.fetch_channel(run["channel_id"])
                    )

                    msg = await ch.fetch_message(run["message_id"])

                    await msg.edit(
                        view=render_run(run, items)
                    )

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

    @payroll.command(
        name="dryrun",
        description="[Admin] Preview payroll from the last run to now (saves nothing)",
    )
    async def dryrun(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            await interaction.response.send_message(
                "❌ Admins only.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        w = await self._window("force", _utcnow())

        if not w:
            await interaction.followup.send(
                "Nothing to preview — window is empty.",
                ephemeral=True,
            )
            return

        data = await compute(*w)

        report = render(
            0,
            w[0],
            w[1],
            data["p_rev"],
            data["u_rev"],
            data["pool_p"],
            data["pool_u"],
            data["rate"],
            data["items"],
            dry=True,
        )

        await interaction.followup.send(
            view=report,
            ephemeral=True,
        )

    @payroll.command(
        name="force",
        description="[Admin] Post a payroll report now for the last run → now",
    )
    async def force(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            await interaction.response.send_message(
                "❌ Admins only.",
                ephemeral=True,
            )
            return

        ch = await self._channel()

        if ch is None:
            await interaction.response.send_message(
                "❌ PAYROLL_CHANNEL_ID isn't set in config.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        async with self._lock:
            await self._run_scheduled(ch)

            w = await self._window("force", _utcnow())
            posted = bool(w) and await self._post(ch, "force", *w)

        await interaction.followup.send(
            f"✅ Posted in {ch.mention}." if posted else "Nothing to pay in this window.",
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(Payroll(bot))