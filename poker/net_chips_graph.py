"""
Rendering for the /poker and /uno stats "Graph" buttons.

History itself now lives in poker/net_history_db.py (poker) and
uno/stats_db.py (UNO); this module only draws it with matplotlib.

The x-axis collapses long idle gaps (see _break_threshold) so active sessions
get the width instead of flat lines; breaks are marked and tick labels stay
real UTC times.

generate_net_chips_graph() is synchronous and CPU-bound — call it via
asyncio.to_thread from the cog so it can't block the event loop.
"""
from datetime import datetime, timezone
import bisect
from io import BytesIO

import matplotlib
matplotlib.use("Agg")  # headless — no display server on the bot's box
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Match the palette already used by leaderboard_image.py / jackpot_image.py
BG = "#11151A"
WHITE = "#F0F0F0"
MUTED = "#9B9EA5"
GREEN = "#2ECC71"
RED = "#E74C3C"


MIN_BREAK_S = 30 * 60        # collapse threshold never goes below 30 min ...
MAX_BREAK_S = 6 * 3600       # ... or above 6 h
MAX_LABELED_BREAKS = 8       # duration labels on every break up to this many
MAX_LINE_BREAKS = 12         # dashed lines up to this many; faint session bands beyond


_BREAK_STEPS = (1800, 3600, 7200, 10800, 14400, 21600)   # 30m 1h 2h 3h 4h 6h


def _break_threshold(span_s: float) -> int:
    """Gaps longer than this get collapsed: the smallest round step >= span/24,
    capped at 6h, so ~1 day -> 1h, 3 days -> 3h, 7+ days -> 6h."""
    want = span_s / 24
    for step in _BREAK_STEPS:
        if step >= want:
            return step
    return MAX_BREAK_S


def _fmt_dur(sec: float) -> str:
    sec = int(sec)
    d, rem = divmod(sec, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    if d:
        return f"{d}d {h}h" if h else f"{d}d"
    if h:
        return f"{h}h {m}m" if m else f"{h}h"
    return f"{max(m, 1)}m"


def _fmt_threshold(sec: int) -> str:
    return f"{sec // 3600}h" if sec % 3600 == 0 else f"{sec // 60}m"


def generate_net_chips_graph(display_name: str, history: list[dict]) -> BytesIO:
    """
    `history` is [{ts: int, net_chips: int}, ...] sorted ascending by ts, as
    returned by the history dbs' get_history(). Caller is expected to have
    already checked len(history) >= 2.

    Idle gaps longer than _break_threshold(span) are squeezed to a fixed narrow
    slice (threshold/4) and marked, so the x-axis is NOT linear in time across
    breaks; it is linear inside each session.
    """
    ts = [row["ts"] for row in history]
    values = [row["net_chips"] for row in history]
    span = max(ts[-1] - ts[0], 1)
    thr = _break_threshold(span)
    # A collapsed gap becomes a narrow slice: at most thr/4, and never more than
    # ~2.5% of the active (non-collapsed) time so short sessions aren't swamped.
    active = sum(d for d in (b - a for a, b in zip(ts, ts[1:])) if d <= thr)
    brk_w = max(1.0, min(thr / 4, active * 0.025))

    # Compressed x coordinates + the list of collapsed gaps
    xs = [0.0]
    breaks = []  # (x_start, x_end, real_gap_seconds)
    for i in range(1, len(ts)):
        dt = ts[i] - ts[i - 1]
        if dt > thr:
            breaks.append((xs[-1], xs[-1] + brk_w, dt))
            xs.append(xs[-1] + brk_w)
        else:
            xs.append(xs[-1] + dt)

    line_color = GREEN if values[-1] > 0 else RED if values[-1] < 0 else WHITE

    fig, ax = plt.subplots(figsize=(10, 5), dpi=150)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    # Session bands (many breaks) — drawn first so they sit behind the line
    if len(breaks) > MAX_LINE_BREAKS:
        edges = [xs[0]] + [e for b in breaks for e in (b[0], b[1])] + [xs[-1]]
        for k in range(0, len(edges) - 1, 4):          # sessions are edges[0:2]... every other one
            ax.axvspan(edges[k], edges[k + 1], color=MUTED, alpha=0.07, linewidth=0)

    ax.plot(xs, values, color=line_color, linewidth=2.2)
    ax.fill_between(xs, values, 0, color=line_color, alpha=0.12)
    ax.axhline(0, color=MUTED, linewidth=0.8, linestyle="--", alpha=0.6)

    # Break markers
    if 0 < len(breaks) <= MAX_LINE_BREAKS:
        for (x0, x1, gap) in breaks:
            xm = (x0 + x1) / 2
            ax.axvline(xm, color=MUTED, linewidth=0.9, linestyle=(0, (4, 3)), alpha=0.55)
            if len(breaks) <= MAX_LABELED_BREAKS:
                # label goes on whichever side (top/bottom) the line is NOT on
                ylo, yhi = ax.get_ylim()
                v = values[min(bisect.bisect_left(xs, x1), len(values) - 1)]
                top = (v - ylo) / ((yhi - ylo) or 1) < 0.5
                ax.text(xm, 0.98 if top else 0.02, f" {_fmt_dur(gap)} ",
                        transform=ax.get_xaxis_transform(), color=MUTED, fontsize=7,
                        rotation=90, ha="right", va="top" if top else "bottom")

    # Ticks: evenly spaced in compressed space, nudged out of break slices,
    # labelled with the real UTC time at that x (linear within a session).
    n_ticks = 6
    x_lo, x_hi = xs[0], xs[-1]
    tick_x = []
    for k in range(n_ticks):
        x = x_lo + (x_hi - x_lo) * k / (n_ticks - 1)
        for (b0, b1, _) in breaks:
            if b0 < x < b1:
                x = b1
        if not tick_x or x - tick_x[-1] > (x_hi - x_lo) / (n_ticks * 2):
            tick_x.append(x)
    fmt = "%H:%M" if span <= 86400 else ("%a %H:%M" if span <= 8 * 86400 else "%d %b")
    labels = []
    for x in tick_x:
        j = min(max(bisect.bisect_left(xs, x), 1), len(xs) - 1)
        x0, x1 = xs[j - 1], xs[j]
        t = ts[j] if x1 == x0 else ts[j - 1] + (ts[j] - ts[j - 1]) * (x - x0) / (x1 - x0)
        if ts[j] - ts[j - 1] > thr:   # tick landed on a collapsed gap: show the session start
            t = ts[j]
        labels.append(datetime.fromtimestamp(t, tz=timezone.utc).strftime(fmt))
    ax.set_xticks(tick_x)
    ax.set_xticklabels(labels)
    pad = (x_hi - x_lo) * 0.01
    ax.set_xlim(x_lo - pad, x_hi + pad)

    ax.set_title(f"Net Chips Over Time — {display_name}", color=WHITE, fontsize=14, pad=14)
    ax.tick_params(colors=MUTED, labelsize=9)
    for spine in ax.spines.values():
        spine.set_color(MUTED)
        spine.set_alpha(0.3)
    ax.grid(True, color=MUTED, alpha=0.15, linestyle="--")

    note = "Times in UTC"
    if breaks:
        note += f" · idle gaps over {_fmt_threshold(thr)} collapsed"
    fig.text(0.99, 0.012, note, color=MUTED, fontsize=7.5, ha="right", va="bottom", alpha=0.8)
    fig.tight_layout(rect=(0, 0.03, 1, 1))

    buf = BytesIO()
    fig.savefig(buf, format="png", facecolor=BG)
    plt.close(fig)
    buf.seek(0)
    return buf