"""
Data + rendering for the /poker stats "Graph" button.

get_history() reads net_chips history from a manually-maintained CSV
(config.NET_CHIPS_HISTORY_CSV_PATH) — this bot never writes to that file
itself; you update it out-of-band (see extract_backups_to_csv.py /
backfill_net_chips_history.py).

CSV format (header required):
    timestamp,user_id,net_chips
    1789187406,259968817290018816,-610
    ...

- timestamp: unix epoch seconds
- net_chips: the *absolute* cumulative value at that timestamp, not a delta

generate_net_chips_graph() renders that data with matplotlib.

Both functions are synchronous (plain `csv` module, no pandas — kept
light) and CPU/IO-bound. Call them via asyncio.to_thread from the cog so
they can't block the event loop.
"""
import csv
import os
import traceback
from datetime import datetime, timezone
from io import BytesIO

import config

import matplotlib
matplotlib.use("Agg")  # headless — no display server on the bot's box
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

CSV_PATH = config.NET_CHIPS_HISTORY_CSV_PATH

# Hard safety ceiling: refuse to parse anything past this rather than risk
# it. Generous headroom over where the file sits today (~20MB) — if it
# ever gets here for real, that's a sign to switch storage, not push the
# limit up.
MAX_CSV_BYTES = 200 * 1024 * 1024

# Match the palette already used by leaderboard_image.py / jackpot_image.py
BG = "#11151A"
WHITE = "#F0F0F0"
MUTED = "#9B9EA5"
GREEN = "#2ECC71"
RED = "#E74C3C"


def get_history(user_id: int, start_ts: int | None = None, end_ts: int | None = None) -> list[dict]:
    """
    Returns [{ts, net_chips}, ...] ordered by ts ascending, for `user_id`
    within [start_ts, end_ts] (either bound may be None for open-ended).

    If start_ts is given, also prepends a synthetic baseline point at
    start_ts using the last known value before it (if any), so the graph
    starts at the player's actual net_chips at that moment rather than
    jumping in cold at the first in-range row.

    Never raises. Any failure — missing file, oversized file, a locked or
    unreadable file, malformed rows, whatever — results in an empty list
    rather than an exception, so a bad CSV can never take down the
    /poker stats command.
    """
    try:
        if not os.path.exists(CSV_PATH):
            return []

        size = os.path.getsize(CSV_PATH)
        if size > MAX_CSV_BYTES:
            print(f"[net_chips_graph] CSV is {size:,} bytes, over the {MAX_CSV_BYTES:,} safety cap — refusing to read it.")
            return []

        all_rows: list[tuple[int, int]] = []  # (ts, net_chips) for this user only

        # Plain csv.reader + manual column lookup instead of DictReader —
        # no dict allocation per row, meaningfully lighter over hundreds of
        # thousands of rows. Streams the file line-by-line rather than
        # loading it whole, so memory stays proportional to this one
        # user's row count, not file size.
        with open(CSV_PATH, newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return []
            try:
                ts_i = header.index("timestamp")
                uid_i = header.index("user_id")
                nc_i = header.index("net_chips")
            except ValueError:
                print("[net_chips_graph] CSV is missing an expected header column.")
                return []

            for row in reader:
                try:
                    if int(row[uid_i]) != user_id:
                        continue
                    all_rows.append((int(row[ts_i]), int(row[nc_i])))
                except (IndexError, ValueError):
                    continue  # skip malformed rows silently — manual CSV, don't hard-fail the command

        all_rows.sort(key=lambda r: r[0])

        baseline = None
        if start_ts is not None:
            before = [r for r in all_rows if r[0] < start_ts]
            if before:
                baseline = {"ts": start_ts, "net_chips": before[-1][1]}

        in_range = [
            {"ts": ts, "net_chips": net_chips}
            for ts, net_chips in all_rows
            if (start_ts is None or ts >= start_ts) and (end_ts is None or ts <= end_ts)
        ]

        if baseline:
            in_range.insert(0, baseline)

        return in_range
    except Exception:
        print("[net_chips_graph] failed to read/parse history CSV")
        traceback.print_exc()
        return []


def generate_net_chips_graph(display_name: str, history: list[dict]) -> BytesIO:
    """
    `history` is [{ts: int, net_chips: int}, ...] sorted ascending by ts,
    as returned by get_history() above. Caller is expected to have already
    checked len(history) >= 2.
    """
    times = [datetime.fromtimestamp(row["ts"], tz=timezone.utc) for row in history]
    values = [row["net_chips"] for row in history]

    line_color = GREEN if values[-1] > 0 else RED if values[-1] < 0 else WHITE

    fig, ax = plt.subplots(figsize=(10, 5), dpi=150)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    ax.plot(times, values, color=line_color, linewidth=2.2)
    ax.fill_between(times, values, 0, color=line_color, alpha=0.12)
    ax.axhline(0, color=MUTED, linewidth=0.8, linestyle="--", alpha=0.6)

    ax.set_title(f"Net Chips Over Time — {display_name}", color=WHITE, fontsize=14, pad=14)
    ax.tick_params(colors=MUTED, labelsize=9)
    for spine in ax.spines.values():
        spine.set_color(MUTED)
        spine.set_alpha(0.3)

    ax.grid(True, color=MUTED, alpha=0.15, linestyle="--")
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))

    fig.autofmt_xdate()
    fig.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format="png", facecolor=BG)
    plt.close(fig)
    buf.seek(0)
    return buf