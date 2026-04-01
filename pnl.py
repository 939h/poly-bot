"""
pnl.py  —  Panic Bot PnL Viewer
================================
Reads bot_state.json written by panic_bot6.py every poll cycle.

Usage:
    python pnl.py            # print once and exit
    python pnl.py --watch    # auto-refresh every 5s (Ctrl+C to stop)

Put this file in the same folder as panic_bot6.py and bot_state.json.
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime

STATE_FILE   = "bot_state.json"
REFRESH_SECS = 5

# ── Color support (Windows safe) ──────────────────────────────────────────────
try:
    import colorama
    from colorama import Fore, Style
    colorama.init()
    def g(s):  return Fore.GREEN  + str(s) + Style.RESET_ALL
    def r(s):  return Fore.RED    + str(s) + Style.RESET_ALL
    def y(s):  return Fore.YELLOW + str(s) + Style.RESET_ALL
    def c(s):  return Fore.CYAN   + str(s) + Style.RESET_ALL
    def b(s):  return Style.BRIGHT + str(s) + Style.RESET_ALL
    def d(s):  return Style.DIM    + str(s) + Style.RESET_ALL
except ImportError:
    def g(s): return str(s)
    def r(s): return str(s)
    def y(s): return str(s)
    def c(s): return str(s)
    def b(s): return str(s)
    def d(s): return str(s)

SEP  = "=" * 62
SEP2 = "-" * 62

# ── Load state ────────────────────────────────────────────────────────────────

def load_state():
    if not os.path.exists(STATE_FILE):
        print(r(f"'{STATE_FILE}' not found. Is panic_bot6.py running?"))
        sys.exit(1)
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(r(f"Failed to read {STATE_FILE}: {e}"))
        sys.exit(1)

# ── Helpers ───────────────────────────────────────────────────────────────────

def pnl_color(v):
    if v > 0:  return g(f"+${v:.4f}")
    if v < 0:  return r(f"-${abs(v):.4f}")
    return d("$0.0000")

def pct_color(v):
    if v > 0:  return g(f"+{v:.1f}%")
    if v < 0:  return r(f"{v:.1f}%")
    return d("0.0%")

def bar(pct, width=20):
    filled = int(width * max(0, min(100, pct)) / 100)
    return g("#" * filled) + d("-" * (width - filled))

def window_time():
    """Current position within the 15-minute window."""
    now   = int(time.time())
    slot  = (now // 900) * 900
    into  = now - slot
    left  = 900 - into
    mm_in = str(into // 60).zfill(2)
    ss_in = str(into % 60).zfill(2)
    mm_lf = str(left // 60).zfill(2)
    ss_lf = str(left % 60).zfill(2)
    if into < 300:
        period = y("EARLY")
    elif into < 600:
        period = c("MID")
    else:
        period = r("LATE")
    return f"{mm_in}:{ss_in} in  |  {mm_lf}:{ss_lf} left  |  {period}"

# ── Main display ──────────────────────────────────────────────────────────────

def print_pnl(state):
    stats     = state.get("stats", {})
    positions = state.get("positions", {})
    prices    = state.get("prices", {})
    cfg       = state.get("settings", {})
    dry_run   = state.get("dry_run", True)
    updated   = state.get("updated", "—")
    assets    = cfg.get("assets", ["btc", "eth", "sol", "xrp"])

    now_str  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mode_str = y("DRY-RUN") if dry_run else r("LIVE")

    # ── Header ────────────────────────────────────────────────────────────────
    print()
    print(b(SEP))
    print(b("  PANIC BOT  —  PnL REPORT"))
    print(f"  {now_str}  |  mode={mode_str}")
    print(f"  state updated: {d(updated)}")
    print(f"  window: {window_time()}")
    print(b(SEP))

    # ── Overall stats ─────────────────────────────────────────────────────────
    scans    = stats.get("scans", 0)
    triggers = stats.get("triggers", 0)
    buys     = stats.get("buys", 0)
    wins     = stats.get("wins", 0)
    losses   = stats.get("losses", 0)
    net_pnl  = stats.get("pnl", 0.0)
    total    = wins + losses
    wr       = f"{round(wins / total * 100)}%" if total > 0 else "—"
    open_ct  = len(positions)

    print()
    print(b("  OVERALL"))
    print(f"  {'Scans':<16} {scans}")
    print(f"  {'Panic triggers':<16} {triggers}")
    print(f"  {'Orders placed':<16} {buys}")
    print(f"  {'Wins':<16} {g(wins)}")
    print(f"  {'Losses':<16} {(r(losses) if losses > 0 else d(losses))}")
    print(f"  {'Win rate':<16} {(g(wr) if total > 0 else d(wr))}")
    print(f"  {'Open positions':<16} {open_ct}")
    print(f"  {'Net PnL':<16} {pnl_color(net_pnl)}")

    # ── Per-asset breakdown ───────────────────────────────────────────────────
    print()
    print(b("  PER-ASSET LIVE PRICES"))
    print(d(f"  {'Asset':<8} {'YES':>8} {'NO':>8}   {'Status':<20} {'Holding?'}"))
    print(d("  " + SEP2))
    for a in assets:
        yp      = prices.get(f"{a}_yes")
        np_     = prices.get(f"{a}_no")
        ystr    = f"${yp:.4f}" if yp is not None else "—"
        nstr    = f"${np_:.4f}" if np_ is not None else "—"
        in_yes  = f"{a}_yes" in positions
        in_no   = f"{a}_no"  in positions
        holding = []
        if in_yes: holding.append(g("YES"))
        if in_no:  holding.append(g("NO"))
        hold_str = " + ".join(holding) if holding else d("—")

        # Color YES/NO prices red if in panic zone
        cap = cfg.get("cap", 0.30)
        y_col = r(ystr) if yp is not None and yp <= cap else ystr
        n_col = r(nstr) if np_ is not None and np_ <= cap else nstr

        # Window period label
        now_ts  = int(time.time())
        slot_ts = (now_ts // 900) * 900
        secs_in = now_ts - slot_ts
        if secs_in < 300:
            period = y("[EARLY]")
        elif secs_in < 600:
            period = c("[MID]  ")
        else:
            period = r("[LATE] ")

        print(f"  {a.upper():<8} {y_col:>8} {n_col:>8}   {period}            {hold_str}")

    # ── Open positions detail ─────────────────────────────────────────────────
    print()
    print(b(f"  OPEN POSITIONS  ({open_ct})"))
    print(d("  " + SEP2))

    if not positions:
        print(d("  No open positions."))
    else:
        for key, pos in positions.items():
            asset     = key.split("_")[0].upper()
            side      = key.split("_")[1].upper()
            entry     = pos.get("entry", 0)
            curr      = pos.get("current", entry)
            peak      = pos.get("peak", entry)
            tp1       = pos.get("tp1", 0)
            tp2       = pos.get("tp2", 0)
            tp1_done  = pos.get("tp1_done", False)
            target    = pos.get("target", tp1)
            pct       = pos.get("pct", 0)
            pnl_v     = pos.get("pnl", 0.0)

            phase = g("TP1 done → riding to TP2") if tp1_done else "watching TP1"
            cur_col = g(f"${curr:.4f}") if curr > entry else r(f"${curr:.4f}")

            print()
            print(f"  {b(f'{asset}-{side}'):}  {phase}")
            print(f"  {'Entry':<14} ${entry:.4f}")
            print(f"  {'Current':<14} {cur_col}")
            print(f"  {'Peak':<14} ${peak:.4f}")
            print(f"  {'TP1':<14} ${tp1:.4f}  {'(done)' if tp1_done else ''}")
            print(f"  {'TP2':<14} ${tp2:.4f}")
            print(f"  {'Unrealised PnL':<14} {pnl_color(pnl_v)}")
            print(f"  {'Progress':<14} [{bar(pct)}] {pct:.0f}% to {'TP2' if tp1_done else 'TP1'}")
            print(d("  " + SEP2))

    # ── Settings summary ──────────────────────────────────────────────────────
    print()
    print(b("  BOT SETTINGS"))
    print(d(f"  settle={cfg.get('settle',30)}s  "
            f"cap=${cfg.get('cap',0.08):.2f}  "
            f"drop={int(cfg.get('drop_ref',0.30)*100)}%  "
            f"sigma={cfg.get('sigma',2.2)}(n={cfg.get('lookback',10)})"))
    print(d(f"  TP1={cfg.get('tp1',2.0)}x  "
            f"TP2={cfg.get('tp2',10.0)}x  "
            f"trail={int(cfg.get('trail',0.30)*100)}%  "
            f"order=${cfg.get('order',5)}  "
            f"poll={cfg.get('poll',1)}s"))
    print(b(SEP))
    print()

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Panic Bot PnL Viewer")
    parser.add_argument("--watch", action="store_true",
                        help=f"Auto-refresh every {REFRESH_SECS}s  (Ctrl+C to stop)")
    args = parser.parse_args()

    if args.watch:
        print(c(f"Watching — refreshing every {REFRESH_SECS}s  (Ctrl+C to stop)"))
        try:
            while True:
                print("\033[2J\033[H", end="")
                print_pnl(load_state())
                time.sleep(REFRESH_SECS)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        print_pnl(load_state())


if __name__ == "__main__":
    main()
