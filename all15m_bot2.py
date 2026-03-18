"""
Polymarket All 15-Minute Up/Down Bot v2 (BTC, ETH, SOL, XRP)
Strategy:
  1. Only buy in first 5 minutes of window
  2. Buy if YES or NO drops to 25c (once per asset per window)
  3. Sell half at 50c (2x)
  4. Hold rest to resolution — pays $1 per share if wins
  5. Stop loss: sell all if price drops to 12c
  6. No new buys in last 3 minutes

Requirements:
    pip install py-clob-client python-dotenv requests
"""

import os
import sys
import csv
import json
import time
import logging
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

sys.stdout.reconfigure(line_buffering=True)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs, OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    from py_clob_client.constants import POLYGON
except ImportError:
    print("Run: pip install py-clob-client python-dotenv requests")
    exit(1)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("all15m_bot2.log"),
    ],
    force=True,
)
for handler in logging.getLogger().handlers:
    handler.flush = lambda: sys.stdout.flush()

log = logging.getLogger(__name__)

# ── Settings ──────────────────────────────────────────────────────────────────

DRY_RUN          = True
ASSETS           = ["btc", "eth", "sol", "xrp"]
BUY_SHARES       = 10
BUY_PRICE        = 0.25    # Buy when price drops to 25c
SELL_HALF_PRICE  = 0.50    # Sell half at 2x (50c)
STOP_LOSS_PRICE  = 0.12    # Stop loss — sell all if drops to 12c
ENTRY_WINDOW     = 810     # Only buy in first 5 minutes (300s)
NO_BUY_LAST_SECS = 180     # No new buys in last 3 minutes
WINDOW_SECS      = 900     # 15-minute window
POLL_SECS        = 1

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
PNL_FILE  = "all15m_pnl2.csv"

# ── PnL Tracker ───────────────────────────────────────────────────────────────

class PnLTracker:
    def __init__(self, path=PNL_FILE):
        self.path = path
        self.trades = []
        self._init_file()

    def _init_file(self):
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "datetime", "asset", "window", "side",
                    "buy_shares", "buy_price", "buy_cost",
                    "sell1_shares", "sell1_price", "sell1_revenue",
                    "sell2_shares", "sell2_price", "sell2_revenue",
                    "total_revenue", "total_pnl", "status", "running_total"
                ])

    def load_from_csv(self):
        if not os.path.exists(self.path):
            return []
        self.trades = []
        pending = []
        with open(self.path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row.get("window", "").strip():
                    continue
                trade = {
                    "datetime":      row["datetime"],
                    "asset":         row.get("asset", "btc"),
                    "window":        int(row["window"]),
                    "side":          row["side"],
                    "buy_shares":    float(row["buy_shares"]),
                    "buy_price":     float(row["buy_price"]),
                    "buy_cost":      float(row["buy_cost"]),
                    "sell1_shares":  float(row.get("sell1_shares") or 0),
                    "sell1_price":   float(row.get("sell1_price") or 0),
                    "sell1_revenue": float(row.get("sell1_revenue") or 0),
                    "sell2_shares":  float(row.get("sell2_shares") or 0),
                    "sell2_price":   float(row.get("sell2_price") or 0),
                    "sell2_revenue": float(row.get("sell2_revenue") or 0),
                    "total_revenue": float(row.get("total_revenue") or 0),
                    "total_pnl":     float(row.get("total_pnl") or 0),
                    "status":        row.get("status", "OPEN"),
                    "shares_held":   float(row.get("buy_shares") or BUY_SHARES),
                    "half_sold":     float(row.get("sell1_revenue") or 0) > 0,
                }
                self.trades.append(trade)
                if trade["status"] == "OPEN":
                    pending.append({
                        "trade_idx": len(self.trades) - 1,
                        "asset":     trade["asset"],
                        "window":    trade["window"],
                        "side":      trade["side"],
                    })
        return pending

    def record_buy(self, asset, window, side, shares, price):
        cost = round(shares * price, 4)
        trade = {
            "datetime":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "asset":         asset,
            "window":        window,
            "side":          side,
            "buy_shares":    shares,
            "buy_price":     price,
            "buy_cost":      cost,
            "sell1_shares":  0.0,
            "sell1_price":   0.0,
            "sell1_revenue": 0.0,
            "sell2_shares":  0.0,
            "sell2_price":   0.0,
            "sell2_revenue": 0.0,
            "total_revenue": 0.0,
            "total_pnl":     0.0,
            "status":        "OPEN",
            "shares_held":   shares,
            "half_sold":     False,
        }
        self.trades.append(trade)
        log.info(
            f"  PnL | [{asset.upper()}] BUY {shares} {side} @ {price:.2%} = ${cost:.4f} USDC | "
            f"target sell-half={SELL_HALF_PRICE:.0%} stop-loss={STOP_LOSS_PRICE:.0%}"
        )
        self._rewrite()
        return len(self.trades) - 1

    def record_sell_half(self, trade_idx, price, reason="2X"):
        if trade_idx >= len(self.trades):
            return
        t = self.trades[trade_idx]
        if t["half_sold"]:
            return
        sell_shares        = BUY_SHARES // 2
        revenue            = round(sell_shares * price, 4)
        t["sell1_shares"]  = sell_shares
        t["sell1_price"]   = price
        t["sell1_revenue"] = revenue
        t["shares_held"]   = t["buy_shares"] - sell_shares
        t["half_sold"]     = True
        log.info(f"  PnL | [{t['asset'].upper()}] SELL HALF ({reason}) {sell_shares} {t['side']} @ {price:.2%} = ${revenue:.4f}")
        self._rewrite()

    def record_sell_all(self, trade_idx, price, reason="50c"):
        if trade_idx >= len(self.trades):
            return
        t = self.trades[trade_idx]
        if t["status"] != "OPEN":
            return
        held               = t["shares_held"]
        revenue            = round(held * price, 4)
        t["sell2_shares"]  = held
        t["sell2_price"]   = price
        t["sell2_revenue"] = revenue
        t["shares_held"]   = 0
        total_rev          = round(t["sell1_revenue"] + revenue, 4)
        total_pnl          = round(total_rev - t["buy_cost"], 4)
        t["total_revenue"] = total_rev
        t["total_pnl"]     = total_pnl
        t["status"]        = "CLOSED"
        log.info(
            f"  PnL | [{t['asset'].upper()}] SELL ALL ({reason}) {held} {t['side']} @ {price:.2%} = ${revenue:.4f} | "
            f"total_pnl={'+' if total_pnl>=0 else ''}{total_pnl:.4f} USDC"
        )
        self._rewrite()

    def record_resolved(self, trade_idx, won):
        """Market resolved — hold shares pay $1 if won, $0 if lost."""
        if trade_idx >= len(self.trades):
            return
        t = self.trades[trade_idx]
        if t["status"] != "OPEN":
            return
        held    = t["shares_held"]
        price   = 1.0 if won else 0.0
        revenue = round(held * price, 4)
        t["sell2_shares"]  = held
        t["sell2_price"]   = price
        t["sell2_revenue"] = revenue
        t["shares_held"]   = 0
        total_rev  = round(t["sell1_revenue"] + revenue, 4)
        total_pnl  = round(total_rev - t["buy_cost"], 4)
        t["total_revenue"] = total_rev
        t["total_pnl"]     = total_pnl
        t["status"]        = "WIN" if won else "LOSS"
        log.info(
            f"  PnL | [{t['asset'].upper()}] RESOLVED {'WIN' if won else 'LOSS'} | "
            f"hold_revenue=${revenue:.4f} | total_pnl={'+' if total_pnl>=0 else ''}{total_pnl:.4f} USDC"
        )
        self._rewrite()

    def print_summary(self):
        if not self.trades:
            log.info("PnL | No trades yet.")
            return
        closed = [t for t in self.trades if t["status"] not in ("OPEN",)]
        open_  = [t for t in self.trades if t["status"] == "OPEN"]
        total  = round(sum(t["total_pnl"] for t in closed), 4)
        spent  = round(sum(t["buy_cost"] for t in self.trades), 4)
        log.info("=" * 55)
        log.info("  PnL SUMMARY — ALL MARKETS")
        for asset in ASSETS:
            ac  = [t for t in closed if t["asset"] == asset]
            ap  = round(sum(t["total_pnl"] for t in ac), 4)
            log.info(f"  {asset.upper()}: {len(ac)} trades | pnl={'+' if ap>=0 else ''}{ap:.4f}")
        log.info(f"  Total trades : {len(self.trades)} ({len(open_)} open)")
        log.info(f"  Total spent  : ${spent:.4f} USDC")
        log.info(f"  Net PnL      : {'+' if total>=0 else ''}{total:.4f} USDC")
        log.info(f"  Log saved to : {self.path}")
        log.info("=" * 55)

    def _rewrite(self):
        running = 0.0
        with open(self.path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "datetime", "asset", "window", "side",
                "buy_shares", "buy_price", "buy_cost",
                "sell1_shares", "sell1_price", "sell1_revenue",
                "sell2_shares", "sell2_price", "sell2_revenue",
                "total_revenue", "total_pnl", "status", "running_total"
            ])
            for t in self.trades:
                if t["status"] != "OPEN":
                    running = round(running + t["total_pnl"], 4)
                writer.writerow([
                    t["datetime"], t["asset"], t["window"], t["side"],
                    t["buy_shares"], t["buy_price"], t["buy_cost"],
                    t["sell1_shares"], t["sell1_price"], t["sell1_revenue"],
                    t["sell2_shares"], t["sell2_price"], t["sell2_revenue"],
                    t["total_revenue"], t["total_pnl"], t["status"],
                    running if t["status"] != "OPEN" else "OPEN"
                ])

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_server_time():
    try:
        r = requests.get(f"{CLOB_API}/time", timeout=5)
        return int(r.json())
    except Exception:
        return int(datetime.now(timezone.utc).timestamp())


def get_current_window_start(server_ts):
    return (server_ts // WINDOW_SECS) * WINDOW_SECS


def build_slug(asset, window_ts):
    return f"{asset}-updown-15m-{window_ts}"


def fetch_market_by_slug(slug):
    try:
        r = requests.get(f"{GAMMA_API}/markets", params={"slug": slug}, timeout=10)
        data = r.json()
        markets = data if isinstance(data, list) else data.get("markets", [])
        if markets:
            return markets[0]
        r2 = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=10)
        events = r2.json()
        if isinstance(events, list) and events:
            nested = events[0].get("markets", [])
            if nested:
                return nested[0]
        return None
    except Exception as e:
        log.error(f"Gamma API error ({slug}): {e}")
        return None


def get_tokens(market):
    raw = market.get("clobTokenIds") or market.get("clob_token_ids", [])
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None, None
    if not raw or len(raw) < 2:
        return None, None
    return raw[0].strip(), raw[1].strip()


def get_midpoint(client, token_id):
    try:
        return float(client.get_midpoint(token_id)["mid"])
    except Exception:
        return 0.0


def build_client():
    pk       = os.getenv("POLY_PRIVATE_KEY")
    api_key  = os.getenv("POLY_API_KEY")
    api_sec  = os.getenv("POLY_API_SECRET")
    api_pass = os.getenv("POLY_API_PASSPHRASE")
    funder   = os.getenv("POLY_FUNDER_ADDRESS", "")
    if not all([pk, api_key, api_sec, api_pass]):
        log.error("Missing .env keys.")
        exit(1)
    client = ClobClient(
        host=CLOB_API, chain_id=POLYGON, key=pk,
        creds={"apiKey": api_key, "apiSecret": api_sec, "apiPassphrase": api_pass},
        signature_type=0, funder=funder or None,
    )
    log.info("Connected to Polymarket CLOB.")
    return client


def market_buy(client, token_id, shares, price, label):
    amount = round(shares * price, 4)
    if DRY_RUN:
        log.info(f"  [DRY RUN] MARKET BUY {shares} {label} @ {price:.2%} = ${amount:.4f} USDC")
        return price
    try:
        order = client.create_market_order(
            MarketOrderArgs(token_id=token_id, amount=amount, side=BUY)
        )
        resp = client.post_order(order, OrderType.FOK)
        log.info(f"  MARKET BUY executed: {label} | {resp}")
        return price
    except Exception as e:
        log.error(f"  MARKET BUY failed ({label}): {e}")
        return None


def market_sell(client, token_id, shares, price, label):
    amount = round(shares * price, 4)
    if DRY_RUN:
        log.info(f"  [DRY RUN] MARKET SELL {shares} {label} @ {price:.2%} = ${amount:.4f} USDC")
        return price
    try:
        order = client.create_market_order(
            MarketOrderArgs(token_id=token_id, amount=amount, side=SELL)
        )
        resp = client.post_order(order, OrderType.FOK)
        log.info(f"  MARKET SELL executed: {label} | {resp}")
        return price
    except Exception as e:
        log.error(f"  MARKET SELL failed ({label}): {e}")
        return None


def resolve_pending_on_startup(pnl):
    unresolved = pnl.load_from_csv()
    if not unresolved:
        return {}
    grouped = {}
    for item in unresolved:
        key = (item["asset"], item["window"])
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(item)
    live_pending = {}
    server_ts = get_server_time()
    for (asset, w), items in grouped.items():
        if server_ts < w + WINDOW_SECS + 30:
            live_pending[(asset, w)] = items
            continue
        try:
            mkt = fetch_market_by_slug(build_slug(asset, w))
            if mkt:
                result = mkt.get("result") or mkt.get("winner") or mkt.get("resolutionResult")
                if result:
                    for item in items:
                        won = (result.strip().upper() == item["side"].upper())
                        pnl.record_resolved(item["trade_idx"], won)
                else:
                    live_pending[(asset, w)] = items
            else:
                live_pending[(asset, w)] = items
        except Exception:
            live_pending[(asset, w)] = items
    return live_pending


# ── Main Loop ─────────────────────────────────────────────────────────────────

def run():
    client = build_client()
    pnl    = PnLTracker()

    if DRY_RUN:
        log.info("=" * 55)
        log.info("  DRY RUN MODE — no real orders will be placed")
        log.info("  Set DRY_RUN = False to go live")
        log.info("=" * 55)

    pending          = resolve_pending_on_startup(pnl)
    traded           = set(pending.keys())
    token_cache      = {}
    active_positions = {}
    for key, items in pending.items():
        active_positions[key] = [
            {"trade_idx": i["trade_idx"], "side": i["side"],
             "half_sold": pnl.trades[i["trade_idx"]]["half_sold"]}
            for i in items
        ]

    log.info(f"Bot started — monitoring: {', '.join(a.upper() for a in ASSETS)}")
    log.info(
        f"Strategy: BUY {BUY_SHARES} @ {BUY_PRICE:.0%} (first {ENTRY_WINDOW//60}min only) | "
        f"SELL HALF @ {SELL_HALF_PRICE:.0%} | STOP LOSS @ {STOP_LOSS_PRICE:.0%} | "
        f"HOLD rest to resolution | No buy in last {NO_BUY_LAST_SECS//60}min"
    )

    while True:
        try:
            server_ts    = get_server_time()
            window_start = get_current_window_start(server_ts)
            secs_into    = server_ts - window_start
            secs_left    = (window_start + WINDOW_SECS) - server_ts

            for asset in ASSETS:
                key = (asset, window_start)

                # ── Pre-cache tokens ──────────────────────────────────────────
                if key not in token_cache:
                    mkt = fetch_market_by_slug(build_slug(asset, window_start))
                    if mkt:
                        yt, nt = get_tokens(mkt)
                        if yt and nt:
                            token_cache[key] = (yt, nt)

                tokens = token_cache.get(key)
                if not tokens:
                    continue

                yes_token, no_token = tokens

                # ── Monitor active positions ──────────────────────────────────
                for pos_key, positions in list(active_positions.items()):
                    pos_asset, pos_window = pos_key
                    pos_tokens = token_cache.get(pos_key)
                    if not pos_tokens:
                        # try to fetch tokens for older windows
                        old_mkt = fetch_market_by_slug(build_slug(pos_asset, pos_window))
                        if old_mkt:
                            yt2, nt2 = get_tokens(old_mkt)
                            if yt2 and nt2:
                                token_cache[pos_key] = (yt2, nt2)
                                pos_tokens = (yt2, nt2)
                    if not pos_tokens:
                        continue

                    pyt, pnt = pos_tokens
                    for pos in list(positions):
                        idx      = pos["trade_idx"]
                        side     = pos["side"]
                        token_id = pyt if side == "YES" else pnt
                        price    = get_midpoint(client, token_id)
                        if price <= 0:
                            continue

                        # Stop loss
                        if price <= STOP_LOSS_PRICE:
                            held = pnl.trades[idx]["shares_held"]
                            if held > 0:
                                log.info(f"  [{pos_asset.upper()} {side}] STOP LOSS @ {price:.2%}!")
                                sp = market_sell(client, token_id, held, price, f"{pos_asset.upper()}-{side}")
                                if sp is not None:
                                    pnl.record_sell_all(idx, sp, "STOP-LOSS")
                                    positions.remove(pos)
                            continue

                        # Sell half at 2x
                        if not pos["half_sold"] and price >= SELL_HALF_PRICE:
                            log.info(f"  [{pos_asset.upper()} {side}] Price {price:.2%} >= {SELL_HALF_PRICE:.0%} — sell half!")
                            sp = market_sell(client, token_id, BUY_SHARES // 2, price, f"{pos_asset.upper()}-{side}")
                            if sp is not None:
                                pnl.record_sell_half(idx, sp, "2X")
                                pos["half_sold"] = True

                    # Resolve windows that have closed
                    if server_ts > pos_window + WINDOW_SECS + 30:
                        try:
                            mkt = fetch_market_by_slug(build_slug(pos_asset, pos_window))
                            if mkt:
                                result = mkt.get("result") or mkt.get("winner") or mkt.get("resolutionResult")
                                if result:
                                    for pos in list(positions):
                                        won = (result.strip().upper() == pos["side"].upper())
                                        pnl.record_resolved(pos["trade_idx"], won)
                                    del active_positions[pos_key]
                                    if pos_key in pending:
                                        del pending[pos_key]
                        except Exception as e:
                            log.error(f"  Resolution error: {e}")

                # ── Skip already-traded ───────────────────────────────────────
                if key in traded:
                    continue

                # ── Only buy in first 5 minutes ───────────────────────────────
                if secs_into > ENTRY_WINDOW:
                    continue

                # ── No buy in last 3 minutes ──────────────────────────────────
                if secs_left < NO_BUY_LAST_SECS:
                    continue

                # ── Check buy trigger ─────────────────────────────────────────
                yes_price = get_midpoint(client, yes_token)
                no_price  = get_midpoint(client, no_token)

                if round(yes_price, 2) <= BUY_PRICE:
                    log.info(f"  [{asset.upper()}] TRIGGER: YES @ {yes_price:.2%} — buying!")
                    fill = market_buy(client, yes_token, BUY_SHARES, yes_price, f"{asset.upper()}-YES")
                    if fill is not None:
                        idx = pnl.record_buy(asset, window_start, "YES", BUY_SHARES, fill)
                        pending.setdefault(key, []).append({"trade_idx": idx, "asset": asset, "side": "YES"})
                        active_positions.setdefault(key, []).append(
                            {"trade_idx": idx, "side": "YES", "half_sold": False}
                        )
                        traded.add(key)

                elif round(no_price, 2) <= BUY_PRICE:
                    log.info(f"  [{asset.upper()}] TRIGGER: NO @ {no_price:.2%} — buying!")
                    fill = market_buy(client, no_token, BUY_SHARES, no_price, f"{asset.upper()}-NO")
                    if fill is not None:
                        idx = pnl.record_buy(asset, window_start, "NO", BUY_SHARES, fill)
                        pending.setdefault(key, []).append({"trade_idx": idx, "asset": asset, "side": "NO"})
                        active_positions.setdefault(key, []).append(
                            {"trade_idx": idx, "side": "NO", "half_sold": False}
                        )
                        traded.add(key)

        except KeyboardInterrupt:
            log.info("Bot stopped by user.")
            pnl.print_summary()
            break
        except Exception as e:
            log.error(f"Unexpected error: {e}")

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    run()
