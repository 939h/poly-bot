"""
Polymarket ETH Daily Bracket Limit Order Bot
=============================================
Strategy:
  On startup and on each new UTC day, place GTC limit BUY orders on the two
  price brackets *outside* the current ETH 100-unit range:

    upper bracket = ceil(eth / 100) * 100    e.g. $2333 → 2400
    lower bracket = floor(eth / 100) * 100 - 100          → 2200

  Upper bracket ("ETH above 2400?") → BUY YES at ORDER_PRICE (cheap ~4%)
  Lower bracket ("ETH above 2200?") → BUY NO  at ORDER_PRICE (cheap ~1%)

  Orders are placed for TODAY and TOMORROW (tomorrow may not be live yet —
  silently skipped if market not found).

  After a BUY fills, three tiered GTC limit SELL orders are placed:
    TP1  50% of shares  at max(buy_price * 2, best_ask)   ← 2x or better
    TP2  25% of shares  at buy_price * 10
    TP3  remaining 25%  at buy_price * 50

  BUY orders are cancelled 2 hours before market resolution (14:00 UTC = midnight MYT-2h).
  SELL orders are placed-and-forget.

  Skip today - line ~1600

Requirements:
    pip install py-clob-client python-dotenv requests colorama

.env keys:
    POLY_PRIVATE_KEY=0x...
    POLY_API_KEY=...
    POLY_API_SECRET=...
    POLY_API_PASSPHRASE=...
    POLY_FUNDER_ADDRESS=0x...
    DRY_RUN=true
    ORDER_PRICE=0.004
    ORDER_SIZE=300
    CANCEL_BEFORE_END_HOURS=3
    XRP_ENABLED=false
    XRP_ORDER_PRICE=0.004
    XRP_ORDER_SIZE=300
    XRP_BRACKET_STEP=0.10
    XRP_EXPIRY_DISTANCE_THRESHOLD=0.25
"""

import math
import os
import re
import sys
import json
import time
import logging
import threading
import requests
from datetime import datetime, timezone, timedelta, date
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import load_dotenv

sys.stdout.reconfigure(line_buffering=True)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        OrderArgs,
        OpenOrderParams,
        OrderType,
        ApiCreds,
        BalanceAllowanceParams,
        AssetType,
    )
    from py_clob_client.order_builder.constants import BUY, SELL
    from py_clob_client.constants import POLYGON
except ImportError:
    print("Run: pip install py-clob-client python-dotenv requests colorama")
    sys.exit(1)

try:
    import colorama
    from colorama import Fore, Style
    colorama.init()
    _COLORS = True
except ImportError:
    _COLORS = False

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

DRY_RUN            = os.getenv("DRY_RUN", "true").lower() == "true"
ORDER_PRICE        = float(os.getenv("ORDER_PRICE", "0.004"))      # inner brackets (upper/lower)
ORDER_PRICE_EXT    = float(os.getenv("ORDER_PRICE_EXT", "0.002"))  # outer brackets (upper2/lower2)
ORDER_SIZE         = int(os.getenv("ORDER_SIZE", "300"))        # shares per side
CANCEL_BEFORE_END_HOURS   = int(os.getenv("CANCEL_BEFORE_END_HOURS", "3"))  # cancel unfilled BUY orders N hours before market resolves
LAST_HOUR_SELL_HOURS      = int(os.getenv("LAST_HOUR_SELL_HOURS", "2"))  # aggressive sell window before end
LAST_HOUR_DISCARD_PRICE   = float(os.getenv("LAST_HOUR_DISCARD_PRICE", "0.001"))
EXPIRY_DISTANCE_THRESHOLD = 50  # $ from bracket: hold if winning by >$50, skip if losing by >$50
MARKET_TZ_OFFSET    = int(os.getenv("MARKET_TZ_OFFSET", "8"))   # UTC+8 = MYT
MARKET_END_UTC_HOUR = (24 - MARKET_TZ_OFFSET) % 24              # midnight MYT = 16:00 UTC
POLL_SECS          = 60      # fill-check interval
LOOP_SLEEP         = 30      # main-loop tick (seconds)

# ── XRP config ────────────────────────────────────────────────────────────────
XRP_ENABLED                   = os.getenv("XRP_ENABLED", "true").lower() == "true"
XRP_ORDER_PRICE               = float(os.getenv("XRP_ORDER_PRICE", "0.003"))
XRP_ORDER_SIZE                = int(os.getenv("XRP_ORDER_SIZE", "300"))
XRP_BRACKET_STEP              = float(os.getenv("XRP_BRACKET_STEP", "0.10"))
XRP_EXPIRY_DISTANCE_THRESHOLD = float(os.getenv("XRP_EXPIRY_DISTANCE_THRESHOLD", "0.25"))

GAMMA_API      = "https://gamma-api.polymarket.com"
CLOB_API       = "https://clob.polymarket.com"
HTTP_PORT      = int(os.getenv("DASHBOARD_PORT", "8765"))

# ── Logging ───────────────────────────────────────────────────────────────────

class _ColorFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        if not _COLORS:
            return msg

        # ── Errors & warnings (check level first) ────────────────────────────
        if record.levelno >= logging.ERROR:
            return Style.BRIGHT + Fore.RED + msg + Style.RESET_ALL
        if record.levelno == logging.WARNING:
            return Style.BRIGHT + Fore.YELLOW + msg + Style.RESET_ALL

        # ── Fill events ───────────────────────────────────────────────────────
        if "FILLED ✓" in msg:
            return Style.BRIGHT + Fore.GREEN + msg + Style.RESET_ALL
        if "All orders filled" in msg:
            return Style.BRIGHT + Fore.GREEN + msg + Style.RESET_ALL

        # ── Expiry strategy outcomes ──────────────────────────────────────────
        if "[EXPIRY]" in msg:
            if "HOLD to resolution" in msg:
                return Style.BRIGHT + Fore.MAGENTA + msg + Style.RESET_ALL
            if "likely losing" in msg:
                return Style.DIM + Fore.RED + msg + Style.RESET_ALL
            return Fore.MAGENTA + msg + Style.RESET_ALL

        # ── Last-hour aggressive sell — all purple ────────────────────────────
        if "[LAST_HOUR]" in msg:
            if "FILLED" in msg:
                return Style.BRIGHT + Fore.MAGENTA + msg + Style.RESET_ALL
            return Fore.MAGENTA + msg + Style.RESET_ALL

        # ── Sell / TP orders placed — purple ─────────────────────────────────
        if any(t in msg for t in ("Limit SELL placed", "Placing sell tranches",
                                   "Partial fill", "sell tranches",
                                   "TP1", "TP2", "TP3",
                                   "sell tranch", "SELL FILLED",
                                   "[SKIP-SELL]")):
            return Fore.MAGENTA + msg + Style.RESET_ALL

        # ── Buy orders placed ─────────────────────────────────────────────────
        if "Limit BUY placed" in msg:
            return Style.BRIGHT + Fore.CYAN + msg + Style.RESET_ALL

        # ── Cancellations ─────────────────────────────────────────────────────
        if any(t in msg for t in ("CANCEL", "Cancelling")):
            return Fore.RED + msg + Style.RESET_ALL

        # ── Dry run ───────────────────────────────────────────────────────────
        if "[DRY RUN]" in msg:
            return Fore.CYAN + msg + Style.RESET_ALL

        # ── Startup resumption ────────────────────────────────────────────────
        if "[STARTUP]" in msg:
            return Fore.BLUE + msg + Style.RESET_ALL

        # ── Market / order not found ──────────────────────────────────────────
        if any(t in msg for t in ("not found", "Market not found", "gone (0 fill)")):
            return Fore.RED + msg + Style.RESET_ALL

        # ── Price snap / tick ─────────────────────────────────────────────────
        if "Price snapped" in msg or "tick_size" in msg:
            return Style.DIM + Fore.YELLOW + msg + Style.RESET_ALL

        # ── Monitoring / waiting ──────────────────────────────────────────────
        if any(t in msg for t in ("Monitoring ", "Waiting on")):
            return Style.DIM + Fore.WHITE + msg + Style.RESET_ALL

        # ── ETH price / brackets ──────────────────────────────────────────────
        if any(t in msg for t in ("Binance ETH", "ETH price", "Brackets:")):
            return Style.BRIGHT + Fore.WHITE + msg + Style.RESET_ALL

        # ── Bracket placement header ──────────────────────────────────────────
        if msg.startswith(msg[:10]) and "| upper=" in msg:
            return Style.BRIGHT + Fore.WHITE + msg + Style.RESET_ALL

        # ── Dashboard / connected ─────────────────────────────────────────────
        if any(t in msg for t in ("[DASH]", "Connected to", "====", "Mode  :",
                                   "Order :", "TP1   :", "TP2   :", "TP3   :")):
            return Style.DIM + Fore.WHITE + msg + Style.RESET_ALL

        return msg

_fmt = "%(asctime)s [%(levelname)s] %(message)s"
_handler = logging.StreamHandler(
    open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
)
_handler.setFormatter(_ColorFormatter(_fmt))
_file_handler = logging.FileHandler("eth_bracket_limit.log", encoding="utf-8")
_file_handler.setFormatter(logging.Formatter(_fmt))
logging.basicConfig(level=logging.INFO, handlers=[_handler, _file_handler], force=True)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ── Global dashboard state ────────────────────────────────────────────────────

_slock = threading.Lock()
_logged_order_errors: set[str] = set()  # suppress repeat permanent errors
_tick_upgraded: set[str] = set()        # prevent double tick-upgrade across threads
_last_hour_no_free_logged: dict[str, float] = {}  # throttle repeated "no free shares" logs
_state: dict = {
    "mode":       "DRY RUN" if DRY_RUN else "LIVE",
    "eth_price":  0.0,
    "upper":      0,
    "upper2":     0,
    "lower":      0,
    "lower2":     0,
    "xrp_price":  0.0,
    "xrp_upper":  0.0,
    "xrp_lower":  0.0,
    "updated":    "",
    "buy_orders": [],   # {id, label, bracket, side, date_str, price, size, cancel_at, status}
    "positions":  [],   # {id, label, token_id, shares, buy_price, current_ask}
    "sell_orders":[],   # {id, label, tp, shares, price, order_id, status}
    "closed":     [],   # {label, side, buy_price, exit_type, exit_price, pnl, ts}
    "stats": {
        "deployed": 0.0, "fills": 0, "cancelled": 0,
        "pnl": 0.0, "wins": 0, "losses": 0,
    },
}
_next_id = 0

def _new_id() -> int:
    global _next_id
    _next_id += 1
    return _next_id

def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def st_set_eth(price: float, upper: int, upper2: int, lower: int, lower2: int) -> None:
    with _slock:
        _state["eth_price"] = price
        _state["upper"]     = upper
        _state["upper2"]    = upper2
        _state["lower"]     = lower
        _state["lower2"]    = lower2
        _state["updated"]   = _now_str()

def st_set_xrp(price: float, upper: float, lower: float) -> None:
    with _slock:
        _state["xrp_price"] = price
        _state["xrp_upper"] = upper
        _state["xrp_lower"] = lower
        _state["updated"]   = _now_str()

def st_buy_placed(label: str, order_id: str, bracket: int, side: str,
                  market_date: date, price: float, size: int, cancel_at: float,
                  token_id: str) -> int:
    rec_id = _new_id()
    with _slock:
        _state["buy_orders"].append({
            "id": rec_id, "label": label, "order_id": order_id,
            "bracket": bracket, "side": side,
            "date_str": market_date.strftime("%b %-d"),
            "price": price, "size": size, "cost": round(price * size, 4),
            "cancel_at": cancel_at, "status": "OPEN", "token_id": token_id,
        })
        _state["stats"]["deployed"] = round(
            _state["stats"]["deployed"] + price * size, 4)
        _state["updated"] = _now_str()
    return rec_id

def st_buy_existing(label: str, order_id: str, bracket: int, side: str,
                    market_date: date, price: float, size: int,
                    cancel_at: float, token_id: str) -> int:
    rec_id = _new_id()
    with _slock:
        _state["buy_orders"].append({
            "id": rec_id, "label": label, "order_id": order_id,
            "bracket": bracket, "side": side,
            "date_str": market_date.strftime("%b %-d"),
            "price": price, "size": size, "cost": round(price * size, 4),
            "cancel_at": cancel_at, "status": "RESUMING", "token_id": token_id,
        })
        _state["updated"] = _now_str()
    return rec_id

def st_buy_filled(order_id: str, filled_shares: int, token_id: str,
                  buy_price: float, label: str, market_end: float = 0.0) -> int:
    pos_id = _new_id()
    with _slock:
        for o in _state["buy_orders"]:
            if o["order_id"] == order_id:
                o["status"] = "FILLED"
                break
        _state["positions"].append({
            "id": pos_id, "label": label, "token_id": token_id,
            "shares": filled_shares, "buy_price": buy_price,
            "current_ask": None, "market_end": market_end,
        })
        _state["stats"]["fills"] += 1
        _state["updated"] = _now_str()
    return pos_id

def st_buy_cancelled(order_id: str) -> None:
    with _slock:
        for o in _state["buy_orders"]:
            if o["order_id"] == order_id:
                o["status"] = "CANCELLED"
                break
        _state["stats"]["cancelled"] += 1
        _state["updated"] = _now_str()

def st_sell_cancelled(order_id: str) -> None:
    with _slock:
        for o in _state["sell_orders"]:
            if o["order_id"] == order_id:
                o["status"] = "CANCELLED"
                break
        _state["updated"] = _now_str()

def st_sell_filled(order_id: str) -> None:
    with _slock:
        for o in _state["sell_orders"]:
            if o["order_id"] == order_id:
                o["status"] = "FILLED"
                shares_sold = o.get("shares", 0)
                sell_label  = o.get("label", "")
                # Reduce the matching position's remaining share count
                for p in _state["positions"]:
                    if p["label"] == sell_label:
                        p["shares"] = max(0, p["shares"] - shares_sold)
                        break
                break
        _state["updated"] = _now_str()

def st_sell_placed(label: str, order_id: str, tp: int,
                   shares: int, price: float) -> None:
    with _slock:
        _state["sell_orders"].append({
            "id": _new_id(), "label": label, "order_id": order_id,
            "tp": tp, "shares": shares, "price": price, "status": "OPEN",
        })
        _state["updated"] = _now_str()

def st_market_closed(label: str) -> None:
    """Remove position and sell orders for a resolved market from the dashboard."""
    with _slock:
        _state["positions"]   = [p for p in _state["positions"]   if p["label"] != label]
        _state["sell_orders"] = [o for o in _state["sell_orders"] if o["label"] != label]
        _state["updated"] = _now_str()

def st_update_ask(token_id: str, ask: float | None) -> None:
    with _slock:
        for p in _state["positions"]:
            if p["token_id"] == token_id:
                p["current_ask"] = ask
        _state["updated"] = _now_str()

def _state_snapshot() -> dict:
    with _slock:
        snap = json.loads(json.dumps(_state))   # deep copy via JSON
    now = time.time()
    # Annotate buy_orders with time fields, then strip cancelled and past entries
    active_labels: set[str] = set()
    filtered_buys = []
    for o in snap["buy_orders"]:
        secs = o["cancel_at"] - now
        o["cancel_dt"] = datetime.fromtimestamp(
            o["cancel_at"], tz=timezone.utc).strftime("%b %-d %H:%M UTC")
        o["mins_left"] = round(secs / 60, 1) if secs > 0 else 0
        # Drop cancelled entries and entries whose cancel window has fully passed
        if o["status"] == "CANCELLED":
            continue
        if o["mins_left"] <= 0 and o["status"] not in ("FILLED", "RESUMING"):
            continue
        filtered_buys.append(o)
        active_labels.add(o["label"])
    snap["buy_orders"] = filtered_buys
    # Drop positions whose market has ended
    snap["positions"] = [
        p for p in snap["positions"]
        if p.get("market_end", 0) == 0 or p.get("market_end", 0) > now
    ]
    # Drop sell orders whose market has ended (no live position label)
    live_pos_labels = {p["label"] for p in snap["positions"]}
    snap["sell_orders"] = [
        o for o in snap["sell_orders"]
        if o.get("label") in live_pos_labels
    ]
    return snap

# ── Dashboard HTML ────────────────────────────────────────────────────────────

_DASH_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETH Bracket Bot</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;background:#0f1117;color:#e8edf5;font-size:14px;padding:20px}
h2{font-size:14px;font-weight:600;margin:0 0 12px;color:#c8d0e0;text-transform:uppercase;letter-spacing:.06em}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin-bottom:16px}
.card{background:#161b27;border:1px solid #2a3347;border-radius:10px;padding:14px}
.card .lbl{font-size:11px;color:#5a6a85;text-transform:uppercase;letter-spacing:.06em;margin-bottom:5px}
.card .val{font-size:20px;font-weight:700;font-family:monospace}
.section{background:#161b27;border:1px solid #2a3347;border-radius:10px;padding:16px;margin-bottom:14px}
.sec-hdr{display:flex;justify-content:space-between;align-items:center;cursor:pointer;user-select:none;margin-bottom:0}
.sec-hdr h2{margin:0}
.sec-hdr:hover h2{color:#e8edf5}
.chevron{color:#5a6a85;font-size:13px;transition:transform .2s;display:inline-block}
.chevron.open{transform:rotate(180deg)}
.sec-body{margin-top:14px;max-height:420px;overflow-y:auto}
table{width:100%;border-collapse:collapse;font-size:12px}
th{text-align:left;color:#5a6a85;font-weight:500;padding:0 8px 8px 0;font-size:11px;text-transform:uppercase;letter-spacing:.04em;white-space:nowrap}
td{padding:7px 8px 7px 0;border-top:1px solid #1e2535;font-family:monospace;font-size:12px;vertical-align:middle}
td:first-child{font-family:system-ui;color:#c8d0e0}
.green{color:#4ade9f}.red{color:#f87171}.amber{color:#fbbf24}.blue{color:#60a5fa}.dim{color:#5a6a85}.white{color:#e8edf5}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;font-family:monospace}
.b-dry{background:#1e1608;color:#fbbf24;border:1px solid #5c3d08}
.b-live{background:#1e0808;color:#f87171;border:1px solid #5c1d1d}
.b-open{background:#0d1e2a;color:#60a5fa;border:1px solid #1a3a5c}
.b-fill{background:#0d2a1e;color:#4ade9f;border:1px solid #1a5c3a}
.b-cancel{background:#1e1010;color:#f87171;border:1px solid #5c2020}
.b-resume{background:#1e1e08;color:#fbbf24;border:1px solid #5c5008}
.b-tp1{background:#0d2a2a;color:#67e8f9;border:1px solid #0e6b6b}
.b-tp2{background:#1a0d2a;color:#c084fc;border:1px solid #5c2a8a}
.b-tp3{background:#2a1a0d;color:#fb923c;border:1px solid #8a5a1a}
.pos-card{background:#1a2130;border:1px solid #2a3347;border-radius:8px;padding:12px;margin-bottom:8px}
.pos-hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;flex-wrap:wrap;gap:6px}
.pos-meta{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:#8a9ab5;font-family:monospace}
.tp-row{display:flex;gap:8px;margin-top:8px;flex-wrap:wrap}
.tp-pill{padding:4px 10px;border-radius:6px;font-size:11px;font-family:monospace;font-weight:600}
.hdr-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;flex-wrap:wrap;gap:8px}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#4ade9f;margin-right:6px;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.refresh-badge{font-size:11px;color:#5a6a85}
footer{text-align:center;color:#2a3347;font-size:11px;margin-top:16px;padding-bottom:8px}
</style>
</head>
<body>
<div id="root"><p style="color:#5a6a85;padding:40px;text-align:center">Loading...</p></div>
<script>
function fmt2(v){return v!=null?'$'+parseFloat(v).toFixed(2):'—'}
function fmt4(v){return v!=null?'$'+parseFloat(v).toFixed(4):'—'}
function fmtC(v){if(v==null)return '—';const c=parseFloat(v)*100;return c.toFixed(2).replace(/\.?0+$/,'')+'¢';}
function fmtPnl(v){
  const n=parseFloat(v)||0;
  return `<span class="${n>0?'green':n<0?'red':'dim'}">${n>=0?'+':''}$${Math.abs(n).toFixed(4)}</span>`;
}
function badge(cls,txt){return `<span class="badge ${cls}">${txt}</span>`}
function statusBadge(s){
  const m={OPEN:'b-open',FILLED:'b-fill',CANCELLED:'b-cancel',RESUMING:'b-resume'};
  return badge(m[s]||'b-open',s);
}
function tpBadge(tp){return badge(['','b-tp1','b-tp2','b-tp3'][tp]||'b-tp1','TP'+tp)}

let _lastState=null;
const _col={buys:false,pos:false,sells:false,closed:false};
function toggle(k){_col[k]=!_col[k];if(_lastState)render(_lastState);}
function secHdr(label,k){
  return `<div class="sec-hdr" onclick="toggle('${k}')"><h2>${label}</h2><span class="chevron ${_col[k]?'':'open'}">▼</span></div>`;
}

function render(s){
  const st=s.stats||{};
  const pnl=parseFloat(st.pnl||0);
  const mode=s.mode==='DRY RUN'?badge('b-dry','DRY RUN'):badge('b-live','⚡ LIVE');
  function assetOf(lbl){return lbl&&lbl.includes('xrp')?'XRP':'ETH';}
  function assetSide(lbl,side){return `${assetOf(lbl)}_${side||''}`;}
  const buys=[...(s.buy_orders||[])].sort((a,b)=>(b.cancel_at||0)-(a.cancel_at||0));
  const pos=[...(s.positions||[])].reverse();
  const sells=[...(s.sell_orders||[])].reverse();
  const closed=[...(s.closed||[])].reverse();
  const openBuys=buys.filter(o=>o.status==='OPEN'||o.status==='RESUMING').length;
  const totalCost=buys.filter(o=>o.status!=='CANCELLED').reduce((a,o)=>a+(o.cost||0),0);
  const filledCost=buys.filter(o=>o.status==='FILLED').reduce((a,o)=>a+(o.cost||0),0);

  // BUY orders table
  const buyRows=buys.length?buys.map(o=>{
    const tLeft=o.mins_left>0?
      (o.mins_left>60?`${(o.mins_left/60).toFixed(1)}h`:`${Math.round(o.mins_left)}m`)
      :'<span class="red">past</span>';
    return `<tr>
      <td>${o.date_str}</td>
      <td>${o.bracket.toLocaleString()}</td>
      <td class="${o.side==='YES'?'green':'amber'}">${assetSide(o.label,o.side)}</td>
      <td>${fmtC(o.price)}</td>
      <td>${(o.size||0).toLocaleString()}</td>
      <td>${fmt4(o.cost)}</td>
      <td>${statusBadge(o.status)}</td>
      <td>${o.cancel_dt}</td>
      <td>${tLeft}</td>
    </tr>`;
  }).join('')
  :'<tr><td colspan="9" class="dim" style="padding:12px 0">No BUY orders placed yet</td></tr>';

  // Position cards
  const posCards=pos.length?pos.map(p=>{
    const ask=p.current_ask!=null?fmtC(p.current_ask):'<span class="dim">—</span>';
    const unreal=p.current_ask!=null?fmtPnl((p.current_ask-p.buy_price)*p.shares):'<span class="dim">—</span>';
    const sellsForPos=sells.filter(o=>o.label&&o.label.startsWith(p.label));
    const tpPills=sellsForPos.map(o=>`
      <div class="tp-pill ${['','b-tp1','b-tp2','b-tp3'][o.tp]||'b-tp1'}">
        TP${o.tp} ${o.shares}sh @ ${fmtC(o.price)} <span class="${o.status==='FILLED'?'green':'dim'}">${o.status}</span>
      </div>`).join('');
    const pSide=p.label&&p.label.startsWith('YES')?'YES':'NO';
    return `<div class="pos-card">
      <div class="pos-hdr">
        <div style="display:flex;align-items:center;gap:8px">
          <strong class="${pSide==='YES'?'green':'amber'}" style="font-size:14px">${assetSide(p.label,pSide)}</strong>
          <span style="font-size:11px;color:#5a6a85">${p.label}</span>
        </div>
        <span class="green" style="font-size:12px">${p.shares} shares filled</span>
      </div>
      <div class="pos-meta">
        <span>Buy @ ${fmtC(p.buy_price)}</span>
        <span>Ask ${ask}</span>
        <span>Unrealised ${unreal}</span>
        <span>Cost ${fmt4(p.buy_price*p.shares)}</span>
      </div>
      ${tpPills?`<div class="tp-row">${tpPills}</div>`:''}
    </div>`;
  }).join('')
  :'<p class="dim" style="padding:8px 0;font-size:12px">No filled positions</p>';

  // Closed trades table
  const closedRows=closed.length?closed.map(c=>`<tr>
    <td>${c.ts||'—'}</td>
    <td>${c.label||'—'}</td>
    <td class="${c.side==='YES'?'green':'amber'}">${assetSide(c.label,c.side)}</td>
    <td>${fmtC(c.buy_price)}</td>
    <td>${badge(c.exit_type==='CANCEL'?'b-cancel':c.exit_type==='TP1'?'b-tp1':c.exit_type==='TP2'?'b-tp2':'b-tp3',c.exit_type||'—')}</td>
    <td>${c.exit_price!=null?fmtC(c.exit_price):'—'}</td>
    <td>${fmtPnl(c.pnl)}</td>
  </tr>`).join('')
  :'<tr><td colspan="7" class="dim" style="padding:12px 0">No closed trades yet</td></tr>';

  document.getElementById('root').innerHTML=`
    <div class="hdr-row">
      <div style="display:flex;align-items:center;gap:10px">
        <strong style="font-size:18px;letter-spacing:-.5px">ETH <span class="blue">Bracket</span> Bot</strong>
        ${mode}
      </div>
      <div style="font-size:12px;color:#5a6a85">
        <span class="dot"></span>${s.updated||'—'} &nbsp;
        <span class="refresh-badge">↻ 10s</span>
      </div>
    </div>

    <div class="grid">
      <div class="card"><div class="lbl">ETH Price</div><div class="val white">$${s.eth_price?s.eth_price.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}):'—'}</div></div>
      <div class="card"><div class="lbl">Upper Bracket</div><div class="val green">${s.upper?'$'+s.upper.toLocaleString():'—'}</div></div>
      <div class="card"><div class="lbl">Upper 2</div><div class="val green">${s.upper2?'$'+s.upper2.toLocaleString():'—'}</div></div>
      <div class="card"><div class="lbl">Lower Bracket</div><div class="val amber">${s.lower?'$'+s.lower.toLocaleString():'—'}</div></div>
      <div class="card"><div class="lbl">Lower 2</div><div class="val amber">${s.lower2?'$'+s.lower2.toLocaleString():'—'}</div></div>
      ${s.xrp_price > 0 ? `<div class="card"><div class="lbl">XRP Price</div><div class="val white">$${s.xrp_price.toFixed(4)}</div></div><div class="card"><div class="lbl">XRP Upper</div><div class="val green">$${s.xrp_upper}</div></div><div class="card"><div class="lbl">XRP Lower</div><div class="val amber">$${s.xrp_lower}</div></div>` : ''}
      <div class="card"><div class="lbl">Open Orders</div><div class="val blue">${openBuys}</div></div>
      <div class="card"><div class="lbl">Filled</div><div class="val green">${st.fills||0}</div></div>
      <div class="card"><div class="lbl">Cancelled</div><div class="val ${(st.cancelled||0)>0?'red':'dim'}">${st.cancelled||0}</div></div>
      <div class="card"><div class="lbl">Deployed</div><div class="val dim">${fmt4(totalCost)}</div></div>
      <div class="card"><div class="lbl">Filled Cost</div><div class="val ${filledCost>0?'green':'dim'}">${fmt4(filledCost)}</div></div>
      <div class="card"><div class="lbl">Net PnL</div><div class="val ${pnl>0?'green':pnl<0?'red':'dim'}">${pnl>=0?'+':''}${fmt4(pnl)}</div></div>
    </div>

    <div class="section">
      ${secHdr(`BUY Orders (${buys.length})`, 'buys')}
      ${_col.buys?'':` <div class="sec-body" style="max-height:480px"><div style="overflow-x:auto">
      <table>
        <thead><tr>
          <th>Date</th><th>Bracket</th><th>Asset</th><th>Price</th>
          <th>Shares</th><th>Cost</th><th>Status</th><th>Cancel At</th><th>Time Left</th>
        </tr></thead>
        <tbody>${buyRows}</tbody>
      </table>
      </div></div>`}
    </div>

    <div class="section">
      ${secHdr(`Positions (${pos.length})`, 'pos')}
      ${_col.pos?'':` <div class="sec-body">${posCards}</div>`}
    </div>

    <div class="section">
      ${secHdr(`Sell Orders (${sells.length})`, 'sells')}
      ${_col.sells?'':` <div class="sec-body">${sells.length?`<div style="overflow-x:auto"><table>
        <thead><tr><th>Market</th><th>TP</th><th>Shares</th><th>Sell @</th><th>Status</th></tr></thead>
        <tbody>${sells.map(o=>`<tr>
          <td>${o.label||'—'}</td>
          <td>${tpBadge(o.tp)}</td>
          <td>${o.shares}</td>
          <td>${fmtC(o.price)}</td>
          <td>${statusBadge(o.status)}</td>
        </tr>`).join('')}</tbody>
      </table></div>`
      :'<p class="dim" style="font-size:12px;padding:4px 0">No sell orders yet</p>'}</div>`}
    </div>

    <div class="section">
      ${secHdr(`Closed Trades (${closed.length})`, 'closed')}
      ${_col.closed?'':` <div class="sec-body"><div style="overflow-x:auto"><table>
        <thead><tr><th>Time</th><th>Market</th><th>Asset</th><th>Buy @</th><th>Exit</th><th>Exit @</th><th>PnL</th></tr></thead>
        <tbody>${closedRows}</tbody>
      </table></div></div>`}
    </div>

    <div class="section">
      <h2>Settings</h2>
      <table style="max-width:480px"><tbody>
        <tr><td style="color:#5a6a85">Inner price</td><td>$${(${ORDER_PRICE}||0.004).toFixed(4)}</td>
            <td style="color:#5a6a85;padding-left:24px">Outer price</td><td>$${(${ORDER_PRICE_EXT}||0.002).toFixed(4)}</td></tr>
        <tr><td style="color:#5a6a85">Order size</td><td>${ORDER_SIZE} shares</td>
            <td style="color:#5a6a85;padding-left:24px">Cancel before</td><td>${CANCEL_BEFORE_END_HOURS}h before end</td></tr>
        <tr><td style="color:#5a6a85">Poll interval</td><td>${POLL_SECS}s</td>
            <td style="color:#5a6a85;padding-left:24px">Dashboard</td><td>:${HTTP_PORT}</td></tr>
        <tr><td style="color:#5a6a85">TP1</td><td>50% @ 2x</td>
            <td style="color:#5a6a85;padding-left:24px">TP2</td><td>25% @ 10x</td></tr>
        <tr><td style="color:#5a6a85">TP3</td><td>25% @ 50x</td>
            <td></td><td></td></tr>
      </tbody></table>
    </div>

    <footer>Auto-refreshes every 10s &nbsp;&mdash;&nbsp; ETH Bracket Bot</footer>`;
}

async function poll(){
  try{
    const r=await fetch('/state');
    const d=await r.json();
    _lastState=d;
    render(d);
  }catch(e){console.error('fetch error',e);}
}
poll();
setInterval(poll,10000);
</script>
</body>
</html>"""

# Replace JS template placeholders with Python values at module load
_DASH_HTML = (
    _DASH_HTML
    .replace("${ORDER_PRICE_EXT}", str(ORDER_PRICE_EXT))
    .replace("${ORDER_PRICE}", str(ORDER_PRICE))
    .replace("${ORDER_SIZE}", str(ORDER_SIZE))
    .replace("${CANCEL_BEFORE_END_HOURS}", str(CANCEL_BEFORE_END_HOURS))
    .replace("${POLL_SECS}", str(POLL_SECS))
    .replace("${HTTP_PORT}", str(HTTP_PORT))
)


class _DashHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/state":
            data = json.dumps(_state_snapshot()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            body = _DASH_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, fmt, *args):  # silence access log
        pass


def start_dashboard():
    server = HTTPServer(("0.0.0.0", HTTP_PORT), _DashHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("[DASH] Dashboard running on http://0.0.0.0:%d", HTTP_PORT)

# ── Binance ───────────────────────────────────────────────────────────────────

def fetch_eth_spot() -> float:
    """Return current ETH/USDT spot price from Binance public REST API.
    Override with ETH_PRICE env var for testing."""
    override = os.getenv("ETH_PRICE")
    if override:
        price = float(override)
        log.info(f"ETH price (override): ${price:,.2f}")
        return price
    url = "https://api.binance.com/api/v3/ticker/price"
    r = requests.get(url, params={"symbol": "ETHUSDT"}, timeout=10)
    r.raise_for_status()
    price = float(r.json()["price"])
    log.info(f"Binance ETH spot: ${price:,.2f}")
    return price


def fetch_xrp_spot() -> float:
    """Return current XRP/USDT spot price from Binance public REST API.
    Override with XRP_PRICE env var for testing."""
    override = os.getenv("XRP_PRICE")
    if override:
        price = float(override)
        log.info(f"XRP price (override): ${price:.4f}")
        return price
    url = "https://api.binance.com/api/v3/ticker/price"
    r = requests.get(url, params={"symbol": "XRPUSDT"}, timeout=10)
    r.raise_for_status()
    price = float(r.json()["price"])
    log.info(f"Binance XRP spot: ${price:.4f}")
    return price

# ── Bracket ───────────────────────────────────────────────────────────────────

def get_brackets(eth_price: float) -> tuple[int, int, int, int]:
    """
    Return (upper, upper2, lower, lower2) based on ETH spot price.

    Example: eth_price = 2333
      upper  = ceil(2333/100)*100       = 2400
      upper2 = upper + 100              = 2500
      lower  = floor(2333/100)*100-100  = 2200
      lower2 = lower - 100              = 2100
    """
    upper  = int(math.ceil(eth_price / 100) * 100)
    upper2 = upper + 100
    lower  = int(math.floor(eth_price / 100) * 100) - 100
    lower2 = lower - 100
    log.info(f"Brackets: upper={upper}/{upper2}, lower={lower}/{lower2} (ETH ${eth_price:,.2f})")
    return upper, upper2, lower, lower2


def get_xrp_brackets(xrp_price: float) -> tuple[float, float]:
    """Return (upper, lower) XRP bracket levels. Same floor-minus-step formula as ETH."""
    step  = XRP_BRACKET_STEP
    upper = round(math.ceil(xrp_price / step) * step, 4)
    lower = round(math.floor(xrp_price / step) * step - step, 4)
    log.info(f"XRP brackets: upper={upper:g}, lower={lower:g} (XRP ${xrp_price:.4f})")
    return upper, lower

# ── Slug / Market ─────────────────────────────────────────────────────────────

_ETH_SLUG_RE = re.compile(r'^ethereum-above-(\d+)-on-\w+-\d+$')
_XRP_SLUG_RE = re.compile(r'^xrp-above-[\dpt]+-on-\w+-\d+$')


def build_slug(bracket: int, target_date: date) -> str:
    """
    Build Polymarket slug for an 'Ethereum above X on [date]' market.
    Format: ethereum-above-{bracket}-on-{month}-{day}
    Example: ethereum-above-2400-on-april-16
    """
    date_str = target_date.strftime("%B-%-d").lower()
    return f"ethereum-above-{bracket}-on-{date_str}"


def build_xrp_slug(bracket: float, target_date: date) -> str:
    """Build Polymarket slug for an XRP daily bracket market.
    Decimal point → 'pt': $1.30 → xrp-above-1pt3-on-april-20"""
    date_str = target_date.strftime("%B-%-d").lower()
    price_str = f"{bracket:g}".replace(".", "pt")
    return f"xrp-above-{price_str}-on-{date_str}"


def get_tick_size(client, token_id: str, market: dict) -> float:
    """
    Return the minimum price tick for this market.

    1. Try Gamma API field names (available at placement time).
    2. Read tick_size directly from the CLOB OrderBookSummary (authoritative).
    3. Fall back to 0.01.
    """
    # 1. Gamma market fields
    for key in ("minimumTickSize", "minimum_tick_size", "minTickSize", "tickSize"):
        raw = market.get(key)
        if raw is not None:
            try:
                v = float(raw)
                if v > 0:
                    log.info(f"  tick_size={v} (from market.{key})")
                    return v
            except (ValueError, TypeError):
                pass

    # 2. CLOB OrderBookSummary.tick_size — the canonical source per CLOB docs
    if client is not None and not DRY_RUN:
        try:
            book = client.get_order_book(token_id)
            raw_tick = (
                book.get("tick_size") if isinstance(book, dict)
                else getattr(book, "tick_size", None)
            )
            if raw_tick is not None:
                v = float(raw_tick)
                if v > 0:
                    log.debug(f"  tick_size={v} (from CLOB orderbook)")
                    return v
        except Exception as e:
            log.debug(f"  tick_size CLOB lookup failed: {e}")

    # 3. Finest tick in dry-run so ORDER_PRICE is not snapped unnecessarily
    if DRY_RUN:
        return 0.001

    return 0.01


def snap_price(price: float, tick: float) -> float:
    """Round price UP to the nearest valid tick increment."""
    snapped = math.ceil(round(price / tick, 10)) * tick
    return round(snapped, 10)


def fetch_market(slug: str) -> dict | None:
    """Fetch market data from Gamma API by slug; fallback to /events."""
    try:
        r = requests.get(f"{GAMMA_API}/markets", params={"slug": slug}, timeout=10)
        r.raise_for_status()
        data = r.json()
        markets = data if isinstance(data, list) else data.get("markets", [])
        if markets:
            return markets[0]
        r2 = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=10)
        r2.raise_for_status()
        events = r2.json()
        if isinstance(events, list) and events:
            nested = events[0].get("markets", [])
            if nested:
                return nested[0]
        return None
    except Exception as e:
        log.error(f"Gamma API error ({slug}): {e}")
        return None


def fetch_market_by_condition_id(condition_id: str) -> dict | None:
    """Fetch market data from Gamma API by condition ID (used for orphan resync)."""
    try:
        r = requests.get(f"{GAMMA_API}/markets",
                         params={"conditionId": condition_id}, timeout=10)
        r.raise_for_status()
        data = r.json()
        markets = data if isinstance(data, list) else data.get("markets", [])
        return markets[0] if markets else None
    except Exception as e:
        log.error(f"Gamma API error (conditionId={condition_id[:12]}): {e}")
        return None


def extract_tokens(market: dict) -> tuple[str | None, str | None]:
    """Return (yes_token_id, no_token_id) from a market dict."""
    raw = market.get("clobTokenIds") or market.get("clob_token_ids", [])
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None, None
    if not raw or len(raw) < 2:
        return None, None
    return str(raw[0]).strip(), str(raw[1]).strip()

# ── Polymarket Client ─────────────────────────────────────────────────────────

def build_client() -> ClobClient | None:
    pk       = os.getenv("POLY_PRIVATE_KEY")
    api_key  = os.getenv("POLY_API_KEY")
    api_sec  = os.getenv("POLY_API_SECRET")
    api_pass = os.getenv("POLY_API_PASSPHRASE")
    funder   = os.getenv("POLY_FUNDER_ADDRESS", "")
    if not all([pk, api_key, api_sec, api_pass]):
        if DRY_RUN:
            log.info("[DRY RUN] No .env credentials — running without CLOB client.")
            return None
        log.error("Missing .env credentials (POLY_PRIVATE_KEY / POLY_API_KEY / POLY_API_SECRET / POLY_API_PASSPHRASE).")
        sys.exit(1)
    client = ClobClient(
        host=CLOB_API,
        chain_id=POLYGON,
        key=pk,
        creds=ApiCreds(
            api_key=api_key,
            api_secret=api_sec,
            api_passphrase=api_pass,
        ),
        signature_type=1,
        funder=funder or None,
    )
    log.info("Connected to Polymarket CLOB.")
    return client

# ── Order Helpers ─────────────────────────────────────────────────────────────

def place_limit_order(
    client: ClobClient,
    token_id: str,
    label: str,
    price: float,
    size: int,
    side,
) -> str | None:
    """
    Place a GTC limit order. Returns order_id on success, None on failure.
    side: BUY or SELL constant from py_clob_client.
    """
    side_str = "BUY" if side == BUY else "SELL"
    cost = round(price * size, 4)

    # Never sell at or below $0.001 — worthless price, discard silently
    if side == SELL and price <= 0.001:
        log.info(
            f"  [SKIP-SELL] {label} — price ${price:.4f} <= $0.001 minimum; "
            "sell discarded (not worth placing)"
        )
        return None

    if DRY_RUN:
        log.info(
            f"[DRY RUN] LIMIT {side_str} {label}"
            f" | price=${price} | size={size} | cost=${cost:.4f} USDC"
        )
        return f"dry-{label.lower().replace(' ', '-')}-{int(time.time())}"

    try:
        order = client.create_order(
            OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side,
            )
        )
        resp = client.post_order(order, OrderType.GTC)
        if isinstance(resp, dict) and not resp.get("success", True):
            log.error(f"  Order rejected ({label}): {resp.get('errorMsg', 'unknown')}")
            return None
        order_id = resp.get("orderID") or resp.get("id") or str(resp)
        log.info(
            f"  Limit {side_str} placed: {label}"
            f" | price=${price} | size={size} | order_id={order_id}"
        )
        return order_id
    except Exception as e:
        err_str = str(e)
        if "lower than the minimum" in err_str:
            key = f"{side_str}:{label}:min_size"
            if key not in _logged_order_errors:
                _logged_order_errors.add(key)
                log.error(f"  Limit {side_str} failed ({label}): {e}")
        else:
            log.error(f"  Limit {side_str} failed ({label}): {e}")
        return None


def get_best_ask(client: ClobClient, token_id: str) -> float | None:
    """Fetch the best (lowest) ask price from the orderbook."""
    try:
        book = client.get_order_book(token_id)
        asks = (
            book.get("asks") if isinstance(book, dict)
            else getattr(book, "asks", None)
        )
        if not asks:
            return None
        prices = []
        for entry in asks:
            p = entry.get("price") if isinstance(entry, dict) else getattr(entry, "price", None)
            if p is not None:
                prices.append(float(p))
        return min(prices) if prices else None
    except Exception as e:
        log.warning(f"  get_best_ask failed for {token_id[:12]}: {e}")
        return None


def get_filled_shares(client: ClobClient, order_id: str) -> int:
    """Get number of shares filled on a GTC order via get_order()."""
    try:
        details = client.get_order(order_id)
        if isinstance(details, dict):
            for field in (
                "size_matched", "sizeMatched",
                "sizeFilled", "size_filled", "filledSize",
            ):
                val = details.get(field)
                if val is not None:
                    n = math.ceil(float(val))
                    if n > 0:
                        return n
            # If status indicates fully matched, fall back to original size
            if details.get("status", "").upper() in ("MATCHED", "MINED", "FILLED"):
                size = details.get("size") or details.get("original_size") or 0
                return math.ceil(float(size))
        else:
            for attr in ("size_matched", "sizeMatched", "size_filled", "sizeFilled"):
                val = getattr(details, attr, None)
                if val is not None:
                    n = math.ceil(float(val))
                    if n > 0:
                        return n
            if getattr(details, "status", "").upper() in ("MATCHED", "MINED", "FILLED"):
                size = getattr(details, "size", 0) or getattr(details, "original_size", 0)
                return math.ceil(float(size or 0))
        return 0
    except Exception as e:
        log.warning(f"  get_filled_shares failed for {order_id}: {e}")
        return ORDER_SIZE  # assume full fill as fallback


def place_sell_tranches(
    client: ClobClient,
    token_id: str,
    label: str,
    filled_shares: int,
    buy_price: float,
) -> list[dict]:
    """
    Place 3 tiered GTC limit SELL orders after a BUY fills.

    TP1  50% shares  at max(buy_price * 2, best_ask)
    TP2  25% shares  at buy_price * 10
    TP3  remaining   at buy_price * 50

    Returns list of placed order dicts for monitoring.
    """
    # Re-verify actual wallet balance to avoid "not enough shares" errors
    if not DRY_RUN:
        actual = get_token_position(client, token_id)
        if actual > 0 and actual < filled_shares:
            log.warning(
                f"  Share count mismatch for {label}: "
                f"filled={filled_shares} but wallet={actual} — using wallet balance"
            )
            filled_shares = actual

    tp1_shares = math.floor(filled_shares * 0.50)
    tp2_shares = math.floor(filled_shares * 0.25)
    tp3_shares = filled_shares - tp1_shares - tp2_shares

    # Polymarket prices are in [0.01, 0.99]; cap all sell prices accordingly
    MAX_SELL_PRICE = 0.99

    # TP1: check best ask; sell at whichever is higher
    tp1_base = min(round(buy_price * 2, 4), MAX_SELL_PRICE)
    if not DRY_RUN:
        best_ask = get_best_ask(client, token_id)
        if best_ask is not None and best_ask > tp1_base:
            log.info(
                f"  TP1 price adjusted: best_ask={best_ask:.4f}"
                f" > 2x={tp1_base:.4f} → using best_ask"
            )
            tp1_base = min(round(best_ask, 4), MAX_SELL_PRICE)
    else:
        log.info(f"  [DRY RUN] TP1 would check orderbook; using 2x=${tp1_base}")

    tp2_price = min(round(buy_price * 10, 4), MAX_SELL_PRICE)
    tp3_price = min(round(buy_price * 50, 4), MAX_SELL_PRICE)
    log.info(
        f"  Placing sell tranches for {label}"
        f" | filled={filled_shares} shares | buy_price=${buy_price}"
    )
    placed = []
    for tp_num, (tp_shares, tp_price) in enumerate(
        [(tp1_shares, tp1_base), (tp2_shares, tp2_price), (tp3_shares, tp3_price)], 1
    ):
        if tp_shares <= 0:
            log.debug(f"  Skipping TP{tp_num} — 0 shares")
            continue
        oid = place_limit_order(client, token_id, f"{label}-TP{tp_num}", tp_price, tp_shares, SELL)
        if oid:
            st_sell_placed(label, oid, tp_num, tp_shares, tp_price)
            entry = {"order_id": oid, "label": label, "tp": tp_num, "token_id": token_id}
            if tp_num == 1:
                entry["buy_price"]     = buy_price
                entry["shares"]        = tp_shares
                entry["current_price"] = tp_price
            placed.append(entry)
    return placed


def sell_at_expiry(
    client: ClobClient,
    token_id: str,
    label: str,
    shares: int,
    buy_price: float,
    bracket: int | float,
    side_label: str,
    condition_id: str = "",
    price_fn=None,
    expiry_threshold=None,
) -> list[dict]:
    """
    At cancel time (T-2h before market end), decide how to handle filled shares
    based on spot price distance from the bracket.

      distance > +threshold  → HOLD to resolution ($1.00 if wins)
      distance < -threshold  → DO NOTHING (likely worthless)
      within ±threshold      → SELL ALL at best_ask

    price_fn: callable() -> float, defaults to fetch_eth_spot (pass fetch_xrp_spot for XRP)
    expiry_threshold: override for EXPIRY_DISTANCE_THRESHOLD (use smaller value for XRP)
    """
    _price_fn  = price_fn or fetch_eth_spot
    _threshold = expiry_threshold if expiry_threshold is not None else EXPIRY_DISTANCE_THRESHOLD
    try:
        spot = _price_fn()
    except Exception as e:
        log.warning(f"  [EXPIRY] spot fetch failed ({e}) — falling back to TP sells")
        return place_sell_tranches(client, token_id, label, shares, buy_price)

    distance = (spot - bracket) if side_label == "YES" else (bracket - spot)
    log.info(
        f"  [EXPIRY] {label} | spot=${spot:.4f} | bracket={bracket}"
        f" | side={side_label} | distance={distance:+.4f}"
    )

    if distance > _threshold:
        log.info(
            f"  [EXPIRY] distance={distance:+.4f} > +{_threshold}"
            f" → HOLD to resolution"
        )
    elif distance < -_threshold:
        log.info(
            f"  [EXPIRY] distance={distance:+.4f} < -{_threshold}"
            f" → likely losing — skipping sells"
        )
    else:
        log.info(
            f"  [EXPIRY] distance={distance:+.4f} within"
            f" ±{_threshold} → uncertain — selling all {shares} shares at best_ask"
        )
        best_ask = get_best_ask(client, token_id)
        if best_ask is None:
            log.warning(f"  [EXPIRY] No asks in book for {label} — cannot exit")
            return []
        sell_price = min(round(best_ask, 4), 0.99)
        oid = place_limit_order(client, token_id, f"{label}-EXIT", sell_price, shares, SELL)
        if oid:
            st_sell_placed(label, oid, 1, shares, sell_price)
            return [{"order_id": oid, "label": label, "tp": 1, "token_id": token_id}]
    return []


def get_open_order_ids(client: ClobClient, condition_id: str) -> set[str] | None:
    """Return set of open order IDs for this market condition, or None on API error.
    Callers must treat None as 'unknown' (not 'no open orders')."""
    if DRY_RUN:
        return set()
    try:
        open_orders = client.get_orders(OpenOrderParams(market=condition_id))
        if isinstance(open_orders, list):
            # Open order objects use "id" per CLOB API docs
            return {o.get("id", "") for o in open_orders if o.get("id")}
        return set()
    except Exception as e:
        log.warning(f"  Could not fetch open orders: {e}")
        return None  # distinguish from genuinely-empty result


def cancel_order(client: ClobClient, order_id: str, label: str) -> bool:
    """Cancel a single order by ID. Returns True on success."""
    if DRY_RUN:
        log.info(f"  [DRY RUN] CANCEL {label} order_id={order_id}")
        st_buy_cancelled(order_id)
        return True
    try:
        client.cancel(order_id)
        log.info(f"  CANCELLED {label} order_id={order_id}")
        st_buy_cancelled(order_id)
        return True
    except Exception as e:
        log.error(f"  Cancel failed ({label} / {order_id}): {e}")
        return False

# ── Sell Fill Monitor ────────────────────────────────────────────────────────

def monitor_tp_sells(
    client: ClobClient,
    condition_id: str,
    token_id: str,
    orders: list[dict],
) -> None:
    """
    Daemon thread: poll open orders once per POLL_SECS until each placed SELL
    order is either filled or goes away (cancelled/expired).

    Each entry in orders: {order_id, label, tp, token_id}
    """
    if DRY_RUN or not orders:
        return

    pending = {o["order_id"]: o for o in orders if o.get("order_id")}
    while pending:
        time.sleep(POLL_SECS)
        open_ids = get_open_order_ids(client, condition_id)
        if open_ids is None:
            continue  # API error; assume all still open, retry next poll

        # ── TP1 best-ask tracking ────────────────────────────────────────────
        for oid in list(pending.keys()):
            o = pending[oid]
            if o.get("tp") != 1 or oid not in open_ids:
                continue
            buy_price = o.get("buy_price", 0)
            tp1_floor = min(round(buy_price * 2, 4), 0.99)
            best_ask  = get_best_ask(client, o["token_id"])
            if best_ask is None:
                continue
            target = min(max(round(best_ask, 4), tp1_floor), 0.99)
            if abs(target - o["current_price"]) < 1e-9:
                continue
            filled_so_far = get_filled_shares(client, oid)
            remaining = o["shares"] - filled_so_far
            if remaining <= 0:
                continue
            log.info(
                f"  TP1 price update: {o['label']} "
                f"{o['current_price']:.4f} → {target:.4f} "
                f"| {remaining} shares remaining"
            )
            try:
                client.cancel(oid)
                st_sell_cancelled(oid)
            except Exception as e:
                log.error(f"  TP1 cancel failed ({o['label']}/{oid}): {e}")
                continue
            new_oid = place_limit_order(client, o["token_id"], f"{o['label']}-TP1",
                                        target, remaining, SELL)
            pending.pop(oid)
            open_ids.discard(oid)
            if new_oid:
                st_sell_placed(o["label"], new_oid, 1, remaining, target)
                new_entry = dict(o)
                new_entry["order_id"]     = new_oid
                new_entry["current_price"] = target
                new_entry["shares"]        = remaining
                pending[new_oid] = new_entry

        for oid in list(pending.keys()):
            if oid in open_ids:
                continue  # still open
            o = pending.pop(oid)
            filled = get_filled_shares(client, oid)
            if filled > 0:
                log.info(
                    f"  SELL FILLED ✓ {o['label']}-TP{o['tp']}"
                    f" | {filled} shares | order_id={oid}"
                )
                st_sell_filled(oid)
                # TP1: re-place remaining shares if only partially filled
                if o.get("tp") == 1:
                    remaining = o.get("shares", 0) - filled
                    if remaining > 0:
                        buy_price = o.get("buy_price", 0)
                        tp1_floor = min(round(buy_price * 2, 4), 0.99)
                        best_ask  = get_best_ask(client, o["token_id"])
                        target    = tp1_floor
                        if best_ask is not None:
                            target = min(max(round(best_ask, 4), tp1_floor), 0.99)
                        log.info(
                            f"  TP1 partial fill: {filled} sold, "
                            f"re-placing {remaining} shares @ {target:.4f}"
                        )
                        new_oid = place_limit_order(client, o["token_id"],
                                                    f"{o['label']}-TP1", target, remaining, SELL)
                        if new_oid:
                            st_sell_placed(o["label"], new_oid, 1, remaining, target)
                            new_entry = dict(o)
                            new_entry["order_id"]     = new_oid
                            new_entry["current_price"] = target
                            new_entry["shares"]        = remaining
                            pending[new_oid] = new_entry
            else:
                # Silently drop if already marked cancelled (last_hour_sell cancelled it)
                with _slock:
                    st = next(
                        (s["status"] for s in _state["sell_orders"] if s["order_id"] == oid),
                        None,
                    )
                if st != "CANCELLED":
                    log.info(f"  SELL gone (0 fill) {o['label']}-TP{o['tp']} | order_id={oid}")


# ── Last-Hour Sell ───────────────────────────────────────────────────────────

def cancel_all_open_sells(
    client: ClobClient, condition_id: str, token_id: str, label: str
) -> int:
    """Cancel all open SELL orders for this token. Returns count cancelled."""
    if DRY_RUN:
        log.info(f"  [DRY RUN] Would cancel all SELL orders for {label}")
        return 0
    try:
        orders = client.get_orders(OpenOrderParams(market=condition_id))
        count = 0
        for o in (orders or []):
            if o.get("asset_id") == token_id and o.get("side", "").upper() == "SELL":
                oid = o.get("id") or o.get("orderID", "")
                if oid:
                    try:
                        client.cancel(oid)
                        log.info(f"  [LAST_HOUR] Cancelled SELL {label} order_id={oid}")
                        st_sell_cancelled(oid)
                    except Exception as e:
                        log.error(f"  [LAST_HOUR] Cancel SELL failed ({label}/{oid}): {e}")
                    count += 1
        return count
    except Exception as e:
        log.warning(f"  [LAST_HOUR] cancel_all_open_sells failed for {label}: {e}")
        return 0


def get_reserved_open_sell_shares(
    client: ClobClient, condition_id: str, token_id: str
) -> int:
    """
    Estimate shares currently reserved by OPEN SELL orders for this token.
    """
    try:
        orders = client.get_orders(OpenOrderParams(market=condition_id))
    except Exception as e:
        log.warning(f"  [LAST_HOUR] reserved-share check failed for {token_id[:12]}: {e}")
        return 0

    reserved = 0
    for o in (orders or []):
        if o.get("asset_id") != token_id or o.get("side", "").upper() != "SELL":
            continue
        try:
            # Prefer explicit remaining size if present.
            rem = (
                o.get("remaining_size")
                or o.get("remainingSize")
                or o.get("size_remaining")
            )
            if rem is not None:
                reserved += math.ceil(float(rem))
                continue

            size = float(o.get("size") or o.get("original_size") or 0)
            matched = float(
                o.get("size_matched")
                or o.get("sizeMatched")
                or o.get("size_filled")
                or o.get("sizeFilled")
                or 0
            )
            reserved += max(0, math.ceil(size - matched))
        except Exception:
            # Ignore malformed rows.
            continue
    return reserved


def get_free_sellable_shares(
    client: ClobClient, condition_id: str, token_id: str
) -> int:
    """
    Shares free to place in NEW SELL orders = wallet shares - shares already
    reserved by open SELL orders.
    """
    wallet = get_token_position(client, token_id)
    if wallet <= 0:
        return 0
    reserved = get_reserved_open_sell_shares(client, condition_id, token_id)
    return max(0, wallet - reserved)


def last_hour_sell_monitor(
    client: ClobClient,
    token_id: str,
    label: str,
    condition_id: str,
    cancel_at: float,
) -> None:
    """
    At T-1h before market end:
      1. Cancel ALL open sell orders (TP1/TP2/TP3/EXIT) for this position.
      2. Re-read actual wallet balance.
      3. Place a single SELL at best_ask.
      4. Every POLL_SECS: if best_ask changes → cancel current, re-place at new price.
    """
    if DRY_RUN:
        log.info(f"[DRY RUN] Skipping last-hour sell monitor for {label}.")
        return

    market_end   = cancel_at + CANCEL_BEFORE_END_HOURS * 3600
    last_hour_at = market_end - LAST_HOUR_SELL_HOURS * 3600

    now = time.time()
    if last_hour_at > now:
        wait = last_hour_at - now
        log.info(
            f"  [LAST_HOUR] {label} — aggressive sell scheduled in {wait/60:.0f}m "
            f"(T-{LAST_HOUR_SELL_HOURS}h before market end)"
        )
        time.sleep(wait)

    if time.time() >= market_end:
        log.info(f"  [LAST_HOUR] {label} — market already ended, skipping")
        return

    log.info(f"  [LAST_HOUR] {label} — entering last-hour aggressive sell phase")

    # Step 1: Cancel all existing sell orders
    n = cancel_all_open_sells(client, condition_id, token_id, label)
    if n:
        log.info(f"  [LAST_HOUR] {label} — cancelled {n} sell order(s)")

    # Step 2: Wait for the CLOB to release the reserved allowance from cancelled orders.
    # Poll get_reserved_open_sell_shares() until it reaches 0 (max 15s).
    # This prevents the "not enough balance / allowance" 400 error caused by placing
    # a new SELL before the previous orders' reservations have propagated off-chain.
    shares = 0
    for attempt in range(6):
        reserved = get_reserved_open_sell_shares(client, condition_id, token_id)
        wallet   = get_token_position(client, token_id)
        if reserved == 0 and wallet > 0:
            shares = wallet
            break
        if wallet == 0:
            break  # nothing held at all
        log.info(
            f"  [LAST_HOUR] {label} — waiting for allowance to clear "
            f"(reserved={reserved}, wallet={wallet}, attempt {attempt+1}/6)"
        )
        time.sleep(2.5)

    if shares <= 0:
        log.info(f"  [LAST_HOUR] {label} — no shares in wallet (already sold), skipping")
        return

    # Inner helper: place sell at current best_ask.
    # Returns (order_id, price, discarded).
    def _place(qty: int) -> tuple[str | None, float | None, bool]:
        ask = get_best_ask(client, token_id)
        if ask is None:
            log.warning(f"  [LAST_HOUR] {label} — no asks in book")
            return None, None, False
        if ask <= LAST_HOUR_DISCARD_PRICE:
            log.info(
                f"  [LAST_HOUR] {label} — best_ask ${ask:.4f} <= ${LAST_HOUR_DISCARD_PRICE:.4f}; "
                "discarding last-hour sell (no sell placed)"
            )
            return None, ask, True
        sell_price = min(round(ask, 4), 0.99)
        # Refresh allowance cache before placing to avoid stale reservation errors
        try:
            client.update_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            )
        except Exception as _e:
            log.warning(f"  [LAST_HOUR] {label} — allowance refresh failed: {_e}")
        oid = place_limit_order(client, token_id, f"{label}-LAST", sell_price, qty, SELL)
        if oid:
            st_sell_placed(label, oid, 1, qty, sell_price)
        return oid, sell_price, False

    # Step 3: Place initial sell
    current_oid, current_price, discarded = _place(shares)
    if discarded:
        return

    # Step 4: Rescan loop until filled or market closes
    while time.time() < market_end:
        time.sleep(POLL_SECS)
        if time.time() >= market_end:
            break

        # If no active order (placement failed / no asks), retry
        if current_oid is None:
            shares = get_free_sellable_shares(client, condition_id, token_id)
            if shares <= 0:
                now = time.time()
                prev = _last_hour_no_free_logged.get(label, 0)
                if now - prev >= 300:  # once per 5 minutes max
                    _last_hour_no_free_logged[label] = now
                    log.info(f"  [LAST_HOUR] {label} — no free shares remaining")
                return
            # Conflict resolution choice: keep codex branch behavior
            # (retry place, and if discarded due tiny ask, exit monitor).
            current_oid, current_price, discarded = _place(shares)
            if discarded:
                return
            continue

        # Check if current order is still open
        open_ids = get_open_order_ids(client, condition_id)
        if open_ids is None:
            continue  # API error; assume still open, retry next poll
        if current_oid not in open_ids:
            filled = get_filled_shares(client, current_oid)
            if filled > 0:
                log.info(f"  [LAST_HOUR] FILLED ✓ {label} | {filled} shares")
                st_sell_filled(current_oid)
            else:
                log.info(f"  [LAST_HOUR] {label} — sell order gone (0 fill)")
            return

        # Check if best_ask has moved
        ask = get_best_ask(client, token_id)
        if ask is None:
            continue
        new_price = min(round(ask, 4), 0.99)
        if new_price == current_price:
            continue

        # Re-price: cancel current and place at new best_ask
        log.info(
            f"  [LAST_HOUR] {label} — re-pricing ${current_price} → ${new_price}"
        )
        try:
            client.cancel(current_oid)
            st_sell_cancelled(current_oid)
        except Exception as e:
            log.error(f"  [LAST_HOUR] Re-price cancel failed ({label}): {e}")
            continue

        shares = get_free_sellable_shares(client, condition_id, token_id)
        if shares <= 0:
            log.info(f"  [LAST_HOUR] {label} — no free shares remaining after re-price")
            return

        current_oid, current_price, discarded = _place(shares)
        if discarded:
            return

    log.info(f"  [LAST_HOUR] {label} — market window ended.")
    # Cancel any sell orders still open at market close (never filled)
    remaining = cancel_all_open_sells(client, condition_id, token_id, label)
    if remaining:
        log.info(f"  [LAST_HOUR] {label} — cancelled {remaining} sell order(s) at market close")
    # Erase position and sell orders from dashboard
    st_market_closed(label)
    log.info(f"  [LAST_HOUR] {label} — cleared from dashboard")


# ── Monitor + Sell ────────────────────────────────────────────────────────────

def monitor_and_sell(client: ClobClient, orders: list[dict]) -> None:
    """
    Poll open orders. On fill → place sell tranches. Cancel unfilled at deadline.

    Each entry in orders:
        {label, order_id, condition_id, token_id, buy_price}
    """
    if not orders:
        return

    if DRY_RUN:
        log.info(f"[DRY RUN] Skipping monitor loop ({len(orders)} orders).")
        # Simulate sell tranches in dry run so output is visible
        for o in orders:
            place_sell_tranches(client, o["token_id"], o["label"], ORDER_SIZE, o["buy_price"])
        return

    pending = {o["order_id"]: o for o in orders if o.get("order_id")}

    if not pending:
        log.warning("No valid order IDs to monitor.")
        return

    for o in pending.values():
        cancel_dt = datetime.fromtimestamp(o["cancel_at"], tz=timezone.utc)
        log.info(
            f"  Monitoring {o['label']} — "
            f"cancel at {cancel_dt.strftime('%Y-%m-%d %H:%M UTC')} "
            f"({CANCEL_BEFORE_END_HOURS}h before market end)"
        )

    while pending:
        # Sleep in short increments so cancel deadlines fire promptly.
        # Each POLL_SECS block is broken into 5s slices; we exit early when
        # any pending order hits its cancel_at, limiting overshoot to ≤5s.
        deadline = min(o["cancel_at"] for o in pending.values())
        sleep_end = time.time() + POLL_SECS
        while time.time() < sleep_end:
            time.sleep(min(5, max(0, deadline - time.time()), sleep_end - time.time()))
            if time.time() >= deadline:
                break
        now = time.time()

        # ── Tick-size upgrade check ───────────────────────────────────────────
        # If a snapped order (e.g. 0.01) can now be placed at the original price
        # (e.g. 0.004) because the market switched to a finer tick, cancel and re-place.
        for oid in list(pending.keys()):
            o = pending[oid]
            if now >= o["cancel_at"]:
                continue  # past T-3h deadline; skip tick-upgrade
            placed_price   = o["buy_price"]
            intended_price = o.get("intended_price", ORDER_PRICE)
            if placed_price <= intended_price:
                continue  # already at desired price, nothing to do
            if oid in _tick_upgraded:
                continue  # already upgraded by another thread; skip
            new_tick = get_tick_size(client, o["token_id"], {})
            ideal_price = snap_price(intended_price, new_tick)
            if ideal_price < placed_price:
                log.info(
                    f"  Tick upgrade detected for {o['label']}: "
                    f"tick={new_tick}, re-placing at ${ideal_price} (was ${placed_price})"
                )
                _tick_upgraded.add(oid)
                cancel_order(client, oid, o["label"])
                new_oid = place_limit_order(
                    client, o["token_id"], o["label"], ideal_price, ORDER_SIZE, BUY
                )
                if new_oid:
                    new_entry = dict(o)
                    new_entry["order_id"] = new_oid
                    new_entry["buy_price"] = ideal_price
                    pending[new_oid] = new_entry
                    st_buy_placed(
                        o["label"], new_oid,
                        o.get("bracket", 0), o.get("side_label", ""),
                        o.get("target_date", datetime.now(timezone.utc).date()),
                        ideal_price, ORDER_SIZE, o["cancel_at"], o["token_id"],
                    )
                pending.pop(oid)
        # ─────────────────────────────────────────────────────────────────────

        # Group by condition_id to minimise API calls
        by_condition: dict[str, list[str]] = {}
        for oid, o in pending.items():
            by_condition.setdefault(o["condition_id"], []).append(oid)

        for cid, oids in by_condition.items():
            open_ids = get_open_order_ids(client, cid)
            for oid in list(oids):
                if oid not in pending:
                    continue
                o = pending[oid]

                # Cancel if deadline passed — always attempt, cancel_order is idempotent
                if now >= o["cancel_at"]:
                    mins_past = (now - o["cancel_at"]) / 60
                    log.warning(
                        f"  Cancelling {o['label']} — "
                        f"{CANCEL_BEFORE_END_HOURS}h before market end"
                        + (f" ({mins_past:.0f}m past cancel deadline)" if mins_past > 0 else "")
                    )
                    cancelled = cancel_order(client, oid, o["label"])
                    if not cancelled and (open_ids is None or oid in (open_ids or set())):
                        # Cancel API failed and order still appears open — retry next poll
                        log.warning(f"  Cancel failed for {o['label']} — will retry next poll")
                        continue
                    # Check for partial fill before discarding
                    partial = get_filled_shares(client, oid)
                    if partial > 0:
                        log.info(f"  Partial fill on cancelled order: {partial} shares — evaluating expiry strategy")
                        st_buy_filled(oid, partial, o["token_id"], o["buy_price"], o["label"],
                                     market_end=o["cancel_at"] + CANCEL_BEFORE_END_HOURS * 3600)
                        _is_xrp = "xrp-above" in o["label"]
                        exp_orders = sell_at_expiry(
                            client, o["token_id"], o["label"], partial, o["buy_price"],
                            o["bracket"], o["side_label"], o["condition_id"],
                            price_fn=fetch_xrp_spot if _is_xrp else None,
                            expiry_threshold=XRP_EXPIRY_DISTANCE_THRESHOLD if _is_xrp else None,
                        )
                        if exp_orders:
                            threading.Thread(
                                target=monitor_tp_sells,
                                args=(client, o["condition_id"], o["token_id"], exp_orders),
                                daemon=True,
                            ).start()
                        threading.Thread(
                            target=last_hour_sell_monitor,
                            args=(client, o["token_id"], o["label"],
                                  o["condition_id"], o["cancel_at"]),
                            daemon=True,
                        ).start()
                    else:
                        st_buy_cancelled(oid)
                    pending.pop(oid)
                    continue

                # Detect fill (order no longer open) — skip if API errored
                if open_ids is None:
                    continue  # unknown state; assume still open, retry next poll
                if oid not in open_ids:
                    pending.pop(oid)
                    filled = get_filled_shares(client, oid)
                    if filled > 0:
                        log.info(f"  FILLED ✓ {o['label']} | {filled} shares | order_id={oid}")
                        st_buy_filled(oid, filled, o["token_id"], o["buy_price"], o["label"],
                                     market_end=o["cancel_at"] + CANCEL_BEFORE_END_HOURS * 3600)
                        tp_orders = place_sell_tranches(
                            client, o["token_id"], o["label"], filled, o["buy_price"]
                        )
                        if tp_orders:
                            threading.Thread(
                                target=monitor_tp_sells,
                                args=(client, o["condition_id"], o["token_id"], tp_orders),
                                daemon=True,
                            ).start()
                        threading.Thread(
                            target=last_hour_sell_monitor,
                            args=(client, o["token_id"], o["label"],
                                  o["condition_id"], o["cancel_at"]),
                            daemon=True,
                        ).start()
                    else:
                        log.info(f"  Order {o['label']} gone (0 fill) — skipping sell.")

        if not pending:
            log.info("All orders filled or gone.")
            return

        # Status line — show closest cancel deadline
        earliest = min(o["cancel_at"] for o in pending.values())
        mins_left = (earliest - now) / 60
        log.debug(
            f"  Waiting on {len(pending)} order(s) | "
            f"next cancel in {mins_left:.0f}m"
        )

# ── Startup checks ───────────────────────────────────────────────────────────

def find_open_buy_order(client: ClobClient, condition_id: str, token_id: str) -> str | None:
    """
    Return the order_id of an existing open BUY order for this token, or None.
    Used on startup to avoid re-placing orders after a redeploy.
    """
    if DRY_RUN:
        return None
    try:
        orders = client.get_orders(OpenOrderParams(market=condition_id))
        for o in (orders or []):
            if o.get("asset_id") == token_id and o.get("side", "").upper() == "BUY":
                oid = o.get("id") or o.get("orderID", "")
                log.info(f"  [STARTUP] Existing open BUY order found: {oid}")
                return oid
    except Exception as e:
        log.warning(f"  [STARTUP] open order check failed for {token_id[:12]}: {e}")
    return None


def resync_orphan_buys(client: ClobClient, known_order_ids: set[str]) -> list[dict]:
    """
    Fetch ALL open BUY orders from the CLOB and return order dicts for any
    ETH/XRP bracket orders not already tracked by place_for_date().
    Fixes the case where ETH price shifted between runs, making the old bracket
    orders invisible to the startup guard that only checks current-price brackets.
    """
    if DRY_RUN:
        return []
    try:
        all_open = client.get_orders(OpenOrderParams()) or []
    except Exception as e:
        log.warning(f"[STARTUP] Failed to fetch all open orders for orphan resync: {e}")
        return []

    orphans = []
    for raw in all_open:
        oid = raw.get("id") or raw.get("orderID", "")
        if not oid or oid in known_order_ids:
            continue
        if raw.get("side", "").upper() != "BUY":
            continue

        condition_id = raw.get("market") or raw.get("condition_id", "")
        token_id     = raw.get("asset_id", "")
        if not condition_id or not token_id:
            continue

        market = fetch_market_by_condition_id(condition_id)
        if not market:
            continue

        slug   = market.get("slug", "")
        is_eth = bool(_ETH_SLUG_RE.match(slug))
        is_xrp = XRP_ENABLED and bool(_XRP_SLUG_RE.match(slug))
        if not is_eth and not is_xrp:
            continue

        end_iso = market.get("endDateIso") or market.get("end_date_iso", "")
        if not end_iso:
            log.warning(f"[STARTUP] No end date for {slug} — skipping orphan")
            continue
        end_ts    = datetime.fromisoformat(end_iso.replace("Z", "+00:00")).timestamp()
        cancel_at = end_ts - CANCEL_BEFORE_END_HOURS * 3600

        if time.time() >= end_ts:
            log.info(f"[STARTUP] Orphan {oid[:12]} in expired market {slug} — skipping")
            continue

        yes_tok, no_tok = extract_tokens(market)
        if token_id == yes_tok:
            side_label = "YES"
        elif token_id == no_tok:
            side_label = "NO"
        else:
            log.warning(f"[STARTUP] token_id not in market tokens for {slug} — skipping")
            continue

        if is_eth:
            bracket: int | float = int(_ETH_SLUG_RE.match(slug).group(1))
        else:
            price_str = re.search(r'xrp-above-([\dpt]+)-', slug).group(1)
            bracket   = float(price_str.replace("pt", "."))

        buy_price   = float(raw.get("price", 0))
        size        = int(float(raw.get("size") or raw.get("original_size") or ORDER_SIZE))
        label       = f"{side_label} {slug}"
        target_date = datetime.fromtimestamp(end_ts, tz=timezone.utc).date()

        log.info(f"[STARTUP] Orphan BUY resynced: {label} | {oid[:12]} | price={buy_price}")
        st_buy_existing(label, oid, bracket, side_label, target_date,
                        buy_price, size, cancel_at, token_id)
        orphans.append({
            "label":          label,
            "order_id":       oid,
            "condition_id":   condition_id,
            "token_id":       token_id,
            "buy_price":      buy_price,
            "intended_price": buy_price,
            "cancel_at":      cancel_at,
            "bracket":        bracket,
            "side_label":     side_label,
            "target_date":    target_date,
        })

    if orphans:
        log.info(f"[STARTUP] {len(orphans)} orphan BUY order(s) added to monitor")
    return orphans


def count_open_sells(client: ClobClient, condition_id: str, token_id: str) -> int:
    """Return number of open SELL orders for this token (used on startup to avoid re-placing TPs)."""
    if DRY_RUN:
        return 0
    try:
        orders = client.get_orders(OpenOrderParams(market=condition_id))
        return sum(
            1 for o in (orders or [])
            if o.get("asset_id") == token_id and o.get("side", "").upper() == "SELL"
        )
    except Exception as e:
        log.warning(f"  [STARTUP] open sell check failed for {token_id[:12]}: {e}")
        return 0


def get_token_position(client: ClobClient, token_id: str) -> int:
    """
    Return number of shares currently held for this conditional token.
    Used on startup to detect filled positions from a previous run.
    """
    if DRY_RUN:
        return 0
    try:
        resp = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        )
        raw = resp.get("balance") if isinstance(resp, dict) else getattr(resp, "balance", 0)
        return int(float(raw or 0) / 1e6)  # contract returns raw units; 1 share = 1e6 raw
    except Exception as e:
        log.warning(f"  [STARTUP] position check failed for {token_id[:12]}: {e}")
    return 0


# ── Per-date placement ────────────────────────────────────────────────────────

def place_for_date(
    client: ClobClient,
    target_date: date,
    brackets_config: list[tuple],
    slug_fn: callable,
    skip_slugs: set[str] | None = None,
) -> list[dict]:
    """
    For a given date, place BUY limit orders for each entry in brackets_config.
    Returns a list of order dicts for the monitor loop.
    Silently skips any bracket whose market is not yet live.

    brackets_config: [(bracket_value, side_label, tok_idx, order_price), ...]
    slug_fn:         callable(bracket, date) -> slug string (build_slug or build_xrp_slug)
    skip_slugs:      if provided, slugs already placed are skipped and newly placed
                     slugs are added to the set (used for tomorrow retry logic).
    """
    date_label = target_date.strftime("%b %-d")
    log.info(f"--- {date_label} | {len(brackets_config)} bracket(s) ---")
    placed = []

    for bracket, side_label, tok_idx, order_price, order_size in brackets_config:
        slug = slug_fn(bracket, target_date)
        # Use side-qualified key so YES and NO of the same slug (e.g. XRP) track independently
        skip_key = f"{side_label}:{slug}"

        if skip_slugs is not None and skip_key in skip_slugs:
            log.info(f"  Already placed: {side_label} {slug} — skipping")
            continue

        market = fetch_market(slug)
        if market is None:
            log.warning(f"  Market not found: {slug} — skipping")
            continue

        condition_id = market.get("conditionId") or market.get("condition_id", "")
        yes_tok, no_tok = extract_tokens(market)
        if not yes_tok or not no_tok:
            log.error(f"  Could not extract tokens for {slug}")
            continue

        token_id = yes_tok if tok_idx == 0 else no_tok
        label = f"{side_label} {slug}"

        tick  = get_tick_size(client, token_id, market)
        price = snap_price(order_price, tick)
        if price != order_price:
            log.info(f"  Price snapped: ${order_price} → ${price} (tick=${tick})")

        # ── Startup guard: don't re-place if already open or already filled ──
        market_end = datetime(
            target_date.year, target_date.month, target_date.day,
            MARKET_END_UTC_HOUR, 0, 0, tzinfo=timezone.utc,
        )
        cancel_at = (market_end - timedelta(hours=CANCEL_BEFORE_END_HOURS)).timestamp()

        existing_oid = find_open_buy_order(client, condition_id, token_id)
        if existing_oid:
            log.info(f"  [STARTUP] Resuming monitor for existing order: {label}")
            placed.append({
                "label": label, "order_id": existing_oid,
                "condition_id": condition_id, "token_id": token_id,
                "buy_price": price, "intended_price": order_price, "cancel_at": cancel_at,
                "bracket": bracket, "side_label": side_label, "target_date": target_date,
            })
            st_buy_existing(label, existing_oid, bracket, side_label,
                            target_date, price, order_size, cancel_at, token_id)
            if skip_slugs is not None:
                skip_slugs.add(skip_key)
            continue

        existing_shares = get_token_position(client, token_id)
        if existing_shares > 0:
            st_buy_filled("startup-" + token_id[:8], existing_shares,
                          token_id, price, label,
                          market_end=cancel_at + CANCEL_BEFORE_END_HOURS * 3600)
            open_sells = count_open_sells(client, condition_id, token_id)
            if open_sells > 0:
                log.info(
                    f"  [STARTUP] {open_sells} open SELL order(s) already exist for {label}"
                    f" — skipping sell placement"
                )
            elif time.time() >= cancel_at:
                # Past the cancel window — re-evaluate with expiry strategy
                # (handles restart after a HOLD decision where no sells were placed)
                log.info(
                    f"  [STARTUP] Existing position: {existing_shares} shares for {label}"
                    f" — within expiry window, re-evaluating"
                )
                _is_xrp = "xrp-above" in label
                exp_orders = sell_at_expiry(client, token_id, label, existing_shares, price,
                                            bracket, side_label, condition_id,
                                            price_fn=fetch_xrp_spot if _is_xrp else None,
                                            expiry_threshold=XRP_EXPIRY_DISTANCE_THRESHOLD if _is_xrp else None)
                if exp_orders:
                    threading.Thread(
                        target=monitor_tp_sells,
                        args=(client, condition_id, token_id, exp_orders),
                        daemon=True,
                    ).start()
            else:
                log.info(
                    f"  [STARTUP] Existing position: {existing_shares} shares for {label}"
                    f" — placing sell tranches"
                )
                tp_orders = place_sell_tranches(client, token_id, label, existing_shares, price)
                if tp_orders:
                    threading.Thread(
                        target=monitor_tp_sells,
                        args=(client, condition_id, token_id, tp_orders),
                        daemon=True,
                    ).start()
            threading.Thread(
                target=last_hour_sell_monitor,
                args=(client, token_id, label, condition_id, cancel_at),
                daemon=True,
            ).start()
            if skip_slugs is not None:
                skip_slugs.add(skip_key)
            continue
        # ─────────────────────────────────────────────────────────────────────

        # Skip new BUYs when too close to market close; existing positions handled above.
        hours_until_close = (market_end.timestamp() - time.time()) / 3600
        if hours_until_close <= CANCEL_BEFORE_END_HOURS:
            log.info(
                f"  [SKIP] {label} — {hours_until_close:.1f}h until close, "
                f"not placing new BUY"
            )
            continue

        order_id = place_limit_order(
            client, token_id, label, price, order_size, BUY
        )

        if order_id:
            placed.append({
                "label":          label,
                "order_id":       order_id,
                "condition_id":   condition_id,
                "token_id":       token_id,
                "buy_price":      price,
                "intended_price": order_price,
                "cancel_at":      cancel_at,
                "bracket":        bracket,
                "side_label":     side_label,
                "target_date":    target_date,
            })
            st_buy_placed(label, order_id, bracket, side_label,
                          target_date, price, order_size, cancel_at, token_id)
            if skip_slugs is not None:
                skip_slugs.add(skip_key)

    return placed

# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("ETH Daily Bracket Limit Order Bot")
    log.info(f"Mode  : {'DRY RUN' if DRY_RUN else 'LIVE'}")
    log.info(f"Order : {ORDER_SIZE} shares | inner=${ORDER_PRICE} | outer=${ORDER_PRICE_EXT}")
    log.info(f"TP1   : 50% @ 2x = ${ORDER_PRICE * 2:.4f}  (or best ask if higher)")
    log.info(f"TP2   : 25% @ 10x = ${ORDER_PRICE * 10:.4f}")
    log.info(f"TP3   : 25% @ 50x = ${ORDER_PRICE * 50:.4f}")
    log.info(f"XRP   : {'ENABLED' if XRP_ENABLED else 'disabled'}")
    log.info("=" * 60)

    start_dashboard()
    client = build_client()
    placed_dates:    set[date] = set()  # today dates fully placed
    tmrw_placed:     set[str]  = set()  # ETH tomorrow slugs placed (for retry)
    xrp_tmrw_placed: set[str]  = set()  # XRP tomorrow slugs placed (for retry)

    while True:
        utc_now  = datetime.now(timezone.utc)
        # After market close (16:00 UTC), the active window shifts forward —
        # Apr 16 closes at 16:00 UTC, so from then "today" is Apr 17 and "tomorrow" is Apr 18.
        if utc_now.hour >= MARKET_END_UTC_HOUR:
            today = utc_now.date() + timedelta(days=1)
        else:
            today = utc_now.date()
        tomorrow = today + timedelta(days=1)

        if today not in placed_dates:
            try:
                eth = fetch_eth_spot()
                upper, upper2, lower, lower2 = get_brackets(eth)
                st_set_eth(eth, upper, upper2, lower, lower2)

                eth_brackets = [
                    (upper,  "YES", 0, ORDER_PRICE,     ORDER_SIZE),
                    (upper2, "YES", 0, ORDER_PRICE_EXT, ORDER_SIZE),
                    (lower,  "NO",  1, ORDER_PRICE,     ORDER_SIZE),
                    (lower2, "NO",  1, ORDER_PRICE_EXT, ORDER_SIZE),
                ]

                all_orders: list[dict] = []

                # Always call place_for_date for today — it resumes existing positions
                # even when too close to place new BUYs (guard is inside place_for_date).
                all_orders += place_for_date(client, today, eth_brackets, build_slug)

                placed_dates.add(today)
                tmrw_placed.clear()
                xrp_tmrw_placed.clear()

                all_orders += place_for_date(client, tomorrow, eth_brackets, build_slug,
                                             skip_slugs=tmrw_placed)

                if XRP_ENABLED:
                    xrp = fetch_xrp_spot()
                    xrp_upper, xrp_lower = get_xrp_brackets(xrp)
                    st_set_xrp(xrp, xrp_upper, xrp_lower)
                    xrp_brackets = [
                        (xrp_upper, "YES", 0, XRP_ORDER_PRICE, XRP_ORDER_SIZE),
                        (xrp_lower, "NO",  1, XRP_ORDER_PRICE, XRP_ORDER_SIZE),
                    ]
                    all_orders += place_for_date(client, today, xrp_brackets, build_xrp_slug)
                    all_orders += place_for_date(client, tomorrow, xrp_brackets, build_xrp_slug,
                                                 skip_slugs=xrp_tmrw_placed)

                orphans = resync_orphan_buys(client, {o["order_id"] for o in all_orders})
                all_orders += orphans

                threading.Thread(
                    target=monitor_and_sell,
                    args=(client, all_orders),
                    daemon=True,
                ).start()

            except Exception as e:
                log.error(f"Placement cycle failed: {e}")

        else:
            # ETH retry: < 4 slugs placed for tomorrow
            if len(tmrw_placed) < 4:
                try:
                    eth = fetch_eth_spot()
                    upper, upper2, lower, lower2 = get_brackets(eth)
                    st_set_eth(eth, upper, upper2, lower, lower2)
                    eth_brackets = [
                        (upper,  "YES", 0, ORDER_PRICE,     ORDER_SIZE),
                        (upper2, "YES", 0, ORDER_PRICE_EXT, ORDER_SIZE),
                        (lower,  "NO",  1, ORDER_PRICE,     ORDER_SIZE),
                        (lower2, "NO",  1, ORDER_PRICE_EXT, ORDER_SIZE),
                    ]
                    retry_orders = place_for_date(client, tomorrow, eth_brackets, build_slug,
                                                  skip_slugs=tmrw_placed)
                    if retry_orders:
                        log.info(f"Tomorrow ETH retry: placed {len(retry_orders)} new order(s).")
                        threading.Thread(
                            target=monitor_and_sell,
                            args=(client, retry_orders),
                            daemon=True,
                        ).start()
                except Exception as e:
                    log.error(f"Tomorrow ETH retry failed: {e}")

            # XRP retry: < 2 slugs placed for tomorrow
            if XRP_ENABLED and len(xrp_tmrw_placed) < 2:
                try:
                    xrp = fetch_xrp_spot()
                    xrp_upper, xrp_lower = get_xrp_brackets(xrp)
                    st_set_xrp(xrp, xrp_upper, xrp_lower)
                    xrp_brackets = [
                        (xrp_upper, "YES", 0, XRP_ORDER_PRICE, XRP_ORDER_SIZE),
                        (xrp_lower, "NO",  1, XRP_ORDER_PRICE, XRP_ORDER_SIZE),
                    ]
                    retry_orders = place_for_date(client, tomorrow, xrp_brackets, build_xrp_slug,
                                                  skip_slugs=xrp_tmrw_placed)
                    if retry_orders:
                        log.info(f"Tomorrow XRP retry: placed {len(retry_orders)} new order(s).")
                        threading.Thread(
                            target=monitor_and_sell,
                            args=(client, retry_orders),
                            daemon=True,
                        ).start()
                except Exception as e:
                    log.error(f"Tomorrow XRP retry failed: {e}")

        time.sleep(LOOP_SLEEP)


if __name__ == "__main__":
    main()
