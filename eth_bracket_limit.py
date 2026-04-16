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

  BUY orders are cancelled 2 hours before market resolution (22:00 UTC).
  SELL orders are placed-and-forget.

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
"""

import math
import os
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
ORDER_PRICE        = float(os.getenv("ORDER_PRICE", "0.004"))   # $ per share — snapped to market tick
ORDER_SIZE         = int(os.getenv("ORDER_SIZE", "10"))        # shares per side
CANCEL_BEFORE_END_HOURS = 2  # cancel unfilled BUY orders N hours before market resolves (midnight UTC)
POLL_SECS          = 60      # fill-check interval
LOOP_SLEEP         = 30      # main-loop tick (seconds)

GAMMA_API      = "https://gamma-api.polymarket.com"
CLOB_API       = "https://clob.polymarket.com"
HTTP_PORT      = int(os.getenv("DASHBOARD_PORT", "8765"))

# ── Logging ───────────────────────────────────────────────────────────────────

class _ColorFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        if not _COLORS:
            return msg
        if "[DRY RUN]" in msg:
            return Fore.CYAN + msg + Style.RESET_ALL
        if any(t in msg for t in ("FILLED", "SELL TP", "placed")):
            return Fore.GREEN + msg + Style.RESET_ALL
        if any(t in msg for t in ("CANCEL", "ERROR", "error", "not found")):
            return Fore.RED + msg + Style.RESET_ALL
        if "WARNING" in msg or "warn" in msg.lower():
            return Fore.YELLOW + msg + Style.RESET_ALL
        return msg

_fmt = "%(asctime)s [%(levelname)s] %(message)s"
_handler = logging.StreamHandler(
    open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
)
_handler.setFormatter(_ColorFormatter(_fmt))
_file_handler = logging.FileHandler("eth_bracket_limit.log", encoding="utf-8")
_file_handler.setFormatter(logging.Formatter(_fmt))
logging.basicConfig(level=logging.INFO, handlers=[_handler, _file_handler], force=True)
log = logging.getLogger(__name__)

# ── Global dashboard state ────────────────────────────────────────────────────

_slock = threading.Lock()
_state: dict = {
    "mode":       "DRY RUN" if DRY_RUN else "LIVE",
    "eth_price":  0.0,
    "upper":      0,
    "lower":      0,
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

def st_set_eth(price: float, upper: int, lower: int) -> None:
    with _slock:
        _state["eth_price"] = price
        _state["upper"]     = upper
        _state["lower"]     = lower
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
                  buy_price: float, label: str) -> int:
    pos_id = _new_id()
    with _slock:
        for o in _state["buy_orders"]:
            if o["order_id"] == order_id:
                o["status"] = "FILLED"
                break
        _state["positions"].append({
            "id": pos_id, "label": label, "token_id": token_id,
            "shares": filled_shares, "buy_price": buy_price,
            "current_ask": None,
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

def st_sell_placed(label: str, order_id: str, tp: int,
                   shares: int, price: float) -> None:
    with _slock:
        _state["sell_orders"].append({
            "id": _new_id(), "label": label, "order_id": order_id,
            "tp": tp, "shares": shares, "price": price, "status": "OPEN",
        })
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
    for o in snap["buy_orders"]:
        secs = o["cancel_at"] - now
        o["cancel_dt"] = datetime.fromtimestamp(
            o["cancel_at"], tz=timezone.utc).strftime("%b %-d %H:%M UTC")
        o["mins_left"] = round(secs / 60, 1) if secs > 0 else 0
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

function render(s){
  const st=s.stats||{};
  const pnl=parseFloat(st.pnl||0);
  const mode=s.mode==='DRY RUN'?badge('b-dry','DRY RUN'):badge('b-live','⚡ LIVE');
  const buys=s.buy_orders||[];
  const pos=s.positions||[];
  const sells=s.sell_orders||[];
  const closed=s.closed||[];
  const openBuys=buys.filter(o=>o.status==='OPEN'||o.status==='RESUMING').length;
  const totalCost=buys.filter(o=>o.status!=='CANCELLED').reduce((a,o)=>a+(o.cost||0),0);

  // BUY orders table
  const buyRows=buys.length?buys.map(o=>{
    const tLeft=o.mins_left>0?
      (o.mins_left>60?`${(o.mins_left/60).toFixed(1)}h`:`${Math.round(o.mins_left)}m`)
      :'<span class="red">past</span>';
    return `<tr>
      <td>${o.date_str}</td>
      <td>${o.bracket.toLocaleString()}</td>
      <td class="${o.side==='YES'?'green':'amber'}">${o.side}</td>
      <td>${fmt4(o.price)}</td>
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
    const ask=p.current_ask!=null?fmt4(p.current_ask):'<span class="dim">—</span>';
    const unreal=p.current_ask!=null?fmtPnl((p.current_ask-p.buy_price)*p.shares):'<span class="dim">—</span>';
    const sellsForPos=sells.filter(o=>o.label&&o.label.startsWith(p.label));
    const tpPills=sellsForPos.map(o=>`
      <div class="tp-pill ${['','b-tp1','b-tp2','b-tp3'][o.tp]||'b-tp1'}">
        TP${o.tp} ${o.shares}sh @ ${fmt4(o.price)} <span class="${o.status==='FILLED'?'green':'dim'}">${o.status}</span>
      </div>`).join('');
    return `<div class="pos-card">
      <div class="pos-hdr">
        <strong style="font-size:13px">${p.label}</strong>
        <span class="green" style="font-size:12px">${p.shares} shares filled</span>
      </div>
      <div class="pos-meta">
        <span>Buy @ ${fmt4(p.buy_price)}</span>
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
    <td class="${c.side==='YES'?'green':'amber'}">${c.side||'—'}</td>
    <td>${fmt4(c.buy_price)}</td>
    <td>${badge(c.exit_type==='CANCEL'?'b-cancel':c.exit_type==='TP1'?'b-tp1':c.exit_type==='TP2'?'b-tp2':'b-tp3',c.exit_type||'—')}</td>
    <td>${c.exit_price!=null?fmt4(c.exit_price):'—'}</td>
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
      <div class="card"><div class="lbl">Lower Bracket</div><div class="val amber">${s.lower?'$'+s.lower.toLocaleString():'—'}</div></div>
      <div class="card"><div class="lbl">Open Orders</div><div class="val blue">${openBuys}</div></div>
      <div class="card"><div class="lbl">Filled</div><div class="val green">${st.fills||0}</div></div>
      <div class="card"><div class="lbl">Cancelled</div><div class="val ${(st.cancelled||0)>0?'red':'dim'}">${st.cancelled||0}</div></div>
      <div class="card"><div class="lbl">Cost</div><div class="val">${fmt4(totalCost)}</div></div>
      <div class="card"><div class="lbl">Net PnL</div><div class="val ${pnl>0?'green':pnl<0?'red':'dim'}">${pnl>=0?'+':''}${fmt4(pnl)}</div></div>
    </div>

    <div class="section">
      <h2>BUY Orders (${buys.length})</h2>
      <div style="overflow-x:auto">
      <table>
        <thead><tr>
          <th>Date</th><th>Bracket</th><th>Side</th><th>Price</th>
          <th>Shares</th><th>Cost</th><th>Status</th><th>Cancel At</th><th>Time Left</th>
        </tr></thead>
        <tbody>${buyRows}</tbody>
      </table>
      </div>
    </div>

    <div class="section">
      <h2>Positions — Filled &amp; Awaiting Sell (${pos.length})</h2>
      ${posCards}
    </div>

    <div class="section">
      <h2>Sell Orders (${sells.length})</h2>
      ${sells.length?`<div style="overflow-x:auto"><table>
        <thead><tr><th>Market</th><th>TP</th><th>Shares</th><th>Sell @</th><th>Status</th></tr></thead>
        <tbody>${sells.map(o=>`<tr>
          <td>${o.label||'—'}</td>
          <td>${tpBadge(o.tp)}</td>
          <td>${o.shares}</td>
          <td>${fmt4(o.price)}</td>
          <td>${statusBadge(o.status)}</td>
        </tr>`).join('')}</tbody>
      </table></div>`
      :'<p class="dim" style="font-size:12px;padding:4px 0">No sell orders yet</p>'}
    </div>

    <div class="section">
      <h2>Closed Trades (${closed.length})</h2>
      <div style="overflow-x:auto"><table>
        <thead><tr><th>Time</th><th>Market</th><th>Side</th><th>Buy @</th><th>Exit</th><th>Exit @</th><th>PnL</th></tr></thead>
        <tbody>${closedRows}</tbody>
      </table></div>
    </div>

    <div class="section">
      <h2>Settings</h2>
      <table style="max-width:480px"><tbody>
        <tr><td style="color:#5a6a85">Order price</td><td>$${(${ORDER_PRICE}||0.004).toFixed(4)}</td>
            <td style="color:#5a6a85;padding-left:24px">Order size</td><td>${ORDER_SIZE} shares</td></tr>
        <tr><td style="color:#5a6a85">Cancel before</td><td>${CANCEL_BEFORE_END_HOURS}h before market end</td>
            <td style="color:#5a6a85;padding-left:24px">Poll interval</td><td>${POLL_SECS}s</td></tr>
        <tr><td style="color:#5a6a85">TP1</td><td>50% @ 2x</td>
            <td style="color:#5a6a85;padding-left:24px">TP2</td><td>25% @ 10x</td></tr>
        <tr><td style="color:#5a6a85">TP3</td><td>25% @ 50x</td>
            <td style="color:#5a6a85;padding-left:24px">Dashboard</td><td>:${HTTP_PORT}</td></tr>
      </tbody></table>
    </div>

    <footer>Auto-refreshes every 10s &nbsp;&mdash;&nbsp; ETH Bracket Bot</footer>`;
}

async function poll(){
  try{
    const r=await fetch('/state');
    const d=await r.json();
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

# ── Bracket ───────────────────────────────────────────────────────────────────

def get_brackets(eth_price: float) -> tuple[int, int]:
    """
    Return (upper_bracket, lower_bracket) based on ETH spot price.

    Example: eth_price = 2333
      upper = ceil(2333 / 100) * 100       = 2400
      lower = floor(2333 / 100) * 100 - 100 = 2200
    """
    upper = int(math.ceil(eth_price / 100) * 100)
    lower = int(math.floor(eth_price / 100) * 100) - 100
    log.info(f"Brackets: upper={upper}, lower={lower} (ETH ${eth_price:,.2f})")
    return upper, lower

# ── Slug / Market ─────────────────────────────────────────────────────────────

def build_slug(bracket: int, target_date: date) -> str:
    """
    Build Polymarket slug for an 'Ethereum above X on [date]' market.
    Format: ethereum-above-{bracket}-on-{month}-{day}
    Example: ethereum-above-2400-on-april-16
    """
    date_str = target_date.strftime("%B-%-d").lower()
    return f"ethereum-above-{bracket}-on-{date_str}"


def get_tick_size(client, token_id: str, market: dict) -> float:
    """
    Return the minimum price tick for this market.

    1. Try known Gamma API field names.
    2. Infer from live orderbook ask-price increments.
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

    # 2. Infer from orderbook (skipped in dry-run — no client)
    if client is not None and not DRY_RUN:
        try:
            book = client.get_order_book(token_id)
            asks = book.get("asks", []) if isinstance(book, dict) else getattr(book, "asks", [])
            prices = sorted({
                float(a["price"] if isinstance(a, dict) else getattr(a, "price", None))
                for a in asks
                if (a.get("price") if isinstance(a, dict) else getattr(a, "price", None)) is not None
            })
            if len(prices) >= 2:
                diffs = [round(prices[i + 1] - prices[i], 6) for i in range(len(prices) - 1)]
                min_diff = min((d for d in diffs if d > 0), default=None)
                if min_diff is not None:
                    tick = 0.001 if min_diff <= 0.005 else 0.01
                    log.info(f"  tick_size={tick} (inferred from orderbook, min_diff={min_diff})")
                    return tick
        except Exception as e:
            log.debug(f"  tick_size inference failed: {e}")

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
        order_id = resp.get("orderID") or resp.get("id") or str(resp)
        log.info(
            f"  Limit {side_str} placed: {label}"
            f" | price=${price} | size={size} | order_id={order_id}"
        )
        return order_id
    except Exception as e:
        log.error(f"  Limit {side_str} failed ({label}): {e}")
        return None


def get_best_ask(client: ClobClient, token_id: str) -> float | None:
    """Fetch the best (lowest) ask price from the orderbook."""
    try:
        book = client.get_order_book(token_id)
        asks = None
        if isinstance(book, dict):
            asks = book.get("asks") or book.get("ask")
        else:
            asks = getattr(book, "asks", None)
        if not asks:
            return None
        prices = []
        for entry in asks:
            if isinstance(entry, dict):
                p = entry.get("price") or entry.get("p")
            else:
                p = getattr(entry, "price", None)
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
            raw = (
                details.get("sizeFilled")
                or details.get("size_filled")
                or details.get("filledSize")
                or 0
            )
        else:
            raw = getattr(details, "size_filled", 0) or getattr(details, "sizeFilled", 0)
        return int(float(raw))
    except Exception as e:
        log.warning(f"  get_filled_shares failed for {order_id}: {e}")
        return ORDER_SIZE  # assume full fill as fallback


def place_sell_tranches(
    client: ClobClient,
    token_id: str,
    label: str,
    filled_shares: int,
    buy_price: float,
) -> None:
    """
    Place 3 tiered GTC limit SELL orders after a BUY fills.

    TP1  50% shares  at max(buy_price * 2, best_ask)
    TP2  25% shares  at buy_price * 10
    TP3  remaining   at buy_price * 50
    """
    tp1_shares = math.floor(filled_shares * 0.50)
    tp2_shares = math.floor(filled_shares * 0.25)
    tp3_shares = filled_shares - tp1_shares - tp2_shares

    # TP1: check best ask; sell at whichever is higher
    tp1_base = round(buy_price * 2, 4)
    if not DRY_RUN:
        best_ask = get_best_ask(client, token_id)
        if best_ask is not None and best_ask > tp1_base:
            log.info(
                f"  TP1 price adjusted: best_ask={best_ask:.4f}"
                f" > 2x={tp1_base:.4f} → using best_ask"
            )
            tp1_base = round(best_ask, 4)
    else:
        log.info(f"  [DRY RUN] TP1 would check orderbook; using 2x=${tp1_base}")

    tp2_price = round(buy_price * 10, 4)
    tp3_price = round(buy_price * 50, 4)
    log.info(
        f"  Placing sell tranches for {label}"
        f" | filled={filled_shares} shares | buy_price=${buy_price}"
    )
    for tp_num, (tp_shares, tp_price) in enumerate(
        [(tp1_shares, tp1_base), (tp2_shares, tp2_price), (tp3_shares, tp3_price)], 1
    ):
        oid = place_limit_order(client, token_id, f"{label}-TP{tp_num}", tp_price, tp_shares, SELL)
        if oid:
            st_sell_placed(label, oid, tp_num, tp_shares, tp_price)


def get_open_order_ids(client: ClobClient, condition_id: str) -> set[str]:
    """Return set of open order IDs for this market condition."""
    if DRY_RUN:
        return set()
    try:
        open_orders = client.get_orders(OpenOrderParams(market=condition_id))
        if isinstance(open_orders, list):
            return {o.get("id") or o.get("orderID", "") for o in open_orders}
        return set()
    except Exception as e:
        log.warning(f"  Could not fetch open orders: {e}")
        return set()


def cancel_order(client: ClobClient, order_id: str, label: str) -> None:
    """Cancel a single order by ID."""
    if DRY_RUN:
        log.info(f"  [DRY RUN] CANCEL {label} order_id={order_id}")
        st_buy_cancelled(order_id)
        return
    try:
        client.cancel(order_id)
        log.info(f"  CANCELLED {label} order_id={order_id}")
        st_buy_cancelled(order_id)
    except Exception as e:
        log.error(f"  Cancel failed ({label} / {order_id}): {e}")

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
        time.sleep(POLL_SECS)
        now = time.time()

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

                # Cancel if deadline passed and order still open
                if now >= o["cancel_at"]:
                    if oid in open_ids:
                        mins_to_end = (o["cancel_at"] + CANCEL_BEFORE_END_HOURS * 3600 - now) / 60
                        log.warning(
                            f"  Cancelling {o['label']} — "
                            f"{CANCEL_BEFORE_END_HOURS}h before market end"
                            + (f" ({-mins_to_end:.0f}m past deadline)" if mins_to_end < 0 else "")
                        )
                        cancel_order(client, oid, o["label"])
                    pending.pop(oid)
                    continue

                # Detect fill (order no longer open)
                if oid not in open_ids:
                    pending.pop(oid)
                    filled = get_filled_shares(client, oid)
                    if filled > 0:
                        log.info(f"  FILLED ✓ {o['label']} | {filled} shares | order_id={oid}")
                        st_buy_filled(oid, filled, o["token_id"], o["buy_price"], o["label"])
                        place_sell_tranches(client, o["token_id"], o["label"], filled, o["buy_price"])
                    else:
                        log.info(f"  Order {o['label']} gone (0 fill) — skipping sell.")

        if not pending:
            log.info("All orders filled or gone.")
            return

        # Status line — show closest cancel deadline
        earliest = min(o["cancel_at"] for o in pending.values())
        mins_left = (earliest - now) / 60
        log.info(
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
        return int(float(raw or 0))
    except Exception as e:
        log.warning(f"  [STARTUP] position check failed for {token_id[:12]}: {e}")
    return 0


# ── Per-date placement ────────────────────────────────────────────────────────

def place_for_date(
    client: ClobClient,
    target_date: date,
    upper: int,
    lower: int,
    skip_slugs: set[str] | None = None,
) -> list[dict]:
    """
    For a given date, place BUY limit orders on the upper (YES) and lower (NO)
    brackets. Returns a list of order dicts for the monitor loop.
    Silently skips any bracket whose market is not yet live.

    skip_slugs: if provided, slugs already placed are skipped and newly placed
                slugs are added to the set (used for tomorrow retry logic).
    """
    date_label = target_date.strftime("%b %-d")
    log.info(f"--- {date_label} | upper={upper} YES | lower={lower} NO ---")
    placed = []

    brackets = [
        (upper, "YES", 0),  # (bracket, side_label, token_index)
        (lower, "NO",  1),
    ]

    for bracket, side_label, tok_idx in brackets:
        slug = build_slug(bracket, target_date)

        if skip_slugs is not None and slug in skip_slugs:
            log.info(f"  Already placed: {slug} — skipping")
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
        price = snap_price(ORDER_PRICE, tick)
        if price != ORDER_PRICE:
            log.info(f"  Price snapped: ${ORDER_PRICE} → ${price} (tick=${tick})")

        # ── Startup guard: don't re-place if already open or already filled ──
        cancel_at = datetime(
            target_date.year, target_date.month, target_date.day,
            24 - CANCEL_BEFORE_END_HOURS, 0, 0, tzinfo=timezone.utc,
        ).timestamp()

        existing_oid = find_open_buy_order(client, condition_id, token_id)
        if existing_oid:
            log.info(f"  [STARTUP] Resuming monitor for existing order: {label}")
            placed.append({
                "label": label, "order_id": existing_oid,
                "condition_id": condition_id, "token_id": token_id,
                "buy_price": price, "cancel_at": cancel_at,
            })
            st_buy_existing(label, existing_oid, bracket, side_label,
                            target_date, price, ORDER_SIZE, cancel_at, token_id)
            if skip_slugs is not None:
                skip_slugs.add(slug)
            continue

        existing_shares = get_token_position(client, token_id)
        if existing_shares > 0:
            log.info(
                f"  [STARTUP] Existing position: {existing_shares} shares for {label}"
                f" — placing sell tranches"
            )
            st_buy_filled("startup-" + token_id[:8], existing_shares,
                          token_id, price, label)
            place_sell_tranches(client, token_id, label, existing_shares, price)
            if skip_slugs is not None:
                skip_slugs.add(slug)
            continue
        # ─────────────────────────────────────────────────────────────────────

        order_id = place_limit_order(
            client, token_id, label, price, ORDER_SIZE, BUY
        )

        if order_id:
            placed.append({
                "label":        label,
                "order_id":     order_id,
                "condition_id": condition_id,
                "token_id":     token_id,
                "buy_price":    price,
                "cancel_at":    cancel_at,
            })
            st_buy_placed(label, order_id, bracket, side_label,
                          target_date, price, ORDER_SIZE, cancel_at, token_id)
            if skip_slugs is not None:
                skip_slugs.add(slug)

    return placed

# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("ETH Daily Bracket Limit Order Bot")
    log.info(f"Mode  : {'DRY RUN' if DRY_RUN else 'LIVE'}")
    log.info(f"Order : {ORDER_SIZE} shares @ ${ORDER_PRICE} each side")
    log.info(f"TP1   : 50% @ 2x = ${ORDER_PRICE * 2:.4f}  (or best ask if higher)")
    log.info(f"TP2   : 25% @ 10x = ${ORDER_PRICE * 10:.4f}")
    log.info(f"TP3   : 25% @ 50x = ${ORDER_PRICE * 50:.4f}")
    log.info("=" * 60)

    start_dashboard()
    client = build_client()
    placed_dates:     set[date] = set()  # today dates fully placed
    tmrw_placed:      set[str]  = set()  # tomorrow slugs already placed (for retry)

    while True:
        today    = datetime.now(timezone.utc).date()
        tomorrow = today + timedelta(days=1)

        if today not in placed_dates:
            try:
                eth   = fetch_eth_spot()
                upper, lower = get_brackets(eth)
                st_set_eth(eth, upper, lower)

                all_orders: list[dict] = []

                # Skip today if < 6h remain before midnight UTC resolution
                utc_now  = datetime.now(timezone.utc)
                hours_left = 24 - utc_now.hour - utc_now.minute / 60
                if hours_left <= 6:
                    log.info(
                        f"[SKIP] {hours_left:.1f}h until market end — "
                        f"skipping today's BUY orders (threshold: 6h)"
                    )
                else:
                    all_orders += place_for_date(client, today, upper, lower)

                placed_dates.add(today)
                tmrw_placed.clear()  # new day — reset tomorrow tracking

                tmrw_orders = place_for_date(client, tomorrow, upper, lower,
                                             skip_slugs=tmrw_placed)
                all_orders += tmrw_orders

                threading.Thread(
                    target=monitor_and_sell,
                    args=(client, all_orders),
                    daemon=True,
                ).start()

            except Exception as e:
                log.error(f"Placement cycle failed: {e}")

        else:
            # Retry tomorrow brackets that weren't live yet when the day started
            if len(tmrw_placed) < 2:
                try:
                    eth = fetch_eth_spot()
                    upper, lower = get_brackets(eth)
                    st_set_eth(eth, upper, lower)
                    retry_orders = place_for_date(client, tomorrow, upper, lower,
                                                  skip_slugs=tmrw_placed)
                    if retry_orders:
                        log.info(f"Tomorrow retry: placed {len(retry_orders)} new order(s).")
                        threading.Thread(
                            target=monitor_and_sell,
                            args=(client, retry_orders),
                            daemon=True,
                        ).start()
                except Exception as e:
                    log.error(f"Tomorrow retry failed: {e}")

        time.sleep(LOOP_SLEEP)


if __name__ == "__main__":
    main()
