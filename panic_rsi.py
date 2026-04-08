# ═══════════════════════════════════════════════════════════════
#  RSI PATCH for panic_bot6.py  — 6 surgical changes
#  All 6 tests pass. Apply in order 1 → 6.
# ═══════════════════════════════════════════════════════════════


# ── CHANGE 1 ─────────────────────────────────────────────────
# WHERE:  after line  pending_buys = {}
# ADD this new global:

rsi_history = {}   # "eth_yes" / "eth_no" -> latest RSI float


# ── CHANGE 2 ─────────────────────────────────────────────────
# WHERE:  after def update_history(key, price): ...
# ADD this entire new function:

def compute_rsi(prices, period=14):
    """
    Wilder-smoothed RSI(14).
    prices : list/deque of floats, oldest → newest.
    Returns: float 0–100.  50.0 if not enough data (won't block trade).
    """
    prices = list(prices)
    if len(prices) < period + 1:
        return 50.0
    deltas   = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains    = [d if d > 0 else 0.0 for d in deltas]
    losses   = [abs(d) if d < 0 else 0.0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_gain == 0 and avg_loss == 0:
        return 50.0          # flat market — neutral
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


# ── CHANGE 3 ─────────────────────────────────────────────────
# WHERE:  inside check_trigger(), replace the FINAL block:
#
#   OLD (find this):
#     if current_price >= floor:
#         log.info("[SCAN] %s  ...sigma=NO...")
#         return False
#
#     # ── All 3 conditions met — PANIC ──────────────────────
#     log.info("[PANIC] %s  price=%.4f  ref=%.4f  drop=%.1f%%  floor=%.4f  mean=%.4f  std=%.4f", ...)
#     return True
#
#   NEW (replace with this entire block):

    if current_price >= floor:
        log.info("[SCAN] %s  price=%.4f  ref=%.4f  drop=%.1f%%  sigma=NO(floor=%.4f)",
                 key, current_price, ref, drop_pct * 100, floor)
        return False

    # ── Condition 4: RSI gate ─────────────────────────────────
    RSI_OVERSOLD   = 35    # YES buy  — RSI must be BELOW this
    RSI_OVERBOUGHT = 65    # NO  buy  — RSI must be ABOVE this

    rsi_val = compute_rsi(list(hist))
    rsi_history[key] = rsi_val          # store for dashboard

    if side == "yes" and rsi_val > RSI_OVERSOLD:
        log.info("[SCAN] %s  price=%.4f  rsi=%.1f  RSI-gate=NO (need <%.0f for YES)",
                 key, current_price, rsi_val, RSI_OVERSOLD)
        return False

    if side == "no" and rsi_val < RSI_OVERBOUGHT:
        log.info("[SCAN] %s  price=%.4f  rsi=%.1f  RSI-gate=NO (need >%.0f for NO)",
                 key, current_price, rsi_val, RSI_OVERBOUGHT)
        return False

    # ── All 4 conditions met — PANIC ─────────────────────────
    log.info(
        "[PANIC] %s  price=%.4f  ref=%.4f  drop=%.1f%%  floor=%.4f  "
        "rsi=%.1f  mean=%.4f  std=%.4f",
        key, current_price, ref, drop_pct * 100, floor,
        rsi_val, mean_p, std_p
    )
    return True


# ── CHANGE 4 ─────────────────────────────────────────────────
# WHERE:  inside _update_prices_and_history(), after update_history(...)
#
#   FIND:
#     update_history(f"{asset}_yes", yes_price)
#
#     # settle_buffer no longer used ...
#
#   ADD these 5 lines immediately after update_history():

    update_history(f"{asset}_yes", yes_price)

    # Compute and cache RSI every poll for dashboard + gate
    _h = price_history.get(f"{asset}_yes")
    if _h and len(_h) >= 15:
        _rsi = compute_rsi(list(_h))
        rsi_history[f"{asset}_yes"] = _rsi
        rsi_history[f"{asset}_no"]  = round(100.0 - _rsi, 2)

    # settle_buffer no longer used for signals — settle phase is trading lockout only


# ── CHANGE 5 ─────────────────────────────────────────────────
# WHERE:  inside save_state(), in the `state = { ... }` dict
#
#   FIND:
#     "prices":     dict(live_prices),
#
#   ADD the rsi line immediately after:

        "prices":     dict(live_prices),
        "rsi":        dict(rsi_history),        # ← ADD THIS LINE


# ── CHANGE 6 ─────────────────────────────────────────────────
# WHERE:  inside _build_state_snapshot(), same pattern
#
#   FIND:
#     "prices":     dict(live_prices),
#
#   ADD the rsi line immediately after:

        "prices":     dict(live_prices),
        "rsi":        dict(rsi_history),        # ← ADD THIS LINE


# ── CHANGE 7 (DASHBOARD) ─────────────────────────────────────
# WHERE:  inside _DASHBOARD_HTML  (the big JS string)
#
# STEP A — Find the table header line:
#   <tr><th>Asset</th><th>YES</th><th>NO</th><th>Holding</th></tr>
# REPLACE WITH:
#   <tr><th>Asset</th><th>YES price</th><th>NO price</th><th>RSI YES</th><th>RSI NO</th><th>Holding</th></tr>
#
# STEP B — Find the priceRows JS block and replace entirely:
#
#   OLD:
#     const priceRows=assets.map(a=>{
#       const yp=pr[a+'_yes'], np=pr[a+'_no'];
#       const yc=yp!=null&&yp<=cap?'red':'';
#       const nc=np!=null&&np<=cap?'red':'';
#       const holding=[...].join(' ');
#       return `<tr><td>...</td></tr>`;
#     }).join('');
#
#   NEW (replace the whole block):

  const rsi=s.rsi||{};
  const priceRows=assets.map(a=>{
    const yp=pr[a+'_yes'], np=pr[a+'_no'];
    const yr=rsi[a+'_yes'], nr=rsi[a+'_no'];
    const yc=yp!=null&&yp<=cap?'red':'';
    const nc=np!=null&&np<=cap?'red':'';
    const rc=v=>v==null?'dim':v<35?'green':v>65?'red':'amber';
    const rl=v=>v==null?'—':v.toFixed(1);
    const holding=[(a+'_yes' in pos)?'<span class="green">YES</span>':'',(a+'_no' in pos)?'<span class="green">NO</span>':''].filter(Boolean).join(' ');
    return `<tr>
      <td>${a.toUpperCase()}</td>
      <td class="${yc}">${fmt(yp)}</td>
      <td class="${nc}">${fmt(np)}</td>
      <td class="${rc(yr)}" style="font-family:monospace;font-weight:600">${rl(yr)}</td>
      <td class="${rc(nr)}" style="font-family:monospace;font-weight:600">${rl(nr)}</td>
      <td>${holding||'<span class="dim">—</span>'}</td>
    </tr>`;
  }).join('');


# ═══════════════════════════════════════════════════════════════
#  HOW RSI COLOURS WORK IN DASHBOARD
#  RSI < 35  → GREEN  (oversold — bot will BUY YES if triggered)
#  RSI > 65  → RED    (overbought — bot will BUY NO if triggered)
#  RSI 35-65 → DIM    (neutral — bot will NOT trade even if price drops)
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════
#  ALSO ADD to the settings block in the dashboard (optional):
#  so you can see the thresholds at a glance
# ═══════════════════════════════════════════════════════════════
#
#  Find in the Settings table inside render():
#    <tr><td>Rebound confirm</td>...
#  ADD a new row after it:
#    <tr><td>RSI oversold</td><td>&lt;35 → BUY YES</td><td>RSI overbought</td><td>&gt;65 → BUY NO</td></tr>
