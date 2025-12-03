# 每单下单金额（默认名义价值为6元，也就是保证金0.3元）
# 每单止损：每单下单金额*0.5 (例如默认名义价值下单6元，则止损为3元)
# 开仓最大账户保证金率：默认为15%（接口：/fapi/v3/account 计算方式=totalMaintMargin ÷ totalMarginBalance × 100）
import os
import json
import time
import hmac
import hashlib
import threading
from collections import deque
from datetime import datetime
from urllib import parse
import math
import requests
import argparse

API_URL = "https://fapi.binance.com"
RECV_WINDOW = 60000

ORDER_NOTIONAL = float(os.getenv("ORDER_NOTIONAL", "6"))
ORDER_STOP_NOTIONAL = ORDER_NOTIONAL * 0.5
MAX_OPEN_MARGIN_RATIO = float(os.getenv("MAX_OPEN_MARGIN_RATIO", "15"))

time_offset_ms = 0

def _sign(params: dict, secret: str) -> str:
    q = parse.urlencode(params, doseq=True)
    return hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()

def load_config(path: str) -> dict:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def now_ms() -> int:
    return int(time.time() * 1000) + int(time_offset_ms)

def api_get(path: str, params: dict | None = None, signed: bool = False, api_key: str | None = None, secret: str | None = None, timeout: float = 10.0):
    params = dict(params or {})
    headers = {}
    if signed:
        ts = now_ms()
        params.update({"timestamp": ts, "recvWindow": RECV_WINDOW})
        sig = _sign(params, secret or "")
        params["signature"] = sig
        headers["X-MBX-APIKEY"] = api_key or ""
    else:
        if api_key:
            headers["X-MBX-APIKEY"] = api_key
    r = requests.get(f"{API_URL}{path}", params=params, headers=headers, timeout=timeout)
    return r.json()

def api_post(path: str, params: dict | None = None, signed: bool = False, api_key: str | None = None, secret: str | None = None, timeout: float = 10.0):
    params = dict(params or {})
    headers = {}
    if signed:
        ts = now_ms()
        params.update({"timestamp": ts, "recvWindow": RECV_WINDOW})
        sig = _sign(params, secret or "")
        params["signature"] = sig
        headers["X-MBX-APIKEY"] = api_key or ""
    else:
        if api_key:
            headers["X-MBX-APIKEY"] = api_key
    r = requests.post(f"{API_URL}{path}", params=params, headers=headers, timeout=timeout)
    return r.json()

def sync_time():
    global time_offset_ms
    try:
        j = api_get("/fapi/v1/time")
        if isinstance(j, dict) and "serverTime" in j:
            server_ms = int(j["serverTime"])
            local_ms = int(time.time() * 1000)
            time_offset_ms = server_ms - local_ms
    except Exception:
        pass

# 全局状态
selected_symbols = set()
symbol_filters: dict[str, dict] = {}
prices: dict[str, deque] = {}
last_macd_dir: dict[str, str] = {}
last_attempt_at: dict[tuple[str, str], float] = {}
fast_symbols = set()
printed_start = False
printed_data_acc = False
printed_ready = False
printed_trade_banner = False

g_api_key = ""
g_secret = ""

lock = threading.Lock()
countdown_done = False
countdown_start = None
pos_pnl_history: dict[tuple[str, str], deque] = {}
pos_first_seen: dict[tuple[str, str], float] = {}



def fetch_exchange_info():
    j = api_get("/fapi/v1/exchangeInfo")
    return j

def leverage_bracket_max(symbol: str) -> int:
    try:
        arr = api_get("/fapi/v1/leverageBracket", {"symbol": symbol}, signed=True, api_key=g_api_key, secret=g_secret)
        if isinstance(arr, list) and arr:
            brackets = arr[0].get("brackets", [])
            mx = 0
            for b in brackets:
                try:
                    lv = int(b.get("initialLeverage", 0))
                    mx = max(mx, lv)
                except Exception:
                    continue
            return mx or 20
    except Exception:
        pass
    return 20

def set_leverage_for_symbols():
    info = fetch_exchange_info()
    if not isinstance(info, dict):
        print("获取 exchangeInfo 失败")
        return
    syms = [s for s in info.get("symbols", []) if s.get("status") == "TRADING"]
    for s in syms:
        sym = s.get("symbol")
        if not sym:
            continue
        target = 50 if sym in ("BTCUSDT", "ETHUSDT") else 20
        try:
            j = api_post("/fapi/v1/leverage", {"symbol": sym, "leverage": target}, signed=True, api_key=g_api_key, secret=g_secret)
            if isinstance(j, dict) and j.get("leverage"):
                continue
            mx = leverage_bracket_max(sym)
            tgt = min(target, mx)
            j2 = api_post("/fapi/v1/leverage", {"symbol": sym, "leverage": tgt}, signed=True, api_key=g_api_key, secret=g_secret)
            # 静默处理
        except Exception as e:
            pass
    print("✓ 杠杆设置完成")

def get_filters_from_symbol(symbol_obj: dict) -> dict:
    filters = symbol_obj.get("filters", [])
    step = None
    tick = None
    min_qty = None
    min_notional = None
    for f in filters:
        t = f.get("filterType")
        if t == "LOT_SIZE":
            step = float(f.get("stepSize", 0) or 0)
            min_qty = float(f.get("minQty", 0) or 0)
        elif t == "PRICE_FILTER":
            tick = float(f.get("tickSize", 0) or 0)
        elif t == "MIN_NOTIONAL":
            v = f.get("notional", f.get("minNotional", 0))
            try:
                min_notional = float(v or 0)
            except Exception:
                min_notional = 0.0
    return {
        "stepSize": step or 0.0,
        "tickSize": tick or 0.0,
        "minQty": min_qty or 0.0,
        "minNotional": min_notional or 0.0,
        "quantityPrecision": int(symbol_obj.get("quantityPrecision", symbol_obj.get("baseAssetPrecision", 0)) or 0),
        "pricePrecision": int(symbol_obj.get("pricePrecision", symbol_obj.get("quotePrecision", 0)) or 0),
    }

def filter_symbols_once():
    info = fetch_exchange_info()
    if not isinstance(info, dict):
        with lock:
            selected_symbols.clear()
            symbol_filters.clear()
            for sym in ("BTCUSDT", "ETHUSDT"):
                selected_symbols.add(sym)
                symbol_filters[sym] = {
                    "stepSize": 0.0,
                    "tickSize": 0.0,
                    "minQty": 0.0,
                    "minNotional": 0.0,
                    "quantityPrecision": 0,
                    "pricePrecision": 0,
                }
                if sym not in prices:
                    prices[sym] = deque(maxlen=1500)
        print(f"✓ 使用默认交易对: {len(selected_symbols)}")
        return
    raw = []
    for s in info.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        sym = s.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        if sym in ("USDTUSDC", "USDCUSDT"):
            continue
        flt = get_filters_from_symbol(s)
        if (flt.get("minNotional") or 0) > 10:
            continue
        raw.append((sym, flt))
    try:
        arr = api_get("/fapi/v1/ticker/24hr")
    except Exception:
        arr = []
    vol_map = {}
    if isinstance(arr, list):
        for it in arr:
            sym = it.get("symbol")
            try:
                qv = float(it.get("quoteVolume", it.get("volume", 0)) or 0)
            except Exception:
                qv = 0.0
            vol_map[sym] = qv
    raw2 = [(sym, flt, vol_map.get(sym, 0.0)) for sym, flt in raw if vol_map.get(sym, 0.0) >= 30000000]
    raw2.sort(key=lambda x: x[2], reverse=True)
    with lock:
        selected_symbols.clear()
        symbol_filters.clear()
        for sym, flt, _ in raw2:
            selected_symbols.add(sym)
            symbol_filters[sym] = flt
            if sym not in prices:
                prices[sym] = deque(maxlen=1500)
        # 选取涨幅榜前10作为秒级处理的交易对
        gain_map = {}
        if isinstance(arr, list):
            for it in arr:
                sym = it.get("symbol")
                try:
                    pct = float(it.get("priceChangePercent", 0) or 0)
                except Exception:
                    pct = 0.0
                gain_map[sym] = pct
        top10 = sorted([sym for sym in selected_symbols], key=lambda s: gain_map.get(s, 0.0), reverse=True)[:10]
        fast_symbols.clear()
        for s in top10:
            fast_symbols.add(s)
    print(f"✓ 符合条件的交易对数量: {len(selected_symbols)}")

def symbol_filter_loop():
    while True:
        try:
            filter_symbols_once()
        except Exception:
            pass
        time.sleep(600)

def get_series_stats() -> tuple[int, int]:
    with lock:
        if not selected_symbols:
            return 0, 0
        mx = 0
        n = 0
        for sym in selected_symbols:
            dq = prices.get(sym)
            mx = max(mx, len(dq) if dq else 0)
            n += 1
        return mx, n

def countdown_loop():
    global countdown_done
    global countdown_start
    next_tick = time.perf_counter()
    while True:
        mx, n = get_series_stats()
        if not printed_data_acc:
            print("\n========== 数据积累 ==========")
            globals()['printed_data_acc'] = True
        if mx >= 600:
            if not countdown_done:
                print("\nK线数据已就绪，开始监控交易信号...")
                print("\n========== 交易执行 ==========")
                countdown_done = True
                globals()['printed_ready'] = True
            break
        rem = max(0, 600 - mx)
        print(f"等待K线数据积累... 当前最大: {mx}/600  剩余: {rem}秒")
        next_tick += 1.0
        delay = next_tick - time.perf_counter()
        if delay > 0:
            time.sleep(delay)

def ema(series: list[float], period: int) -> list[float]:
    if not series or period <= 0:
        return []
    k = 2 / (period + 1)
    out = []
    ema_prev = series[0]
    for x in series:
        ema_prev = x * k + ema_prev * (1 - k)
        out.append(ema_prev)
    return out

def sma(series: list[float], period: int) -> list[float]:
    out = []
    s = 0.0
    q = deque()
    for x in series:
        q.append(x)
        s += x
        if len(q) > period:
            s -= q.popleft()
        out.append(s / (len(q)))
    return out

def rolling_std(series: list[float], period: int) -> list[float]:
    out = []
    q = deque()
    s = 0.0
    s2 = 0.0
    for x in series:
        q.append(x)
        s += x
        s2 += x * x
        if len(q) > period:
            y = q.popleft()
            s -= y
            s2 -= y * y
        n = len(q)
        mean = s / n
        var = (s2 / n) - (mean * mean)
        out.append(math.sqrt(max(var, 0.0)))
    return out

def boll(series: list[float], period: int, width: float = 1.0):
    mid = sma(series, period)
    sd = rolling_std(series, period)
    up = [m + width * v for m, v in zip(mid, sd)]
    down = [m - width * v for m, v in zip(mid, sd)]
    return mid, up, down

def macd(series: list[float], fast: int, slow: int, signal: int):
    e_fast = ema(series, fast)
    e_slow = ema(series, slow)
    dif = [a - b for a, b in zip(e_fast, e_slow)]
    dea = ema(dif, signal)
    return dif, dea

def rsi(series: list[float], period: int) -> list[float]:
    out = []
    prev = None
    avg_gain = 0.0
    avg_loss = 0.0
    for i, x in enumerate(series):
        if prev is None:
            out.append(50.0)
            prev = x
            continue
        change = x - prev
        prev = x
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        if i == 1:
            avg_gain = gain
            avg_loss = loss
        else:
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = (avg_gain / avg_loss) if avg_loss > 1e-12 else 999999.0
        val = 100.0 - (100.0 / (1.0 + rs))
        out.append(val)
    return out

def kdj_from_close(series: list[float], n: int, k_period: int, d_period: int):
    if not series:
        return [], [], []
    hh = []
    ll = []
    q = deque()
    for x in series:
        q.append(x)
        if len(q) > n:
            q.popleft()
        hh.append(max(q))
        ll.append(min(q))
    rsv = []
    for x, h, l in zip(series, hh, ll):
        if h == l:
            rsv.append(50.0)
        else:
            rsv.append((x - l) / (h - l) * 100.0)
    k = sma(rsv, k_period)
    d = sma(k, d_period)
    j = [3 * a - 2 * b for a, b in zip(k, d)]
    return k, d, j

def get_account_info():
    j = api_get("/fapi/v3/account", signed=True, api_key=g_api_key, secret=g_secret)
    return j if isinstance(j, dict) else {}

def get_positions():
    arr = api_get("/fapi/v3/positionRisk", signed=True, api_key=g_api_key, secret=g_secret)
    return arr if isinstance(arr, list) else []

def current_margin_ratio() -> float:
    acc = get_account_info()
    try:
        tmm = float(acc.get("totalMaintMargin", 0) or 0)
        tmb = float(acc.get("totalMarginBalance", 0) or 0)
        if tmb <= 0:
            return 0.0
        return tmm / tmb * 100.0
    except Exception:
        return 0.0

def adjust_qty(sym: str, qty: float) -> float:
    flt = symbol_filters.get(sym, {})
    step = float(flt.get("stepSize", 0) or 0)
    min_qty = float(flt.get("minQty", 0) or 0)
    if step and qty > 0:
        qty = math.floor(qty / step) * step
    if qty < min_qty:
        return 0.0
    # 避免科学计数法/浮点误差
    qp = int(flt.get("quantityPrecision", 0) or 0)
    qty = float(f"{qty:.{max(qp, 0)}f}")
    return qty

def adjust_price(sym: str, price: float) -> float:
    flt = symbol_filters.get(sym, {})
    tick = float(flt.get("tickSize", 0) or 0)
    pp = int(flt.get("pricePrecision", 0) or 0)
    if tick and price > 0:
        price = round(price / tick) * tick
    price = float(f"{price:.{max(pp, 0)}f}")
    return price

def fmt_price(sym: str, price: float) -> float:
    pp = int(symbol_filters.get(sym, {}).get("pricePrecision", 0) or 0)
    return float(f"{price:.{max(pp, 0)}f}")

def place_market(sym: str, side: str, position_side: str, qty: float) -> dict:
    p = {"symbol": sym, "side": side, "type": "MARKET", "positionSide": position_side, "quantity": qty}
    j = api_post("/fapi/v1/order", p, signed=True, api_key=g_api_key, secret=g_secret)
    return j if isinstance(j, dict) else {"error": j}

def query_order(sym: str, order_id: int | None = None, client_id: str | None = None) -> dict:
    p = {"symbol": sym}
    if order_id is not None:
        p["orderId"] = order_id
    if client_id:
        p["origClientOrderId"] = client_id
    j = api_get("/fapi/v1/order", p, signed=True, api_key=g_api_key, secret=g_secret)
    return j if isinstance(j, dict) else {}

def wait_fill_avg_price(sym: str, order_id: int | None, client_id: str | None) -> float:
    for _ in range(30):
        j = query_order(sym, order_id, client_id)
        try:
            ap = float(j.get("avgPrice", 0) or 0)
        except Exception:
            ap = 0.0
        if ap > 0:
            return ap
        time.sleep(1)
    return 0.0

def place_tp_trailing(sym: str, position_side: str, entry: float, qty: float, is_long: bool):
    if is_long:
        activation = entry * 1.02
        side = "SELL"
    else:
        activation = entry * 0.98
        side = "BUY"
    activation = adjust_price(sym, activation)
    p = {
        "symbol": sym,
        "side": side,
        "type": "TRAILING_STOP_MARKET",
        "positionSide": position_side,
        "quantity": qty,
        "activationPrice": activation,
        "callbackRate": 0.5,
    }
    j = api_post("/fapi/v1/order", p, signed=True, api_key=g_api_key, secret=g_secret)
    return j

def place_tp_limit_or_market(sym: str, position_side: str, entry: float, qty: float, is_long: bool, use_market: bool = True):
    if is_long:
        target = entry * 1.01
        side = "SELL"
    else:
        target = entry * 0.99
        side = "BUY"
    target = adjust_price(sym, target)
    if use_market:
        p = {
            "symbol": sym,
            "side": side,
            "type": "TAKE_PROFIT_MARKET",
            "positionSide": position_side,
            "stopPrice": target,
            "quantity": qty,
        }
    else:
        p = {
            "symbol": sym,
            "side": side,
            "type": "TAKE_PROFIT",
            "positionSide": position_side,
            "price": target,
            "stopPrice": target,
            "quantity": qty,
        }
    j = api_post("/fapi/v1/order", p, signed=True, api_key=g_api_key, secret=g_secret)
    return j

def latest_ohlc_1m(sym: str):
    arr = api_get("/fapi/v1/klines", {"symbol": sym, "interval": "1m", "limit": 1})
    if isinstance(arr, list) and arr:
        it = arr[0]
        try:
            o = float(it[1]); h = float(it[2]); l = float(it[3]); c = float(it[4])
            return o, h, l, c
        except Exception:
            return None
    return None

def klines_1m_quote_vol(sym: str, limit: int = 1000) -> list[float]:
    arr = api_get("/fapi/v1/klines", {"symbol": sym, "interval": "1m", "limit": limit})
    out = []
    if isinstance(arr, list):
        for it in arr:
            try:
                qv = float(it[7]) if len(it) > 7 else float(it[5])
            except Exception:
                qv = 0.0
            out.append(qv)
    return out

def fmt_time(ts: int | None = None) -> str:
    d = datetime.fromtimestamp(((ts or int(time.time() * 1000)))/1000.0)
    return d.strftime("%Y-%m-%d %H:%M:%S")

def ticker_loop():
    next_tick = time.perf_counter()
    while True:
        try:
            j = api_get("/fapi/v2/ticker/price")
            mp = {}
            if isinstance(j, list):
                with lock:
                    sel = set(selected_symbols)
                for it in j:
                    sym = it.get("symbol")
                    if sym in sel:
                        try:
                            p = float(it.get("price", 0) or 0)
                        except Exception:
                            p = 0.0
                        mp[sym] = p
            now = int(time.time() * 1000)
            with lock:
                for sym, price in mp.items():
                    dq = prices.get(sym)
                    if dq is None:
                        dq = deque(maxlen=1500)
                        prices[sym] = dq
                    dq.append({"t": now, "p": price})
        except Exception:
            pass
        next_tick += 1.0
        delay = next_tick - time.perf_counter()
        if delay > 0:
            time.sleep(delay)

def second_eval_loop():
    import concurrent.futures
    next_tick = time.perf_counter()
    while True:
        try:
            with lock:
                syms = list(selected_symbols)
            def eval_symbol(sym: str):
                dq = prices.get(sym, deque())
                if len(dq) < 600:
                    return False
                closes = [x["p"] for x in dq]
                mid, up, down = boll(closes, 200, 1)
                ema7 = ema(closes, 7)
                ema25 = ema(closes, 25)
                ema60 = ema(closes, 60)
                ema100 = ema(closes, 100)
                ema200 = ema(closes, 200)
                ema500 = ema(closes, 500)
                dif, dea = macd(closes, 60, 200, 60)
                rsi80 = rsi(closes, 80)
                rsi200 = rsi(closes, 200)
                rsi500 = rsi(closes, 500)
                k, d, jv = kdj_from_close(closes, 20, 80, 200)
                idx = len(closes) - 1
                cp = closes[idx]
                cond_long1 = (
                    cp > ema500[idx] and
                    cp > up[idx] and
                    (ema7[idx] > ema25[idx] > ema60[idx] > ema100[idx] > ema200[idx] > ema500[idx]) and
                    any(ema7[idx - i] < ema500[idx - i] for i in range(1, min(10, idx)+1)) and
                    dif[idx] > dea[idx] and
                    (rsi80[idx] > rsi200[idx] and rsi80[idx] > rsi500[idx]) and
                    ((k[idx] > d[idx] > jv[idx]) or all(k[idx - i] > k[idx - i - 1] and jv[idx - i] > jv[idx - i - 1] for i in range(1, min(3, idx))) )
                )
                cond_long2 = (
                    cp > ema500[idx] and
                    cp > up[idx] and
                    (ema7[idx] > ema25[idx] > ema60[idx] > ema100[idx] > ema200[idx] > ema500[idx]) and
                    any(ema7[idx - i] < ema25[idx - i] for i in range(1, min(3, idx)+1)) and
                    dif[idx] > dea[idx] and
                    (rsi80[idx] > rsi200[idx] and rsi80[idx] > rsi500[idx])
                )
                cond_short1 = (
                    cp < ema500[idx] and
                    cp < down[idx] and
                    (ema7[idx] < ema25[idx] < ema60[idx] < ema100[idx] < ema200[idx] < ema500[idx]) and
                    any(ema7[idx - i] > ema500[idx - i] for i in range(1, min(10, idx)+1)) and
                    dif[idx] < dea[idx] and
                    (rsi80[idx] < rsi200[idx] and rsi80[idx] < rsi500[idx]) and
                    ((k[idx] < d[idx] < jv[idx]) or all(k[idx - i] < k[idx - i - 1] and jv[idx - i] < jv[idx - i - 1] for i in range(1, min(3, idx))) )
                )
                cond_short2 = (
                    cp < ema500[idx] and
                    cp < down[idx] and
                    (ema7[idx] < ema25[idx] < ema60[idx] < ema100[idx] < ema200[idx] < ema500[idx]) and
                    any(ema7[idx - i] > ema25[idx - i] for i in range(1, min(3, idx)+1)) and
                    dif[idx] < dea[idx] and
                    (rsi80[idx] < rsi200[idx] and rsi80[idx] < rsi500[idx])
                )
                def pass_macd_once(dir_flag: str) -> bool:
                    last = last_macd_dir.get(sym)
                    if last == dir_flag:
                        return False
                    return True
                signal = None
                if cond_long1 and pass_macd_once("LONG_DIF_GT_DEA"):
                    vols = klines_1m_quote_vol(sym, 1000)
                    if len(vols) >= 2:
                        if vols[-1] >= 2 * vols[-2] and (sum(vols[-20:]) / 20.0) > (sum(vols[-500:]) / 500.0):
                            signal = ("LONG", "做多1")
                if signal is None and cond_long2 and pass_macd_once("LONG_DIF_GT_DEA"):
                    vols = klines_1m_quote_vol(sym, 1000)
                    if len(vols) >= 500:
                        if (sum(vols[-20:]) / 20.0) > (sum(vols[-500:]) / 500.0):
                            signal = ("LONG", "做多2")
                if signal is None and cond_short1 and pass_macd_once("SHORT_DIF_LT_DEA"):
                    vols = klines_1m_quote_vol(sym, 1000)
                    if len(vols) >= 2:
                        if vols[-1] <= 0.5 * vols[-2] and (sum(vols[-20:]) / 20.0) < (sum(vols[-500:]) / 500.0):
                            signal = ("SHORT", "做空1")
                if signal is None and cond_short2 and pass_macd_once("SHORT_DIF_LT_DEA"):
                    vols = klines_1m_quote_vol(sym, 1000)
                    if len(vols) >= 500:
                        if (sum(vols[-20:]) / 20.0) < (sum(vols[-500:]) / 500.0):
                            signal = ("SHORT", "做空2")
                if signal:
                    side_dir, label = signal
                    try:
                        try_open(sym, cp, side_dir, label)
                    except Exception as e:
                        print(f"下单失败: {sym} {side_dir} {e}")
                return True
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
                list(ex.map(eval_symbol, syms))
        except Exception:
            pass
        next_tick += 1.0
        delay = next_tick - time.perf_counter()
        if delay > 0:
            time.sleep(delay)

def minute_eval_loop():
    import concurrent.futures
    while True:
        try:
            now = now_ms()
            target = ((now // 60000) * 60000) + 57_000
            if now > target:
                target += 60_000
            time.sleep(max(0.0, (target - now) / 1000.0))
            with lock:
                syms = list(selected_symbols)
            def process_symbol(sym: str):
                kl = api_get("/fapi/v1/klines", {"symbol": sym, "interval": "1m", "limit": 1000})
                if not isinstance(kl, list) or len(kl) < 600:
                    return None
                closes = [float(it[4]) for it in kl]
                highs = [float(it[2]) for it in kl]
                lows = [float(it[3]) for it in kl]
                opens = [float(it[1]) for it in kl]
                mid, up, down = boll(closes, 200, 1)
                ema7 = ema(closes, 7)
                ema25 = ema(closes, 25)
                ema60 = ema(closes, 60)
                ema100 = ema(closes, 100)
                ema200 = ema(closes, 200)
                ema500 = ema(closes, 500)
                dif, dea = macd(closes, 60, 200, 60)
                rsi80 = rsi(closes, 80)
                rsi200 = rsi(closes, 200)
                rsi500 = rsi(closes, 500)
                k, d, jv = kdj_from_close(closes, 20, 80, 200)
                idx = len(closes) - 1
                cp = closes[idx]
                cond_long1 = (
                    opens[idx] > ema500[idx] and closes[idx] > ema500[idx] and highs[idx] > ema500[idx] and lows[idx] > ema500[idx] and
                    cp > up[idx] and
                    (ema7[idx] > ema25[idx] > ema60[idx] > ema100[idx] > ema200[idx] > ema500[idx]) and
                    any(ema7[idx - i] < ema500[idx - i] for i in range(1, min(10, idx)+1)) and
                    dif[idx] > dea[idx] and
                    (rsi80[idx] > rsi200[idx] and rsi80[idx] > rsi500[idx]) and
                    ((k[idx] > d[idx] > jv[idx]) or all(k[idx - i] > k[idx - i - 1] and jv[idx - i] > jv[idx - i - 1] for i in range(1, min(3, idx))) )
                )
                cond_long2 = (
                    opens[idx] > ema100[idx] and closes[idx] > ema100[idx] and highs[idx] > ema100[idx] and lows[idx] > ema100[idx] and
                    cp > up[idx] and
                    (ema7[idx] > ema25[idx] > ema60[idx] > ema100[idx] > ema200[idx] > ema500[idx]) and
                    any(ema7[idx - i] < ema25[idx - i] for i in range(1, min(3, idx)+1)) and
                    dif[idx] > dea[idx] and
                    (rsi80[idx] > rsi200[idx] and rsi80[idx] > rsi500[idx])
                )
                cond_short1 = (
                    opens[idx] < ema500[idx] and closes[idx] < ema500[idx] and highs[idx] < ema500[idx] and lows[idx] < ema500[idx] and
                    cp < down[idx] and
                    (ema7[idx] < ema25[idx] < ema60[idx] < ema100[idx] < ema200[idx] < ema500[idx]) and
                    any(ema7[idx - i] > ema500[idx - i] for i in range(1, min(10, idx)+1)) and
                    dif[idx] < dea[idx] and
                    (rsi80[idx] < rsi200[idx] and rsi80[idx] < rsi500[idx]) and
                    ((k[idx] < d[idx] < jv[idx]) or all(k[idx - i] < k[idx - i - 1] and jv[idx - i] < jv[idx - i - 1] for i in range(1, min(3, idx))) )
                )
                cond_short2 = (
                    opens[idx] < ema100[idx] and closes[idx] < ema100[idx] and highs[idx] < ema100[idx] and lows[idx] < ema100[idx] and
                    cp < down[idx] and
                    (ema7[idx] < ema25[idx] < ema60[idx] < ema100[idx] < ema200[idx] < ema500[idx]) and
                    any(ema7[idx - i] > ema25[idx - i] for i in range(1, min(3, idx)+1)) and
                    dif[idx] < dea[idx] and
                    (rsi80[idx] < rsi200[idx] and rsi80[idx] < rsi500[idx])
                )
                def pass_macd_once(dir_flag: str) -> bool:
                    last = last_macd_dir.get(sym)
                    if last == dir_flag:
                        return False
                    return True
                signal = None
                if cond_long1 and pass_macd_once("LONG_DIF_GT_DEA"):
                    vols = klines_1m_quote_vol(sym, 1000)
                    if len(vols) >= 2:
                        if vols[-1] >= 2 * vols[-2] and (sum(vols[-20:]) / 20.0) > (sum(vols[-500:]) / 500.0):
                            signal = ("LONG", "做多1")
                if signal is None and cond_long2 and pass_macd_once("LONG_DIF_GT_DEA"):
                    vols = klines_1m_quote_vol(sym, 1000)
                    if len(vols) >= 500:
                        if (sum(vols[-20:]) / 20.0) > (sum(vols[-500:]) / 500.0):
                            signal = ("LONG", "做多2")
                if signal is None and cond_short1 and pass_macd_once("SHORT_DIF_LT_DEA"):
                    vols = klines_1m_quote_vol(sym, 1000)
                    if len(vols) >= 2:
                        if vols[-1] <= 0.5 * vols[-2] and (sum(vols[-20:]) / 20.0) < (sum(vols[-500:]) / 500.0):
                            signal = ("SHORT", "做空1")
                if signal is None and cond_short2 and pass_macd_once("SHORT_DIF_LT_DEA"):
                    vols = klines_1m_quote_vol(sym, 1000)
                    if len(vols) >= 500:
                        if (sum(vols[-20:]) / 20.0) < (sum(vols[-500:]) / 500.0):
                            signal = ("SHORT", "做空2")
                if signal:
                    side_dir, label = signal
                    try:
                        try_open(sym, cp, side_dir, label)
                    except Exception as e:
                        print(f"下单失败: {sym} {side_dir} {e}")
                return True
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
                list(ex.map(process_symbol, syms))
        except Exception:
            time.sleep(0.5)

def try_open(sym: str, price: float, side_dir: str, label: str):
    key = (sym, side_dir)
    ts_now = time.time()
    if last_attempt_at.get(key, 0) and (ts_now - last_attempt_at[key]) < 30:
        return
    last_attempt_at[key] = ts_now
    ratio = current_margin_ratio()
    if ratio > MAX_OPEN_MARGIN_RATIO:
        print(f"拒绝开单: 账户保证金率 {ratio:.2f}% 超过 {MAX_OPEN_MARGIN_RATIO}%")
        return
    pos = get_positions()
    has_same = False
    total_nominal = 0.0
    wallet = 0.0
    acc = get_account_info()
    try:
        wallet = float(acc.get("totalWalletBalance", 0) or 0)
    except Exception:
        wallet = 0.0
    for it in pos:
        if it.get("symbol") != sym:
            continue
        amt = float(str(it.get("positionAmt", "0")) or 0)
        side = str(it.get("positionSide", "")).upper()
        if side_dir == "LONG" and side == "LONG" and abs(amt) > 1e-12:
            has_same = True
        if side_dir == "SHORT" and side == "SHORT" and abs(amt) > 1e-12:
            has_same = True
    for it in pos:
        try:
            notional = float(str(it.get("notional", "0")) or 0)
            total_nominal += abs(notional)
        except Exception:
            continue
    if has_same:
        print("拒绝开单: 已存在同方向持仓")
        return
    if wallet > 0 and total_nominal > wallet * 10.0:
        print("拒绝开单: 持仓名义价值超过钱包余额10倍")
        return
    qty_raw = ORDER_NOTIONAL / max(price, 1e-12)
    qty = adjust_qty(sym, qty_raw)
    if qty <= 0:
        flt = symbol_filters.get(sym, {})
        print(f"拒绝开单: 数量不满足最小/步长约束 qty_raw={qty_raw:.8f} minQty={flt.get('minQty',0)} stepSize={flt.get('stepSize',0)}")
        return
    side = "BUY" if side_dir == "LONG" else "SELL"
    position_side = "LONG" if side_dir == "LONG" else "SHORT"
    j = place_market(sym, side, position_side, qty)
    if not isinstance(j, dict) or (j.get("orderId") is None and j.get("clientOrderId") is None):
        print(f"下单失败: {sym} {side_dir} {j}")
        return
    oid = j.get("orderId")
    cid = j.get("clientOrderId")
    avg = wait_fill_avg_price(sym, oid, cid)
    if avg <= 0:
        avg = price
    print(f"{fmt_time()}   {sym}   {position_side}  {avg:.8f}  策略：{label}")
    last_macd_dir[sym] = "LONG_DIF_GT_DEA" if side_dir == "LONG" else "SHORT_DIF_LT_DEA"
    # 止损展示（不下单，轮询止损）
    per_coin = ORDER_STOP_NOTIONAL / max(qty, 1e-12)
    sl_price = (avg - per_coin) if position_side == "LONG" else (avg + per_coin)
    print(f"  └─ 止损单: 止损价 {float(f'{sl_price:.2f}')}" )
    if label in ("做多1", "做空1"):
        place_tp_trailing(sym, position_side, avg, qty, is_long=(side_dir == "LONG"))
        act = adjust_price(sym, avg * (1.02 if side_dir == "LONG" else 0.98))
        print(f"  └─ 跟踪止盈: 激活价 {fmt_price(sym, act)}, 回调 0.5%")
    else:
        gains = 0.0
        losses = 0.0
        for pit in pos:
            try:
                amt2 = float(str(pit.get("positionAmt", "0")) or 0)
                if abs(amt2) <= 1e-12:
                    continue
                u = float(str(pit.get("unRealizedProfit", "0")) or 0)
                if u >= 0:
                    gains += u
                else:
                    losses += -u
            except Exception:
                continue
        if gains > losses:
            place_tp_trailing(sym, position_side, avg, qty, is_long=(side_dir == "LONG"))
            act = adjust_price(sym, avg * (1.02 if side_dir == "LONG" else 0.98))
            print(f"  └─ 跟踪止盈(条件2动态): 激活价 {fmt_price(sym, act)}, 回调 0.5%  盈亏汇总 盈利>{'亏损' if gains>losses else '亏损'}")
        else:
            place_tp_limit_or_market(sym, position_side, avg, qty, is_long=(side_dir == "LONG"), use_market=False)
            tgt = adjust_price(sym, avg * (1.01 if side_dir == "LONG" else 0.99))
            print(f"  └─ 限价止盈(条件2动态): 止盈价 {fmt_price(sym, tgt)}  盈亏汇总 盈利<={'亏损' if gains<=losses else '亏损'}")

def risk_loop():
    while True:
        try:
            pos = get_positions()
            prices_map = {}
            try:
                tp = api_get("/fapi/v2/ticker/price")
                if isinstance(tp, list):
                    for it in tp:
                        prices_map[it.get("symbol")] = float(it.get("price", 0) or 0)
            except Exception:
                pass
            # 同币种多空同时持仓的联动平仓判断
            sym_agg: dict[str, dict[str, dict]] = {}
            for it in pos:
                try:
                    sym = it.get("symbol")
                    side = str(it.get("positionSide", "")).upper()
                    amt = float(str(it.get("positionAmt", "0")) or 0)
                    unreal = float(str(it.get("unRealizedProfit", "0")) or 0)
                    notional_val = abs(float(str(it.get("notional", "0")) or 0))
                    if abs(amt) <= 1e-12:
                        continue
                    d = sym_agg.setdefault(sym, {})
                    d[side] = {"amt": amt, "unreal": unreal, "notional": notional_val}
                except Exception:
                    continue
            pair_closed_syms = set()
            for sym, sides in sym_agg.items():
                if "LONG" in sides and "SHORT" in sides:
                    la = abs(sides["LONG"]["amt"])
                    sa = abs(sides["SHORT"]["amt"])
                    lun = float(sides["LONG"]["unreal"])
                    sun = float(sides["SHORT"]["unreal"])
                    lnot = float(sides["LONG"]["notional"]) or 0.0
                    snot = float(sides["SHORT"]["notional"]) or 0.0
                    base = max(lnot, snot)
                    net = lun + sun
                    trigger_pair = False
                    if base > 0 and net >= 0.02 * base:
                        trigger_pair = True
                    elif abs(la - sa) <= 1e-12 and net > 0:
                        trigger_pair = True
                    if trigger_pair:
                        ql = adjust_qty(sym, la)
                        qs = adjust_qty(sym, sa)
                        if ql > 0:
                            place_market(sym, "SELL", "LONG", ql)
                        if qs > 0:
                            place_market(sym, "BUY", "SHORT", qs)
                        print(f"{fmt_time()}   {sym}   LONG/SHORT  对冲同时平仓  净盈亏 {float(f'{net:.4f}')}  基准名义 {float(f'{base:.4f}')}  阈值 2%")
                        pair_closed_syms.add(sym)
            for it in pos:
                try:
                    sym = it.get("symbol")
                    if sym in pair_closed_syms:
                        continue
                    side = str(it.get("positionSide", "")).upper()
                    amt = float(str(it.get("positionAmt", "0")) or 0)
                    entry = float(str(it.get("entryPrice", "0")) or 0)
                    if abs(amt) <= 1e-12:
                        continue
                    unreal = float(str(it.get("unRealizedProfit", "0")) or 0)
                    notional_val = float(str(it.get("notional", "0")) or 0)
                    loss = max(0.0, -unreal)
                    pnl_pct = (unreal / abs(notional_val) * 100.0) if abs(notional_val) > 1e-12 else 0.0
                    key = (sym, side)
                    nowt = time.time()
                    if key not in pos_pnl_history:
                        pos_pnl_history[key] = deque(maxlen=600)
                    pos_pnl_history[key].append({"t": nowt, "pct": pnl_pct})
                    if key not in pos_first_seen and abs(amt) > 1e-12:
                        pos_first_seen[key] = nowt
                    upd = float(str(it.get("updateTime", "0")) or 0.0)
                    age_sec = 0.0
                    if upd > 0:
                        age_sec = max(0.0, (time.time()*1000 - upd) / 1000.0)
                    else:
                        age_sec = max(0.0, nowt - pos_first_seen.get(key, nowt))
                    if unreal > 0 and age_sec >= 300.0:
                        wnd = [x for x in pos_pnl_history.get(key, deque()) if nowt - x.get("t", nowt) <= 300.0]
                        if wnd:
                            mxp = max(x["pct"] for x in wnd)
                            mnp = min(x["pct"] for x in wnd)
                            if (mxp - mnp) < 0.1:
                                q = adjust_qty(sym, abs(amt))
                                sd = "SELL" if side == "LONG" else "BUY"
                                place_market(sym, sd, side, q)
                                print(f"{fmt_time()}   {sym}   {side}  盈利停滞平仓  波动 {float(f'{(mxp-mnp):.4f}')}%  年龄 {float(f'{age_sec:.0f}')}s")
                                continue
                    if loss >= ORDER_STOP_NOTIONAL:
                        q = adjust_qty(sym, abs(amt))
                        sd = "SELL" if side == "LONG" else "BUY"
                        j = place_market(sym, sd, side, q)
                        print(f"{fmt_time()}   {sym}   {side}  紧急止损平仓  未实现盈亏 {float(f'{unreal:.4f}')} ({float(f'{pnl_pct:.2f}')}%)")
                        continue
                    closes = None
                    dq = prices.get(sym)
                    if dq and len(dq) >= 500:
                        closes = [x["p"] for x in dq]
                    else:
                        kl = api_get("/fapi/v1/klines", {"symbol": sym, "interval": "1m", "limit": 1000})
                        if isinstance(kl, list) and len(kl) >= 600:
                            closes = [float(it[4]) for it in kl]
                    if not closes:
                        continue
                    idx = len(closes) - 1
                    ema500 = ema(closes, 500)
                    cp = closes[idx]
                    trigger = False
                    if side == "LONG":
                        if (cp < ema500[idx]):
                            trigger = True
                            sd = "SELL"
                    else:
                        if (cp > ema500[idx]):
                            trigger = True
                            sd = "BUY"
                        if trigger:
                            q = adjust_qty(sym, abs(amt))
                            j = place_market(sym, sd, side, q)
                            print(f"{fmt_time()}   {sym}   {side}  策略平仓")
                except Exception:
                    continue
        except Exception:
            pass
        time.sleep(2)

def setup_once():
    print("========== 启动 ==========")
    sync_time()
    set_leverage_for_symbols()
    filter_symbols_once()

def main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    cfg_file = os.path.join(os.getcwd(), 'config.json')
    conf = load_config(cfg_file)
    api_key = conf.get('binance', {}).get('api_key') or os.getenv("BINANCE_API_KEY", "")
    secret = conf.get('binance', {}).get('api_secret') or os.getenv("BINANCE_API_SECRET", "")
    if not api_key or not secret:
        raise SystemExit("缺少密钥: 请在 config.json 的 binance.api_key/api_secret 填写或设置环境变量")
    global g_api_key, g_secret
    g_api_key, g_secret = api_key, secret
    t4 = threading.Thread(target=countdown_loop, daemon=True)
    t1 = threading.Thread(target=symbol_filter_loop, daemon=True)
    t2 = threading.Thread(target=ticker_loop, daemon=True)
    t5 = threading.Thread(target=minute_eval_loop, daemon=True)
    t4.start(); t1.start(); t2.start(); t5.start()
    t6 = threading.Thread(target=second_eval_loop, daemon=True)
    t6.start()
    setup_once()
    t3 = threading.Thread(target=risk_loop, daemon=True)
    t3.start()
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
