import os
import json
import time
import hmac
import hashlib
import threading
from collections import deque
from datetime import datetime
import argparse
import requests
from flask import Flask, jsonify, Response
from urllib import parse

API_URL = "https://fapi.binance.com"

def _sign(params: dict, secret: str) -> str:
    query = parse.urlencode(params, doseq=True)
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
HISTORY_JSON = 'bn.json'
HISTORY_TXT = 'bn.txt'
last_balance_json = None
last_account_json = None

def load_config(path: str) -> dict:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def fetch_balances(api_key: str, secret: str, asset: str) -> tuple[float, float]:
    ts = int(time.time() * 1000)
    params = {"timestamp": ts, "recvWindow": 60000}
    sig = _sign(params, secret)
    headers = {"X-MBX-APIKEY": api_key}
    wallet = 0.0
    margin = 0.0
    try:
        r2 = requests.get(f"{API_URL}/fapi/v3/account", params={**params, "signature": sig}, headers=headers, timeout=10)
        j2 = r2.json()
        globals()['last_account_json'] = j2
        if isinstance(j2, dict):
            try:
                wallet = float(j2.get("totalWalletBalance", 0) or 0)
            except Exception:
                wallet = 0.0
            try:
                margin = float(j2.get("totalMarginBalance", 0) or 0)
            except Exception:
                margin = 0.0
    except Exception:
        pass
    if wallet == 0.0:
        try:
            r = requests.get(f"{API_URL}/fapi/v3/balance", params={**params, "signature": sig}, headers=headers, timeout=10)
            j = r.json()
            globals()['last_balance_json'] = j
            if isinstance(j, list):
                for it in j:
                    if str(it.get("asset", "")).upper() == asset.upper():
                        v = it.get("balance", it.get("availableBalance", 0))
                        try:
                            wallet = float(v)
                        except Exception:
                            wallet = 0.0
                        break
        except Exception:
            pass
    return wallet, margin

app = Flask(__name__)

series = deque(maxlen=21600)
latest = {"t": None, "wallet": None, "margin": None}
g_api_key = None
g_secret = None
cfg = {"interval": 1.0, "field": "balance", "asset": "USDT"}
lock = threading.Lock()

@app.route("/")
def index():
    html = """
    <!doctype html>
    <html>
    <head>
      <meta charset='utf-8'/>
      <meta name='viewport' content='width=device-width, initial-scale=1'/>
      <title>实时合约余额</title>
      <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
      <style>
        body{background:#000;color:#9cc;margin:0;font-family:Arial}
        #layout{display:flex;gap:10px;padding:10px}
        #left{flex:0 0 65%;}
        #right{flex:1;display:flex;flex-direction:column;gap:10px}
        #title{color:#00ff88;margin-bottom:8px}
        .panel{background:#0a0f12;border:1px solid #203030;border-radius:6px;overflow:auto}
        #cur{flex:0 0 33%}
        #hist{flex:1}
        table{width:100%;border-collapse:collapse}
        th,td{padding:6px 8px;border-bottom:1px solid #203030;color:#9cc}
        th{color:#cfe}
        .mono{font-family:Consolas, monospace}
        .toolbar{display:flex;justify-content:flex-end;gap:8px;padding:8px}
        .btn{background:#16302c;border:1px solid #2a5b52;color:#9ff;padding:6px 10px;border-radius:4px;cursor:pointer}
        .btn:hover{filter:brightness(1.1)}
        canvas{height:200px}
        @media (max-width: 900px){
          #layout{flex-direction:column}
          #left{flex:1 1 auto;height:50vh}
          #right{flex:1 1 auto}
          #cur,#hist{height:auto}
          canvas{height:100%;width:100%}
          th,td{padding:6px 6px;font-size:12px}
          .mono{font-size:12px}
        }
      </style>
    </head>
    <body>
      <div id="layout">
        <div id="left" class="panel">
          <div id="title"></div>
          <canvas id="c"></canvas>
        </div>
  <div id="right">
    <div id="cur" class="panel">
      <table>
        <thead>
          <tr><th>Symbol</th><th>Side</th><th class="mono">Qty</th><th class="mono">Entry</th><th class="mono">uPNL</th></tr>
        </thead>
        <tbody id="curBody"></tbody>
      </table>
    </div>
          <div id="hist" class="panel">
            <div class="toolbar"><button id="refreshOrders" class="btn">刷新历史订单</button></div>
            <table>
              <thead>
                <tr>
                  <th>Time</th><th>Symbol</th><th>Side</th>
                  <th class="mono">Qty</th><th class="mono">Price</th><th class="mono">Realized PnL</th>
                </tr>
              </thead>
              <tbody id="ordersBody"></tbody>
            </table>
            <div id="ordersCards" style="padding:10px"></div>
          </div>
  </div>
      </div>
      <script>
        const ctx = document.getElementById('c').getContext('2d');
        const data = { labels: [], datasets: [
          { label: '保证金余额', data: [], borderColor:'#00ff88', borderWidth:2, tension:0.25, pointRadius:0, pointHoverRadius:0, pointHitRadius:0, fill:false },
          { label: '钱包余额', data: [], borderColor:'#55aaff', borderWidth:2, tension:0.25, pointRadius:0, pointHoverRadius:0, pointHitRadius:0, fill:false }
        ] };
        const grid = { color:'#203030' };
        const chart = new Chart(ctx, { type:'line', data, options:{ responsive:true, maintainAspectRatio:(window.innerWidth <= 900 ? false : true), plugins:{legend:{display:true}}, elements:{ point:{ radius:0, hoverRadius:0, hitRadius:0 }, line:{ borderWidth:2 } }, scales:{ x:{ ticks:{color:'#9cc'}, grid }, y:{ ticks:{color:'#9cc'}, grid } } } });
        async function fetchData(){
          const r = await fetch('/api/data');
          const j = await r.json();
          const rows = j.filter(p => typeof p.t === 'number' && Number.isFinite(p.t) && typeof p.wallet === 'number' && typeof p.margin === 'number' && (p.wallet>0 || p.margin>0));
          const fmt = t => { const d = new Date(t); return d.toLocaleTimeString(); };
          data.labels = rows.map(p => fmt(p.t));
          data.datasets[0].data = rows.map(p => p.margin);
          data.datasets[1].data = rows.map(p => p.wallet);
          chart.update('none');
          if(rows.length){
            const last = rows[rows.length-1];
            const mw = (last.margin ?? 0).toFixed(4);
            const ww = (last.wallet ?? 0).toFixed(4);
            document.getElementById('title').textContent = `保证金:${mw}  钱包:${ww}`;
          }
        }
        async function fetchStatus(){
          try{
            const r = await fetch('/api/status');
            const s = await r.json();
            const asset = (s.asset || 'USDT').toUpperCase();
            let wallet = 0, margin = 0;
            if(Array.isArray(s.balance)){
              let item = s.balance.find(it => String(it.asset||'').toUpperCase() === asset);
              if(!item && s.balance.length>0) item = s.balance[0];
              if(item){ wallet = parseFloat(item.balance ?? item.availableBalance ?? 0); }
            }
            if(s.account && typeof s.account === 'object'){
              if(s.account.totalWalletBalance){ wallet = parseFloat(s.account.totalWalletBalance); }
              if(s.account.totalMarginBalance){ margin = parseFloat(s.account.totalMarginBalance); }
            }
            if(wallet>0 || margin>0){
              document.getElementById('title').textContent = `保证金:${(margin||0).toFixed(4)}  钱包:${(wallet||0).toFixed(4)}`;
              if(data.labels.length===0){
                const fmt = t => { const d = new Date(t); return d.toLocaleTimeString(); };
                const now = Date.now();
                data.labels.push(fmt(now));
                data.datasets[0].data.push(margin||0);
                data.datasets[1].data.push(wallet||0);
                chart.update('none');
              }
            }
          }catch(e){}
        }
        async function fetchCurrent(){
          try{
            const rc = await fetch('/api/positions_current');
            const cur = await rc.json();
            const hb = document.getElementById('curBody');
            hb.innerHTML = '';
            cur.forEach(p => {
              const tr = document.createElement('tr');
              const upnl = Number(p.unRealizedProfit);
              const color = upnl > 0 ? '#00ff88' : (upnl < 0 ? '#ff5566' : '#9cc');
              tr.innerHTML = `<td>${p.symbol}</td><td>${p.positionSide}</td><td class='mono'>${Number(p.positionAmt).toFixed(4)}</td><td class='mono'>${Number(p.entryPrice).toFixed(4)}</td><td class='mono' style='color:${color}'>${upnl.toFixed(4)}</td>`;
              hb.appendChild(tr);
            });
          }catch(e){}
        }
        async function fetchHistory(){
          try{
            const rh = await fetch('/api/orders_history');
            const orders = await rh.json();
            const isMobile = window.innerWidth <= 900;
            // group by time+symbol+side
            const groups = new Map();
            for (const o of orders){
              const key = `${o.time}-${o.symbol}-${o.side}`;
              const qty = Number(o.qty||0);
              const price = Number(o.price||0);
              const pnl = Number(o.realizedPnl||0);
              if(!groups.has(key)) groups.set(key, { time: o.time, symbol: o.symbol, side: o.side, sumQty: 0, sumPriceQty: 0, sumPnl: 0 });
              const g = groups.get(key);
              g.sumQty += qty;
              g.sumPriceQty += price * qty;
              g.sumPnl += pnl;
            }
            const merged = Array.from(groups.values()).map(g => ({
              time: g.time,
              symbol: g.symbol,
              side: g.side,
              qty: g.sumQty,
              price: g.sumQty > 0 ? g.sumPriceQty / g.sumQty : 0,
              realizedPnl: g.sumPnl
            })).filter(x => Math.abs(x.realizedPnl||0) > 1e-8).sort((a,b) => b.time - a.time);
            const tb = document.getElementById('ordersBody');
            const cards = document.getElementById('ordersCards');
            tb.innerHTML = '';
            cards.innerHTML = '';
            if(!isMobile){
              merged.forEach(o => {
                const tr = document.createElement('tr');
                const tstr = new Date(o.time).toLocaleString();
                const upnl = Number(o.realizedPnl||0);
                const color = upnl>0?'#00ff88':(upnl<0?'#ff5566':'#9cc');
                tr.innerHTML = `<td class='mono'>${tstr}</td><td>${o.symbol}</td><td>${o.side}</td><td class='mono'>${Number(o.qty).toFixed(4)}</td><td class='mono'>${Number(o.price).toFixed(6)}</td><td class='mono' style='color:${color}'>${upnl.toFixed(4)}</td>`;
                tb.appendChild(tr);
              });
            }else{
              merged.forEach(o => {
                const card = document.createElement('div');
                card.style.border = '1px solid #203030';
                card.style.borderRadius = '6px';
                card.style.padding = '10px';
                card.style.marginBottom = '10px';
                const tstr = new Date(o.time).toLocaleString();
                const upnl = Number(o.realizedPnl||0);
                const color = upnl>0?'#00ff88':(upnl<0?'#ff5566':'#9cc');
                card.innerHTML = `
                  <div style='display:flex;justify-content:space-between;align-items:center;'>
                    <div><span style='color:${o.side==='BUY'?'#00ff88':'#ff5566'};font-weight:bold;'>${o.side==='BUY'?'买':'卖'}</span> <span style='font-weight:bold'>${o.symbol}</span></div>
                    <div class='mono'>${tstr}</div>
                  </div>
                  <div style='display:flex;gap:20px;margin-top:8px;'>
                    <div><div style='color:#cfe'>数量</div><div class='mono'>${Number(o.qty).toFixed(4)}</div></div>
                    <div><div style='color:#cfe'>价格</div><div class='mono'>${Number(o.price).toFixed(6)}</div></div>
                    <div><div style='color:#cfe'>盈亏</div><div class='mono' style='color:${color}'>${upnl.toFixed(4)}</div></div>
                  </div>
                `;
                cards.appendChild(card);
              });
            }
          }catch(e){}
        }
        function adjustHeights(){
          const isMobile = window.innerWidth <= 900;
          if(isMobile){
            document.getElementById('cur').style.height = 'auto';
            document.getElementById('hist').style.height = 'auto';
            return;
          }
          const cv = document.getElementById('c');
          const h = cv.getBoundingClientRect().height || 200;
          document.getElementById('cur').style.height = (h/3) + 'px';
          document.getElementById('hist').style.height = (h*2/3) + 'px';
        }
        async function tick(){ await fetchData(); setTimeout(tick, 1000); }
        tick();
        setInterval(fetchStatus, 1000);
        setInterval(fetchCurrent, 2000);
        fetchHistory();
        document.getElementById('refreshOrders').addEventListener('click', fetchHistory);
        adjustHeights();
        window.addEventListener('resize', adjustHeights);
      </script>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")

@app.route("/api/data")
def api_data():
    with lock:
        return jsonify(list(series))

@app.route("/api/status")
def api_status():
    return jsonify({
        "balance": globals().get('last_balance_json'),
        "account": globals().get('last_account_json'),
        "field": cfg.get("field"),
        "asset": cfg.get("asset")
    })

@app.route("/api/positions_current")
def api_positions_current():
    ts = int(time.time() * 1000)
    params = {"timestamp": ts, "recvWindow": 60000}
    query = {**params}
    sig = _sign(query, g_secret)
    headers = {"X-MBX-APIKEY": g_api_key}
    try:
        r = requests.get(f"{API_URL}/fapi/v2/positionRisk", params={**query, "signature": sig}, headers=headers, timeout=10)
        arr = r.json()
        if isinstance(arr, list):
            cur = []
            for it in arr:
                try:
                    amt = float(str(it.get("positionAmt", "0")))
                except Exception:
                    amt = 0.0
                if abs(amt) <= 1e-12:
                    continue
                cur.append({
                    "symbol": it.get("symbol"),
                    "positionSide": it.get("positionSide"),
                    "positionAmt": amt,
                    "entryPrice": it.get("entryPrice"),
                    "unRealizedProfit": it.get("unRealizedProfit")
                })
            return jsonify(cur)
    except Exception:
        pass
    return jsonify([])

@app.route("/api/orders_history")
def api_orders_history():
    try:
        items = []
        p_txt = os.path.join(os.getcwd(), HISTORY_TXT)
        p_json = os.path.join(os.getcwd(), HISTORY_JSON)
        if os.path.isfile(p_txt):
            with open(p_txt, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(obj, dict) and (obj.get('kind') == 'session' or ('openTime' in obj and 'closeTime' in obj)):
                        items.append(obj)
        elif os.path.isfile(p_json):
            try:
                with open(p_json, 'r', encoding='utf-8') as f:
                    arr = json.load(f)
                    for obj in arr:
                        if isinstance(obj, dict) and (obj.get('kind') == 'session' or ('openTime' in obj and 'closeTime' in obj)):
                            items.append(obj)
            except Exception:
                items = []
        # if local file has orders records
        orders = []
        for s in items:
            if 'time' in s and 'price' in s and 'qty' in s and 'side' in s and 'symbol' in s:
                rp = float(s.get('realizedPnl', 0) or 0)
                orders.append({
                    'time': s['time'], 'symbol': s['symbol'], 'side': s['side'],
                    'price': s['price'], 'qty': s['qty'], 'realizedPnl': rp
                })
        if orders:
            orders.sort(key=lambda x: x.get('time', 0), reverse=True)
            return jsonify(orders[:100])
        # fallback: query recent symbols then trades
        ts = int(time.time() * 1000)
        start = ts - 24*60*60*1000
        headers = {"X-MBX-APIKEY": g_api_key}
        try:
            syms = []
            seen = set()
            def pull_symbols(income_type: str):
                try:
                    p = {"timestamp": ts, "recvWindow": 60000, "incomeType": income_type, "startTime": start}
                    s = _sign(p, g_secret)
                    r = requests.get(f"{API_URL}/fapi/v1/income", params={**p, "signature": s}, headers=headers, timeout=10)
                    j = r.json()
                    if isinstance(j, list):
                        j.sort(key=lambda x: x.get("time", 0), reverse=True)
                        for it in j:
                            sym = it.get("symbol")
                            if sym and sym not in seen:
                                seen.add(sym)
                                syms.append(sym)
                except Exception:
                    pass
            pull_symbols("COMMISSION")
            pull_symbols("REALIZED_PNL")
            syms = syms[:10]
            all_trades = []
            for sym in syms:
                p2 = {"timestamp": ts, "recvWindow": 60000, "symbol": sym, "startTime": start, "limit": 200}
                s2 = _sign(p2, g_secret)
                r2 = requests.get(f"{API_URL}/fapi/v1/userTrades", params={**p2, "signature": s2}, headers=headers, timeout=10)
                trades = r2.json()
                if isinstance(trades, list):
                    for t in trades:
                        try:
                            constPnl = float(t.get('realizedPnl', 0) or 0)
                            orders.append({
                                'time': int(t.get('time', 0)),
                                'symbol': sym,
                                'side': str(t.get('side','')).upper(),
                                'price': float(t.get('price', 0) or 0),
                                'qty': float(t.get('qty', 0) or 0),
                                'realizedPnl': constPnl
                            })
                        except Exception:
                            continue
            orders.sort(key=lambda x: x.get('time', 0), reverse=True)
            # persist to bn.txt
            try:
                with open(HISTORY_TXT, 'a', encoding='utf-8') as f:
                    for o in orders[:100]:
                        f.write(json.dumps({"kind":"order", **o}) + "\n")
            except Exception:
                pass
            return jsonify(orders[:100])
        except Exception:
            return jsonify([])
    except Exception:
        return jsonify([])

def poll_loop(api_key: str, secret: str):
    last = None
    last_wallet = None
    last_margin = None
    while True:
        try:
            wallet, margin = fetch_balances(api_key, secret, cfg["asset"]) 
            if (
                (wallet == 0.0 and (last_wallet or latest.get("wallet") or 0) > 0) or
                (margin == 0.0 and (last_margin or latest.get("margin") or 0) > 0)
            ):
                time.sleep(cfg["interval"])
                continue
            if last is None or abs((wallet + margin) - (last or 0)) > 1e-9:
                now = int(time.time() * 1000)
                with lock:
                    latest["t"], latest["wallet"], latest["margin"] = now, wallet, margin
                    series.append({"t": now, "wallet": wallet, "margin": margin})
                try:
                    with open(HISTORY_TXT, 'a', encoding='utf-8') as f:
                        f.write(json.dumps({"t": now, "wallet": wallet, "margin": margin}) + "\n")
                except Exception:
                    pass
                last = wallet + margin
                last_wallet = wallet
                last_margin = margin
            time.sleep(cfg["interval"])
        except Exception as e:
            time.sleep(cfg["interval"]) 

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset", default="USDT")
    parser.add_argument("--field", default="balance")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--port", type=int, default=18882)
    args = parser.parse_args()

    cfg_file = os.path.join(os.getcwd(), 'config.json')
    conf = load_config(cfg_file)
    api_key = conf.get('binance', {}).get('api_key') or os.getenv("BINANCE_API_KEY", "")
    secret = conf.get('binance', {}).get('api_secret') or os.getenv("BINANCE_API_SECRET", "")
    if not api_key or not secret:
        raise SystemExit("缺少密钥: 请在 config.json 的 binance.api_key/api_secret 填写或设置环境变量")

    cfg["interval"] = args.interval
    cfg["field"] = args.field
    cfg["asset"] = args.asset

    try:
        p_json = os.path.join(os.getcwd(), HISTORY_JSON)
        p_txt = os.path.join(os.getcwd(), HISTORY_TXT)
        loaded = []
        if os.path.isfile(p_json):
            with open(p_json, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
        elif os.path.isfile(p_txt):
            with open(p_txt, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        loaded.append(obj)
                    except Exception:
                        continue
        if loaded:
            with lock:
                for obj in loaded[-series.maxlen:]:
                    if isinstance(obj, dict) and ("t" in obj) and ("wallet" in obj) and ("margin" in obj):
                        try:
                            tt = int(obj.get("t"))
                            ww = float(obj.get("wallet"))
                            mm = float(obj.get("margin"))
                        except Exception:
                            continue
                        if ww == 0.0 and mm == 0.0:
                            continue
                        series.append({"t": tt, "wallet": ww, "margin": mm})
                if series:
                    last_obj = series[-1]
                    latest["t"] = last_obj.get("t")
                    latest["wallet"] = last_obj.get("wallet")
                    latest["margin"] = last_obj.get("margin")
    except Exception:
        pass

    global g_api_key, g_secret
    g_api_key, g_secret = api_key, secret
    t = threading.Thread(target=poll_loop, args=(api_key, secret), daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=args.port)

if __name__ == "__main__":
    main()

