#!/usr/bin/env python3
"""Bybit demo API — общий модуль. Подпись, запросы, чтение ключей.

Паттерн подписи (грабля 16.09): HMAC-SHA256 от {ts}{key}{recvWindow}{payload},
где payload = query-string для GET ( urlencode, ВКЛЮЧАЯ параметры) и compact-JSON для POST.
"""
import hashlib, hmac, json, os, time, urllib.request, urllib.parse, urllib.error

DIR = os.path.dirname(os.path.abspath(__file__))
KEY = open(os.path.join(DIR, "..", "BYBIT_DEMO_API_KEY.env")).read().strip()
SECRET = open(os.path.join(DIR, "..", "BYBIT_DEMO_API_SECRET.env")).read().strip()
BASE = "https://api-demo.bybit.com"
RECV_WINDOW = "5000"

_last_call = 0.0

def _sign(payload: str) -> str:
    ts = str(int(time.time() * 1000))
    s = hmac.new(SECRET.encode(), f"{ts}{KEY}{RECV_WINDOW}{payload}".encode(),
                 hashlib.sha256).hexdigest()
    return ts, s

def _throttle(min_interval=0.12):
    global _last_call
    dt = time.time() - _last_call
    if dt < min_interval:
        time.sleep(min_interval - dt)
    _last_call = time.time()

def get(path: str, params=None) -> dict:
    _throttle()
    qs = urllib.parse.urlencode(params or {})
    ts, sign = _sign(qs)
    r = urllib.request.Request(BASE + path + ("?" + qs if qs else ""), headers={
        "X-BAPI-API-KEY": KEY, "X-BAPI-TIMESTAMP": ts, "X-BAPI-SIGN": sign,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW})
    return _open(r)

def post(path: str, params=None) -> dict:
    _throttle(0.3)
    body = json.dumps(params or {}, separators=(",", ":"))
    ts, sign = _sign(body)
    r = urllib.request.Request(BASE + path, data=body.encode(), method="POST", headers={
        "X-BAPI-API-KEY": KEY, "X-BAPI-TIMESTAMP": ts, "X-BAPI-SIGN": sign,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW, "Content-Type": "application/json"})
    return _open(r)

def _open(r):
    for attempt in range(2):
        try:
            with urllib.request.urlopen(r, timeout=15) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return {"http": e.code, "body": e.read().decode()[:300]}
        except Exception as e:
            if attempt == 1:
                return {"error": str(e)[:200]}
            time.sleep(1)

# --- семантические обёртки ---

def kline_closes(symbol: str, interval: str = "5", limit: int = 300) -> list:
    """Закрытия 5m-баров (старые -> новые) для vol_filter. Публичный эндпоинт
    (подпись не нужна, ключи не участвуют); None-цены и некорректные строки
    отбрасываются. При ошибке сети — пустой список (скейлер уйдёт в neutral)."""
    q = urllib.parse.urlencode({"category": "linear", "symbol": symbol,
                                "interval": interval, "limit": str(limit)})
    try:
        with urllib.request.urlopen(BASE + "/v5/market/kline?" + q, timeout=10) as r:
            d = json.loads(r.read().decode())
    except Exception:
        return []
    rows = d.get("result", {}).get("list", []) if d.get("retCode") == 0 else []
    out = []
    for row in rows:                      # API отдаёт новые -> старые
        try:
            out.append((int(row[0]), float(row[4])))   # (tsMs, close)
        except (ValueError, TypeError, IndexError):
            continue
    out.sort()                            # старые -> новые
    return [c for _, c in out]

def ticker(symbol: str) -> dict:
    d = get("/v5/market/tickers", {"category": "linear", "symbol": symbol})
    return d["result"]["list"][0] if d.get("retCode") == 0 and d["result"]["list"] else {}

def instrument(symbol: str) -> dict:
    d = get("/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
    return d["result"]["list"][0] if d.get("retCode") == 0 and d["result"]["list"] else {}

def wallet() -> dict:
    d = get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
    return d["result"]["list"][0] if d.get("retCode") == 0 else {}

def place_order(symbol, side, qty, price, tif="PostOnly", reason="", strategy="manual"):
    """Выставить лимитную заявку. qty/price — уже нормализованные строки."""
    return post("/v5/order/create", {
        "category": "linear", "symbol": symbol, "side": side, "orderType": "Limit",
        "qty": str(qty), "price": str(price), "timeInForce": tif,
        "orderLinkId": f"loki-{int(time.time()*1000)}"})

def cancel_order(symbol, order_id):
    return post("/v5/order/cancel", {"category": "linear", "symbol": symbol, "orderId": order_id})

def open_orders(symbol=None):
    p = {"category": "linear"}
    if symbol: p["symbol"] = symbol
    d = get("/v5/order/realtime", p)
    return d.get("result", {}).get("list", []) if d.get("retCode") == 0 else []

def order_history(symbol=None, limit=20):
    p = {"category": "linear", "limit": str(limit)}
    if symbol: p["symbol"] = symbol
    d = get("/v5/order/history", p)
    return d.get("result", {}).get("list", []) if d.get("retCode") == 0 else []

def positions(symbol=None):
    p = {"category": "linear", "settleCoin": "USDT"}
    if symbol: p["symbol"] = symbol
    d = get("/v5/position/list", p)
    return d.get("result", {}).get("list", []) if d.get("retCode") == 0 else []

def closed_pnl(symbol=None, limit=20):
    p = {"category": "linear", "limit": str(limit)}
    if symbol: p["symbol"] = symbol
    d = get("/v5/position/closed-pnl", p)
    return d.get("result", {}).get("list", []) if d.get("retCode") == 0 else []
