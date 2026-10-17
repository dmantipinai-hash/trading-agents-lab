#!/usr/bin/env python3
"""basis_scanner.py — BS-1 базис-датчик (файл 24, карточка t_4189b569).

Раз в час (крон) снимает по всем expiry-фьючерсам Bybit демо:
  фьюч bid1Price + спот ask1Price -> exec-базис годовых
    = (bid_fut - ask_spot) / ask_spot * 365 / days_to_expiry
Только исполняемые цены, last не используется (ловушка №18).

Тихий режим (по умолчанию, для no_agent-крона):
  - пустой stdout при отсутствии новых событий
  - строка алерта печатается ТОЛЬКО при переходе состояние->событие
    (дедуп: последняя event_flag=1 строка этого контракта в БД старше
    ALERT_REPEAT_HOURS, либо это первое событие контракта вообще;
    событие длится — молчим)

Порог зафиксирован ДО запуска и не меняется (протокол §8.3): 700 bps год (7%).

P0 db-locked (18.09): все HTTP-запросы идут ДО открытия БД (пасс 1 в память),
затем дедуп-SELECT одним запросом и ОДНА короткая write-транзакция с INSERT
всех строк + commit (пасс 2). Write-лок больше не удерживается между
HTTP-запросами — боты не падают с database is locked.
"""
import argparse
import datetime
import json
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request

BASE = "https://api-demo.bybit.com"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "..", "trading.db")

# --- параметры, зафиксированные до запуска (протокол §8.3) ---
THRESHOLD_BPS = 700.0          # 7% годовых exec-базис
MIN_NOTIONAL_USD = 5.0         # исполнимость обеих ног по мин-номиналу
ALERT_REPEAT_HOURS = 24        # пауза между алертами одного контракта (страховка)


def pub_get(path, params=None, timeout=20):
    qs = urllib.parse.urlencode(params or {})
    url = BASE + path + ("?" + qs if qs else "")
    req = urllib.request.Request(url, headers={"User-Agent": "basis-scanner/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def fetch_expiry_contracts():
    """Все LinearFutures в статусе Trading: symbol -> {delivery_ms, min_notional}."""
    d = pub_get("/v5/market/instruments-info", {"category": "linear", "limit": 1000})
    if d.get("retCode") != 0:
        raise RuntimeError(f"instruments-info retCode={d.get('retCode')} {d.get('retMsg')}")
    out = {}
    for it in d.get("result", {}).get("list", []):
        if it.get("contractType") != "LinearFutures":
            continue
        if it.get("status") != "Trading":
            continue
        sym = it.get("symbol", "")
        dt_ms = it.get("deliveryTime")
        if not sym or not dt_ms:
            continue
        min_not = MIN_NOTIONAL_USD
        try:
            mnv = float((it.get("lotSizeFilter") or {}).get("minNotionalValue") or 0)
            if mnv > 0:
                min_not = mnv  # "5" у всех 40 контрактов (проверено пробой 18.09)
        except (TypeError, ValueError):
            pass
        out[sym] = {"delivery_ms": int(dt_ms), "min_notional": min_not}
    return out


def fetch_all_linear_tickers():
    """Вся linear-категория одним запросом: symbol -> сырой тикер."""
    d = pub_get("/v5/market/tickers", {"category": "linear"})
    if d.get("retCode") != 0:
        raise RuntimeError(f"tickers retCode={d.get('retCode')} {d.get('retMsg')}")
    return {t["symbol"]: t for t in d.get("result", {}).get("list", []) if "symbol" in t}


def fetch_spot_ask(base):
    """Спот-цены базовой монеты. Возвращает (ask, bid) или (None, None)."""
    d = pub_get("/v5/market/tickers", {"category": "spot", "symbol": base + "USDT"})
    if d.get("retCode") != 0:
        return None, None
    lst = d.get("result", {}).get("list", [])
    if not lst:
        return None, None
    t = lst[0]
    ask = float(t.get("ask1Price") or 0)
    bid = float(t.get("bid1Price") or 0)
    return (ask or None), (bid or None)


def spot_ask_depth_usd(sym, top=5):
    """Глубина top5 ask-стороны спота в USDT (исполнимость buy-ноги)."""
    d = pub_get("/v5/market/orderbook", {"category": "spot", "symbol": sym, "limit": 5})
    if d.get("retCode") != 0:
        return None
    asks = d.get("result", {}).get("a") or []
    if not asks:
        return None
    return round(sum(float(p) * float(q) for p, q in asks[:top]), 2)


def ensure_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS basis_snapshots (
            id INTEGER PRIMARY KEY,
            ts TEXT NOT NULL,
            symbol_fut TEXT NOT NULL,
            symbol_spot TEXT NOT NULL,
            bid_fut REAL,
            ask_spot REAL,
            days_to_expiry REAL,
            basis_annual_bps REAL,
            event_flag INTEGER NOT NULL DEFAULT 0,
            min_notional_ok INTEGER NOT NULL DEFAULT 0,
            depth_ask5_spot_usd REAL,
            depth_bid5_fut_usd REAL
        )""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_basis_sym_ts ON basis_snapshots (symbol_fut, ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_basis_event ON basis_snapshots (event_flag, ts)")
    con.commit()


def last_event_rows(con, symbols):
    """Последние event_flag=1 строки для списка контрактов: {sym: (ts, basis_bps)}.

    Один SELECT вместо N (P0 db-locked: дедуп читается в короткой
    write-транзакции вместе с INSERT, без HTTP между ними).
    """
    if not symbols:
        return {}
    ph = ",".join("?" * len(symbols))
    out = {}
    for sym, ts, bps in con.execute(
        f"""SELECT b.symbol_fut, b.ts, b.basis_annual_bps
            FROM basis_snapshots b
            JOIN (SELECT symbol_fut, MAX(id) AS mid
                  FROM basis_snapshots
                  WHERE event_flag=1 AND symbol_fut IN ({ph})
                  GROUP BY symbol_fut) m ON b.id = m.mid""",
        symbols):
        out[sym] = (ts, bps)
    return out


def parse_ts(ts):
    return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def main():
    ap = argparse.ArgumentParser(description="BS-1 базис-датчик")
    ap.add_argument("--db", default=DB_PATH, help="путь к trading.db")
    ap.add_argument("--threshold-bps", type=float, default=THRESHOLD_BPS,
                    help="порог события в bps годовых (default 700)")
    ap.add_argument("--verbose", action="store_true",
                    help="печать сводки снимка (ручные прогоны и приёмка)")
    ap.add_argument("--limit", type=int, default=0,
                    help="только первые N контрактов по близости экспирации (тест)")
    args = ap.parse_args()

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    ts = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    threshold = args.threshold_bps

    contracts = fetch_expiry_contracts()
    if args.limit:
        items = sorted(contracts.items(), key=lambda kv: kv[1]["delivery_ms"])[:args.limit]
        contracts = dict(items)

    tickers = fetch_all_linear_tickers()

    # --- ПАСС 1: сбор снимков в память (HTTP здесь; коннект к БД ещё не открыт,
    #     лок не держится — фикс P0 db-locked 18.09) ---
    snapshots = []
    spot_cache = {}
    for sym, meta in sorted(contracts.items()):
        t = tickers.get(sym)
        if not t:
            continue
        base = sym.split("USDT-")[0] if "USDT-" in sym else sym.replace("USDT", "")
        spot_sym = base + "USDT"
        fut_bid = float(t.get("bid1Price") or 0)
        if fut_bid <= 0:
            continue

        if spot_sym not in spot_cache:
            spot_cache[spot_sym] = fetch_spot_ask(base)
        spot_ask, _spot_bid = spot_cache[spot_sym]

        days = (datetime.datetime.fromtimestamp(meta["delivery_ms"] / 1000,
                tz=datetime.timezone.utc) - now_utc).total_seconds() / 86400.0
        if days <= 0:
            continue  # истёк/доставка сегодня — годовой множитель не определён

        basis_bps = None
        min_notional_ok = 0
        depth_spot = None
        depth_fut = round(fut_bid * float(t.get("bid1Size") or 0), 2)
        if spot_ask and spot_ask > 0:
            basis_bps = (fut_bid - spot_ask) / spot_ask * 365.0 / days * 1e4
            depth_spot = spot_ask_depth_usd(spot_sym)
            ok_fut = depth_fut >= meta["min_notional"]
            ok_spot = bool(depth_spot and depth_spot >= meta["min_notional"])
            min_notional_ok = int(ok_fut and ok_spot)

        event = int(basis_bps is not None and basis_bps >= threshold and min_notional_ok)
        snapshots.append({
            "sym": sym, "spot_sym": spot_sym, "fut_bid": fut_bid, "spot_ask": spot_ask,
            "days": days, "basis_bps": basis_bps, "event": event,
            "min_notional_ok": min_notional_ok, "depth_spot": depth_spot,
            "depth_fut": depth_fut,
        })
        time.sleep(0.05)

    # --- ПАСС 2: дедуп-чтение одним SELECT + ОДНА короткая write-транзакция ---
    con = sqlite3.connect(args.db, timeout=30)
    ensure_table(con)
    event_syms = [s["sym"] for s in snapshots if s["event"]]
    prev_events = last_event_rows(con, event_syms)

    inserted, events_new, alerts = 0, 0, []
    try:
        for s in snapshots:
            # дедуп: смотрим ПРЕДЫДУЩУЮ event-строку ДО вставки текущей
            prev = prev_events.get(s["sym"]) if s["event"] else None
            is_new_event = bool(
                s["event"] and (prev is None
                                or (now_utc - parse_ts(prev[0])).total_seconds()
                                > ALERT_REPEAT_HOURS * 3600))
            con.execute(
                """INSERT INTO basis_snapshots
                   (ts, symbol_fut, symbol_spot, bid_fut, ask_spot, days_to_expiry,
                    basis_annual_bps, event_flag, min_notional_ok,
                    depth_ask5_spot_usd, depth_bid5_fut_usd)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (ts, s["sym"], s["spot_sym"], s["fut_bid"], s["spot_ask"],
                 round(s["days"], 4),
                 round(s["basis_bps"], 2) if s["basis_bps"] is not None else None,
                 s["event"], s["min_notional_ok"], s["depth_spot"], s["depth_fut"]))
            inserted += 1
            if is_new_event:
                events_new += 1
                alerts.append((s["sym"], s["basis_bps"], s["days"], s["fut_bid"],
                               s["spot_ask"], s["depth_fut"], s["depth_spot"]))
        con.commit()
    finally:
        con.close()

    if alerts:
        print("=== BS-1: дислокация базиса (exec: bid фьюча vs ask спота) ===")
        for sym, bps, days, fb, sa, df, ds in alerts:
            print(f"{sym}: {bps/100:.2f}% год | bid_fut {fb} / ask_spot {sa} | "
                  f"{days:.1f} дн до эксп. | глубина bid_fut ${df}, ask_spot ${ds} | "
                  f"мин-номиналы обеих ног OK ($5)")
        print("Датчик — только уведомление; решение о входе за Димой (файл 24).")
    elif args.verbose:
        print(f"[basis-scanner] {ts}: контрактов снято {inserted}, новых событий {events_new}, "
              f"порог {threshold/100:.1f}% год")
        con2 = sqlite3.connect(args.db, timeout=30)
        top = [f"{r[0]} {r[1]/100:.2f}% ({r[2]:.0f}д)" for r in con2.execute(
            "SELECT symbol_fut, basis_annual_bps, days_to_expiry FROM basis_snapshots "
            "WHERE ts=? AND basis_annual_bps IS NOT NULL "
            "ORDER BY basis_annual_bps DESC LIMIT 5", (ts,))]
        con2.close()
        if top:
            print("top-5 базиса:", "; ".join(top))
    # тихий режим без событий: stdout пуст (правило проекта: тихие сборщики молчат)
    return 0


if __name__ == "__main__":
    sys.exit(main())
