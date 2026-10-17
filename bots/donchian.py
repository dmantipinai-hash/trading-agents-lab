#!/usr/bin/env python3
"""donchian.py — единый контракт свечных данных + расчёт Donchian.

Пакет А, фикс 1 (аудит №14 §4.1): один модуль используют live (bot_v3),
replay и приёмочные тесты. Контракт данных:
  - сортировка по ts ПО ВОЗРАСТАНИЮ (Bybit отдаёт «новые сверху» — неважно);
  - уникальные ts (при дубликате остаётся последний — provisional заменяется);
  - только ЗАКРЫТЫЕ бары: ts + interval_ms <= now_ms;
  - последние N необходимых баров;
  - проверка непрерывности сетки: соседние ts отличаются ровно на interval_ms,
    иначе ValueError (разрыв → сигнал не считается, вход запрещён).

Время — строго UTC: datetime.fromtimestamp(..., tz=timezone.utc).
Сигнал считается по ЗАКРЫТОМУ бару bars[-1]; окна канала — только из
ПРЕДШЕСТВУЮЩИХ бар (срезы перед текущим), как в исходном бэктесте №12.
"""
import datetime
import json
import time
import urllib.parse
import urllib.request

INTERVAL_MS = 300000  # 5-минутные бары


def utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_from_ms(ms):
    return datetime.datetime.fromtimestamp(int(ms) / 1000, datetime.timezone.utc)


def utc_iso_from_ms(ms):
    return utc_from_ms(ms).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_hhmm_from_ms(ms):
    return utc_from_ms(ms).strftime("%H:%M")


def normalize_bars(raw, interval_ms=INTERVAL_MS, now_ms=None, last_n=None):
    """Приводит любой порядок входных свечей к контракту.

    raw — список dict(ts,o,h,l,c) или списков Bybit [ts,o,h,l,c,v,t].
    Возвращает список dict по возрастанию ts (см. модульный докстринг).
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    by_ts = {}
    for r in raw:
        if isinstance(r, dict):
            ts = int(r["ts"])
            bar = dict(ts=ts, o=float(r["o"]), h=float(r["h"]),
                       l=float(r["l"]), c=float(r["c"]))
        else:
            ts = int(r[0])
            bar = dict(ts=ts, o=float(r[1]), h=float(r[2]),
                       l=float(r[3]), c=float(r[4]))
        if bar["h"] < bar["l"]:
            raise ValueError("бар %s: high %.6f < low %.6f" % (utc_iso_from_ms(ts), bar["h"], bar["l"]))
        by_ts[ts] = bar  # дубликат ts → остаётся последний
    bars = [by_ts[k] for k in sorted(by_ts) if k + interval_ms <= now_ms]
    if last_n is not None:
        bars = bars[-last_n:]
    if len(bars) >= 2:
        for a, b in zip(bars, bars[1:]):
            if b["ts"] - a["ts"] != interval_ms:
                raise ValueError("разрыв сетки между %s и %s (шаг %d мс)"
                                 % (utc_iso_from_ms(a["ts"]), utc_iso_from_ms(b["ts"]), interval_ms))
    return bars


def donchian_signal(bars, entry_bars, exit_bars, interval_ms=INTERVAL_MS, decision_ts=None):
    """(entry, exit, ctx) по ЗАКРЫТОМУ бару bars[-1]; окна из предшествующих.

    bars должны удовлетворять контракту normalize_bars;
    длина >= max(entry_bars, exit_bars) + 1, иначе ValueError.
    ctx: close, hi, lo (уровни канала), bar_open_ts, bar_close_ts,
    decision_ts (UTC ISO) и ts (HH:MM UTC для коротких логов).
    """
    need = max(entry_bars, exit_bars) + 1
    if len(bars) < need:
        raise ValueError("свечей мало: %d < %d" % (len(bars), need))
    cur = bars[-1]
    hi_window = max(b["h"] for b in bars[-(entry_bars + 1):-1])  # entry_bars предшествующих, без текущего
    lo_window = min(b["l"] for b in bars[-(exit_bars + 1):-1])   # exit_bars предшествующих, без текущего
    entry = cur["c"] > hi_window
    exit_ = cur["c"] < lo_window
    if decision_ts is None:
        decision_ts = utc_now_iso()
    ctx = dict(
        close=cur["c"], hi=round(hi_window, 6), lo=round(lo_window, 6),
        bar_open_ts=utc_iso_from_ms(cur["ts"]),
        bar_close_ts=utc_iso_from_ms(cur["ts"] + interval_ms),
        decision_ts=decision_ts,
        ts=utc_hhmm_from_ms(cur["ts"]),
    )
    return entry, exit_, ctx


def fetch_closed_bars(symbol, bars, interval_ms=INTERVAL_MS,
                      base="https://api-demo.bybit.com", now_ms=None, timeout=20):
    """Живая загрузка последних закрытых свечей с Bybit + контракт normalize_bars.

    Публичные данные демо-эндпоинта = mainnet. Биржа отдаёт «новые сверху» —
    контракт нормализует порядок.
    """
    end = int(time.time() * 1000)
    q = urllib.parse.urlencode({"category": "linear", "symbol": symbol,
                                "interval": str(int(interval_ms // 60000)),
                                "limit": str(bars + 2), "end": str(end)})
    with urllib.request.urlopen(urllib.request.Request(
            base + "/v5/market/kline?" + q,
            headers={"User-Agent": "bot-v3/1.1"}), timeout=timeout) as r:
        data = json.loads(r.read().decode())
    if data.get("retCode") != 0:
        raise RuntimeError("kline: %s %s" % (data.get("retCode"), data.get("retMsg")))
    rows = data["result"]["list"]  # новые сверху
    return normalize_bars(rows, interval_ms=interval_ms, now_ms=now_ms, last_n=bars)
