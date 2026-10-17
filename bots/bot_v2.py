#!/usr/bin/env python3
"""Бот v2 «Ленивый ММ, гибрид» — ТЗ 08 (Астерикс) + ТЗ 09 (Прима), внедрение 17.09.

Гибридное решение (решение Димы 17.09, разбор в 10-гибридном-ТЗ... см. доки проекта):
  Из ТЗ 09 (Прима, P0):
    - Истина о сделках — ТОЛЬКО биржа: /v5/position/closed-pnl -> closed_trades,
      net_pnl = closedPnl (уже за вычетом комиссий; проверено сверкой с executions 17.09).
      Локальный расчёт PnL по тикеру больше НЕ пишется как результат сделки.
    - Виртуальный банк 20 USDT (virtual_ledger) — демо-баланс всего счёта в отчёты не попадает.
    - Исполнения (executions) с фактическими комиссиями и maker-флагом.
  Из ТЗ 08 (Астерикс, Приоритет 1):
    - Позиция ПЕРЕЖИВАЕТ сессию: конец 50-минутного окна больше НЕ закрывает её маркетом.
      Выходы остаются только смысловые: TP / SL / TIME (max-hold 240 мин, ТЗ 08 §2.1).
      На старте сессия ПОДХВАТЫВАЕТ живую позицию и висящий TP с биржи.
  Стратегия (вход на биде) — без изменений: это деплой учёта и жизненного цикла,
  математика входа (фильтры RSI/BB, R/R>=1) — следующий этап по лестнице ТЗ 08.

Запуск: python3 bot_v2.py [--minutes 50] [--flatten]
  --flatten: аварийная уборка (закрыть позицию маркетом, отменить ордера) — вручную.

Эпоха v2-hybrid стартует 2026-09-17T11:05:00Z (exsync.CUTOFF_V2): все сделки,
закрытые позже, считаются от этой точки; v1-убыток (-0.0748) перенесён стартовой
корректировкой банка (см. virtual_ledger).
"""
import argparse, math, os, sqlite3, sys, time, traceback, uuid, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bybit_api as api
import botlog
import exsync
import position_state as pstate
import vol_filter as vf   # риск-модуль VOL-1 (t_39829ff5): нейтрален без VOL1_ENABLED=1

SYMBOL = "XRPUSDT"
# px-точность инструмента = tickSize (живой API 02.10, карточка t_28fa61b5):
# XRP 0.0001 / DOGE 0.00001 / ADA 0.0001 / ENA 0.00001 (проверено instruments-info).
PX_DECIMALS = {"XRPUSDT": 4, "DOGEUSDT": 5, "ADAUSDT": 4, "ENAUSDT": 5}
STRATEGY_VERSION = "pkgA-v2.1"
STRATEGY_ID = "bot-v2"
ACCOUNT = "v2-hybrid"  # виртуальный счёт бота (разделение банков 17.09 вечер)

# --- Фаза 4-live (ветка B, подготовка 26.09): ордера через центральный демон ---
# TRADE_CORE_DAEMON_SOCK: intent идёт на Unix-сокет демона (hello ->
# fencing token -> intent), как у v3 с Фазы 2b. Приоритет: демон > прямой
# API. Политика отказа: ВХОД (OPEN_RISK) fail-closed — без демона не входим;
# ВЫХОДЫ (CLOSE/отмены) при недоступном демоне идут прямым путём
# (reduce-only, новых рисков нет — выход обязан работать всегда).
# Отличие от v3: вход остаётся лимитным (Limit по биде; контракт intent
# знает только Market|Limit — PostOnly в нём нет, мейкер-семантика
# сохраняется ценой на биде, GTC вместо PostOnly-тайфрейма).
DAEMON_SOCK = os.environ.get("TRADE_CORE_DAEMON_SOCK", "")
EPOCH_ID = "epoch-19c58ada4fbc44af"  # открытая эпоха risk.db (MANIFEST v3.3.2)


class DaemonDown(Exception):
    """Демон недоступен (нет сокета / таймаут) — ордер НЕ отправлен."""


def _px_canon(p):
    """Цена для контракта intent: формат инструмента БЕЗ хвостовых нулей
    ('0.15980' -> '0.1598') — decimal.v1 канонический, иначе демон отвергнет
    leg с CONTRACT_INVALID (грабелька найдена оффлайн-тестом L6.6)."""
    s = f"{p:.{PX_DECIMALS.get(SYMBOL, 4)}f}".rstrip("0").rstrip(".")
    return s or "0"


def _daemon_client():
    """Клиент сокета демона. Корень репо в sys.path (не сам пакет trade_core
    — грабелька из v3 daemon_emit, коммит-фикс 26.09)."""
    sys.path.insert(0, os.path.join(
        os.path.dirname(os.path.abspath(__file__)), ".."))
    from trade_core.live import client as tcc
    return tcc


def daemon_order(db, bl, *, side, qty, price, purpose, reason,
                 order_type, reduce_only):
    """Ордер через центральный демон (Фаза 4-live): hello -> токен -> intent.

    Возвращает client_order_id (= orderLinkId биржи, журнал в orders уже
    написан) или None при явном reject. Недоступность демона -> DaemonDown.
    """
    tcc = _daemon_client()
    hello = tcc.hello(DAEMON_SOCK, "bot-v2", "BYBIT_DEMO", "local-account")
    if not hello.get("ok"):
        bl.error("risk", f"DAEMON hello отклонён: {hello.get('error')} "
                         f"{hello.get('detail', '')}", "")
        return None
    token = hello["fencing_token"]
    intent_id = f"bot-v2:v2-{purpose[:3].lower()}-{int(time.time()*1000)}"
    leg = {
        "category": "linear", "symbol": SYMBOL, "side": side,
        "order_type": order_type, "requested_qty": f"{qty:g}",
        "reduce_only": reduce_only, "limit_price": None,
        "leg_index": 0, "position_idx": 0,
    }
    if order_type == "Limit":
        leg["limit_price"] = _px_canon(price)
    intent = {
        "schema_version": "strategy-intent.v2",
        "intent_id": intent_id,
        "idempotency_key": intent_id,
        "bot_id": "bot-v2",
        "strategy_id": "lazy-mm",
        "strategy_version": STRATEGY_VERSION,
        "environment": "BYBIT_DEMO",
        "account_id": "local-account",
        "purpose": purpose,
        "legs": [leg],
        "signal_time": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                     time.gmtime(time.time() - 1)),
        "created_at": now(),
        "valid_until": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                     time.gmtime(time.time() + 60)),
        "decision_horizon_ms": 30000,
        "expected_hold_ms": MAX_HOLD_MIN * 60 * 1000,
        "market_snapshot_ref": "mds-live-collector",
        "epoch_id": EPOCH_ID,
    }
    try:
        r = tcc.intent(DAEMON_SOCK, intent, token)
    except tcc.DaemonUnavailable as e:
        raise DaemonDown(str(e)) from e
    gws = r.get("gateway_status")
    if gws == "ACCEPTED":
        coid = r.get("client_order_id")
        db.con.execute(
            """INSERT OR IGNORE INTO orders (order_id, symbol, category, side, order_type,
            time_in_force, qty, price, notional_usd, status, orderLinkId, reason, strategy,
            market_price_at_place, deviation_pct, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (coid, SYMBOL, "linear", side, order_type,
             "IOC" if order_type == "Market" else "GTC",
             float(qty), float(price or 0), float(qty) * float(price or 0),
             "New", coid, reason, "bot-v2",
             float(price) if price else None, None, now(), now()))
        db.con.commit()
        bl.info("risk", f"DAEMON {purpose} {qty:g} {SYMBOL} accepted "
                        f"(order_state={r.get('order_state')})",
                f"coid={coid}")
        return coid
    if gws == "DUPLICATE_RETURNED":
        bl.warn("risk", f"DAEMON duplicate: {r.get('reason_codes')}", "")
        return r.get("client_order_id")
    bl.error("risk", f"DAEMON {purpose} отклонён: {gws} "
                     f"{r.get('reject_code', '')} {r.get('reason_codes')}", reason)
    return None


def daemon_cancel(bl, coid):
    """Отмена ордера демона по client_order_id (= orderLinkId).

    True — отмена принята / ордер уже не активен; False — отказ
    (в т.ч. RECONCILE_FIRST: ордер мог исполниться раньше отмены).
    Недоступность демона -> DaemonDown (вызывающий решает про fallback).
    """
    tcc = _daemon_client()
    hello = tcc.hello(DAEMON_SOCK, "bot-v2", "BYBIT_DEMO", "local-account")
    if not hello.get("ok"):
        bl.error("risk", f"DAEMON cancel: hello отклонён {hello.get('error')}", "")
        return False
    try:
        r = tcc.cancel(DAEMON_SOCK, coid, hello["fencing_token"])
    except tcc.DaemonUnavailable as e:
        raise DaemonDown(str(e)) from e
    gws = r.get("gateway_status")
    if gws == "ACCEPTED":
        return True
    bl.warn("risk", f"DAEMON cancel: {gws} {r.get('reason_codes')} "
                    f"(order_state={r.get('order_state')})", "")
    return False

def px_fmt(p):
    return f"{p:.{PX_DECIMALS.get(SYMBOL, 4)}f}"
TP_PCT = 0.008        # +0.8% (пересмотр — этап 2 ТЗ 08)
SL_PCT = 0.012        # -1.2%  (пересмотр — этап 2 ТЗ 08)
MAX_WAIT_MIN = 10
MAX_HOLD_MIN = 240    # тайм-стоп позиции (живёт теперь РЕАЛЬЛЬНО, не обрезается сессией)
MAX_TRADES = 6
DAILY_LOSS_LIMIT = 0.20   # USDT виртуального банка за календарный день (ТЗ 09 §9.1: 1% от 20)
NOTIONAL_MIN, NOTIONAL_MAX = 5.05, 6.5
POLL_SEC = 30
QTY_STEP = 0.1
TICK = 0.0001

def now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def log(msg):
    print(f"[{now()}] {msg}", flush=True)

class Db:
    def __init__(self):
        self.con = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "trading.db"), timeout=30)
    def journal_order(self, o, symbol, side, qty, price, reason, strategy, mkt, orderLinkId=""):
        self.con.execute("""INSERT OR IGNORE INTO orders (order_id, symbol, category, side, order_type,
            time_in_force, qty, price, notional_usd, status, orderLinkId, reason, strategy,
            market_price_at_place, deviation_pct, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (o["result"]["orderId"], symbol, "linear", side, o.get("type", "Limit"),
             o.get("timeInForce", ""), float(qty), float(price), float(qty)*float(price),
             "New", orderLinkId or o["result"].get("orderLinkId", ""), reason, strategy, mkt,
             (float(price)-float(mkt))/float(mkt)*100 if mkt else None, now(), now()))
        self.con.commit()
        return o["result"]["orderId"]
    def update_status(self, oid, status, fills=None, avg=None, fee=None):
        ts_f = now() if status == "Filled" else None
        ts_c = now() if status in ("Cancelled", "Rejected", "Deactivated") else None
        self.con.execute("""UPDATE orders SET status=?, filled_qty=COALESCE(?,filled_qty),
            avg_fill_price=COALESCE(?,avg_fill_price), fee_paid=COALESCE(?,fee_paid),
            updated_at=?, ts_filled=COALESCE(?,ts_filled), ts_cancelled=COALESCE(?,ts_cancelled)
            WHERE order_id=?""", (status, fills, avg, fee, now(), ts_f, ts_c, oid))
        self.con.commit()
    def tick(self, symbol, t, source):
        bid = float(t.get("bid1Price", 0) or 0); ask = float(t.get("ask1Price", 0) or 0)
        mid = (bid+ask)/2 if bid and ask else float(t.get("lastPrice", 0))
        self.con.execute("""INSERT INTO price_ticks (symbol, ts, price, bid1, ask1, bid_qty1, ask_qty1, spread_bps, source)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (symbol, now(), float(t.get("lastPrice",0)), bid or None, ask or None,
             float(t.get("bid1Size",0) or 0) or None, float(t.get("ask1Size",0) or 0) or None,
             (ask-bid)/mid*10000 if bid and ask and mid else None, source))
        self.con.commit()
    def pos_event(self, symbol, side, event, qty, price, pnl=None, note=""):
        self.con.execute("""INSERT INTO position_events (symbol, side, event, qty, avg_price,
            position_value, realised_pnl, position_status, ts, note) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (symbol, side, event, qty, price, qty*price, pnl, event, now(), note))
        self.con.commit()
    def equity(self, note):
        w = api.wallet()
        if w:
            self.con.execute("""INSERT INTO equity_snapshots (ts, total_equity, available, wallet, note)
                VALUES (?,?,?,?,?)""",
                (now(), float(w.get("totalEquity", 0) or 0), float(w.get("totalAvailableBalance", 0) or 0),
                 float(w.get("totalWalletBalance", 0) or 0), note))
            self.con.commit()
    def run_update(self, run_id, **kw):
        sets = ", ".join(f"{k}=?" for k in kw)
        self.con.execute(f"UPDATE bot_runs SET {sets} WHERE run_id=?", (*kw.values(), run_id))
        self.con.commit()

def qty_for(px):
    step = 1.0 if SYMBOL in ("DOGEUSDT", "ADAUSDT", "ENAUSDT") else QTY_STEP
    q = math.floor(NOTIONAL_MIN / px / step) * step
    while q * px < NOTIONAL_MIN:
        q += step
    if q * px > NOTIONAL_MAX:
        return None
    return round(q, 1)

def qty_for_scaled(px, scale):
    """VOL-1: цель notional = база × scale, но НЕ НИЖЕ биржевого минимума $5.05
    (Bybit linear min $5). При текущих позициях у самого пола множитель <1.0
    не меняет qty — скейлер дозревает с ростом банка; экстремальная вола
    отсекается ручкой VOL1_SKIP_EXTREME в vol_filter.entry_check."""
    step = 1.0 if SYMBOL in ("DOGEUSDT", "ADAUSDT", "ENAUSDT") else QTY_STEP
    target = max(NOTIONAL_MIN, NOTIONAL_MIN * scale)
    q = math.floor(target / px / step) * step
    while q * px < NOTIONAL_MIN:
        q += step
    if q * px > NOTIONAL_MAX:
        return None
    return round(q, 1)

def market_close(db, bl, qty, reason, order_link_prefix):
    """Маркет-закрытие (только SL/TIME/flatten — НЕ сессионный таймер)."""
    m = api.post("/v5/order/create", {
        "category": "linear", "symbol": SYMBOL, "side": "Sell", "orderType": "Market",
        "qty": f"{qty:g}", "reduceOnly": "true", "timeInForce": "IOC",
        "orderLinkId": f"{order_link_prefix}-{int(time.time()*1000)}"})
    if m.get("retCode") == 0:
        # v2: цена/статус уточнятся синхронизацией (executions/closed-pnl) — не выдумываем fill
        db.journal_order(m, SYMBOL, "Sell", qty, 0.0, reason, "bot-v2", 0.0, "")
        bl.warn("exit", "маркет-выход отправлен; истина о цене/комиссиях придёт с биржи (sync)",
                f"reason={reason} qty={qty}")
        return True
    bl.error("api_err", f"маркет-закрытие отклонено биржей: {m.get('retCode')} {m.get('retMsg','')[:80]}",
             f"reason={reason} qty={qty}")
    return False

def _order_by_link_id(symbol, link_id):
    """Ордер биржи по orderLinkId (ордера демона живут под client_order_id
    = orderLinkId, а не под orderId биржи). Поиск: realtime, затем история."""
    for o in api.open_orders(symbol):
        if o.get("orderLinkId") == link_id:
            return o
    for h in api.order_history(symbol, limit=20):
        if h.get("orderLinkId") == link_id:
            return h
    return None


def wait_fill(oid, deadline, db, symbol):
    while time.time() < deadline:
        time.sleep(POLL_SEC)
        t = api.ticker(symbol)
        if t: db.tick(symbol, t, "bot-poll")
        oo = [o for o in api.open_orders(symbol) if o["orderId"] == oid or o.get("orderLinkId") == oid]
        if not oo:
            for h in api.order_history(symbol, limit=10):
                if h["orderId"] == oid or h.get("orderLinkId") == oid:
                    st = h["orderStatus"]
                    db.update_status(oid, st, float(h.get("cumExecQty",0) or 0),
                                     float(h.get("avgPrice",0) or 0), float(h.get("cumExecFee",0) or 0))
                    if st == "Filled":
                        return float(h["cumExecQty"]), float(h["avgPrice"])
                    return None, None
            continue
        cur = oo[0]
        if cur.get("orderStatus") == "Filled":
            db.update_status(oid, "Filled", float(cur.get("cumExecQty",0) or 0),
                             float(cur.get("avgPrice",0) or 0), float(cur.get("cumExecFee",0) or 0))
            return float(cur["cumExecQty"]), float(cur["avgPrice"])
    return None, None

def settle(db, bl, run_id, note_ctx):
    """Синхронизация сделок с биржи + лог новых результатов (v2: истина — биржа).
    Пакет А, фикс 2: symbol обязателен — DOGE синхронизируется как DOGE (§4.2)."""
    try:
        new = exsync.sync_closed_trades(db.con, symbol=SYMBOL)
        exsync.sync_executions(db.con, symbol=SYMBOL)
    except Exception as e:
        bl.error("sync", f"sync с биржей не удался: {e}", "")
        return 0.0
    pnl_sum = 0.0
    for t in new:
        pnl_sum += float(t["closedPnl"])
        bl.info("exit", f"СДЕЛКА ЗАКРЫТА (биржа): net {float(t['closedPnl']):+.5f}$ "
                        f"qty={t['qty']} вход~{t['avgEntryPrice']} выход~{t['avgExitPrice']}",
                f"orderId={t['orderId'][:8]} {note_ctx}")
    if new:
        bal = exsync.virtual_balance(db.con, ACCOUNT)
        bl.info("bank", f"виртуальный банк {ACCOUNT}: {bal:.5f} USDT", "")
        log(f"SYNC: {len(new)} новых сделок, банк {ACCOUNT} {bal:.5f}")
        db.run_update(run_id, pnl_realised=round(pnl_sum, 4))
    return pnl_sum

def adopt_position(db, bl):
    """Подхват живой позиции с биржи (ТЗ 08 Приоритет 1-Б). Возвращает (qty, entry_px, tp_oid) или (None,..).
    Пакет А, фикс 5: после ADOPT hold-deadline НЕ сбрасывается — он из БД (position_hold),
    для неизвестного старта восстанавливается из executions (не «сейчас»)."""
    for p in api.positions(SYMBOL):
        sz = float(p.get("size", 0) or 0)
        if sz > 0 and p.get("side", "").lower() == "buy":
            entry_px = float(p.get("avgPrice", 0) or 0)
            # неизменяемый deadline: из БД, иначе из executions, иначе (last resort) сейчас
            row = db.con.execute("SELECT deadline FROM position_hold WHERE strategy=? AND symbol=?",
                                 (STRATEGY_ID, SYMBOL)).fetchone() if _has_table(db.con, "position_hold") else None
            if row and row[0]:
                hold_deadline, dl_src = row[0], "db"
            else:
                ff = _first_fill_epoch(db, SYMBOL)
                if ff is None:
                    ff = time.time()
                    dl_src = "now (executions пусты — хуже, но явно)"
                else:
                    dl_src = "executions"
                hold_deadline, _ = pstate.load_or_init_deadline(db.con, STRATEGY_ID, SYMBOL, ff, MAX_HOLD_MIN)
            bl.warn("adopt", f"подхват позиции {sz} @ {entry_px}; hold-deadline из {dl_src}: "
                              f"{datetime.datetime.fromtimestamp(hold_deadline, datetime.timezone.utc).strftime('%H:%M:%SZ')}",
                    f"возраст позиции сохранён (§4.5)")
            tp_oid = None
            for o in api.open_orders(SYMBOL):
                if o.get("side") == "Sell" and o.get("orderType") == "Limit":
                    tp_oid = o["orderId"]
                    db.pos_event(SYMBOL, "Buy", "reduce", 0, entry_px, None,
                                 f"ADOPT: подхват позиции с прошлой сессии (маркер; qty см. биржу), TP уже на бирже @ {o.get('price')}")
                    break
            if tp_oid is None:
                tp_px = round(entry_px * (1 + TP_PCT), PX_DECIMALS.get(SYMBOL, 4))
                tp_reason = f"v2-adopt: восстановлен TP от входа {entry_px}"
                o2 = api.place_order(SYMBOL, "Sell", f"{sz:g}", f"{px_fmt(tp_px)}", "GTC", tp_reason, "bot-v2")
                if o2.get("retCode") == 0:
                    tp_oid = db.journal_order(o2, SYMBOL, "Sell", sz, tp_px, tp_reason, "bot-v2", entry_px)
                    bl.warn("adopt", f"подхвачена позиция {sz} @ {entry_px} БЕЗ TP — TP восстановлен @ {tp_px}",
                            f"oid={tp_oid}")
                else:
                    bl.error("adopt", f"подхвачена позиция {sz} @ {entry_px}, TP поставить НЕ УДАЛОСЬ: "
                                      f"{o2.get('retMsg','')[:60]} —monitor via SL only", "")
            else:
                bl.warn("adopt", f"подхвачена позиция {sz} {SYMBOL} @ {entry_px} (TP на бирже)", f"tp={tp_oid[:8]}")
                log(f"ADOPT: позиция {sz} {SYMBOL} @ {entry_px}, tp_oid={tp_oid}")
            return sz, entry_px, tp_oid, hold_deadline
    return None, None, None, None

def _has_table(con, name):
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None

def _first_fill_epoch(db, symbol, lookback_min=2880):
    """Время первого исполнения живой позиции из executions (§4.5). None — данных нет."""
    since = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=lookback_min)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        row = db.con.execute(
            "SELECT exec_time FROM executions WHERE symbol=? AND side='Buy' AND exec_time>=? "
            "ORDER BY exec_time DESC LIMIT 1", (symbol, since)).fetchone()
        if row:
            t = datetime.datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
            return t.timestamp()
    except Exception:
        pass
    return None

def monitor_position(db, bl, run_id, fq, entry_px, tp_oid, t_end, hold_deadline=None):
    """Мониторинг позиции до TP/SL/TIME или конца сессии (v2: конец сессии = ПЕРЕДАЧА, не закрытие).
    Пакет А, фикс 5 (§4.5): hold_deadline приходит снаружи (БД position_hold) и НЕ
    пересчитывается от «сейчас» — после ADOPT возраст позиции сохраняется.
    Возвращает ('handover', None) если сессия кончилась при живой позиции, иначе (reason, exit_px)."""
    if hold_deadline is None:
        hold_deadline = time.time() + MAX_HOLD_MIN * 60  # только для позиций, открытых в этой же сессии
    last = 0.0
    while True:
        time.sleep(POLL_SEC)
        t = api.ticker(SYMBOL)
        if t:
            db.tick(SYMBOL, t, "bot-poll")
            last = float(t.get("lastPrice", 0) or 0)
        # TP исполнен?
        if tp_oid:
            oo = [x for x in api.open_orders(SYMBOL)
                  if x["orderId"] == tp_oid or x.get("orderLinkId") == tp_oid]
            if not oo:
                for h in api.order_history(SYMBOL, limit=10):
                    if h["orderId"] == tp_oid or h.get("orderLinkId") == tp_oid:
                        st = h["orderStatus"]
                        db.update_status(tp_oid, st, float(h.get("cumExecQty",0) or 0),
                                         float(h.get("avgPrice",0) or 0), float(h.get("cumExecFee",0) or 0))
                        if st == "Filled":
                            return "TP", float(h["avgPrice"])
                        break
        # SL по lastPrice
        if last and last < entry_px * (1 - SL_PCT):
            return "SL", None
        # TIME: честный тайм-стоп 240 мин (ТЗ 08 §2.1)
        if time.time() >= hold_deadline:
            return "TIME", None
        # конец СЕССИИ при живой позиции -> передача следующей сессии (v2-я суть)
        if time.time() >= t_end:
            return "handover", None

def main():
    global SYMBOL
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=50)
    ap.add_argument("--flatten", action="store_true", help="аварийная уборка: закрыть всё, отменить ордера")
    ap.add_argument("--symbol", default=SYMBOL)
    args = ap.parse_args()
    SYMBOL = args.symbol
    t_end = time.time() + args.minutes * 60
    run_id = str(uuid.uuid4())[:8]
    bl = botlog.BotLog(run_id)
    db = Db()

    if args.flatten:
        bl.warn("flatten", "ручной --flatten: закрываю всё", "")
        for o in api.open_orders(SYMBOL):
            if api.cancel_order(SYMBOL, o["orderId"]).get("retCode") == 0:
                db.update_status(o["orderId"], "Cancelled")
        for p in api.positions(SYMBOL):
            sz = float(p.get("size", 0) or 0)
            if sz > 0:
                market_close(db, bl, sz, "v2-manual-flatten", "loki-flat")
        settle(db, bl, run_id, "manual-flatten")
        return

    db.con.execute("""INSERT INTO bot_runs (run_id, strategy, script, started_at, orders_placed,
        orders_filled, orders_cancelled, pnl_realised, notes)
        VALUES (?,?,?,?,0,0,0,NULL,?)""",
        (run_id, "bot-v2-lazy-mm-hybrid", "bot_v2.py", now(),
         f"v2-HYBRID {STRATEGY_VERSION}(ТЗ08+09+ПакетА) TP={TP_PCT} SL={SL_PCT} hold={MAX_HOLD_MIN}m max={MAX_TRADES} "
         f"dayLoss={DAILY_LOSS_LIMIT} позиция переживает сессию"))
    db.con.commit()
    db.equity(f"run={run_id} start")

    # Пакет А, фикс 6 (§4.7): lease — второй экземпляр bot-v2 на том же символе не стартует
    if not pstate.acquire_lease(db.con, STRATEGY_ID, SYMBOL, run_id, ttl_sec=args.minutes * 60 + 120):
        bl.error("risk", f"LEASE: {STRATEGY_ID}/{SYMBOL} уже ведётся другой сессией — выходим", "")
        log("LEASE занят — выход")
        db.run_update(run_id, ended_at=now())  # фикс 15.10: lease-выход тоже закрывает строку bot_runs
        return
    try:
        _trading_loop(db, bl, run_id, args, t_end)
    finally:
        pstate.release_lease(db.con, STRATEGY_ID, SYMBOL, run_id)


def _trading_loop(db, bl, run_id, args, t_end):
    bl.session("start", f"v2-гибрид: сессия {args.minutes} мин, {SYMBOL}, TP +{TP_PCT*100}% / SL -{SL_PCT*100}%",
               f"банк {ACCOUNT}={exsync.virtual_balance(db.con, ACCOUNT):.4f} (вирт., раздельные банки)")
    log(f"СТАРТ v2 run={run_id}: гибрид ТЗ08+09, банк {ACCOUNT} {exsync.virtual_balance(db.con, ACCOUNT):.4f}")

    # === v2: сначала синхронизация — что закрылось, пока сессия спала ===
    settle(db, bl, run_id, "стартовая синхронизация")

    # === v2: дневной лимит убытка по виртуальному банку (ТЗ 09 §9.1) ===
    # Пакет А, фикс 6 (§4.7): по дню БИРЖЕВОГО СОБЫТИЯ (closed_at), не дню импорта
    day_start = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    day_pnl = exsync.pnl_since_event(db.con, day_start, account=ACCOUNT)
    day_block = day_pnl <= -DAILY_LOSS_LIMIT
    if day_block:
        bl.warn("risk", f"дневной лимит убытка (по событиям): {day_pnl:+.4f}$ <= -{DAILY_LOSS_LIMIT}$ — новые входы запрещены до конца дня", "")

    # === v2: подхват живой позиции (если была передана прошлой сессией) ===
    pos_qty, entry_px, tp_oid, hold_deadline = adopt_position(db, bl)
    adopted = pos_qty is not None
    trades = 0
    reason = None
    placed_cnt = filled_cnt = cancelled_cnt = 0
    api_bad = False

    while time.time() < t_end and (trades < MAX_TRADES or adopted):
        if time.time() >= t_end:
            break
        if adopted:
            fq, trades = pos_qty, trades + 1  # продолжение сделки прошлого окна
        else:
            # === фаза: нет позиции — вход (если лимит дня не пробит) ===
            if day_block:
                time.sleep(POLL_SEC)
                continue
            t = api.ticker(SYMBOL)
            if not t:
                if not api_bad:
                    bl.warn("api_err", "тикер не отвечает (переход в ожидание)")
                    api_bad = True
                time.sleep(POLL_SEC); continue
            if api_bad:
                bl.info("api_err", "тикер снова отвечает")
                api_bad = False
            db.tick(SYMBOL, t, "bot-cycle")
            bid = float(t["bid1Price"])
            q = qty_for(bid)
            if q is None:
                time.sleep(POLL_SEC); continue
            # VOL-1 (t_39829ff5): гейт часа + масштаб от волы; без VOL1_ENABLED=1 нейтральны.
            # adopted-позиции (переданные прошлой сессией) НЕ гейтятся — только новые входы.
            if not adopted:
                vf_ok, vf_scale, vf_why = vf.entry_check(SYMBOL, None, lambda: api.kline_closes(SYMBOL))
                if not vf_ok:
                    log(vf_why + " — вход пропущен, ждём")
                    time.sleep(POLL_SEC); continue
                if vf_scale < 1.0:
                    q = qty_for_scaled(bid, vf_scale)
                    if q is None:
                        log(f"VOL1: масштаб {vf_scale} вывел notional из ${NOTIONAL_MIN}-{NOTIONAL_MAX} — вход пропущен")
                        time.sleep(POLL_SEC); continue
            reason = f"{vf_why} | v2-mm: вход на биде B={bid}, спред={(float(t['ask1Price'])-bid)/bid*10000:.1f}bps"
            # Пакет А, фикс 6 (§4.7): атомарная проверка+резерв перед КАЖДЫМ входом —
            # свежий банк, свежий дневной лимит (по событиям), свободный капитал после резервов
            allowed, why, rctx = pstate.risk_gate(db.con, STRATEGY_ID, SYMBOL, q * bid,
                                                  DAILY_LOSS_LIMIT, run_id,
                                                  ttl_sec=args.minutes * 60 + 120,
                                                  reserve_notional=False)
            if not allowed:
                bl.warn("risk", why, "")
                time.sleep(POLL_SEC); continue
            # Фаза 4-live: вход через демона (Limit по биде, fail-closed —
            # без демона новых входов НЕТ, повтор на следующем цикле)
            entry_oid = None
            if DAEMON_SOCK:
                try:
                    entry_oid = daemon_order(db, bl, side="Buy", qty=q, price=bid,
                                             purpose="OPEN_RISK", reason=reason,
                                             order_type="Limit", reduce_only=False)
                except DaemonDown as e:
                    bl.error("risk", f"DAEMON недоступен ({e}) — вход НЕ отправлен "
                                     f"(fail-closed), ждём следующего цикла", "")
                    time.sleep(POLL_SEC); continue
                if entry_oid is None:
                    time.sleep(POLL_SEC); continue
            else:
                o = api.place_order(SYMBOL, "Buy", f"{q:g}", f"{px_fmt(bid)}", "PostOnly", reason, "bot-v2")
                if o.get("retCode") != 0:
                    bl.warn("entry", f"вход отклонён биржей: {o.get('retCode')} {o.get('retMsg','')[:80]}", reason)
                    time.sleep(POLL_SEC); continue
                entry_oid = db.journal_order(o, SYMBOL, "Buy", q, f"{bid:.4f}", reason, "bot-v2", bid)
            placed_cnt += 1
            db.run_update(run_id, orders_placed=placed_cnt)
            bl.info("entry", f"вход выставлен {q} {SYMBOL} @ {bid} "
                             f"{'Limit-GTC (daemon)' if DAEMON_SOCK else 'PostOnly'}, ждём {MAX_WAIT_MIN} мин", f"oid={entry_oid}")
            fq, ap_ = wait_fill(entry_oid, time.time() + MAX_WAIT_MIN * 60, db, SYMBOL)
            if fq is None:
                # Фаза 4-live: отмена входа через демона; демон недоступен ->
                # прямой путь (отмена не создаёт риска, а зависший ордер хуже)
                cancelled = False
                if DAEMON_SOCK:
                    try:
                        cancelled = daemon_cancel(bl, entry_oid)
                    except DaemonDown as e:
                        bl.warn("risk", f"DAEMON недоступен ({e}) — отмена входа "
                                        f"прямым путём (fallback)", "")
                if not cancelled:
                    api.cancel_order(SYMBOL, entry_oid)
                db.update_status(entry_oid, "Cancelled")
                cancelled_cnt += 1
                db.run_update(run_id, orders_cancelled=cancelled_cnt)
                bl.info("entry", "вход не исполнился за окно — отмена, новый круг")
                continue
            entry_px = ap_
            filled_cnt += 1
            bl.info("fill", f"ВХОД исполнен: {fq} {SYMBOL} @ {entry_px}")
            db.pos_event(SYMBOL, "Buy", "open", fq, entry_px, note=reason)
            # Пакет А, фикс 5: неизменяемый deadline от фактического первого исполнения
            ff = _first_fill_epoch(db, SYMBOL) or time.time()
            hold_deadline, _ = pstate.load_or_init_deadline(db.con, STRATEGY_ID, SYMBOL, ff, MAX_HOLD_MIN)
            tp_px = round(entry_px * (1 + TP_PCT), PX_DECIMALS.get(SYMBOL, 4))
            tp_reason = f"v2-mm: TP от входа {entry_px}"
            # Пакет А, фикс 4 (§4.4): TP — reduceOnly (нет голого шорта при ручном/аварийном закрытии)
            # Фаза 4-live: TP через демона (Limit GTC reduceOnly). Демон упал
            # или reject -> прямой TP (выход обязан стоять всегда, §4.4)
            tp_oid = None
            if DAEMON_SOCK:
                try:
                    tp_oid = daemon_order(db, bl, side="Sell", qty=fq, price=tp_px,
                                          purpose="REDUCE_RISK", reason=tp_reason,
                                          order_type="Limit", reduce_only=True)
                except DaemonDown as e:
                    bl.warn("risk", f"DAEMON недоступен ({e}) — TP прямым путём (fallback)", "")
            if tp_oid is not None:
                placed_cnt += 1
                db.run_update(run_id, orders_placed=placed_cnt)
                bl.info("tp", f"TP выставлен @ {tp_px} (GTC, daemon)", f"oid={tp_oid} вход={entry_px}")
            else:
                o2 = api.post("/v5/order/create", {
                    "category": "linear", "symbol": SYMBOL, "side": "Sell", "orderType": "Limit",
                    "qty": f"{fq:g}", "price": f"{px_fmt(tp_px)}", "timeInForce": "GTC",
                    "reduceOnly": "true",
                    "orderLinkId": f"v2tp-{int(time.time()*1000)}"})
                if o2.get("retCode") == 0:
                    placed_cnt += 1
                    tp_oid = db.journal_order(o2, SYMBOL, "Sell", fq, tp_px, tp_reason, "bot-v2", bid)
                    db.run_update(run_id, orders_placed=placed_cnt)
                    bl.info("tp", f"TP выставлен @ {tp_px} (GTC)", f"oid={tp_oid} вход={entry_px}")
                else:
                    bl.error("tp", f"TP ОТКЛОНЁН: {o2.get('retCode')} {o2.get('retMsg','')[:80]} — идём к SL/TIME", "")

        # === мониторинг позиции ===
        reason, exit_px = monitor_position(db, bl, run_id, fq, entry_px, tp_oid, t_end)
        if reason == "handover":
            # v2: ГЛАВНОЕ ИЗМЕНЕНИЕ — позиция НЕ закрывается, передаётся следующей сессии
            db.pos_event(SYMBOL, "Buy", "reduce", 0, 0.0, None,
                         f"HANDOVER: позиция {fq} @ {entry_px} передана следующей сессии run={run_id}")
            bl.warn("handover", f"конец сессии при живой позиции {fq} {SYMBOL} @ {entry_px} — ПЕРЕДАЮ следующей сессии "
                                f"(TP/SL остаются на бирже; ТЗ 08 Приоритет 1)", "")
            log(f"HANDOVER: позиция {fq} @ {entry_px} жива, TP на бирже — не закрываю")
            break
        if reason == "TP":
            bl.info("exit", f"TP исполнен @ {exit_px} — итог придёт с биржи (sync)", f"вход={entry_px}")
            log(f"TP исполнен @ {exit_px}")
            pstate.clear_deadline(db.con, STRATEGY_ID, SYMBOL)
        else:  # SL / TIME
            bl.warn("exit", f"сработал {reason}: закрываю позицию маркетом", f"вход={entry_px}")
            if tp_oid:
                # Фаза 4-live: отмена TP через демона (fallback — прямой путь;
                # отмена риска не создаёт, зависший TP при выходе хуже)
                tp_cancelled = False
                if DAEMON_SOCK:
                    try:
                        tp_cancelled = daemon_cancel(bl, tp_oid)
                    except DaemonDown as e:
                        bl.warn("risk", f"DAEMON недоступен ({e}) — отмена TP прямым путём", "")
                if not tp_cancelled:
                    api.cancel_order(SYMBOL, tp_oid)
                db.update_status(tp_oid, "Cancelled")
                cancelled_cnt += 1
                db.run_update(run_id, orders_cancelled=cancelled_cnt)
            # Фаза 4-live: закрытие через демона (CLOSE Market reduceOnly);
            # демон упал/reject -> прямой маркет-выход (позицию нельзя бросать)
            closed_ok = False
            if DAEMON_SOCK:
                try:
                    closed_ok = daemon_order(db, bl, side="Sell", qty=fq, price=None,
                                             purpose="CLOSE", reason=f"v2-mm: {reason} выход маркетом",
                                             order_type="Market", reduce_only=True) is not None
                except DaemonDown as e:
                    bl.warn("risk", f"DAEMON недоступен ({e}) — маркет-выход прямым путём (fallback)", "")
            if not closed_ok:
                closed_ok = market_close(db, bl, fq, f"v2-mm: {reason} выход маркетом", "loki-exit")
            if not closed_ok:
                # Пакет А, фикс 4 (§4.4): отказ биржи ≠ закрытие — позиция жива, следующий
                # вход невозможен; следующая итерация/сессия обязана начать с reconciliation
                bl.error("exit", "маркет-закрытие НЕ прошло — позиция считается живой (EXIT_PENDING), "
                                 "новых входов нет до подтверждения закрытия", "")
                db.pos_event(SYMBOL, "Buy", "reduce", 0, entry_px, None,
                             f"EXIT_PENDING: {reason} — закрытие отправлено/отклонено, ждём подтверждения биржи")
                break
            pstate.clear_deadline(db.con, STRATEGY_ID, SYMBOL)
        # === v2: истина о результате — только биржа ===
        time.sleep(3)
        settle(db, bl, run_id, f"после {reason}")
        adopted = False
        trades += 1
        # circuit breaker по виртуальному банку за день (по событиям, §4.7)
        day_pnl = exsync.pnl_since_event(db.con, day_start, account=ACCOUNT)
        if day_pnl <= -DAILY_LOSS_LIMIT:
            bl.warn("interrupt", f"CIRCUIT BREAKER v2: дневной убыток {day_pnl:+.4f}$ >= {DAILY_LOSS_LIMIT}$ — стоп", "")
            break

    db.run_update(run_id, ended_at=now())
    db.equity(f"run={run_id} end")
    bal = exsync.virtual_balance(db.con, ACCOUNT)
    bl.session("finish", f"ФИНИШ v2: банк {ACCOUNT} {bal:.5f} USDT, ордеров {placed_cnt}/{filled_cnt}/{cancelled_cnt}",
               f"сделок в окне {trades}; позиция {'ПЕРЕДАНА (жива)' if reason == 'handover' else 'закрыта/отсутствует'}")
    log(f"ФИНИШ v2 run={run_id}: банк {ACCOUNT} {bal:.5f}")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print(f"[{now()}] прервано вручную", flush=True)
        try:
            botlog.BotLog().crash("прервано вручную (KeyboardInterrupt) — ПОЗИЦИЯ МОЖЕТ БЫТЬ ЖИВА, запусти --flatten при необходимости")
        except Exception:
            pass
        raise
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[{now()}] CRASH: {e}\n{tb}", file=sys.stderr, flush=True)
        try:
            botlog.BotLog().crash(f"v2 сессия упала: {type(e).__name__}: {e} — позиция может быть жива", tb)
        except Exception:
            pass
        sys.exit(1)
