#!/usr/bin/env python3
"""Бот v1 «Ленивый маркет-мейкер» — REVIVAL (карточка t_776e9b14, 17.09 вечер).

Архивный v1 (XRP, SESSION-END-логика) поднят как живой демо-бот №1 на ADAUSDT.
Стратегия НЕ менялась (06-стратегия-v1-ленивый-ММ.md): вход PostOnly на биде,
TP лимиткой, SL/time-stop маркетом, circuit breaker, сессия заканчивается ЧИСТО
(SESSION-END закрывает позицию — это идентичность v1, отличие от v2/v3).

Что добавлено при revival (инфраструктура v2, фиксы Пакета А где дёшево):
  - символ ADAUSDT (НЕ XRP — конфликт позиций с v3-Donchian на демо-счёте);
  - STRATEGY_VERSION='v1-revival-1', epoch='v1-revival' (не смешивать с мёртвой v1);
  - истина о сделках — биржа: exsync closed-pnl -> closed_trades, банк v1-revival;
  - виртуальный банк v1-revival (раздельные банки трёх ботов, старт 19.91029153);
  - явный symbol во ВСЕХ sync-вызовах (аудит №14 §4.2);
  - reduceOnly на всех выходах (§4.4): TP и маркет;
  - UTC-время в логах (было и так; контролируем);
  - неудачный маркет-выход НЕ считает позицию FLAT: стоп сессии + финальный
    flatten попробует снова (§4.4 минимум);
  - lease + risk_gate перед входом (§4.7): свежий банк СВОЕГО счёта, дневной
    лимит по дню события, атомарный резерв;
  - hold-deadline от времени первого исполнения, переживает рестарты (§4.5).

Запуск: python3 bot_v1.py [--minutes 30] [--symbol ADAUSDT] [--flatten]
"""
import argparse, math, os, sqlite3, sys, time, traceback, uuid, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bybit_api as api
import botlog
import exsync
import position_state as pstate
import vol_filter as vf   # риск-модуль VOL-1 (t_39829ff5): нейтрален без VOL1_ENABLED=1

SYMBOL = "ADAUSDT"
PX_DECIMALS = {"XRPUSDT": 4, "DOGEUSDT": 5, "ADAUSDT": 4}
QTY_STEPS = {"XRPUSDT": 0.1, "DOGEUSDT": 1.0, "ADAUSDT": 1.0}
STRATEGY_VERSION = "v1-revival-1"
STRATEGY_ID = "bot-v1"
EPOCH = "v1-revival"
ACCOUNT = "v1-revival"   # виртуальный счёт бота (разделение банков 17.09 вечер)
TP_PCT = 0.008        # +0.8% (параметры ленивого ММ без изменений)
SL_PCT = 0.012        # -1.2%
MAX_WAIT_MIN = 10     # сколько ждать исполнения входа
MAX_HOLD_MIN = 240    # тайм-стоп позиции
MAX_TRADES = 6        # сделок за сессию
SESSION_LOSS_LIMIT = 1.00   # $ стоп-лосс сессии (диагностическая оценка)
DAILY_LOSS_LIMIT = 1.00     # risk_gate: лимит дня по счёту v1-revival (биржа)
NOTIONAL_MIN, NOTIONAL_MAX = 5.05, 6.5
POLL_SEC = 30

# --- Фаза 4 offline (карточка t_7ea8df42): ордера через центральный демон ---
# TRADE_CORE_DAEMON_SOCK: intent идёт на Unix-сокет демона (hello ->
# fencing token -> intent), как у v3 (Фаза 2b) и v2 (Фаза 4-live, ветка B).
# Приоритет: демон > прямой API. Политика отказа — как у v2: ВХОД
# (OPEN_RISK) fail-closed (без демона не входим), ВЫХОДЫ (TP/отмены/CLOSE)
# при недоступном демоне идут прямым путём (reduce-only, новых рисков нет).
# Вход остаётся лимитным по биде (Limit GTC вместо PostOnly: контракт intent
# знает только Market|Limit, мейкер-семантика сохраняется ценой на биде).
# ФЛИПА НЕТ: обёртка v1 НЕ экспортирует sock — включит карточка cutover v1.
DAEMON_SOCK = os.environ.get("TRADE_CORE_DAEMON_SOCK", "")
EPOCH_ID = "epoch-19c58ada4fbc44af"  # открытая эпоха risk.db (MANIFEST v3.3.2)


class DaemonDown(Exception):
    """Демон недоступен (нет сокета / таймаут) — ордер НЕ отправлен."""


def _px_canon(p):
    """Цена для контракта intent: формат инструмента БЕЗ хвостовых нулей
    ('0.53500' -> '0.535') — decimal.v1 канонический, иначе демон отвергнет
    leg с CONTRACT_INVALID (грабелка v2, оффлайн-тест L6.6)."""
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
    """Ордер через центральный демон (Фаза 4): hello -> токен -> intent.

    Возвращает client_order_id (= orderLinkId биржи, журнал в orders уже
    написан) или None при явном reject. Недоступность демона -> DaemonDown.
    Отличие от v2: недоступность на ШАГЕ hello тоже конвертируется в
    DaemonDown (в v2 hello вне try — латентный дефект, найден при порте,
    отмечен в отчёте Фазы 4: разово упавший сокет ронял бы сессию).
    """
    tcc = _daemon_client()
    try:
        hello = tcc.hello(DAEMON_SOCK, "bot-v1", "BYBIT_DEMO",
                          "local-account")
    except tcc.DaemonUnavailable as e:
        raise DaemonDown(str(e)) from e
    if not hello.get("ok"):
        bl.error("risk", f"DAEMON hello отклонён: {hello.get('error')} "
                         f"{hello.get('detail', '')}", "")
        return None
    token = hello["fencing_token"]
    intent_id = f"bot-v1:v1-{purpose[:3].lower()}-{int(time.time()*1000)}"
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
        "bot_id": "bot-v1",
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
             "New", coid, reason, "bot-v1",
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
    Недоступность демона (включая шаг hello) -> DaemonDown.
    """
    tcc = _daemon_client()
    try:
        hello = tcc.hello(DAEMON_SOCK, "bot-v1", "BYBIT_DEMO",
                          "local-account")
    except tcc.DaemonUnavailable as e:
        raise DaemonDown(str(e)) from e
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

def now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def log(msg):
    print(f"[{now()}] {msg}", flush=True)

def px_fmt(p):
    return f"{p:.{PX_DECIMALS.get(SYMBOL, 4)}f}"

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
    step = QTY_STEPS.get(SYMBOL, 0.1)
    q = math.floor(NOTIONAL_MIN / px / step) * step
    while q * px < NOTIONAL_MIN:
        q += step
    if q * px > NOTIONAL_MAX:
        return None
    return q

def qty_for_scaled(px, scale):
    """VOL-1: цель notional = база × scale, но НЕ НИЖЕ биржевого минимума $5.05
    (Bybit linear min $5 — ниже нельзя). При текущих позициях у самого пола
    (базовый расчёт уже = минимум) множитель <1.0 не меняет qty — скейлер
    дозревает с ростом банка; экстремальная вола отсекается гейтом
    VOL1_SKIP_EXTREME в vol_filter.entry_check, не размером."""
    step = QTY_STEPS.get(SYMBOL, 0.1)
    target = max(NOTIONAL_MIN, NOTIONAL_MIN * scale)
    q = math.floor(target / px / step) * step
    while q * px < NOTIONAL_MIN:
        q += step
    if q * px > NOTIONAL_MAX:
        return None
    return q

def market_close(db, bl, qty, reason, order_link_prefix):
    """Маркет-закрытие позиции Sell reduceOnly + журнал. Истина о цене — sync."""
    m = api.post("/v5/order/create", {
        "category": "linear", "symbol": SYMBOL, "side": "Sell", "orderType": "Market",
        "qty": f"{qty:g}", "reduceOnly": "true", "timeInForce": "IOC",
        "orderLinkId": f"{order_link_prefix}-{int(time.time()*1000)}"})
    if m.get("retCode") == 0:
        db.journal_order(m, SYMBOL, "Sell", qty, 0.0, reason, "bot-v1", 0.0, "")
        bl.warn("exit", "маркет-выход отправлен; цена/комиссии придут с биржи (sync)",
                f"reason={reason} qty={qty}")
        return True
    bl.error("api_err", f"маркет-закрытие отклонено биржей: {m.get('retCode')} {m.get('retMsg','')[:80]}",
             f"reason={reason} qty={qty}")
    log(f"ОШИБКА маркет-закрытия: {m.get('retCode')} {m.get('retMsg','')[:60]}")
    return False

def flatten_leftovers(db, bl, note):
    """Уборка хвостов на СВОЁМ символе: отменить ордера, закрыть позиции.

    Возвращает (acted, clean): acted — число действий, clean — True если после
    уборки на символе пусто (позиций нет, ордеров нет). Неудачное закрытие
    НЕ считается чистым (§4.4 минимум: не считать FLAT без подтверждения).
    """
    acted = 0
    for o in api.open_orders(SYMBOL):
        c = api.cancel_order(SYMBOL, o["orderId"])
        if c.get("retCode") == 0:
            db.update_status(o["orderId"], "Cancelled")
            bl.warn("flatten", f"отменён висящий ордер {o['orderId'][:8]} (хвост прошлой сессии)", note)
            log(f"уборка: отменён висящий ордер {o['orderId'][:8]}")
            acted += 1
    clean = True
    for p in api.positions(SYMBOL):
        sz = float(p.get("size", 0) or 0)
        if sz > 0:
            ok = market_close(db, bl, sz, f"v1-mm: {note}", "loki-flat")
            if ok:
                db.pos_event(SYMBOL, "Sell", "close", sz, 0.0, None, note)
                bl.warn("flatten", f"закрыта позиция-хвост {sz} {SYMBOL} маркетом (sync уточнит)", note)
                log(f"уборка: закрыта позиция {sz} {SYMBOL} маркетом")
                acted += 1
                # ack есть, но подтверждения закрытия нет — до sync не считаем чистым
            else:
                clean = False
                bl.error("flatten", f"НЕ УДАЛОСЬ закрыть хвост {sz} {SYMBOL} — позиция жива (не FLAT)", note)
                log(f"ВНИМАНИЕ: не удалось закрыть хвост {sz} {SYMBOL} — скажет следующий чек/сессия")
    return acted, clean

def wait_fill(oid, deadline, db, symbol):
    """Поллим ордер до исполнения/отмены. (filled_qty, avg_price) или (None, None).

    Фаза 4: ордер демона живёт под client_order_id (= orderLinkId биржи),
    поэтому матчим и по orderId, и по orderLinkId (как v2)."""
    while time.time() < deadline:
        time.sleep(POLL_SEC)
        t = api.ticker(symbol)
        if t: db.tick(symbol, t, "bot-poll")
        oo = [o for o in api.open_orders(symbol)
              if o["orderId"] == oid or o.get("orderLinkId") == oid]
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
        if oo[0].get("orderStatus") == "Filled":
            db.update_status(oid, "Filled", float(oo[0].get("cumExecQty",0) or 0),
                             float(oo[0].get("avgPrice",0) or 0), float(oo[0].get("cumExecFee",0) or 0))
            return float(oo[0]["cumExecQty"]), float(oo[0]["avgPrice"])
    return None, None

def settle(db, bl, run_id, note_ctx):
    """Синхронизация сделок с биржи (symbol обязателен, §4.2) + лог новых."""
    try:
        new = exsync.sync_closed_trades(db.con, symbol=SYMBOL)
        exsync.sync_executions(db.con, symbol=SYMBOL)
    except Exception as e:
        bl.error("sync", f"sync с биржей не удался: {e}", "")
        return []
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
    return new

def _first_fill_epoch(db, symbol, lookback_min=2880):
    """Время первого исполнения живой позиции из executions (§4.5)."""
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

def _trading_loop(db, bl, run_id, args, t_end):
    bl.session("start", f"v1-revival: сессия {args.minutes} мин, {SYMBOL}, TP +{TP_PCT*100}% / SL -{SL_PCT*100}%",
               f"банк {ACCOUNT}={exsync.virtual_balance(db.con, ACCOUNT):.4f} | версия {STRATEGY_VERSION} | SESSION-END логика")
    log(f"СТАРТ v1-revival run={run_id}: {SYMBOL}, банк {ACCOUNT} {exsync.virtual_balance(db.con, ACCOUNT):.4f}")

    # синхронизация: что закрылось, пока сессии не было (истина — биржа)
    settle(db, bl, run_id, "стартовая синхронизация")

    # стартовая уборка хвостов на СВОЁМ символе (страховка от упавших прошлых сессий v1)
    acted, clean = flatten_leftovers(db, bl, "flatten при старте (хвост прошлой сессии)")
    if acted:
        time.sleep(3)
        settle(db, bl, run_id, "после стартового flatten")
    if not clean:
        bl.error("risk", "старт с живым хвостом, который не закрылся — новых входов нет, жду финиша", "")
        return

    # дневной лимит по счёту (день СОБЫТИЯ, §4.7)
    day_start = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    day_pnl = exsync.pnl_since_event(db.con, day_start, account=ACCOUNT)
    day_block = day_pnl <= -DAILY_LOSS_LIMIT
    if day_block:
        bl.warn("risk", f"дневной лимит {ACCOUNT} (по событиям): {day_pnl:+.4f}$ <= -{DAILY_LOSS_LIMIT}$ — входы запрещены", "")

    db.equity(f"run={run_id} start")
    trades = 0
    session_pnl = 0.0  # диагностическая оценка (истина — биржа)
    placed_cnt = filled_cnt = cancelled_cnt = 0
    api_bad = False
    reason = None

    while time.time() < t_end and trades < MAX_TRADES:
        if time.time() >= t_end:
            break
        # === фаза: нет позиции ===
        t = api.ticker(SYMBOL)
        if not t:
            if not api_bad:
                bl.warn("api_err", "тикер не отвечает (переход в ожидание)")
                api_bad = True
            log("нет тикера, ждём"); time.sleep(POLL_SEC); continue
        if api_bad:
            bl.info("api_err", "тикер снова отвечает")
            api_bad = False
        db.tick(SYMBOL, t, "bot-cycle")
        bid = float(t["bid1Price"])
        q = qty_for(bid)
        if q is None:
            log(f"цена {bid}: не подобрать qty под ${NOTIONAL_MIN}-{NOTIONAL_MAX}"); time.sleep(POLL_SEC); continue
        if day_block:
            time.sleep(POLL_SEC); continue

        # VOL-1 (t_39829ff5): гейт часа + масштаб от волы; без VOL1_ENABLED=1 нейтральны
        vf_ok, vf_scale, vf_why = vf.entry_check(SYMBOL, None, lambda: api.kline_closes(SYMBOL))
        if not vf_ok:
            log(vf_why + " — вход пропущен, ждём")
            time.sleep(POLL_SEC); continue
        if vf_scale < 1.0:
            q = qty_for_scaled(bid, vf_scale)
            if q is None:
                log(f"VOL1: масштаб {vf_scale} вывел notional из ${NOTIONAL_MIN}-{NOTIONAL_MAX} — вход пропущен")
                time.sleep(POLL_SEC); continue

        reason_s = f"{vf_why} | v1-mm: вход на биде B={bid}, спред={(float(t['ask1Price'])-bid)/bid*10000:.1f}bps"
        # risk gate: банк СВОЕГО счёта, свежий дневной лимит, lease+резерв (§4.7)
        allowed, why, rctx = pstate.risk_gate(db.con, STRATEGY_ID, SYMBOL, q * bid,
                                              DAILY_LOSS_LIMIT, run_id,
                                              ttl_sec=args.minutes * 60 + 120,
                                              reserve_notional=True)
        if not allowed:
            bl.warn("risk", why, "")
            time.sleep(POLL_SEC); continue
        # Фаза 4: вход через демона (Limit по биде, fail-closed —
        # без демона новых входов НЕТ, повтор на следующем цикле)
        entry_oid = None
        if DAEMON_SOCK:
            try:
                entry_oid = daemon_order(db, bl, side="Buy", qty=q, price=bid,
                                         purpose="OPEN_RISK", reason=reason_s,
                                         order_type="Limit", reduce_only=False)
            except DaemonDown as e:
                bl.error("risk", f"DAEMON недоступен ({e}) — вход НЕ отправлен "
                                 f"(fail-closed), снимаем резерв, ждём следующего цикла", "")
                pstate.release_reserve(db.con, STRATEGY_ID, SYMBOL)
                time.sleep(POLL_SEC); continue
            if entry_oid is None:
                pstate.release_reserve(db.con, STRATEGY_ID, SYMBOL)
                time.sleep(POLL_SEC); continue
        else:
            o = api.place_order(SYMBOL, "Buy", f"{q:g}", px_fmt(bid), "PostOnly", reason_s, "bot-v1")
            if o.get("retCode") != 0:
                pstate.release_reserve(db.con, STRATEGY_ID, SYMBOL)
                bl.warn("entry", f"вход отклонён биржей: {o.get('retCode')} {o.get('retMsg','')[:80]}", reason_s)
                log(f"вход отклонён: {o.get('retCode')} {o.get('retMsg','')[:60]}"); time.sleep(POLL_SEC); continue
            entry_oid = db.journal_order(o, SYMBOL, "Buy", q, float(px_fmt(bid)), reason_s, "bot-v1", bid)
        placed_cnt += 1
        db.run_update(run_id, orders_placed=placed_cnt)
        bl.info("entry", f"вход выставлен {q:g} {SYMBOL} @ {bid} "
                         f"{'Limit-GTC (daemon)' if DAEMON_SOCK else 'PostOnly'}, ждём {MAX_WAIT_MIN} мин", f"oid={entry_oid}")
        log(f"вход выставлен {q:g} {SYMBOL} @ {bid} (id {entry_oid[:8]}), ждём {MAX_WAIT_MIN} мин")

        fq, ap_ = wait_fill(entry_oid, time.time() + MAX_WAIT_MIN * 60, db, SYMBOL)
        if fq is None:
            # Фаза 4: отмена входа через демона; демон недоступен ->
            # прямой путь (отмена риска не создаёт, зависший ордер хуже)
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
            pstate.release_reserve(db.con, STRATEGY_ID, SYMBOL)
            cancelled_cnt += 1
            db.run_update(run_id, orders_cancelled=cancelled_cnt)
            bl.info("entry", "вход не исполнился за окно ожидания — отмена, новый круг")
            log("вход не исполнился, отмена, новый круг")
            continue

        # === фаза: в позиции ===
        entry_px = ap_
        pstate.confirm_reserve(db.con, STRATEGY_ID, SYMBOL)
        filled_cnt += 1
        bl.info("fill", f"ВХОД исполнен: {fq:g} {SYMBOL} @ {entry_px}")
        log(f"ВХОД исполнен: {fq:g} {SYMBOL} @ {entry_px}")
        db.pos_event(SYMBOL, "Buy", "open", fq, entry_px, note=reason_s)
        # неизменяемый deadline от фактического первого исполнения (§4.5)
        ff = _first_fill_epoch(db, SYMBOL) or time.time()
        hold_deadline, _ = pstate.load_or_init_deadline(db.con, STRATEGY_ID, SYMBOL, ff, MAX_HOLD_MIN)
        tp_px = round(entry_px * (1 + TP_PCT), 4)
        tp_reason = f"v1-mm: TP от входа {entry_px}"
        # TP — reduceOnly (§4.4): нет голого шорта при аварийном закрытии
        # Фаза 4: TP через демона (Limit GTC reduceOnly). Демон упал или
        # reject -> прямой TP (выход обязан стоять всегда, §4.4)
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
                "qty": f"{fq:g}", "price": px_fmt(tp_px), "timeInForce": "GTC",
                "reduceOnly": "true",
                "orderLinkId": f"v1tp-{int(time.time()*1000)}"})
            if o2.get("retCode") == 0:
                placed_cnt += 1
                tp_oid = db.journal_order(o2, SYMBOL, "Sell", fq, tp_px, tp_reason, "bot-v1", bid)
                db.run_update(run_id, orders_placed=placed_cnt)
                bl.info("tp", f"TP выставлен @ {tp_px} (GTC, reduceOnly)", f"oid={tp_oid} вход={entry_px}")
                log(f"TP выставлен @ {tp_px} (id {tp_oid[:8]})")
            else:
                bl.error("tp", f"TP ОТКЛОНЁН биржей: {o2.get('retCode')} {o2.get('retMsg','')[:80]} — позиция без TP, идём к SL/time-stop",
                         f"вход={entry_px} qty={fq}")
                log(f"ВНИМАНИЕ: TP отклонён: {o2.get('retMsg','')[:60]}")
        exit_px, exit_reason, last = None, None, 0.0

        # мониторинг: t_end НЕ в условии — SESSION-END ловится внутри и закрывает позицию
        while time.time() < hold_deadline:
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
                            db.update_status(tp_oid, "Filled", float(h.get("cumExecQty",0) or 0),
                                             float(h.get("avgPrice",0) or 0), float(h.get("cumExecFee",0) or 0))
                            exit_px, exit_reason = float(h["avgPrice"]), "TP"
                            break
                    if exit_px: break
            # SL по цене маркировки (lastPrice как прокси)
            if last and last < entry_px * (1 - SL_PCT):
                exit_reason = "SL"
                break
            # конец сессии при живой позиции — v1 НЕ оставляет хвост (идентичность v1)
            if time.time() >= t_end:
                exit_reason = "SESSION-END"
                break

        # выход
        if exit_reason == "TP":
            bl.info("exit", f"TP исполнен @ {exit_px} — итог придёт с биржи (sync)", f"вход={entry_px}")
            log(f"TP исполнен @ {exit_px}")
            session_pnl += (exit_px - entry_px) * fq
            pstate.clear_deadline(db.con, STRATEGY_ID, SYMBOL)
        else:  # SL / SESSION-END / TIME (hold_deadline истёк без причины)
            r = exit_reason if exit_reason else "TIME"
            bl.warn("exit", f"сработал {r}: закрываю позицию маркетом",
                    f"вход={entry_px} last={last} hold оставалось {max(0,(hold_deadline-time.time())/60):.0f}м")
            if tp_oid:
                # Фаза 4: отмена TP через демона (fallback — прямой путь;
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
            # Фаза 4: закрытие через демона (CLOSE Market reduceOnly);
            # демон упал/reject -> прямой маркет-выход (позицию нельзя бросать)
            closed_ok = False
            if DAEMON_SOCK:
                try:
                    closed_ok = daemon_order(db, bl, side="Sell", qty=fq, price=None,
                                             purpose="CLOSE", reason=f"v1-mm: {r} выход маркетом",
                                             order_type="Market", reduce_only=True) is not None
                except DaemonDown as e:
                    bl.warn("risk", f"DAEMON недоступен ({e}) — маркет-выход прямым путём (fallback)", "")
            if not closed_ok:
                ok = market_close(db, bl, fq, f"v1-mm: {r} выход маркетом", "loki-exit")
            else:
                ok = True
            if ok:
                filled_cnt += 1
                exit_reason = r
                session_pnl += ((last or entry_px) - entry_px) * fq  # диагностическая оценка
                pstate.clear_deadline(db.con, STRATEGY_ID, SYMBOL)
            else:
                # позиция жива, маркет не прошёл — стоп сессии (не считаем FLAT, §4.4);
                # финальный flatten ниже попытается ещё раз
                db.run_update(run_id, orders_cancelled=cancelled_cnt)
                db.pos_event(SYMBOL, "Buy", "reduce", 0, entry_px, None,
                             f"EXIT_PENDING: {r} — закрытие не прошло, позиция жива, ждём финального flatten run={run_id}")
                bl.error("exit", "стоп сессии после неудачного маркет-выхода (позиция жива, будет финальный flatten)")
                log("стоп сессии после неудачного маркет-выхода")
                break

        if exit_reason:
            db.pos_event(SYMBOL, "Sell", "close", fq, exit_px or 0.0, None,
                         f"{exit_reason} run={run_id} (net — биржа, см. closed_trades)")
            db.run_update(run_id, orders_filled=filled_cnt)
        trades += 1
        # истина о сделке — биржа
        time.sleep(3)
        settle(db, bl, run_id, f"после {exit_reason}")
        # circuit breaker: по биржевому дню, счёт v1-revival
        day_pnl = exsync.pnl_since_event(db.con, day_start, account=ACCOUNT)
        if day_pnl <= -DAILY_LOSS_LIMIT:
            bl.warn("interrupt", f"CIRCUIT BREAKER v1: дневной убыток {ACCOUNT} {day_pnl:+.4f}$ >= {DAILY_LOSS_LIMIT}$ — стоп", "")
            log(f"CIRCUIT BREAKER: дневной убыток {day_pnl:+.4f}$ — стоп")
            break
        if session_pnl <= -SESSION_LOSS_LIMIT:
            bl.warn("interrupt", f"CIRCUIT BREAKER v1: убыток сессии (оценка) {session_pnl:.3f}$ >= {SESSION_LOSS_LIMIT}$ — стоп", "")
            log(f"CIRCUIT BREAKER: убыток сессии {session_pnl:.3f}$ — стоп")
            break

    # === финальная уборка: гарантия «позиций/ордеров на символе не осталось» ===
    acted2, clean2 = flatten_leftovers(db, bl, f"flatten при финише run={run_id}")
    if acted2:
        time.sleep(3)
        settle(db, bl, run_id, "после финального flatten")

    db.run_update(run_id, ended_at=now())
    db.equity(f"run={run_id} end")
    bal = exsync.virtual_balance(db.con, ACCOUNT)
    bl.session("finish", f"ФИНИШ v1-revival: банк {ACCOUNT} {bal:.5f} USDT, ордеров {placed_cnt}/{filled_cnt}/{cancelled_cnt}",
               f"сделок в окне {trades}; символ чист: {'да' if clean2 else 'НЕТ — проверить вручную'}; {STRATEGY_VERSION}")
    log(f"ФИНИШ v1-revival run={run_id}: банк {ACCOUNT} {bal:.5f}, символ чист: {'да' if clean2 else 'НЕТ'}")

def main():
    global SYMBOL
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=30)
    ap.add_argument("--symbol", default=SYMBOL)
    ap.add_argument("--flatten", action="store_true", help="аварийная уборка: закрыть всё на символе")
    args = ap.parse_args()
    SYMBOL = args.symbol
    t_end = time.time() + args.minutes * 60
    run_id = str(uuid.uuid4())[:8]
    bl = botlog.BotLog(run_id)
    db = Db()

    if args.flatten:
        bl.warn("flatten", f"ручной --flatten: закрываю всё на {SYMBOL}", "")
        acted, _ = flatten_leftovers(db, bl, "v1-manual-flatten")
        if acted:
            time.sleep(3)
        settle(db, bl, run_id, "manual-flatten")
        return

    db.con.execute("""INSERT INTO bot_runs (run_id, strategy, script, started_at, orders_placed,
        orders_filled, orders_cancelled, pnl_realised, notes)
        VALUES (?,?,?,?,0,0,0,NULL,?)""",
        (run_id, "bot-v1-lazy-mm", "bot_v1.py", now(),
         f"v1-REVIVAL {STRATEGY_VERSION} {SYMBOL} TP={TP_PCT} SL={SL_PCT} wait={MAX_WAIT_MIN}m "
         f"hold={MAX_HOLD_MIN}m max={MAX_TRADES} банк={ACCOUNT} SESSION-END-логика"))
    db.con.commit()

    # lease: второй экземпляр bot-v1 на том же символе не стартует (§4.7)
    if not pstate.acquire_lease(db.con, STRATEGY_ID, SYMBOL, run_id, ttl_sec=args.minutes * 60 + 120):
        bl.error("risk", f"LEASE: {STRATEGY_ID}/{SYMBOL} уже ведётся другой сессией — выходим", "")
        log("LEASE занят — выход")
        db.run_update(run_id, ended_at=now())  # фикс 15.10: lease-выход тоже закрывает строку bot_runs
        return
    try:
        _trading_loop(db, bl, run_id, args, t_end)
    finally:
        pstate.release_lease(db.con, STRATEGY_ID, SYMBOL, run_id)

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print(f"[{now()}] прервано вручную (Ctrl-C)", flush=True)
        try:
            botlog.BotLog().crash("прервано вручную (KeyboardInterrupt) — ПОЗИЦИЯ МОЖЕТ БЫТЬ ЖИВА, запусти --flatten")
        except Exception:
            pass
        raise
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[{now()}] CRASH: {e}\n{tb}", file=sys.stderr, flush=True)
        try:
            botlog.BotLog().crash(f"v1-revival сессия упала: {type(e).__name__}: {e} — позиция может быть жива", tb)
        except Exception:
            pass
        sys.exit(1)
