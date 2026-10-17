#!/usr/bin/env python3
"""Бот v3 «Donchian» — тренд-следование (бэктест №2, 12-бэктест-2-тренд.md, этап 1.7).

Наследует от v2 (10-ТЗ-v2-гибрид-внедрение.md):
  - истина о сделках — биржа (exsync: closed-pnl -> closed_trades, executions)
  - виртуальный банк в virtual_ledger (общий банк проекта; сделки размечаются по эпохе)
  - позиция переживает сессию: ADOPT на старте, HANDOVER в конце окна
  - дневной лимит убытка
Стратегия (T2 из бэктеста №2, проверена на 156k свечей XRP, 1.5 года):
  ВХОД:  close бара > максимума high за предыдущие ENTRY_BARS (24 ч = 288×5м)
  ВЫХОД: close бара < минимума low за предыдущие EXIT_BARS (12 ч = 144×5м)
  Номинал ~$5.2, вход/выход taker (сигнальные, как в бэктесте).
  Сигнал — только по ЗАКРЫТОМУ бару; свечи берутся с биржи /v5/market/kline.

Ключевое отличие от v2: бот НЕ молотит каждые 30 сек — цикл раз в CYCLE_SEC (5 мин),
синхронизация свечей инкрементальная. Ожидаемая частота сделок: 1 в 2–3 дня.

ПАКЕТ А (аудит №14, 17.09) — v3.1:
  Фикс 1 (§4.1): сигнал считается модулем donchian.py — единый контракт данных
    (сортировка по ts по возрастанию, уникальные ts, только закрытые бары,
    непрерывность сетки) для live/replay/тестов. Логи: bar_open_ts/bar_close_ts/
    decision_ts в UTC.
  Фикс 3 (§4.3): сделки v3 меняют общий банк (exsync.LEDGER_EPOCHS).
  Фикс 4 (§4.4): машина состояний FLAT/ENTRY_PENDING/OPEN/EXIT_PENDING/UNKNOWN;
    ack выхода ≠ закрытие; UNKNOWN/EXIT_PENDING запрещают вход.
  Фикс 5 (§4.5): для входов по v3 hold-deadline не нужен (выход только по сигналу),
    но позиция регистрируется в position_hold с временем ПЕРВОГО исполнения.
  Фикс 6 (§4.7): перед входом risk_gate — свежий банк, дневной лимит по дню
    события (pnl_since_event), lease + атомарный резерв капитала.

Запуск: python3 bot_v3.py [--minutes 50] [--flatten] [--symbol XRPUSDT]
"""
import argparse, math, os, sqlite3, sys, time, traceback, uuid, datetime, json
import urllib.request, urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bybit_api as api
import botlog
import exsync
import donchian
import position_state as pstate

SYMBOL = "XRPUSDT"
ENTRY_BARS = 288   # 24 ч по 5-м барам
EXIT_BARS = 144    # 12 ч
NOTIONAL = 5.2
POLL_SEC = 300     # цикл раз в 5 минут (равен бару)
KLINES_NEED = ENTRY_BARS + 10
DAILY_LOSS_LIMIT = 0.20
EPOCH = "v3-donchian"
STRATEGY_VERSION = "pkgA-v3.1"
STRATEGY_ID = "bot-v3"
ACCOUNT = "v3-donchian"  # виртуальный счёт бота (разделение банков 17.09 вечер)

# --- Risk Engine v5 (карточка ХОД-2, 19.09): ордера через Gateway --------
# RISK_ENFORCE_V3=1: вход/выход идут как trade-intent через Gateway
# (risk.db, политика policy-enforce-v1, ENFORCE_DEMO). Без флага — прямой
# путь как раньше (откат = снятие флага в обёртке/окружении).
RISK_ENFORCE_V3 = os.environ.get("RISK_ENFORCE_V3", "") == "1"
RISK_GATEWAY = None

# --- Фаза 2b (cutover 26.09): владелец исполнения — центральный демон -----
# TRADE_CORE_DAEMON_SOCK: intent идёт на Unix-сокет демона (hello ->
# fencing token -> intent). Приоритет пути: демон > in-process Gateway >
# прямой API (старый путь до ХОД-2). Демон недоступен -> DaemonUnavailable
# -> лог + позиции не трогаем (fail-closed, повтор на следующем баре).
DAEMON_SOCK = os.environ.get("TRADE_CORE_DAEMON_SOCK", "")


def risk_gateway():
    """Ленивая инициализация Gateway (один процесс — один коннект)."""
    global RISK_GATEWAY
    if RISK_GATEWAY is not None:
        return RISK_GATEWAY
    import json as _json
    import sqlite3 as _sq
    RE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "risk_engine")
    sys.path.insert(0, RE)
    sys.path.insert(0, os.path.join(RE, "policy"))
    sys.path.insert(0, os.path.join(RE, "gateway"))
    from gateway.core import Gateway
    from transport_bybit_demo import BybitDemoTransport
    from live_collector import build_snapshot
    policy = _json.load(open(os.path.join(RE, "config", "policy-enforce-v1.json"),
                             encoding="utf-8"))
    tdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "trading.db")
    rdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "risk.db")
    tcon = _sq.connect(f"file:{tdb}?mode=ro", uri=True, timeout=10)

    def _now_iso():
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _snap(intent):
        syms = {l["symbol"] for l in intent.get("legs", [])} | {SYMBOL}
        return build_snapshot(tcon, symbols=sorted(syms))

    RISK_GATEWAY = Gateway(rdb, BybitDemoTransport(), policy_config=policy,
                           snapshot_provider=_snap,
                           service_identity="bot-v3",
                           environment="BYBIT_DEMO",
                           account_id="local-account",
                           clock=_now_iso)
    return RISK_GATEWAY


def daemon_emit(db, bl, qty, reason, purpose, side, reduce_only):
    """Ордер через центральный демон (Фаза 2b): hello -> токен -> intent."""
    # ВАЖНО: корень репо, а не сам каталог trade_core — иначе пакетный
    # импорт 'trade_core.live' не разрешается (латентный дефект cutover-ночи,
    # пойман разведкой Фазы 4-live: импорт падал бы на первом же сигнале)
    sys.path.insert(0, os.path.join(
        os.path.dirname(os.path.abspath(__file__)), ".."))
    from trade_core.live import client as tcc

    # живая mid-цена (журнал в trading.db + оценка notional для лога)
    try:
        q = urllib.parse.urlencode({"category": "linear", "symbol": SYMBOL,
                                    "limit": "1"})
        with urllib.request.urlopen(
                "https://api-demo.bybit.com/v5/market/orderbook?" + q,
                timeout=10) as r:
            d = json.loads(r.read())
        b_, a_ = d["result"]["b"][0][0], d["result"]["a"][0][0]
        px = str((float(b_) + float(a_)) / 2)
    except Exception:
        bl.error("risk", "DAEMON: нет живой цены (orderbook) — отмена", "")
        return None
    hello = tcc.hello(DAEMON_SOCK, "bot-v3", "BYBIT_DEMO", "local-account")
    if not hello.get("ok"):
        bl.error("risk", f"DAEMON hello отклонён: {hello.get('error')} "
                         f"{hello.get('detail', '')}", "")
        return None
    token = hello["fencing_token"]
    now_iso = donchian.utc_now_iso()
    intent_id = f"bot-v3:v3d-{purpose[:3]}-{int(time.time()*1000)}"
    signal_time = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                time.gmtime(time.time() - 1))
    intent = {
        "schema_version": "strategy-intent.v2",
        "intent_id": intent_id,
        "idempotency_key": intent_id,
        "bot_id": "bot-v3",
        "strategy_id": "donchian",
        "strategy_version": STRATEGY_VERSION,
        "environment": "BYBIT_DEMO",
        "account_id": "local-account",
        "purpose": purpose,
        "legs": [{
            "category": "linear", "symbol": SYMBOL, "side": side,
            "order_type": "Market", "requested_qty": f"{qty}",
            "reduce_only": reduce_only, "limit_price": None,
            "leg_index": 0, "position_idx": 0,
        }],
        "signal_time": signal_time,
        "created_at": now_iso,
        "valid_until": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                     time.gmtime(time.time() + 60)),
        "decision_horizon_ms": 30000,
        "expected_hold_ms": 0,
        "market_snapshot_ref": "mds-live-collector",
        "epoch_id": "epoch-19c58ada4fbc44af",
    }
    try:
        r = tcc.intent(DAEMON_SOCK, intent, token)
    except tcc.DaemonUnavailable as e:
        bl.error("risk", f"DAEMON недоступен ({e}) — ордер НЕ отправлен, "
                         f"попытка не потреблена", "")
        return None
    gws = r.get("gateway_status")
    if gws == "ACCEPTED":
        coid = r.get("client_order_id")
        oid = coid
        db.con.execute(
            """INSERT OR IGNORE INTO orders (order_id, symbol, category, side, order_type,
            time_in_force, qty, price, notional_usd, status, orderLinkId, reason, strategy,
            market_price_at_place, deviation_pct, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (oid, SYMBOL, "linear", side, "Market", "IOC", float(qty), float(px),
             float(qty) * float(px), "New", coid, reason, "bot-v3", float(px), None,
             now(), now()))
        db.con.commit()
        bl.info("risk", f"DAEMON {purpose} {qty} {SYMBOL} accepted "
                        f"(order_state={r.get('order_state')}, "
                        f"decision={r.get('decision_id')})",
                f"client_order_id={coid}")
        return oid
    if gws == "DUPLICATE_RETURNED":
        bl.warn("risk", f"DAEMON duplicate: {r.get('reason_codes')}", "")
        return r.get("client_order_id")
    # REJECTED / SUBMIT_UNKNOWN / RECONCILE_FIRST
    bl.error("risk", f"DAEMON {purpose} отклонён: {gws} "
                     f"{r.get('reject_code', '')} {r.get('reason_codes')}",
             reason)
    return None


def risk_emit(db, bl, qty, reason, purpose, side, reduce_only):
    """Ордер через Gateway: intent -> decision -> reservation -> submit."""
    if DAEMON_SOCK:
        return daemon_emit(db, bl, qty, reason, purpose, side, reduce_only)
    gw = risk_gateway()
    # живая mid-цена orderbook (для Market-intent — оценка notional)
    try:
        q = urllib.parse.urlencode({"category": "linear", "symbol": SYMBOL,
                                    "limit": "1"})
        with urllib.request.urlopen(
                "https://api-demo.bybit.com/v5/market/orderbook?" + q,
                timeout=10) as r:
            d = json.loads(r.read())
        b_, a_ = d["result"]["b"][0][0], d["result"]["a"][0][0]
        px = str((float(b_) + float(a_)) / 2)
    except Exception:
        bl.error("risk", "RISK_EMIT: нет живой цены (orderbook) — отмена", "")
        return None
    intent_id = f"bot-v3:v3gw-{purpose[:3]}-{int(time.time()*1000)}"
    leg = {"category": "linear", "symbol": SYMBOL, "side": side,
           "order_type": "Market", "requested_qty": f"{qty}",
           "reduce_only": reduce_only}
    intent = {
        "schema_version": "trade-intent.v1",
        "intent_id": intent_id, "idempotency_key": intent_id,
        "bot_id": "bot-v3", "strategy_id": "donchian",
        "strategy_version": STRATEGY_VERSION, "environment": "BYBIT_DEMO",
        "account_id": "local-account", "purpose": purpose,
        "created_at": donchian.utc_now_iso(),
        "valid_until": donchian.utc_now_iso(),
        "legs": [leg], "market_snapshot_ref": "mds-live-collector",
        "reason": reason,
    }
    if purpose in ("CLOSE", "REDUCE_RISK"):
        intent["target_position_key"] = SYMBOL
        intent["expected_position_version"] = 1
        intent["max_reduce_qty"] = f"{qty}"
    r = gw.ingest(intent)
    gws = r.get("gateway_status")
    if gws == "ACCEPTED":
        coid = r.get("client_order_id")
        # журналируем в trading.db (orders) с orderLinkId=coid
        oid = coid  # orderLinkId — наш клиентский id
        db.con.execute(
            """INSERT OR IGNORE INTO orders (order_id, symbol, category, side, order_type,
            time_in_force, qty, price, notional_usd, status, orderLinkId, reason, strategy,
            market_price_at_place, deviation_pct, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (oid, SYMBOL, "linear", side, "Market", "IOC", float(qty), float(px),
             float(qty) * float(px), "New", coid, reason, "bot-v3", float(px), None,
             now(), now()))
        db.con.commit()
        bl.info("risk", f"GATEWAY {purpose} {qty} {SYMBOL} accepted "
                        f"(order_state={r.get('order_state')}, decision={r.get('decision_id')})",
                f"client_order_id={coid}")
        return oid
    if gws == "DUPLICATE_RETURNED":
        bl.warn("risk", f"GATEWAY duplicate: {r.get('reason_codes')}", "")
        return r.get("client_order_id")
    # REJECTED / SUBMIT_UNKNOWN / RECONCILE_FIRST
    lvl = bl.error if purpose in ("OPEN_RISK",) else bl.error
    lvl("risk", f"GATEWAY {purpose} отклонён: {gws} {r.get('reason_codes')}", reason)
    return None


def now():
    return donchian.utc_now_iso()


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
            (o["result"]["orderId"], symbol, "linear", side, o.get("type", "Market"),
             o.get("timeInForce", ""), float(qty), float(price), float(qty) * float(price),
             "New", orderLinkId or o["result"].get("orderLinkId", ""), reason, strategy, mkt,
             None, now(), now()))
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
    def pos_event(self, symbol, side, event, qty, price, pnl=None, note=""):
        self.con.execute("""INSERT INTO position_events (symbol, side, event, qty, avg_price,
            position_value, realised_pnl, position_status, ts, note) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (symbol, side, event, qty, price, qty * price, pnl, event, now(), note))
        self.con.commit()
    def run_update(self, run_id, **kw):
        sets = ", ".join(f"{k}=?" for k in kw)
        self.con.execute(f"UPDATE bot_runs SET {sets} WHERE run_id=?", (*kw.values(), run_id))
        self.con.commit()


def market_entry(db, bl, qty, reason):
    if RISK_ENFORCE_V3:
        return risk_emit(db, bl, qty, reason, "OPEN_RISK", "Buy", False)
    m = api.post("/v5/order/create", {
        "category": "linear", "symbol": SYMBOL, "side": "Buy", "orderType": "Market",
        "qty": f"{qty}", "orderLinkId": f"v3in-{int(time.time()*1000)}"})
    if m.get("retCode") == 0:
        oid = db.journal_order(m, SYMBOL, "Buy", qty, 0.0, reason, "bot-v3", 0.0)
        bl.info("entry", f"РЫНОЧНЫЙ ВХОД {qty} {SYMBOL} отправлен (ack ≠ исполнение; подтверждение — sync)",
                f"oid={oid}")
        return oid
    bl.error("entry", f"вход отклонён: {m.get('retCode')} {m.get('retMsg','')[:80]}", reason)
    return None


def market_exit(db, bl, qty, reason):
    if RISK_ENFORCE_V3:
        return risk_emit(db, bl, qty, reason, "CLOSE", "Sell", True)
    m = api.post("/v5/order/create", {
        "category": "linear", "symbol": SYMBOL, "side": "Sell", "orderType": "Market",
        "qty": f"{qty}", "reduceOnly": "true", "timeInForce": "IOC",
        "orderLinkId": f"v3out-{int(time.time()*1000)}"})
    if m.get("retCode") == 0:
        oid = db.journal_order(m, SYMBOL, "Sell", qty, 0.0, reason, "bot-v3", 0.0)
        bl.warn("exit", f"РЫНОЧНЫЙ ВЫХОД {qty} {SYMBOL} ({reason}) отправлен; подтверждение — sync",
                f"oid={oid}")
        return oid
    bl.error("exit", f"выход отклонён: {m.get('retCode')} {m.get('retMsg','')[:80]}", reason)
    return None


def first_fill_epoch(db, symbol, lookback_min=2880):
    """Время первого исполнения живой позиции (§4.5: не «сейчас» при ADOPT).

    Смотрит executions по symbol за lookback: первый Buy-fill последнего
    входного цикла. None, если данных нет.
    """
    since = donchian.utc_from_ms(int(time.time() * 1000) - lookback_min * 60000).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        row = db.con.execute(
            "SELECT exec_time, exec_qty FROM executions WHERE symbol=? AND side='Buy' AND exec_time>=? "
            "ORDER BY exec_time DESC LIMIT 1", (symbol, since)).fetchone()
        if row:
            t = datetime.datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
            return t.timestamp()
    except Exception:
        pass
    return None


def adopt_position(db, bl):
    """Подхват живой long-позиции: состояние OPEN (остаток>0) + первый fill из executions."""
    for p in api.positions(SYMBOL):
        sz = float(p.get("size", 0) or 0)
        if sz > 0 and p.get("side", "").lower() == "buy":
            entry_px = float(p.get("avgPrice", 0) or 0)
            ff = first_fill_epoch(db, SYMBOL)
            db.pos_event(SYMBOL, "Buy", "reduce", 0, entry_px, None,
                         f"ADOPT v3: подхват позиции (маркер; qty см. биржу); first_fill из executions: "
                         f"{'есть' if ff else 'нет — deadline по остатку позиции'}")
            bl.warn("adopt", f"подхвачена позиция {sz} {SYMBOL} @ {entry_px} — веду по правилу выхода Donchian",
                    f"эпоха {EPOCH}, first_fill={'%.0f' % ff if ff else 'unknown'}")
            return sz, entry_px, ff
    return None, None, None


def settle(db, bl, run_id, ctx):
    try:
        new = exsync.sync_closed_trades(db.con, symbol=SYMBOL)
        exsync.sync_executions(db.con, symbol=SYMBOL)
    except Exception as e:
        bl.error("sync", f"sync не удался: {e}", "")
        return []
    for t in new:
        bl.info("exit", f"СДЕЛКА ЗАКРЫТА (биржа): net {float(t['closedPnl']):+.5f}$ "
                        f"qty={t['qty']} вх~{t['avgEntryPrice']} вых~{t['avgExitPrice']}",
                f"orderId={t['orderId'][:8]} {ctx}")
    return new


def bank_balance(db):
    return exsync.virtual_balance(db.con, ACCOUNT)


def main():
    global SYMBOL
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=50)
    ap.add_argument("--flatten", action="store_true")
    ap.add_argument("--symbol", default=SYMBOL)
    args = ap.parse_args()
    SYMBOL = args.symbol
    t_end = time.time() + args.minutes * 60
    run_id = str(uuid.uuid4())[:8]
    bl = botlog.BotLog(run_id)
    db = Db()

    if args.flatten:
        bl.warn("flatten", "ручной --flatten: закрываю всё по " + SYMBOL, "")
        for o in api.open_orders(SYMBOL):
            if api.cancel_order(SYMBOL, o["orderId"]).get("retCode") == 0:
                db.update_status(o["orderId"], "Cancelled")
        for p in api.positions(SYMBOL):
            sz = float(p.get("size", 0) or 0)
            if sz > 0:
                market_exit(db, bl, f"{sz:g}", "v3-manual-flatten")
        settle(db, bl, run_id, "manual-flatten")
        return

    db.con.execute("""INSERT INTO bot_runs (run_id, strategy, script, started_at, orders_placed,
        orders_filled, orders_cancelled, pnl_realised, notes)
        VALUES (?,?,?,?,0,0,0,NULL,?)""",
        (run_id, "bot-v3-donchian", "bot_v3.py", now(),
         f"v3-DONCHIAN {STRATEGY_VERSION} entry={ENTRY_BARS}б(24ч) exit={EXIT_BARS}б(12ч) "
         f"${NOTIONAL} {SYMBOL} эпоха={EPOCH}"))

    db.con.commit()

    # lease: второй экземпляр bot-v3 на том же символе не стартует (§4.7)
    if not pstate.acquire_lease(db.con, STRATEGY_ID, SYMBOL, run_id, ttl_sec=args.minutes * 60 + 120):
        bl.error("risk", f"LEASE: {STRATEGY_ID}/{SYMBOL} уже ведётся другой сессией — выходим", "")
        log("LEASE занят — выход")
        db.run_update(run_id, ended_at=now())  # фикс 15.10: lease-выход тоже закрывает строку bot_runs
        return
    try:
        _main_loop(db, bl, run_id, args, t_end)
    finally:
        pstate.release_lease(db.con, STRATEGY_ID, SYMBOL, run_id)
        db.run_update(run_id, ended_at=now())


def _main_loop(db, bl, run_id, args, t_end):
    # 1) синхронизация: что закрылось, пока сессия спала (истина — биржа)
    settle(db, bl, run_id, "стартовая синхронизация v3")

    # 2) подхват живой позиции (если передана) + состояние
    pos_qty, pos_entry, first_fill = adopt_position(db, bl)

    # FIX-5 маркер: регистрируем позицию в position_hold (для v3 deadline
    # информационный — выход только по сигналу Donchian; заголовок таблицы
    # пишет время первого исполнения, не «сейчас»)
    if pos_qty:
        pstate.load_or_init_deadline(db.con, STRATEGY_ID, SYMBOL, first_fill, MAX_HOLD_INFO_MIN)

    # 3) дневной лимит убытка — по дню СОБЫТИЯ (§4.7), свежие данные
    day_start = pstate.utc_day_start_iso()
    day_pnl = exsync.pnl_since_event(db.con, day_start, account=ACCOUNT)
    day_block = day_pnl <= -DAILY_LOSS_LIMIT
    if day_block:
        bl.warn("risk", f"дневной лимит (по событиям): {day_pnl:+.4f}$ <= -{DAILY_LOSS_LIMIT}$ — "
                        f"входы запрещены до конца дня", "")

    bl.session("start", f"v3-Donchian {SYMBOL} [{STRATEGY_VERSION}]: окно {args.minutes} мин, цикл {POLL_SEC}с, "
                        f"вход>max(24ч) выход<min(12ч), ${NOTIONAL}",
               f"банк {bank_balance(db):.4f}$ | позиция: {'есть ' + str(pos_qty) if pos_qty else 'нет'} | "
               f"day_pnl(события) {day_pnl:+.4f}$")
    log(f"СТАРТ v3 run={run_id} {SYMBOL}: банк {bank_balance(db):.4f}$, позиция {'есть' if pos_qty else 'нет'}")

    api_bad = False
    sig_state = ""
    entry_pending_oid = None   # oid входа, ack есть, исполнение не подтверждено
    exit_pending = False       # выход отправлен, подтверждения нет
    while time.time() < t_end:
        time.sleep(min(POLL_SEC, max(5, t_end - time.time())))
        try:
            bars = donchian.fetch_closed_bars(SYMBOL, KLINES_NEED)
            if len(bars) < ENTRY_BARS + 2:
                bl.warn("data", f"свечей мало: {len(bars)}")
                continue
            entry_sig, exit_sig, ctx = donchian.donchian_signal(bars, ENTRY_BARS, EXIT_BARS)
        except ValueError as e:
            # контракт данных нарушен (разрыв сетки и т.п.) — сигнал не считается
            bl.warn("data", f"контракт свечей нарушен: {e}", "")
            continue
        except Exception as e:
            if not api_bad:
                bl.warn("api_err", f"klines недоступны: {type(e).__name__}: {e}")
                api_bad = True
            continue
        if api_bad:
            bl.info("api_err", "klines снова доступны")
            api_bad = False

        state = pstate.assess_state(
            "Buy",
            filled_entry_qty=(pos_qty if pos_qty else (1.0 if entry_pending_oid else 0.0)),
            remaining_position=None if (entry_pending_oid and not pos_qty) else (pos_qty or 0.0),
            exit_ack_pending=exit_pending,
        )
        line = f"{'POS' if pos_qty else ('PEND' if entry_pending_oid else 'FLAT')} c={ctx['close']} " \
               f"hi24={ctx['hi']} lo12={ctx['lo']}"
        if line != sig_state:
            bl.info("signal", line,
                    f"бар {ctx['bar_open_ts']}..{ctx['bar_close_ts']} UTC, решение {ctx['decision_ts']}")
            sig_state = line
        else:
            log(line)

        # === reconciliation ENTRY_PENDING: исполнение входа? ===
        if entry_pending_oid and not pos_qty:
            st, fq = _order_state(db, entry_pending_oid)
            if st == "Filled":
                pos_qty, pos_entry = fq, ctx["close"]
                entry_pending_oid = None
                pstate.confirm_reserve(db.con, STRATEGY_ID, SYMBOL)
                bl.info("fill", f"ВХОД подтверждён executions/order: {fq} {SYMBOL}", f"state->OPEN")
            elif st in ("Cancelled", "Rejected"):
                entry_pending_oid = None
                pstate.release_reserve(db.con, STRATEGY_ID, SYMBOL)
                bl.warn("entry", f"вход {st} — резерв снят, снова FLAT", "")
            elif state == pstate.UNKNOWN:
                bl.warn("risk", "state=UNKNOWN после входа — входы запрещены до reconciliation", "")
            continue

        # === выход ===
        if pos_qty and exit_sig and not exit_pending:
            if market_exit(db, bl, f"{pos_qty:g}", f"v3-donchian: close<{ctx['lo']} (выход из тренда)"):
                exit_pending = True  # ack ≠ исполнение: ждём подтверждения sync (§4.4)
                time.sleep(3)
                settle(db, bl, run_id, "после сигнала выхода")
                sz = _live_size()
                if sz <= 0:
                    pos_qty, pos_entry, exit_pending = None, None, False
                    pstate.clear_deadline(db.con, STRATEGY_ID, SYMBOL)
                    bl.info("exit", "выход подтверждён (остаток 0) — FLAT", "")
                    continue
                bl.warn("exit", f"выход отправлен, но остаток {sz} ещё жив — EXIT_PENDING", "")
                continue
            # отказ биржи: позиция жива, остаёмся OPEN, ждём следующего цикла
            bl.error("exit", "выход отклонён — позиция жива, повтор на следующем баре", "")
            continue

        # === вход ===
        if not pos_qty and not entry_pending_oid and entry_sig and not day_block:
            last = ctx["close"]
            qty = exsync_round_qty(last)
            if qty:
                allowed, why, rctx = pstate.risk_gate(db.con, STRATEGY_ID, SYMBOL, qty * last,
                                                      DAILY_LOSS_LIMIT, run_id,
                                                      ttl_sec=args.minutes * 60 + 120)
                if not allowed:
                    bl.warn("risk", why, "")
                    continue
                bl.info("risk", why, "")
                oid = market_entry(db, bl, f"{qty:g}", f"v3-donchian: close>{ctx['hi']} (пробой 24ч)")
                if oid:
                    entry_pending_oid = oid  # ENTRY_PENDING: подтверждение — только sync/executions
                    time.sleep(3)
                    settle(db, bl, run_id, "после входа")
                    sz = _live_size()
                    if sz > 0:
                        pos_qty, entry_pending_oid = sz, None
                        pstate.confirm_reserve(db.con, STRATEGY_ID, SYMBOL)
                        ff = first_fill_epoch(db, SYMBOL)
                        pstate.load_or_init_deadline(db.con, STRATEGY_ID, SYMBOL, ff, MAX_HOLD_INFO_MIN)
                else:
                    pstate.release_reserve(db.con, STRATEGY_ID, SYMBOL)
            continue

        # === reconciliation EXIT_PENDING: остаток ещё жив? ===
        if exit_pending:
            sz = _live_size()
            if sz <= 0:
                pos_qty, pos_entry, exit_pending = None, None, False
                pstate.clear_deadline(db.con, STRATEGY_ID, SYMBOL)
                bl.info("exit", "выход подтверждён (остаток 0) — FLAT", "")
            else:
                bl.warn("exit", f"EXIT_PENDING: остаток {sz} жив, повтор выхода", "")
                if market_exit(db, bl, f"{sz:g}", "v3-donchian: повтор выхода (EXIT_PENDING)"):
                    time.sleep(3)
                    settle(db, bl, run_id, "после повтора выхода")

    # HANDOVER: позиция жива — передаём (TP нет, выход только по сигналу следующей сессии)
    if pos_qty:
        db.pos_event(SYMBOL, "Buy", "reduce", 0, pos_entry or 0.0, None,
                     f"HANDOVER v3: позиция {pos_qty} передана следующей сессии run={run_id}")
        bl.warn("handover", f"конец окна, позиция {pos_qty} {SYMBOL} жива — ПЕРЕДАЮ (выход по Donchian в след. сессии)", "")
        log(f"HANDOVER: позиция {pos_qty} жива")
    elif exit_pending:
        bl.warn("handover", "конец окна при EXIT_PENDING — следующая сессия начнёт с reconciliation", "")
    elif entry_pending_oid:
        bl.warn("handover", "конец окна при ENTRY_PENDING — следующая сессия начнёт с reconciliation", "")
    db.run_update(run_id, ended_at=now())
    bl.session("finish", f"ФИНИШ v3: банк {bank_balance(db):.4f}$",
               f"позиция {'ПЕРЕДАНА' if pos_qty else 'закрыта/отсутствует'}; {STRATEGY_VERSION}")


MAX_HOLD_INFO_MIN = 5760  # информационный маркер возраста позиции v3 (4 суток), выход — только сигнал


def _order_state(db, oid):
    """Статус и filled_qty ордера: сначала локально, потом у биржи."""
    row = db.con.execute("SELECT status, filled_qty FROM orders WHERE order_id=?", (oid,)).fetchone()
    if row and row[0] in ("Filled", "Cancelled", "Rejected"):
        return row[0], row[1]
    for h in api.order_history(SYMBOL, limit=10):
        if h["orderId"] == oid:
            st = h["orderStatus"]
            fq = float(h.get("cumExecQty", 0) or 0)
            db.update_status(oid, st, fq, float(h.get("avgPrice", 0) or 0), float(h.get("cumExecFee", 0) or 0))
            return st, fq
    return "New", 0.0


def _live_size():
    for p in api.positions(SYMBOL):
        sz = float(p.get("size", 0) or 0)
        if sz > 0 and p.get("side", "").lower() == "buy":
            return sz
    return 0.0


def exsync_round_qty(price):
    """qty под номинал $5.2 по шагу инструмента (0.1 XRP / 1 DOGE / 1 ADA)."""
    step = 0.1 if SYMBOL == "XRPUSDT" else 1.0
    q = math.floor(NOTIONAL / price / step) * step
    if q * price < 5.0:
        q += step
    if q * price > 6.5:
        return None
    return round(q, 1)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print(f"[{now()}] прервано вручную", flush=True)
        raise
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[{now()}] CRASH: {e}\n{tb}", file=sys.stderr, flush=True)
        try:
            botlog.BotLog().crash(f"v3 сессия упала: {type(e).__name__}: {e} — позиция может быть жива", tb)
        except Exception:
            pass
        sys.exit(1)
