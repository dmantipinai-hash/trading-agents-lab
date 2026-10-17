#!/usr/bin/env python3
"""exsync.py — синхронизация истины о деньгах с биржи (ТЗ 09/Прима, P0-1..P0-3).

Источники:
  /v5/position/closed-pnl  -> closed_trades (net_pnl = closedPnl, УЖЕ за вычетом комиссий;
                              проверено 17.09 сверкой с executions: closedPnl = pricePnl - openFee - closeFee)
  /v5/execution/list       -> executions (каждое частичное исполнение, fee, maker-флаг)

Правило (ТЗ 09 §5.2-F): локальный расчёт PnL — только диагностка; истина — биржа.
Виртуальный банк (virtual_ledger) меняется ТОЛЬКО через sync — одна точка входа.

Пакет А (аудит №14 §4.2, §4.3), 17.09:
  - symbol — ОБЯЗАТЕЛЬНЫЙ аргумент sync-функций: неявного default XRPUSDT больше нет
    (диагноз: DOGE торговался, синхронизировался XRP).
  - виртуальный банк меняют ВСЕ банковские стратегии (см. LEDGER_EPOCHS):
    epoch — исследовательская версия, принадлежность банку задаёт whitelist-множество,
    а не ветка if epoch == "v2-hybrid". v1 не переносится повторно (однократный
    архивный перенос уже сделан 17.09, id=2 в virtual_ledger).
  - в note проводки: полный source_event_id, symbol, strategy, event_at (время
    биржевого события, не импорта).
  - pnl_since_event(): дневной лимит считается по closed_at (день СОБЫТИЯ, §4.7) —
    поздняя синхронизация вчерашней сделки не съедает сегодняшний лимит.

Использование:
  из bot_v2/v3:   import exsync; exsync.sync_closed_trades(db.con, SYMBOL)
  вручную:        python3 exsync.py XRPUSDT     # синхронизация + сводка в stdout
"""
import os
import sys
import time
import datetime
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bybit_api as api
from position_state import LEDGER_EPOCHS, BANKS

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "trading.db")
CUTOFF_V2 = "2026-09-17T10:20:00Z"  # точка разреза эпох: 13:20 МСК (внедрение гибрида; см. 10-ТЗ-v2)
# Точка разреза эпохи v4-vol (включение VOL-1 в прод, карточка t_c1464b6b, решение Димы 19.09 ~23:50):
# сделки bot-v1/bot-v2, ЗАКРЫТЫЕ позже этой точки, попадают в эпоху v4-vol. Кроны v2 :05 / v1 :15.
# 22:00Z выбрано: старые процессы вышли (v1 ~21:45Z, v2 ~21:56Z, со старым exsync в памяти),
# первый запуск с VOL1 — 22:05Z. Исключает неверную атрибуцию сделок на стыке.
CUTOFF_V4VOL = "2026-09-18T22:00:00Z"
# Точка разреза эпохи v2-ena (DOGE→ENA, карточка t_28fa61b5, подготовка 03.10).
# ПОКА НЕ АКТИВНА: None — ветка разметки ниже не срабатывает. Значение (UTC ISO)
# проставит flip-карточка Локи в момент переключения обёртки (после DOGE-flatten);
# сделки bot-v2, закрытые позже точки, размечаются эпохой v2-ena. Банк НЕ меняется:
# счёт по стратегии bot-v2 (v2-hybrid), как у v4-vol (файл 32, правило BANK_OF_EPOCH).
CUTOFF_V2ENA = "2026-10-05T07:05:00Z"   # flip-карточка t_8b5522fc, 05.10; активация обёртки Локи до 10:05 МСК
# LEDGER_EPOCHS (какие эпохи меняют банки) — в position_state.py, единый источник


def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_from_ms(ms):
    return datetime.datetime.fromtimestamp(int(ms) / 1000, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------- банки (17.09 вечер)
# Раздельные виртуальные банки трёх ботов (карточка t_776e9b14): каждая стратегия
# торгует от своего счёта virtual_ledger(account). Старт 19.91029153 — фактический
# остаток общего банка в точке разреза; история НЕ пересчитывается (запрет ТЗ).

# id последней проводки ДО разреза банков (миграция 17.09): всё, что ниже — общий банк
SPLIT_LEDGER_ID = 7

# эпоха закрытия -> банк (проводки банковских эпох; v1-архив банком не owns)
BANK_OF_EPOCH = {
    "v2-hybrid": "v2-hybrid",
    "v3-donchian": "v3-donchian",
    "pkgA-v2": "v2-hybrid",
    "pkgA-v3": "v3-donchian",
    "v1-revival": "v1-revival",
}
# сделка, у которой банк не определился (стратегия вне STRATEGY_ACCOUNTS) — не проводим
# (manual/operator: демо-эксперименты, к банкам не относятся)


def ensure_accounts(db_con):
    """Идемпотентно добавляет колонку account в virtual_ledger/closed_trades (17.09)."""
    vl_cols = [r[1] for r in db_con.execute("PRAGMA table_info(virtual_ledger)").fetchall()]
    if vl_cols and "account" not in vl_cols:
        db_con.execute("ALTER TABLE virtual_ledger ADD COLUMN account TEXT")
    ct_cols = [r[1] for r in db_con.execute("PRAGMA table_info(closed_trades)").fetchall()]
    if ct_cols and "account" not in ct_cols:
        db_con.execute("ALTER TABLE closed_trades ADD COLUMN account TEXT")
    db_con.commit()


def account_of(strategy, epoch):
    """Виртуальный счёт сделки (единая точка атрибуции банков; см. STRATEGY_ACCOUNTS).

    bot-v1 без эпохи v1-revival/v4-vol — мёртвая эпоха, в банки не попадает.
    v4-vol (с 18.09 21:00Z) не меняет банк: счёт по стратегии, как раньше.
    """
    if strategy == "bot-v1" and epoch not in ("v1-revival", "v4-vol"):
        return None
    from position_state import STRATEGY_ACCOUNTS
    return STRATEGY_ACCOUNTS.get(strategy)


def account_balance(db_con, account):
    """Баланс счёта: последняя проводка account; если счёт пуст — остаток общего
    банка ДО разреза (последняя проводка без счёта, id<=SPLIT_LEDGER_ID)."""
    ensure_accounts(db_con)
    row = db_con.execute("SELECT balance FROM virtual_ledger WHERE account=? ORDER BY id DESC LIMIT 1", (account,)).fetchone()
    if row:
        return row[0]
    row = db_con.execute(
        "SELECT balance FROM virtual_ledger WHERE account IS NULL AND id<=? ORDER BY id DESC LIMIT 1",
        (SPLIT_LEDGER_ID,)).fetchone()
    return row[0] if row else 20.0


def _reason_from_orders(con, order_id):
    row = con.execute("SELECT reason FROM orders WHERE order_id=?", (order_id,)).fetchone()
    if not row or not row[0]:
        return "UNKNOWN"
    r = row[0]
    if "SESSION-END" in r:
        return "SESSION-END"
    if "TP" in r:
        return "TP"
    if "SL" in r:
        return "SL"
    if "flatten" in r.lower() or "хвост" in r:
        return "FLATTEN"
    if "выход маркетом" in r:
        return "TIME"
    return "OTHER"


def _strategy_of(con, order_id):
    """Какой бот поставил закрывающий ордер (атрибуция сделок, предложение Димы 17.09)."""
    row = con.execute("SELECT strategy FROM orders WHERE order_id=?", (order_id,)).fetchone()
    return row[0] if row and row[0] else "unknown"


def sync_closed_trades(db_con, symbol):
    """Тянет закрытые сделки symbol с биржи, вставляет новые в closed_trades,
    применяет их net_pnl к virtual_ledger (для эпох из LEDGER_EPOCHS).
    symbol обязателен (§4.2). Возвращает список новых сделок (dict)."""
    if not symbol:
        raise ValueError("sync_closed_trades: symbol обязателен (аудит №14 §4.2)")
    rows, cursor = [], ""
    while True:
        params = {"category": "linear", "symbol": symbol, "limit": 50}
        if cursor:
            params["cursor"] = cursor
        res = api.get("/v5/position/closed-pnl", params)
        if res.get("retCode") != 0:
            raise RuntimeError("closed-pnl retCode=%s %s" % (res.get("retCode"), res.get("retMsg")))
        batch = res.get("result", {}).get("list", [])
        rows.extend(batch)
        cursor = res.get("result", {}).get("nextPageCursor", "")
        if not cursor or not batch:
            break
        time.sleep(0.15)

    new_trades = []
    ensure_accounts(db_con)
    for r in rows:
        oid = r["orderId"]
        closed_at = iso_from_ms(r["updatedTime"])
        existing = db_con.execute("SELECT 1 FROM closed_trades WHERE id=?", (oid,)).fetchone()
        if existing:
            continue
        reason = _reason_from_orders(db_con, oid)
        strategy = _strategy_of(db_con, oid)
        # эпоха: сначала атрибуция по стратегии (банки раздельные с 17.09 вечер),
        # потом исторический разрез по CUTOFF_V2 (manual/operator/unknown).
        # v4-vol (18.09 21:00Z): сделки ботов v1/v2 после точки разреза — новая эпоха,
        # банк СВОЕЙ стратегии (v1-revival/v2-hybrid не меняются: риск-модуль не
        # меняет принадлежность счёту, см. 25-рисковый-модуль §6.2).
        if strategy == "bot-v3":
            epoch = "v3-donchian"
        elif strategy == "bot-v2":
            if CUTOFF_V2ENA and closed_at >= CUTOFF_V2ENA:
                epoch = "v2-ena"       # эпоха ENA (t_28fa61b5): банк прежний, счёт bot-v2
            elif closed_at >= CUTOFF_V4VOL:
                epoch = "v4-vol"
            else:
                epoch = "v2-hybrid"
        elif strategy == "bot-v1":
            epoch = "v4-vol" if closed_at >= CUTOFF_V4VOL else "v1-revival"  # мёртвая v1 уже в БД (dedupe)
        else:
            epoch = "v1" if closed_at < CUTOFF_V2 else "v2-hybrid"
        acct = account_of(strategy, epoch)
        db_con.execute(
            """INSERT OR IGNORE INTO closed_trades
               (id, symbol, qty, avg_entry, avg_exit, closed_pnl, open_fee, close_fee, net_pnl,
                exit_reason, epoch, strategy, order_id_close, opened_at, closed_at, source, synced_at,
                account)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (oid, r["symbol"], float(r["qty"]), float(r["avgEntryPrice"]), float(r["avgExitPrice"]),
             float(r["closedPnl"]), float(r["openFee"]), float(r["closeFee"]), float(r["closedPnl"]),
             reason, epoch, strategy, oid, iso_from_ms(r["createdTime"]), closed_at, "exchange", now_iso(),
             acct))
        # виртуальный банк: проводка на счёт стратегии (разделение банков 17.09 вечер);
        # сделки без счёта (manual/operator/мёртвая v1) не проводятся
        if acct:
            bal = account_balance(db_con, acct)
            bal += float(r["closedPnl"])
            # полный source_event_id + symbol + strategy + время СОБЫТИЯ (§4.3 приёмка)
            db_con.execute(
                """INSERT INTO virtual_ledger (ts, kind, amount, balance, note, account)
                   VALUES (?,?,?,?,?,?)""",
                (now_iso(), "trade_pnl", float(r["closedPnl"]), bal,
                 "source_event_id=%s symbol=%s strategy=%s event_at=%s qty=%s reason=%s" % (
                     oid, r["symbol"], strategy, closed_at, r["qty"], reason), acct))
        new_trades.append(r)
    db_con.commit()
    return new_trades


def sync_executions(db_con, symbol):
    """Тянет исполнения (частичные филлы) symbol в таблицу executions.
    symbol обязателен (§4.2). Возвращает число новых."""
    if not symbol:
        raise ValueError("sync_executions: symbol обязателен (аудит №14 §4.2)")
    rows, cursor = [], ""
    while True:
        params = {"category": "linear", "symbol": symbol, "limit": 50}
        if cursor:
            params["cursor"] = cursor
        res = api.get("/v5/execution/list", params)
        if res.get("retCode") != 0:
            raise RuntimeError("execution/list retCode=%s %s" % (res.get("retCode"), res.get("retMsg")))
        batch = res.get("result", {}).get("list", [])
        rows.extend(batch)
        cursor = res.get("result", {}).get("nextPageCursor", "")
        if not cursor or not batch:
            break
        time.sleep(0.15)

    n = 0
    for e in rows:
        eid = e.get("execId") or "%s-%s-%s" % (e.get("orderId"), e.get("execTime"), e.get("execQty"))
        cur = db_con.execute(
            """INSERT OR IGNORE INTO executions
               (exec_id, order_id, symbol, side, exec_time, exec_price, exec_qty, exec_fee, is_maker, orderLinkId)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (eid, e["orderId"], e.get("symbol"), e.get("side"), iso_from_ms(e["execTime"]),
             float(e.get("execPrice", 0) or 0), float(e.get("execQty", 0) or 0),
             float(e.get("execFee", 0) or 0), 1 if e.get("isMaker") else 0, e.get("orderLinkId", "")))
        n += cur.rowcount
    db_con.commit()
    return n


def virtual_balance(db_con, account=None):
    """Баланс виртуального банка. account=None — последняя проводка любого счёта (legacy)."""
    if account:
        return account_balance(db_con, account)
    row = db_con.execute("SELECT balance FROM virtual_ledger ORDER BY id DESC LIMIT 1").fetchone()
    return row[0] if row else 20.0


def pnl_since(db_con, iso_ts, account=None):
    """Сумма trade_pnl счёта с момента iso_ts (legacy, по времени ИМПОРТА).

    Для дневных лимитов использовать pnl_since_event(): поздний sync вчерашней
    сделки не должен переносить убыток в сегодняшний лимит (§4.7).
    """
    if account:
        row = db_con.execute(
            "SELECT COALESCE(SUM(amount),0) FROM virtual_ledger WHERE kind='trade_pnl' AND ts >= ? AND account=?",
            (iso_ts, account)).fetchone()
        return row[0]
    row = db_con.execute(
        "SELECT COALESCE(SUM(amount),0) FROM virtual_ledger WHERE kind='trade_pnl' AND ts >= ?",
        (iso_ts,)).fetchone()
    return row[0]


def pnl_since_event(db_con, iso_ts, account=None):
    """Дневной/периодный PnL по дню БИРЖЕВОГО СОБЫТИЯ (closed_at, истина — биржа).

    Идемпотентен и независим от времени импорта: одна и та же сделка,
    синхронизированная сегодня или завтра, относится к своему дню закрытия.
    account=стратегии фильтрует по счёту (банки раздельные с 17.09 вечер).
    """
    ph = ",".join("?" * len(LEDGER_EPOCHS))
    q = ("SELECT COALESCE(SUM(net_pnl),0) FROM closed_trades "
         "WHERE closed_at >= ? AND epoch IN (%s)" % ph)
    args = (iso_ts,) + tuple(LEDGER_EPOCHS)
    if account:
        q += " AND account=?"
        args += (account,)
    row = db_con.execute(q, args).fetchone()
    return row[0]


if __name__ == "__main__":
    sym = sys.argv[1] if len(sys.argv) > 1 else ""
    if not sym:
        print("использование: python3 exsync.py <SYMBOL>  (symbol обязателен с Пакета А)")
        sys.exit(2)
    con = sqlite3.connect(DB_PATH, timeout=30)  # P0 t_6e99a982: длинные транзакции сканера
    new = sync_closed_trades(con, sym)
    ex = sync_executions(con, sym)
    print("exsync[%s]: новых закрытых сделок %d, новых исполнений %d" % (sym, len(new), ex))
    day_start = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    week_start = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for acct in BANKS:
        try:
            bal = account_balance(con, acct)
        except Exception:
            continue
        print("  банк %-12s %.5f USDT | за сегодня (события): %+.5f | за 7 дней: %+.5f" % (
            acct, bal, pnl_since_event(con, day_start, account=acct),
            pnl_since_event(con, week_start, account=acct)))
