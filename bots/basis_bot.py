#!/usr/bin/env python3
"""basis_bot.py — BS-2 базис-арбитраж (карточка t_e9bc7355, файл 28).

Четвёртый бот проекта: автономный арбитражёр дислокаций expiry-базиса.
Сигнал — из таблицы basis_snapshots (датчик BS-1, файл 24; сеть в тике
решений НЕ трогается — данные уже собраны сканером). Не LLM: чистые
правила, готовые к переносу на сервер (файл 28 §7).

Событие входа = базис ≥ 700 bps год В ТРЁХ ПОДРЯД тиках датчика И
min_notional_ok=1 во всех трёх И глубина обеих ног ≥ $20 в каждом.
Однотиковые вспышки (ETHUSDT 19.09: 729→506 bps) отфильтрованы.

Ноги: спот BUY ~$6 + фьючерс SELL ~$6 тейкером. Максимум 1 открытая
пара. Истина об исполнении — биржа (execution/list); журналы:
basis_positions / basis_trades / orders / bot_log; банк — virtual_ledger
счёт 'basis-bot' (депозит $12 в момент первого тика, файл 28 §5).
Экономика замкнута на себя: closed-pnl по expiry-символам никто кроме
этого бота не синхронизирует (exsync зовётся ботами только по своим
перпетуалам), двойного учёта нет.

Пороги зафиксированы ДО запуска (pre-registration, файл 28 §2).
Тик (одиночный, под крон): python3 basis_bot.py [--db путь]
"""
import argparse
import datetime
import hashlib
import json
import os
import sqlite3
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
import bybit_api as api  # подпись/троттлинг общие с ботами v1/v2/v3

DB_PATH = os.path.join(SCRIPT_DIR, "..", "trading.db")

EPOCH = "v4-basis"
STRATEGY = "basis-bot"            # orders.strategy / virtual_ledger.account
LEASE_KEY = ("basis-bot", "*")    # глобальный lease: один тик одновременно
# --- Фаза 5 offline (карточка t_9ba9223d): сокет-ветка ПО ОБРАЗЦУ v1/v2 ---
# TRADE_CORE_DAEMON_SOCK: ОБЕ ноги входа идут через Unix-сокет демона
# (hello -> fencing token -> intent с ДВУМЯ leg, atomic_group_id = link_id).
# Приоритет: демон > прямой API. Политика отказа как у v1/v2: ВХОД
# (OPEN_RISK, новая пара) fail-closed — без демона не входим; ВЫХОДЫ
# (exit/flatten/rollback, reduce-only) при недоступном демоне идут прямым
# путём (новых рисков нет). ФЛИПА НЕТ: обёртка НЕ экспортирует sock —
# включит отдельная карточка live-cutover BS-2 (паттерн ветки B).
DAEMON_SOCK = os.environ.get("TRADE_CORE_DAEMON_SOCK", "")
DAEMON_EPOCH_ID = "epoch-19c58ada4fbc44af"  # открытая эпоха risk.db
DAEMON_ACCOUNT = "local-account"
# D-3: окружение демона для intent-контракта. Живой демон крутится на
# демо-счёте BYBIT_DEMO; тесты фейк-демона подменяют через этот атрибут
# (bb.DAEMON_ENV = "ISOLATED_TEST"), чтобы политика пропускала ISOLATED.
DAEMON_ENV = "BYBIT_DEMO"

# --- манифест pre-registration (файл 28 §2; параметры НЕ подбирать) ---
THRESHOLD_BPS = 700.0     # 7% годовых exec-базис
CONFIRM_TICKS = 3         # столько ПОДРЯД тиков должны подтверждать событие
DEPTH_MIN_USD = 20.0      # глубина КАЖДОЙ ноги ≥ $20 в каждом тике
LEG_USD = 6.0             # номинал каждой ноги
LEG_USD_MAX = 6.5         # после округления не дороже
BANK_START = 12.0         # депозит счёта basis-bot на включении
COOLDOWN_MAX_ENTRIES = 2  # максимум входов в окне...
COOLDOWN_DAYS = 30        # ...30 дней (2/30дн)
DAILY_STOP = 0.30         # дневной лимит убытка счёта (блокирует ВХОДЫ)
PAIR_STOP = -0.50         # жёсткий стоп пары по суммарному чистому PnL
EXIT_FRAC = 0.40          # выход при базисе ≤ 40% от входного
MAX_HOLD_DAYS = 30        # максимальное удержание пары
DELIVERY_EXIT_DAYS = 1.0  # флаттен при ≤1 дня до поставки (delivery fee 0 —
                          # страховка от ручной доставки, выходим сами)
SNAP_FRESH_H = 2.0        # свежесть снапшота для решения, часов
LIVE_GUARD_FRAC = 0.40    # живой базис на входе ≥ 40% порога (280 bps)
SPOT_TAKER_BPS = 10.0     # файл 18: спот taker
FUT_TAKER_BPS = 5.5       # файл 18: деривативы taker
DB_TIMEOUT = 30           # P0 db-locked: timeout=30, короткие транзакции


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def now_iso():
    return now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(ts):
    return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def fmt_qty(qty, step):
    """Строка qty с числом знаков, кратным шагу инструмента."""
    if step <= 0:
        step = 1e-8
    s = ("%.10f" % step).rstrip("0")
    dec = len(s.split(".")[1]) if "." in s else 0
    return f"{qty:.{dec}f}"


def step_floor(qty, step):
    """Округлить вниз до кратного шагу (для остатков после частичных филлов)."""
    if step <= 0:
        return qty
    return int(qty / step + 1e-9) * step


def round_leg(price, step, min_qty):
    """qty (в МОНЕТАХ) под номинал для ФЬЮЧЕРС-ноги; None если не собирается.
    Спот-ногу считать нельзя: там Market BUY qty = сумма в USDT (см. spot_buy_usd)."""
    if price <= 0 or step <= 0:
        return None
    q = int(LEG_USD / price / step) * step
    if q * price < 5.0:            # minNotional деривативов $5 (файл 18)
        q += step
    q = round(q, 10)
    if q < min_qty or q * price < 5.0 or q * price > LEG_USD_MAX:
        return None
    return q


def spot_buy_usd():
    """Номинал спот-ноги: Market BUY на Bybit тратит qty USDT (живой пробой 19.09,
    retCode 170140 ниже $5 — минимум демо-спота)."""
    return LEG_USD


def spot_sellable(coins, step):
    """Сколько монет можно продать после Market BUY: fee 10 bps удержан В МОНЕТАХ,
    поэтому продаём с буфером; шаг = basePrecision спота."""
    if step <= 0:
        step = 0.01
    return int(coins * 0.9985 / step) * step


# --------------------------------------------------------------- сеть (тонкие обёртки; патчатся в тестах)

def net_get(path, params):
    return api.get(path, params)


def net_post(path, params):
    return api.post(path, params)


def get_instrument(category, symbol):
    d = net_get("/v5/market/instruments-info", {"category": category, "symbol": symbol})
    lst = d.get("result", {}).get("list") if d.get("retCode") == 0 else None
    if not lst:
        return None
    it = lst[0]
    lsf = it.get("lotSizeFilter") or {}
    try:
        if category == "spot":
            # спот-формат (живой пробой 19.09): basePrecision/minOrderQty/minOrderAmt,
            # поля qtyStep/minNotionalValue отсутствуют
            return {"qty_step": float(lsf.get("basePrecision") or 0),
                    "min_qty": float(lsf.get("minOrderQty") or 0),
                    "min_notional": float(lsf.get("minOrderAmt") or 0) or 5.0,
                    "delivery_ms": 0}
        return {"qty_step": float(lsf.get("qtyStep") or 0),
                "min_qty": float(lsf.get("minOrderQty") or 0),
                "min_notional": float(lsf.get("minNotionalValue") or 0) or 5.0,
                "delivery_ms": int(it.get("deliveryTime") or 0)}
    except (TypeError, ValueError):
        return None


def get_ticker(category, symbol):
    d = net_get("/v5/market/tickers", {"category": category, "symbol": symbol})
    lst = d.get("result", {}).get("list") if d.get("retCode") == 0 else None
    if not lst:
        return None
    t = lst[0]
    try:
        return {"bid": float(t.get("bid1Price") or 0), "ask": float(t.get("ask1Price") or 0)}
    except (TypeError, ValueError):
        return None


def market_order(category, symbol, side, qty_str, reduce_only, link_id):
    p = {"category": category, "symbol": symbol, "side": side,
         "orderType": "Market", "qty": qty_str, "orderLinkId": link_id}
    if reduce_only:
        p["reduceOnly"] = "true"
    return net_post("/v5/order/create", p)


def execs_by_link(category, symbol, prefix):
    """Свои исполнения по префиксу orderLinkId (входы/откаты/пробы).
    D-3: у групп orderLinkId = coid вида eg:<eg_gid>:l<leg>:<attempt> —
    eg_gid = 'eg-'+sha256(intent_id)[:12]; сверки по bs2g-<ts> больше нет."""
    d = net_get("/v5/execution/list", {"category": category, "symbol": symbol, "limit": 50})
    lst = d.get("result", {}).get("list") if d.get("retCode") == 0 else None
    if not lst:
        return []
    out = []
    for e in lst:
        lid = e.get("orderLinkId") or ""
        if lid.startswith(prefix):
            try:
                out.append({"qty": float(e.get("execQty", 0) or 0),
                            "price": float(e.get("execPrice", 0) or 0),
                            "fee": float(e.get("execFee", 0) or 0),
                            "link_id": lid})
            except (TypeError, ValueError):
                continue
    return out


def eg_gid_of(intent_id):
    """Детерминированный group_id gateway (D-2/D-3): 'eg-'+sha256(intent_id)[:12].
    Совпадает с group_id в risk_engine/gateway/core.py::_enforce_group."""
    return "eg-" + hashlib.sha256(intent_id.encode("utf-8")).hexdigest()[:12]


def fut_position(symbol):
    """Живая шорт-позиция по expiry-фьючерсу: (size, avgPrice) или None."""
    d = net_get("/v5/position/list", {"category": "linear", "symbol": symbol})
    lst = d.get("result", {}).get("list") if d.get("retCode") == 0 else None
    if not lst:
        return None
    for p in lst:
        sz = float(p.get("size", 0) or 0)
        if sz > 0 and p.get("side", "").lower() == "sell":
            return sz, float(p.get("avgPrice", 0) or 0)
    return None


def journal_order(con, resp, symbol, category, side, qty, price, reason, link_id):
    """Ордер в общую таблицу orders (strategy='basis-bot')."""
    if resp.get("retCode") != 0:
        return None
    oid = resp["result"]["orderId"]
    con.execute("""INSERT OR IGNORE INTO orders (order_id, symbol, category, side, order_type,
        time_in_force, qty, price, notional_usd, status, orderLinkId, reason, strategy,
        market_price_at_place, deviation_pct, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, symbol, category, side, "Market", "IOC", float(qty), float(price),
         float(qty) * float(price), "New", link_id, reason, STRATEGY,
         None, None, now_iso(), now_iso()))
    con.commit()
    return oid


# --------------------------------------------------------------- БД

DDL = """
CREATE TABLE IF NOT EXISTS basis_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opened_at TEXT NOT NULL,
    symbol_fut TEXT NOT NULL,
    symbol_spot TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',      -- open | closing | closed | rollback
    entry_snapshot_id INTEGER,
    basis_at_entry REAL,                      -- год bps по exec-ценам входа
    exit_threshold_bps REAL NOT NULL,
    spot_entry_qty REAL, spot_entry_price REAL, spot_entry_fee REAL,
    fut_entry_qty REAL, fut_entry_price REAL, fut_entry_fee REAL,
    closed_at TEXT, close_reason TEXT, basis_at_exit REAL,
    pair_pnl_net REAL, run_id TEXT,
    note TEXT
);
CREATE INDEX IF NOT EXISTS ix_bpos_status ON basis_positions (status);
CREATE INDEX IF NOT EXISTS ix_bpos_opened ON basis_positions (opened_at);
CREATE TABLE IF NOT EXISTS basis_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id INTEGER NOT NULL,
    epoch TEXT NOT NULL DEFAULT 'v4-basis',
    strategy TEXT NOT NULL DEFAULT 'basis-bot',
    ts TEXT NOT NULL,
    leg TEXT NOT NULL,        -- spot | fut | pair | incident
    side TEXT,
    qty REAL, price REAL, exec_price REAL, fee REAL,
    basis_bps REAL,
    pnl_gross REAL, pnl_net REAL,
    reason TEXT
);
CREATE INDEX IF NOT EXISTS ix_btr_pos ON basis_trades (position_id);
CREATE TABLE IF NOT EXISTS bs2_open_intents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id TEXT NOT NULL,
    eg_gid TEXT NOT NULL,
    symbol_fut TEXT NOT NULL, symbol_spot TEXT NOT NULL,
    snap_id INTEGER, basis_bps REAL, days REAL,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending|registered|incident|failed|stuck
    attempts INTEGER NOT NULL DEFAULT 0,
    resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_bsoi_status ON bs2_open_intents (status);
CREATE TABLE IF NOT EXISTS bs2_exit_intents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id INTEGER NOT NULL,
    intent_id TEXT NOT NULL,
    eg_gid TEXT NOT NULL,
    ts TEXT NOT NULL, reason TEXT,
    status TEXT NOT NULL DEFAULT 'sent'
);
CREATE INDEX IF NOT EXISTS ix_bsei_pos ON bs2_exit_intents (position_id);
"""


def ensure_tables(con):
    con.executescript(DDL)
    con.commit()


def ensure_deposit(con):
    """Депозит $12 в момент включения (идемпотентно). Точка разреза — файл 28 §5."""
    row = con.execute(
        "SELECT 1 FROM virtual_ledger WHERE account=? AND kind='deposit'", (STRATEGY,)).fetchone()
    if row:
        return False
    con.execute("""INSERT INTO virtual_ledger (ts, kind, amount, balance, note, account)
                   VALUES (?,?,?,?,?,?)""",
                (now_iso(), "deposit", BANK_START, BANK_START,
                 "basis-bot стартовый депозит (BS-2, t_e9bc7355, файл 28)", STRATEGY))
    con.commit()
    return True


def bank_balance(con):
    row = con.execute(
        "SELECT balance FROM virtual_ledger WHERE account=? ORDER BY id DESC LIMIT 1",
        (STRATEGY,)).fetchone()
    return row[0] if row else 0.0


def bank_apply_pnl(con, amount, note):
    bal = bank_balance(con) + amount
    con.execute("""INSERT INTO virtual_ledger (ts, kind, amount, balance, note, account)
                   VALUES (?,?,?,?,?,?)""",
                (now_iso(), "trade_pnl", amount, bal, note, STRATEGY))
    con.commit()
    return bal


def blog(run_id, level, phase, msg, detail=None):
    ts = now_iso()
    try:
        con = sqlite3.connect(DB_PATH, timeout=5)
        con.execute("INSERT INTO bot_log (ts, run_id, level, phase, msg, detail) VALUES (?,?,?,?,?,?)",
                    (ts, run_id, level, phase, str(msg)[:500],
                     (str(detail)[:2000] if detail else None)))
        con.commit()
        con.close()
    except Exception as e:
        print(f"[{ts}] BOTLOG-FAIL {e}", file=sys.stderr, flush=True)


def acquire_lease(con, holder, ttl_sec=300):
    con.execute("""CREATE TABLE IF NOT EXISTS strategy_leases (
        strategy TEXT NOT NULL, symbol TEXT NOT NULL, holder TEXT,
        acquired_at TEXT, expires_at REAL, note TEXT,
        PRIMARY KEY (strategy, symbol))""")
    con.commit()
    now = time.time()
    try:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute(
            "SELECT holder, expires_at FROM strategy_leases WHERE strategy=? AND symbol=?",
            LEASE_KEY).fetchone()
        if row and row[1] > now and row[0] != holder:
            con.execute("ROLLBACK")
            return False
        con.execute("""INSERT OR REPLACE INTO strategy_leases
            (strategy, symbol, holder, acquired_at, expires_at, note)
            VALUES (?,?,?,?,?,?)""",
            (LEASE_KEY[0], LEASE_KEY[1], holder, now_iso(), now + ttl_sec, "basis-bot tick"))
        con.commit()
        return True
    except sqlite3.OperationalError:
        try:
            con.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        return False


def release_lease(con, holder):
    con.execute("DELETE FROM strategy_leases WHERE strategy=? AND symbol=? AND holder=?",
                (LEASE_KEY[0], LEASE_KEY[1], holder))
    con.commit()


# --------------------------------------------------------------- решения (чистые функции)

def last_ticks(con, symbol, n=CONFIRM_TICKS):
    """Последние n снимков контракта (старые -> новые)."""
    return con.execute(
        """SELECT id, ts, basis_annual_bps, min_notional_ok,
                  depth_bid5_fut_usd, depth_ask5_spot_usd, days_to_expiry
           FROM basis_snapshots WHERE symbol_fut=? ORDER BY id DESC LIMIT ?""",
        (symbol, n)).fetchall()[::-1]


def is_event(ticks, now=None):
    """Три подряд тика ≥ THRESHOLD_BPS, min_notional_ok=1, глубины ≥ $20,
    снапшот свежий. Возвращает (True, basis_latest) или (False, причина)."""
    now = now or now_utc()
    if len(ticks) < CONFIRM_TICKS:
        return False, f"тиков {len(ticks)} < {CONFIRM_TICKS}"
    newest_age_h = (now - parse_iso(ticks[-1][1])).total_seconds() / 3600.0
    if newest_age_h > SNAP_FRESH_H:
        return False, f"снапшот несвежий ({newest_age_h:.1f} ч)"
    for t in ticks:
        if t[2] is None or t[2] < THRESHOLD_BPS:
            return False, f"базис {t[2] and round(t[2])} < {THRESHOLD_BPS}"
        if not t[3]:
            return False, "min_notional_ok=0"
        if (t[4] or 0) < DEPTH_MIN_USD:
            return False, f"глубина фьюча ${t[4]} < ${DEPTH_MIN_USD}"
        if (t[5] or 0) < DEPTH_MIN_USD:
            return False, f"глубина спота ${t[5]} < ${DEPTH_MIN_USD}"
    return True, ticks[-1][2]


def event_candidates(con, now=None):
    """Контракты с валидным событием прямо сейчас: [(basis, symbol, snap_id, days)]."""
    now = now or now_utc()
    symbols = [r[0] for r in con.execute(
        "SELECT DISTINCT symbol_fut FROM basis_snapshots ORDER BY id DESC LIMIT 200")]
    out = []
    for sym in symbols:
        ticks = last_ticks(con, sym)
        if not ticks:
            continue
        ok, info = is_event(ticks, now)
        if ok and ticks[-1][6] > DELIVERY_EXIT_DAYS + 1:
            out.append((info, sym, ticks[-1][0], ticks[-1][6]))
    out.sort(reverse=True)
    return out


def entry_precheck(con):
    """Предсетевые проверки (все обязательные). Возвращает (ok, why)."""
    n_open = con.execute(
        "SELECT COUNT(*) FROM basis_positions WHERE status IN ('open','closing')").fetchone()[0]
    if n_open:
        return False, f"открытых пар {n_open} (максимум 1)"
    since = (now_utc() - datetime.timedelta(days=COOLDOWN_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    n_recent = con.execute(
        "SELECT COUNT(*) FROM basis_positions WHERE opened_at >= ?", (since,)).fetchone()[0]
    if n_recent >= COOLDOWN_MAX_ENTRIES:
        return False, f"кулдаун: {n_recent} входов за {COOLDOWN_DAYS} дн (макс {COOLDOWN_MAX_ENTRIES})"
    bank = bank_balance(con)
    if bank < BANK_START:
        return False, f"банк {bank:.2f} < {BANK_START:.2f}"
    day_start = now_utc().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    day_pnl = con.execute(
        "SELECT COALESCE(SUM(pnl_net),0) FROM basis_trades WHERE leg IN ('pair','incident') AND ts >= ?",
        (day_start,)).fetchone()[0]
    if day_pnl <= -DAILY_STOP:
        return False, f"дневной лимит: {day_pnl:+.2f} ≤ -{DAILY_STOP:.2f}"
    return True, "OK"


def exit_reason(pos, snap, marks, delivery_ms, now=None):
    """Первый сработавший выход открытой пары. marks=(spot_bid, fut_ask) или None.
    snap=(ts, basis_bps) или None."""
    now = now or now_utc()
    opened = parse_iso(pos["opened_at"])
    if (now - opened).total_seconds() / 86400.0 >= MAX_HOLD_DAYS:
        return "max_hold_30d"
    if delivery_ms and (delivery_ms / 1000.0 - now.timestamp()) / 86400.0 <= DELIVERY_EXIT_DAYS:
        return "delivery_tomorrow"
    if snap is not None:
        snap_age_h = (now - parse_iso(snap[0])).total_seconds() / 3600.0
        if snap_age_h <= SNAP_FRESH_H and snap[1] is not None:
            if snap[1] <= pos["exit_threshold_bps"]:
                return f"basis_collapse_{snap[1]:.0f}bps"
    if marks:
        spot_bid, fut_ask = marks
        gross = ((spot_bid - pos["spot_entry_price"]) * pos["spot_entry_qty"]
                 + (pos["fut_entry_price"] - fut_ask) * pos["fut_entry_qty"])
        est_exit_fees = (spot_bid * pos["spot_entry_qty"] * SPOT_TAKER_BPS / 1e4
                         + fut_ask * pos["fut_entry_qty"] * FUT_TAKER_BPS / 1e4)
        entry_fees = (pos["spot_entry_fee"] or 0) + (pos["fut_entry_fee"] or 0)
        if gross - est_exit_fees - entry_fees <= PAIR_STOP:
            return "hard_stop_-0.50"
    return None


def live_basis_bps(fut_bid, spot_ask, days):
    if not fut_bid or not spot_ask or days <= 0:
        return None
    return (fut_bid - spot_ask) / spot_ask * 365.0 / days * 1e4


# --------------------------------------------------- Фаза 5: сокет-ветка демона

class DaemonDown(Exception):
    """Демон недоступен (нет сокета / таймаут) — ордер НЕ отправлен."""


def _daemon_client():
    """Клиент сокета демона. Корень репо в sys.path (не сам пакет
    trade_core — грабелька из v3 daemon_emit, фикс-коммит 26.09)."""
    sys.path.insert(0, os.path.join(
        os.path.dirname(os.path.abspath(__file__)), ".."))
    from trade_core.live import client as tcc
    return tcc


def daemon_hello():
    tcc = _daemon_client()
    hello = tcc.hello(DAEMON_SOCK, "bot-bs2", "BYBIT_DEMO", DAEMON_ACCOUNT)
    if not hello.get("ok"):
        return None, hello
    return hello["fencing_token"], hello


def _leg(category, symbol, side, qty, order_type, reduce_only, price=None,
         leg_index=0):
    leg = {
        "category": category, "symbol": symbol, "side": side,
        "order_type": order_type, "requested_qty": qty,
        "reduce_only": reduce_only, "limit_price": None,
        "leg_index": leg_index, "position_idx": 0,
    }
    if order_type == "Limit" and price is not None:
        # канонический decimal без хвостовых нулей (грабелка v2, L6.6)
        s = ("%f" % price).rstrip("0").rstrip(".")
        leg["limit_price"] = s or "0"
    return leg


def daemon_group_entry(con, run_id, sym_fut, spot_sym, q_fut, inst_f,
                       basis, snap_id, days, spot_px=None, fut_px=None):
    """ВХОД BS-2 через демона (D-3): intent с ДВУМЯ ногами (спот BUY
    USDT-номинал + фьюч SELL в монетах). Прямой submit недопустим:
    недоступен демон / reject — входа НЕТ (fail-closed), ждём тик.
    spot_px/fut_px (ask/bid из do_entry) идут в limit_price Market-ног —
    gateway использует их как normalized_price (не нулевой резерв).

    Возвращает (ok, detail):
      ok=True  — группа доведена демоном до BALANCED_OPEN, ПАРА
                 зарегистрирована в basis_positions (pid в detail);
      ok=False — детерминированный отказ/инцидент (detail str) — пара
                 не открыта, риска нет или откат оформлен демоном
                 (BALANCED_CLOSED/FAILED_EXPOSED — стоимости в detail).
    Частичный fill ноги демона = группа PARTIALLY_BALANCED → COMPENSATING
    (bounded, D-2) — бот НИКОГДА не торгует сам (шаг 3 карточки D-3).
    """
    tcc = _daemon_client()
    token, hello = daemon_hello()
    if token is None:
        return False, f"hello отклонён: {hello.get('error')} {hello.get('detail', '')}"
    ts = int(time.time() * 1000)
    intent_id = f"bot-bs2:grp-{ts}"
    qty_spot = fmt_qty(spot_buy_usd(), 0.01)  # спот BUY: сумма в USDT
    intent = {
        "schema_version": "strategy-intent.v2",
        "intent_id": intent_id,
        "idempotency_key": intent_id,
        "bot_id": "bot-bs2",
        "strategy_id": "basis-bot",
        "strategy_version": EPOCH,
        "environment": DAEMON_ENV,
        "account_id": DAEMON_ACCOUNT,
        "purpose": "OPEN_RISK",
        "legs": [
            _leg("spot", spot_sym, "Buy", qty_spot, "Market", False,
                 price=spot_px, leg_index=0),
            _leg("linear", sym_fut, "Sell",
                 fmt_qty(q_fut, inst_f["qty_step"]), "Market", False,
                 price=fut_px, leg_index=1),
        ],
        "signal_time": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                     time.gmtime(time.time() - 1)),
        "created_at": now_iso(),
        "valid_until": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                     time.gmtime(time.time() + 60)),
        "decision_horizon_ms": 15000,
        "expected_hold_ms": MAX_HOLD_DAYS * 86400 * 1000,
        "market_snapshot_ref": "bs1-basis-snapshots",
        "epoch_id": DAEMON_EPOCH_ID,
    }
    eg_gid = eg_gid_of(intent_id)
    # pending ДО отправки: kill между intent и регистрацией пары не теряет
    # вход — следующий тик сверит executions по eg-префиксу (шаг 3 п.5)
    con.execute("""INSERT INTO bs2_open_intents
        (intent_id, eg_gid, symbol_fut, symbol_spot, snap_id, basis_bps, days, created_at)
        VALUES (?,?,?,?,?,?,?,?)""",
        (intent_id, eg_gid, sym_fut, spot_sym, snap_id, basis, days, now_iso()))
    con.commit()
    try:
        r = tcc.intent(DAEMON_SOCK, intent, token)
    except Exception as e:
        # включая DaemonDown/таймаут: intent мог дойти и исполниться —
        # pending остаётся, сверка на тике; новых ордеров НЕ шлём
        blog(run_id, "ERROR", "entry",
             f"DAEMON intent-ответ не получен ({type(e).__name__}: {e}) — "
             f"pending {intent_id}, сверка на следующем тике", "")
        return False, f"daemon transport: {e}"
    gws = r.get("gateway_status")
    reasons = r.get("reason_codes") or []
    if gws == "ACCEPTED" and "GROUP_BALANCED_OPEN" in reasons:
        return _register_group_pair(con, run_id, sym_fut, spot_sym, snap_id,
                                    basis, days, eg_gid, intent_id)
    if gws == "ACCEPTED" and "GROUP_COMPENSATED" in reasons:
        # BALANCED_CLOSED: демона компенсировал односторонний fill —
        # стоимость инцидента из фактических филлов группы
        return False, _incident_from_group(
            con, run_id, spot_sym, sym_fut, eg_gid, intent_id,
            note="daemon-group: компенсация демона (BALANCED_CLOSED)")
    if gws == "REJECTED" and r.get("reject_code") == "GROUP_FAILED_EXPOSED":
        # обе ноги не исполнились — риска нет (резерв RELEASED);
        # это чистый отказ, не инцидент: пара не открывалась
        _resolve_open_intent(con, intent_id, "failed")
        return False, "группа не исполнилась (FAILED_EXPOSED), риска нет"
    if gws in ("ACCEPTED", "DUPLICATE_RETURNED") or "GROUP_IN_PROGRESS" in reasons:
        # SUBMITTING/PARTIALLY_BALANCED/COMPENSATING ещё в полёте — демон
        # доведёт (resume при рестарте); бот ждёт, ничего сам не шлёт
        blog(run_id, "WARN", "entry",
             f"группа {eg_gid} в полёте ({gws}/{reasons}) — pending, "
             f"доводка на следующем тике", str(r)[:300])
        return False, "group in progress"
    _resolve_open_intent(con, intent_id, "incident")
    return False, f"gateway={gws} codes={reasons}"


def _fills_by_eg(con, category, sym, eg_gid):
    """Филлы группы с биржи по eg-префиксу (orderLinkId = eg:<gid>:l|u<n>:<a>).
    Истина — биржа (execution/list)."""
    return execs_by_link(category, sym, f"eg:{eg_gid}:")


def _register_group_pair(con, run_id, sym_fut, spot_sym, snap_id, basis, days,
                         eg_gid, intent_id):
    """BALANCED_OPEN: собрать филлы обеих ног по eg-префиксу, зарегистриро-
    вать пару (идемпотентно по intent_id в note)."""
    fills_s = _fills_by_eg(con, "spot", spot_sym, eg_gid)
    fills_f = _fills_by_eg(con, "linear", sym_fut, eg_gid)
    filled_s = abs(sum(f["qty"] for f in fills_s))
    filled_f = abs(sum(f["qty"] for f in fills_f))
    if not (filled_s > 0 and filled_f > 0):
        # снапшот группы BALANCED_OPEN, но execution/list ещё догоняет —
        # pending, следующий тик дозарегистрирует
        blog(run_id, "WARN", "entry",
             f"группа {eg_gid} BALANCED_OPEN, но филлы ещё не видны "
             f"(spot={filled_s:g} fut={filled_f:g}) — pending", "")
        return False, "fills not visible yet"
    pid = _register_pair(con, run_id, sym_fut, spot_sym, snap_id,
                         basis, days, fills_s, filled_s, fills_f, filled_f,
                         intent_id=intent_id)
    _resolve_open_intent(con, intent_id, "registered")
    blog(run_id, "SESSION", "entry",
         f"DAEMON группа-вход зарегистрирован: pid={pid} gid={eg_gid}",
         f"intent={intent_id}")
    return True, {"pid": pid, "gid": eg_gid, "intent_id": intent_id}


def _incident_from_group(con, run_id, spot_sym, sym_fut, eg_gid, intent_id,
                         note):
    """BALANCED_CLOSED/FAILED: посчитать стоимость инцидента по фактическим
    филлам группы (входы + unwind демона), записать rollback-позу."""
    fills_s = _fills_by_eg(con, "spot", spot_sym, eg_gid)
    fills_f = _fills_by_eg(con, "linear", sym_fut, eg_gid)
    filled_s = abs(sum(f["qty"] for f in fills_s))
    filled_f = abs(sum(f["qty"] for f in fills_f))
    cost = 0.0
    # чистые потоки ноги: Buy+Sell по одному символу/категории внутри
    # группы (вход + unwind демона) — суммарная комиссия и есть стоимость
    fee_s = abs(sum(f["fee"] for f in fills_s))
    fee_f = abs(sum(f["fee"] for f in fills_f))
    cost = -(fee_s + fee_f)
    _resolve_open_intent(con, intent_id, "incident")
    pid = con.execute("""INSERT INTO basis_positions
        (opened_at, symbol_fut, symbol_spot, status, basis_at_entry, exit_threshold_bps,
         spot_entry_qty, fut_entry_qty, closed_at, close_reason, pair_pnl_net, run_id, note)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (now_iso(), sym_fut, spot_sym, "rollback", None, 0.0,
         filled_s or None, filled_f or None, now_iso(),
         "group_compensated", round(cost, 8), run_id,
         f"{note}; филлы группы {eg_gid}: spot={filled_s:g} fut={filled_f:g}; "
         f"intent={intent_id}")).lastrowid
    con.execute("""INSERT INTO basis_trades
        (position_id, ts, leg, pnl_net, reason)
        VALUES (?,?,?,?,?)""",
        (pid, now_iso(), "incident", round(cost, 8), note))
    bal = bank_apply_pnl(con, cost,
                         f"basis-bot групповой инцидент {sym_fut} (pos {pid})")
    con.commit()
    blog(run_id, "ERROR", "rollback",
         f"ИНЦИДЕНТ BS-2 (группа): {sym_fut} компенсирована демоном — "
         f"стоимость {cost:+.4f}$ | банк {bal:.2f}$", f"gid={eg_gid} {note}")
    return (f"инцидент: компенсация демона, стоимость {cost:+.4f}$ "
            f"(pos {pid})")


def _resolve_open_intent(con, intent_id, status):
    con.execute("UPDATE bs2_open_intents SET status=?, resolved_at=? WHERE intent_id=?",
                (status, now_iso(), intent_id))
    con.commit()


def resolve_pending_opens(con, run_id):
    """Pending-входы (транспортный обрыв/kill между intent и регистрацией):
    сверить executions по eg-префиксу, дозарегистрировать пару."""
    rows = con.execute("""SELECT * FROM bs2_open_intents
                          WHERE status='pending' ORDER BY id""").fetchall()
    for row in rows:
        row = dict(row)
        row["attempts"] = (row.get("attempts") or 0) + 1
        fills_s = _fills_by_eg(con, "spot", row["symbol_spot"], row["eg_gid"])
        fills_f = _fills_by_eg(con, "linear", row["symbol_fut"], row["eg_gid"])
        filled_s = abs(sum(f["qty"] for f in fills_s))
        filled_f = abs(sum(f["qty"] for f in fills_f))
        if filled_s > 0 and filled_f > 0:
            blog(run_id, "SESSION", "entry",
                 f"pending {row['intent_id']}: обе ноги исполнены — регистрирую пару")
            _register_pair(con, run_id, row["symbol_fut"], row["symbol_spot"],
                           row["snap_id"], row["basis_bps"] or 0.0,
                           row["days"] or 30.0, fills_s, filled_s,
                           fills_f, filled_f, intent_id=row["intent_id"])
            _resolve_open_intent(con, row["intent_id"], "registered")
        elif row["attempts"] >= 6:
            # много тиков без второй ноги: группа демона обязана была
            # сойтись (bounded) — фиксируем stuck для разбора человеком
            _resolve_open_intent(con, row["intent_id"], "stuck")
            blog(run_id, "ERROR", "entry",
                 f"pending {row['intent_id']} застрял ({row['attempts']} тиков, "
                 f"spot={filled_s:g} fut={filled_f:g}) — stuck, разбор вручную", "")
        else:
            con.execute("UPDATE bs2_open_intents SET attempts=? WHERE id=?",
                        (row["attempts"], row["id"]))
            con.commit()


def daemon_group_exit(con, run_id, pos, reason, need_s, need_f,
                      inst_s, inst_f):
    """ВЫХОД BS-2 через демона (D-3): ОДИН 2-ноги intent purpose=CLOSE —
    spot Sell (монеты, sellable-буфер уже учтён в need_s) + linear Buy
    reduce_only (шаг уже учтён в need_f). Прямых ордеров бот не шлёт
    ВООБЩЕ: демон недоступен / reject → False, следующий тик повторит
    (недостающее пересчитается по executions — идемпотентно).

    Возвращает (ok, detail): ok=True — группа выхода доведена демоном
    (BALANCED_OPEN у exit-группы = обе ноги исполнены); ok=False —
    повтор на тике. Позиция переводится в 'closing' ТОЛЬКО при ok.
    """
    tcc = _daemon_client()
    token, hello = daemon_hello()
    if token is None:
        return False, f"hello отклонён: {hello.get('error')}"
    spot_sym, sym_fut = pos["symbol_spot"], pos["symbol_fut"]
    ts = int(time.time() * 1000)
    intent_id = f"bot-bs2:exit-{ts}"
    # цены для Market-нормализации (gateway берёт limit_price как
    # normalized_price из risk-request; Market-ордера их не шлют на биржу)
    tk_s = get_ticker("spot", spot_sym)
    tk_f = get_ticker("linear", sym_fut)
    px_s = tk_s["bid"] if tk_s else None
    px_f = tk_f["ask"] if tk_f else None
    if px_s is None or px_f is None:
        return False, "тикеры недоступны для exit-цен"
    intent = {
        "schema_version": "strategy-intent.v2",
        "intent_id": intent_id,
        "idempotency_key": intent_id,
        "bot_id": "bot-bs2",
        "strategy_id": "basis-bot",
        "strategy_version": EPOCH,
        "environment": DAEMON_ENV,
        "account_id": DAEMON_ACCOUNT,
        "purpose": "CLOSE",
        "legs": [
            _leg("spot", spot_sym, "Sell",
                 fmt_qty(need_s, (inst_s or {}).get("qty_step", 0)),
                 "Market", False, price=px_s, leg_index=0),
            _leg("linear", sym_fut, "Buy",
                 fmt_qty(need_f, (inst_f or {}).get("qty_step", 0)),
                 "Market", True, price=px_f, leg_index=1),
        ],
        "signal_time": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                     time.gmtime(time.time() - 1)),
        "created_at": now_iso(),
        "valid_until": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                     time.gmtime(time.time() + 60)),
        "decision_horizon_ms": 15000,
        "expected_hold_ms": 60000,
        "market_snapshot_ref": "bs1-basis-snapshots",
        "epoch_id": DAEMON_EPOCH_ID,
    }
    eg_gid = eg_gid_of(intent_id)
    con.execute("""INSERT INTO bs2_exit_intents
        (position_id, intent_id, eg_gid, ts, reason)
        VALUES (?,?,?,?,?)""",
        (pos["id"], intent_id, eg_gid, now_iso(), reason))
    con.commit()
    try:
        r = tcc.intent(DAEMON_SOCK, intent, token)
    except Exception as e:
        blog(run_id, "ERROR", "exit",
             f"DAEMON exit-intent не доставлен ({type(e).__name__}: {e}) — "
             f"повтор на следующем тике", f"intent={intent_id}")
        return False, f"daemon transport: {e}"
    gws = r.get("gateway_status")
    reasons = r.get("reason_codes") or []
    if gws == "ACCEPTED" and "GROUP_BALANCED_OPEN" in reasons:
        blog(run_id, "SESSION", "exit",
             f"DAEMON группа-выход исполнена ({reason})",
             f"gid={eg_gid} intent={intent_id}")
        return True, {"gid": eg_gid, "intent_id": intent_id, "detail": r}
    if gws == "ACCEPTED" and "GROUP_COMPENSATED" in reasons:
        # exit-группа скомпенсирована (одна нога частично) — недостающее
        # доберётся повтором: settle видит фактические филлы по eg-префиксу
        blog(run_id, "WARN", "exit",
             f"DAEMON exit-группа компенсирована ({reason}) — добор на тике",
             f"gid={eg_gid}")
        return False, "exit group compensated"
    if gws in ("ACCEPTED", "DUPLICATE_RETURNED") or "GROUP_IN_PROGRESS" in reasons:
        blog(run_id, "WARN", "exit",
             f"exit-группа в полёте ({gws}/{reasons}) — доводка на тике",
             f"gid={eg_gid}")
        return False, "exit in progress"
    blog(run_id, "ERROR", "exit",
         f"DAEMON exit-группа отклонена: {gws} {reasons} — повтор на тике",
         f"gid={eg_gid}")
    return False, f"gateway={gws} codes={reasons}"


def daemon_exit_leg(con, run_id, category, sym, side, qty, qty_step,
                    reduce_only, tag):
    """[DIAGNOSTIC-ONLY, D-3] одноногий intent — прямого submit не делает,
    оставлен для диагностики/отладки сокета. В путях исполнения НЕcaller."""
    tcc = _daemon_client()
    token, hello = daemon_hello()
    if token is None:
        return False, {"error": f"hello: {hello.get('error')}"}
    ts = int(time.time() * 1000)
    purpose = "CLOSE" if reduce_only else "REDUCE_RISK"
    intent = {
        "schema_version": "strategy-intent.v2",
        "intent_id": f"bot-bs2:{tag}-{ts}",
        "idempotency_key": f"bot-bs2:{tag}-{ts}",
        "bot_id": "bot-bs2",
        "strategy_id": "basis-bot",
        "strategy_version": EPOCH,
        "environment": DAEMON_ENV,
        "account_id": DAEMON_ACCOUNT,
        "purpose": purpose,
        "legs": [_leg(category, sym, side, fmt_qty(qty, qty_step),
                      "Market", reduce_only)],
        "signal_time": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                     time.gmtime(time.time() - 1)),
        "created_at": now_iso(),
        "valid_until": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                     time.gmtime(time.time() + 60)),
        "decision_horizon_ms": 15000,
        "expected_hold_ms": 60000,
        "market_snapshot_ref": "bs1-basis-snapshots",
        "epoch_id": DAEMON_EPOCH_ID,
    }
    r = tcc.intent(DAEMON_SOCK, intent, token)
    gws = r.get("gateway_status")
    return gws == "ACCEPTED", r


# --------------------------------------------------------------- исполнения выхода

def exit_fills(con, pos):
    """Агрегат исполнений выхода: (q_s, fee_s, q_f, fee_f).

    D-3: выход идёт группой демона — филлы атрибутируются по eg-префиксу
    exit-групп из bs2_exit_intents (orderLinkId = eg:<gid>:l|u<n>:<a>).
    Прямых bs2xs-/bs2xf-ордеров бот больше не шлёт; legacy-строки
    (до D-3) по-прежнему читаются по reason bs2-exit-* (старые позы)."""
    prefixes = tuple(f"eg:{r['eg_gid']}:" for r in con.execute(
        "SELECT eg_gid FROM bs2_exit_intents WHERE position_id=?",
        (pos["id"],)).fetchall())
    legacy_s = tuple(r[0] for r in con.execute(
        """SELECT o.orderLinkId FROM orders o WHERE o.strategy=? AND o.symbol=?
           AND o.side='Sell' AND o.reason LIKE 'bs2-exit-spot%'
           AND o.orderLinkId IS NOT NULL""",
        (STRATEGY, pos["symbol_spot"])).fetchall())
    legacy_f = tuple(r[0] for r in con.execute(
        """SELECT o.orderLinkId FROM orders o WHERE o.strategy=? AND o.symbol=?
           AND o.side='Buy' AND o.reason LIKE 'bs2-exit-fut%'
           AND o.orderLinkId IS NOT NULL""",
        (STRATEGY, pos["symbol_fut"])).fetchall())

    def _agg(sym, want_lids, want_side):
        """Signed side-aware: q = Σ(+qty стороны выхода) − Σ(qty обратной
        стороны). Обратный филл — unwind-возврат компенсации группы
        (spot Buy после Sell / fut Sell после Buy) ОТКАТЫВАЕТ исполнение
        выхода к нулю — иначе бот сочтёт ногу закрытой и недозакроет её.
        Комиссия платится всегда (по модулю)."""
        if not want_lids:
            return 0.0, 0.0
        other = "Buy" if want_side == "Sell" else "Sell"
        q = fee = 0.0
        for lid, qty, f, side in con.execute(
                """SELECT orderLinkId, exec_qty, exec_fee, side
                   FROM executions WHERE symbol=?""", (sym,)):
            if not (lid and lid.startswith(want_lids)):
                continue
            if (side or "") == want_side:
                q += abs(qty); fee += abs(f)
            elif (side or "") == other:
                q -= abs(qty); fee += abs(f)
        return q, fee

    q_s, fee_s = _agg(pos["symbol_spot"], prefixes + legacy_s, "Sell")
    q_f, fee_f = _agg(pos["symbol_fut"], prefixes + legacy_f, "Buy")
    return q_s, fee_s, q_f, fee_f


def send_exits(con, run_id, pos, reason):
    """Закрыть ОБЕ ноги группой демона (D-3): intent purpose=CLOSE со
    spot Sell + linear Buy reduce-only. Прямых ордеров НЕТ — недоступен
    демон / reject = ретрай на следующем тике (Guardian держит экстренное
    закрытие отдельно). Идемпотентно: qty = недостающее после фактических
    филлов (exit_fills по eg-префиксам)."""
    sym_fut, spot_sym = pos["symbol_fut"], pos["symbol_spot"]
    q_s_done, _, q_f_done, _ = exit_fills(con, pos)
    inst_s = get_instrument("spot", spot_sym)
    inst_f = get_instrument("linear", sym_fut)
    need_s = spot_sellable(pos["spot_entry_qty"], (inst_s or {}).get("qty_step", 0)) - q_s_done
    need_f = pos["fut_entry_qty"] - q_f_done
    if inst_f:
        need_f = step_floor(need_f, inst_f["qty_step"])

    min_s = (pos["spot_entry_qty"] or 0) * 0.05
    min_f = (pos["fut_entry_qty"] or 0) * 0.05
    if need_s < min_s and need_f < min_f:
        con.execute("""UPDATE basis_positions SET status='closing',
                       close_reason=COALESCE(close_reason,?),
                       closed_at=COALESCE(closed_at,?) WHERE id=?""",
                    (reason, now_iso(), pos["id"]))
        con.commit()
        return True

    if not DAEMON_SOCK:
        blog(run_id, "ERROR", "exit",
             f"демон недоступен (нет TRADE_CORE_DAEMON_SOCK) — выход "
             f"{reason} отложен, повтор на следующем тике", "")
        return False

    ok, det = daemon_group_exit(con, run_id, pos, reason, max(need_s, 0.0),
                                max(need_f, 0.0), inst_s, inst_f)
    if not ok:
        blog(run_id, "ERROR", "exit",
             f"группа-выход не доведена ({det}) — повтор на тике ({reason})")
        return False
    sync_execs_into_executions(con, spot_sym, sym_fut)
    con.execute("""UPDATE basis_positions SET status='closing',
                   close_reason=COALESCE(close_reason,?),
                   closed_at=COALESCE(closed_at,?) WHERE id=?""",
                (reason, now_iso(), pos["id"]))
    con.commit()
    return True


def settle_pair(con, run_id, pos):
    """Пара в 'closing': собрать исполнения выхода, посчитать, финализировать.
    True — финализирована; False — исполнения неполны, повтор на следующем тике.
    D-3: исполнения атрибутируются по eg-префиксам exit-групп (exit_fills)."""
    pid = pos["id"]
    sym_fut, spot_sym = pos["symbol_fut"], pos["symbol_spot"]
    q_s_done, fee_s_out, q_f_done, fee_f_out = exit_fills(con, pos)

    # px-агрегация по тем же префиксам (для pnl)
    prefixes = tuple(f"eg:{r['eg_gid']}:" for r in con.execute(
        "SELECT eg_gid FROM bs2_exit_intents WHERE position_id=?",
        (pid,)).fetchall())
    legacy_s = tuple(r[0] for r in con.execute(
        """SELECT o.orderLinkId FROM orders o WHERE o.strategy=? AND o.symbol=?
           AND o.side='Sell' AND o.reason LIKE 'bs2-exit-spot%'
           AND o.orderLinkId IS NOT NULL""",
        (STRATEGY, spot_sym)).fetchall())
    legacy_f = tuple(r[0] for r in con.execute(
        """SELECT o.orderLinkId FROM orders o WHERE o.strategy=? AND o.symbol=?
           AND o.side='Buy' AND o.reason LIKE 'bs2-exit-fut%'
           AND o.orderLinkId IS NOT NULL""",
        (STRATEGY, sym_fut)).fetchall())

    def _px_agg(sym, want_lids, want_side):
        """Signed (как exit_fills): обратный unwind-филл вычитается —
        средняя цена строится только по чистому исполнению выхода."""
        other = "Buy" if want_side == "Sell" else "Sell"
        q = vsum = 0.0
        for lid, qty, f, px, side in con.execute(
                """SELECT orderLinkId, exec_qty, exec_fee, exec_price, side
                   FROM executions WHERE symbol=?""", (sym,)):
            if not (lid and lid.startswith(want_lids)):
                continue
            if (side or "") == want_side:
                q += abs(qty); vsum += abs(qty) * px
            elif (side or "") == other:
                q -= abs(qty)
        return q, (vsum / q if q > 0 else 0.0)

    q_s, px_s = _px_agg(spot_sym, prefixes + legacy_s, "Sell")
    q_f, px_f = _px_agg(sym_fut, prefixes + legacy_f, "Buy")
    if q_s < (pos["spot_entry_qty"] or 0) * 0.95 or q_f < (pos["fut_entry_qty"] or 0) * 0.95:
        return False

    pnl_spot = (px_s - pos["spot_entry_price"]) * q_s - (pos["spot_entry_fee"] or 0) - fee_s_out
    pnl_fut = (pos["fut_entry_price"] - px_f) * q_f - (pos["fut_entry_fee"] or 0) - fee_f_out
    pair = pnl_spot + pnl_fut

    snap = con.execute(
        """SELECT basis_annual_bps FROM basis_snapshots WHERE symbol_fut=?
           ORDER BY id DESC LIMIT 1""", (sym_fut,)).fetchone()
    basis_exit = snap[0] if snap and snap[0] is not None else None

    for leg, side, qty, px, fee, pnl in (("spot", "Sell", q_s, px_s, fee_s_out, pnl_spot),
                                         ("fut", "Buy", q_f, px_f, fee_f_out, pnl_fut)):
        con.execute("""INSERT INTO basis_trades
            (position_id, ts, leg, side, qty, price, exec_price, fee, basis_bps, pnl_gross, reason)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (pid, now_iso(), leg, side, round(qty, 10), None, round(px, 10),
             round(fee, 10), basis_exit, round(pnl, 8), f"bs2-exit ({pos['close_reason']})"))
    con.execute("""INSERT INTO basis_trades
        (position_id, ts, leg, qty, basis_bps, pnl_net, reason)
        VALUES (?,?,?,?,?,?,?)""",
        (pid, now_iso(), "pair", None, basis_exit, round(pair, 8),
         f"pair close: {pos['close_reason']}; pnl_spot={pnl_spot:+.4f} pnl_fut={pnl_fut:+.4f}"))
    con.execute("""UPDATE basis_positions SET status='closed', closed_at=?, basis_at_exit=?,
                   pair_pnl_net=? WHERE id=?""",
                (now_iso(), basis_exit, round(pair, 8), pid))
    con.commit()
    bal = bank_apply_pnl(con, pair,
                          f"basis-bot пара {sym_fut} закрыта ({pos['close_reason']}), pos {pid}")
    blog(run_id, "SESSION", "exit",
         f"ВЫХОД BS-2 ({pos['close_reason']}): {sym_fut} | спот {pnl_spot:+.4f}$ + фьюч {pnl_fut:+.4f}$ "
         f"= {pair:+.4f}$ | банк basis-bot {bal:.2f}$",
         f"вход {pos['basis_at_entry'] and round(pos['basis_at_entry'],1)} bps → выход "
         f"{basis_exit and round(basis_exit,1)} bps; pos {pid}")
    print(f"BS-2 ВЫХОД ({pos['close_reason']}): {sym_fut} | спот {pnl_spot:+.4f}$ + "
          f"фьюч {pnl_fut:+.4f}$ = {pair:+.4f}$ | банк basis-bot {bal:.2f}$")
    return True


def sync_execs_into_executions(con, spot_sym, sym_fut):
    """Затянуть исполнения обеих ног в таблицу executions (settle читает оттуда)."""
    for category, sym in (("spot", spot_sym), ("linear", sym_fut)):
        d = net_get("/v5/execution/list", {"category": category, "symbol": sym, "limit": 50})
        lst = d.get("result", {}).get("list") if d.get("retCode") == 0 else None
        if not lst:
            continue
        for e in lst:
            eid = e.get("execId") or f"{e.get('orderId')}-{e.get('execTime')}-{e.get('execQty')}"
            con.execute("""INSERT OR IGNORE INTO executions
                (exec_id, order_id, symbol, side, exec_time, exec_price, exec_qty, exec_fee, is_maker, orderLinkId)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (eid, e.get("orderId"), e.get("symbol"), e.get("side"),
                 datetime.datetime.fromtimestamp(int(e.get("execTime", 0)) / 1000,
                                                 datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                 float(e.get("execPrice", 0) or 0), float(e.get("execQty", 0) or 0),
                 float(e.get("execFee", 0) or 0), 1 if e.get("isMaker") else 0,
                 e.get("orderLinkId", "")))
        con.commit()


# --------------------------------------------------------------- вход и P0-откат

def do_entry(con, run_id, basis, sym_fut, snap_id, days):
    spot_sym = sym_fut.split("USDT-")[0] + "USDT"
    inst_s = get_instrument("spot", spot_sym)
    inst_f = get_instrument("linear", sym_fut)
    if not inst_s or not inst_f or not inst_f.get("delivery_ms"):
        blog(run_id, "ERROR", "entry", f"инструменты недоступны: {spot_sym}/{sym_fut}")
        return None
    tk_s = get_ticker("spot", spot_sym)
    tk_f = get_ticker("linear", sym_fut)
    if not tk_s or not tk_f:
        blog(run_id, "ERROR", "entry", "тикеры недоступны")
        return None
    lb = live_basis_bps(tk_f["bid"], tk_s["ask"], days)
    if lb is None or lb < LIVE_GUARD_FRAC * THRESHOLD_BPS:
        blog(run_id, "WARN", "entry",
             f"живой базис {lb and round(lb)} bps < {LIVE_GUARD_FRAC * THRESHOLD_BPS:.0f} — "
             f"окно схлопнулось, вход отменён",
             f"сигнал {round(basis)} bps по снапшоту {snap_id}")
        return None

    # ФЬЮЧ-нога: qty в монетах как раньше (round_leg)
    q_fut = round_leg(tk_f["bid"], inst_f["qty_step"], inst_f["min_qty"])
    if not q_fut:
        blog(run_id, "WARN", "entry",
             f"фьюч-номинал не собирается: fut_qty={q_fut} (bid={tk_f['bid']})")
        return None

    # D-3 шаг 3: вход ВСЕГДА через демона группой (2 ноги). Прямого пути
    # НЕТ: нет сокета / transport-сбой / reject → входа нет (fail-closed),
    # ERROR-лог + повтор на следующем тике. Kill между intent и регистрацией
    # пары закрывает pending-механика bs2_open_intents на следующем тике.
    if not DAEMON_SOCK:
        blog(run_id, "ERROR", "entry",
             "демон недоступен (нет TRADE_CORE_DAEMON_SOCK) — вход "
             f"{sym_fut} отложен (fail-closed), повтор на следующем тике", "")
        return None
    try:
        ok, det = daemon_group_entry(con, run_id, sym_fut, spot_sym, q_fut,
                                     inst_f, basis, snap_id, days,
                                     spot_px=tk_s["ask"], fut_px=tk_f["bid"])
    except DaemonDown as e:
        ok, det = False, f"демон недоступен: {e}"
    except Exception as e:  # защитный контур: неожиданная ошибка адаптера
        ok, det = False, f"адаптер входа: {type(e).__name__}: {e}"
    if not ok or not isinstance(det, dict):
        blog(run_id, "ERROR", "entry",
             f"DAEMON группа-вход не доведён ({det}) — вход НЕ отправлен "
             f"(fail-closed), ждём следующего тика", "")
        return None
    blog(run_id, "SESSION", "entry",
         f"DAEMON группа-вход зарегистрирован: pid={det['pid']} "
         f"gid={det['gid']}", f"intent={det['intent_id']}")
    return det["pid"]


def _register_pair(con, run_id, sym_fut, spot_sym, snap_id, basis, days,
                   fills_s, filled_s, fills_f, filled_f, intent_id=None):
    """Идемпотентно по eg_gid в note (kill между INSERT пары и резолвом
    intent не даёт второй пары: повторный вызов возвращает тот же pid)."""
    if intent_id:
        gid = eg_gid_of(intent_id)
        row = con.execute(
            "SELECT id FROM basis_positions WHERE note LIKE ? AND status='open'",
            (f"%{gid}%",)).fetchone()
        if row:
            blog(run_id, "WARN", "entry",
                 f"пара по группе {gid} уже зарегистрирована (pos {row[0]}) — "
                 f"повторный вызов проигнорирован", "")
            return row[0]
    px_s = sum(f["price"] * f["qty"] for f in fills_s) / filled_s
    fee_s = sum(f["fee"] for f in fills_s)
    px_f = sum(f["price"] * f["qty"] for f in fills_f) / filled_f
    fee_f = sum(f["fee"] for f in fills_f)
    basis_exec = live_basis_bps(px_f, px_s, days) or basis
    note = f"вход по 3-тиковому событию BS-1"
    if intent_id:
        note += f"; intent={intent_id}; gid={eg_gid_of(intent_id)}"
    cur = con.execute("""INSERT INTO basis_positions
        (opened_at, symbol_fut, symbol_spot, status, entry_snapshot_id, basis_at_entry,
         exit_threshold_bps, spot_entry_qty, spot_entry_price, spot_entry_fee,
         fut_entry_qty, fut_entry_price, fut_entry_fee, run_id, note)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (now_iso(), sym_fut, spot_sym, "open", snap_id, round(basis_exec, 2),
         round(EXIT_FRAC * basis_exec, 2),
         filled_s, round(px_s, 10), round(fee_s, 10),
         filled_f, round(px_f, 10), round(fee_f, 10),
         run_id, note))
    con.commit()
    pid = cur.lastrowid
    sync_execs_into_executions(con, spot_sym, sym_fut)  # входные филлы — в executions
    for leg, side, qty, px, fee in (("spot", "Buy", filled_s, px_s, fee_s),
                                    ("fut", "Sell", filled_f, px_f, fee_f)):
        con.execute("""INSERT INTO basis_trades
            (position_id, ts, leg, side, qty, price, exec_price, fee, basis_bps, reason)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (pid, now_iso(), leg, side, round(qty, 10), None, round(px, 10),
             round(fee, 10), round(basis_exec, 2), "bs2-entry"))
    con.commit()
    blog(run_id, "SESSION", "entry",
         f"ВХОД BS-2: {sym_fut} базис {basis_exec/100:.2f}% год "
         f"(выход ≤{EXIT_FRAC*basis_exec/100:.2f}%) | спот BUY {filled_s:g} {spot_sym} @ {px_s:.6g} "
         f"+ фьюч SELL {filled_f:g} @ {px_f:.6g}",
         f"ноги ~${filled_s*px_s:.2f}+${filled_f*px_f:.2f}, snap_id={snap_id}, "
         f"банк {bank_balance(con):.2f}$")
    print(f"BS-2 ВХОД: {sym_fut} | базис {basis_exec/100:.2f}% год, выход при "
          f"≤{EXIT_FRAC*basis_exec/100:.2f}% | спот BUY {filled_s:g}@{px_s:.6g} + "
          f"фьюч SELL {filled_f:g}@{px_f:.6g} | банк basis-bot {bank_balance(con):.2f}$")
    return pid


def _flatten_one_leg(con, run_id, category, sym, side, qty, qty_step, reduce_only, tag):
    """[DIAGNOSTIC-ONLY, D-3] прямой market-ордер — в путях исполнения НЕ
    вызывается (остался только внутри incident_rollback, см. ниже).
    Возвращает чистый результат ноги или None."""
    for attempt in range(2):
        ts = int(time.time() * 1000)
        link = f"{tag}-{ts}-{attempt}"
        r = market_order(category, sym, side, fmt_qty(qty, qty_step), reduce_only, link)
        if r.get("retCode") != 0:
            blog(run_id, "ERROR", "rollback",
                 f"flatten {sym} отклонён (попытка {attempt+1}): {r.get('retCode')} {r.get('retMsg')}")
            time.sleep(1.0)
            continue
        journal_order(con, r, sym, category, side, qty, 0.0, f"bs2-rollback-{tag}", link)
        time.sleep(2.0)
        fills = execs_by_link(category, sym, link)
        if fills:
            q = abs(sum(f["qty"] for f in fills))
            px = sum(f["price"] * f["qty"] for f in fills) / q
            fee = sum(f["fee"] for f in fills)
            return {"qty": q, "px": px, "fee": fee}
        blog(run_id, "ERROR", "rollback", f"flatten {sym}: исполнений нет (попытка {attempt+1})")
        time.sleep(1.0)
    return None


def incident_rollback(con, run_id, spot_sym, sym_fut, link_s, link_f,
                      filled_s, fills_s, filled_f, fills_f, inst_s, inst_f, rejects=""):
    """[DIAGNOSTIC-ONLY, D-3] одноногочный flatten прямыми ордерами —
    вызывающих в путях исполнения НЕТ (откат входа делает демон группой
    COMPENSATING/BALANCED_CLOSED; откат выхода — повтор exit-группы).
    Оставлен для ручной диагностики живых односторонних поз (P0-карточка §4)."""
    cost = 0.0
    detail_legs = []
    if filled_s > 0:
        px_in = sum(f["price"] * f["qty"] for f in fills_s) / filled_s
        fee_in = sum(f["fee"] for f in fills_s)
        out = _flatten_one_leg(con, run_id, "spot", spot_sym, "Sell",
                               spot_sellable(filled_s, (inst_s or {}).get("qty_step", 0)),
                               (inst_s or {}).get("qty_step", 0), False, "rs")
        if out:
            cost += (out["px"] - px_in) * out["qty"] - fee_in - out["fee"]
            detail_legs.append(f"spot: in {px_in:.6g} out {out['px']:.6g}")
    if filled_f > 0:
        px_in = sum(f["price"] * f["qty"] for f in fills_f) / filled_f
        fee_in = sum(f["fee"] for f in fills_f)
        out = _flatten_one_leg(con, run_id, "linear", sym_fut, "Buy", filled_f,
                               (inst_f or {}).get("qty_step", 0), True, "rf")
        if out:
            cost += (px_in - out["px"]) * out["qty"] - fee_in - out["fee"]
            detail_legs.append(f"fut: in {px_in:.6g} out {out['px']:.6g}")
    cur = con.execute("""INSERT INTO basis_positions
        (opened_at, symbol_fut, symbol_spot, status, basis_at_entry, exit_threshold_bps,
         spot_entry_qty, spot_entry_price, fut_entry_qty, fut_entry_price, closed_at,
         close_reason, pair_pnl_net, run_id, note)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (now_iso(), sym_fut, spot_sym, "rollback", None, 0.0,
         filled_s or None, None, filled_f or None, None, now_iso(),
         "one_leg_rollback", round(cost, 8), run_id,
         f"инцидент одноногочного входа; {'; '.join(detail_legs)}; rejects: {rejects[:300]}"))
    con.commit()
    pid = cur.lastrowid
    con.execute("""INSERT INTO basis_trades
        (position_id, ts, leg, side, qty, price, exec_price, fee, pnl_net, reason)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (pid, now_iso(), "incident", None, None, None, None, None, round(cost, 8),
         "bs2-rollback: плата за инцидент (комиссии/спред)"))
    con.commit()
    bal = bank_apply_pnl(con, cost, f"basis-bot инцидент одноногочного входа {sym_fut} (pos {pid})")
    blog(run_id, "ERROR", "rollback",
         f"ИНЦИДЕНТ BS-2: исполнена одна нога ({'spot' if filled_s else 'fut'}) по {sym_fut} — "
         f"второй нет, исполненная закрыта немедленно, стоимость инцидента {cost:+.4f}$ "
         f"| банк {bal:.2f}$", rejects)
    print(f"BS-2 ИНЦИДЕНТ: одноногочный вход {sym_fut}, откат выполнен, "
          f"стоимость {cost:+.4f}$ — детали bot_log run={run_id}")


# --------------------------------------------------------------- тик

def tick(db_path=DB_PATH, run_id=None, verbose=False):
    run_id = run_id or format(int(time.time() * 1000) % 10**10, "x")
    con = sqlite3.connect(db_path, timeout=DB_TIMEOUT)
    con.row_factory = sqlite3.Row
    try:
        ensure_tables(con)
        if not acquire_lease(con, run_id):
            blog(run_id, "WARN", "tick", "lease занят — предыдущий тик ещё жив, пропускаю")
            return 0
        try:
            if ensure_deposit(con):
                blog(run_id, "SESSION", "bank",
                     f"ДЕПОЗИТ BS-2: ${BANK_START:.2f} на счёт basis-bot (точка разреза, файл 28 §5)")
                print(f"BS-2 ВКЛЮЧЁН: депозит ${BANK_START:.2f} на виртуальный счёт basis-bot")
            return _tick_inner(con, run_id, verbose)
        finally:
            release_lease(con, run_id)
    finally:
        con.close()


def _tick_inner(con, run_id, verbose):
    # 0) D-3: pending-входы (kill/обрыв между intent и регистрацией) —
    # сверка с биржей по eg-префиксу ДО всего остального
    resolve_pending_opens(con, run_id)
    # 1) добить незавершённые выходы: sync исполнений -> settle; недостающее — дозаказать
    for pos in con.execute(
            "SELECT * FROM basis_positions WHERE status='closing' ORDER BY id").fetchall():
        sync_execs_into_executions(con, pos["symbol_spot"], pos["symbol_fut"])
        if settle_pair(con, run_id, pos):
            continue
        blog(run_id, "WARN", "exit",
             f"пара {pos['id']} {pos['symbol_fut']}: исполнения выхода неполны — дозаказываю остаток")
        send_exits(con, run_id, pos, "retry-exit")
        sync_execs_into_executions(con, pos["symbol_spot"], pos["symbol_fut"])
        pos2 = con.execute("SELECT * FROM basis_positions WHERE id=?", (pos["id"],)).fetchone()
        settle_pair(con, run_id, pos2)

    pos = con.execute(
        "SELECT * FROM basis_positions WHERE status='open' ORDER BY id DESC LIMIT 1").fetchone()

    # 2) управление открытой парой: выходы; сеть — только на живые метки
    if pos:
        snap = con.execute(
            """SELECT ts, basis_annual_bps FROM basis_snapshots WHERE symbol_fut=?
               ORDER BY id DESC LIMIT 1""", (pos["symbol_fut"],)).fetchone()
        marks = None
        tk_s = get_ticker("spot", pos["symbol_spot"])
        tk_f = get_ticker("linear", pos["symbol_fut"])
        if tk_s and tk_f:
            marks = (tk_s["bid"], tk_f["ask"])
        inst = get_instrument("linear", pos["symbol_fut"])
        reason = exit_reason(pos, snap, marks, inst and inst.get("delivery_ms"))
        if reason:
            blog(run_id, "INFO", "exit",
                 f"сработал выход: {reason} (пара {pos['id']} {pos['symbol_fut']})")
            send_exits(con, run_id, pos, reason)
            sync_execs_into_executions(con, pos["symbol_spot"], pos["symbol_fut"])
            pos2 = con.execute("SELECT * FROM basis_positions WHERE id=?", (pos["id"],)).fetchone()
            if pos2["status"] == "closing":
                settle_pair(con, run_id, pos2)
        else:
            blog(run_id, "INFO", "tick",
                 f"пара {pos['id']} {pos['symbol_fut']} жива: базис "
                 f"{snap and snap[1] and round(snap[1])} bps / выход ≤{pos['exit_threshold_bps']:.0f} bps")
        return 0

    # 3) поиск события входа (решение — только чтение БД; сеть — при самом входе)
    cands = event_candidates(con)
    if not cands:
        blog(run_id, "INFO", "tick", "событий нет — тишина")
        return 0
    ok, why = entry_precheck(con)
    basis, sym_fut, snap_id, days = cands[0]
    if not ok:
        blog(run_id, "INFO", "tick",
             f"событие {sym_fut} {round(basis)} bps есть, вход заблокирован: {why}")
        return 0
    blog(run_id, "INFO", "entry",
         f"событие подтверждено: {sym_fut} {basis:.0f} bps ×{CONFIRM_TICKS} тика, {why}")
    do_entry(con, run_id, basis, sym_fut, snap_id, days)
    return 0


def main():
    ap = argparse.ArgumentParser(description="BS-2 базис-бот (одиночный тик)")
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    return tick(db_path=args.db, verbose=args.verbose)


if __name__ == "__main__":
    sys.exit(main())
