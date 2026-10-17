#!/usr/bin/env python3
"""position_state.py — машина состояний позиции + риск-гейт с атомарным резервом.

Пакет А, фиксы 4–6 (аудит №14 §4.4, 4.5, 4.7):
  Фикс 4 — состояния FLAT/ENTRY_PENDING/OPEN/EXIT_PENDING/UNKNOWN.
    Ack биржи (retCode==0) != исполнение: ack переводит в *_PENDING,
    подтверждение — только исполнение (executions/positions) + остаток.
    UNKNOWN запрещает входы до reconciliation.
  Фикс 5 — неизменяемый hold-deadline: время первого исполнения хранится в БД,
    ADOPT/рестарты его не сдвигают. Для неизвестного старта — из executions.
  Фикс 6 — risk gate: перед КАЖДЫМ входом свежий банк+дневной лимит из
    virtual_ledger; атомарное резервирование BEGIN IMMEDIATE; lease-строка в БД
    блокирует второй экземпляр той же стратегии/символа; двойной вход с общим
    остатком невозможен (reserved учитывается в доступном капитале).

Таблица strategy_leases (лёгкая, создаётся на первом использовании):
  strategy TEXT, symbol TEXT — уникальная пара; holder, expires_at, note.
"""
import datetime
import sqlite3
import time

FLAT = "FLAT"
ENTRY_PENDING = "ENTRY_PENDING"
OPEN = "OPEN"
EXIT_PENDING = "EXIT_PENDING"
UNKNOWN = "UNKNOWN"

LEASE_DDL = """
CREATE TABLE IF NOT EXISTS strategy_leases (
    strategy TEXT NOT NULL,
    symbol TEXT NOT NULL,
    holder TEXT,
    acquired_at TEXT,
    expires_at REAL NOT NULL,
    note TEXT,
    PRIMARY KEY (strategy, symbol)
);
"""

# Эпохи, чьи сделки меняют виртуальные банки (аудит №14 §4.3; split 17.09 вечер):
# epoch — исследовательская версия; принадлежность банку задаёт STRATEGY_ACCOUNTS.
# v1 (мёртвая) исключён: убыток уже перенесён однократной корректировкой 17.09.
# v4-vol (18.09 21:00Z, карточка t_c1464b6b): VOL-1 включён в прод; сделки bot-v1/v2
# после точки разреза — эпоха v4-vol, банк прежний (счёт по стратегии). Нужна здесь,
# иначе risk_gate/pnl_since_event перестанут видеть сделки ботов (epoch IN (...)).
LEDGER_EPOCHS = ("v2-hybrid", "v3-donchian", "pkgA-v2", "pkgA-v3", "v1-revival", "v4-vol",
                 "v2-ena")  # v2-ena: ДОМОРЩЁННАЯ (сделок нет, CUTOFF_V2ENA=None в exsync);
                 # добавлена заранее, иначе после flip risk_gate/дневной лимит перестанут
                 # видеть сделки bot-v2 (молчаливый отказ стопора); активация — только CUTOFF

# Раздельные виртуальные банки ботов (карточка t_776e9b14, решение Димы 17.09 ~21:15):
# каждый бот торгует от своего счёта; старт каждого = 19.91029153 (остаток общего
# банка в точке разреза). Атрибуция сделки -> счёт по ЕЁ стратегии (закрывающий ордер).
BANKS = ("v1-revival", "v2-hybrid", "v3-donchian")

# стратегия (closed_trades.strategy / STRATEGY_ID) -> виртуальный счёт
STRATEGY_ACCOUNTS = {
    "bot-v1": "v1-revival",
    "bot-v2": "v2-hybrid",
    "bot-v3": "v3-donchian",
}

BANK_START_BALANCE = 19.91029153  # старт раздельных банков (факт, точка разреза 17.09)


def utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------- состояния

def assess_state(side_filter, filled_entry_qty, remaining_position, exit_ack_pending):
    """Чистая функция переходов (тест на fake exchange).

    side_filter: 'Buy'/'Sell' — направление позиции бота.
    filled_entry_qty: суммарный исполненный объём входа (из executions/ордера).
    remaining_position: живой остаток на бирже (size из /position/list).
    exit_ack_pending: True, если выход отправлен, исполнения ещё не видели.

    Возвращает одно из FLAT/ENTRY_PENDING/OPEN/EXIT_PENDING/UNKNOWN.
    """
    if filled_entry_qty is None:
        filled_entry_qty = 0.0
    if remaining_position is None:
        remaining_position = 0.0
    if exit_ack_pending:
        if remaining_position <= 0 and filled_entry_qty > 0:
            return EXIT_PENDING  # выход мог исполниться, подтверждения нет
        if remaining_position > 0:
            return EXIT_PENDING  # выход отправлен, остаток ещё жив
        return UNKNOWN
    if remaining_position > 0:
        if filled_entry_qty > 0:
            return OPEN
        return UNKNOWN  # позиция на бирже есть, наших fills не видно
    if filled_entry_qty > 0 and remaining_position <= 0:
        return FLAT  # вход был исполнен, позиции больше нет
    return FLAT


def entries_allowed(state):
    return state == FLAT


def state_note(state):
    return {
        FLAT: "FLAT — входы разрешены",
        ENTRY_PENDING: "ENTRY_PENDING — вход отправлен, исполнения нет",
        OPEN: "OPEN — позиция жива",
        EXIT_PENDING: "EXIT_PENDING — выход отправлен, подтверждения нет",
        UNKNOWN: "UNKNOWN — reconciliation требуется, входы запрещены",
    }[state]


# ---------------------------------------------------------------- deadline

def load_or_init_deadline(db_con, strategy, symbol, first_fill_ts_epoch, max_hold_min):
    """Возвращает (deadline_epoch, source) для позиции стратегии/символа.

    Таблица position_hold: (strategy, symbol) PK, first_fill_at REAL, deadline REAL, updated_at.
    Если записи нет — создаётся от first_fill_ts_epoch (время ПЕРВОГО исполнения,
    не «сейчас» при ADOPT) и deadline больше не меняется. Для неизвестного старта
    вызывающий код восстанавливает first_fill из executions и передаёт сюда.
    """
    db_con.execute("""CREATE TABLE IF NOT EXISTS position_hold (
        strategy TEXT NOT NULL, symbol TEXT NOT NULL,
        first_fill_at REAL, deadline REAL, updated_at TEXT,
        PRIMARY KEY (strategy, symbol))""")
    row = db_con.execute("SELECT first_fill_at, deadline FROM position_hold WHERE strategy=? AND symbol=?",
                         (strategy, symbol)).fetchone()
    if row and row[1]:
        return row[1], "db"
    ff = float(first_fill_ts_epoch if first_fill_ts_epoch is not None else time.time())
    dl = ff + max_hold_min * 60
    db_con.execute("""INSERT OR REPLACE INTO position_hold (strategy, symbol, first_fill_at, deadline, updated_at)
        VALUES (?,?,?,?,?)""", (strategy, symbol, ff, dl, utc_now_iso()))
    db_con.commit()
    return dl, "init"


def clear_deadline(db_con, strategy, symbol):
    db_con.execute("DELETE FROM position_hold WHERE strategy=? AND symbol=?", (strategy, symbol))
    db_con.commit()


# ---------------------------------------------------------------- risk gate

def account_balance(db_con, account):
    """Баланс виртуального счёта: последняя проводка account; если счёт пуст —
    старт раздельного банка (BANK_START_BALANCE, точка разреза 17.09)."""
    row = db_con.execute("SELECT balance FROM virtual_ledger WHERE account=? ORDER BY id DESC LIMIT 1",
                         (account,)).fetchone()
    if row:
        return row[0]
    return BANK_START_BALANCE


def virtual_balance(db_con, account=None):
    if account:
        return account_balance(db_con, account)
    row = db_con.execute("SELECT balance FROM virtual_ledger ORDER BY id DESC LIMIT 1").fetchone()
    return row[0] if row else 20.0


def day_pnl(db_con, day_start_iso):
    row = db_con.execute(
        "SELECT COALESCE(SUM(amount),0) FROM virtual_ledger WHERE kind='trade_pnl' AND ts >= ?",
        (day_start_iso,)).fetchone()
    return row[0]


def utc_day_start_iso(now_epoch=None):
    dt = datetime.datetime.now(datetime.timezone.utc) if now_epoch is None \
        else datetime.datetime.fromtimestamp(now_epoch, datetime.timezone.utc)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def acquire_lease(db_con, strategy, symbol, holder, ttl_sec=1800):
    """Берёт lease на пару (strategy, symbol); True если удалось.

    Свободный/протухший lease перезаписывается атомарно (BEGIN IMMEDIATE).
    """
    db_con.execute(LEASE_DDL)
    db_con.commit()  # DDL применён; явная транзакция ниже (implicit-тx мешает BEGIN IMMEDIATE)
    now = time.time()
    try:
        db_con.execute("BEGIN IMMEDIATE")
        row = db_con.execute("SELECT holder, expires_at FROM strategy_leases WHERE strategy=? AND symbol=?",
                             (strategy, symbol)).fetchone()
        if row and row[1] > now and row[0] != holder:
            db_con.execute("ROLLBACK")
            return False
        db_con.execute("""INSERT OR REPLACE INTO strategy_leases
            (strategy, symbol, holder, acquired_at, expires_at, note)
            VALUES (?,?,?,?,?,?)""",
                       (strategy, symbol, holder, utc_now_iso(), now + ttl_sec, ""))
        db_con.commit()
        return True
    except sqlite3.OperationalError:
        try:
            db_con.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        return False


def release_lease(db_con, strategy, symbol, holder):
    db_con.execute("DELETE FROM strategy_leases WHERE strategy=? AND symbol=? AND holder=?",
                   (strategy, symbol, holder))
    db_con.commit()


def risk_gate(db_con, strategy, symbol, notional, daily_loss_limit, holder,
              ttl_sec=1800, reserve_notional=True):
    """Атомарная проверка+резерв перед входом.

    Возвращает (allowed, reason, ctx). В одной транзакции BEGIN IMMEDIATE:
      1) lease на (strategy, symbol) — второй экземпляр не пройдёт;
      2) свежий банк virtual_ledger СЧЁТА стратегии (split 17.09 вечер);
      3) дневной лимит (по closed_at дня события, по СЧЁТУ стратегии);
      4) зарезервированный капитал: сумма не-протухших strategy_reserves
         (в т.ч. ВРЕМЯ существования ENTRY_PENDING другой стратегии);
      5) доступный = банк − резервы; требуемый notional должен влезть.
    Резерв создаётся ТОЛЬКО при allowed и снимается при подтверждении
    исполнения (confirm_reserve) или отказе (release_reserve).
    """
    account = STRATEGY_ACCOUNTS.get(strategy)
    db_con.execute(LEASE_DDL)
    db_con.execute("""CREATE TABLE IF NOT EXISTS strategy_reserves (
        strategy TEXT NOT NULL, symbol TEXT NOT NULL,
        notional REAL NOT NULL, created_at TEXT, expires_at REAL,
        PRIMARY KEY (strategy, symbol))""")
    db_con.commit()  # DDL применён до BEGIN IMMEDIATE (иначе implicit-tx = busy)
    now = time.time()
    try:
        db_con.execute("BEGIN IMMEDIATE")
        # lease
        row = db_con.execute("SELECT holder, expires_at FROM strategy_leases WHERE strategy=? AND symbol=?",
                             (strategy, symbol)).fetchone()
        if row and row[1] > now and row[0] != holder:
            db_con.execute("ROLLBACK")
            return False, "RISK_BLOCK: lease занят другой сессией %s" % row[0], None
        # банк счёта стратегии
        if account:
            b = db_con.execute("SELECT balance FROM virtual_ledger WHERE account=? ORDER BY id DESC LIMIT 1",
                               (account,)).fetchone()
        else:
            b = db_con.execute("SELECT balance FROM virtual_ledger ORDER BY id DESC LIMIT 1").fetchone()
        bank = b[0] if b else BANK_START_BALANCE
        # дневной лимит — по дню БИРЖЕВОГО СОБЫТИЯ (closed_at; §4.7), по СЧЁТУ стратегии:
        # поздний sync вчерашней сделки не переносит убыток в сегодняшний лимит
        day_start = utc_day_start_iso()
        ph = ",".join("?" * len(LEDGER_EPOCHS))
        q = "SELECT COALESCE(SUM(net_pnl),0) FROM closed_trades WHERE closed_at >= ? AND epoch IN (%s)" % ph
        args = (day_start,) + tuple(LEDGER_EPOCHS)
        if account:
            q += " AND account=?"
            args += (account,)
        d = db_con.execute(q, args).fetchone()
        dpnl = d[0]
        if dpnl <= -daily_loss_limit:
            db_con.execute("ROLLBACK")
            return False, ("RISK_BLOCK: дневной лимит %.4f <= -%.4f (свежие данные счёта %s)"
                           % (dpnl, daily_loss_limit, account or "?")), None
        # резервы ТОЛЬКО своего счёта (банки раздельные): чужой банк наш резерв не касается
        acct_strats = [s for s, a in STRATEGY_ACCOUNTS.items() if account and a == account]
        if acct_strats:
            phs = ",".join("?" * len(acct_strats))
            res = db_con.execute("SELECT COALESCE(SUM(notional),0) FROM strategy_reserves "
                                 "WHERE expires_at > ? AND strategy IN (%s)" % phs,
                                 (now,) + tuple(acct_strats)).fetchone()
        else:
            res = db_con.execute("SELECT COALESCE(SUM(notional),0) FROM strategy_reserves "
                                 "WHERE expires_at > ?", (now,)).fetchone()
        reserved = res[0]
        available = bank - reserved
        if notional > available + 1e-9:
            db_con.execute("ROLLBACK")
            return False, ("RISK_BLOCK: свободного капитала %.4f < требуемого %.4f "
                           "(банк %.4f − зарезервировано %.4f)" % (available, notional, bank, reserved)), None
        if reserve_notional:
            db_con.execute("""INSERT OR REPLACE INTO strategy_reserves
                (strategy, symbol, notional, created_at, expires_at)
                VALUES (?,?,?,?,?)""", (strategy, symbol, notional, utc_now_iso(), now + 1800))
        db_con.execute("""INSERT OR REPLACE INTO strategy_leases
            (strategy, symbol, holder, acquired_at, expires_at, note)
            VALUES (?,?,?,?,?,?)""",
                       (strategy, symbol, holder, utc_now_iso(), now + ttl_sec, "risk_gate"))
        db_con.commit()
        return True, "OK: банк %s=%.4f, зарезервировано %.4f, доступно %.4f, day_pnl %+.4f" % (
            account or "общий", bank, reserved, available, dpnl), dict(bank=bank, reserved=reserved, available=available, day_pnl=dpnl)
    except sqlite3.OperationalError as e:
        try:
            db_con.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        return False, "RISK_BLOCK: sqlite busy (%s)" % e, None


def confirm_reserve(db_con, strategy, symbol):
    """Исполнение подтверждено — резерв больше не нужен (капитал уже в позиции)."""
    db_con.execute("DELETE FROM strategy_reserves WHERE strategy=? AND symbol=?", (strategy, symbol))
    db_con.commit()


def release_reserve(db_con, strategy, symbol):
    confirm_reserve(db_con, strategy, symbol)


# ---------------------------------------------------------------- meta

STRATEGY_VERSION = "pkgA-1.0"
