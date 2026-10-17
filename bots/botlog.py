#!/usr/bin/env python3
"""Журнал событий бота: таблица bot_log в trading.db.

Создан 17.09 после инцидента 16.09 ~21:38 (упавшая тест-сессия; разбор занял ~40 мин,
потому что важные события уходили в никуда). Документация: 07-логирование-бота.md.

Принципы:
  - пишем только ВАЖНОЕ: переходы состояний, ошибки, решения — не каждый полл
  - каждая строка знает run_id → вся сессия восстанавливается одним запросом
  - CLI для быстрого разбора: python3 botlog.py [--run RUN_ID] [--hours N]

Уровни: INFO (шаг протокола) / WARN (отклонение) / ERROR (сбой действия)
        / CRASH (сессия упала, с traceback) / SESSION (старт/финиш с итогами).
Фазы: start / flatten / entry / fill / tp / exit / api_err / interrupt / crash / finish.
"""
import os, sqlite3, sys, datetime, argparse

DIR = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(DIR, "..", "trading.db")

DDL = """
CREATE TABLE IF NOT EXISTS bot_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    run_id TEXT,
    level TEXT NOT NULL,
    phase TEXT,
    msg TEXT,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS ix_botlog_run ON bot_log(run_id, id);
CREATE INDEX IF NOT EXISTS ix_botlog_level_ts ON bot_log(level, ts);
"""

def ensure_table(con=None):
    """Создать bot_log, если её нет. Идемпотентно (вызывается и из init_db.py)."""
    own = con is None
    con = con or sqlite3.connect(DB)
    con.executescript(DDL)
    con.commit()
    if own:
        con.close()

class BotLog:
    """Пишет события в bot_log. run_id присваивается при старте сессии бота."""
    def __init__(self, run_id=None):
        ensure_table()
        self.run_id = run_id

    def _write(self, level, phase, msg, detail=None):
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            con = sqlite3.connect(DB, timeout=5)
            con.execute("INSERT INTO bot_log (ts, run_id, level, phase, msg, detail) VALUES (?,?,?,?,?,?)",
                        (ts, self.run_id, level, phase, str(msg)[:500], (str(detail)[:2000] if detail else None)))
            con.commit()
            con.close()
        except Exception as e:
            print(f"[{ts}] BOTLOG-FAIL {e}", file=sys.stderr)

    def info(self, phase, msg, detail=None):    self._write("INFO", phase, msg, detail)
    def warn(self, phase, msg, detail=None):    self._write("WARN", phase, msg, detail)
    def error(self, phase, msg, detail=None):   self._write("ERROR", phase, msg, detail)
    def session(self, phase, msg, detail=None): self._write("SESSION", phase, msg, detail)
    def crash(self, msg, detail=None):          self._write("CRASH", "crash", msg, detail)

def _ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def cli():
    ap = argparse.ArgumentParser(description="Сводка журнала бота для быстрого разбора")
    ap.add_argument("--run", help="полный таймлайн одной сессии (run_id)")
    ap.add_argument("--hours", type=int, default=24, help="окно сводки (по умолчанию 24)")
    ap.add_argument("--errors", type=int, default=6, help="сколько последних проблем показать")
    a = ap.parse_args()
    ensure_table()
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    now = datetime.datetime.now(datetime.timezone.utc)

    if a.run:
        # === полный таймлайн одной сессии ===
        r = con.execute("SELECT * FROM bot_runs WHERE run_id=?", (a.run,)).fetchone()
        if r:
            print(f"=== СЕССИЯ {a.run} ===")
            print(f"{r['strategy']} {r['started_at']}..{r['ended_at'] or 'НЕ ЗАВЕРШИЛАСЬ'} | "
                  f"ордеров {r['orders_placed']}/{r['orders_filled']}/{r['orders_cancelled']} (мести/исполнено/отменено) | "
                  f"PnL {r['pnl_realised'] if r['pnl_realised'] is not None else '—'}")
            print(f"notes: {r['notes'] or '—'}")
        rows = con.execute("SELECT * FROM bot_log WHERE run_id=? ORDER BY id", (a.run,)).fetchall()
        if not rows:
            print("событий в bot_log нет (сессия до 17.09 или лог не писался)")
        for w in rows:
            line = f"{w['ts'][11:19]} [{w['level']}] {w['phase']}: {w['msg']}"
            print(line)
            if w["detail"] and w["level"] in ("ERROR", "CRASH"):
                print("    " + w["detail"].replace("\n", "\n    ")[:600])
        deals = con.execute("SELECT ts, qty, avg_price, realised_pnl, note FROM position_events "
                            "WHERE note LIKE ? ORDER BY id", (f"%run={a.run}%",)).fetchall()
        if deals:
            print(f"--- сделки сессии ({len(deals)}) ---")
            for d in deals:
                pnl = d['realised_pnl'] if d['realised_pnl'] is not None else 0.0
                print(f"{d['ts'][11:19]} {d['qty']}@{d['avg_price']} PnL {pnl:+.4f}$ ({(d['note'] or '')[:40]})")
        return

    # === сводка за N часов ===
    since = _ts(now - datetime.timedelta(hours=a.hours))
    print(f"=== СЕССИИ БОТА ЗА {a.hours} ч (с {since}) ===")
    runs = con.execute("""
        SELECT r.run_id, r.started_at, r.ended_at, r.pnl_realised, r.strategy,
               SUM(CASE WHEN b.level='WARN' THEN 1 ELSE 0 END) w,
               SUM(CASE WHEN b.level='ERROR' THEN 1 ELSE 0 END) e,
               SUM(CASE WHEN b.level='CRASH' THEN 1 ELSE 0 END) c,
               COUNT(b.id) logged
        FROM bot_runs r LEFT JOIN bot_log b ON b.run_id = r.run_id
        WHERE r.started_at >= ?
        GROUP BY r.run_id ORDER BY r.started_at""", (since,)).fetchall()
    if not runs:
        print("сессий нет")
    for r in runs:
        pnl = f"{r['pnl_realised']:+.4f}$" if r["pnl_realised"] is not None else "—"
        span = f"{r['started_at'][11:16]}..{(r['ended_at'] or '')[11:16] or '???'}"
        if not r["logged"]:
            state = "(без лога — до 17.09)"
        else:
            flags = []
            if r["c"]: flags.append(f"CRASH={r['c']}")
            if r["e"]: flags.append(f"ERR={r['e']}")
            if r["w"]: flags.append(f"WARN={r['w']}")
            state = " ".join(flags) or "чисто"
        warn_mark = " ⚠️" if (r["c"] or r["e"]) else ""
        print(f"{r['run_id']} {r['started_at'][:10]} {span} | PnL {pnl} | {state}{warn_mark}")
    print()
    print("=== ПОСЛЕДНИЕ ПРОБЛЕМЫ (WARN/ERROR/CRASH) ===")
    probs = con.execute("""SELECT ts, run_id, level, phase, msg FROM bot_log
                           WHERE level IN ('WARN','ERROR','CRASH') AND ts >= ?
                           ORDER BY id DESC LIMIT ?""", (since, a.errors)).fetchall()
    if not probs:
        print("проблем нет")
    for p in probs:
        print(f"{p['ts']} [{p['level']}] run={p['run_id'] or '—'} {p['phase']}: {p['msg'][:90]}")
    print()
    print("детали сессии: python3 botlog.py --run <run_id> | сводка: python3 botlog.py --hours 48")

if __name__ == "__main__":
    cli()
