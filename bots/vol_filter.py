#!/usr/bin/env python3
"""vol_filter.py — риск-модуль VOL-1: часовой гейт + масштаб позиции от волы.

Карточка t_39829ff5 (решение Димы 19.09). Построен на единственной живой
структуре данных скрининга №22 (T4/T5): направление случайно, РАЗМАХ
предсказуем (кластеризация 20/20, суточный профиль 97% устойчив).

Две функции, ноль изменений торговой логики:
  hour_gate(symbol, ts_utc)   — открыт ли час для НОВЫХ входов (T5: пик 13-16 UTC,
                                тихие 04-09 UTC; токен-акции — американская сессия)
  size_scaler(symbol, recent_bars) — множитель 0.5..1.0 обратно пропорционально
                                текущей EWMA-воле (T4: следующий размах ~ текущего)

ВЫКЛЮЧАЕТСЯ одной переменной окружения VOL1_ENABLED=1 (в wrapper-скрипте крона).
Без неё обе функции нейтральны: гейт всегда открыт, масштаб всегда 1.0 —
боты работают на старой логике. Отдельные флаги: VOL1_GATE=0/VOL1_SCALE=0.

Эпоха v4-vol: переключение = правка wrapper'ов + миграция exsync (в этой карточке
НЕ делаем; см. 25-рисковый-модуль-вол.md §6).

Manifest (pre-registration, параметры зафиксированы до прогона; не подбирать):
  - торговые часы крипты: 12-17 UTC (плечи вокруг пика 13-16; файл 22 §6)
  - токен-акции (SOXL/SNDK/SPCX): 13-21 UTC (американская сессия)
  - шкала: EWMA lambda=0.94 (RiskMetrics), окно 96 баров (8ч), референс —
    медиана EWMA за 288 баров (24ч), scale = clamp(sqrt(ref/now), 0.5, 1.0)
    (квадратичный корень = вола в 2x режет размер в sqrt(2), в 4x — до пола 0.5)
Критерии успеха (заранее, проверка на ПРОШЛЫХ сделках closed_trades):
  - hit-rate сделок, открытых в пик 13-16 UTC, выше, чем в тихие 04-09 UTC
  - среднее время в позиции в тихие часы не меньше, чем в пик (тишина = вялое TP)
  - MaxDD гипотетического «пикового» потока не хуже полного

Использование:
  import vol_filter as vf
  ok, why = vf.hour_gate("ADAUSDT", ts)          # ts: datetime/ISO/unix/None=сейчас
  scale, why = vf.size_scaler("ADAUSDT", bars)   # bars: закрытия 5m, любые типы
  vf.enabled()                                   # VOL1_ENABLED=1 ?
"""
import datetime
import math
import os

MODULE_VERSION = "vol1-manifest-1"   # в reason строках ботов и в отчёте

# --- манифест-константы (pre-registration; менять только через новую версию модуля) ---
PEAK_HOURS_UTC = (13, 16)      # включительно (T5: пик |r|)
CRYPTO_TRADE_HOURS = tuple(range(12, 18))   # 12..17: пик + плечи
STOCK_TRADE_HOURS = tuple(range(13, 22))    # 13..21: амер. сессия токен-акций
# суффиксы токенизированных акций (файл 22: SOXL/SNDK/SPCX — пик в амер. сессию)
STOCK_SUFFIXES = ("SOXL", "SNDK", "SPCX")

EWMA_LAMBDA = 0.94     # RiskMetrics
SCALER_WINDOW = 96     # баров 5m = 8ч — окно оценки текущей волы
SCALER_REF_WINDOW = 288  # баров 5m = 24ч — референс (медиана EWMA)
SCALE_FLOOR, SCALE_CEIL = 0.5, 1.0
# экстремальная вола = отношение текущей EWMA к суточной медиане, при котором
# вход пропускается целиком (румка VOL1_SKIP_EXTREME, по умолчанию ВЫКЛ;
# включать на эпохе v4-vol вместе с гейтом)
EXTREME_SKIP_RATIO = 3.0


def enabled():
    """Модуль включён? По умолчанию ВЫКЛ (боты на старой логике)."""
    return os.environ.get("VOL1_ENABLED", "").strip() == "1"


def _flag(name):
    """Флаг с дефолтом ВКЛ (VOL1_GATE/VOL1_SCALE): всё, кроме '0'."""
    return os.environ.get(name, "").strip() != "0"


def _flag_optin(name):
    """Флаг с дефолтом ВЫКЛ (VOL1_SKIP_EXTREME): только явное '1'."""
    return os.environ.get(name, "").strip() == "1"


def gate_enabled():
    return enabled() and _flag("VOL1_GATE")


def scale_enabled():
    return enabled() and _flag("VOL1_SCALE")


# ---------------------------------------------------------------- время -> час UTC

def _to_utc_hour(ts_utc):
    """datetime / ISO-строка / unix-секунды / None(=сейчас) -> час UTC 0..23."""
    if ts_utc is None:
        return datetime.datetime.now(datetime.timezone.utc).hour
    if isinstance(ts_utc, datetime.datetime):
        dt = ts_utc
    elif isinstance(ts_utc, (int, float)):
        dt = datetime.datetime.fromtimestamp(ts_utc, datetime.timezone.utc)
    else:
        s = str(ts_utc).strip().replace("Z", "+00:00")
        dt = datetime.datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)  # наивное время считаем UTC
    return dt.astimezone(datetime.timezone.utc).hour


def trade_hours(symbol):
    """Торговые часы символа по манифесту (без проверки флага)."""
    sym = (symbol or "").upper()
    if any(sym.startswith(s) for s in STOCK_SUFFIXES):
        return STOCK_TRADE_HOURS
    return CRYPTO_TRADE_HOURS


def hour_gate(symbol, ts_utc=None):
    """Открыт ли час для НОВЫХ входов. Возвращает (open: bool, reason: str).

    Смысл (файл 22 §11 п.1): не котировать в тихие часы 04-09 UTC — там ниже
    плотность движений и TP исполняются вяло; котируем в часы пиковой |r|.
    Выходы (TP/SL/TIME/flatten) НЕ гейтятся — только новые входы.
    """
    h = _to_utc_hour(ts_utc)
    if not gate_enabled():
        return True, f"VOL1 off (hour={h:02d})"
    hours = trade_hours(symbol)
    lo, hi = PEAK_HOURS_UTC
    if h in hours:
        cls = "peak" if lo <= h <= hi else "shoulder"
        return True, f"VOL1 hour OPEN {cls} ({h:02d}UTC in {hours[0]:02d}-{hours[-1]:02d})"
    return False, f"VOL1 hour CLOSED quiet ({h:02d}UTC not in {hours[0]:02d}-{hours[-1]:02d})"


# ---------------------------------------------------------------- нормализация баров

def _closes_from(recent_bars):
    """Список закрытий (по возрастанию времени, если ts есть). Ест любые:
    числа, dict'и (close/c), кортежи (ts,o,h,l,c[,v]) — как от kline API."""
    closes, ts = [], []
    for b in recent_bars or []:
        if isinstance(b, (int, float)):
            closes.append(float(b)); ts.append(None)
        elif isinstance(b, dict):
            c = b.get("close", b.get("c"))
            if c is None:
                continue
            closes.append(float(c))
            t = b.get("ts", b.get("timestamp", b.get("start"))
                      if isinstance(b.get("ts", b.get("timestamp", b.get("start"))), (int, float))
                      else None)
            ts.append(t)
        elif isinstance(b, (tuple, list)) and len(b) >= 5:
            # kline API: [startMs, open, high, low, close, volume, turnover]
            ts.append(float(b[0]) if isinstance(b[0], (int, float)) else None)
            closes.append(float(b[4]))
    if any(t is not None for t in ts):
        pairs = sorted((t, c) for t, c in zip(ts, closes) if t is not None)
        if len(pairs) == len(closes):   # все с ts -> сортируем по времени
            closes = [c for _, c in pairs]
        # неполные ts: доверяем порядку вызова (старые -> новые)
    return closes


def _ewma_sigma(returns):
    """EWMA-σ лог-доходностей (RiskMetrics lambda)."""
    if not returns:
        return None
    lam, var = EWMA_LAMBDA, None
    for r in returns:
        var = r * r if var is None else lam * var + (1 - lam) * r * r
    return math.sqrt(var)


def _vol_ratio(recent_bars):
    """(ratio, now_sig, ref) текущей EWMA-волы к суточной медиане; None если не оценить."""
    closes = _closes_from(recent_bars)
    if len(closes) < 3:
        return None
    rets = [math.log(c2 / c1) for c1, c2 in zip(closes, closes[1:]) if c1 > 0 and c2 > 0]
    if len(rets) < 2:
        return None
    now_sig = _ewma_sigma(rets[-SCALER_WINDOW:])
    wins = []
    rwin = rets[-SCALER_REF_WINDOW:]
    for i in range(0, max(1, len(rwin) - 23), 24):
        s = _ewma_sigma(rwin[i:i + 24])
        if s:
            wins.append(s)
    if not now_sig or not wins or now_sig <= 0:
        return None
    wins.sort()
    mid = len(wins) // 2
    ref = wins[mid] if len(wins) % 2 else (wins[mid - 1] + wins[mid]) / 2
    if ref <= 0:
        return None
    return now_sig / ref, now_sig, ref


def size_scaler(symbol, recent_bars):
    """Множитель размера позиции 0.5..1.0 (без флага — 1.0).

    Логика (T4: следующий размах ~ текущему, ρ₁(r²)≈0.24): чем выше СЕЙЧАС
    EWMA-вола относительно собственной суточной медианы, тем меньше позиция.
    scale = clamp(sqrt(ref_median / ewma_now), 0.5, 1.0):
      вола = медиане  -> 1.0 (полный размер)
      вола 2x медианы -> ~0.71
      вола >= 4x      -> 0.5 (пол)
      вола ниже медианы -> 1.0 (потолок; не наращиваем сверх обычного)
    """
    if not scale_enabled():
        return 1.0, "VOL1 off"
    rr = _vol_ratio(recent_bars)
    if rr is None:
        return 1.0, "VOL1 scale neutral (нет оценки волы)"
    ratio = rr[0]
    raw = math.sqrt(1.0 / ratio)
    scale = max(SCALE_FLOOR, min(SCALE_CEIL, raw))
    return round(scale, 3), (f"VOL1 scale={scale:.3f} (вола x{ratio:.2f} от суточной медианы; "
                             f"sqrt-правило, пол {SCALE_FLOOR}-{SCALE_CEIL})")


def entry_check(symbol, ts_utc, recent_bars):
    """Оба фильтра одним вызовом (точка входа ботов). Возвращает
    (allowed: bool, scale: float, reason: str) — reason для reason-строки ордера.
    recent_bars может быть callable (лямбда с api.kline_closes) — вызывается
    ТОЛЬКО если гейт открыт и скейлер включён: при VOL1 off или закрытом часе
    сеть не трогается вовсе.
    Экстремальная вола (>= EXTREME_SKIP_RATIO к суточной медиане) при
    VOL1_SKIP_EXTREME=1 пропускает вход целиком — единственная реализуемая
    часть скейлера, пока позиции прижаты к биржевому минимуму $5."""
    ok, why_gate = hour_gate(symbol, ts_utc)
    if not ok:
        return False, 1.0, why_gate
    if not enabled():
        return True, 1.0, "VOL1 off"
    if not scale_enabled():
        return True, 1.0, f"{why_gate}; VOL1 off"
    bars = recent_bars() if callable(recent_bars) else recent_bars
    scale, why_scale = size_scaler(symbol, bars)
    if _flag_optin("VOL1_SKIP_EXTREME"):
        rr = _vol_ratio(bars)
        if rr is not None and rr[0] >= EXTREME_SKIP_RATIO:
            return False, 1.0, f"{why_gate}; {why_scale} — EXTREME (x{rr[0]:.1f}), вход пропущен"
    return True, scale, f"{why_gate}; {why_scale}"


if __name__ == "__main__":
    # smoke без сети: оба вызова нейтральны (VOL1_ENABLED не задан)
    print("enabled:", enabled())
    print("gate:", hour_gate("ADAUSDT"))
    print("scaler:", size_scaler("ADAUSDT", [1.0, 1.001, 0.999, 1.002, 1.0]))
