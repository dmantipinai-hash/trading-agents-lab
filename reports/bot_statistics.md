# Bot statistics — per-epoch aggregates (demo, anonymized)

Source: `closed_trades_anonymized.csv` (582 closed trades, 2026-09-16 → 2026-10-17 UTC).
Net PnL is after fees, USDT. Epoch = a distinct parameter/strategy generation; statistics never mix epochs.

## Per-bot summary (click a bot for its full trade list)

| bot | strategy | symbol(s) | trades | net PnL | winrate | report |
|---|---|---|---|---|---|---|
| **bot-v1** | lazy market-maker | ADAUSDT | 35 | **+0.24** | 57% | [bot-v1_trades.md](bot-v1_trades.md) |
| **bot-v2** | hybrid mean-reversion | ENA, DOGE | 518 | **−2.92** | 51% | [bot-v2_trades.md](bot-v2_trades.md) |
| **bot-v3** | Donchian breakout | XRPUSDT | 19+10 | **−0.10** | 41% | [bot-v3_trades.md](bot-v3_trades.md) |

bot-v3 trades come from two epochs on XRPUSDT (v1 = early Donchian class, v2-hybrid = interim
hybrid period on the same symbol); both are listed in its report.

## Per-epoch detail

| symbol | epoch | trades | avg PnL | net PnL | winrate |
|---|---|---|---|---|---|
| ADAUSDT | v1-revival | 35 | +0.01 | +0.24 | 57% |
| ADAUSDT | v2-hybrid | 91 | -0.01 | -0.74 | 41% |
| ADAUSDT | v4-vol | 180 | -0.01 | -0.91 | 41% |
| DOGEUSDT | v2-hybrid | 12 | +0.02 | +0.26 | 83% |
| DOGEUSDT | v4-vol | 107 | 0.00 | -0.28 | 63% |
| ENAUSDT | v2-ena | 1 | +0.06 | +0.06 | 100% |
| ENAUSDT | v2-hybrid | 127 | -0.01 | -1.31 | 58% |
| XRPUSDT | v1 | 19 | 0.00 | -0.07 | 42% |
| XRPUSDT | v2-hybrid | 10 | 0.00 | +0.05 | 40% |

Total: 582 trades, net ≈ **−2.70 USDT** across one month of cron-driven demo trading.

## Reading guide

- Exit reasons in the CSV: TP / SL / TIME / SESSION-END / FLATTEN / OTHER / UNKNOWN — all bot-side
  reasons; core-side rejection reasons are intentionally not part of the public data.
- Win rates swing 40–83% per epoch while net stays near zero: classic small-size demo profile
  where commissions dominate. The system's value is discipline (no position drift, no orphaned
  orders, clean epoch attribution), not alpha at this scale.
- Bots do not place orders themselves: every entry intent goes through the private Trade Core
  (see `core_interface/`). What you see here is the visible outcome, not the decisioning.
