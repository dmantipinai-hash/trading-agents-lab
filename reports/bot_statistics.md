# Bot statistics — per-epoch aggregates (demo, anonymized)

Source: `closed_trades_anonymized.csv` (851 closed trades, 2026-09-16 → 2026-10-30 UTC).
Net PnL is after fees, USDT. Epoch = a distinct parameter/strategy generation; statistics never mix epochs.

## Per-bot summary

| bot | strategy | trades | net PnL | winrate | report |
|---|---|---|---|---|---|
| **bot-v1** | lazy market-maker | 349 | **-1.5495** | 42% | [bot-v1_trades.md](bot-v1_trades.md) |
| **bot-v2** | hybrid mean-reversion | 256 | **-2.0955** | 59% | [bot-v2_trades.md](bot-v2_trades.md) |
| **bot-v3** | Donchian breakout | 7 | **-0.3452** | 29% | [bot-v3_trades.md](bot-v3_trades.md) |
| *unattributed* | settled outside bot sessions | 239 | *+2.0172* | 63% | — (in CSV) |

## Per-epoch detail

| symbol | epoch | trades | avg PnL | net PnL | winrate |
|---|---|---|---|---|---|
| XRPUSDT | v1 | 19 | -0.0039 | -0.0748 | 42% |
| XRPUSDT | v2-hybrid | 7 | +0.0491 | +0.3437 | 57% |
| DOGEUSDT | v2-hybrid | 12 | +0.0216 | +0.2592 | 83% |
| ADAUSDT | v1-revival | 35 | +0.0068 | +0.2391 | 57% |
| DOGEUSDT | v4-vol | 107 | -0.0026 | -0.2773 | 63% |
| ADAUSDT | v4-vol | 295 | -0.0058 | -1.7129 | 41% |
| ENAUSDT | v2-hybrid | 158 | +0.0113 | +1.7878 | 76% |
| ENAUSDT | v2-ena | 137 | -0.0153 | -2.1021 | 53% |
| XRPUSDT | v3-donchian | 7 | -0.0493 | -0.3452 | 29% |
| ADAUSDT | v2-hybrid | 74 | -0.0012 | -0.0904 | 36% |

Total: 851 trades, net ≈ **-1.97 USDT** across the live demo window.

## Reading guide

- Exit reasons in the CSV: TP / SL / TIME / SESSION-END / FLATTEN / OTHER / UNKNOWN — bot-side reasons.
- `unattributed` = trades settled outside bot sessions (core-side position management or manual/operator close). For those rows the exit reason reads UNKNOWN by design: core-side decision reasons are not part of the public data.
- Win rates swing widely per epoch while net stays near zero: classic small-size demo profile where commissions dominate. The value shown here is discipline (no position drift, no orphaned orders, clean epoch attribution), not alpha at this scale.
- Bots do not place orders themselves: every entry intent goes through the private Trade Core (see `core_interface/`). What you see here is the visible outcome, not the decisioning.
