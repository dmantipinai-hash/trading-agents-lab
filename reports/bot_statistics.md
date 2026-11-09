# Bot statistics — per-epoch aggregates (demo, anonymized)

Source: `closed_trades_anonymized.csv` (1007 closed trades, 2026-09-16 → 2026-11-09 UTC).
Net PnL is after fees, USDT. Epoch = a distinct parameter/strategy generation; statistics never mix epochs.

## Per-bot summary

| bot | strategy | trades | net PnL | winrate | report |
|---|---|---|---|---|---|
| **bot-v1** | lazy market-maker | 516 | **-2.0938** | 41% | [bot-v1_trades.md](bot-v1_trades.md) |
| **bot-v2** | hybrid mean-reversion | 468 | **-1.2141** | 64% | [bot-v2_trades.md](bot-v2_trades.md) |
| **bot-v3** | Donchian breakout | 13 | **-0.6326** | 23% | [bot-v3_trades.md](bot-v3_trades.md) |
| *unattributed* | settled outside bot sessions | 10 | *+0.5429* | 80% | — (in CSV) |

## Per-epoch detail

| symbol | epoch | trades | avg PnL | net PnL | winrate |
|---|---|---|---|---|---|
| XRPUSDT | v1 | 19 | -0.0039 | -0.0748 | 42% |
| XRPUSDT | v2-hybrid | 5 | +0.0746 | +0.3729 | 60% |
| DOGEUSDT | v2-hybrid | 12 | +0.0216 | +0.2592 | 83% |
| ADAUSDT | v1-revival | 35 | +0.0068 | +0.2391 | 57% |
| DOGEUSDT | v4-vol | 107 | -0.0026 | -0.2773 | 63% |
| ADAUSDT | v4-vol | 462 | -0.0049 | -2.2572 | 40% |
| ENAUSDT | v2-hybrid | 5 | +0.0388 | +0.1940 | 100% |
| ENAUSDT | v2-ena | 349 | -0.0035 | -1.2207 | 63% |
| XRPUSDT | v3-donchian | 13 | -0.0487 | -0.6326 | 23% |

Total: 1007 trades, net ≈ **-3.40 USDT** across the live demo window.

## Reading guide

- Exit reasons in the CSV: TP / SL / TIME / SESSION-END / FLATTEN / OTHER / UNKNOWN — bot-side reasons.
- `unattributed` = trades settled outside bot sessions (core-side position management or manual/operator close). For those rows the exit reason reads UNKNOWN by design: core-side decision reasons are not part of the public data.
- Win rates swing widely per epoch while net stays near zero: classic small-size demo profile where commissions dominate. The value shown here is discipline (no position drift, no orphaned orders, clean epoch attribution), not alpha at this scale.
- Bots do not place orders themselves: every entry intent goes through the private Trade Core (see `core_interface/`). What you see here is the visible outcome, not the decisioning.
