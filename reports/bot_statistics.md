# Bot statistics — per-epoch aggregates (demo, anonymized)

Source: `closed_trades_anonymized.csv` (582 closed trades, 2026-09-16 → 2026-10-17 UTC).
Aggregated by symbol + epoch. Net PnL is after fees, USDT.

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

Total: 582 trades, net ≈ **-2.70 USDT** across one month of cron-driven demo trading.

Reading guide:
- Epoch = a distinct parameter/strategy generation; statistics never mix epochs.
- Exit reasons in the CSV: TP / SL / TIME / SESSION-END / FLATTEN / OTHER / UNKNOWN — all bot-side
  reasons; core-side rejection reasons are intentionally not part of the public data.
- Win rates swing 40–83% per epoch while net stays near zero: classic small-size demo profile
  where commissions dominate. The system's value is discipline (no position drift, no orphaned
  orders, clean epoch attribution), not alpha at this scale.
