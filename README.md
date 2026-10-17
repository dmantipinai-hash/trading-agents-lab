# Trading Agents Lab — bots that trade through a decision core they never own

**What this is:** the visible tip of an iceberg — open-source trading bots (Bybit demo) that do NOT place orders themselves. Each bot computes a signal and submits an *intent* to a separate Trade Core (not in this repo), which owns the final decision: risk checks, position sizing, execution, kill switch. Think "strategies as clients, decisioning as a private service".

**Why open the bots but not the core:** bot algorithms are textbook (mean-reversion, Donchian breakout, funding/basis). Anyone can write a bot. What is rare is the discipline layer that decides whether an intent becomes an order. The reports below show the system trading daily — and you can't reconstruct the "why" behind accept/reject from anything published here. That is by design.

## What is public here — and what is not

Public (~1% of the system, the visible tip):
- Bot source code: signal computation, order-wrapping, session lifecycle, own risk limits (TP/SL/time-stop).
- The intent-protocol client (`core_interface/`) — the wire format bots speak to the core.
- Anonymized trade reports: outcomes (entries/exits/PnL/exit reasons), per-bot and per-epoch.

Not public (~99%, under the waterline — never will be in this repo):
- The Trade Core itself: risk engine, intent arbitration, fencing, position ledger, kill switch.
- Core-side rejection reasons (why a specific intent became an order — or didn't).
- Config, infrastructure and operational detail.

Even with every public file, a rebuilt system will not trade the same way: the decision center keeps evolving (the next stage lands after 2026-10-21), and it is the part that decides what each bot is actually allowed to do.

## Repository layout

- `bots/` — demo bots, production code, stdlib-only Python:
  - `bot_v1.py` — lazy market-maker (ADA): bid entries, fixed TP/SL, session adoption.
  - `bot_v2.py` — hybrid mean-reversion (ENA/DOGE): regime-aware entries, epoch-isolated PnL.
  - `bot_v3.py` — Donchian breakout (XRP): 24h channel entry, 12h channel exit.
  - `basis_bot.py` + `basis_scanner.py` — spot/perp basis capture with two-leg atomic intent groups.
  - `donchian.py`, `botlog.py`, `bybit_api.py`, `exsync.py`, `position_state.py`, `vol_filter.py` — shared plumbing. `bybit_api.py` is a thin request-signing wrapper: keys are read from local env files at runtime, nothing is embedded.
- `core_interface/` — the ONLY public surface of the private core: a thin socket client (`hello → fencing_token → intent → result`). Bots speak this protocol; the implementation behind the socket stays closed.
- `reports/` — anonymized trade history from the live demo run, per bot:
  - `bot-v1_trades.md`, `bot-v2_trades.md`, `bot-v3_trades.md` — full trade lists, one file per bot.
  - `bot_statistics.md` — summary per bot + per-epoch aggregates.
  - `closed_trades_anonymized.csv` — raw data (582 closed trades).

## The numbers, honestly

Demo account. One month of live cron-driven trading (mid-Sep → mid-Oct 2026). Net result across 582 trades is roughly **flat** — commissions eat the edge at this size. Published as-is: the point of this repo is the *architecture* (verifiable, risk-gated autonomy), not a get-rich claim. Bots that survive a flat month without position drift or orphaned orders are the actual product here.

## Design notes (the iceberg metaphor)

1. Bots are stateless between cron ticks — a bot can be killed, updated, or rolled back without touching positions.
2. Every intent carries a fencing token; the core serializes decisions, deduplicates, and can reject with reasons the bot never sees on the wire in this repo.
3. Epochs (`v1`, `v2-hybrid`, `v2-ena`, `v4-vol`, …) partition history so strategy changes never contaminate each other's statistics.
4. If you reconstruct enough from the public CSVs to sense "there must be something else deciding" — that intuition is correct.

## License

MIT. Use the bots, learn from the reports, wire them to your own decisioning — or come ask about ours.

---
*Demo trading only. Nothing here is financial advice.*
