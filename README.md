# Trading Agents Lab — bots that trade through a decision core they never own

**What this is:** the visible tip of an iceberg — open-source trading bots (Bybit demo) that do NOT place orders themselves. Each bot computes a signal and submits an *intent* to a separate Trade Core (not in this repo), which owns the final decision: risk checks, position sizing, execution, kill switch. Think "strategies as clients, decisioning as a private service".

**Why open the bots but not the core:** bot algorithms are textbook (mean-reversion, Donchian breakout, funding/basis). Anyone can write a bot. What is rare is the discipline layer that decides whether an intent becomes an order. The reports below show the system trading daily — and you can't reconstruct the "why" behind accept/reject from anything published here. That is by design.

## Repository layout

- `bots/` — four demo bots, production code, stdlib-only Python:
  - `bot_v1.py` — lazy market-maker (ADA): bid entries, fixed TP/SL, session adoption.
  - `bot_v2.py` — hybrid mean-reversion (ENA/DOGE): regime-aware entries, epoch-isolated PnL.
  - `bot_v3.py` — Donchian breakout (XRP): 24h channel entry, 12h channel exit.
  - `basis_bot.py` + `basis_scanner.py` — spot/perp basis capture with two-leg atomic intent groups.
  - `donchian.py`, `botlog.py`, `bybit_api.py` — shared indicator, logging, and signed-request plumbing (keys read from env files, never hardcoded).
- `core_interface/` — the ONLY public surface of the private core: a thin socket client (`hello → fencing_token → intent → result`). Bots speak this protocol; the implementation behind the socket stays closed.
- `reports/` — anonymized trade history from the live demo run:
  - `closed_trades_anonymized.csv` — 582 closed trades (symbol, qty, entries/exits, net PnL after fees, exit reason, epoch, UTC timestamps).
  - `bot_statistics.md` — per-epoch aggregates: counts, win rates, net PnL.

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
