# Delivery Window Anchor — Design

**Date:** 2026-08-27
**Status:** Implemented (issue #64)
**Driver:** The 2026-08-27 scheduled run started at 13:53 UTC (cron is `17 10 * * 1-5`; every other run in the prior two weeks started ~10:25 — a GitHub-side scheduling delay). Delivery's window was "rows created in the last 24 h" by its own wall clock, which closed at ~14:01 on 08-26 — *after* every row from the 08-26 run — so the email carried exactly the 19 rows ingested that day. Combined with #63 (the Serper yield collapse) that produced the "Low Signal" report.

## The defect

`fetch_todays_intelligence()` → `repo.fetch_recent(hours=24)` filtered `created_at >= now - 24h`. That window is never "since the last email":

- **Normal day** (delivery ~10:38, yesterday's rows stored 10:25–10:44): the window overlaps the tail of yesterday's run, so every email re-delivers a slice of yesterday's late rows (08-26 fetched 87 = 42 new + ~45 from 08-25).
- **Late day**: the window closes after all of yesterday's rows and silently drops them.

## Options considered

1. **Anchor to the last production delivery** — record a delivery timestamp on `daily_summaries` and fetch `created_at > last_delivery_at`. Exact semantics, no double delivery, robust to any start time. Needs a migration + repo methods.
2. **Anchor to the schedule** — `cutoff = min(now, today@10:17Z) - lookback`. Robust to late starts, but on a normal day widens the window by ~20 min → *more* double delivery than today.
3. **Grace on late starts only** — extend the lookback by the lag past 10:17Z. Minimal, but hard-codes the cron in code (or an env var the workflow sets) and keeps the normal-day double delivery.

**Chosen: (1).** Migrations are routine here (006 shipped for issue #43), and it is the only option whose window is the thing the reader actually wants — everything since the last email. The issue's own recommendation was (1) if a migration is acceptable.

## Design

### Storage — migration 007

`daily_summaries.delivered_at timestamptz`, nullable. Stamped by delivery **after** the Resend call succeeds, on the `(run_date, run_mode)` row ingestion wrote that day (ingestion always writes one — the accounting-only row from issue #43 guarantees it even on zero-yield runs).

Degrades gracefully if the code ships before the migration: the anchor read fails (`column does not exist`), is swallowed like every repo read, and the engine falls back to the wall-clock window; the post-send stamp fails and logs a warning. Nothing crashes, but the late-start gap stays open until the column exists — so apply first.

### Repo seam — three changes to `IntelligenceRepo`

- `fetch_recent(hours)` → **`fetch_since(cutoff: datetime)`**: rows with `created_at` **strictly after** the cutoff. Strict so a row stored at the instant of the last delivery — which that email already carried — is not delivered twice. One query method replaces the old one; nothing was left dead.
- **`fetch_last_delivery(*, run_mode, before_date) -> Optional[datetime]`**: `delivered_at` of the most recent `run_mode` delivery on a `run_date` strictly before `before_date`, as a naive UTC datetime. Read: swallows to `None`.
- **`record_delivery(*, run_date, run_mode, delivered_at)`**: UPDATE `delivered_at` on the compound key; silent no-op when no row matches (the delivery-only QA run has no test row). Write: raises; the engine wrapper decides.

`_coerce_created_at` became `_coerce_timestamp` (it now normalizes both columns); `_utc_isoformat` stores the stamp with an explicit `+00:00`.

### Delivery engine

- **`delivery_window(now, last_delivered_at) -> DeliveryWindow(cutoff, anchored)`** — pure. A recorded delivery wins outright. Without one, the legacy wall-clock lookback: `FALLBACK_LOOKBACK_HOURS` = 24, `FALLBACK_LOOKBACK_HOURS_MONDAY` = 72.
- **`fetch_todays_intelligence(now=None)`** reads the anchor with `run_mode="production"` and `before_date=today` in **every** run mode, then `fetch_since(window.cutoff)`. Logs at INFO when anchored, WARNING when falling back (the fallback should be rare after rollout — a warning makes a silently-unapplied migration visible in the job log).
- **`_record_delivery(delivered_at)`** wraps `record_delivery` on `(today, config.run_mode())`. Called from `execute_pipeline` after `send_email` returns — for the no-news variant too (a no-news email is still an email). Failure is a logged warning: the email is already out, and a red job would invite a manual re-run and a duplicate email.

### Review hardening (Codex on PR #67)

Two ways the anchor could move past rows nobody received, both closed:

- **A failed fetch.** `fetch_since` was a swallow-to-`[]` read, so a Supabase outage would have produced a no-news email and then a stamp hiding every row that existed. It is now a **strict read** (raises), the second such read after `require_delivery_state` and for the same reason — a write depends on it. A database outage is a red job with no email and no stamp.
- **A row written mid-run.** A concurrent ingestion (QA workflow with `run_ingestion=true`, or a manual dispatch) can land a row between the fetch and the send; stamping the send time would put it before the anchor forever. `execute_pipeline` now reads the clock **once**, uses it as the window's `now`, and stamps that same instant after the send — so the row is inside the next window.

### Two deliberate choices

**The anchor is the last delivery on a strictly *earlier* run_date, not simply the latest.** A same-day retry (manual re-dispatch after a bad or failed morning email) therefore re-sends the whole day's window plus anything new, instead of only what arrived since the morning email. The same rule is what makes the QA path work: a `run_ingestion=false` test re-render at 14:00 sees the rows the 10:38 production email saw.

**The anchor is production-only in every mode.** Test-mode runs stamp their own `run_mode='test'` row (a no-op on production accounting — the existing write-back contract) and are never read as an anchor, so a late QA run yesterday cannot shrink today's production window.

### Monday

Under the anchor, Monday needs no special rule: Friday's `delivered_at` already spans the weekend, and widening it to 72 h would re-deliver Friday's rows. The 72-hour rule survives only in the fallback, so a fallback Monday run still covers weekend news.

### Not changed

- The `todays_intelligence` SQL view keeps its 24 h window — it is an ad-hoc query aid, not a runtime path.
- `fetch_macro_summary`'s `run_date >= yesterday` window is unrelated (it selects the summary row, not the article rows).
- `recent_headlines(hours=72)` — ingestion's semantic-dedup hydration — is a different window with a different purpose.

## Tests

- `tests/test_delivery_window.py` — the pure window (anchor wins; 24 h / 72 h Monday fallback; anchor supersedes Monday), the issue-#64 gate against `InMemoryIntelligenceRepo` (rows at T-25h and T-1h, delivery at T, yesterday's email at T-27h → both fetched), no double delivery on a normal day (strict cutoff), wall-clock fallback without a recorded delivery, same-day-retry semantics, test mode reading the production anchor, and the `execute_pipeline` wiring (stamp after a successful send, for no-news too; no stamp when the send raises; test-mode stamp is a no-op on the production row; stamp failure warns instead of raising).
- `tests/test_intelligence_repo.py` — `fetch_since` strictness and the existing filter/copy/timezone fidelity tests migrated from `fetch_recent`; `fetch_last_delivery` / `record_delivery` for both adapters, including the Supabase filter chain (`eq run_mode` / `lt run_date` / `not_.is_ delivered_at null` / `order delivered_at desc` / `limit 1`) and the swallow-on-read / raise-on-write policy.

## Rollout

1. Apply `migrations/007_add_delivered_at.sql` in the Supabase SQL editor.
2. Merge. The first production run after the merge has no recorded anchor and logs the fallback warning once; from the second run on, every window is anchored.
3. Verify on the job log: "Fetched N intelligence record(s) created after the last production delivery at …" and no "No prior production delivery recorded" warning.
