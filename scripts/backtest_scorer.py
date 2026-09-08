"""Re-score the labeled scorer backtest — or a replay of recent production
rows — through the live insight prompt. Operator tool; not part of the cron.

Two modes, one scoring path. Every row goes through exactly what production
runs after the scrape: `prompts.insight_prompt` (the assembled RULE 0–7
system prompt, config-injected) → the `llm` seam (`OpenAILLM`, shipped model
and temperature) → `insight.is_discard` / `insight.normalize`. The only
difference from the cron is the article text: the stored `article_summary`
stands in for the scraped markdown (see backtest/BACKTEST_README.md).

    python scripts/backtest_scorer.py csv
        Re-scores backtest/market_pulse_scorer_backtest.csv, fills
        revised_score / revised_rationale / pass, writes the _v2.csv next to
        it, and prints the README's four pass criteria with every failure
        named. Exit 1 when a criterion fails.

    python scripts/backtest_scorer.py replay --days 5
        Re-scores the daily_intelligence rows created in the last N days
        (the real repo seam) and prints, per run day, how many rows the
        shipped rubric put at >=5 / >=6 against how many the live prompt
        does — the order-of-magnitude check on surfaced_count without
        re-billing Serper or Firecrawl.

Needs OPENAI_API_KEY (both modes) and SUPABASE_URL / SUPABASE_KEY (replay).
"""
import argparse
import csv
import os
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Optional

import yaml

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

import insight  # noqa: E402
import prompts  # noqa: E402
from llm import _llm  # noqa: E402

BACKTEST_DIR = os.path.join(_REPO_ROOT, "backtest")
CSV_IN = os.path.join(BACKTEST_DIR, "market_pulse_scorer_backtest.csv")
CSV_OUT = os.path.join(BACKTEST_DIR, "market_pulse_scorer_backtest_v2.csv")
REPLAY_OUT = os.path.join(BACKTEST_DIR, "replay_rescored.csv")

SURFACE_MIN = 5          # criterion 1: a `surface` row must score >= this
SUPPRESS_MAX = 4         # criterion 2: a `suppress` row must score <= this
SHARE_GE5_BAND = (0.10, 0.15)   # criterion 3: share of rows at >= 5
MAX_SINGLE_SCORE_SHARE = 0.50   # criterion 3: no one score value above this
REGRESSION_FLOOR = 6     # criterion 4: surface rows originally >= 6 stay >= 6
PASS_RATE = 0.90

# A RULE 1 DISCARD is not stored in production, so it has no score. For grading
# it counts as the lowest possible value: it satisfies `suppress` (<= 4) and
# fails `surface` (>= 5), which is exactly what a discard means for the report.
DISCARD_SCORE = 0


def _load_config() -> dict:
    path = os.path.join(_REPO_ROOT, "market_pulse_config.yaml")
    with open(path, "r") as fh:
        return yaml.safe_load(fh) or {}


def _article_text(headline: str, summary: str, publication: str) -> str:
    """The stand-in for scraped markdown: what the backtest set carries."""
    parts = [headline.strip()]
    if publication.strip():
        parts.append(f"Publisher: {publication.strip()}")
    parts.append(summary.strip())
    return "\n\n".join(p for p in parts if p)


def score_article(
    config: dict,
    *,
    headline: str,
    summary: str,
    publication: str,
    trigger_entity: str,
    category: str,
    source_url: str,
    attempts: int = 3,
) -> dict:
    """One row through the production scoring path. Returns
    {score, rationale, so_what, signal_type, discard}; score is None only when
    the LLM seam failed `attempts` times (the row is then excluded from the
    criteria rather than silently scored)."""
    spec = prompts.insight_prompt(
        config,
        article_text=_article_text(headline, summary, publication),
        source_url=source_url,
        trigger_entity=trigger_entity,
        category=category,
    )
    llm = _llm()
    raw: Optional[dict] = None
    for _ in range(attempts):
        raw = llm.complete_json(**spec.kwargs())
        if raw is not None:
            break
    if raw is None:
        return {"score": None, "rationale": "LLM_FAILED", "so_what": "", "signal_type": "", "discard": False}
    if insight.is_discard(raw):
        return {"score": DISCARD_SCORE, "rationale": "DISCARD (RULE 1 false entity match)",
                "so_what": "", "signal_type": "", "discard": True}
    row = insight.normalize(raw)
    if row is None:
        return {"score": None, "rationale": "UNUSABLE_RESPONSE", "so_what": "", "signal_type": "", "discard": False}
    return {
        "score": insight.effective_impact(row),
        "rationale": (row.get("impact_rationale") or "").strip(),
        "so_what": (row.get("americhem_impact") or "").strip(),
        "signal_type": row.get("signal_type") or "",
        "discard": False,
    }


# ---------------------------------------------------------------------------
# csv mode — the labeled set
# ---------------------------------------------------------------------------

def _grade(tier: str, score: Optional[int]) -> str:
    """The README's formula. Blank for `unlabeled` and for a row the seam failed on."""
    if score is None or tier == "unlabeled":
        return ""
    if tier == "surface":
        return str(score >= SURFACE_MIN)
    if tier == "suppress":
        return str(score <= SUPPRESS_MAX)
    return ""


def _fmt_row(r: dict) -> str:
    return (f"  {r['backtest_id']}  [{r['expected_tier']:<8}] orig={r['original_score']:>4} "
            f"rev={str(r['revised_score']):>4}  {r['trigger_entity'][:22]:<22} "
            f"{r['headline'][:58]}\n        ↳ {r['revised_rationale'][:110]}")


def _share_line(label: str, rows: list[dict]) -> tuple[bool, str]:
    scored = [r for r in rows if r["revised_score"] is not None]
    if not scored:
        return False, f"  {label}: no scored rows"
    ge5 = sum(1 for r in scored if r["revised_score"] >= SURFACE_MIN)
    share = ge5 / len(scored)
    dist = Counter(r["revised_score"] for r in scored)
    top_score, top_n = dist.most_common(1)[0]
    top_share = top_n / len(scored)
    lo, hi = SHARE_GE5_BAND
    ok = lo <= share <= hi and top_share <= MAX_SINGLE_SCORE_SHARE
    dist_txt = "  ".join(f"{s}:{dist[s]}" for s in sorted(dist))
    return ok, (f"  {label}: {ge5}/{len(scored)} = {share:.1%} at >= {SURFACE_MIN} "
                f"(band {lo:.0%}–{hi:.0%}); most common score {top_score} = {top_share:.1%} "
                f"(max {MAX_SINGLE_SCORE_SHARE:.0%}) -> {'PASS' if ok else 'FAIL'}\n"
                f"      distribution: {dist_txt}")


def _prompt_fingerprint(config: dict) -> str:
    """The insight system prompt's identity (PromptSpec.system_fingerprint) —
    printed at the top of every report so a result is never read against the
    wrong wording."""
    spec = prompts.insight_prompt(config, article_text="", source_url="", trigger_entity="", category="")
    return spec.system_fingerprint


def run_csv(path_in: str, path_out: str, workers: int) -> int:
    config = _load_config()
    print(f"Insight prompt fingerprint: {_prompt_fingerprint(config)}")
    with open(path_in, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    print(f"Loaded {len(rows)} rows from {os.path.relpath(path_in, _REPO_ROOT)}")

    def _one(r: dict) -> dict:
        return score_article(
            config,
            headline=r["headline"],
            summary=r["article_summary"],
            publication=r.get("source_publication", ""),
            trigger_entity=r["trigger_entity"],
            category=r["category"],
            # The set omits source_url on purpose; RULE 1 only needs the model
            # to echo whatever URL the user prompt carries.
            source_url=f"https://backtest.invalid/{r['backtest_id']}",
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_one, rows))

    for r, res in zip(rows, results):
        r["revised_score"] = res["score"]
        r["revised_rationale"] = res["rationale"]
        r["pass"] = _grade(r["expected_tier"], res["score"])

    with open(path_out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            out = dict(r)
            out["revised_score"] = "" if r["revised_score"] is None else r["revised_score"]
            writer.writerow(out)
    print(f"Wrote {os.path.relpath(path_out, _REPO_ROOT)}\n")

    unscored = [r for r in rows if r["revised_score"] is None]
    if unscored:
        print(f"WARNING: {len(unscored)} row(s) got no usable LLM response and are excluded "
              f"from the criteria: {', '.join(r['backtest_id'] for r in unscored)}\n")

    surface = [r for r in rows if r["expected_tier"] == "surface" and r["revised_score"] is not None]
    suppress = [r for r in rows if r["expected_tier"] == "suppress" and r["revised_score"] is not None]
    unlabeled = [r for r in rows if r["expected_tier"] == "unlabeled" and r["revised_score"] is not None]

    verdicts: list[bool] = []

    # 1. Recall
    hits = [r for r in surface if r["revised_score"] >= SURFACE_MIN]
    misses = [r for r in surface if r["revised_score"] < SURFACE_MIN]
    ok = len(surface) > 0 and len(hits) / len(surface) >= PASS_RATE
    verdicts.append(ok)
    print(f"1. RECALL    {len(hits)}/{len(surface)} surface rows >= {SURFACE_MIN} "
          f"({len(hits)/max(len(surface),1):.1%}, need >= {PASS_RATE:.0%}) -> {'PASS' if ok else 'FAIL'}")
    for r in misses:
        print(_fmt_row(r))

    # 2. Precision
    hits = [r for r in suppress if r["revised_score"] <= SUPPRESS_MAX]
    leaks = [r for r in suppress if r["revised_score"] > SUPPRESS_MAX]
    ok = len(suppress) > 0 and len(hits) / len(suppress) >= PASS_RATE
    verdicts.append(ok)
    print(f"\n2. PRECISION {len(hits)}/{len(suppress)} suppress rows <= {SUPPRESS_MAX} "
          f"({len(hits)/max(len(suppress),1):.1%}, need >= {PASS_RATE:.0%}) -> {'PASS' if ok else 'FAIL'}")
    for r in leaks:
        print(_fmt_row(r))

    # 3. Distribution — reported two ways. The README's literal wording is
    #    "across all 150 rows", but the set is stratified: 37 surface rows must
    #    score >= 5 for criterion 1, and 37/150 alone is 24.7%, so the literal
    #    form can never pass while criterion 1 does. The README also says the
    #    74 unlabeled rows are "a proportional sample for measuring
    #    distribution shape", so that is the sample the verdict is taken on;
    #    the literal number is printed alongside it, never hidden.
    print("\n3. DISTRIBUTION")
    ok_all, line_all = _share_line("all rows (literal)", surface + suppress + unlabeled)
    ok_unl, line_unl = _share_line("unlabeled rows (proportional sample — the verdict)", unlabeled)
    print(line_all)
    print("      (cannot pass jointly with criterion 1 on this stratified set: "
          f"{len(surface)} surface rows alone are {len(surface)/max(len(rows),1):.1%} of it)")
    print(line_unl)
    verdicts.append(ok_unl)

    # 4. No regression on threshold items
    threshold_rows = [r for r in surface if float(r["original_score"] or 0) >= REGRESSION_FLOOR]
    kept = [r for r in threshold_rows if r["revised_score"] >= REGRESSION_FLOOR]
    regressed = [r for r in threshold_rows if r["revised_score"] < REGRESSION_FLOOR]
    ok = not regressed
    verdicts.append(ok)
    print(f"\n4. THRESHOLD {len(kept)}/{len(threshold_rows)} surface rows with original >= "
          f"{REGRESSION_FLOOR} still >= {REGRESSION_FLOOR} -> {'PASS' if ok else 'FAIL'}")
    for r in regressed:
        print(_fmt_row(r))

    # Context: the per-tier means, and which unlabeled rows the rubric lifts.
    def _mean(rs: list[dict]) -> float:
        return sum(r["revised_score"] for r in rs) / len(rs) if rs else float("nan")
    print(f"\nmean revised score — surface {_mean(surface):.2f} | suppress {_mean(suppress):.2f} "
          f"| unlabeled {_mean(unlabeled):.2f}")
    lifted = [r for r in unlabeled if r["revised_score"] >= SURFACE_MIN]
    print(f"unlabeled rows at >= {SURFACE_MIN} ({len(lifted)}):")
    for r in lifted:
        print(_fmt_row(r))

    overall = all(verdicts)
    print(f"\nOVERALL: {'PASS' if overall else 'FAIL'}  "
          f"(recall {verdicts[0]}, precision {verdicts[1]}, distribution {verdicts[2]}, threshold {verdicts[3]})")
    return 0 if overall else 1


# ---------------------------------------------------------------------------
# replay mode — recent production rows
# ---------------------------------------------------------------------------

def run_replay(days: int, path_out: str, workers: int) -> int:
    from daily_intelligence_repo import _repo
    from run_instant import naive_utcnow
    from scoring import Scoring

    config = _load_config()
    print(f"Insight prompt fingerprint: {_prompt_fingerprint(config)}")
    scorer = Scoring.from_config(config)
    cutoff = naive_utcnow() - timedelta(days=days)
    rows = _repo().fetch_since(cutoff)
    print(f"Fetched {len(rows)} daily_intelligence rows created after {cutoff.isoformat()}")

    def _one(r: dict) -> dict:
        return score_article(
            config,
            headline=r.get("headline") or "",
            summary=r.get("article_summary") or "",
            publication=r.get("source_publication") or "",
            trigger_entity=r.get("trigger_entity") or "",
            category=r.get("category") or "",
            source_url=r.get("source_url") or "",
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_one, rows))

    by_day: dict[str, list[tuple[dict, dict]]] = {}
    for r, res in zip(rows, results):
        day = str(r.get("created_at") or "")[:10]
        by_day.setdefault(day, []).append((r, res))

    with open(path_out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["created_at", "url_hash", "trigger_entity", "category", "headline",
                         "original_score", "revised_score", "revised_rationale", "revised_signal_type"])
        for day in sorted(by_day):
            for r, res in by_day[day]:
                writer.writerow([r.get("created_at"), r.get("url_hash"), r.get("trigger_entity"),
                                 r.get("category"), r.get("headline"), insight.effective_impact(r),
                                 "" if res["score"] is None else res["score"], res["rationale"],
                                 res["signal_type"]])
    print(f"Wrote {os.path.relpath(path_out, _REPO_ROOT)}\n")

    vis, sup = scorer.visible, SURFACE_MIN
    print(f"{'day':<12}{'rows':>6}{'orig>=' + str(sup):>10}{'orig>=' + str(vis):>10}"
          f"{'new>=' + str(sup):>10}{'new>=' + str(vis):>10}{'discard':>9}{'failed':>8}")
    for day in sorted(by_day):
        pairs = by_day[day]
        orig = [insight.effective_impact(r) for r, _ in pairs]
        new = [res["score"] for _, res in pairs if res["score"] is not None]
        print(f"{day:<12}{len(pairs):>6}"
              f"{sum(1 for s in orig if s >= sup):>10}{sum(1 for s in orig if s >= vis):>10}"
              f"{sum(1 for s in new if s >= sup):>10}{sum(1 for s in new if s >= vis):>10}"
              f"{sum(1 for _, res in pairs if res['discard']):>9}"
              f"{sum(1 for _, res in pairs if res['score'] is None):>8}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)
    p_csv = sub.add_parser("csv", help="re-score the labeled backtest set")
    p_csv.add_argument("--input", default=CSV_IN)
    p_csv.add_argument("--output", default=CSV_OUT)
    p_csv.add_argument("--workers", type=int, default=6)
    p_rep = sub.add_parser("replay", help="re-score recent daily_intelligence rows")
    p_rep.add_argument("--days", type=int, default=5)
    p_rep.add_argument("--output", default=REPLAY_OUT)
    p_rep.add_argument("--workers", type=int, default=6)
    args = parser.parse_args(argv)
    if args.mode == "csv":
        return run_csv(args.input, args.output, args.workers)
    return run_replay(args.days, args.output, args.workers)


if __name__ == "__main__":
    sys.exit(main())
