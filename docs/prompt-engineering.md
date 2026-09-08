# Prompt-engineering notes

What we have learned, empirically, about how the shipped scorer (`gpt-5.4-nano`,
temperature 0.2, `prompts.insight_prompt`) reads a rubric. Each entry names the
pass of the 2026-09-08 RULE 3 recalibration (PR #99) that produced it; the
labeled set in `backtest/` and the `Scorer Backtest` workflow are how a claim
here gets checked again.

## The model applies a band's closed list; it does not apply an exception inside a band

Nine real-model passes, one finding that held every time:

- **Membership works.** A closed list under a band header ("3 — BUSINESS
  CONTENT, NOT AN EVENT: market-research forecasts, analyst ratings, …") is
  applied at ~100% on the labeled set from the first pass on.
- **Exceptions don't.** "X is band 2 — except ISM PMI, which is 5" floored the
  PMI rows it exempted (pass 5). "Results with no price signal are 4" floored
  Dow's +20% PE quarter *with the rationale naming the price signal* (passes
  7–8). "Every other statistic is band 2" placed under band 5 let vehicle
  sales, permits and freight indices drift up to 5 (pass 9); naming them in
  band 2's own list held.
- **Headings beat sentences.** A "MACRO STATISTICS ARE NOT EVENTS" heading was
  applied to everything macro, including the two prints the next sentence
  carved out, and to resin *price reports* that are not statistics at all
  (pass 5). The fix was structural, not verbal: no heading, the two prints as
  their own positive band-5 entry, price reports listed under DIRECT.
- **Order is a rule.** Bands are read top-down. With band 5's "generic event"
  case above DIRECT, "resin markets swing as buyers regain leverage" was filed
  as generic before it reached the DIRECT bullet that quotes it verbatim
  (pass 8); reading DIRECT before WATCH before 5 fixed it in one pass (pass 9).
- **Checklists are judgement.** "6 when the article names a counterparty,
  plant, grade, figure or date; 5 otherwise" did not move the 5/6 decision at
  all (pass 3). Making 6 the *labeled default* band and 5 the narrow exception
  did (pass 4).

Corollary for editing RULE 3: to change where an article lands, move it into
the list of the band you want, in the model's own words for it (its
`impact_rationale` tells you which phrase it matched). Do not add a qualifier
to the band it is wrongly landing in.

## Run-to-run noise at the band edges

At temperature 0.2, two runs of an identical prompt over the 150-row set change
~28 scores, ~7 of them across the 5/6 or 4/5 line. Single-shot gates that
require every row of a 15-row subset to land on the right side of a line sit
inside that noise. Margin — putting a class two notches above the line it must
clear (named-target M&A and priced input moves in DIRECT, not WATCH) — is the
lever; re-running until a sample passes is not.

## The template exit is a floor magnet

RULE 6's honest low-exposure templates are bound to the appendix band (3–4 in
production). Any wording that makes the template the *destination* for an
article that lacks a stated Americhem effect ("a template is for articles that
matter little", #74) turns nearly every article into a template at the band's
floor — the score-3 floor (`CONTEXT.md`). The template must be confined to
articles that are already in a floor band for another reason.
