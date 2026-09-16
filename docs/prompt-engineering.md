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

## A band's heading label moves the classes that already read as that band; the rest follow the words in the list

The score-6 ceiling (issue #109, 2026-09-16): six production crons after the
recalibration put 136 rows at 6 and none at 8+, with the events DIRECT names
verbatim — Univar/Interpur, Sudarshan/Clariant, AdvanSix caprolactam — all at
6. Two passes against the labeled set, graded on the majority-of-3 protocol
(`--runs 3`, the median run stands) with the `expected_band` labels as the
criterion (21 `direct` rows must score ≥ 7, 11 `watch` rows ≤ 6):

- **Pass 1 — the heading default** (`9ea7f27943b6`). DIRECT's heading gained
  its own "default for …" label (only WATCH's had said "default"), WATCH's
  bullets stopped restating DIRECT's members with carve-outs, and four
  carve-outs that still said "DIRECT, below" — a pointer to a band that has
  sat *above* WATCH since pass 9 — were turned round. Result: every priced
  input moved (Dow PE, Chemours TiO2 ×2, AdvanSix, PET resin, the resin-swing
  call; 9/21 direct, with 8s appearing for the first time), and **every
  named-target deal and supplier bankruptcy stayed at 6** — 12/12 misses in
  those two classes. Their rationales were WATCH's words back: "distribution
  expansion via acquisition", "channel consolidation", "plant sale changes
  supply ownership", "restructuring", "supplier distress". The heading label
  was enough for a class the model already read as an input price; for a
  class it read through another band's vocabulary, the label did nothing.
- **Pass 2 — the model's own words** (`588c432b0f15`). Those phrases were
  moved into DIRECT's list as members ("distribution expansion via
  acquisition", "channel consolidation", a plant's "supply ownership change",
  a Chapter 11 filing, a court's mediation order, "supplier distress"), with
  the labeled rows as examples (Univar acquires H.M. Royal; Trinseo files
  Chapter 11), and the hooks were removed from WATCH's lists ("or sold",
  "distribution-agreement change", "financial distress — restructuring").
  Result: **the distress class moved** (Trinseo's filing and its
  force-majeure quarter both 8; 12/21 direct), the priced inputs held, and
  **the named-target acquisitions did not** — the Interpur rows 0/4, the
  H.M. Royal rows 2/4 on near-identical text, Sudarshan/Clariant 0/2 — with
  rationales that now quoted DIRECT's new members back ("distributor
  acquisition", "channel expansion via acquisition") and still scored 6.
  Precision dipped to 35/39 on three one-notch flips of rows the floor names
  verbatim (EV sales, a bus-camera trial, a trade-show exhibit): edge noise,
  not the reword.
- **Pass 3 — margin and region** (`23e480082652`). The deal bullet stated
  its own score ("score 8 whichever region the deal is in"), named the
  newest rationale words, and band 4's "bare" result gained "or guidance
  change" so a competitor's beat-and-raise is not floored before WATCH reads
  it. Result: 11/21 — the acquisition rows 0/8 this time, rationales
  identical in vocabulary to the passing rows' and still at 6; precision
  back to 37/39. **A stated score inside a bullet did not move a class the
  model had already decided was a 6.** What the classes that did move share
  is a cost mechanism (an input price, a supplier's supply); the one that
  never moved was framed — by the rubric, by RULE 6's own example So-What
  ("can shift additive channel pricing"), and by every rationale — as
  "channel dynamics".
- **Pass 4 — the mechanism the model already scores** (`aa6681a868fe`,
  shipped). The actor paragraph files a distributor (Univar, Brenntag,
  Nexeo, H.M. Royal) as a SUPPLIER that sets the price and allocation of
  what Americhem buys through it; the deal bullet scores a distributor's
  acquisition "like a supplier's price increase, never like a 'channel
  dynamics' watch item"; RULE 6's example So-What names the input-price and
  allocation exposure. Result: **17/21 direct (81%), 39/39 precision, 36/37
  recall, distribution 6:19 / 7:14 / 8:8** — the H.M. Royal rows 4/4, the
  Interpur rows 2/4, and the rationales now say "allocation and input-cost
  exposure". The two Sudarshan/Clariant rows (a pigment plant sale) and the
  recyclate price report are the remaining misses at 6, and Avient's bare
  beat-and-raise is the recall miss the floor keeps taking (band 4 wins over
  WATCH by construction). The pass is one row above the 80% line — inside
  the run-to-run noise — so the production watch in issue #109 (`direct > 0`
  on every one of the first five crons, `surfaced_count` inside 8–15) is the
  real acceptance, not this sample.

Corollary, added to the one above: a class the model has already decided is
a 6 is not moved by naming it in the higher band's list, by writing its
score into the bullet, or by pointing the carve-outs the right way. It moves
when the rubric gives it the *mechanism* the model already scores at that
level — here, an input-cost effect — in the actor definition and in the
So-What example, not only in the band.
