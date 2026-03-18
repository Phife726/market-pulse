# Design: Email Intelligence Upgrade — Scoring, Actionability, Sentiment & Logo
Date: 2026-03-17
Approach: Option A — Surgical fixes

## Goal
Make the daily AmI Market-Pulse email more actionable and visually scannable for Americhem stakeholders, while fixing score calibration that currently collapses most items to 5/10.

## Architecture
Pipeline is unchanged. Four targeted changes across three files:
1. `ingestion_engine.py` — LLM prompt recalibration + new `recommended_action` field
2. `schema.sql` + Supabase — new `recommended_action` column
3. `delivery_engine.py` — sentiment badge + action line in card HTML + logo CSS filter

### Alert tier boundaries — UNCHANGED
The existing `alert_tier` derivation (CRITICAL=1–3, ROUTINE=4–7, STRATEGIC=8–10) stays exactly as-is in both `delivery_engine.py` and the `todays_intelligence` DB view. The new 8-band Rule 2 is a scoring calibration guide for the LLM — it does not change how tiers are derived.

---

## Section 1: LLM Prompt Recalibration (`ingestion_engine.py`)

### Rule 2 — Threat Matrix Calibration (replace existing Rule 2 only)
```
Score 1–2: Immediate physical supply chain threat (plant fire, port strike, supplier bankruptcy, force majeure)
Score 3:   Significant disruption risk — major price spike, force majeure warning, capacity cut >10%
Score 4:   Negative trend with indirect Americhem exposure (demand softness, margin pressure signals)
Score 5:   Genuinely neutral — no discernible positive or negative lean for Americhem
Score 6:   Mild positive — market growth or innovation in Americhem's end markets
Score 7:   Moderate positive — competitor weakness, OEM expansion, favorable regulation
Score 8–9: Clear commercial opportunity — large feedstock price drops, competitor capacity loss
Score 10:  Transformational opportunity — major OEM win potential or supply disruption benefiting Americhem

Alert tier mapping (do not change):
  CRITICAL  = score 1–3
  ROUTINE   = score 4–7
  STRATEGIC = score 8–10
```

### Rule 3 — Impact Statement (replace existing Rule 3 only)
Replace the current text (which permits "No direct impact. Monitoring required." as a fallback) with:

> Always write a specific So-What even for routine items. Identify which Americhem business unit or cost line could be affected and in what direction. If truly no commercial connection exists, write: "Indirect exposure only — monitor for [specific reason]."

### Rule 4 — Domain Relevance Firewall (update last two lines)
Remove the two lines that instruct the model to output "No direct impact. Monitoring required." for uncertain cases. Replace with:

> When relevance is uncertain, do NOT discard. Set sentiment_score to 5 and apply Rule 3 to write a specific indirect-exposure statement.

### New JSON field: `recommended_action`
Add to the LLM JSON schema output:
```json
"recommended_action": "<one of: No action | Monitor | Flag to procurement | Share with sales | Escalate to leadership>"
```

Full updated schema:
```json
{
  "headline": "<concise factual summary, max 12 words>",
  "source_publication": "<name of the publisher>",
  "americhem_impact": "<BLUF So What for Americhem — specific, never generic>",
  "sentiment_score": <integer 1-10 per Rule 2>,
  "sentiment_rationale": "<max 10 words explaining exactly why this score was assigned>",
  "recommended_action": "<No action | Monitor | Flag to procurement | Share with sales | Escalate to leadership>",
  "source_url": "<MUST EXACTLY MATCH the URL provided in the user prompt>",
  "entities_mentioned": ["<companies, chemicals, or regions mentioned>"]
}
```

Note: `category` is NOT in the LLM schema — it is appended from `targets.yaml` by `execute_pipeline()` as before.

### `synthesize_insight()` update
- Do NOT add `recommended_action` to `required_keys` (keep validation soft)
- After receiving the LLM response, validate `recommended_action` separately:
  - If missing or not one of the five allowed values → set to `"Monitor"` (soft default, do not discard article)
  - Allowed values: `{"No action", "Monitor", "Flag to procurement", "Share with sales", "Escalate to leadership"}`

### `execute_pipeline()` update
- Add `recommended_action` to the upsert payload dict (value from `insight.get("recommended_action", "Monitor")`)

### Tests (`tests/test_pipeline.py`)
Add one new test:
- `test_recommended_action_default`: mock OpenAI to return a response with `recommended_action` omitted or set to an invalid value; assert `synthesize_insight()` returns `"Monitor"` in that field.

---

## Section 2: Database Schema (`schema.sql` + Supabase)

### Migration sequencing — IMPORTANT
Run the Supabase migration BEFORE deploying code. If code is deployed first and tries to upsert `recommended_action` on a table without the column, Supabase will return a column-not-found error and the entire upsert will fail.

### Migration SQL
```sql
ALTER TABLE daily_intelligence
  ADD COLUMN IF NOT EXISTS recommended_action TEXT;
```

Run this in the Supabase SQL editor, then deploy the code.

Also update `schema.sql` in the repo to include the column definition, keeping it in sync with the live DB.

### Notes
- `todays_intelligence` view selects `*` — no view change needed
- Existing rows will have `NULL`; email card renders without the action line for old rows
- Non-destructive; safe to run on live DB

---

## Section 3: Email Card Redesign (`delivery_engine.py`)

### Sentiment word helper function
Add module-level helper `_sentiment_word(score: int) -> tuple[str, str]` returning `(word, hex_color)`.

**Score-to-sentiment mapping:**

| Score | Word | Hex color | Notes |
|---|---|---|---|
| 1–2 | Negative | `#DC2626` | Aligns with CRITICAL tier |
| 3 | Cautionary | `#D97706` | Still CRITICAL but less severe |
| 4 | Cautionary | `#D97706` | Negative trend, indirect exposure |
| 5–6 | Neutral | `#6B7280` | No clear lean |
| 7–8 | Positive | `#16A34A` | Moderate opportunity |
| 9–10 | Opportunity | `#15803D` | Clear commercial upside |

Note: Scores 3–4 both map to "Cautionary" to reflect their negative-leaning definitions in Rule 2.

### Card bottom row update
Current:
```
[ CATEGORY ]  via Source                          Score: 5/10
```

New:
```
[ CATEGORY ]  via Source        Neutral ●  Score: 5/10
```
- Sentiment word rendered as inline colored text (not a pill — avoids clutter)
- Color from `_sentiment_word(score)` above
- `●` bullet rendered as `&#9679;` for email safety

### Recommended action line
Rendered between the impact statement (`rationale_html`) and the bottom row.
**Not rendered** (block omitted entirely, not `display:none`) if `recommended_action` is `None`, empty string, or `"No action"`.

Use the existing `accent` parameter passed to `_render_card()` for the left border color, and a light version of the section background (`#F9FAFB`) as the fill — no new parameter needed:

```html
<p style="margin:0 0 10px 0; padding:6px 10px; background-color:#F9FAFB;
           border-left:3px solid {accent}; font-size:12px; font-weight:600;
           font-family:Arial,sans-serif; color:{accent};">
  &#9654; ACTION: {recommended_action}
</p>
```

`_render_card()` signature is unchanged — `recommended_action` is read from `item.get("recommended_action", "")`.

---

## Section 4: Logo Fix (`delivery_engine.py`)

Apply `filter:brightness(0) invert(1)` to the header `<img>` tag for the Americhem logo. Previous attempts used a white pill background or a brightness filter — the current state of the file should be checked and the style attribute set to exactly:

```html
<img src="{_LOGO_URL}" alt="Americhem" width="140"
     style="display:block;height:auto;max-height:40px;
            filter:brightness(0) invert(1);">
```

Remove any previously added wrapper `<td>` background-color or pill styles around the logo if present — the filter alone is sufficient.

**Client support:**
- Gmail, Outlook Web, Apple Mail: white logo ✓
- Outlook desktop (Windows): CSS filter ignored, original dark logo shown — acceptable fallback

Footer logo is unchanged (already has `opacity:0.4`).

---

## Success Criteria
1. Sentiment scores use the full 1–10 range — no run should have >50% of items at exactly score 5
2. Every card shows a non-generic impact statement (no bare "No direct impact. Monitoring required.")
3. Every card shows a colored sentiment word matching the score band
4. Cards with actionable items (not "No action") show the `▶ ACTION:` line
5. Americhem header logo renders white in Gmail and Outlook Web
6. `test_recommended_action_default` passes in CI
