# 📈 Market-Pulse: AmI Market Intelligence Pipeline

## The Objective

Market-Pulse is an automated, high-leverage market intelligence pipeline engineered for Americhem. It replaces legacy, high-cost subscription models (e.g., Moody's News Edge) with a custom, LLM-powered synthesis engine.

**System ROI:** Reduces annual SaaS OPEX by >90% while increasing Information Yield via automated "So What?" synthesis for commercial and procurement stakeholders.

---

## 🏗️ System Architecture (The Physics)

The pipeline is fully serverless, executing autonomously via GitHub Actions.

1. **Discovery:** Queries the open web (via Serper.dev) for recent news matching Americhem's target entities. Active entity-mode targets with a mapped ZoomInfo company id are additionally enriched via the **ZoomInfo News Enrichment** API (supplemental, never a replacement for Serper; gated behind `ZOOMINFO_NEWS_ENABLED`).
2. **Extraction:** Bypasses paywalls and extracts clean article markdown (via Firecrawl).
3. **Normalization & Deduplication:** Strips URL tracking parameters and computes a SHA-256 hash to guarantee zero duplicate entries in the database.
4. **Synthesis:** Passes the raw text to OpenAI (`gpt-5.4-nano`) to extract strict JSON: the Americhem impact, a sentiment score (1-10), and the exact source URL.
5. **Storage:** Upserts the structured intelligence into a PostgreSQL database (Supabase).
6. **Delivery:** Formats the last 24 hours of data (72 h on Mondays, to capture weekend news) into a BLUF (Bottom Line Up Front) HTML email and transmits it to stakeholders via the **Resend HTTP API** with exponential-backoff retry.

---

## ⚙️ Operational Controls (The Control Panel)

You do not need to edit Python code to change what this system monitors.

Two YAML files control the pipeline; no Python changes are required to retune either.

- **`targets.yaml`** — what to monitor. To add a competitor, customer, or supplier:
  1. Open `targets.yaml`.
  2. Add the entity name under the appropriate `entities:` list (entity-mode groups) or append a term to `include_any:` (concept-mode groups).
  3. Ensure `active: true` is set.
  4. Commit the change. The pipeline picks it up on the next scheduled run.

  **Optional ZoomInfo enrichment (entity-mode only).** An entity may carry a ZoomInfo company id to add company-news enrichment alongside Serper:

  ```yaml
  - name: Magna International
    active: true
    zoominfo_company_id: 12345678
    zoominfo_news: true
  ```

  `zoominfo_company_id` is optional. `zoominfo_news` is optional and defaults to `true` when an id is present — set it to `false` to keep an entity Serper-only. Entities without an id, and all concept-mode groups, remain Serper-only. ZoomInfo discovery is additionally gated globally by the `ZOOMINFO_NEWS_ENABLED` variable.

- **`market_pulse_config.yaml`** — how to report. Controls report tuning (`visible_impact_threshold`, per-segment cap, total-article cap) and the strategic-segment taxonomy that the LLM uses to classify articles. Raise `visible_impact_threshold` if the report feels noisy; lower it if too sparse.

---

## 🔐 Environment Variables (The Constraints)

To execute this pipeline, the following secrets must be injected into the environment (or configured in GitHub Settings -> Secrets and variables -> Actions):

| Secret Name | Provider / Purpose |
| :--- | :--- |
| `SUPABASE_URL` | Supabase Project URL |
| `SUPABASE_KEY` | Supabase **Service Role / Secret** Key (Bypasses RLS) |
| `FIRECRAWL_API_KEY` | Firecrawl Web Scraper |
| `SERPER_API_KEY` | Serper.dev News Search |
| `OPENAI_API_KEY` | OpenAI LLM (used by both ingestion and delivery for thematic synthesis) |
| `SMTP_PASS` | **Resend API key** (legacy variable name; the pipeline calls the Resend HTTP API, not SMTP) |
| `SENDER_EMAIL` | Verified sending address (e.g., `alerts@ami-pulse.com`) |
| `RECIPIENT_EMAILS` | Comma-separated list of production inboxes |
| `TEST_RECIPIENT_EMAILS` | Comma-separated QA inboxes; used only by the test workflow |
| `ZOOMINFO_CLIENT_ID` | ZoomInfo OAuth client id (**preferred** auth — Client Credentials) |
| `ZOOMINFO_CLIENT_SECRET` | ZoomInfo OAuth client secret (**preferred** auth — Client Credentials) |
| `ZOOMINFO_BEARER_TOKEN` | ZoomInfo static bearer token — **fallback** for local/dev when no OAuth client is configured |

### ZoomInfo authentication

The client authenticates in priority order:

1. **OAuth Client Credentials (preferred).** When `ZOOMINFO_CLIENT_ID` and `ZOOMINFO_CLIENT_SECRET` are set, the client exchanges them for a short-lived access token at `ZOOMINFO_TOKEN_URL` (HTTP Basic auth, `grant_type=client_credentials`, `Content-Type: application/x-www-form-urlencoded`). The token is cached in-process until shortly before it expires, so a single run reuses one token across all mapped companies. Required ZoomInfo scopes: `api:data:company` and `api:data:news`.
2. **Static bearer token (fallback).** When no OAuth client is configured, `ZOOMINFO_BEARER_TOKEN` is used directly — convenient for local/dev testing.
3. **None configured** → the client logs a warning and returns no ZoomInfo candidates (Serper is unaffected).

The client secret and all access/bearer tokens are never logged. Any auth/transport failure degrades to zero ZoomInfo candidates rather than failing the run.

### Repository Variables (Operational Controls)

These are GitHub **repository variables** (Settings → Secrets and variables → Actions → Variables), not secrets — they tune ZoomInfo discovery without code changes.

| Variable | Purpose |
| :--- | :--- |
| `ZOOMINFO_TOKEN_URL` | OAuth token endpoint (default `https://api.zoominfo.com/gtm/oauth/v1/token`). Override only if ZoomInfo changes the path. |
| `ZOOMINFO_NEWS_ENABLED` | Enables ZoomInfo company-news discovery when `true` (default off). Accepts `true`/`1`/`yes`/`on`. |
| `ZOOMINFO_NEWS_LOOKBACK_DAYS` | Lookback window (days) for ZoomInfo news enrichment (default `2`). |
| `ZOOMINFO_NEWS_PER_COMPANY` | Max ZoomInfo news records requested per mapped company (default `5`). |
| `STORE_DISCOVERY_METADATA` | When `true`, persists discovery-provenance columns (`discovery_source`, `external_company_id`, `published_at`, `source_metadata`). Keep off until `migrations/003_add_discovery_metadata.sql` is applied. |

> **Rollout order for ZoomInfo:** merge the code (all flags off → production unaffected) → apply `migrations/003_add_discovery_metadata.sql` in Supabase → set `STORE_DISCOVERY_METADATA=true` → set `ZOOMINFO_NEWS_ENABLED=true`.

---

## ⏱️ Automation Schedule

The pipeline is orchestrated by `.github/workflows/market_pulse.yml`.

- **Execution Time:** Monday through Friday at 10:00 UTC (6:00 AM EDT).
- **Hard Limits:** The ingestion engine enforces `MAX_DAILY_SCRAPES = 150` (sized to paid-tier Serper/Firecrawl/OpenAI subscriptions) and a `PIPELINE_DEADLINE_SECONDS = 600` wall-clock cutoff so the run completes inside the 15-minute GitHub Actions ceiling.

To manually trigger a run, navigate to the **Actions** tab in GitHub, select the workflow, and click **Run workflow**.

A second workflow — `.github/workflows/market_pulse_test.yml` — runs the pipeline in **test mode**: it sets `MARKET_PULSE_RUN_MODE=test`, routes mail to `TEST_RECIPIENT_EMAILS` instead of the production list, and marks both the subject (`[TEST]`) and HTML body (amber "TEST RUN" banner). Inputs let you skip ingestion or skip the email send, so you can re-render the existing day's rows without re-billing the APIs.

---

## Target metadata enrichment (`scripts/enrich_targets.py`)

A standalone, reviewable utility that populates `target_metadata.yaml` — a
machine-managed companion file holding ZoomInfo company-identity metadata
(canonical name, HQ revenue/employee ranges, industries, HQ country/state) plus
conservative helper terms for a future relevance gate.

**Daily ingestion never runs this.** Ingestion consumes the checked-in, reviewed
`target_metadata.yaml` only; enrichment is an offline, human-in-the-loop step.

```bash
# Dry-run (default): prints a unified diff, writes nothing
python scripts/enrich_targets.py

# Apply the proposed changes
python scripts/enrich_targets.py --write

# One target only
python scripts/enrich_targets.py --only "Avient"
```

**Per-endpoint entitlement caveat.** A working OAuth token + News Enrich access
(proved by the ingestion pipeline) does NOT imply access to the Company Enrich
or Company Search endpoints this utility uses — ZoomInfo scopes are granted
per-endpoint. When an endpoint returns 401/403/invalid-scope, the affected target
degrades to `zoominfo_metadata_status: error` (prior good data is preserved) and
the run continues; it never crashes.

**Company Enrich request shape (verified live).** The GTM Company Enrich endpoint
identifies records by a `matchCompanyInput` list and **requires** an `outputFields`
list; `enrich_company` sends both with the verified tokens (`name`, `revenue`,
`employeeCount`, `primaryIndustry`, `industries`, `country`, `state`). Industry
fields can come back as objects/lists, so `extract_firmographics` coerces them to
clean labels. A sparse Enrich response (no `canonical_name`) is still recorded as
`missing`, never a misleading `verified`, and the CLI logs a `WARNING` naming the
target when an Enrich returns `ok` but sparse.

**Optional — preview a target before `--write`.** A manual GitHub Actions workflow,
**ZoomInfo Metadata Enrich Dry-Run** (`.github/workflows/zoominfo_company_enrich_smoke.yml`),
runs `enrich_targets.py --only <target>` inside Actions (where the ZoomInfo secrets
live) as a dry-run — it writes nothing and only prints the proposed diff. Dispatch
it from the Actions tab or:

```bash
gh workflow run "ZoomInfo Metadata Enrich Dry-Run" -f target=Avient
```

The client logs are sanitized (keys-only structural summaries; a capped 400
snippet) — no tokens, headers, request body, or firmographic values are ever
logged.

**Reviewing output.** Records carry `zoominfo_metadata_status`
(`verified|needs_review|missing|error`) and `zoominfo_metadata_confidence`
(`high|medium|low`). Anything not `verified` warrants a human look before trust.
Edit only the human-curated fields — `manual_aliases` (e.g. risky short acronyms
like `RTP` that the utility deliberately will not auto-generate) and
`exclude_terms`; the enricher preserves them on re-runs. Extend
`INDUSTRY_TERM_MAP` in `target_enricher.py` when an `industry_unmapped: true`
record appears.

Removed targets are kept and flagged `metadata_record_status: orphaned`, never
auto-deleted.
