# 📈 Market-Pulse: AmI Market Intelligence Pipeline

## The Objective

Market-Pulse is an automated, high-leverage market intelligence pipeline engineered for Americhem. It replaces legacy, high-cost subscription models (e.g., Moody's News Edge) with a custom, LLM-powered synthesis engine.

**System ROI:** Reduces annual SaaS OPEX by >90% while increasing Information Yield via automated "So What?" synthesis for commercial and procurement stakeholders.

---

## 🏗️ System Architecture (The Physics)

The pipeline is fully serverless, executing autonomously via GitHub Actions.

1. **Discovery:** Queries the open web (via Serper.dev) for recent news matching Americhem's target entities.
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

---

## ⏱️ Automation Schedule

The pipeline is orchestrated by `.github/workflows/market_pulse.yml`.

- **Execution Time:** Monday through Friday at 10:00 UTC (6:00 AM EDT).
- **Hard Limits:** The ingestion engine enforces `MAX_DAILY_SCRAPES = 150` (sized to paid-tier Serper/Firecrawl/OpenAI subscriptions) and a `PIPELINE_DEADLINE_SECONDS = 600` wall-clock cutoff so the run completes inside the 15-minute GitHub Actions ceiling.

To manually trigger a run, navigate to the **Actions** tab in GitHub, select the workflow, and click **Run workflow**.

A second workflow — `.github/workflows/market_pulse_test.yml` — runs the pipeline in **test mode**: it sets `MARKET_PULSE_RUN_MODE=test`, routes mail to `TEST_RECIPIENT_EMAILS` instead of the production list, and marks both the subject (`[TEST]`) and HTML body (amber "TEST RUN" banner). Inputs let you skip ingestion or skip the email send, so you can re-render the existing day's rows without re-billing the APIs.
