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
6. **Delivery:** Formats the last 24 hours of data into a BLUF (Bottom Line Up Front) HTML email and transmits it to stakeholders via verified SMTP (Resend).

---

## ⚙️ Operational Controls (The Control Panel)
You do not need to edit Python code to change what this system monitors.

The system's targets are strictly controlled by the `targets.yaml` file. To add a new competitor, customer, or raw material market:
1. Open `targets.yaml`.
2. Add the entity name under the appropriate category.
3. Ensure `active: true` is set.
4. Commit the change. The pipeline will automatically begin monitoring the new target on the next scheduled run.

---

## 🔐 Environment Variables (The Constraints)
To execute this pipeline, the following secrets must be injected into the environment (or configured in GitHub Settings -> Secrets and variables -> Actions):

| Secret Name | Provider / Purpose |
| :--- | :--- |
| `SUPABASE_URL` | Supabase Project URL |
| `SUPABASE_KEY` | Supabase **Service Role / Secret** Key (Bypasses RLS) |
| `FIRECRAWL_API_KEY` | Firecrawl Web Scraper |
| `SERPER_API_KEY` | Serper.dev News Search |
| `OPENAI_API_KEY` | OpenAI LLM |
| `SMTP_SERVER` | Email Relay (e.g., smtp.resend.com) |
| `SMTP_PORT` | Typically 465 or 587 |
| `SMTP_USER` | SMTP Username (e.g., 'resend') |
| `SMTP_PASS` | SMTP Password / API Key |
| `SENDER_EMAIL` | Verified sending address (e.g., alerts@ami-pulse.com) |
| `RECIPIENT_EMAILS`| Comma-separated list of target inboxes |

---

## ⏱️ Automation Schedule
The pipeline is orchestrated by `.github/workflows/market_pulse.yml`.
* **Execution Time:** Monday through Friday at 10:00 UTC (6:00 AM EDT).
* **Hard Limits:** The ingestion engine enforces a strict `MAX_DAILY_SCRAPES = 20` to protect free-tier API quotas. 

To manually trigger a run, navigate to the **Actions** tab in GitHub, select the workflow, and click **Run workflow**.