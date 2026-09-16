# Runbook: the run was green, but nobody received the email

**For:** whoever is on call for Market-Pulse, and Americhem IT when a message
has to be traced through the mail gateway.

**Symptom:** the `Market Pulse Pipeline` GitHub Actions run finished green,
the delivery step logged a send, Resend shows the message, and the recipients
have nothing in their inbox, junk folder, or Proofpoint digest.

This happened on 2026-09-16. Resend reported "Delivered" for all 41
recipients; Americhem's Proofpoint gateway had quarantined the message
because one linked article sat on a compromised domain (`chargedevs.com`).
The steps below are the path that found it, in the order to take them.

---

## 1. What "green run, no email" means

A green run proves only that the pipeline ran and that Resend accepted the
message. Everything after that is invisible to the run:

| The run says | It means | It does **not** mean |
|---|---|---|
| Delivery step green, `Email sent` logged | Resend accepted the message (HTTP 200) | anyone received it |
| Resend shows **Delivered** | Americhem's mail gateway accepted it at SMTP | it reached a mailbox |
| Nothing in the Proofpoint End User Digest | no *personal* Spam / Low Priority filing | it was not quarantined at the admin level |

So: if the log shows a send and Resend shows Delivered, the message was lost
**after** SMTP acceptance, inside Americhem's gateway. Go to step 4. If the log
or Resend say otherwise, steps 2 and 3 tell you what actually happened.

## 2. Read the delivery-step log

Open the run in GitHub Actions and expand **Run delivery_engine.py**. Search
for these lines (the phrases are quoted from the code, so search for them
verbatim).

**Did it find anything to send?**

- `log:created after the last production` — the normal case: "Fetched N
  intelligence record(s) created after the last production delivery at …".
  N is how many articles were in the window.
- `log:No prior production delivery recorded` — the fallback window was used
  (a fresh database, or the `delivered_at` column missing). Not itself a
  fault, but note it.
- `log:sending no-news notification` — the window was empty; a short
  "no significant market events" email went out instead of the digest. If
  that is unexpected, the problem is upstream in ingestion, not in delivery.
- `log:No macro summary found for run_date >=` — the executive summary was
  missing; the email still went out, without it.

**Did it render and send?**

- `log:Rendering email` — the report was assembled (with its critical /
  strategic / routine counts).
- `log:Email sent` — the send succeeded. The line reads
  `Email sent — subject: '…' | recipients: N | id: <resend-message-id>`.
  **Copy the id**: it opens the exact message in the Resend dashboard (step 3).
  Check that N is the size of the recipient pool you expect; the pool is the
  `RECIPIENT_EMAILS` secret, and a wrong secret sends to the wrong people
  with a green run.
- `log:Transient HTTP` — Resend returned a retryable status and the send was
  retried with backoff. Fine if an `Email sent` line follows.
- `log:Resend API returned HTTP` — a permanent error; the step is red and
  nothing was sent. The body of the response is on the same line.
- `log:Failed to stamp delivered_at on daily_summaries` — the email went out
  but the bookkeeping write failed. Harmless for the reader; the next run's
  window will simply reach back further.

**Was a link removed for safety?** (informational — these are the guards
working, not faults)

- `log:BLOCKED_DOMAIN — skipped pre-scrape` (ingestion step) — a candidate on
  a domain listed under `security.blocked_domains` was dropped.
- `log:UNSAFE_URL — skipped pre-scrape` (ingestion step) and
  `log:Safe Browsing flagged URL` — Google Safe Browsing flagged a page and
  it was dropped. `log:Link-reputation check off` means the
  `SAFE_BROWSING_API_KEY` secret is not set, so that check did not run.

**If the ingestion step is red** with `log:SYNTHESIS OUTAGE`, the delivery step
was skipped on purpose (the LLM was down and a "no news" email would have
been misleading). That is a red run, not this runbook; fix the LLM credential
or outage and re-run.

## 3. Check the Resend dashboard

1. Open [resend.com/emails](https://resend.com/emails) and search for the
   message id from the `Email sent` line (or by subject and date).
2. **Per-recipient events.** Each recipient shows a status: *Sent*,
   *Delivered*, *Bounced*, *Complained*, *Delivery delayed*. A bounce or a
   delay names the recipient and quotes the gateway's response; act on that
   text. If **every** recipient is *Delivered*, the message left Resend
   correctly and step 4 applies.
3. **Insights tab.** Resend lists deliverability signals for the message
   (authentication results, content flags such as images loaded from a
   domain other than the sending domain, link findings). On 2026-09-16 it
   flagged the logo, then served from `www.americhem.com`, as an
   impersonation signal; the logo is now embedded in the email. Anything
   listed here is a lead for the IT conversation in step 5.
4. Confirm the domain's DNS status under **Domains** is still verified (DKIM
   and SPF green). A lapsed record makes every message look forged.

## 4. Why Resend "Delivered" does not mean delivered

Americhem's mail exchanger is Proofpoint (`americhem.com` MX points at
`pphosted.com`). Proofpoint accepts the message at SMTP first and filters it
afterwards, so "Delivered" is the SMTP acceptance, nothing more. What
Proofpoint then does is invisible to Resend and to the run.

Two kinds of filtering exist, and only one is visible to recipients:

- **Personal filing** (Spam, Low Priority): the message appears in the
  recipient's Proofpoint End User Digest, and the recipient can release it.
- **Admin-level dispositions** (malware, impostor / brand-impersonation
  rules, phish, custom policies): the message is quarantined centrally and
  **never appears in the End User Digest**. Recipients see nothing at all.
  This is what happened on 2026-09-16, under a malware score driven by a
  single link to a compromised site.

A digest email carries every impostor signal on its own: an external sending
domain, "Americhem" in the subject, the Americhem brand, and about 40
internal addresses on one message. One bad link tips it over. The guards in
this repository (the blocked-domain list, the Safe Browsing check) exist to
keep the bad link out; they cannot make Proofpoint trust the message.

## 5. What to ask Americhem IT for

Send IT the message id, the sender address (the `SENDER_EMAIL` secret), the
subject line, and the send time in UTC, and ask for:

1. **A Proofpoint Smart Search** on the sender address for the send window.
   It shows the message's final disposition (delivered, quarantined, and
   under which policy or threat name) for every recipient. Ask for the
   disposition and the reason text; the reason names the trigger (on
   2026-09-16 it named the domain and "malicious injection code").
2. **Release** of the quarantined message if the content is safe, or agree
   that it will be re-sent after the fix (step 6) instead.
3. **An allow-list entry** for the sender address, so a future borderline
   score is not enough on its own. IT applied this on 2026-09-16 to the
   current sender.
4. **The trigger, in writing.** If the trigger was a link to a domain, that
   domain goes into the block list (step 7) before anything is re-sent.

## 6. Re-send the day's report without re-running ingestion

The production workflow has a delivery-only mode. It re-reads the rows
already stored for the day, re-builds the email (with the executive summary
ingestion wrote), and sends it to the production recipients; no article is
discovered, scraped, or scored again, so nothing is billed.

```bash
gh workflow run market_pulse.yml --ref main -f run_ingestion=false
```

Run it from a checkout of the repository (or add
`-R Phife726/market-pulse`). Watch the run with `gh run watch`.

Before you re-send:

- **Fix the cause first.** A re-send of the same content is quarantined the
  same way. If IT named a domain, block it (step 7) and merge that change
  before dispatching; the re-send picks up the new list without any
  database work.
- **Optional dry run to the QA pool.** The `Market Pulse Test Pipeline`
  workflow with `run_ingestion=false` and `send_email=true` sends the same
  report, marked `[TEST]`, only to the `TEST_RECIPIENT_EMAILS` pool. Use it
  to eyeball the email before the production re-send.
- **The whole day goes out again.** The re-send covers every article since
  the previous day's delivery, not only the ones added since the failed
  attempt, so recipients get one complete digest.

## 7. Block a domain, and remove anything already stored

### Block it (a config edit, no deploy, no database access)

Edit `market_pulse_config.yaml` at the top of the file and add the domain
under `security.blocked_domains`, one per line with the date and IT's reason
as a comment:

```yaml
security:
  blocked_domains:
    - chargedevs.com      # 2026-09-16: malicious injection code (IT sandbox analysis)
    - example-bad.net     # 2026-10-02: IT ticket 12345, credential-harvesting page
```

Rules for an entry: the bare registrable domain (`example.com`, never
`https://example.com/` or a path), which also blocks every subdomain. A
mis-shaped entry (a URL, a single word like `com`, or the list indented
wrongly) deliberately fails the next run at startup rather than silently
unblocking, so run `pytest tests/` before merging.

Once merged, the domain is blocked on both sides without touching the
database: new articles from it are never fetched, and articles already
stored are kept out of every part of the email (cards, Watch List, appendix,
and the executive summary's citations) from the next delivery onward.
Removing a domain from the list re-admits its stored rows, so remove one only
when IT clears it.

### Remove stored rows (optional, belt and braces)

Blocking already hides the rows. If IT wants the content gone from the
database as well, run this in the Supabase SQL editor. Look first, then
delete:

```sql
-- 1. Look: which stored articles point at the domain (any subdomain, any path)?
select id, url_hash, created_at, trigger_entity, headline, source_url
from daily_intelligence
where source_url ~* '^https?://([^/]+\.)?chargedevs\.com(/|$)';

-- 2. Delete exactly those rows.
delete from daily_intelligence
where source_url ~* '^https?://([^/]+\.)?chargedevs\.com(/|$)';
```

Replace `chargedevs\.com` with the domain in question (keep the backslash
before the dot). The executive summary's source list for that day may still
name the article; the block list withdraws that citation at render time, so
no further edit is needed.

---

## Quick reference

| Question | Where to look |
|---|---|
| Did the pipeline send? | Delivery step log: `Email sent … id: …` |
| Did Resend deliver? | resend.com/emails → the message id → per-recipient events |
| Any content or auth flags? | Resend → the message → Insights tab; Domains → DNS status |
| All Delivered, nobody got it? | Proofpoint admin quarantine → ask IT for a Smart Search (step 5) |
| Re-send today without re-billing | `gh workflow run market_pulse.yml --ref main -f run_ingestion=false` |
| Block a domain | `security.blocked_domains` in `market_pulse_config.yaml` |
| Purge stored rows | the SQL in step 7 |
