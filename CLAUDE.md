# Market-Pulse

## Project Purpose
Automated daily market intelligence pipeline to replace Moody's News Edge subscription.
Scrapes open-web news, applies LLM synthesis, delivers a BLUF-formatted daily email.

## Stack
- Python 3.10+
- Supabase (PostgreSQL via supabase-py)
- Serper.dev (news article URL discovery)
- Firecrawl (article content extraction)
- OpenAI gpt-4o-mini (synthesis and sentiment scoring)
- Resend (email delivery via SMTP)
- GitHub Actions (daily automation at 10:00 UTC / 6:00 AM EDT Mon–Fri)

## File Structure
market-pulse/
├── CLAUDE.md                    # this file
├── targets.yaml                 # entity Control Panel (non-technical editors use this)
├── ingestion_engine.py          # scrape → synthesize → store
├── delivery_engine.py           # fetch DB → format HTML → send email
├── schema.sql                   # Supabase table definition (run once)
├── requirements.txt             # Python dependencies
├── .env.example                 # template for local environment variables
├── .github/
│   └── workflows/
│       └── market_pulse.yml     # GitHub Actions scheduler
└── tests/
    └── test_pipeline.py         # smoke tests

## Key Design Decisions
- Serper.dev discovers article URLs → Firecrawl extracts content.
- URLs MUST be normalized (query parameters stripped) before SHA-256 hashing to prevent duplicates.
- MAX_DAILY_SCRAPES = 20 is strictly enforced to protect free-tier limits.
- source_url is injected into LLM prompt deterministically.
- targets.yaml is the single source of truth for what to monitor.