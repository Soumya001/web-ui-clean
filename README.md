\# XP Pool WebUI — Multi-Coin Skeleton



Production-oriented Web UI for mining pools.  

Current state: \*\*Bitcoin (BTC)\*\* dashboard active; other coins present as unavailable pages until configured.



\## Highlights

\- Clean multi-coin landing page + coin selector

\- Live BTC dashboard with node and pool stats

\- Graceful fallback for unconfigured coins (demo mode)

\- Lightweight Flask backend, SQLite for local snapshots

\- Designed for easy extension to additional coins



\## Quickstart

1\. Copy `config.example.json` → `config.json`, fill in RPC credentials.

2\. Create and activate a Python virtual environment:

&nbsp;  ```bash

&nbsp;  python -m venv venv

&nbsp;  source venv/bin/activate

&nbsp;  pip install -r requirements.txt



