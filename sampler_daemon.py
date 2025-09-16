#!/usr/bin/env python3
"""
sampler_daemon.py — always-on sampler to persist wallet hashrate history.

Place next to app.py and ckpool_parser.py and run as a service (systemd) or via nohup/screen.
"""

import time, os, sys, traceback, re
from pathlib import Path
from typing import Optional, Any

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

# config
SAMPLE_EVERY_SEC = int(os.getenv("SAMPLE_EVERY_SEC", "30"))
HIST_WINDOW_SEC   = int(os.getenv("HIST_WINDOW_SEC", str(24*3600)))
DB_PATH = os.getenv("CKPOOL_DB", str(BASE / "ckpool.sqlite"))

# import ckpool parser and helpers
try:
    import ckpool_parser as parser
except Exception:
    # try dynamic import for robustness
    import importlib.util
    p = BASE / "ckpool_parser.py"
    spec = importlib.util.spec_from_file_location("ckpool_parser", str(p))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    parser = module  # type: ignore

# helper: ensure table exists
def ensure_table(conn):
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS wallet_history (
                wallet TEXT NOT NULL,
                ts INTEGER NOT NULL,
                hashrate REAL NOT NULL
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wallet_history_wallet_ts ON wallet_history(wallet, ts);")
        conn.commit()
    except Exception:
        try: conn.rollback()
        except: pass

# parse hashrate fallback (simple, similar to client chart code)
def parse_hashrate_str(s: Optional[Any]) -> float:
    if s is None: return 0.0
    try:
        return float(s)
    except Exception:
        ss = str(s).strip()
        ss = re.sub(r"[,\s]", "", ss)
        m = re.match(r"^([0-9]*\.?[0-9]+)\s*([kKmMgGtTpP]?)$", ss)
        if m:
            num = float(m.group(1))
            unit = m.group(2).upper() if m.group(2) else ""
            mult = {'':1, 'K':1e3, 'M':1e6, 'G':1e9, 'T':1e12, 'P':1e15}.get(unit, 1)
            return num * mult
        try:
            # last ditch: remove non-digit, parse
            import re as _re
            s2 = _re.sub(r"[^\d\.]", "", ss)
            return float(s2) if s2 else 0.0
        except Exception:
            return 0.0

def get_db_conn():
    # prefer parser._connect if available
    try:
        conn = parser._connect(Path(DB_PATH))
        return conn
    except Exception:
        import sqlite3
        conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
        conn.row_factory = None
        try:
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA busy_timeout=10000;")
            conn.commit()
        except Exception:
            pass
        return conn

def main_loop():
    while True:
        try:
            # build snapshot from parser (safe). Use CKPoolState if available; else try DB snapshot fallback.
            try:
                st = parser.CKPoolState(db_path=DB_PATH, log_path=os.getenv("CKPOOL_LOG", ""))
                snap = st.snapshot()
            except Exception:
                # fallback: open DB and query user_stats (coarse)
                snap = {"pool": {}, "users": []}
                try:
                    conn = get_db_conn()
                    cur = conn.execute("""
                        SELECT address, hashrate1m FROM user_stats
                        """)
                    for addr, h1 in cur.fetchall():
                        snap["users"].append({"wallet": addr, "address": addr, "hashrate1m": h1})
                    try: conn.close()
                    except: pass
                except Exception:
                    snap = {"pool": {}, "users": []}

            ts = int(time.time())
            bat = []
            for u in (snap.get("users") or []):
                addr = u.get("wallet") or u.get("address")
                if not addr:
                    continue
                raw = u.get("hashrate1m") or u.get("hashrate") or ""
                v = parse_hashrate_str(raw)
                bat.append((addr, ts, float(v)))

            if bat:
                conn = get_db_conn()
                ensure_table(conn)
                try:
                    conn.executemany("INSERT INTO wallet_history(wallet, ts, hashrate) VALUES (?, ?, ?);", bat)
                    conn.commit()
                except Exception:
                    try: conn.rollback()
                    except: pass
                try: conn.close()
                except: pass

        except Exception:
            # don't crash; sleep and continue
            traceback.print_exc()

        time.sleep(max(1, SAMPLE_EVERY_SEC))

if __name__ == "__main__":
    main_loop()
