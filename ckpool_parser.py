#!/usr/bin/env python3
"""
ckpool_parser.py — CKPool log/ledger -> SQLite + live state for WebUI

- One row per wallet (no per-worker rows).
- Tracks worker names from Authorised/Dropped lines and exposes active_workers list.
- Copies wallet totals into that single row; UI will display names in the Worker cell.
"""

from __future__ import annotations

import argparse, csv, datetime as dt, io, json, os, re, sqlite3, threading, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

# ---------------- Config ----------------
WORKER_TIMEOUT = int(os.getenv("WORKER_TIMEOUT", "300"))  # seconds

# ---------------- Retry-safe commit ----------------
def safe_commit(conn: sqlite3.Connection, retries: int = 5, delay: float = 0.2) -> None:
    for i in range(retries):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(delay)
                continue
            raise
    raise sqlite3.OperationalError("commit failed after retries (database locked)")

def execute_with_retry(conn: sqlite3.Connection, sql: str, params: tuple = (), retries: int = 5, delay: float = 0.2):
    for i in range(retries):
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(delay * (i + 1))
                continue
            raise
    raise sqlite3.OperationalError(f"SQL failed after {retries} retries: {sql}")

# ---------------- Regex patterns ----------------
# Accept full addresses or truncated 'prefix...' forms (we'll treat merged-suffix cases specially)
ADDR_RE   = r"(?:[13][a-km-zA-HJ-NP-Z1-9]{5,}|[tb]?c1[0-9ac-hj-np-z]{5,})(?:\.\.\.)?"
WORKER_RE = r"[A-Za-z0-9_\-\.]{1,64}"

POOL_JSON_RE = re.compile(
    r"""^(?:\[(?P<ts>[\d\-:\. ]+)\]\s*)?Pool:\s*(?P<payload>\{.*\})\s*$""",
    re.DOTALL,
)

USER_RE = re.compile(
    rf"""^(?:\[(?P<ts>[\d\-:\. ]+)\]\s*)?User\s+(?P<addr>{ADDR_RE})\s*:?\s*(?P<payload>\{{.*\}})\s*$""",
    re.IGNORECASE,
)

# Strict patterns: wallet.worker (we'll validate further in code)
AUTH_RE = re.compile(
    rf"""^(?:\[(?P<ts>[\d\-:\. ]+)\]\s*)?
         Authorised\s+client\s+\d+\s+\S+\s+worker\s+(?P<wallet>{ADDR_RE})\.(?P<worker>{WORKER_RE})\s+as\s+user\s+(?P<addr>{ADDR_RE})\b.*$""",
    re.IGNORECASE | re.VERBOSE,
)

DROP_RE = re.compile(
    rf"""^(?:\[(?P<ts>[\d\-:\. ]+)\]\s*)?
         Dropped\s+client\s+\d+\s+\S+\s+user\s+(?P<addr>{ADDR_RE})\s+worker\s+(?P<wallet>{ADDR_RE})\.(?P<worker>{WORKER_RE})\b.*$""",
    re.IGNORECASE | re.VERBOSE,
)

# Fallbacks: wallet-only (no dot) — treat as wallet with worker "X"
_AUTH_NO_WORKER_RE = re.compile(
    rf"""^(?:\[(?P<ts>[\d\-:\. ]+)\]\s*)?
         Authorised\s+client\s+\d+\s+\S+\s+worker\s+(?P<wallet_only>{ADDR_RE})\s+as\s+user\s+(?P<addr>{ADDR_RE})\b.*$""",
    re.IGNORECASE | re.VERBOSE,
)
_DROP_NO_WORKER_RE = re.compile(
    rf"""^(?:\[(?P<ts>[\d\-:\. ]+)\]\s*)?
         Dropped\s+client\s+\d+\s+\S+\s+user\s+(?P<addr>{ADDR_RE})\s+worker\s+(?P<wallet_only>{ADDR_RE})\b.*$""",
    re.IGNORECASE | re.VERBOSE,
)

# Share patterns (various formats)
SHARE_PATS = [
    re.compile(
        rf"""^(?:\[(?P<ts>[\d\-:\. ]+)\]\s*)?Share\s+(?P<status>accepted|rejected)\s+from\s+(?P<id>{ADDR_RE}(?:\.[A-Za-z0-9_\-\.]{{1,64}})?)\s+(?:at\s+)?diff\s+(?P<rawdiff>\d+(?:/\d+)?).*$""",
        re.IGNORECASE,
    ),
    re.compile(
        rf"""^(?:\[(?P<ts>[\d\-:\. ]+)\]\s*)?(?:Accepted|Rejected)\s+share\s+from\s+(?P<id>{ADDR_RE}(?:\.[A-Za-z0-9_\-\.]{{1,64}})?)\s+(?:at\s+)?diff\s+(?P<rawdiff>\d+(?:/\d+)?).*$""",
        re.IGNORECASE,
    ),
]

TS_FORMATS = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]

# ---------------- Units helpers ----------------
UNIT_MAP = {"":1.0,"H":1.0,"K":1e3,"KH":1e3,"M":1e6,"MH":1e6,"G":1e9,"GH":1e9,"T":1e12,"TH":1e12,"P":1e15,"PH":1e15,"E":1e18,"EH":1e18}

def _parse_hashrate_to_hs(v: Any) -> float:
    if v is None: return 0.0
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().upper().replace(" ","")
    if s in ("—","-",""): return 0.0
    try: return float(s)
    except Exception: pass
    m = re.match(r"([0-9]*\.?[0-9]+)([A-Z]{0,2})$", s)
    if not m: return 0.0
    num = float(m.group(1)); unit = m.group(2) or ""
    return num * UNIT_MAP.get(unit, 1.0)

def _human_hashrate(hs: float) -> str:
    for u,m in (("E",1e18),("P",1e15),("T",1e12),("G",1e9),("M",1e6),("K",1e3)):
        if hs >= m: return f"{hs/m:.2f}{u}"
    return f"{hs:.0f}"

# ---------------- SQLite helpers ----------------
def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path),
        timeout=30.0,
        check_same_thread=False,
        detect_types=sqlite3.PARSE_DECLTYPES,
    )
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA busy_timeout=10000;")
        safe_commit(conn)
    except Exception:
        pass
    return conn

def _ensure_workers_seen_columns(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info('workers_seen');").fetchall()}
    if "active" not in cols:
        conn.execute("ALTER TABLE workers_seen ADD COLUMN active INTEGER NOT NULL DEFAULT 1;")

def init_db(conn: sqlite3.Connection) -> None:
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS pool_metrics (
        ts INTEGER PRIMARY KEY,
        hashrate1m TEXT,
        hashrate5m TEXT,
        hashrate15m TEXT,
        hashrate1hr TEXT,
        hashrate6hr TEXT,
        hashrate1d TEXT,
        hashrate7d TEXT,
        runtime INTEGER,
        lastupdate INTEGER,
        users INTEGER,
        workers INTEGER,
        idle INTEGER,
        disconnected INTEGER,
        accepted INTEGER,
        rejected INTEGER,
        bestshare INTEGER,
        sps1m REAL,
        sps5m REAL,
        diff REAL
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS user_stats (
        address TEXT PRIMARY KEY,
        ts INTEGER,
        hashrate1m TEXT,
        hashrate5m TEXT,
        hashrate1hr TEXT,
        hashrate1d TEXT,
        hashrate7d TEXT,
        lastshare INTEGER,
        workers INTEGER,
        shares INTEGER,
        bestshare REAL,
        bestever REAL,
        authorised INTEGER
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS workers_seen (
        wallet TEXT,
        worker TEXT,
        last_seen INTEGER,
        active INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY(wallet, worker)
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS shares (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        status TEXT CHECK(status IN ('accepted','rejected')) NOT NULL,
        address TEXT NOT NULL,
        worker TEXT,
        rawdiff TEXT,
        scaled REAL,
        ip TEXT
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS blocks (
        height INTEGER PRIMARY KEY,
        blockhash TEXT,
        ts INTEGER,
        reward_btc REAL,
        txid TEXT,
        address TEXT
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_pool_ts ON pool_metrics(ts);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_shares_addr_ts ON shares(address, ts);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_workers_wallet ON workers_seen(wallet);")
    safe_commit(conn)
    _ensure_workers_seen_columns(conn)
    c.execute("CREATE INDEX IF NOT EXISTS idx_workers_active ON workers_seen(wallet, active, last_seen DESC);")
    safe_commit(conn)

def set_meta(conn: sqlite3.Connection, key: str, val: str) -> None:
    conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;", (key,val))
    safe_commit(conn)

def get_meta(conn: sqlite3.Connection, key: str) -> Optional[str]:
    r = conn.execute("SELECT value FROM meta WHERE key=?;", (key,)).fetchone()
    return r[0] if r else None

# ---------------- parse helpers ----------------
def parse_bracket_ts(v: Optional[str]) -> int:
    if not v: return int(dt.datetime.utcnow().timestamp())
    for fmt in TS_FORMATS:
        try: return int(dt.datetime.strptime(v.strip(), fmt).timestamp())
        except Exception: continue
    try: return int(dt.datetime.fromisoformat(v.strip()).timestamp())
    except Exception: return int(dt.datetime.utcnow().timestamp())

def parse_pool_json_line(line: str) -> Optional[Dict[str, Any]]:
    m = POOL_JSON_RE.match(line.strip())
    if not m: return None
    ts = parse_bracket_ts(m.group("ts"))
    try:
        data = json.loads(m.group("payload"))
    except json.JSONDecodeError:
        return None
    out: Dict[str, Any] = {"ts": ts}
    for k in ("hashrate1m","hashrate5m","hashrate15m","hashrate1hr","hashrate6hr","hashrate1d","hashrate7d"):
        if k in data: out[k] = data[k]
    for k in ("runtime","lastupdate","Users","Workers","Idle","Disconnected"):
        if k in data: out[k.lower() if k[0].isupper() else k] = data[k]
    for k in ("accepted","rejected","bestshare","diff","SPS1m","SPS5m"):
        if k in data: out[k.lower()] = data[k]
    return out

def parse_user_line(line: str) -> Optional[Dict[str, Any]]:
    m = USER_RE.match(line.strip())
    if not m: return None
    ts = parse_bracket_ts(m.group("ts"))
    addr = m.group("addr")
    try:
        data = json.loads(m.group("payload"))
    except json.JSONDecodeError:
        return None
    return {"ts": ts, "address": addr, "data": data}

def _contains_truncator(s: Optional[str]) -> bool:
    """Return True if the string contains the truncation ellipsis or suspicious pattern that indicates merged suffix."""
    if not s: return False
    return "..." in s or s.endswith("...") or s.startswith("...")

def _clean_worker(worker: Optional[str], wallet: str) -> str:
    """Normalize a worker string. If suspicious, return 'X'."""
    if worker is None:
        return "X"
    w = str(worker).strip()
    # strip edge dots/spaces
    w = w.strip(". ").strip()
    # reject very short or obviously invalid
    if not w or w == wallet or "..." in w or len(w) > 64:
        return "X"
    if re.fullmatch(WORKER_RE, w):
        return w
    return "X"

def parse_authorised_line(line: str) -> Optional[Dict[str, Any]]:
    sline = line.strip()
    # strict wallet.worker first
    m = AUTH_RE.match(sline)
    if m:
        wallet = m.group("wallet")
        worker = m.group("worker")
        # If either side contains the truncation pattern '...' treat as merged-suffix and ignore (case 2)
        if _contains_truncator(wallet) or _contains_truncator(worker):
            # merged/truncated wallet or worker — treat as "no worker" (use placeholder "X")
            return {"ts": parse_bracket_ts(m.group("ts")), "wallet": wallet, "worker": "X"}
        # otherwise clean worker and return
        worker_clean = _clean_worker(worker, wallet)
        return {"ts": parse_bracket_ts(m.group("ts")), "wallet": wallet, "worker": worker_clean}
    # fallback: wallet-only (no dot) -> worker "X"
    m2 = _AUTH_NO_WORKER_RE.match(sline)
    if m2:
        wallet_only = m2.group("wallet_only")
        # if the wallet-only has a truncator we still accept but worker is "X"
        return {"ts": parse_bracket_ts(m2.group("ts")), "wallet": wallet_only, "worker": "X"}
    return None

def parse_dropped_line(line: str) -> Optional[Dict[str, Any]]:
    sline = line.strip()
    m = DROP_RE.match(sline)
    if m:
        wallet = m.group("wallet")
        worker = m.group("worker")
        if _contains_truncator(wallet) or _contains_truncator(worker):
            # merged/truncated wallet or worker — treat as "no worker" (use placeholder "X")
            return {"ts": parse_bracket_ts(m.group("ts")), "wallet": wallet, "worker": "X"}
        worker_clean = _clean_worker(worker, wallet)
        return {"ts": parse_bracket_ts(m.group("ts")), "wallet": wallet, "worker": worker_clean}
    m2 = _DROP_NO_WORKER_RE.match(sline)
    if m2:
        wallet_only = m2.group("wallet_only")
        return {"ts": parse_bracket_ts(m2.group("ts")), "wallet": wallet_only, "worker": "X"}
    return None

def _split_addr_worker(s: str) -> Tuple[str, Optional[str]]:
    if "." in s:
        a,w = s.split(".",1); return a,w
    return s, None

def _scale_diff(raw: str) -> Optional[float]:
    try:
        if "/" in raw:
            a,b = raw.split("/",1); return float(a)/float(b)
        return float(raw)
    except Exception:
        return None

def parse_share_line(line: str) -> Optional[Dict[str, Any]]:
    s = line.strip()
    for pat in SHARE_PATS:
        m = pat.match(s)
        if m:
            ts = parse_bracket_ts(m.group("ts"))
            combined = m.group("id"); addr, worker = _split_addr_worker(combined)
            rawdiff = m.group("rawdiff"); scaled = _scale_diff(rawdiff) if rawdiff else None
            status = "accepted" if ("Accepted" in s or "accepted" in s) else "rejected"
            return {"ts": ts, "status": status, "address": addr, "worker": worker, "rawdiff": rawdiff, "scaled": scaled, "ip": None}
    return None

# ---------------- ingest ----------------

def upsert_user_stats(conn: sqlite3.Connection, rec: Dict[str, Any]) -> None:
    d = rec["data"]; wallet = rec["address"]
    key_addr = wallet
    for k in ("user","address"):
        val = d.get(k)
        if isinstance(val,str) and val.startswith(wallet + "."):
            key_addr = val; break
    conn.execute(
        """
        INSERT INTO user_stats(address, ts, hashrate1m, hashrate5m, hashrate1hr, hashrate1d, hashrate7d,
                               lastshare, workers, shares, bestshare, bestever, authorised)
        VALUES (:address,:ts,:h1,:h5,:h1h,:h1d,:h7d,:last,:workers,:shares,:best,:bestever,:auth)
        ON CONFLICT(address) DO UPDATE SET
            ts=excluded.ts, hashrate1m=excluded.hashrate1m, hashrate5m=excluded.hashrate5m,
            hashrate1hr=excluded.hashrate1hr, hashrate1d=excluded.hashrate1d, hashrate7d=excluded.hashrate7d,
            lastshare=excluded.lastshare, workers=excluded.workers, shares=excluded.shares,
            bestshare=excluded.bestshare, bestever=excluded.bestever, authorised=excluded.authorised;
        """,
        {
            "address": key_addr, "ts": rec["ts"],
            "h1": d.get("hashrate1m"), "h5": d.get("hashrate5m"), "h1h": d.get("hashrate1hr"),
            "h1d": d.get("hashrate1d"), "h7d": d.get("hashrate7d"),
            "last": d.get("lastshare"), "workers": d.get("workers"), "shares": d.get("shares"),
            "best": d.get("bestshare") or d.get("bestever"), "bestever": d.get("bestever"),
            "auth": d.get("authorised"),
        },
    )

def upsert_worker_seen(conn: sqlite3.Connection, wallet: str, worker: str, ts: int, active: int) -> None:
    conn.execute(
        """INSERT INTO workers_seen(wallet,worker,last_seen,active) VALUES(?,?,?,?)
           ON CONFLICT(wallet,worker) DO UPDATE SET last_seen=excluded.last_seen, active=excluded.active;""",
        (wallet, worker, ts, int(bool(active))),
    )

def upsert_pool_metrics(conn: sqlite3.Connection, rec: Dict[str, Any]) -> None:
    cols = ["hashrate1m","hashrate5m","hashrate15m","hashrate1hr","hashrate6hr","hashrate1d","hashrate7d",
            "runtime","lastupdate","users","workers","idle","disconnected",
            "accepted","rejected","bestshare","sps1m","sps5m","diff"]
    sets = ",".join([f"{c}=COALESCE(excluded.{c}, {c})" for c in cols])
    params = {c: rec.get(c) for c in cols}
    params["ts"] = rec["ts"]
    qcols = ",".join(["ts"] + cols)
    qvals = ",".join([":ts"] + [f":{c}" for c in cols])
    conn.execute(
        f"""INSERT INTO pool_metrics({qcols}) VALUES({qvals})
            ON CONFLICT(ts) DO UPDATE SET {sets};""",
        params,
    )

def expire_stale_workers(conn: sqlite3.Connection, now_ts: int, timeout_s: int) -> None:
    conn.execute(
        "UPDATE workers_seen SET active=0 WHERE active=1 AND (? - last_seen) > ?;",
        (now_ts, timeout_s),
    )

def _active_workers_for_wallet(conn: sqlite3.Connection, wallet: str, now_ts: int, timeout_s: int) -> List[str]:
    rows = conn.execute(
        "SELECT worker FROM workers_seen WHERE wallet=? AND active=1 AND last_seen>=? ORDER BY last_seen DESC;",
        (wallet, now_ts - timeout_s),
    ).fetchall()
    return [r[0] for r in rows]

def _refresh_single_worker_if_safe(conn: sqlite3.Connection, wallet: str, workers_count: Optional[int], now_ts: int, timeout_s: int) -> None:
    """
    If snapshot says workers==1 *and* we currently know exactly one active name,
    bump its last_seen (and active=1) so the name never expires while mining continues.
    """
    if workers_count != 1:
        return
    actives = _active_workers_for_wallet(conn, wallet, now_ts, timeout_s)
    if len(actives) != 1:
        return
    worker = actives[0]
    conn.execute(
        "UPDATE workers_seen SET last_seen=?, active=1 WHERE wallet=? AND worker=?;",
        (now_ts, wallet, worker),
    )

def _to_int_safe(v: Any) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        try:
            return int(float(str(v).strip()))
        except Exception:
            return None

def ingest_log(conn: sqlite3.Connection, log_path: Path, from_bytes: int = 0) -> int:
    """
    Read a ckpool log file and ingest events into the DB:
      - Pool JSON lines -> pool_metrics
      - User snapshot lines -> user_stats (existing behaviour)
      - Authorised lines -> workers_seen (active)
      - Dropped lines -> workers_seen (inactive)
      - Share lines -> shares table (and upsert worker as active)
    Returns number of processed events (approx).
    """
    init_db(conn)
    key = f"log_ingest_cursor:{str(log_path)}"
    if from_bytes == 0:
        saved = get_meta(conn, key)
        if saved and saved.isdigit():
            from_bytes = int(saved)

    n = 0
    with log_path.open("rb") as f:
        if from_bytes:
            try:
                f.seek(from_bytes)
            except Exception:
                pass

        while True:
            b = f.readline()
            if not b:
                break
            try:
                line = b.decode("utf-8", errors="replace").rstrip("\n")
            except Exception:
                continue

            # Pool JSON status
            pj = parse_pool_json_line(line)
            if pj:
                try:
                    upsert_pool_metrics(conn, pj)
                except Exception:
                    pass
                n += 1
                continue

            # Full snapshot line for a user
            usr = parse_user_line(line)
            if usr:
                try:
                    # update user_stats row
                    upsert_user_stats(conn, usr)
                    n += 1
                except Exception:
                    # non-fatal
                    pass

                # preserve previous safety behavior: if snapshot reports workers>0,
                # try to bump an existing workers_seen entry so the UI keeps a worker name
                try:
                    d = usr.get("data") or {}
                    wc = _to_int_safe(d.get("workers"))
                    if wc and wc > 0:
                        cur = conn.execute(
                            "SELECT worker FROM workers_seen WHERE wallet=? ORDER BY last_seen DESC LIMIT 1;",
                            (usr["address"],),
                        ).fetchone()
                        if cur and cur[0]:
                            upsert_worker_seen(conn, usr["address"], cur[0], usr["ts"], active=1)
                except Exception:
                    pass

                # keep single-worker refresh behavior
                try:
                    _refresh_single_worker_if_safe(conn, usr["address"], _to_int_safe(usr.get("data", {}).get("workers")), usr["ts"], WORKER_TIMEOUT)
                except Exception:
                    pass

                continue

            # Authorised client (explicit worker name)
            auth = parse_authorised_line(line)
            if auth:
                try:
                    # mark worker active with the timestamp from the log
                    upsert_worker_seen(conn, auth["wallet"], auth["worker"], auth["ts"], active=1)
                    n += 1
                except Exception:
                    pass
                continue

            # Dropped client (worker left / disconnected) -> mark inactive
            drop = parse_dropped_line(line)
            if drop:
                try:
                    upsert_worker_seen(conn, drop["wallet"], drop["worker"], drop["ts"], active=0)
                    n += 1
                except Exception:
                    pass
                continue

            # Share line -> insert into shares and mark worker active (if provided)
            share = parse_share_line(line)
            if share:
                try:
                    # insert share record (best-effort)
                    conn.execute(
                        "INSERT INTO shares(ts,status,address,worker,rawdiff,scaled,ip) VALUES(?,?,?,?,?,?,?);",
                        (share.get("ts"), share.get("status"), share.get("address"), share.get("worker"), share.get("rawdiff"), share.get("scaled"), share.get("ip"))
                    )
                    # if worker fragment present, mark it active with share ts
                    if share.get("worker"):
                        upsert_worker_seen(conn, share.get("address"), share.get("worker"), share.get("ts"), active=1)
                    n += 1
                except Exception:
                    # ignore failures on share insert
                    pass
                continue

        # commit once per ingest call (safe)
        try:
            safe_commit(conn)
        except Exception:
            # ignore commit errors here (they surface elsewhere)
            pass

        pos = f.tell()

    set_meta(conn, key, str(pos))
    return n

# ---------------- Blocks ledger (rewards) ----------------

def _to_int_ts(v: Any) -> Optional[int]:
    if v is None or v == "": return None
    if isinstance(v,(int,float)): return int(v // 1000) if v > 10_000_000_000 else int(v)
    s = str(v).strip()
    if s.isdigit(): return int(s)
    for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%dT%H:%M:%S","%Y-%m-%d"):
        try: return int(dt.datetime.strptime(s, fmt).timestamp())
        except Exception: continue
    try: return int(dt.datetime.fromisoformat(s).timestamp())
    except Exception: return None

def _to_float(v: Any) -> Optional[float]:
    try: return float(v)
    except Exception: return None

def _parse_csv_blocks(fh: io.TextIOBase) -> Iterable[Dict[str, Any]]:
    rd = csv.DictReader(fh)
    for row in rd:
        yield {
            "height": int(row.get("height") or row.get("block_height") or 0),
            "blockhash": row.get("hash") or row.get("blockhash"),
            "ts": _to_int_ts(row.get("time") or row.get("timestamp")),
            "reward_btc": _to_float(row.get("reward_btc") or row.get("reward")),
            "txid": row.get("txid"),
            "address": row.get("address") or row.get("miner") or row.get("wallet"),
        }

def _parse_jsonl_blocks(fh: io.TextIOBase) -> Iterable[Dict[str, Any]]:
    for line in fh:
        line=line.strip()
        if not line: continue
        d=json.loads(line)
        yield {
            "height": int(d.get("height") or d.get("block_height") or 0),
            "blockhash": d.get("hash") or d.get("blockhash"),
            "ts": _to_int_ts(d.get("time") or d.get("timestamp")),
            "reward_btc": _to_float(d.get("reward_btc") or d.get("reward")),
            "txid": d.get("txid"),
            "address": d.get("address") or d.get("miner") or d.get("wallet"),
        }

def _parse_json_blocks(fh: io.TextIOBase) -> Iterable[Dict[str, Any]]:
    obj = json.load(fh)
    it = obj if isinstance(obj, list) else [obj]
    for d in it:
        yield {
            "height": int(d.get("height") or d.get("block_height") or 0),
            "blockhash": d.get("hash") or d.get("blockhash"),
            "ts": _to_int_ts(d.get("time") or d.get("timestamp")),
            "reward_btc": _to_float(d.get("reward_btc") or d.get("reward")),
            "txid": d.get("txid"),
            "address": d.get("address") or d.get("miner") or d.get("wallet"),
        }

def ingest_blocks_ledger(conn: sqlite3.Connection, path: Path) -> int:
    init_db(conn)
    ext = path.suffix.lower()
    n = 0
    if ext == ".csv":
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for rec in _parse_csv_blocks(fh): _upsert_block(conn, rec); n += 1
    elif ext in (".jsonl",".ndjson",".jsonlines"):
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for rec in _parse_jsonl_blocks(fh): _upsert_block(conn, rec); n += 1
    elif ext == ".json":
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for rec in _parse_json_blocks(fh): _upsert_block(conn, rec); n += 1
    else:
        raise ValueError(f"Unsupported ledger format: {ext}")
    safe_commit(conn)
    return n

def _upsert_block(conn: sqlite3.Connection, rec: Dict[str, Any]) -> None:
    conn.execute(
        """INSERT INTO blocks(height,blockhash,ts,reward_btc,txid,address)
           VALUES(:height,:blockhash,:ts,:reward_btc,:txid,:address)
           ON CONFLICT(height) DO UPDATE SET
             blockhash=COALESCE(excluded.blockhash,blocks.blockhash),
             ts=COALESCE(excluded.ts,blocks.ts),
             reward_btc=COALESCE(excluded.reward_btc,blocks.reward_btc),
             txid=COALESCE(excluded.txid,blocks.txid),
             address=COALESCE(excluded.address,blocks.address);""",
        rec,
    )

# ---------------- queries/state ----------------

def get_pool_snapshot(conn: sqlite3.Connection) -> Dict[str, Any]:
    r = conn.execute(
        """SELECT ts, hashrate1m, hashrate5m, hashrate15m, hashrate1hr, hashrate6hr, hashrate1d, hashrate7d,
                  runtime, lastupdate, users, workers, idle, disconnected,
                  accepted, rejected, bestshare, sps1m, sps5m, diff
           FROM pool_metrics
           ORDER BY ts DESC LIMIT 1;"""
    ).fetchone()
    if not r:
        return {}
    keys = ["ts","hashrate1m","hashrate5m","hashrate15m","hashrate1hr","hashrate6hr","hashrate1d","hashrate7d",
            "runtime","lastupdate","Users","Workers","Idle","Disconnected",
            "accepted","rejected","bestshare","SPS1m","SPS5m","diff"]
    return dict(zip(keys, r))

def get_pool_runtime_seconds(conn: sqlite3.Connection) -> int:
    now = int(dt.datetime.utcnow().timestamp())
    mins = []
    for t in ("pool_metrics","user_stats","shares"):
        r = conn.execute(f"SELECT MIN(ts) FROM {t}").fetchone()
        if r and r[0]: mins.append(int(r[0]))
    earliest = min(mins) if mins else None
    return max(0, now - earliest) if earliest else 0

def get_wallet_rewards(conn: sqlite3.Connection, address: str) -> List[Dict[str, Any]]:
    cur = conn.execute("SELECT height,blockhash,ts,reward_btc,txid FROM blocks WHERE address=? ORDER BY ts DESC;", (address,))
    return [{"height":r[0],"hash":r[1],"ts":r[2],"reward_btc":r[3],"txid":r[4]} for r in cur.fetchall()]

class CKPoolState:
    """Builds snapshot either from HTTP status JSON or from the SQLite fed by ckpool.log.

    NOTE: this version opens a new sqlite connection for each refresh/build operation.
    That avoids sharing a single Connection object between threads (which caused
    'database is locked' when multiple Gunicorn threads tried to write).
    """

    def __init__(self, db_path: str = "ckpool.sqlite", log_path: Optional[str] = None, status_url: Optional[str] = None):
        self.db_path = Path(db_path)
        # do NOT keep a long-lived self.conn here — open per-operation
        # Ensure DB schema exists by briefly opening a connection
        try:
            c = _connect(self.db_path)
            init_db(c)
            try:
                safe_commit(c)
            except Exception:
                pass
            c.close()
        except Exception:
            pass

        self.log_path = Path(log_path) if log_path else None
        self.status_url = status_url.strip() if status_url else ""
        self._last_refresh_ts = 0
        self._snapshot: Dict[str, Any] = {"pool": {}, "users": []}
        self._lock = threading.Lock()

    def refresh(self) -> None:
        """Refresh snapshot (throttled to once per second by timestamp)."""
        now = int(dt.datetime.utcnow().timestamp())
        if now == self._last_refresh_ts:
            return
        self._last_refresh_ts = now

        if self.status_url and requests:
            # HTTP-based status (no DB needed)
            self._snapshot = self._fetch_status_http()
        else:
            # Build snapshot from DB/log using a fresh connection
            try:
                self._snapshot = self._build_from_log()
            except Exception:
                # keep previous snapshot on error
                return

    def snapshot(self) -> Dict[str, Any]:
        if not self._snapshot.get("pool") and not self._snapshot.get("users"):
            self.refresh()
        return self._snapshot

    def connections_snapshot(self) -> List[Dict[str, Any]]:
        return []

    # ---- internals ----

    def _fetch_status_http(self) -> Dict[str, Any]:
        try:
            r = requests.get(self.status_url, timeout=3)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "status" in data and isinstance(data["status"], dict):
                data = data["status"]
            if not isinstance(data, dict):
                return {"pool": {}, "users": []}
            pool = data.get("pool") or {}
            users = data.get("users") or []
            for u in users:
                wallet = u.get("wallet") or (u.get("user") or "").split(".",1)[0]
                u["wallet"] = wallet
                u["address"] = wallet
                wrk = ""
                user_s = str(u.get("user") or "")
                if user_s.startswith(wallet + "."):
                    wrk = user_s[len(wallet)+1:]
                u["worker"] = wrk
            return {"pool": pool, "users": users, "totals": {}}
        except Exception:
            return {"pool": {}, "users": [], "totals": {}}

    def _build_from_log(self) -> Dict[str, Any]:
        """Aggregate to one row per wallet and attach active_workers.

        Uses a short-lived DB connection to avoid cross-thread connection sharing.
        This version is defensive about DB locks and ensures all recent active
        worker names are returned (deduped and ordered by recency).
        """
        try:
            lookback = int(os.getenv("LOOKBACK_SEC", "86400"))
        except Exception:
            lookback = 86400

        conn = _connect(self.db_path)
        try:
            # Ensure schema exists
            init_db(conn)

            # Ingest any new log lines (safe; ingest_log commits)
            if self.log_path and self.log_path.exists():
                try:
                    ingest_log(conn, self.log_path)
                except Exception:
                    # swallow; we prefer returning a snapshot even if ingest fails
                    pass

            pool = get_pool_snapshot(conn)
            runtime_s = get_pool_runtime_seconds(conn)
            now = int(dt.datetime.utcnow().timestamp())

            # Expire stale workers (best-effort)
            try:
                expire_stale_workers(conn, now, WORKER_TIMEOUT)
            except Exception:
                pass

            # Latest snapshot per *address* (wallet or wallet.worker), limited by lookback
            raw_rows = conn.execute(
                """
                SELECT us.address, us.ts, us.hashrate1m, us.hashrate5m, us.hashrate1hr,
                       us.lastshare, us.workers, us.shares, us.bestshare
                FROM user_stats us
                JOIN (
                    SELECT address, MAX(ts) AS max_ts
                    FROM user_stats
                    GROUP BY address
                ) m ON m.address=us.address AND m.max_ts=us.ts
                """
            ).fetchall()

            # Consolidate to one snapshot per wallet by AGGREGATING multiple address rows.
            per_wallet: Dict[str, Dict[str, Any]] = {}
            for key_addr, tsu, h1, h5, h1h, lastshare, workers_cnt, shares, bestshare in raw_rows:
                wallet, _ = _split_addr_worker(key_addr)
                cur = per_wallet.get(wallet)
                if cur is None:
                    per_wallet[wallet] = {
                        "ts": tsu,
                        "hashrate1m_sum": _parse_hashrate_to_hs(h1 or 0),
                        "hashrate5m_sum": _parse_hashrate_to_hs(h5 or 0),
                        "hashrate1hr_sum": _parse_hashrate_to_hs(h1h or 0),
                        "lastshare": lastshare or tsu,
                        "workers_reported": int(workers_cnt or 0),
                        "shares": int(shares or 0),
                        "bestshare": bestshare or 0,
                    }
                else:
                    if tsu and tsu > (cur.get("ts") or 0):
                        cur["ts"] = tsu
                    cur["hashrate1m_sum"] += _parse_hashrate_to_hs(h1 or 0)
                    cur["hashrate5m_sum"] += _parse_hashrate_to_hs(h5 or 0)
                    cur["hashrate1hr_sum"] += _parse_hashrate_to_hs(h1h or 0)
                    try:
                        if lastshare and int(lastshare or 0) > int(cur.get("lastshare") or 0):
                            cur["lastshare"] = lastshare
                    except Exception:
                        pass
                    cur["workers_reported"] = max(cur.get("workers_reported", 0), int(workers_cnt or 0))
                    try:
                        cur["shares"] = int(cur.get("shares", 0)) + int(shares or 0)
                    except Exception:
                        pass
                    cur["bestshare"] = max(cur.get("bestshare", 0) or 0, bestshare or 0)

            for wallet, agg in per_wallet.items():
                try:
                    agg["hashrate1m"] = _human_hashrate(agg.get("hashrate1m_sum", 0.0)) if agg.get("hashrate1m_sum", 0.0) > 0 else "—"
                except Exception:
                    agg["hashrate1m"] = "—"
                try:
                    agg["hashrate5m"] = _human_hashrate(agg.get("hashrate5m_sum", 0.0)) if agg.get("hashrate5m_sum", 0.0) > 0 else "—"
                except Exception:
                    agg["hashrate5m"] = "—"
                try:
                    agg["hashrate1hr"] = _human_hashrate(agg.get("hashrate1hr_sum", 0.0)) if agg.get("hashrate1hr_sum", 0.0) > 0 else "—"
                except Exception:
                    agg["hashrate1hr"] = "—"
                agg["workers_cnt"] = agg.get("workers_reported", 0)
                agg["shares"] = agg.get("shares", 0)
                agg["lastshare"] = agg.get("lastshare", agg.get("ts"))

            active_by_wallet: Dict[str, List[Tuple[str, int]]] = {}
            try:
                curw = conn.execute(
                    "SELECT wallet, worker, last_seen FROM workers_seen "
                    "WHERE last_seen >= ? "
                    "ORDER BY wallet, last_seen DESC;",
                    (now - WORKER_TIMEOUT,),
                )
                for w, wn, ts in curw.fetchall():
                    active_by_wallet.setdefault(w, []).append((wn, ts))
            except Exception:
                active_by_wallet = {}

        finally:
            try:
                conn.close()
            except Exception:
                pass

        users: List[Dict[str, Any]] = []
        wallets = set(per_wallet.keys()) | set(active_by_wallet.keys())
        for wallet in wallets:
            snap = per_wallet.get(wallet, {})
            workers_seen = active_by_wallet.get(wallet, [])
            seen_names = []
            seen_set = set()
            for name, _ts in workers_seen:
                if not name:
                    continue
                if name not in seen_set:
                    seen_set.add(name)
                    seen_names.append(name)

            workers_count_fallback = int(snap.get("workers_cnt") or 0) if snap else 0

            row = {
                "wallet": wallet,
                "address": wallet,
                "user": wallet,
                "worker": "",
                "active_workers": seen_names,
                "shares": snap.get("shares") or 0,
                "lastshare": snap.get("lastshare") or snap.get("ts"),
                "hashrate1m": snap.get("hashrate1m") or "—",
                "hashrate5m": snap.get("hashrate5m") or "—",
                "hashrate1hr": snap.get("hashrate1hr") or "—",
                "bestshare": snap.get("bestshare"),
                "workers": (len(seen_names) if seen_names else (workers_count_fallback or (1 if snap else 0))),
            }
            users.append(row)

        total_h1_hs = sum(_parse_hashrate_to_hs(u.get("hashrate1m")) for u in users)
        workers_total = sum(len(u.get("active_workers") or []) for u in users)
        if not workers_total:
            try:
                workers_total = sum(int(u.get("workers") or 0) for u in users)
            except Exception:
                workers_total = 0

        totals = {
            "workers_count": workers_total,
            "users_count": len(users),
            "total_hashrate1m": _human_hashrate(total_h1_hs) if total_h1_hs > 0 else (pool.get("hashrate1m") or None),
            "runtime_s": pool.get("runtime") or runtime_s,
        }

        if totals["total_hashrate1m"]:
            pool["hashrate1m"] = totals["total_hashrate1m"]

        def _key(u: Dict[str, Any]):
            def _hs(v): return _parse_hashrate_to_hs(v or 0.0)
            return (-_hs(u.get("hashrate5m")), u.get("wallet") or "")

        users.sort(key=_key)

        return {"pool": pool, "users": users, "totals": totals}


# ---------------- Exports ----------------

__all__ = [
    "CKPoolState", "_connect", "init_db", "ingest_log", "ingest_blocks_ledger",
    "get_pool_snapshot", "get_pool_runtime_seconds", "get_wallet_rewards"
]

# ---------------- CLI ----------------

def _cmd_ingest(args: argparse.Namespace) -> None:
    db = _connect(Path(args.db)); init_db(db)
    n = ingest_log(db, Path(args.log), from_bytes=int(args.from_bytes or 0))
    print(f"Ingested {n} records from {args.log}")
    key = f"log_ingest_cursor:{str(Path(args.log))}"
    print(f"Cursor now at byte: {get_meta(db, key)}")

def _cmd_ingest_ledger(args: argparse.Namespace) -> None:
    db = _connect(Path(args.db)); init_db(db)
    n = ingest_blocks_ledger(db, Path(args.path))
    print(f"Ingested {n} block entries from {args.path}")

def make_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="CKPool log/ledger -> SQLite parser & state")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("ingest", help="Ingest CKPool log file")
    s.add_argument("--db", required=True); s.add_argument("--log", required=True)
    s.add_argument("--from-bytes", dest="from_bytes")
    s.set_defaults(func=_cmd_ingest)

    s = sub.add_parser("ingest-ledger", help="Ingest blocks/rewards ledger (CSV/JSON/JSONL)")
    s.add_argument("--db", required=True); s.add_argument("--path", required=True)
    s.set_defaults(func=_cmd_ingest_ledger)

    return p

def main(argv: Optional[List[str]] = None) -> None:
    args = make_argparser().parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
