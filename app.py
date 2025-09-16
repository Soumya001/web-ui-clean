#!/usr/bin/env python3
# app.py — XP CKPool WebUI (single-row wallet + active_workers + /api/user)
# Robust import of local ckpool_parser.py and safe fallback to keep web UI up.

import os
import re
import sys
import threading
import time
import importlib.util
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional
from flask import Response, render_template, redirect, Flask, abort, jsonify, request, send_from_directory

# --- .env (optional) ---
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# --- Site config ---
SITE_NAME        = os.getenv("SITE_NAME", "XP Pool")
SITE_TAGLINE     = os.getenv("SITE_TAGLINE", "Mining Dashboard")
PUBLIC_HOST      = os.getenv("PUBLIC_HOST", "0.0.0.0")
PUBLIC_PORT      = int(os.getenv("PUBLIC_PORT", "8088"))

BASE_DIR         = os.path.dirname(os.path.abspath(__file__))

CKPOOL_DB        = os.getenv("CKPOOL_DB", os.path.join(BASE_DIR, "ckpool.sqlite"))
if not os.path.isabs(CKPOOL_DB):
    CKPOOL_DB = os.path.join(BASE_DIR, CKPOOL_DB)

CKPOOL_LOG       = os.getenv("CKPOOL_LOG", "/mnt/blockchain/ckpool_logs/ckpool.log")
CKPOOL_STATUS_URL= os.getenv("CKPOOL_STATUS_URL", "").strip()  # optional HTTP status JSON

# Optional Bitcoin RPC (for /api/node and the Node page)
BTC_RPC_URL      = os.getenv("BITCOIN_RPC_URL", "")
BTC_COOKIE_PATH  = os.getenv("BITCOIN_COOKIE_PATH", "")
BTC_RPC_HOST     = os.getenv("BITCOIN_RPC_HOST", "127.0.0.1")
BTC_RPC_PORT     = int(os.getenv("BITCOIN_RPC_PORT", "8332"))

# Explorers (for rewards page)
EXPLORER_TX      = os.getenv("EXPLORER_TX", "https://mempool.space/tx/{txid}")
EXPLORER_BLOCK   = os.getenv("EXPLORER_BLOCK", "https://mempool.space/block/{blockhash}")

REFRESH_SEC      = int(os.getenv("REFRESH_SEC", "5"))

# --- Optional RPC client ---
try:
    from bitcoinrpc.authproxy import AuthServiceProxy  # type: ignore
except Exception:
    AuthServiceProxy = None  # type: ignore

def get_rpc() -> Optional[Any]:
    """Return an RPC client if configured and available, else None."""
    if not AuthServiceProxy:
        return None
    if BTC_RPC_URL:
        try:
            return AuthServiceProxy(BTC_RPC_URL)
        except Exception:
            pass
    if BTC_COOKIE_PATH and os.path.exists(BTC_COOKIE_PATH):
        try:
            with open(BTC_COOKIE_PATH, "r", encoding="utf-8") as f:
                creds = f.read().strip()
            return AuthServiceProxy(f"http://{creds}@{BTC_RPC_HOST}:{BTC_RPC_PORT}")
        except Exception:
            return None
    return None

# --- Pool fee helper for About page ---
FEE_KEYS = ["donationpercent","donation_percent","fee_percent","pool_fee","fee","operator_fee","donation"]

def _read_fee_from_conf(path: str):
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or line.startswith(";"):
                    continue
                parts = re.split(r"\s*=\s*|\s+", line, maxsplit=1)
                if len(parts) != 2:
                    continue
                key, val = parts[0].strip().lower(), parts[1].strip()
                if key in FEE_KEYS:
                    m = re.search(r"[-+]?\d*\.?\d+", val)
                    if m:
                        return float(m.group(0))
    except Exception:
        return None
    return None

def get_pool_fee_pct() -> str:
    conf = os.getenv("CKPOOL_CONF", "")
    fee = _read_fee_from_conf(conf)
    return "" if fee is None else str(fee)

# --- Ensure local package importability for ckpool_parser.py ---
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

_ckpool_parser = None
_ckpool_import_error = None

try:
    import ckpool_parser as _ckpool_parser  # type: ignore
except Exception:
    # fallback: load by filepath and capture traceback for logging
    parser_path = os.path.join(BASE_DIR, "ckpool_parser.py")
    if os.path.exists(parser_path):
        try:
            spec = importlib.util.spec_from_file_location("ckpool_parser", parser_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                _ckpool_parser = module
                sys.modules["ckpool_parser"] = module
        except Exception as e:
            _ckpool_parser = None
            _ckpool_import_error = traceback.format_exc()
    else:
        _ckpool_parser = None
        _ckpool_import_error = f"ckpool_parser.py not found at {parser_path}"

# If import failed, log the reason and create a dummy fallback so the web UI stays up.
if _ckpool_parser is None:
    # print the detailed traceback to stderr (captured by systemd / journalctl)
    print("WARNING: failed to import local module 'ckpool_parser'. Falling back to empty state.", file=sys.stderr)
    if _ckpool_import_error:
        print(_ckpool_import_error, file=sys.stderr)
    # Minimal fallback: provide dummy functions and a DummyState so app still runs
    class DummyState:
        def __init__(self, *args, **kwargs):
            self._snapshot = {"pool": {}, "users": [], "totals": {}}
        def refresh(self):
            return
        def snapshot(self):
            return self._snapshot
        def connections_snapshot(self):
            return []
    class DummyDBHelpers:
        @staticmethod
        def _connect(p): raise RuntimeError("DB unavailable in fallback")
        @staticmethod
        def init_db(c): pass
        @staticmethod
        def get_wallet_rewards(conn, addr): return []
    CKPoolState = DummyState  # type: ignore
    def get_wallet_rewards(conn, addr): return []
    def _connect(*a, **k): raise RuntimeError("DB unavailable in fallback")
    def init_db(*a, **k): pass
else:
    # import needed names from actual module
    try:
        from ckpool_parser import CKPoolState, get_wallet_rewards, _connect, init_db  # type: ignore
    except Exception:
        # if this fails unexpectedly, fallback to dummy behaviour
        print("WARNING: ckpool_parser import partial failure; using fallback. See logs for details.", file=sys.stderr)
        CKPoolState = None  # type: ignore
        def get_wallet_rewards(conn, addr): return []
        def _connect(*a, **k): raise RuntimeError("DB unavailable")
        def init_db(*a, **k): pass
        # ensure we have a state object below referencing DummyState
        class DummyState:
            def __init__(self, *args, **kwargs):
                self._snapshot = {"pool": {}, "users": [], "totals": {}}
            def refresh(self): return
            def snapshot(self): return self._snapshot
            def connections_snapshot(self): return []
        CKPoolState = DummyState  # type: ignore

# --- Parser / DB state ---
from pathlib import Path
import sqlite3
import sys
import types

DB_PATH = Path(CKPOOL_DB)

def get_db() -> sqlite3.Connection:
    """
    Open a fresh connection and ensure DB schema exists. Use helpers from the
    loaded _ckpool_parser module when available; otherwise fall back to a
    resilient sqlite connection.
    """
    # If the module was loaded successfully, use its _connect/init_db helpers
    if _ckpool_parser:
        try:
            _connect = getattr(_ckpool_parser, "_connect")
            init_db = getattr(_ckpool_parser, "init_db")
            conn = _connect(DB_PATH)
            try:
                init_db(conn)
            except Exception:
                pass
            return conn
        except Exception:
            # fall back to plain sqlite below
            pass

    # Fallback sqlite connection
    conn = sqlite3.connect(str(DB_PATH), timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA busy_timeout=10000;")
        conn.commit()
    except Exception:
        pass
    return conn

# Wire up parser exports from the previously-loaded _ckpool_parser module
try:
    if not _ckpool_parser:
        raise ImportError("ckpool_parser module not available")

    # Extract objects from the already-loaded module object
    CKPoolState = getattr(_ckpool_parser, "CKPoolState")
    get_wallet_rewards = getattr(_ckpool_parser, "get_wallet_rewards")
    # Do NOT re-import ckpool_parser with 'from ckpool_parser import ...' (would re-trigger import).
    # Initialize live state object (may raise).
    state = CKPoolState(db_path=CKPOOL_DB, log_path=CKPOOL_LOG, status_url=CKPOOL_STATUS_URL)

except Exception as e:
    # Log the reason (appears in journalctl)
    print("WARNING: failed to initialize CKPoolState:", repr(e), file=sys.stderr)

    # Provide safe stubs so the app still starts (pages load but are empty)
    class _EmptyState:
        def __init__(self):
            self._snapshot = {"pool": {}, "users": [], "totals": {}}
        def snapshot(self):
            return self._snapshot
        def refresh(self):
            return
        def connections_snapshot(self):
            return []

    # safe stub for rewards function
    def get_wallet_rewards(conn, addr):
        return []

    state = _EmptyState()  # type: ignore

# End Parser / DB state


# --- Flask ---
app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["TEMPLATES_AUTO_RELOAD"] = True

@app.context_processor
def inject_site():
    return {"SITE_NAME": SITE_NAME, "SITE_TAGLINE": SITE_TAGLINE, "now": datetime.utcnow}

def _bg_refresh():
    while True:
        try:
            state.refresh()
        except Exception:
            # swallow; don't let background thread crash
            pass
        time.sleep(max(1, REFRESH_SEC))

threading.Thread(target=_bg_refresh, daemon=True).start()

from datetime import datetime

@app.template_filter('datetimeformat')
def datetimeformat(value):
    try:
        return datetime.utcfromtimestamp(int(value)).strftime('%Y-%m-%d %H:%M:%S UTC')
    except Exception:
        return ''

# ========== In-memory hashrate history (24h rolling) ==========
from collections import defaultdict, deque

HIST_WINDOW_SEC   = int(os.getenv("HIST_WINDOW_SEC", str(24 * 3600)))
SAMPLE_EVERY_SEC  = int(os.getenv("SAMPLE_EVERY_SEC", "30"))

HISTORY = defaultdict(lambda: deque(maxlen=HIST_WINDOW_SEC // max(1, SAMPLE_EVERY_SEC)))

def _bg_sampler():
    while True:
        try:
            snap = state.snapshot()
            ts = int(time.time())
            for u in (snap.get("users") or []):
                addr = u.get("wallet") or u.get("address")
                if not addr:
                    continue
                try:
                    v = float(u.get("hashrate1m") or 0)
                except Exception:
                    v = 0.0
                HISTORY[addr].append((ts, v))
        except Exception:
            pass
        time.sleep(max(1, SAMPLE_EVERY_SEC))

threading.Thread(target=_bg_sampler, daemon=True).start()
# ==================================================================

# ---------- helpers ----------

def _find_wallet_row(wallet: str) -> Optional[Dict[str, Any]]:
    snap = state.snapshot()
    for u in (snap.get("users") or []):
        if (u.get("wallet") or u.get("address")) == wallet:
            return u
    return None

def wallet_last_seen_map(conn: sqlite3.Connection) -> Dict[str, Optional[int]]:
    out: Dict[str, Optional[int]] = {}
    try:
        cur = conn.execute(
            "SELECT wallet, MAX(last_seen) as last_seen FROM workers_seen GROUP BY wallet;"
        )
    except Exception:
        return {}
    rows = cur.fetchall()
    for r in rows:
        try:
            w = r[0]
            ts = r[1]
            if w is None:
                continue
            out[w] = int(ts) if ts is not None else None
        except Exception:
            continue
    return out

# ---------- Pages ----------

@app.route("/")
def index():
    # just render a landing / coin selector page
    return render_template("home.html")

@app.route("/coin/<coin>")
def coin_page(coin: str):
    coin_l = coin.lower()
    if coin_l != "btc":
        # redirect all non-BTC coins to unavailable.html
        return render_template("unavailable.html", coin=coin)

    # normal BTC dashboard
    snap = state.snapshot() or {"pool": {}, "users": [], "totals": {}}
    pool_obj = types.SimpleNamespace(**snap)
    users = (snap.get("users") or [])[:100]

    node = {}
    rpc = get_rpc()
    if rpc:
        try:
            node = {
                "block": rpc.getblockcount(),
                "difficulty": rpc.getdifficulty(),
                "connections": rpc.getconnectioncount(),
            }
        except Exception:
            node = {}

    return render_template("index.html", pool=pool_obj, node=node, users=users, coin=coin)



@app.route("/node")
def node_page():
    node = {}
    rpc = get_rpc()
    if rpc:
        try:
            node = {
                "block": rpc.getblockcount(),
                "difficulty": rpc.getdifficulty(),
                "connections": rpc.getconnectioncount(),
                "bestblockhash": rpc.getbestblockhash(),
            }
        except Exception:
            node = {}
    return render_template("node.html", node=node)

@app.route("/miners")
def miners_page():
    snap = state.snapshot()
    users = snap.get("users") or []
    page = max(int(request.args.get("page", 1)), 1)
    size = max(min(int(request.args.get("size", 50)), 200), 10)
    start, end = (page - 1) * size, (page - 1) * size + size
    return render_template("miners.html", users=users[start:end], page=page, size=size)

@app.route("/blocks")
def blocks_page():
    # coin may be passed as query param ?coin=btc ; keep same behavior as other pages
    coin = (request.args.get("coin") or "btc")
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT height, blockhash, ts, reward_btc, txid, address "
            "FROM blocks ORDER BY ts DESC LIMIT 100;"
        ).fetchall()
        try:
            conn.close()
        except Exception:
            pass
    except Exception:
        # fallback to empty list on DB error
        rows = []

    # Convert sqlite rows to simple dicts (safer for template)
    blocks = []
    for r in rows:
        try:
            blocks.append({
                "height": r[0],
                "blockhash": r[1],
                "ts": int(r[2]) if r[2] is not None else None,
                "reward_btc": float(r[3]) if r[3] is not None else None,
                "txid": r[4],
                "address": r[5]
            })
        except Exception:
            # ignore malformed row
            continue

    return render_template(
        "blocks.html",
        blocks=blocks,
        explorer_tx=EXPLORER_TX,
        explorer_block=EXPLORER_BLOCK,
        coin=coin
    )


@app.route("/connections")
def connections_page():
    try:
        conns = state.connections_snapshot()
    except Exception:
        conns = []
    return render_template("connections.html", connections=conns)

@app.route("/search")
def search_page():
    q = (request.args.get("q") or "").strip()
    matches: List[Dict[str, Any]] = []
    if q:
        ql = q.lower()
        snap = state.snapshot()
        for u in (snap.get("users") or []):
            fields = [(u.get("address") or ""), (u.get("wallet") or ""), (u.get("user") or ""), (u.get("worker") or "")]
            if any(ql in str(f).lower() for f in fields if f):
                matches.append(u)
    return render_template("search.html", q=q, matches=matches)

@app.route("/about")
def about_page():
    info = {"fee": get_pool_fee_pct(), "ckpool_conf": os.getenv("CKPOOL_CONF", "")}
    return render_template("about.html", info=info)

# ---- Wallet detail ----
@app.route("/wallet/<addr>")
def wallet_page(addr: str):
    row = _find_wallet_row(addr)

    agg = None
    if row:
        def _to_int(val, default=0):
            try:
                if val is None:
                    return default
                return int(float(str(val).replace(",", "")))
            except Exception:
                return default

        agg = {
            "address": addr,
            "workers": _to_int(len(row.get("active_workers") or [])) or _to_int(row.get("workers"), 0),
            "hashrate1m": row.get("hashrate1m"),
            "hashrate5m": row.get("hashrate5m"),
            "hashrate1hr": row.get("hashrate1hr"),
            "shares": _to_int(row.get("shares"), 0),
            "lastshare": _to_int(row.get("lastshare"), 0),
        }

    return render_template("wallet.html", addr=addr, agg=agg, row=row)

@app.route("/rewards")
def rewards_page():
    # read from blocks table (ckpool_parser stores block rewards there)
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT height, blockhash, ts, reward_btc, txid, address FROM blocks ORDER BY ts DESC LIMIT 50;"
        ).fetchall()
        try: conn.close()
        except Exception: pass
    except Exception:
        rows = []
    return render_template(
        "rewards.html",
        address=None,
        rewards=rows,
        explorer_tx=EXPLORER_TX,
        explorer_block=EXPLORER_BLOCK
    )

@app.route("/xpgame", strict_slashes=False)
@app.route("/xpgame/", strict_slashes=False)
def xpgame_redirect():
    return redirect("https://xpfun.lol", code=301)

# ---- Rewards (HTML + API) ----
@app.get("/wallet/<address>/rewards")
def wallet_rewards_page(address: str):
    try:
        conn = get_db()
        rows = get_wallet_rewards(conn, address)
        try: conn.close()
        except: pass
    except Exception:
        rows = []
    return render_template("rewards.html", address=address, rewards=rows,
                           explorer_tx=EXPLORER_TX, explorer_block=EXPLORER_BLOCK)

@app.get("/api/wallet/<address>/rewards")
def wallet_rewards_api(address: str):
    try:
        conn = get_db()
        rows = get_wallet_rewards(conn, address)
        try: conn.close()
        except: pass
    except Exception:
        rows = []
    out = []
    for r in rows:
        out.append({
            "height": r.get("height"),
            "hash": r.get("hash"),
            "ts": r.get("ts"),
            "reward_btc": r.get("reward_btc"),
            "txid": r.get("txid"),
            "tx_url": EXPLORER_TX.format(txid=r.get("txid")) if r.get("txid") else None,
            "block_url": EXPLORER_BLOCK.format(blockhash=r.get("hash")) if r.get("hash") else None,
        })
    return jsonify({"address": address, "rewards": out})

# ---------- APIs ----------

@app.get("/api/pool")
def api_pool():
    snap = state.snapshot()
    users = (snap.get("users") or [])

    try:
        timeout_s = int(os.getenv("WORKER_TIMEOUT", "300"))
    except Exception:
        timeout_s = 300
    now = int(time.time())
    cutoff = now - max(0, timeout_s)

    try:
        conn = get_db()
        try:
            last_map = wallet_last_seen_map(conn)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception:
        last_map = {}

    filtered = []
    for u in users:
        wallet = u.get("wallet") or u.get("address") or (u.get("user") and str(u.get("user")).split('.', 1)[0])
        last_ts = None
        if wallet:
            last_ts = last_map.get(wallet)

        # attach last_seen metadata
        u["last_seen_ts"] = int(last_ts) if last_ts else None
        try:
            u["last_seen"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(u["last_seen_ts"])) if u["last_seen_ts"] else None
        except Exception:
            u["last_seen"] = None

        keep = False
        if u.get("last_seen_ts") is not None and u["last_seen_ts"] >= cutoff:
            keep = True
        else:
            try:
                if float(u.get("hashrate1m") or 0) > 0:
                    keep = True
            except Exception:
                pass
            if not keep and isinstance(u.get("active_workers"), list) and len(u.get("active_workers")) > 0:
                keep = True

        if keep:
            filtered.append(u)

    out_snap = dict(snap)
    out_snap["users"] = filtered

    # annotate with node height where possible
    try:
        rpc = get_rpc()
        if rpc:
            try:
                blk = rpc.getblockcount()
                out_snap.setdefault("pool", {})["block"] = int(blk)
                out_snap["pool"]["height"] = int(blk)
            except Exception:
                try:
                    bh = rpc.getbestblockhash()
                    out_snap.setdefault("pool", {})["bestblockhash"] = str(bh)
                except Exception:
                    pass
    except Exception:
        pass

    return jsonify(out_snap)

@app.get("/api/node")
def api_node():
    rpc = get_rpc()
    if not rpc:
        return jsonify({})
    data = {}
    try:
        data["block"] = int(rpc.getblockcount())
    except Exception:
        pass
    try:
        data["difficulty"] = rpc.getdifficulty()
    except Exception:
        pass
    try:
        data["connections"] = int(rpc.getconnectioncount())
    except Exception:
        pass
    try:
        data["bestblockhash"] = rpc.getbestblockhash()
    except Exception:
        pass
    return jsonify(data)

@app.get("/api/search")
def api_search():
    q = (request.args.get("q") or "").strip()
    res: List[Dict[str, Any]] = []
    if q:
        ql = q.lower()
        snap = state.snapshot()
        for u in (snap.get("users") or []):
            fields = [(u.get("address") or ""), (u.get("wallet") or ""), (u.get("user") or ""), (u.get("worker") or "")]
            if any(ql in str(f).lower() for f in fields if f):
                res.append(u)
    return jsonify({"query": q, "matches": res})

@app.get("/api/user/<wallet>")
def api_user(wallet: str):
    row = _find_wallet_row(wallet)
    if not row:
        abort(404)
    return jsonify(row)

@app.get("/api/wallet/<wallet>/workers")
def api_wallet_workers(wallet: str):
    try:
        limit = max(1, min(int(request.args.get("limit", 50)), 200))
    except Exception:
        limit = 50
    try:
        offset = max(0, int(request.args.get("offset", 0)))
    except Exception:
        offset = 0

    try:
        timeout_s = int(os.getenv("WORKER_TIMEOUT", "300"))
    except Exception:
        timeout_s = 300
    now = int(time.time())

    try:
        conn = get_db()
        total = conn.execute(
            "SELECT COUNT(*) FROM workers_seen WHERE wallet=? AND active=1 AND last_seen>=?",
            (wallet, now - timeout_s),
        ).fetchone()[0]

        rows = conn.execute(
            "SELECT worker, last_seen FROM workers_seen "
            "WHERE wallet=? AND active=1 AND last_seen>=? "
            "ORDER BY last_seen DESC LIMIT ? OFFSET ?;",
            (wallet, now - timeout_s, limit, offset),
        ).fetchall()
        try: conn.close()
        except: pass
    except Exception:
        total = 0
        rows = []

    workers = [{"name": r[0], "last_seen": r[1]} for r in rows]
    return jsonify({"wallet": wallet, "total": int(total), "active": int(total), "workers": workers})

@app.get("/api/history/<addr>")
def api_history(addr: str):
    try:
        window = int(request.args.get("window", str(6 * 3600)))
    except Exception:
        window = 6 * 3600
    now = int(time.time())
    cutoff = now - max(60, window)

    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT ts, hashrate FROM wallet_history WHERE wallet=? AND ts>=? ORDER BY ts ASC;",
            (addr, cutoff)
        ).fetchall()
        try: conn.close()
        except: pass
        pts = [(int(r[0]), float(r[1] or 0.0)) for r in rows]
        return jsonify({"wallet": addr, "points": pts})
    except Exception:
        pts = [(ts, v) for (ts, v) in HISTORY.get(addr, []) if ts >= cutoff]
        return jsonify({"wallet": addr, "points": pts})

@app.after_request
def add_header(r):
    r.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r

@app.route("/favicon.ico")
def favicon():
    path = os.path.join(app.root_path, "static")
    file_path = os.path.join(path, "favicon.ico")
    if os.path.exists(file_path):
        return send_from_directory(path, "favicon.ico")
    return ("", 204)

@app.route("/robots.txt")
def robots_txt():
    return Response("User-agent: *\nAllow: /\nSitemap: https://xppool.in/sitemap.xml\n", mimetype="text/plain")

@app.route("/sitemap.xml")
def sitemap_xml():
    BASE_URL = "https://xppool.in"
    pages: List[tuple] = [
        ("/", "1.0"),
        ("/miners", "0.8"),
        ("/connections", "0.6"),
        ("/node", "0.6"),
        ("/about", "0.5"),
        ("/search", "0.5"),
    ]
    lastmod = datetime.utcnow().date().isoformat()
    items = []
    for path, priority in pages:
        items.append(f"""
  <url>
    <loc>{BASE_URL}{path}</loc>
    <lastmod>{lastmod}</lastmod>
    <priority>{priority}</priority>
  </url>""")
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{''.join(items)}
</urlset>"""
    return Response(xml, mimetype="application/xml")

if __name__ == "__main__":
    app.run(host=PUBLIC_HOST, port=PUBLIC_PORT, debug=False)
