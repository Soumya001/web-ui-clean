# parsers/ckpool_btc.py
"""
Thin wrapper so the parser runner can call `parsers.ckpool_btc.parse(metadata, base_path)`
and get a dict the web UI uses.

This file deliberately *does not* import ckpool_parser at module import time to
avoid circular imports. The ckpool_parser module is imported inside parse().
"""

from pathlib import Path
from typing import Any, Dict, Optional
import json
import os

def _resolve_db_path(db: str, base_path: Optional[Path]) -> str:
    """Resolve relative db path against base_path if provided."""
    if not db:
        return db
    try:
        if base_path and not Path(db).is_absolute():
            return str((Path(base_path) / db).resolve())
        return str(Path(db).resolve()) if not Path(db).is_absolute() else db
    except Exception:
        # fallback to given value
        return db

def _detect_log_path(logdir: str) -> Optional[str]:
    """If a directory is given, look for ckpool.log inside it. Else return the path if it exists."""
    if not logdir:
        return None
    p = Path(logdir)
    try:
        if p.is_dir():
            cand = p / "ckpool.log"
            if cand.exists():
                return str(cand)
            # maybe there's a file named like ckpool.out etc. prefer explicit ckpool.log only
            return None
        if p.exists():
            return str(p)
    except Exception:
        return None
    return None

def parse(metadata: Dict[str, Any], base_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    metadata: dict from config.json for this coin (keys: 'logdir', 'db' etc.)
    base_path: project base path (optional)
    Returns: { "coins": { "btc": <snapshot> } }
    """
    # Import ckpool_parser lazily to avoid circular-import at module import time.
    try:
        from ckpool_parser import CKPoolState  # type: ignore
    except Exception as e:
        # If the core parser isn't importable, return a safe empty shape so runner won't crash
        # (the run_parsers loop will log the import error separately).
        return {"coins": {"btc": {"pool": {}, "users": [], "totals": {}}}}

    logdir = metadata.get("logdir") or metadata.get("logfile") or ""
    db = metadata.get("db") or ""

    # Resolve db path against base_path if needed
    db_resolved = _resolve_db_path(db, base_path) if db else db

    # detect ckpool.log inside logdir (if a directory was provided)
    log_path = _detect_log_path(logdir) or (logdir if logdir else None)

    # Build CKPoolState with resolved paths
    try:
        state = CKPoolState(db_path=db_resolved or "ckpool.sqlite",
                            log_path=str(log_path) if log_path else None)
    except Exception:
        return {"coins": {"btc": {"pool": {}, "users": [], "totals": {}}}}

    try:
        # refresh/snapshot may raise; guard it
        state.refresh()
        snap = state.snapshot() or {"pool": {}, "users": [], "totals": {}}
        # ensure it's a dict with expected keys
        if not isinstance(snap, dict):
            snap = {"pool": {}, "users": [], "totals": {}}
    except Exception:
        # return safe empty structure on error so runner doesn't crash repeatedly
        return {"coins": {"btc": {"pool": {}, "users": [], "totals": {}}}}

    return {"coins": {"btc": snap}}
