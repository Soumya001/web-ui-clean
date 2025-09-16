#!/usr/bin/env python3
"""
run_parsers.py
Simple loop that loads parser modules defined in config.json and writes per-coin cache JSON into data/.
Parsers must expose parse(metadata: dict, base_path: Path) -> dict
"""
import importlib
import json
import time
from pathlib import Path
HERE = Path(__file__).resolve().parent
CFG = json.loads((HERE / "config.json").read_text())
POLL_INTERVAL = 10

def load_parser(module_path):
    mod = importlib.import_module(module_path)
    return mod

def main():
    parsers = {}
    for coin, md in CFG["coins"].items():
        try:
            parsers[coin] = load_parser(md["parser"])
            print(f"Loaded parser for {coin}: {md['parser']}")
        except Exception as e:
            print("Error loading parser for", coin, e)
    while True:
        for coin, md in CFG["coins"].items():
            p = parsers.get(coin)
            if not p:
                continue
            try:
                result = p.parse(md, HERE)
                out = HERE / f"data/{coin}.json"
                out.write_text(json.dumps(result))
                print(f"[{coin}] parsed OK")
            except Exception as e:
                print(f"[{coin}] parser error:", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
