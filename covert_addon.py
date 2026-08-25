"""Covert early-hold shadow strategy - Render add-on for tennis-live-scores.

Consumes the app's existing live feed, runs the validated CovertDetector,
resolves triggers to Kalshi markets (public API), and keeps a shadow ledger.
SHADOW ONLY: this module contains no order-placement code and no credentials.
Ledger persists to ./covert_data (ephemeral across deploys; the local daemon
on the research machine is the ledger of record; /covert mirrors rows).
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import math
import threading
import time
from pathlib import Path

import requests

from covert_detector import CovertDetector, norm_name

KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
SERIES = ["KXATPCHALLENGERMATCH", "KXWTACHALLENGERMATCH", "KXATPMATCH", "KXWTAMATCH"]
DATA = Path("./covert_data")
LEDGER = DATA / "ledger.csv"
FIELDS = ["ts_utc", "event_id", "league", "p1", "p2", "fav_name", "dog_name",
          "p_fav", "fav_labored", "games", "kalshi_event", "kalshi_dog_ticker",
          "prematch_dog_ask", "trigger_dog_bid", "trigger_dog_ask",
          "would_enter", "result", "settled_pnl_per_100", "settled_ts"]

_BOARD_LOCK = threading.Lock()
_BOARD: dict[str, dict] = {}
_LEDGER_LOCK = threading.Lock()
_SNAP = json.loads(Path(__file__).with_name("elo_snapshot.json").read_text())
STATUS = {"triggers": 0, "board_events": 0, "board_updated": None}


def _elo_lookup(p1: str, p2: str):
    a, b = _SNAP.get(norm_name(p1)), _SNAP.get(norm_name(p2))
    if not a or not b:
        return None
    return 1.0 / (1.0 + 10.0 ** ((b[0] - a[0]) / 400.0)), a[1], b[1]


DETECTOR = CovertDetector(_elo_lookup)


def _kalshi_get(path: str) -> dict:
    try:
        r = requests.get(f"{KALSHI}{path}", timeout=20)
        return r.json() if r.ok else {}
    except requests.RequestException:
        return {}


def _pair_key(a: str, b: str) -> str:
    return "|".join(sorted([norm_name(a), norm_name(b)]))


def _board_loop() -> None:
    while True:
        fresh: dict[str, dict] = {}
        for series in SERIES:
            payload = _kalshi_get(f"/markets?series_ticker={series}&status=open&limit=1000")
            by_event: dict[str, list] = {}
            for mk in payload.get("markets", []):
                by_event.setdefault(mk["event_ticker"], []).append(mk)
            for etk, mks in by_event.items():
                if len(mks) != 2:
                    continue
                pk = _pair_key(mks[0].get("yes_sub_title", ""), mks[1].get("yes_sub_title", ""))
                entry = fresh.setdefault(pk, {"event_ticker": etk, "tickers": {}, "ask": {}})
                for mk in mks:
                    nm = norm_name(mk.get("yes_sub_title", ""))
                    entry["tickers"][nm] = mk["ticker"]
                    entry["ask"][nm] = float(mk.get("yes_ask_dollars") or 1)
            time.sleep(0.3)
        with _BOARD_LOCK:
            for pk, entry in fresh.items():
                prev = _BOARD.get(pk)
                entry["first_ask"] = (prev.get("first_ask", prev.get("ask", {}))
                                      if prev else dict(entry["ask"]))
                _BOARD[pk] = entry
            STATUS["board_events"] = len(_BOARD)
            STATUS["board_updated"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        time.sleep(600)


def _append(row: dict) -> None:
    DATA.mkdir(exist_ok=True)
    with _LEDGER_LOCK:
        new = not LEDGER.exists()
        with LEDGER.open("a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            if new:
                w.writeheader()
            w.writerow({k: row.get(k, "") for k in FIELDS})


def _on_trigger(trig: dict) -> None:
    pk = _pair_key(trig["p1"], trig["p2"])
    with _BOARD_LOCK:
        entry = _BOARD.get(pk)
    row = {"ts_utc": trig["ts"], "event_id": trig["event_id"], "league": trig["league"],
           "p1": trig["p1"], "p2": trig["p2"], "fav_name": trig["fav_name"],
           "dog_name": trig["dog_name"], "p_fav": trig["p_fav"],
           "fav_labored": trig["fav_labored"], "games": json.dumps(trig["games"])}
    if entry:
        nm = norm_name(trig["dog_name"])
        ticker = entry["tickers"].get(nm)
        row["kalshi_event"] = entry["event_ticker"]
        if ticker:
            live = _kalshi_get(f"/markets/{ticker}").get("market") or {}
            bid = float(live.get("yes_bid_dollars") or 0)
            ask = float(live.get("yes_ask_dollars") or 1)
            pre = entry.get("first_ask", {}).get(nm, "")
            row.update({"kalshi_dog_ticker": ticker, "prematch_dog_ask": pre,
                        "trigger_dog_bid": bid, "trigger_dog_ask": ask,
                        "would_enter": int(bool(pre) and 0.05 <= ask <= 0.60
                                           and ask <= float(pre) + 0.03)})
    _append(row)
    STATUS["triggers"] += 1


def _settle_loop() -> None:
    while True:
        time.sleep(3600)
        if not LEDGER.exists():
            continue
        with _LEDGER_LOCK:
            rows = list(csv.DictReader(LEDGER.open()))
        changed = False
        for r in rows:
            if r["result"] or not r.get("kalshi_dog_ticker"):
                continue
            res = (_kalshi_get(f"/markets/{r['kalshi_dog_ticker']}").get("market") or {}).get("result") or ""
            if res in ("yes", "no"):
                r["result"] = res
                try:
                    ask = float(r["trigger_dog_ask"])
                    fee = math.ceil(7.0 * ask * (1 - ask)) / 100.0
                    pnl = (1 - ask - fee) if res == "yes" else (-ask - fee)
                    r["settled_pnl_per_100"] = f"{pnl / ask * 100:.2f}"
                except (ValueError, ZeroDivisionError):
                    pass
                r["settled_ts"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
                changed = True
        if changed:
            with _LEDGER_LOCK, LEDGER.open("w", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=FIELDS)
                w.writeheader()
                w.writerows(rows)


def start_threads() -> None:
    threading.Thread(target=_board_loop, daemon=True).start()
    threading.Thread(target=_settle_loop, daemon=True).start()


def feed(results: list, ts: str) -> None:
    """Called from the app's live-events handler. Never raises."""
    for r in results:
        if not isinstance(r, dict) or r.get("status") != "InPlay":
            continue
        try:
            trig = DETECTOR.update(ts, str(r.get("id")), r.get("name", ""),
                                   r.get("participant1", ""), r.get("participant2", ""),
                                   r.get("league", ""), r.get("score", ""),
                                   r.get("points", ""), r.get("indicator", ""))
            if trig:
                threading.Thread(target=_on_trigger, args=(trig,), daemon=True).start()
        except Exception:  # noqa: BLE001
            continue


def ledger_rows() -> list[dict]:
    if not LEDGER.exists():
        return []
    with _LEDGER_LOCK:
        return list(csv.DictReader(LEDGER.open()))
