#!/usr/bin/env python3
"""
CoinDCX NVDA 3:30 IST ±1% monitor
---------------------------------
Monitoring-only port of the TradingView logic:
- Auto-discovers the active CoinDCX NVDA futures instrument.
- Captures each day's 03:30 Asia/Kolkata 5-minute candle OPEN.
- Keeps that fixed open active until the next 03:30 IST candle.
- Computes +1% / -1% levels from that fixed open.
- Evaluates only fully closed 5-minute candles.
- Emits neutral UP/DOWN state-change alerts (no order placement).
- Does not repeat the same state until the opposite state occurs.
- Persists state locally so a Render restart doesn't forget the current cycle.

Environment variables:
  MOVE_PCT=1.0
  POLL_SECONDS=10
  ALERT_WEBHOOK_URL=https://your-backend.example.com/...
  COINDCX_PAIR=B-NVDA_USDT        # optional; auto-detected if omitted
  STATE_FILE=/tmp/coindcx_nvda_state.json

Install:
  pip install requests

Run:
  python coindcx_nvda_monitor.py
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")

ACTIVE_URL = (
    "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
    "active_instruments"
)
CANDLES_URL = "https://public.coindcx.com/market_data/candlesticks"

MOVE_PCT = float(os.getenv("MOVE_PCT", "1.0")) / 100.0
POLL_SECONDS = max(5, int(os.getenv("POLL_SECONDS", "10")))
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
PAIR_OVERRIDE = os.getenv("COINDCX_PAIR", "").strip()
STATE_FILE = Path(os.getenv("STATE_FILE", "/tmp/coindcx_nvda_state.json"))

HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "coindcx-nvda-monitor/1.0"})

DEFAULT_STATE = {
    "pair": None,
    "session_date_ist": None,
    "session_open": None,
    "state": 0,                 # 0 neutral, 1 UP +1%, -1 DOWN -1%
    "last_processed_open_ms": None,
}


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).isoformat()} {msg}", flush=True)


def load_state() -> dict:
    data = DEFAULT_STATE.copy()
    try:
        if STATE_FILE.exists():
            saved = json.loads(STATE_FILE.read_text())
            if isinstance(saved, dict):
                data.update(saved)
    except Exception as exc:
        log(f"[STATE] load failed: {exc}")
    return data


def save_state(state: dict) -> None:
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
        tmp.replace(STATE_FILE)
    except Exception as exc:
        log(f"[STATE] save failed: {exc}")


def discover_nvda_pair() -> str:
    if PAIR_OVERRIDE:
        log(f"[PAIR] using override {PAIR_OVERRIDE}")
        return PAIR_OVERRIDE

    # 1) Legacy/crypto futures active-instruments endpoint.
    active_matches = []
    try:
        r = HTTP.get(
            ACTIVE_URL,
            params=[("margin_currency_short_name[]", "USDT")],
            timeout=15,
        )
        r.raise_for_status()
        instruments = r.json()
        if isinstance(instruments, list):
            active_matches = [
                str(x) for x in instruments
                if "NVDA" in str(x).upper()
            ]
            log(f"[PAIR DISCOVERY] active_instruments NVDA matches={active_matches}")
        else:
            log(f"[PAIR DISCOVERY] active_instruments unexpected type={type(instruments)}")
    except Exception as exc:
        log(f"[PAIR DISCOVERY] active_instruments failed: {exc}")

    if active_matches:
        preferred = next(
            (x for x in active_matches if x.upper() == "B-NVDA_USDT"),
            active_matches[0],
        )
        log(f"[PAIR] selected from active_instruments={preferred}")
        return preferred

    # 2) Fallback: CoinDCX public real-time futures price map.
    # Global Futures may not appear in the legacy active_instruments list.
    current_prices_url = (
        "https://public.coindcx.com/market_data/v3/current_prices/futures/rt"
    )
    try:
        r = HTTP.get(current_prices_url, timeout=15)
        r.raise_for_status()
        payload = r.json()
        prices = payload.get("prices", {}) if isinstance(payload, dict) else {}

        price_matches = [
            str(pair) for pair in prices.keys()
            if "NVDA" in str(pair).upper()
        ]
        log(f"[PAIR DISCOVERY] current_prices NVDA matches={price_matches}")

        if price_matches:
            preferred = next(
                (x for x in price_matches if x.upper() == "B-NVDA_USDT"),
                price_matches[0],
            )
            log(f"[PAIR] selected from current_prices={preferred}")
            return preferred

        # Useful diagnostic if CoinDCX names Global Futures differently.
        global_like = []
        for pair, info in prices.items():
            haystack = f"{pair} {info}".upper()
            if any(token in haystack for token in ("NVIDIA", "NVDA")):
                global_like.append(str(pair))
        if global_like:
            log(f"[PAIR DISCOVERY] NVIDIA-like candidates={global_like}")

    except Exception as exc:
        log(f"[PAIR DISCOVERY] current_prices failed: {exc}")

    raise RuntimeError(
        "NVDA was not exposed by CoinDCX's documented futures active-instruments "
        "or public current-prices endpoints. The app's Global Futures product may "
        "use a separate API/symbol namespace. Set COINDCX_PAIR only after the exact "
        "Global Futures API symbol is confirmed."
    )


def get_candles(pair: str, start_utc: datetime, end_utc: datetime) -> list:
    params = {
        "pair": pair,
        "from": int(start_utc.timestamp()),
        "to": int(end_utc.timestamp()),
        "resolution": "5",
        "pcode": "f",
    }
    r = HTTP.get(CANDLES_URL, params=params, timeout=15)
    r.raise_for_status()
    payload = r.json()

    if isinstance(payload, dict):
        if payload.get("s") not in (None, "ok"):
            raise RuntimeError(f"CoinDCX candle API status: {payload}")
        rows = payload.get("data", [])
    elif isinstance(payload, list):
        rows = payload
    else:
        raise RuntimeError(f"Unexpected candles response: {type(payload)}")

    clean = []
    for row in rows:
        try:
            open_ms = int(float(row["time"]))
            clean.append(
                {
                    "time": open_ms,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("volume", 0) or 0),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue

    clean.sort(key=lambda x: x["time"])
    return clean


def current_session_anchor(now_ist: datetime) -> datetime:
    """
    The active session anchor is today's 03:30 IST if already past it;
    otherwise yesterday's 03:30 IST.
    """
    today_330 = now_ist.replace(hour=3, minute=30, second=0, microsecond=0)
    if now_ist >= today_330:
        return today_330
    return today_330 - timedelta(days=1)


def find_330_open(candles: list, anchor_ist: datetime):
    target_ms = int(anchor_ist.astimezone(timezone.utc).timestamp() * 1000)

    # Exact match first.
    for c in candles:
        if c["time"] == target_ms:
            return c["open"]

    # Defensive fallback: accept within 30 seconds only.
    nearest = None
    nearest_diff = None
    for c in candles:
        diff = abs(c["time"] - target_ms)
        if nearest_diff is None or diff < nearest_diff:
            nearest = c
            nearest_diff = diff

    if nearest is not None and nearest_diff is not None and nearest_diff <= 30_000:
        log(
            f"[3:30 OPEN] exact timestamp unavailable; "
            f"using nearest candle diff_ms={nearest_diff}"
        )
        return nearest["open"]

    return None


def send_webhook(title: str, message: str) -> None:
    log(f"[ALERT] {title} | {message}")

    if not ALERT_WEBHOOK_URL:
        log("[ALERT] ALERT_WEBHOOK_URL not set; console-only mode")
        return

    try:
        r = HTTP.post(
            ALERT_WEBHOOK_URL,
            json={"title": title, "message": message},
            timeout=15,
        )
        log(f"[WEBHOOK] status={r.status_code}")
        r.raise_for_status()
    except Exception as exc:
        log(f"[WEBHOOK ERROR] {exc}")


def ensure_session_open(pair: str, state: dict, now_ist: datetime) -> bool:
    anchor_ist = current_session_anchor(now_ist)
    anchor_date = anchor_ist.date().isoformat()

    if (
        state.get("session_date_ist") == anchor_date
        and state.get("session_open") is not None
    ):
        return True

    # Fetch a narrow window around the 03:30 candle.
    start = (anchor_ist - timedelta(minutes=10)).astimezone(timezone.utc)
    end = (anchor_ist + timedelta(minutes=15)).astimezone(timezone.utc)
    candles = get_candles(pair, start, end)
    open_px = find_330_open(candles, anchor_ist)

    if open_px is None:
        log(
            f"[3:30 OPEN] not available yet for {anchor_date} "
            f"| pair={pair}"
        )
        return False

    state["session_date_ist"] = anchor_date
    state["session_open"] = open_px
    state["state"] = 0
    state["last_processed_open_ms"] = None
    save_state(state)

    upper = open_px * (1 + MOVE_PCT)
    lower = open_px * (1 - MOVE_PCT)
    log(
        f"[NEW SESSION] date_ist={anchor_date} | pair={pair} | "
        f"03:30_open={open_px:.4f} | upper={upper:.4f} | lower={lower:.4f}"
    )
    return True


def fetch_recent_closed_candles(pair: str, now_utc: datetime) -> list:
    start = now_utc - timedelta(minutes=30)
    rows = get_candles(pair, start, now_utc + timedelta(seconds=2))

    closed = []
    now_ms = int(now_utc.timestamp() * 1000)

    for c in rows:
        # A 5m candle is considered confirmed only after its 5m close time.
        close_ms = c["time"] + 5 * 60 * 1000
        if close_ms <= now_ms:
            closed.append(c)

    return closed


def process_candle(pair: str, state: dict, candle: dict) -> None:
    open_ms = candle["time"]
    if state.get("last_processed_open_ms") is not None:
        if open_ms <= int(state["last_processed_open_ms"]):
            return

    session_open = float(state["session_open"])
    upper = session_open * (1 + MOVE_PCT)
    lower = session_open * (1 - MOVE_PCT)
    close_px = float(candle["close"])
    old_state = int(state.get("state", 0))

    new_state = old_state
    label = None

    if old_state != 1 and close_px >= upper:
        new_state = 1
        label = "UP +1%"
    elif old_state != -1 and close_px <= lower:
        new_state = -1
        label = "DOWN -1%"

    candle_ist = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc).astimezone(IST)
    close_ist = candle_ist + timedelta(minutes=5)

    log(
        f"[5M CLOSED] {close_ist:%Y-%m-%d %H:%M:%S IST} | "
        f"pair={pair} | close={close_px:.4f} | "
        f"03:30_open={session_open:.4f} | "
        f"upper={upper:.4f} | lower={lower:.4f} | state={old_state}"
    )

    state["last_processed_open_ms"] = open_ms

    if label is not None:
        state["state"] = new_state

        pct_from_open = ((close_px / session_open) - 1.0) * 100.0
        opposite = lower if new_state == 1 else upper
        opposite_name = "-1% level" if new_state == 1 else "+1% level"

        title = f"COINDCX NVDA {label}"
        message = (
            f"COINDCX NVDA | {label}"
            f" | 3:30 OPEN {session_open:.2f}"
            f" | CLOSE {close_px:.2f}"
            f" | MOVE {pct_from_open:+.2f}%"
            f" | {opposite_name.upper()} {opposite:.2f}"
            f" | 5M CLOSE {close_ist:%H:%M IST}"
        )
        send_webhook(title, message)

    save_state(state)


def main() -> None:
    log("=== COINDCX NVDA 03:30 IST ±1% MONITOR START ===")

    pair = discover_nvda_pair()
    state = load_state()

    # If persisted state belongs to another pair, reset it.
    if state.get("pair") != pair:
        state = DEFAULT_STATE.copy()
        state["pair"] = pair
        save_state(state)

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc.astimezone(IST)

            if not ensure_session_open(pair, state, now_ist):
                time.sleep(POLL_SECONDS)
                continue

            candles = fetch_recent_closed_candles(pair, now_utc)
            for candle in candles:
                process_candle(pair, state, candle)

            time.sleep(POLL_SECONDS)

        except KeyboardInterrupt:
            log("[STOP] keyboard interrupt")
            break
        except Exception as exc:
            log(f"[ERROR] {type(exc).__name__}: {exc}")
            time.sleep(max(POLL_SECONDS, 15))


if __name__ == "__main__":
    main()
