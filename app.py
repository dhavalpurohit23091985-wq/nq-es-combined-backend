import os
import time
import threading
import json
import csv
import io
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import deque

from flask import Flask, request, jsonify, redirect
import requests


app = Flask(__name__)


# ==================================================
# ENVIRONMENT
# ==================================================

PUSHOVER_TOKEN = os.environ.get("PUSHOVER_TOKEN")
PUSHOVER_USER = os.environ.get("PUSHOVER_USER")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")
COINALYZE_API_KEY = os.environ.get("COINALYZE_API_KEY")
CRON_SECRET = os.environ.get("CRON_SECRET", "").strip()
DIRECT_LIQ_SECRET = os.environ.get("DIRECT_LIQ_SECRET", "").strip()
MT5_BRIDGE_SECRET = os.environ.get("MT5_BRIDGE_SECRET", "").strip()
PUSHOVER_URL = "https://api.pushover.net/1/messages.json"


# ==================================================
# COINALYZE RATE-LIMIT / RETRY SETTINGS
# ==================================================

COINALYZE_MAX_RETRIES = 3
COINALYZE_FALLBACK_RETRY_DELAYS = (3, 6, 12)
COINALYZE_MIN_REQUEST_GAP_SECONDS = 3.0

_coinalyze_request_lock = threading.Lock()
_coinalyze_last_request_monotonic = 0.0


# ==================================================
# MARGINPAD SETTINGS
# ==================================================
# Free/keyless liquidation feed.
# Kept COMPLETELY SEPARATE from Coinalyze totals/alerts.
#
# Official endpoints used:
#   GET /api/v1/price?symbol=BTC
#   GET /api/v1/liquidations/live?symbol=BTC&limit=400
#
# MarginPad documents side as:
#   long_liquidated
#   short_liquidated

MARGINPAD_BASE_URL = "https://marginpad.io"
MARGINPAD_MAX_RETRIES = 3
MARGINPAD_RETRY_DELAYS = (2, 4, 8)
MARGINPAD_LIVE_LIMIT = 400

# Small overlap protects against events that arrive a little late.
# Fingerprint de-duplication prevents the overlap from double-counting.
MARGINPAD_OVERLAP_MS = 5 * 60 * 1000
MARGINPAD_SEEN_MAX = 10000

_marginpad_request_lock = threading.Lock()


# ==================================================
# NQ / ES LIVE DATA
# ==================================================

latest_delta = {
    "NQ": None,
    "ES": None
}

latest_price = {
    "NQ": None,
    "ES": None,
    "JPN": None
}


# ==================================================
# NQ + ES STATE
# ==================================================

state = 0
THRESHOLD = 1000

entry_side = None
entry_nq_price = None
entry_jpn_price = None


# ==================================================
# BTC FRESH LIQUIDATION SETTINGS - COINALYZE
# ==================================================

BTC_LIQ_THRESHOLD = 5_000_000
BTC_LOW_MOVE_POINTS = 500

btc_long_cumulative = 0.0
btc_short_cumulative = 0.0

btc_cycle_ref_price = None
btc_last_processed_liq_ts = None

# Keep the most recent completed BTC alert snapshot so the other
# provider can show it as a same-message comparison even after reset.
btc_last_alert_snapshot = None

btc_symbol_cache = None


# ==================================================
# BTC FRESH LIQUIDATION SETTINGS - MARGINPAD
# ==================================================
# Same threshold logic as the existing BTC Coinalyze feed,
# but state and alerts are independent.

MARGINPAD_BTC_LIQ_THRESHOLD = 5_000_000
MARGINPAD_BTC_LOW_MOVE_POINTS = 500

marginpad_btc_long_cumulative = 0.0
marginpad_btc_short_cumulative = 0.0

marginpad_btc_cycle_ref_price = None

# Most recent completed MarginPad BTC alert snapshot for cross-reference.
marginpad_btc_last_alert_snapshot = None

# We process events only through the last fully closed minute.
marginpad_btc_processed_through_ms = None

# In-memory event de-duplication.
marginpad_seen_queue = deque()
marginpad_seen_set = set()

# Per-exchange audit state for the standalone MarginPad BTC cycle.
marginpad_btc_by_exchange = {}


# ==================================================
# BTC DIRECT LIQUIDATOR - STANDALONE
# ==================================================
# The 4 direct exchanges are intentionally kept separate from MarginPad BTC.
# They have their own cumulative cycle, threshold, alert and reset.

DIRECT_BTC_LIQ_THRESHOLD = 5_000_000.0
direct_btc_long_cumulative = 0.0
direct_btc_short_cumulative = 0.0
direct_btc_cycle_ref_price = None
direct_btc_last_alert_snapshot = None
direct_btc_by_exchange = {
    ex: {"long": 0.0, "short": 0.0}
    for ex in ("bitget", "aster", "coinex", "lighter")
}


# ==================================================
# BTC 13-EXCHANGE OBSERVER - MARGINPAD 9 + DIRECT 4
# ==================================================
# Alert-only observer. It does NOT change/reset/read Coinalyze BTC state and
# does NOT publish MT5 signals. It receives the same already-accepted fresh
# contributions from MarginPad and the direct BTC liquidator, then maintains
# its own independent $5M cycle across 13 unique exchanges.

BTC_OBSERVER_THRESHOLD = 5_000_000.0
BTC_OBSERVER_EXCHANGES = (
    "binance", "bybit", "okx", "hyperliquid", "gate", "htx",
    "dydx", "bitmex", "bitfinex",
    "bitget", "aster", "coinex", "lighter",
)
btc_observer_long_cumulative = 0.0
btc_observer_short_cumulative = 0.0
btc_observer_cycle_ref_price = None
btc_observer_last_alert_snapshot = None
btc_observer_by_exchange = {
    ex: {"long": 0.0, "short": 0.0}
    for ex in BTC_OBSERVER_EXCHANGES
}

# BTC GAP + ROLLING 60M STATE
BTC_GAP_THRESHOLD = 5_000_000.0
BTC_ROLLING_WINDOW_SECONDS = 3600
BTC_ROLLING_GAP_THRESHOLD = 5_000_000.0
btc_coinalyze_gap_state = None
btc_observer_gap_state = None
btc_coinalyze_rolling_events = deque()
btc_observer_rolling_events = deque()
btc_coinalyze_rolling_state = None
btc_observer_rolling_state = None
_btc_rolling_lock = threading.RLock()


# ==================================================
# XAU FRESH LIQUIDATION SETTINGS - MARGINPAD
# ==================================================
# Completely separate from both BTC MarginPad and XAU Coinalyze.

MARGINPAD_XAU_LIQ_THRESHOLD = 100_000

# XAU final GAP + rolling 60M state. Same architecture as BTC,
# with a $100K GAP threshold instead of $5M.
XAU_GAP_THRESHOLD = 100_000.0
XAU_ROLLING_WINDOW_SECONDS = 3600
XAU_ROLLING_GAP_THRESHOLD = 100_000.0
xau_coinalyze_gap_state = None
xau_observer_gap_state = None
xau_coinalyze_rolling_events = deque()
xau_observer_rolling_events = deque()
xau_coinalyze_rolling_state = None
xau_observer_rolling_state = None
_xau_rolling_lock = threading.RLock()


# ==================================================
# ALL CRYPTO LIQUIDATION - MARGINPAD MARKET-WIDE FEED
# ==================================================
# Independent test setup. Existing BTC/XAU logic is untouched.
# Source: MarginPad GET /api/v1/feed (all tracked liquidation symbols).
# Only symbols classified as crypto are accepted. XAU/metals/indices are excluded.
#
# Setup A: actual LONG-SHORT GAP +/-$5M, RESET after valid reverse-only alert.
# Setup B: exact trailing 60m GAP +/-$5M, NO RESET, reverse-only.

ALL_CRYPTO_GAP_THRESHOLD = 5_000_000.0
ALL_CRYPTO_ROLLING_WINDOW_SECONDS = 3600
ALL_CRYPTO_ROLLING_GAP_THRESHOLD = 5_000_000.0
ALL_CRYPTO_POLL_SECONDS = 5.0
ALL_CRYPTO_SEEN_MAX = 50_000

all_crypto_long_cumulative = 0.0
all_crypto_short_cumulative = 0.0
all_crypto_gap_state = None
# Start timestamp of the current cumulative RESET cycle.
# Starts on the first accepted liquidation after reset and persists across restarts.
all_crypto_cycle_start_ts = None
all_crypto_rolling_events = deque()
all_crypto_rolling_state = None
all_crypto_seen_queue = deque()
all_crypto_seen_set = set()
all_crypto_by_symbol = {}
all_crypto_last_poll_ts = None
all_crypto_last_error = None
all_crypto_crypto_symbols = set()
all_crypto_crypto_symbols_refreshed_ts = 0.0
all_crypto_first_poll_seeded = False
_all_crypto_lock = threading.RLock()
_all_crypto_poller_started = False

marginpad_xau_long_cumulative = 0.0
marginpad_xau_short_cumulative = 0.0

marginpad_xau_cycle_ref_price = None
marginpad_xau_processed_through_ms = None

# Separate de-duplication cache for XAU MarginPad events.
marginpad_xau_seen_queue = deque()
marginpad_xau_seen_set = set()


# ==================================================
# COMBINED LIQUIDATION STATE - MARGINPAD + DIRECT
# ==================================================
# MarginPad remains the base source. The direct worker contributes only
# supplemental exchanges: Bitget, Aster, CoinEx and Lighter.
# Coinalyze is intentionally NOT part of this combined execution signal.

COMBINED_LIQ_THRESHOLDS = {
    "BTC": 5_000_000.0,
    "XAU": 100_000.0,
}

COMBINED_DIRECT_EXCHANGES = (
    "bitget",
    "aster",
    "coinex",
    "lighter",
)

COMBINED_SOURCE_KEYS = (
    "marginpad",
    *COMBINED_DIRECT_EXCHANGES,
)

COMBINED_DIRECT_SEEN_MAX = 40_000

combined_liq = {
    "BTC": {"long": 0.0, "short": 0.0},
    "XAU": {"long": 0.0, "short": 0.0},
}

combined_by_source = {
    asset: {
        source: {"long": 0.0, "short": 0.0}
        for source in COMBINED_SOURCE_KEYS
    }
    for asset in ("BTC", "XAU")
}

# Exchange-level audit breakdown for the current combined cycle.
# BTC MarginPad events are recorded by their real exchange name, while
# direct-worker events use their direct exchange name. This is display/audit
# state only; combined threshold calculations remain unchanged.
combined_by_exchange = {
    "BTC": {},
    "XAU": {},
}

combined_cycle_ref_price = {"BTC": None, "XAU": None}
combined_latest_price = {"BTC": None, "XAU": None}
combined_last_alert = {"BTC": None, "XAU": None}

combined_direct_seen_queue = deque()
combined_direct_seen_set = set()
_combined_liq_lock = threading.RLock()


# ==================================================
# MT5 DEMO SIGNAL BRIDGE
# ==================================================
# This backend only publishes signals. The MT5 EA must enforce DEMO account
# mode before any order action. Synthetic test signals are stored separately
# and never touch liquidation totals, thresholds, Pushover or reset logic.

mt5_latest_signals = {
    "BTC": None,
    "XAU": None,
}

mt5_test_signals = {
    "BTC": None,
    "XAU": None,
}

_mt5_signal_lock = threading.RLock()


def _mt5_make_signal(asset, winner, side, source, mode, *, long_usd=None, short_usd=None):
    now_ns = time.time_ns()
    return {
        "id": f"{asset}-{now_ns}-{side}",
        "asset": asset,
        "winner": winner,
        "side": side,
        "source": source,
        "mode": mode,
        "ts": int(time.time()),
        "long_usd": long_usd,
        "short_usd": short_usd,
    }


def _publish_mt5_live_signal(alert_snapshot):
    asset = str(alert_snapshot.get("asset", "")).upper().strip()
    winner = str(alert_snapshot.get("winner", "")).upper().strip()

    if asset not in ("BTC", "XAU"):
        return None

    # Confirmed mapping:
    # liquidation LONG WINS  -> trade SELL
    # liquidation SHORT WINS -> trade BUY
    # BOTH -> alert only, no execution signal.
    if winner == "LONG":
        side = "SELL"
    elif winner == "SHORT":
        side = "BUY"
    else:
        print(
            f"[MT5 BRIDGE] {asset} winner={winner} -> NO EXECUTION SIGNAL",
            flush=True,
        )
        return None

    signal = _mt5_make_signal(
        asset=asset,
        winner=winner,
        side=side,
        source="combined_liquidation",
        mode="LIVE_COMBINED",
        long_usd=round(float(alert_snapshot.get("long", 0.0) or 0.0), 2),
        short_usd=round(float(alert_snapshot.get("short", 0.0) or 0.0), 2),
    )
    signal["threshold_usd"] = COMBINED_LIQ_THRESHOLDS[asset]

    with _mt5_signal_lock:
        mt5_latest_signals[asset] = signal

    print(
        f"[MT5 BRIDGE] LIVE {asset} {winner} -> {side} | id={signal['id']}",
        flush=True,
    )
    return signal


# ==================================================
# XAU FRESH LIQUIDATION SETTINGS
# ==================================================

XAU_LIQ_THRESHOLD = 100_000

xau_long_cumulative = 0.0
xau_short_cumulative = 0.0

xau_cycle_ref_price = None
xau_last_processed_liq_ts = None

xau_symbol_cache = None
xau_price_symbol_cache = None


# ==================================================
# FUTURE MARKETS CACHE
# ==================================================

future_markets_cache = None


# ==================================================
# PUSHOVER
# ==================================================

def send_pushover(title, message):

    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        return False

    payload = {
        "token": PUSHOVER_TOKEN,
        "user": PUSHOVER_USER,
        "title": title,
        "message": message,
        "priority": 2,
        "retry": 30,
        "expire": 3600
    }

    try:
        response = requests.post(
            PUSHOVER_URL,
            data=payload,
            timeout=10
        )
        return response.ok

    except requests.RequestException:
        return False


def _nasdaq_extract_line(message, prefix):
    for raw_line in str(message or "").splitlines():
        line = raw_line.strip()
        if line.upper().startswith(prefix.upper()):
            return line[len(prefix):].strip()
    return None


def _nasdaq_classify_alert(title):
    upper = str(title or "").upper().strip()

    # Keep HOURLY and DAILY as two independent setups.
    if "NASDAQ DAILY TOP5" in upper:
        setup = "DAILY"
        source = "TOP5"
    elif "NASDAQ DAILY BOTTOM5" in upper:
        setup = "DAILY"
        source = "BOTTOM5"
    elif "NASDAQ TOP5" in upper:
        setup = "HOURLY"
        source = "TOP5"
    elif "NASDAQ BOTTOM5" in upper:
        setup = "HOURLY"
        source = "BOTTOM5"
    else:
        return None

    if " BUY " in f" {upper} ":
        direction = "BUY"
    elif " SELL " in f" {upper} ":
        direction = "SELL"
    else:
        return None

    return setup, source, direction


def _nasdaq_combined_process(title, message):
    """Consume one final TOP5/BOTTOM5 alert with separate HOURLY/DAILY pending states.

    Pairing logic is unchanged inside each setup:
      BUY -> BUY   = COMBINED BUY, then reset that setup's pending state
      SELL -> SELL = COMBINED SELL, then reset that setup's pending state
      Opposite     = no combined alert; latest becomes pending for that setup

    HOURLY signals can only pair with HOURLY.
    DAILY signals can only pair with DAILY.
    Pushover-only; no MT5 publication and no basket percentage calculations.
    """
    global nasdaq_combined_pending, nasdaq_combined_recent

    classified = _nasdaq_classify_alert(title)
    if classified is None:
        return {
            "recognized": False,
            "duplicate": False,
            "combined_sent": False,
            "result": None,
        }

    setup, source, direction = classified
    fingerprint = f"{setup}|{str(title)}|{str(message)}"

    completed_hour = _nasdaq_extract_line(message, "COMPLETED HOUR:")
    nq_text = _nasdaq_extract_line(message, "NQ:")
    received_ist = datetime.now(NASDAQ_COMBINED_IST).strftime("%d-%m-%Y %H:%M:%S IST")
    event_time = completed_hour or received_ist

    with _nasdaq_combined_lock:
        if fingerprint in nasdaq_combined_recent:
            print(
                f"[NASDAQ COMBINED DUPLICATE] {setup} {source} {direction} | {event_time}",
                flush=True,
            )
            return {
                "recognized": True,
                "setup": setup,
                "duplicate": True,
                "combined_sent": False,
                "result": None,
                "pending": nasdaq_combined_pending.get(setup),
            }

        nasdaq_combined_recent.append(fingerprint)

        current = {
            "setup": setup,
            "source": source,
            "direction": direction,
            "time": event_time,
            "received_ist": received_ist,
            "nq": nq_text,
            "title": str(title),
        }

        pending = nasdaq_combined_pending.get(setup)

        if pending is None:
            nasdaq_combined_pending[setup] = current
            print(
                f"[NASDAQ COMBINED PENDING] {setup} {source} {direction} | {event_time}",
                flush=True,
            )
            return {
                "recognized": True,
                "setup": setup,
                "duplicate": False,
                "combined_sent": False,
                "result": None,
                "pending": dict(current),
            }

        first = dict(pending)

        # Same direction = confirmation. Reset ONLY this setup.
        if first["direction"] == direction:
            combined_direction = direction
            nasdaq_combined_pending[setup] = None

            nq_display = nq_text or first.get("nq") or "NA"

            combined_title = f"NASDAQ {setup} COMBINED {combined_direction} | CONFIRMED"
            combined_message = (
                f"SETUP: {setup}\n\n"
                "ALERT 1:\n"
                f"{first['source']} {first['direction']} | {first['time']}\n\n"
                "ALERT 2:\n"
                f"{source} {direction} | {event_time}\n\n"
                "SEQUENCE:\n"
                f"{first['direction']} -> {direction}\n\n"
                "RESULT:\n"
                f"COMBINED {combined_direction}\n\n"
                f"NQ: {nq_display}\n\n"
                "SEQUENCE RESET:\n"
                f"{setup} WAITING FOR NEW ALERT"
            )

            combined_sent = send_pushover(combined_title, combined_message)

            print(
                f"[NASDAQ COMBINED CONFIRMED] {setup} "
                f"{first['source']} {first['direction']} -> "
                f"{source} {direction} | "
                f"result={combined_direction} | sent={combined_sent}",
                flush=True,
            )

            return {
                "recognized": True,
                "setup": setup,
                "duplicate": False,
                "combined_sent": bool(combined_sent),
                "result": combined_direction,
                "pending": None,
            }

        # Opposite direction = no trade. Replace pending ONLY for this setup.
        nasdaq_combined_pending[setup] = current

        print(
            f"[NASDAQ COMBINED MISMATCH] {setup} "
            f"{first['source']} {first['direction']} -> "
            f"{source} {direction} | "
            f"new_pending={source} {direction}",
            flush=True,
        )

        return {
            "recognized": True,
            "setup": setup,
            "duplicate": False,
            "combined_sent": False,
            "result": None,
            "pending": dict(current),
        }


def _combined_remember_direct_event(event_key):
    if not event_key:
        return False

    if event_key in combined_direct_seen_set:
        return False

    if len(combined_direct_seen_queue) >= COMBINED_DIRECT_SEEN_MAX:
        old = combined_direct_seen_queue.popleft()
        combined_direct_seen_set.discard(old)

    combined_direct_seen_queue.append(event_key)
    combined_direct_seen_set.add(event_key)
    return True


def _combined_reset_asset(asset, reset_price=None):
    global marginpad_btc_long_cumulative, marginpad_btc_short_cumulative
    global marginpad_btc_cycle_ref_price
    global marginpad_xau_long_cumulative, marginpad_xau_short_cumulative
    global marginpad_xau_cycle_ref_price

    combined_liq[asset]["long"] = 0.0
    combined_liq[asset]["short"] = 0.0

    for source in COMBINED_SOURCE_KEYS:
        combined_by_source[asset][source]["long"] = 0.0
        combined_by_source[asset][source]["short"] = 0.0

    combined_by_exchange[asset].clear()

    combined_cycle_ref_price[asset] = reset_price

    # Keep the old MarginPad read-only/debug fields aligned with the
    # current combined cycle instead of letting them grow independently.
    if asset == "BTC":
        marginpad_btc_long_cumulative = 0.0
        marginpad_btc_short_cumulative = 0.0
        marginpad_btc_cycle_ref_price = reset_price
    else:
        marginpad_xau_long_cumulative = 0.0
        marginpad_xau_short_cumulative = 0.0
        marginpad_xau_cycle_ref_price = reset_price


def add_combined_liquidation_batch(
    asset,
    source,
    exchange,
    long_usd,
    short_usd,
    event_key=None,
    price=None,
    exchange_breakdown=None,
):
    """Add one atomic batch to the shared MarginPad + Direct cycle.

    MarginPad calls this once per successfully processed closed minute with
    both sides together. The direct worker calls the HTTP endpoint once per
    liquidation event, so only one side is normally non-zero there.
    """

    global marginpad_btc_long_cumulative, marginpad_btc_short_cumulative
    global marginpad_btc_last_alert_snapshot
    global marginpad_xau_long_cumulative, marginpad_xau_short_cumulative
    global xau_observer_gap_state

    asset = str(asset or "").upper().strip()
    source = str(source or "").lower().strip()
    exchange = str(exchange or source or "").lower().strip()

    if asset not in COMBINED_LIQ_THRESHOLDS:
        return {"ok": False, "error": "unsupported_asset"}

    if source == "marginpad":
        source_key = "marginpad"
    elif source == "direct" and exchange in COMBINED_DIRECT_EXCHANGES:
        source_key = exchange
    else:
        return {"ok": False, "error": "unsupported_source"}

    try:
        long_usd = float(long_usd or 0.0)
        short_usd = float(short_usd or 0.0)
    except (TypeError, ValueError):
        return {"ok": False, "error": "invalid_amount"}

    if long_usd < 0 or short_usd < 0:
        return {"ok": False, "error": "negative_amount"}

    alert_snapshot = None

    with _combined_liq_lock:
        if source == "direct":
            if not _combined_remember_direct_event(str(event_key or "")):
                return {
                    "ok": True,
                    "duplicate": True,
                    "asset": asset,
                    "source": source_key,
                    "combined_long_usd": round(combined_liq[asset]["long"], 2),
                    "combined_short_usd": round(combined_liq[asset]["short"], 2),
                    "alert_sent": False,
                }

        if price is not None:
            try:
                p = float(price)
                if p > 0:
                    combined_latest_price[asset] = p
                    if combined_cycle_ref_price[asset] is None:
                        combined_cycle_ref_price[asset] = p
            except (TypeError, ValueError):
                pass

        combined_liq[asset]["long"] += long_usd
        combined_liq[asset]["short"] += short_usd
        combined_by_source[asset][source_key]["long"] += long_usd
        combined_by_source[asset][source_key]["short"] += short_usd

        # Preserve the real exchange-level contribution for BTC alert auditing.
        # MarginPad supplies a per-exchange breakdown; direct events already
        # arrive with their exchange name. This does not alter combined totals.
        if asset == "BTC":
            if source == "marginpad" and isinstance(exchange_breakdown, dict):
                for ex_name, ex_totals in exchange_breakdown.items():
                    ex_key = str(ex_name or "unknown").strip().lower() or "unknown"
                    if not isinstance(ex_totals, dict):
                        continue
                    try:
                        ex_long = float(ex_totals.get("long", 0.0) or 0.0)
                        ex_short = float(ex_totals.get("short", 0.0) or 0.0)
                    except (TypeError, ValueError):
                        continue
                    bucket = combined_by_exchange[asset].setdefault(
                        ex_key, {"long": 0.0, "short": 0.0}
                    )
                    bucket["long"] += max(0.0, ex_long)
                    bucket["short"] += max(0.0, ex_short)
            elif source == "direct":
                ex_key = exchange or source_key
                bucket = combined_by_exchange[asset].setdefault(
                    ex_key, {"long": 0.0, "short": 0.0}
                )
                bucket["long"] += long_usd
                bucket["short"] += short_usd

        if source_key == "marginpad":
            if asset == "BTC":
                marginpad_btc_long_cumulative += long_usd
                marginpad_btc_short_cumulative += short_usd
            else:
                marginpad_xau_long_cumulative += long_usd
                marginpad_xau_short_cumulative += short_usd

        cycle_long = combined_liq[asset]["long"]
        cycle_short = combined_liq[asset]["short"]
        threshold = COMBINED_LIQ_THRESHOLDS[asset]
        if asset == "XAU":
            signed_gap = cycle_long - cycle_short
            long_hit = signed_gap >= XAU_GAP_THRESHOLD and xau_observer_gap_state != "LONG"
            short_hit = signed_gap <= -XAU_GAP_THRESHOLD and xau_observer_gap_state != "SHORT"
        else:
            # Legacy combined BTC cycle stays available for background/MT5 state,
            # but its Pushover is disabled below. BTC user-facing GAP alert is
            # the dedicated 13-exchange BTC Observer.
            long_hit = cycle_long >= threshold
            short_hit = cycle_short >= threshold

        if long_hit or short_hit:
            if asset == "XAU":
                if long_hit:
                    winner = "LONG"
                    xau_observer_gap_state = "LONG"
                    title = "XAU OBSERVER LONG WINS | 100K GAP"
                else:
                    winner = "SHORT"
                    xau_observer_gap_state = "SHORT"
                    title = "XAU OBSERVER SHORT WINS | 100K GAP"
            elif long_hit and short_hit:
                winner = "BOTH HIT SAME CYCLE"
                title = f"{asset} COMBINED BOTH HIT +{threshold/1_000_000:g}M"
            elif long_hit:
                winner = "LONG"
                title = f"{asset} COMBINED LONG WINS +{threshold/1_000_000:g}M"
            else:
                winner = "SHORT"
                title = f"{asset} COMBINED SHORT WINS +{threshold/1_000_000:g}M"

            cycle_total = cycle_long + cycle_short
            long_pct = (cycle_long / cycle_total * 100.0) if cycle_total > 0 else 0.0
            short_pct = (cycle_short / cycle_total * 100.0) if cycle_total > 0 else 0.0
            gap = abs(cycle_long - cycle_short)

            source_lines = []
            for src_name in COMBINED_SOURCE_KEYS:
                src_long = combined_by_source[asset][src_name]["long"]
                src_short = combined_by_source[asset][src_name]["short"]
                if src_long > 0 or src_short > 0:
                    label = "MarginPad" if src_name == "marginpad" else src_name.title()
                    source_lines.append(
                        f"{label}: L ${src_long:,.0f} | S ${src_short:,.0f}"
                    )

            exchange_lines = []
            if asset == "BTC":
                exchange_labels = {
                    "binance": "Binance",
                    "okx": "OKX",
                    "bybit": "Bybit",
                    "bitget": "Bitget",
                    "aster": "Aster",
                    "coinex": "CoinEx",
                    "lighter": "Lighter",
                    "bitfinex": "Bitfinex",
                    "hyperliquid": "Hyperliquid",
                    "gate": "Gate",
                    "htx": "HTX",
                }

                if winner == "LONG":
                    display_side = "long"
                elif winner == "SHORT":
                    display_side = "short"
                else:
                    display_side = None

                ranked = []
                for ex_name, ex_totals in combined_by_exchange[asset].items():
                    ex_long = float(ex_totals.get("long", 0.0) or 0.0)
                    ex_short = float(ex_totals.get("short", 0.0) or 0.0)
                    if ex_long <= 0 and ex_short <= 0:
                        continue
                    rank_amount = (
                        ex_long if display_side == "long"
                        else ex_short if display_side == "short"
                        else max(ex_long, ex_short)
                    )
                    ranked.append((rank_amount, ex_name, ex_long, ex_short))

                ranked.sort(key=lambda row: row[0], reverse=True)
                for _, ex_name, ex_long, ex_short in ranked:
                    label = exchange_labels.get(ex_name, ex_name.title())
                    if display_side == "long":
                        exchange_lines.append(f"{label}: ${ex_long:,.0f}")
                    elif display_side == "short":
                        exchange_lines.append(f"{label}: ${ex_short:,.0f}")
                    else:
                        exchange_lines.append(
                            f"{label}: L ${ex_long:,.0f} | S ${ex_short:,.0f}"
                        )

            current_price = combined_latest_price.get(asset)
            ref_price = combined_cycle_ref_price.get(asset)
            move = None
            if current_price is not None and ref_price is not None:
                move = abs(current_price - ref_price)

            alert_snapshot = {
                "asset": asset,
                "winner": winner,
                "title": title,
                "long": cycle_long,
                "short": cycle_short,
                "gap": gap,
                "long_pct": long_pct,
                "short_pct": short_pct,
                "price": current_price,
                "move": move,
                "sources": source_lines,
                "exchanges": exchange_lines,
                "ts": int(time.time()),
            }

            combined_last_alert[asset] = dict(alert_snapshot)

            # Publish a read-only MT5 demo bridge signal from the same canonical
            # combined threshold event. BOTH remains alert-only.
            _publish_mt5_live_signal(alert_snapshot)

            if asset == "BTC":
                marginpad_btc_last_alert_snapshot = {
                    "ts": int(time.time()),
                    "long": combined_by_source[asset]["marginpad"]["long"],
                    "short": combined_by_source[asset]["marginpad"]["short"],
                    "winner": winner,
                }

            _combined_reset_asset(asset, current_price)

        result = {
            "ok": True,
            "duplicate": False,
            "asset": asset,
            "source": source_key,
            "combined_long_usd": round(cycle_long, 2),
            "combined_short_usd": round(cycle_short, 2),
            "threshold_usd": threshold,
            "winner": alert_snapshot["winner"] if alert_snapshot else None,
            "alert_sent": False,
            "reset": bool(alert_snapshot),
        }

    if alert_snapshot:
        price_text = "NA"
        if alert_snapshot["price"] is not None:
            if asset == "BTC":
                price_text = f"{alert_snapshot['price']:,.0f}"
            else:
                price_text = f"{alert_snapshot['price']:,.2f}"

        move_text = "NA"
        if alert_snapshot["move"] is not None:
            move_text = (
                f"{alert_snapshot['move']:,.0f} pts"
                if asset == "BTC"
                else f"{alert_snapshot['move']:,.2f} pts"
            )

        if asset == "BTC" and alert_snapshot.get("exchanges"):
            breakdown = "\n".join(alert_snapshot["exchanges"])
            message = (
                f"{breakdown}\n\n"
                f"COMBINED SHORT: ${alert_snapshot['short']:,.0f}\n"
                f"COMBINED LONG: ${alert_snapshot['long']:,.0f}\n"
                f"GAP: ${alert_snapshot['gap']:,.0f} ({_usd_m(alert_snapshot['gap'])})\n"
                f"BTC {price_text} | BTC MOVE {move_text}"
            )
        else:
            breakdown = "\n".join(alert_snapshot["sources"]) or "No source breakdown"
            message = (
                f"SOURCE COMBINED | WINNER {alert_snapshot['winner']} | "
                f"LONG ${alert_snapshot['long']:,.0f} ({_usd_m(alert_snapshot['long'])}) ({alert_snapshot['long_pct']:.2f}%) | "
                f"SHORT ${alert_snapshot['short']:,.0f} ({_usd_m(alert_snapshot['short'])}) ({alert_snapshot['short_pct']:.2f}%) | "
                f"GAP ${alert_snapshot['gap']:,.0f} ({_usd_m(alert_snapshot['gap'])}) | "
                f"{asset} {price_text} | {asset} MOVE {move_text}\n"
                f"{breakdown}"
            )

        if asset == "XAU":
            sent = send_pushover(alert_snapshot["title"], message)
            result["alert_sent"] = sent
        else:
            sent = False
            result["alert_sent"] = False
            print(f"[COMBINED BTC PUSHOVER SILENT] {alert_snapshot['title']}", flush=True)

        print(
            f"[COMBINED ALERT] {alert_snapshot['title']} "
            f"L=${alert_snapshot['long']:,.0f} S=${alert_snapshot['short']:,.0f} "
            f"sent={sent}",
            flush=True,
        )

    else:
        print(
            f"[COMBINED {asset}] {source_key.upper()} "
            f"+L=${long_usd:,.0f} +S=${short_usd:,.0f} | "
            f"TOTAL L=${result['combined_long_usd']:,.0f} "
            f"S=${result['combined_short_usd']:,.0f}",
            flush=True,
        )

    return result


# ==================================================
# COINALYZE GLOBAL REQUEST SPACING
# ==================================================

def wait_for_coinalyze_slot():

    global _coinalyze_last_request_monotonic

    now_mono = time.monotonic()

    wait_seconds = (
        COINALYZE_MIN_REQUEST_GAP_SECONDS
        - (
            now_mono
            - _coinalyze_last_request_monotonic
        )
    )

    if wait_seconds > 0:
        time.sleep(wait_seconds)

    _coinalyze_last_request_monotonic = (
        time.monotonic()
    )


# ==================================================
# PARSE RETRY-AFTER
# ==================================================

def get_retry_after_seconds(response):

    try:
        raw = response.headers.get(
            "Retry-After"
        )

        if raw is None:
            return None

        value = float(raw)

        if value < 0:
            return None

        return min(value, 20.0)

    except (
        TypeError,
        ValueError
    ):
        return None


# ==================================================
# COINALYZE GET WITH GLOBAL SPACING + RETRY
# ==================================================

def coinalyze_get(
    url,
    *,
    params=None,
    timeout=15,
    stage="coinalyze"
):

    last_error = None

    with _coinalyze_request_lock:

        for attempt in range(
            COINALYZE_MAX_RETRIES + 1
        ):

            wait_for_coinalyze_slot()

            try:
                response = requests.get(
                    url,
                    params=params,
                    headers={
                        "api_key":
                            COINALYZE_API_KEY
                    },
                    timeout=timeout
                )

            except requests.RequestException as e:

                last_error = {
                    "stage": stage,
                    "error": str(e),
                    "attempt": attempt + 1
                }

                retryable = True
                retry_after = None

            else:

                if response.status_code == 200:
                    return response, None

                retry_after = (
                    get_retry_after_seconds(
                        response
                    )
                )

                last_error = {
                    "stage": stage,
                    "status_code":
                        response.status_code,
                    "response":
                        response.text[:500],
                    "attempt": attempt + 1,
                    "retry_after":
                        retry_after
                }

                retryable = (
                    response.status_code == 429
                    or
                    500 <= response.status_code <= 599
                )

                if not retryable:
                    return None, last_error

            if attempt >= COINALYZE_MAX_RETRIES:
                break

            if retry_after is not None:
                delay = retry_after

            else:
                delay = (
                    COINALYZE_FALLBACK_RETRY_DELAYS[
                        min(
                            attempt,
                            len(
                                COINALYZE_FALLBACK_RETRY_DELAYS
                            ) - 1
                        )
                    ]
                )

            print(
                f"{stage}: retrying in "
                f"{delay:.1f}s "
                f"(attempt {attempt + 2})"
            )

            time.sleep(delay)

    return None, last_error


# ==================================================
# MARGINPAD GET WITH RETRY
# ==================================================

def marginpad_get(
    path,
    *,
    params=None,
    timeout=10,
    stage="marginpad"
):

    last_error = None

    with _marginpad_request_lock:

        for attempt in range(
            MARGINPAD_MAX_RETRIES + 1
        ):

            try:
                response = requests.get(
                    f"{MARGINPAD_BASE_URL}{path}",
                    params=params,
                    timeout=timeout
                )

            except requests.RequestException as e:

                last_error = {
                    "stage": stage,
                    "error": str(e),
                    "attempt": attempt + 1
                }

                retryable = True
                retry_after = None

            else:

                if response.status_code == 200:

                    try:
                        payload = response.json()

                    except ValueError:
                        return None, {
                            "stage": stage,
                            "error": "invalid json"
                        }

                    if (
                        isinstance(payload, dict)
                        and payload.get("ok") is False
                    ):
                        return None, {
                            "stage": stage,
                            "error": payload.get(
                                "error",
                                "marginpad returned ok=false"
                            )
                        }

                    return payload, None

                retry_after = (
                    get_retry_after_seconds(
                        response
                    )
                )

                last_error = {
                    "stage": stage,
                    "status_code":
                        response.status_code,
                    "response":
                        response.text[:500],
                    "attempt": attempt + 1,
                    "retry_after":
                        retry_after
                }

                retryable = (
                    response.status_code == 429
                    or
                    500 <= response.status_code <= 599
                )

                if not retryable:
                    return None, last_error

            if attempt >= MARGINPAD_MAX_RETRIES:
                break

            if retry_after is not None:
                delay = retry_after
            else:
                delay = MARGINPAD_RETRY_DELAYS[
                    min(
                        attempt,
                        len(MARGINPAD_RETRY_DELAYS) - 1
                    )
                ]

            print(
                f"{stage}: retrying in "
                f"{delay:.1f}s "
                f"(attempt {attempt + 2})"
            )

            time.sleep(delay)

    return None, last_error


# ==================================================
# GET FUTURE MARKETS
# ==================================================

def get_future_markets():

    global future_markets_cache

    if future_markets_cache is not None:
        return future_markets_cache, None

    response, error = coinalyze_get(
        "https://api.coinalyze.net/v1/future-markets",
        timeout=10,
        stage="future-markets"
    )

    if error:
        return None, error

    try:
        markets = response.json()

    except ValueError:
        return None, {
            "stage": "future-markets",
            "error": "invalid json"
        }

    future_markets_cache = markets

    return markets, None


# ==================================================
# MARGINPAD HELPERS
# ==================================================

def normalize_marginpad_ts_ms(raw_ts):

    try:
        value = float(raw_ts)
    except (TypeError, ValueError):
        return None

    # MarginPad documents server/event timestamps in Unix milliseconds.
    # This fallback also tolerates seconds if an event ever arrives that way.
    if value < 10_000_000_000:
        value *= 1000.0

    return int(value)


def marginpad_event_fingerprint(event):

    return "|".join([
        str(event.get("ts", "")),
        str(event.get("exchange", "")),
        str(event.get("symbol", "")),
        str(event.get("side", "")),
        str(event.get("price", "")),
        str(event.get("qty", "")),
        str(event.get("notional", ""))
    ])


def remember_marginpad_event(fingerprint):

    if fingerprint in marginpad_seen_set:
        return

    marginpad_seen_set.add(fingerprint)
    marginpad_seen_queue.append(fingerprint)

    while len(marginpad_seen_queue) > MARGINPAD_SEEN_MAX:
        old = marginpad_seen_queue.popleft()
        marginpad_seen_set.discard(old)


def get_marginpad_btc_price():

    payload, error = marginpad_get(
        "/api/v1/price",
        params={
            "symbol": "BTC"
        },
        timeout=10,
        stage="marginpad-btc-price"
    )

    if error:
        return None, error

    try:
        data = payload.get("data", {})
        price = float(data["price"])

    except (
        AttributeError,
        KeyError,
        TypeError,
        ValueError
    ):
        return None, {
            "stage": "marginpad-btc-price",
            "error": "price missing or invalid",
            "response": str(payload)[:500]
        }

    return price, None


def extract_marginpad_events(payload):

    if not isinstance(payload, dict):
        return []

    data = payload.get("data")

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in (
            "events",
            "rows",
            "liquidations",
            "items"
        ):
            value = data.get(key)
            if isinstance(value, list):
                return value

    # Defensive fallback in case the endpoint returns
    # {events:[...]} outside the standard data envelope.
    value = payload.get("events")
    if isinstance(value, list):
        return value

    return []


def get_marginpad_fresh_btc_liquidations(
    previous_through_ms,
    closed_minute_ts
):

    payload, error = marginpad_get(
        "/api/v1/liquidations/live",
        params={
            "symbol": "BTC",
            "limit": MARGINPAD_LIVE_LIMIT
        },
        timeout=12,
        stage="marginpad-btc-liquidations"
    )

    if error:
        return None, error

    events = extract_marginpad_events(
        payload
    )

    if not isinstance(events, list):
        return None, {
            "stage": "marginpad-btc-liquidations",
            "error": "events payload is not a list"
        }

    closed_end_ms = (
        closed_minute_ts
        + 59
    ) * 1000 + 999

    if previous_through_ms is None:
        lower_bound_ms = (
            closed_end_ms
            - MARGINPAD_OVERLAP_MS
        )
    else:
        lower_bound_ms = max(
            0,
            previous_through_ms
            - MARGINPAD_OVERLAP_MS
        )

    fresh_long = 0.0
    fresh_short = 0.0
    accepted_events = 0
    newest_event_ms = None
    exchanges = set()
    fresh_by_exchange = {}
    rolling_events = []

    # Oldest first makes logging/debugging easier.
    normalized_events = []

    for event in events:

        if not isinstance(event, dict):
            continue

        # Defensive BTC symbol guard. The API is requested with symbol=BTC,
        # but we still verify each returned event before it can reach totals.
        event_symbol = str(
            event.get("symbol", "")
        ).strip().upper()

        if event_symbol and event_symbol != "BTC":
            print(
                "[MARGINPAD BTC SYMBOL REJECT] "
                f"symbol={event_symbol} | "
                f"exchange={event.get('exchange', '')} | "
                f"ts={event.get('ts', '')} | "
                f"side={event.get('side', '')} | "
                f"notional={event.get('notional', '')}"
            )
            continue

        event_ts_ms = normalize_marginpad_ts_ms(
            event.get("ts")
        )

        if event_ts_ms is None:
            continue

        normalized_events.append(
            (event_ts_ms, event)
        )

    normalized_events.sort(
        key=lambda item: item[0]
    )

    for event_ts_ms, event in normalized_events:

        if event_ts_ms > closed_end_ms:
            continue

        if event_ts_ms <= lower_bound_ms:
            continue

        fingerprint = (
            marginpad_event_fingerprint(
                event
            )
        )

        if fingerprint in marginpad_seen_set:
            continue

        # Debug only: log the full normalized MarginPad BTC event before
        # any parsing/accumulation so upstream anomalies can be traced.
        print(
            "[MARGINPAD BTC RAW EVENT] "
            + json.dumps(
                event,
                sort_keys=True,
                default=str
            ),
            flush=True
        )

        try:
            notional = float(
                event.get("notional", 0)
                or 0
            )
        except (
            TypeError,
            ValueError
        ):
            continue

        if notional < 0:
            notional = abs(notional)

        side = str(
            event.get("side", "")
        ).strip().lower()

        exchange = str(
            event.get("exchange", "")
        ).strip()
        exchange_key = exchange.lower() or "unknown"

        if side == "long_liquidated":
            fresh_long += notional
            bucket = fresh_by_exchange.setdefault(
                exchange_key, {"long": 0.0, "short": 0.0}
            )
            bucket["long"] += notional

        elif side == "short_liquidated":
            fresh_short += notional
            bucket = fresh_by_exchange.setdefault(
                exchange_key, {"long": 0.0, "short": 0.0}
            )
            bucket["short"] += notional

        else:
            continue

        if exchange:
            exchanges.add(exchange)

        # Audit every event that is actually accepted into BTC totals.
        # This does not change the calculation; it only makes anomalies traceable.
        print(
            "[MARGINPAD BTC ACCEPTED] "
            f"ts_ms={event_ts_ms} | "
            f"exchange={exchange or '-'} | "
            f"symbol={event_symbol or 'BTC'} | "
            f"side={side} | "
            f"price={event.get('price', '')} | "
            f"qty={event.get('qty', '')} | "
            f"notional=${notional:,.2f}"
        )

        remember_marginpad_event(
            fingerprint
        )
        rolling_events.append({"ts_ms": event_ts_ms, "exchange": exchange_key, "side": "long" if side == "long_liquidated" else "short", "notional": notional})

        accepted_events += 1

        if (
            newest_event_ms is None
            or event_ts_ms > newest_event_ms
        ):
            newest_event_ms = (
                event_ts_ms
            )

    print(
        "MARGINPAD BTC | "
        f"events_returned={len(events)} | "
        f"events_accepted={accepted_events} | "
        f"exchanges={len(exchanges)}"
    )

    return {
        "events_returned":
            len(events),

        "events_accepted":
            accepted_events,

        "fresh_long_usd":
            round(
                fresh_long,
                2
            ),

        "fresh_short_usd":
            round(
                fresh_short,
                2
            ),

        "fresh_net_short_minus_long":
            round(
                fresh_short
                -
                fresh_long,
                2
            ),

        "exchanges_seen":
            sorted(
                exchanges
            ),

        "fresh_by_exchange": {
            name: {
                "long": round(values.get("long", 0.0), 2),
                "short": round(values.get("short", 0.0), 2),
            }
            for name, values in fresh_by_exchange.items()
        },

        "newest_event_ts_ms":
            newest_event_ms,

        "rolling_events": rolling_events,

        "closed_end_ms":
            closed_end_ms
    }, None


# ==================================================
# MARGINPAD XAU HELPERS
# ==================================================

def remember_marginpad_xau_event(fingerprint):

    if fingerprint in marginpad_xau_seen_set:
        return

    marginpad_xau_seen_set.add(fingerprint)
    marginpad_xau_seen_queue.append(fingerprint)

    while len(marginpad_xau_seen_queue) > MARGINPAD_SEEN_MAX:
        old = marginpad_xau_seen_queue.popleft()
        marginpad_xau_seen_set.discard(old)


def get_marginpad_xau_price():

    payload, error = marginpad_get(
        "/api/v1/price",
        params={
            "symbol": "XAU"
        },
        timeout=10,
        stage="marginpad-xau-price"
    )

    if error:
        return None, error

    try:
        data = payload.get("data", {})

        if isinstance(data, dict) and "price" in data:
            price = float(data["price"])
        else:
            # Defensive fallback for a flat response such as
            # {"symbol":"XAU","price":4437.8}.
            price = float(payload["price"])

    except (
        AttributeError,
        KeyError,
        TypeError,
        ValueError
    ):
        return None, {
            "stage": "marginpad-xau-price",
            "error": "price missing or invalid",
            "response": str(payload)[:500]
        }

    return price, None


def get_marginpad_fresh_xau_liquidations(
    previous_through_ms,
    closed_minute_ts
):

    payload, error = marginpad_get(
        "/api/v1/liquidations/live",
        params={
            "symbol": "XAU",
            "limit": MARGINPAD_LIVE_LIMIT
        },
        timeout=12,
        stage="marginpad-xau-liquidations"
    )

    if error:
        return None, error

    events = extract_marginpad_events(
        payload
    )

    # The live XAU response can also be flat:
    # {"symbol":"XAU","events":[...]}.
    if not events and isinstance(payload, dict):
        value = payload.get("events")
        if isinstance(value, list):
            events = value

    if not isinstance(events, list):
        return None, {
            "stage": "marginpad-xau-liquidations",
            "error": "events payload is not a list"
        }

    closed_end_ms = (
        closed_minute_ts
        + 59
    ) * 1000 + 999

    if previous_through_ms is None:
        lower_bound_ms = (
            closed_end_ms
            - MARGINPAD_OVERLAP_MS
        )
    else:
        lower_bound_ms = max(
            0,
            previous_through_ms
            - MARGINPAD_OVERLAP_MS
        )

    fresh_long = 0.0
    fresh_short = 0.0
    accepted_events = 0
    newest_event_ms = None
    exchanges = set()
    rolling_events = []
    normalized_events = []

    for event in events:

        if not isinstance(event, dict):
            continue

        # Extra guard so a malformed mixed payload can never leak BTC
        # events into the XAU accumulator.
        event_symbol = str(
            event.get("symbol", "")
        ).strip().upper()

        if event_symbol and event_symbol != "XAU":
            continue

        event_ts_ms = normalize_marginpad_ts_ms(
            event.get("ts")
        )

        if event_ts_ms is None:
            continue

        normalized_events.append(
            (event_ts_ms, event)
        )

    normalized_events.sort(
        key=lambda item: item[0]
    )

    for event_ts_ms, event in normalized_events:

        if event_ts_ms > closed_end_ms:
            continue

        if event_ts_ms <= lower_bound_ms:
            continue

        fingerprint = marginpad_event_fingerprint(
            event
        )

        if fingerprint in marginpad_xau_seen_set:
            continue

        try:
            notional = float(
                event.get("notional", 0)
                or 0
            )
        except (
            TypeError,
            ValueError
        ):
            continue

        if notional < 0:
            notional = abs(notional)

        side = str(
            event.get("side", "")
        ).strip().lower()

        if side == "long_liquidated":
            fresh_long += notional

        elif side == "short_liquidated":
            fresh_short += notional

        else:
            continue

        exchange = str(
            event.get("exchange", "")
        ).strip()

        if exchange:
            exchanges.add(exchange)

        remember_marginpad_xau_event(
            fingerprint
        )

        rolling_events.append({
            "ts_ms": event_ts_ms,
            "exchange": exchange.lower() or "unknown",
            "side": "long" if side == "long_liquidated" else "short",
            "notional": notional,
        })

        accepted_events += 1

        if (
            newest_event_ms is None
            or event_ts_ms > newest_event_ms
        ):
            newest_event_ms = event_ts_ms

    print(
        "MARGINPAD XAU | "
        f"events_returned={len(events)} | "
        f"events_accepted={accepted_events} | "
        f"exchanges={len(exchanges)}"
    )

    return {
        "events_returned": len(events),
        "events_accepted": accepted_events,
        "fresh_long_usd": round(fresh_long, 2),
        "fresh_short_usd": round(fresh_short, 2),
        "fresh_net_short_minus_long": round(
            fresh_short - fresh_long,
            2
        ),
        "exchanges_seen": sorted(exchanges),
        "newest_event_ts_ms": newest_event_ms,
        "rolling_events": rolling_events,
        "closed_end_ms": closed_end_ms
    }, None


# ==================================================
# DEBUG: COINALYZE NVDA MARKET IDS
# ==================================================

@app.get("/debug/coinalyze-nvda")
def debug_coinalyze_nvda():

    markets, error = get_future_markets()

    if error:
        return jsonify({
            "ok": False,
            "error": error
        }), 500

    matches = []

    for market in (markets or []):
        if not isinstance(market, dict):
            continue

        # Coinalyze market payloads can evolve, so search both the
        # common fields and the full row text for NVDA / NVIDIA.
        searchable = " ".join([
            str(market.get("symbol", "")),
            str(market.get("base_asset", "")),
            str(market.get("quote_asset", "")),
            str(market.get("exchange", "")),
            str(market.get("name", "")),
            str(market.get("instrument", "")),
            str(market)
        ]).upper()

        if "NVDA" in searchable or "NVIDIA" in searchable:
            matches.append(market)
            print(
                "[COINALYZE NVDA MARKET] "
                + json.dumps(
                    market,
                    sort_keys=True,
                    default=str
                ),
                flush=True
            )

    print(
        f"[COINALYZE NVDA DEBUG] matches={len(matches)}",
        flush=True
    )

    return jsonify({
        "ok": True,
        "matches": len(matches),
        "markets": matches
    })


# ==================================================
# DEBUG: COINALYZE NVDA 5-MIN OHLC TEST
# ==================================================

@app.get("/debug/coinalyze-nvda-5m")
def debug_coinalyze_nvda_5m():

    # Candidate confirmed from /v1/future-markets.
    symbol = request.args.get(
        "symbol",
        "NVDAUSDT_PERP.A"
    ).strip()

    now = int(time.time())

    response, error = coinalyze_get(
        "https://api.coinalyze.net/v1/ohlcv-history",
        params={
            "symbols": symbol,
            "interval": "5min",
            "from": now - (6 * 60 * 60),
            "to": now
        },
        timeout=15,
        stage="nvda-5m-debug"
    )

    if error:
        return jsonify({
            "ok": False,
            "symbol": symbol,
            "error": error
        }), 500

    try:
        payload = response.json()

    except ValueError:
        return jsonify({
            "ok": False,
            "symbol": symbol,
            "error": "invalid json",
            "response": response.text[:1000]
        }), 500

    history = []

    if isinstance(payload, list) and payload:
        first = payload[0]

        if isinstance(first, dict):
            history = first.get(
                "history",
                []
            )

    # Keep only the latest 12 x 5-minute candles in browser output.
    latest = history[-12:] if history else []

    print(
        f"[COINALYZE NVDA 5M DEBUG] "
        f"symbol={symbol} | "
        f"candles={len(history)} | "
        f"latest={latest[-1] if latest else None}",
        flush=True
    )

    return jsonify({
        "ok": True,
        "symbol": symbol,
        "candles_returned": len(history),
        "latest_12": latest,
        "raw_series_meta": (
            {
                k: v
                for k, v in payload[0].items()
                if k != "history"
            }
            if (
                isinstance(payload, list)
                and payload
                and isinstance(payload[0], dict)
            )
            else {}
        )
    })



# ==================================================
# COINALYZE NVDA 3:30 IST FIXED OPEN ±1% STATE
# ==================================================

NVDA_COINALYZE_SYMBOL = os.environ.get(
    "NVDA_COINALYZE_SYMBOL",
    "NVDAUSDT_PERP.A"
)

NVDA_MOVE_PCT = float(
    os.environ.get(
        "NVDA_MOVE_PCT",
        "1.0"
    )
) / 100.0

NVDA_IST = ZoneInfo("Asia/Kolkata")

nvda_session_date_ist = None
nvda_session_open = None
nvda_state = 0
nvda_last_processed_candle_ts = None


# ==================================================
# NASDAQ TOP5 + BOTTOM5 COMBINED CONFIRMATION
# ==================================================
# Pushover-only phase.
# No stock-percentage math here and NO MT5 publication.
#
# Option A:
#   BUY  -> BUY  = COMBINED BUY  -> reset pending
#   SELL -> SELL = COMBINED SELL -> reset pending
#   BUY  -> SELL = no combined alert; SELL becomes pending
#   SELL -> BUY  = no combined alert; BUY becomes pending
#
# Source does not matter. TOP5/TOP5, BOTTOM5/BOTTOM5 and mixed pairs
# are all valid if the two consecutive directions match.

NASDAQ_COMBINED_IST = ZoneInfo("Asia/Kolkata")
nasdaq_combined_pending = {"HOURLY": None, "DAILY": None}
nasdaq_combined_recent = deque(maxlen=50)
_nasdaq_combined_lock = threading.RLock()


# ==================================================
# PERSISTENT RUNTIME STATE - BTC/XAU/NVDA
# ==================================================
# Core strategy logic is unchanged. This only preserves in-memory runtime
# state across Render deploys/restarts using the existing /var/data disk.

RUNTIME_STATE_FILE = os.path.join('/var/data', 'backend_runtime_state.json')
_runtime_state_lock = threading.Lock()


def _runtime_state_payload():
    return {
        'version': 1,
        'saved_at_utc': datetime.now(timezone.utc).isoformat(),

        'coinalyze_btc': {
            'long_cumulative': btc_long_cumulative,
            'short_cumulative': btc_short_cumulative,
            'cycle_ref_price': btc_cycle_ref_price,
            'last_processed_liq_ts': btc_last_processed_liq_ts,
            'last_alert_snapshot': btc_last_alert_snapshot,
        },

        'marginpad_btc': {
            'long_cumulative': marginpad_btc_long_cumulative,
            'short_cumulative': marginpad_btc_short_cumulative,
            'cycle_ref_price': marginpad_btc_cycle_ref_price,
            'processed_through_ms': marginpad_btc_processed_through_ms,
            'last_alert_snapshot': marginpad_btc_last_alert_snapshot,
            'by_exchange': marginpad_btc_by_exchange,
            'seen_queue': list(marginpad_seen_queue),
        },

        'direct_btc_liquidator': {
            'long_cumulative': direct_btc_long_cumulative,
            'short_cumulative': direct_btc_short_cumulative,
            'cycle_ref_price': direct_btc_cycle_ref_price,
            'last_alert_snapshot': direct_btc_last_alert_snapshot,
            'by_exchange': direct_btc_by_exchange,
        },

        'btc_observer': {
            'long_cumulative': btc_observer_long_cumulative,
            'short_cumulative': btc_observer_short_cumulative,
            'cycle_ref_price': btc_observer_cycle_ref_price,
            'last_alert_snapshot': btc_observer_last_alert_snapshot,
            'by_exchange': btc_observer_by_exchange,
        },

        'btc_gap_direction_states': {'coinalyze': btc_coinalyze_gap_state, 'observer': btc_observer_gap_state},
        'btc_rolling_60m': {
            'coinalyze_state': btc_coinalyze_rolling_state,
            'observer_state': btc_observer_rolling_state,
            'coinalyze_events': list(btc_coinalyze_rolling_events),
            'observer_events': list(btc_observer_rolling_events),
        },

        'all_crypto_marginpad': {
            'long_cumulative': all_crypto_long_cumulative,
            'short_cumulative': all_crypto_short_cumulative,
            'gap_state': all_crypto_gap_state,
            'cycle_start_ts': all_crypto_cycle_start_ts,
            'rolling_state': all_crypto_rolling_state,
            'rolling_events': list(all_crypto_rolling_events),
            'seen_queue': list(all_crypto_seen_queue),
            'by_symbol': all_crypto_by_symbol,
            'last_poll_ts': all_crypto_last_poll_ts,
            'crypto_symbols': sorted(all_crypto_crypto_symbols),
            'crypto_symbols_refreshed_ts': all_crypto_crypto_symbols_refreshed_ts,
            'first_poll_seeded': all_crypto_first_poll_seeded,
        },

        'coinalyze_xau': {
            'long_cumulative': xau_long_cumulative,
            'short_cumulative': xau_short_cumulative,
            'cycle_ref_price': xau_cycle_ref_price,
            'last_processed_liq_ts': xau_last_processed_liq_ts,
        },

        'xau_gap_direction_states': {
            'coinalyze': xau_coinalyze_gap_state,
            'observer': xau_observer_gap_state,
        },
        'xau_rolling_60m': {
            'coinalyze_state': xau_coinalyze_rolling_state,
            'observer_state': xau_observer_rolling_state,
            'coinalyze_events': list(xau_coinalyze_rolling_events),
            'observer_events': list(xau_observer_rolling_events),
        },

        'marginpad_xau': {
            'long_cumulative': marginpad_xau_long_cumulative,
            'short_cumulative': marginpad_xau_short_cumulative,
            'cycle_ref_price': marginpad_xau_cycle_ref_price,
            'processed_through_ms': marginpad_xau_processed_through_ms,
            'seen_queue': list(marginpad_xau_seen_queue),
        },

        'combined_liquidation': {
            'totals': combined_liq,
            'by_source': combined_by_source,
            'by_exchange': combined_by_exchange,
            'cycle_ref_price': combined_cycle_ref_price,
            'latest_price': combined_latest_price,
            'last_alert': combined_last_alert,
            'direct_seen_queue': list(combined_direct_seen_queue),
        },

        'mt5_bridge': {
            'latest_signals': mt5_latest_signals,
        },

        'nvda': {
            'session_date_ist': nvda_session_date_ist,
            'session_open': nvda_session_open,
            'state': nvda_state,
            'last_processed_candle_ts': nvda_last_processed_candle_ts,
        },

        'nasdaq_combined': {
            'pending': nasdaq_combined_pending,
            'recent': list(nasdaq_combined_recent),
        },
    }


def _save_runtime_state():
    try:
        os.makedirs(os.path.dirname(RUNTIME_STATE_FILE), exist_ok=True)
        payload = _runtime_state_payload()
        tmp_path = RUNTIME_STATE_FILE + '.tmp'
        with _runtime_state_lock:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, separators=(',', ':'), sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, RUNTIME_STATE_FILE)
        return True
    except Exception as exc:
        print(f'[RUNTIME STATE SAVE ERROR] {exc}', flush=True)
        return False


def _load_runtime_state():
    global btc_long_cumulative, btc_short_cumulative
    global btc_cycle_ref_price, btc_last_processed_liq_ts
    global btc_last_alert_snapshot
    global btc_coinalyze_gap_state
    global marginpad_btc_long_cumulative, marginpad_btc_short_cumulative
    global marginpad_btc_cycle_ref_price, marginpad_btc_processed_through_ms
    global marginpad_btc_last_alert_snapshot, marginpad_btc_by_exchange
    global marginpad_seen_queue, marginpad_seen_set
    global direct_btc_long_cumulative, direct_btc_short_cumulative
    global direct_btc_cycle_ref_price, direct_btc_last_alert_snapshot
    global direct_btc_by_exchange
    global btc_observer_long_cumulative, btc_observer_short_cumulative
    global btc_observer_cycle_ref_price, btc_observer_last_alert_snapshot
    global btc_observer_by_exchange
    global btc_coinalyze_gap_state, btc_observer_gap_state
    global btc_coinalyze_rolling_events, btc_observer_rolling_events
    global btc_coinalyze_rolling_state, btc_observer_rolling_state
    global all_crypto_long_cumulative, all_crypto_short_cumulative
    global all_crypto_gap_state, all_crypto_rolling_events, all_crypto_rolling_state
    global all_crypto_cycle_start_ts
    global all_crypto_seen_queue, all_crypto_seen_set, all_crypto_by_symbol
    global all_crypto_last_poll_ts, all_crypto_crypto_symbols
    global all_crypto_crypto_symbols_refreshed_ts, all_crypto_first_poll_seeded
    global xau_long_cumulative, xau_short_cumulative
    global xau_cycle_ref_price, xau_last_processed_liq_ts
    global xau_coinalyze_gap_state, xau_observer_gap_state
    global xau_coinalyze_rolling_events, xau_observer_rolling_events
    global xau_coinalyze_rolling_state, xau_observer_rolling_state
    global marginpad_xau_long_cumulative, marginpad_xau_short_cumulative
    global marginpad_xau_cycle_ref_price, marginpad_xau_processed_through_ms
    global marginpad_xau_seen_queue, marginpad_xau_seen_set
    global combined_liq, combined_by_source, combined_by_exchange
    global combined_cycle_ref_price, combined_latest_price, combined_last_alert
    global combined_direct_seen_queue, combined_direct_seen_set
    global mt5_latest_signals
    global nvda_session_date_ist, nvda_session_open
    global nvda_state, nvda_last_processed_candle_ts
    global nasdaq_combined_pending, nasdaq_combined_recent

    if not os.path.exists(RUNTIME_STATE_FILE):
        print('[RUNTIME STATE] no saved state yet', flush=True)
        return False

    try:
        with _runtime_state_lock:
            with open(RUNTIME_STATE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)

        cbtc = data.get('coinalyze_btc') or {}
        btc_long_cumulative = float(cbtc.get('long_cumulative', 0.0) or 0.0)
        btc_short_cumulative = float(cbtc.get('short_cumulative', 0.0) or 0.0)
        btc_cycle_ref_price = cbtc.get('cycle_ref_price')
        btc_last_processed_liq_ts = cbtc.get('last_processed_liq_ts')
        btc_last_alert_snapshot = cbtc.get('last_alert_snapshot')

        mbtc = data.get('marginpad_btc') or {}
        marginpad_btc_long_cumulative = float(mbtc.get('long_cumulative', 0.0) or 0.0)
        marginpad_btc_short_cumulative = float(mbtc.get('short_cumulative', 0.0) or 0.0)
        marginpad_btc_cycle_ref_price = mbtc.get('cycle_ref_price')
        marginpad_btc_processed_through_ms = mbtc.get('processed_through_ms')
        marginpad_btc_last_alert_snapshot = mbtc.get('last_alert_snapshot')
        marginpad_btc_by_exchange = {}
        for ex_name, ex_totals in (mbtc.get('by_exchange') or {}).items():
            if isinstance(ex_totals, dict):
                marginpad_btc_by_exchange[str(ex_name).lower()] = {
                    'long': float(ex_totals.get('long', 0.0) or 0.0),
                    'short': float(ex_totals.get('short', 0.0) or 0.0),
                }
        mbtc_seen = list(mbtc.get('seen_queue') or [])[-MARGINPAD_SEEN_MAX:]
        marginpad_seen_queue = deque(mbtc_seen)
        marginpad_seen_set = set(mbtc_seen)

        dbtc = data.get('direct_btc_liquidator') or {}
        direct_btc_long_cumulative = float(dbtc.get('long_cumulative', 0.0) or 0.0)
        direct_btc_short_cumulative = float(dbtc.get('short_cumulative', 0.0) or 0.0)
        direct_btc_cycle_ref_price = dbtc.get('cycle_ref_price')
        direct_btc_last_alert_snapshot = dbtc.get('last_alert_snapshot')
        direct_btc_by_exchange = {
            ex: {'long': 0.0, 'short': 0.0}
            for ex in COMBINED_DIRECT_EXCHANGES
        }
        for ex_name, ex_totals in (dbtc.get('by_exchange') or {}).items():
            if ex_name in direct_btc_by_exchange and isinstance(ex_totals, dict):
                direct_btc_by_exchange[ex_name] = {
                    'long': float(ex_totals.get('long', 0.0) or 0.0),
                    'short': float(ex_totals.get('short', 0.0) or 0.0),
                }

        observer = data.get('btc_observer') or {}
        btc_observer_long_cumulative = float(observer.get('long_cumulative', 0.0) or 0.0)
        btc_observer_short_cumulative = float(observer.get('short_cumulative', 0.0) or 0.0)
        btc_observer_cycle_ref_price = observer.get('cycle_ref_price')
        btc_observer_last_alert_snapshot = observer.get('last_alert_snapshot')
        btc_observer_by_exchange = {
            ex: {'long': 0.0, 'short': 0.0}
            for ex in BTC_OBSERVER_EXCHANGES
        }
        for ex_name, ex_totals in (observer.get('by_exchange') or {}).items():
            key = _btc_exchange_key(ex_name)
            if key in btc_observer_by_exchange and isinstance(ex_totals, dict):
                btc_observer_by_exchange[key] = {
                    'long': float(ex_totals.get('long', 0.0) or 0.0),
                    'short': float(ex_totals.get('short', 0.0) or 0.0),
                }

        gap_states = data.get('btc_gap_direction_states') or {}
        btc_coinalyze_gap_state = gap_states.get('coinalyze') if gap_states.get('coinalyze') in ('LONG','SHORT') else None
        btc_observer_gap_state = gap_states.get('observer') if gap_states.get('observer') in ('LONG','SHORT') else None
        rolling = data.get('btc_rolling_60m') or {}
        btc_coinalyze_rolling_state = rolling.get('coinalyze_state') if rolling.get('coinalyze_state') in ('LONG','SHORT') else None
        btc_observer_rolling_state = rolling.get('observer_state') if rolling.get('observer_state') in ('LONG','SHORT') else None
        def _restore_roll(rows):
            out = deque(); cutoff = time.time() - BTC_ROLLING_WINDOW_SECONDS
            for row in (rows or []):
                try:
                    ts=float(row[0]); side=str(row[1]); amount=float(row[2]); ex=str(row[3]) if len(row)>3 else ""
                    if ts > 10_000_000_000: ts /= 1000.0
                    if ts > cutoff and side in ('long','short') and amount > 0: out.append((ts,side,amount,ex))
                except (TypeError,ValueError,IndexError): pass
            return deque(sorted(out,key=lambda r:r[0]))
        btc_coinalyze_rolling_events = _restore_roll(rolling.get('coinalyze_events'))
        btc_observer_rolling_events = _restore_roll(rolling.get('observer_events'))

        allc = data.get('all_crypto_marginpad') or {}
        all_crypto_long_cumulative = float(allc.get('long_cumulative', 0.0) or 0.0)
        all_crypto_short_cumulative = float(allc.get('short_cumulative', 0.0) or 0.0)
        all_crypto_gap_state = allc.get('gap_state') if allc.get('gap_state') in ('LONG','SHORT') else None
        _saved_all_crypto_cycle_start = allc.get('cycle_start_ts')
        try:
            all_crypto_cycle_start_ts = float(_saved_all_crypto_cycle_start) if _saved_all_crypto_cycle_start is not None else None
        except (TypeError, ValueError):
            all_crypto_cycle_start_ts = None
        # Backward compatibility for a cycle already in progress before this field existed.
        if all_crypto_cycle_start_ts is None and (all_crypto_long_cumulative > 0 or all_crypto_short_cumulative > 0):
            all_crypto_cycle_start_ts = time.time()
        all_crypto_rolling_state = allc.get('rolling_state') if allc.get('rolling_state') in ('LONG','SHORT') else None
        _all_cutoff = time.time() - ALL_CRYPTO_ROLLING_WINDOW_SECONDS
        _all_rows = deque()
        for row in (allc.get('rolling_events') or []):
            try:
                ts=float(row[0]); side=str(row[1]); amount=float(row[2]); symbol=str(row[3]) if len(row)>3 else ""
                if ts > 10_000_000_000: ts /= 1000.0
                if ts > _all_cutoff and side in ('long','short') and amount > 0:
                    _all_rows.append((ts,side,amount,symbol))
            except (TypeError,ValueError,IndexError):
                pass
        all_crypto_rolling_events = deque(sorted(_all_rows,key=lambda r:r[0]))
        _all_seen = list(allc.get('seen_queue') or [])[-ALL_CRYPTO_SEEN_MAX:]
        all_crypto_seen_queue = deque(_all_seen)
        all_crypto_seen_set = set(_all_seen)
        all_crypto_by_symbol = {}
        for sym, totals in (allc.get('by_symbol') or {}).items():
            if isinstance(totals, dict):
                all_crypto_by_symbol[str(sym).upper()] = {
                    'long': float(totals.get('long', 0.0) or 0.0),
                    'short': float(totals.get('short', 0.0) or 0.0),
                }
        all_crypto_last_poll_ts = allc.get('last_poll_ts')
        all_crypto_crypto_symbols = set(str(x).upper() for x in (allc.get('crypto_symbols') or []) if x)
        all_crypto_crypto_symbols_refreshed_ts = float(allc.get('crypto_symbols_refreshed_ts', 0.0) or 0.0)
        all_crypto_first_poll_seeded = bool(allc.get('first_poll_seeded', False))

        cxau = data.get('coinalyze_xau') or {}
        xau_long_cumulative = float(cxau.get('long_cumulative', 0.0) or 0.0)
        xau_short_cumulative = float(cxau.get('short_cumulative', 0.0) or 0.0)
        xau_cycle_ref_price = cxau.get('cycle_ref_price')
        xau_last_processed_liq_ts = cxau.get('last_processed_liq_ts')

        xgap = data.get('xau_gap_direction_states') or {}
        xau_coinalyze_gap_state = xgap.get('coinalyze') if xgap.get('coinalyze') in ('LONG','SHORT') else None
        xau_observer_gap_state = xgap.get('observer') if xgap.get('observer') in ('LONG','SHORT') else None
        xroll = data.get('xau_rolling_60m') or {}
        xau_coinalyze_rolling_state = xroll.get('coinalyze_state') if xroll.get('coinalyze_state') in ('LONG','SHORT') else None
        xau_observer_rolling_state = xroll.get('observer_state') if xroll.get('observer_state') in ('LONG','SHORT') else None
        def _restore_xau_roll(rows):
            out = deque(); cutoff = time.time() - XAU_ROLLING_WINDOW_SECONDS
            for row in (rows or []):
                try:
                    ts=float(row[0]); side=str(row[1]); amount=float(row[2]); ex=str(row[3]) if len(row)>3 else ""
                    if ts > 10_000_000_000: ts /= 1000.0
                    if ts > cutoff and side in ('long','short') and amount > 0: out.append((ts,side,amount,ex))
                except (TypeError,ValueError,IndexError): pass
            return deque(sorted(out,key=lambda r:r[0]))
        xau_coinalyze_rolling_events = _restore_xau_roll(xroll.get('coinalyze_events'))
        xau_observer_rolling_events = _restore_xau_roll(xroll.get('observer_events'))

        mxau = data.get('marginpad_xau') or {}
        marginpad_xau_long_cumulative = float(mxau.get('long_cumulative', 0.0) or 0.0)
        marginpad_xau_short_cumulative = float(mxau.get('short_cumulative', 0.0) or 0.0)
        marginpad_xau_cycle_ref_price = mxau.get('cycle_ref_price')
        marginpad_xau_processed_through_ms = mxau.get('processed_through_ms')
        mxau_seen = list(mxau.get('seen_queue') or [])[-MARGINPAD_SEEN_MAX:]
        marginpad_xau_seen_queue = deque(mxau_seen)
        marginpad_xau_seen_set = set(mxau_seen)

        comb = data.get('combined_liquidation') or {}
        saved_totals = comb.get('totals') or {}
        saved_by_source = comb.get('by_source') or {}
        saved_by_exchange = comb.get('by_exchange') or {}

        for asset in ("BTC", "XAU"):
            asset_totals = saved_totals.get(asset) or {}
            combined_liq[asset]["long"] = float(asset_totals.get("long", 0.0) or 0.0)
            combined_liq[asset]["short"] = float(asset_totals.get("short", 0.0) or 0.0)

            asset_sources = saved_by_source.get(asset) or {}
            for source in COMBINED_SOURCE_KEYS:
                source_totals = asset_sources.get(source) or {}
                combined_by_source[asset][source]["long"] = float(source_totals.get("long", 0.0) or 0.0)
                combined_by_source[asset][source]["short"] = float(source_totals.get("short", 0.0) or 0.0)

            combined_by_exchange[asset] = {}
            asset_exchanges = saved_by_exchange.get(asset) or {}
            for ex_name, ex_totals in asset_exchanges.items():
                if not isinstance(ex_totals, dict):
                    continue
                combined_by_exchange[asset][str(ex_name).lower()] = {
                    "long": float(ex_totals.get("long", 0.0) or 0.0),
                    "short": float(ex_totals.get("short", 0.0) or 0.0),
                }

            # One-time migration from older runtime state that did not yet
            # persist per-exchange MarginPad totals. Keep any in-flight cycle
            # reconcilable instead of silently losing its pre-upgrade amount.
            if asset == "BTC" and not asset_exchanges:
                for direct_name in COMBINED_DIRECT_EXCHANGES:
                    direct_totals = combined_by_source[asset][direct_name]
                    if direct_totals["long"] > 0 or direct_totals["short"] > 0:
                        combined_by_exchange[asset][direct_name] = dict(direct_totals)

                legacy_mp = combined_by_source[asset]["marginpad"]
                if legacy_mp["long"] > 0 or legacy_mp["short"] > 0:
                    combined_by_exchange[asset]["marginpad_preupgrade"] = dict(legacy_mp)

        saved_ref = comb.get('cycle_ref_price') or {}
        saved_latest = comb.get('latest_price') or {}
        saved_last_alert = comb.get('last_alert') or {}
        for asset in ("BTC", "XAU"):
            combined_cycle_ref_price[asset] = saved_ref.get(asset)
            combined_latest_price[asset] = saved_latest.get(asset)
            combined_last_alert[asset] = saved_last_alert.get(asset)

        direct_seen = list(comb.get('direct_seen_queue') or [])[-COMBINED_DIRECT_SEEN_MAX:]
        combined_direct_seen_queue = deque(direct_seen)
        combined_direct_seen_set = set(direct_seen)

        mt5_saved = data.get('mt5_bridge') or {}
        saved_signals = mt5_saved.get('latest_signals') or {}
        for asset in ("BTC", "XAU"):
            candidate = saved_signals.get(asset)
            mt5_latest_signals[asset] = candidate if isinstance(candidate, dict) else None

        nvd = data.get('nvda') or {}
        nvda_session_date_ist = nvd.get('session_date_ist')
        nvda_session_open = nvd.get('session_open')
        nvda_state = int(nvd.get('state', 0) or 0)
        nvda_last_processed_candle_ts = nvd.get('last_processed_candle_ts')

        nas_comb = data.get('nasdaq_combined') or {}
        saved_pending = nas_comb.get('pending')

        # New format: {"HOURLY": <pending-or-None>, "DAILY": <pending-or-None>}.
        # Backward compatibility: the old backend stored one pending dict;
        # Daily titles were not recognized then, so migrate that old pending to HOURLY.
        if isinstance(saved_pending, dict) and (
            "HOURLY" in saved_pending or "DAILY" in saved_pending
        ):
            hourly_pending = saved_pending.get("HOURLY")
            daily_pending = saved_pending.get("DAILY")
        elif isinstance(saved_pending, dict) and saved_pending.get('direction') in ('BUY', 'SELL'):
            hourly_pending = saved_pending
            daily_pending = None
        else:
            hourly_pending = None
            daily_pending = None

        nasdaq_combined_pending = {
            "HOURLY": (
                hourly_pending
                if isinstance(hourly_pending, dict)
                and hourly_pending.get('direction') in ('BUY', 'SELL')
                else None
            ),
            "DAILY": (
                daily_pending
                if isinstance(daily_pending, dict)
                and daily_pending.get('direction') in ('BUY', 'SELL')
                else None
            ),
        }

        saved_recent = list(nas_comb.get('recent') or [])[-50:]
        nasdaq_combined_recent = deque(saved_recent, maxlen=50)

        print(
            '[RUNTIME STATE RESTORED] '
            f'BTC C={btc_long_cumulative:.0f}/{btc_short_cumulative:.0f} | '
            f'BTC M={marginpad_btc_long_cumulative:.0f}/{marginpad_btc_short_cumulative:.0f} | '
            f'XAU C={xau_long_cumulative:.0f}/{xau_short_cumulative:.0f} | '
            f'XAU M={marginpad_xau_long_cumulative:.0f}/{marginpad_xau_short_cumulative:.0f} | '
            f'COMB BTC={combined_liq["BTC"]["long"]:.0f}/{combined_liq["BTC"]["short"]:.0f} | '
            f'COMB XAU={combined_liq["XAU"]["long"]:.0f}/{combined_liq["XAU"]["short"]:.0f} | '
            f'NVDA state={nvda_state}',
            flush=True
        )
        return True
    except Exception as exc:
        print(f'[RUNTIME STATE LOAD ERROR] {exc}', flush=True)
        return False


_runtime_state_restored = _load_runtime_state()


@app.after_request
def _persist_runtime_state_after_request(response):
    # One worker is used on Render. Persist after every completed request so
    # cron-triggered state changes survive the next deploy/restart.
    _save_runtime_state()
    return response


@app.get('/debug/runtime-state')
def debug_runtime_state():
    return jsonify({
        'ok': True,
        'state_file': RUNTIME_STATE_FILE,
        'state_file_exists': os.path.exists(RUNTIME_STATE_FILE),
        'restored_on_startup': _runtime_state_restored,
        'coinalyze_btc': {
            'long': btc_long_cumulative,
            'short': btc_short_cumulative,
            'ref_price': btc_cycle_ref_price,
            'last_processed': btc_last_processed_liq_ts,
        },
        'marginpad_btc': {
            'long': marginpad_btc_long_cumulative,
            'short': marginpad_btc_short_cumulative,
            'ref_price': marginpad_btc_cycle_ref_price,
            'processed_through_ms': marginpad_btc_processed_through_ms,
            'seen_count': len(marginpad_seen_set),
        },
        'all_crypto_marginpad': {
            'long': all_crypto_long_cumulative,
            'short': all_crypto_short_cumulative,
            'gap': all_crypto_long_cumulative - all_crypto_short_cumulative,
            'gap_state': all_crypto_gap_state,
            'cycle_start_ts': all_crypto_cycle_start_ts,
            'cycle_age': _format_accumulation_duration(all_crypto_cycle_start_ts),
            'rolling_state': all_crypto_rolling_state,
            'rolling_event_count': len(all_crypto_rolling_events),
            'seen_count': len(all_crypto_seen_set),
            'symbol_count': len(all_crypto_crypto_symbols),
            'last_poll_ts': all_crypto_last_poll_ts,
            'last_error': all_crypto_last_error,
        },
        'coinalyze_xau': {
            'long': xau_long_cumulative,
            'short': xau_short_cumulative,
            'ref_price': xau_cycle_ref_price,
            'last_processed': xau_last_processed_liq_ts,
        },
        'marginpad_xau': {
            'long': marginpad_xau_long_cumulative,
            'short': marginpad_xau_short_cumulative,
            'ref_price': marginpad_xau_cycle_ref_price,
            'processed_through_ms': marginpad_xau_processed_through_ms,
            'seen_count': len(marginpad_xau_seen_set),
        },
        'nvda': {
            'session_date_ist': nvda_session_date_ist,
            'session_open': nvda_session_open,
            'state': nvda_state,
            'last_processed_candle_ts': nvda_last_processed_candle_ts,
        },
        'nasdaq_combined': {
            'pending': nasdaq_combined_pending,
            'recent_count': len(nasdaq_combined_recent),
            'phase': 'PUSHOVER_ONLY',
            'option': 'A_RESET_AFTER_CONFIRMED_PAIR',
        },
    })


def _nvda_active_anchor_ist(now_ist):

    anchor = now_ist.replace(
        hour=3,
        minute=30,
        second=0,
        microsecond=0
    )

    if now_ist < anchor:
        anchor -= timedelta(days=1)

    return anchor


def _nvda_fetch_5m_history(from_ts, to_ts):

    response, error = coinalyze_get(
        "https://api.coinalyze.net/v1/ohlcv-history",
        params={
            "symbols": NVDA_COINALYZE_SYMBOL,
            "interval": "5min",
            "from": int(from_ts),
            "to": int(to_ts)
        },
        timeout=15,
        stage="nvda-5m-state"
    )

    if error:
        return None, error

    try:
        payload = response.json()

    except ValueError:
        return None, {
            "stage": "nvda-5m-state",
            "error": "invalid json"
        }

    if (
        not isinstance(payload, list)
        or not payload
        or not isinstance(payload[0], dict)
    ):
        return [], None

    history = payload[0].get(
        "history",
        []
    )

    if not isinstance(history, list):
        history = []

    history = [
        row
        for row in history
        if isinstance(row, dict)
    ]

    history.sort(
        key=lambda row: int(
            row.get("t", 0)
        )
    )

    return history, None


def _nvda_find_session_open(history, anchor_ist):

    target_ts = int(
        anchor_ist.astimezone(
            timezone.utc
        ).timestamp()
    )

    for row in history:
        try:
            if int(row.get("t")) == target_ts:
                return float(row.get("o"))
        except (TypeError, ValueError):
            continue

    return None


def _nvda_alert_payload(direction, session_open, close_price):

    upper = session_open * (
        1 + NVDA_MOVE_PCT
    )

    lower = session_open * (
        1 - NVDA_MOVE_PCT
    )

    if direction == 1:
        title = "COINALYZE NVDA +1% STATE"
        message = (
            "COINALYZE NVDA +1% STATE"
            f" | 3:30 OPEN {session_open:.2f}"
            f" | +1% LEVEL {upper:.2f}"
            f" | CLOSE {close_price:.2f}"
            f" | NEXT -1% LEVEL {lower:.2f}"
            f" | SOURCE COINALYZE {NVDA_COINALYZE_SYMBOL}"
        )

    else:
        title = "COINALYZE NVDA -1% STATE"
        message = (
            "COINALYZE NVDA -1% STATE"
            f" | 3:30 OPEN {session_open:.2f}"
            f" | -1% LEVEL {lower:.2f}"
            f" | CLOSE {close_price:.2f}"
            f" | NEXT +1% LEVEL {upper:.2f}"
            f" | SOURCE COINALYZE {NVDA_COINALYZE_SYMBOL}"
        )

    return title, message


@app.get("/nvda-5m-alert")
def nvda_5m_alert():

    global nvda_session_date_ist
    global nvda_session_open
    global nvda_state
    global nvda_last_processed_candle_ts

    now_utc = datetime.now(
        timezone.utc
    )

    now_ist = now_utc.astimezone(
        NVDA_IST
    )

    anchor_ist = _nvda_active_anchor_ist(
        now_ist
    )

    anchor_date = anchor_ist.date().isoformat()

    # Pull enough history to include the active 03:30 IST candle
    # plus all confirmed 5-minute closes since then.
    from_utc = (
        anchor_ist
        - timedelta(minutes=5)
    ).astimezone(
        timezone.utc
    )

    history, error = _nvda_fetch_5m_history(
        from_utc.timestamp(),
        now_utc.timestamp()
    )

    if error:
        return jsonify({
            "ok": False,
            "error": error
        }), 500

    if not history:
        return jsonify({
            "ok": False,
            "error": "no NVDA 5-minute history returned",
            "symbol": NVDA_COINALYZE_SYMBOL
        }), 503

    session_open = _nvda_find_session_open(
        history,
        anchor_ist
    )

    if session_open is None:
        return jsonify({
            "ok": False,
            "error": "03:30 IST candle open not found",
            "session_date_ist": anchor_date,
            "symbol": NVDA_COINALYZE_SYMBOL
        }), 503

    # New 03:30 IST session -> reset state exactly like the Pine script.
    if (
        nvda_session_date_ist != anchor_date
        or nvda_session_open is None
    ):
        nvda_session_date_ist = anchor_date
        nvda_session_open = session_open
        nvda_state = 0
        nvda_last_processed_candle_ts = None

        print(
            "[NVDA NEW 3:30 SESSION] "
            f"date_ist={anchor_date} | "
            f"open={session_open:.2f}",
            flush=True
        )

    upper = nvda_session_open * (
        1 + NVDA_MOVE_PCT
    )

    lower = nvda_session_open * (
        1 - NVDA_MOVE_PCT
    )

    now_ts = int(
        now_utc.timestamp()
    )

    confirmed_rows = []

    for row in history:
        try:
            candle_ts = int(
                row.get("t")
            )
            close_price = float(
                row.get("c")
            )
        except (TypeError, ValueError):
            continue

        # Confirm only after the full 5-minute candle has closed.
        if candle_ts + 300 <= now_ts:
            confirmed_rows.append(
                (candle_ts, close_price)
            )

    if not confirmed_rows:
        return jsonify({
            "ok": True,
            "symbol": NVDA_COINALYZE_SYMBOL,
            "session_open": round(nvda_session_open, 4),
            "upper_level": round(upper, 4),
            "lower_level": round(lower, 4),
            "state": nvda_state,
            "message": "no confirmed 5-minute candle yet"
        })

    confirmed_rows.sort(
        key=lambda item: item[0]
    )

    signals_sent = []

    # On a fresh process, initialize at the latest confirmed candle without
    # replaying historical alerts. Future calls then process only new closes.
    if nvda_last_processed_candle_ts is None:

        latest_ts, latest_close = confirmed_rows[-1]

        inferred_state = 0

        for candle_ts, close_price in confirmed_rows:

            if (
                inferred_state != 1
                and close_price >= upper
            ):
                inferred_state = 1

            elif (
                inferred_state != -1
                and close_price <= lower
            ):
                inferred_state = -1

        nvda_state = inferred_state
        nvda_last_processed_candle_ts = latest_ts

        print(
            "[NVDA INIT] "
            f"state={nvda_state} | "
            f"last_close={latest_close:.2f} | "
            f"last_ts={latest_ts}",
            flush=True
        )

    else:

        for candle_ts, close_price in confirmed_rows:

            if candle_ts <= nvda_last_processed_candle_ts:
                continue

            direction = 0

            if (
                nvda_state != 1
                and close_price >= upper
            ):
                direction = 1

            elif (
                nvda_state != -1
                and close_price <= lower
            ):
                direction = -1

            if direction != 0:

                nvda_state = direction

                title, message = _nvda_alert_payload(
                    direction,
                    nvda_session_open,
                    close_price
                )

                sent = send_pushover(
                    title,
                    message
                )

                signals_sent.append({
                    "title": title,
                    "message": message,
                    "pushover_sent": bool(sent),
                    "candle_ts": candle_ts
                })

                print(
                    f"[NVDA STATE ALERT] "
                    f"{title} | {message}",
                    flush=True
                )

            nvda_last_processed_candle_ts = candle_ts

    latest_ts, latest_close = confirmed_rows[-1]

    latest_ist = datetime.fromtimestamp(
        latest_ts,
        tz=timezone.utc
    ).astimezone(
        NVDA_IST
    )

    return jsonify({
        "ok": True,
        "symbol": NVDA_COINALYZE_SYMBOL,
        "session_date_ist": nvda_session_date_ist,
        "session_open": round(nvda_session_open, 4),
        "upper_level": round(upper, 4),
        "lower_level": round(lower, 4),
        "state": nvda_state,
        "last_confirmed_5m_open_ist": latest_ist.isoformat(),
        "last_confirmed_close": round(latest_close, 4),
        "last_processed_candle_ts": nvda_last_processed_candle_ts,
        "signals_sent": signals_sent
    })


@app.get("/debug/coinalyze-nvda-state")
def debug_coinalyze_nvda_state():

    return jsonify({
        "symbol": NVDA_COINALYZE_SYMBOL,
        "move_pct": NVDA_MOVE_PCT * 100,
        "session_date_ist": nvda_session_date_ist,
        "session_open": nvda_session_open,
        "state": nvda_state,
        "last_processed_candle_ts": nvda_last_processed_candle_ts
    })


# ==================================================
# HOME
# ==================================================


# ==========================================================
# ZERODHA / KITE CONNECT AUTH
# ==========================================================
ZERODHA_API_KEY = os.getenv("ZERODHA_API_KEY", "").strip()
ZERODHA_API_SECRET = os.getenv("ZERODHA_API_SECRET", "").strip()
ZERODHA_STATE_DIR = os.getenv("ZERODHA_STATE_DIR", "/var/data").strip() or "/var/data"
ZERODHA_TOKEN_FILE = os.getenv(
    "ZERODHA_TOKEN_FILE",
    os.path.join(ZERODHA_STATE_DIR, "zerodha_token.json"),
)

zerodha_access_token = None
zerodha_access_token_created_at = None


def _zerodha_atomic_json_write(path, payload):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    os.replace(tmp, path)


def _zerodha_restore_token_from_disk():
    global zerodha_access_token, zerodha_access_token_created_at
    try:
        with open(ZERODHA_TOKEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        token = (data.get("access_token") or "").strip()
        if token:
            zerodha_access_token = token
            zerodha_access_token_created_at = data.get("token_created_at_utc")
            print("[ZERODHA TOKEN] restored from persistent disk", flush=True)
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f"[ZERODHA TOKEN RESTORE ERROR] {exc}", flush=True)


_zerodha_restore_token_from_disk()


@app.get("/zerodha-login")
def zerodha_login():
    """Start the official Kite Connect login flow."""
    if not ZERODHA_API_KEY or not ZERODHA_API_SECRET:
        return jsonify({
            "ok": False,
            "error": "ZERODHA_API_KEY / ZERODHA_API_SECRET missing in environment"
        }), 500

    login_url = (
        "https://kite.zerodha.com/connect/login"
        f"?v=3&api_key={ZERODHA_API_KEY}"
    )
    return redirect(login_url, code=302)


@app.get("/zerodha-callback")
def zerodha_callback():
    """Exchange Kite request_token for access_token without exposing secrets."""
    global zerodha_access_token, zerodha_access_token_created_at

    if not ZERODHA_API_KEY or not ZERODHA_API_SECRET:
        return jsonify({
            "ok": False,
            "error": "ZERODHA_API_KEY / ZERODHA_API_SECRET missing in environment"
        }), 500

    status = (request.args.get("status") or "").strip().lower()
    request_token = (request.args.get("request_token") or "").strip()

    if status == "error":
        return jsonify({
            "ok": False,
            "error": request.args.get("message") or "Kite login returned an error"
        }), 400

    if not request_token:
        return jsonify({
            "ok": False,
            "error": "request_token missing from Zerodha callback"
        }), 400

    import hashlib
    checksum = hashlib.sha256(
        f"{ZERODHA_API_KEY}{request_token}{ZERODHA_API_SECRET}".encode("utf-8")
    ).hexdigest()

    try:
        resp = requests.post(
            "https://api.kite.trade/session/token",
            data={
                "api_key": ZERODHA_API_KEY,
                "request_token": request_token,
                "checksum": checksum,
            },
            headers={"X-Kite-Version": "3"},
            timeout=20,
        )
        data = resp.json()
    except Exception as exc:
        return jsonify({
            "ok": False,
            "error": f"Kite token exchange failed: {exc}"
        }), 502

    if resp.status_code >= 400 or data.get("status") != "success":
        return jsonify({
            "ok": False,
            "error": data.get("message") or "Kite token exchange failed",
            "http_status": resp.status_code,
        }), 502

    token = ((data.get("data") or {}).get("access_token") or "").strip()
    if not token:
        return jsonify({
            "ok": False,
            "error": "Kite response did not contain access_token"
        }), 502

    zerodha_access_token = token
    zerodha_access_token_created_at = datetime.now(timezone.utc).isoformat()

    user_id = (data.get("data") or {}).get("user_id")
    try:
        _zerodha_atomic_json_write(
            ZERODHA_TOKEN_FILE,
            {
                "access_token": zerodha_access_token,
                "token_created_at_utc": zerodha_access_token_created_at,
                "user_id": user_id,
            },
        )
    except Exception as exc:
        return jsonify({
            "ok": False,
            "error": f"Zerodha login succeeded but token could not be persisted: {exc}"
        }), 500

    return jsonify({
        "ok": True,
        "message": "Zerodha login successful. Access token stored on persistent disk and loaded in this web-service process.",
        "user_id": user_id,
        "token_created_at_utc": zerodha_access_token_created_at,
        "next": "Use /zerodha-auth-status to verify token state. Token value is intentionally not returned."
    })


@app.get("/zerodha-auth-status")
def zerodha_auth_status():
    """Safe status endpoint; never returns the access token itself."""
    return jsonify({
        "ok": True,
        "api_key_configured": bool(ZERODHA_API_KEY),
        "api_secret_configured": bool(ZERODHA_API_SECRET),
        "access_token_present": bool(zerodha_access_token),
        "token_created_at_utc": zerodha_access_token_created_at,
        "persistent_token_file": ZERODHA_TOKEN_FILE,
        "persistent_token_file_exists": os.path.exists(ZERODHA_TOKEN_FILE),
    })


# ==========================================================
# ZERODHA NIFTY SPOT + FIXED 5-STRIKE OI TEST
# ==========================================================
ZERODHA_IST = ZoneInfo("Asia/Kolkata")


def _zerodha_headers():
    if not zerodha_access_token:
        raise RuntimeError("Zerodha access token is not present. Open /zerodha-login and authenticate first.")
    return {
        "Authorization": f"token {ZERODHA_API_KEY}:{zerodha_access_token}",
        "X-Kite-Version": "3",
    }


def _zerodha_json_get(url, *, params=None, timeout=25):
    resp = requests.get(url, headers=_zerodha_headers(), params=params, timeout=timeout)
    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"Kite returned non-JSON response (HTTP {resp.status_code})")

    if resp.status_code >= 400 or data.get("status") != "success":
        msg = data.get("message") or f"Kite HTTP {resp.status_code}"
        raise RuntimeError(msg)
    return data.get("data") or {}


@app.get("/zerodha-nifty-oi-test")
def zerodha_nifty_oi_test():
    """
    Read-only test endpoint:
      1) NIFTY 50 spot
      2) nearest NIFTY option expiry
      3) closest ATM strike
      4) ATM +/- 2 strikes (5 strikes total)
      5) CE/PE full quotes including current OI

    No orders are placed.
    """
    if not ZERODHA_API_KEY or not ZERODHA_API_SECRET:
        return jsonify({
            "ok": False,
            "error": "ZERODHA_API_KEY / ZERODHA_API_SECRET missing in environment"
        }), 500

    if not zerodha_access_token:
        return jsonify({
            "ok": False,
            "error": "Zerodha access token missing. Open /zerodha-login and authenticate first."
        }), 401

    try:
        # 1) Spot quote
        spot_data = _zerodha_json_get(
            "https://api.kite.trade/quote/ltp",
            params=[("i", "NSE:NIFTY 50")],
        )
        spot_row = spot_data.get("NSE:NIFTY 50") or {}
        spot = float(spot_row.get("last_price"))

        # 2) NFO instrument dump. Zerodha recommends refreshing this daily.
        inst_resp = requests.get(
            "https://api.kite.trade/instruments/NFO",
            headers=_zerodha_headers(),
            timeout=35,
        )
        if inst_resp.status_code >= 400:
            raise RuntimeError(f"NFO instrument dump failed: HTTP {inst_resp.status_code}")

        reader = csv.DictReader(io.StringIO(inst_resp.text))
        today_ist = datetime.now(ZERODHA_IST).date()
        nifty_options = []

        for row in reader:
            if (row.get("name") or "").strip().upper() != "NIFTY":
                continue
            itype = (row.get("instrument_type") or "").strip().upper()
            if itype not in {"CE", "PE"}:
                continue
            expiry_txt = (row.get("expiry") or "").strip()
            if not expiry_txt:
                continue
            try:
                expiry_date = datetime.strptime(expiry_txt, "%Y-%m-%d").date()
                strike = float(row.get("strike") or 0)
            except Exception:
                continue
            if expiry_date < today_ist:
                continue
            nifty_options.append({
                "tradingsymbol": (row.get("tradingsymbol") or "").strip(),
                "exchange": (row.get("exchange") or "NFO").strip() or "NFO",
                "instrument_token": int(float(row.get("instrument_token") or 0)),
                "expiry": expiry_date,
                "strike": strike,
                "instrument_type": itype,
                "lot_size": int(float(row.get("lot_size") or 0)),
            })

        if not nifty_options:
            raise RuntimeError("No active NIFTY CE/PE contracts found in NFO instrument dump")

        nearest_expiry = min(x["expiry"] for x in nifty_options)
        expiry_rows = [x for x in nifty_options if x["expiry"] == nearest_expiry]
        strikes = sorted({x["strike"] for x in expiry_rows})
        if len(strikes) < 5:
            raise RuntimeError("Nearest NIFTY expiry has fewer than 5 strikes in instrument dump")

        # Pick the available strike closest to spot, then 2 strikes on each side.
        atm_index = min(range(len(strikes)), key=lambda i: abs(strikes[i] - spot))
        if atm_index < 2 or atm_index > len(strikes) - 3:
            raise RuntimeError("Could not form ATM +/- 2 strike basket from available strikes")

        selected_strikes = strikes[atm_index - 2: atm_index + 3]
        atm_strike = strikes[atm_index]

        selected_rows = [
            x for x in expiry_rows
            if x["strike"] in selected_strikes and x["instrument_type"] in {"CE", "PE"}
        ]

        # Expect 5 CE + 5 PE.
        quote_keys = [f"{x['exchange']}:{x['tradingsymbol']}" for x in selected_rows]
        quote_data = _zerodha_json_get(
            "https://api.kite.trade/quote",
            params=[("i", key) for key in quote_keys],
        )

        contracts = []
        for x in sorted(selected_rows, key=lambda r: (r["strike"], r["instrument_type"])):
            key = f"{x['exchange']}:{x['tradingsymbol']}"
            q = quote_data.get(key) or {}
            contracts.append({
                "key": key,
                "strike": x["strike"],
                "type": x["instrument_type"],
                "expiry": x["expiry"].isoformat(),
                "lot_size": x["lot_size"],
                "instrument_token": x["instrument_token"],
                "last_price": q.get("last_price"),
                "oi_raw": q.get("oi"),
                "oi_lots": (
                    (q.get("oi") / x["lot_size"])
                    if isinstance(q.get("oi"), (int, float)) and x["lot_size"]
                    else None
                ),
                "oi_day_high": q.get("oi_day_high"),
                "oi_day_low": q.get("oi_day_low"),
            })

        ce_oi_raw = sum((c.get("oi_raw") or 0) for c in contracts if c["type"] == "CE")
        pe_oi_raw = sum((c.get("oi_raw") or 0) for c in contracts if c["type"] == "PE")

        return jsonify({
            "ok": True,
            "mode": "READ_ONLY_TEST",
            "nifty_spot": spot,
            "nearest_expiry": nearest_expiry.isoformat(),
            "atm_strike": atm_strike,
            "selected_strikes": selected_strikes,
            "contracts_returned": len(contracts),
            "ce_total_oi_raw": ce_oi_raw,
            "pe_total_oi_raw": pe_oi_raw,
            "contracts": contracts,
            "note": "This endpoint only verifies spot, expiry, fixed 5-strike selection and current OI. COI baseline/threshold logic is not enabled yet.",
        })

    except Exception as exc:
        return jsonify({
            "ok": False,
            "error": str(exc),
        }), 500



# ==========================================================
# ZERODHA NIFTY FIXED 09:15 COI MONITOR
# ==========================================================
ZERODHA_NIFTY_COI_THRESHOLD = int(os.getenv("ZERODHA_NIFTY_COI_THRESHOLD", "100000"))
ZERODHA_NIFTY_STATE_FILE = os.getenv(
    "ZERODHA_NIFTY_STATE_FILE",
    os.path.join(ZERODHA_STATE_DIR, "nifty_coi_state.json"),
)


def _zerodha_read_nifty_state():
    try:
        with open(ZERODHA_NIFTY_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _zerodha_write_nifty_state(state):
    try:
        _zerodha_atomic_json_write(ZERODHA_NIFTY_STATE_FILE, state)
    except Exception as exc:
        print(f"[ZERODHA NIFTY STATE WRITE ERROR] {exc}", flush=True)


def _zerodha_get_csv_rows(url, timeout=35):
    resp = requests.get(url, headers=_zerodha_headers(), timeout=timeout)
    if resp.status_code >= 400:
        raise RuntimeError(f"Kite instrument dump failed: HTTP {resp.status_code}")
    return list(csv.DictReader(io.StringIO(resp.text)))


def _zerodha_historical_candles(instrument_token, start_dt, end_dt, *, oi=False):
    url = f"https://api.kite.trade/instruments/historical/{int(instrument_token)}/minute"
    params = {
        "from": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "to": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "continuous": 0,
        "oi": 1 if oi else 0,
    }
    data = _zerodha_json_get(url, params=params, timeout=25)
    candles = data.get("candles") if isinstance(data, dict) else None
    return candles or []


def _zerodha_find_nifty_index_token():
    rows = _zerodha_get_csv_rows("https://api.kite.trade/instruments/NSE")
    candidates = []
    for row in rows:
        ts = (row.get("tradingsymbol") or "").strip().upper()
        name = (row.get("name") or "").strip().upper()
        segment = (row.get("segment") or "").strip().upper()
        if ts == "NIFTY 50" or name == "NIFTY 50":
            try:
                tok = int(float(row.get("instrument_token") or 0))
            except Exception:
                continue
            if tok:
                candidates.append((0 if segment == "INDICES" else 1, tok))
    if not candidates:
        raise RuntimeError("Could not find NIFTY 50 index token in NSE instrument dump")
    candidates.sort()
    return candidates[0][1]


def _zerodha_nifty_0915_open(session_date):
    token = _zerodha_find_nifty_index_token()
    start = datetime.combine(session_date, datetime.min.time()).replace(
        hour=9, minute=15, second=0, tzinfo=ZERODHA_IST
    )
    end = start + timedelta(minutes=2)
    candles = _zerodha_historical_candles(token, start, end, oi=False)
    if not candles:
        raise RuntimeError("No NIFTY 09:15 historical candle found for today")
    first = candles[0]
    if not isinstance(first, (list, tuple)) or len(first) < 5:
        raise RuntimeError("Unexpected NIFTY historical candle format")
    return float(first[1])


def _zerodha_build_nifty_fixed_basket(session_date, nifty_open):
    rows = _zerodha_get_csv_rows("https://api.kite.trade/instruments/NFO")
    options = []
    for row in rows:
        if (row.get("name") or "").strip().upper() != "NIFTY":
            continue
        itype = (row.get("instrument_type") or "").strip().upper()
        if itype not in {"CE", "PE"}:
            continue
        expiry_txt = (row.get("expiry") or "").strip()
        if not expiry_txt:
            continue
        try:
            expiry = datetime.strptime(expiry_txt, "%Y-%m-%d").date()
            strike = float(row.get("strike") or 0)
            token = int(float(row.get("instrument_token") or 0))
            lot = int(float(row.get("lot_size") or 0))
        except Exception:
            continue
        if expiry < session_date or not token or not strike:
            continue
        options.append({
            "tradingsymbol": (row.get("tradingsymbol") or "").strip(),
            "exchange": (row.get("exchange") or "NFO").strip() or "NFO",
            "instrument_token": token,
            "expiry": expiry,
            "strike": strike,
            "instrument_type": itype,
            "lot_size": lot,
        })

    if not options:
        raise RuntimeError("No active NIFTY options found")

    nearest_expiry = min(x["expiry"] for x in options)
    expiry_rows = [x for x in options if x["expiry"] == nearest_expiry]
    strikes = sorted({x["strike"] for x in expiry_rows})
    if len(strikes) < 5:
        raise RuntimeError("Nearest NIFTY expiry has fewer than 5 strikes")

    atm_i = min(range(len(strikes)), key=lambda i: abs(strikes[i] - nifty_open))
    if atm_i < 2 or atm_i > len(strikes) - 3:
        raise RuntimeError("Could not form fixed ATM +/-2 strike basket")

    selected_strikes = strikes[atm_i - 2:atm_i + 3]
    atm_strike = strikes[atm_i]
    selected_rows = [
        x for x in expiry_rows
        if x["strike"] in selected_strikes and x["instrument_type"] in {"CE", "PE"}
    ]
    if len(selected_rows) != 10:
        raise RuntimeError(
            f"Expected 10 fixed NIFTY contracts (5 CE + 5 PE), got {len(selected_rows)}"
        )
    return nearest_expiry, atm_strike, selected_strikes, selected_rows


def _zerodha_option_0915_oi(option_row, session_date):
    start = datetime.combine(session_date, datetime.min.time()).replace(
        hour=9, minute=15, second=0, tzinfo=ZERODHA_IST
    )
    end = start + timedelta(minutes=2)
    candles = _zerodha_historical_candles(
        option_row["instrument_token"], start, end, oi=True
    )
    if not candles:
        raise RuntimeError(
            f"No 09:15 OI candle for {option_row['tradingsymbol']}"
        )
    first = candles[0]
    if not isinstance(first, (list, tuple)) or len(first) < 7:
        raise RuntimeError(
            f"Historical OI missing for {option_row['tradingsymbol']}"
        )
    return int(first[6])


def _zerodha_nifty_snapshot():
    now_ist = datetime.now(ZERODHA_IST)
    session_date = now_ist.date()
    session_start = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    if now_ist < session_start:
        raise RuntimeError("NIFTY 09:15 session has not started yet")

    # Reuse the exact 09:15 basket/baseline from persistent disk when present.
    # On the first call of a new trading day, build it once and persist it.
    state = _zerodha_read_nifty_state()
    baseline_state = state.get("baseline") if isinstance(state, dict) else None
    use_saved = (
        isinstance(baseline_state, dict)
        and baseline_state.get("session_date") == session_date.isoformat()
        and isinstance(baseline_state.get("contracts"), list)
        and len(baseline_state.get("contracts")) == 10
    )

    if use_saved:
        nifty_open = float(baseline_state["nifty_open"])
        nearest_expiry = datetime.strptime(
            baseline_state["nearest_expiry"], "%Y-%m-%d"
        ).date()
        atm_strike = float(baseline_state["atm_strike"])
        selected_strikes = [float(x) for x in baseline_state["selected_strikes"]]
        selected_rows = []
        baseline = {}
        for saved in baseline_state["contracts"]:
            row = {
                "tradingsymbol": saved["tradingsymbol"],
                "exchange": saved.get("exchange") or "NFO",
                "instrument_token": int(saved["instrument_token"]),
                "expiry": datetime.strptime(saved["expiry"], "%Y-%m-%d").date(),
                "strike": float(saved["strike"]),
                "instrument_type": saved["instrument_type"],
                "lot_size": int(saved["lot_size"]),
            }
            selected_rows.append(row)
            key = f"{row['exchange']}:{row['tradingsymbol']}"
            baseline[key] = int(saved["baseline_oi_raw"])
    else:
        nifty_open = _zerodha_nifty_0915_open(session_date)
        nearest_expiry, atm_strike, selected_strikes, selected_rows = (
            _zerodha_build_nifty_fixed_basket(session_date, nifty_open)
        )

        baseline = {}
        persisted_contracts = []
        for row in selected_rows:
            key = f"{row['exchange']}:{row['tradingsymbol']}"
            base_oi = _zerodha_option_0915_oi(row, session_date)
            baseline[key] = base_oi
            persisted_contracts.append({
                "tradingsymbol": row["tradingsymbol"],
                "exchange": row["exchange"],
                "instrument_token": row["instrument_token"],
                "expiry": row["expiry"].isoformat(),
                "strike": row["strike"],
                "instrument_type": row["instrument_type"],
                "lot_size": row["lot_size"],
                "baseline_oi_raw": base_oi,
            })

        # Preserve crossing/alert state while adding the persistent daily baseline.
        state = state if isinstance(state, dict) else {}
        if state.get("session_date") != session_date.isoformat():
            state = {
                "session_date": session_date.isoformat(),
                "initialized": False,
                "ce_above": False,
                "pe_above": False,
            }
        state["baseline"] = {
            "session_date": session_date.isoformat(),
            "nifty_open": nifty_open,
            "nearest_expiry": nearest_expiry.isoformat(),
            "atm_strike": atm_strike,
            "selected_strikes": selected_strikes,
            "contracts": persisted_contracts,
            "saved_at_ist": now_ist.isoformat(),
        }
        _zerodha_write_nifty_state(state)

    quote_keys = [f"{x['exchange']}:{x['tradingsymbol']}" for x in selected_rows]
    quote_data = _zerodha_json_get(
        "https://api.kite.trade/quote",
        params=[("i", key) for key in quote_keys],
    )
    spot_data = _zerodha_json_get(
        "https://api.kite.trade/quote/ltp",
        params=[("i", "NSE:NIFTY 50")],
    )
    nifty_now = float((spot_data.get("NSE:NIFTY 50") or {}).get("last_price"))

    contracts = []
    ce_baseline = pe_baseline = 0
    ce_current = pe_current = 0
    ce_coi = pe_coi = 0
    ce_lots = pe_lots = 0.0

    for row in sorted(selected_rows, key=lambda r: (r["strike"], r["instrument_type"])):
        key = f"{row['exchange']}:{row['tradingsymbol']}"
        q = quote_data.get(key) or {}
        current_oi = int(q.get("oi") or 0)
        base_oi = int(baseline[key])
        coi = current_oi - base_oi
        lots = (coi / row["lot_size"]) if row["lot_size"] else 0.0

        if row["instrument_type"] == "CE":
            ce_baseline += base_oi
            ce_current += current_oi
            ce_coi += coi
            ce_lots += lots
        else:
            pe_baseline += base_oi
            pe_current += current_oi
            pe_coi += coi
            pe_lots += lots

        contracts.append({
            "key": key,
            "strike": row["strike"],
            "type": row["instrument_type"],
            "lot_size": row["lot_size"],
            "baseline_oi_raw": base_oi,
            "current_oi_raw": current_oi,
            "coi_raw": coi,
            "coi_lots": round(lots, 2),
        })

    return {
        "session_date": session_date.isoformat(),
        "nifty_open": nifty_open,
        "nifty_now": nifty_now,
        "nifty_move": nifty_now - nifty_open,
        "nearest_expiry": nearest_expiry.isoformat(),
        "atm_strike": atm_strike,
        "selected_strikes": selected_strikes,
        "ce_baseline_oi_raw": ce_baseline,
        "pe_baseline_oi_raw": pe_baseline,
        "ce_current_oi_raw": ce_current,
        "pe_current_oi_raw": pe_current,
        "ce_coi_raw": ce_coi,
        "pe_coi_raw": pe_coi,
        "ce_coi_lots": ce_lots,
        "pe_coi_lots": pe_lots,
        "gap_raw": ce_coi - pe_coi,
        "contracts": contracts,
    }


@app.get("/zerodha-nifty-coi-monitor")
def zerodha_nifty_coi_monitor():
    """
    Read-only NIFTY NET COI monitor.

    Fixed rules:
      - NIFTY 09:15 IST open determines ATM.
      - Fixed basket = ATM +/- 2 strikes, nearest expiry, 5 strikes total.
      - 09:15 option OI is the baseline for the whole session.
      - CE/PE COI = current total OI - 09:15 total OI.
      - NET GAP = CE COI - PE COI.
      - Alert when NET GAP crosses +100,000 (CE dominant) or -100,000 (PE dominant).
      - Same net side does not repeat-alert; opposite threshold is a flip.
    """
    if not zerodha_access_token:
        return jsonify({
            "ok": False,
            "error": "Zerodha access token missing. Open /zerodha-login and authenticate first."
        }), 401

    try:
        snap = _zerodha_nifty_snapshot()
        threshold = ZERODHA_NIFTY_COI_THRESHOLD
        state = _zerodha_read_nifty_state()

        if state.get("session_date") != snap["session_date"]:
            prior_baseline = state.get("baseline") if isinstance(state, dict) else None
            state = {
                "session_date": snap["session_date"],
                "initialized": False,
                "net_state": 0,
            }
            if isinstance(prior_baseline, dict) and prior_baseline.get("session_date") == snap["session_date"]:
                state["baseline"] = prior_baseline

        gap = snap["gap_raw"]  # CE COI - PE COI
        current_net_state = 1 if gap >= threshold else (-1 if gap <= -threshold else 0)

        # Fresh trading day: if the first successful observation is already
        # outside +/-1L, allow that first net-dominance alert.
        if not state.get("initialized"):
            net_cross = current_net_state != 0
            state["initialized"] = True
        else:
            previous_net_state = int(state.get("net_state", 0) or 0)
            net_cross = current_net_state != 0 and current_net_state != previous_net_state

        alert_sent = False
        alert_title = None
        winner = None

        if net_cross:
            if current_net_state == 1:
                winner = "CE"
                alert_title = "NIFTY COI CE WINS +1L NET"
            else:
                winner = "PE"
                alert_title = "NIFTY COI PE WINS +1L NET"

            strikes_text = " | ".join(
                f"{int(x) if float(x).is_integer() else x:g}"
                for x in snap["selected_strikes"]
            )
            move = snap["nifty_move"]

            # Percentage share is based on absolute CE/PE COI magnitude so the
            # display remains meaningful even if one side's COI is negative.
            ce_abs = abs(snap["ce_coi_raw"])
            pe_abs = abs(snap["pe_coi_raw"])
            total_abs = ce_abs + pe_abs
            if total_abs > 0:
                ce_pct = ce_abs / total_abs * 100.0
                pe_pct = pe_abs / total_abs * 100.0
            else:
                ce_pct = 0.0
                pe_pct = 0.0

            net_contracts = gap / 65.0

            message = (
                f"SOURCE ZERODHA | WINNER {winner} | "
                f"CE COI {snap['ce_coi_raw']:+,d} ({ce_pct:.2f}%) | "
                f"PE COI {snap['pe_coi_raw']:+,d} ({pe_pct:.2f}%) | "
                f"GAP {gap:+,d} | "
                f"NET CONTRACTS {net_contracts:+,.0f} | "
                f"NIFTY {snap['nifty_now']:,.2f} | "
                f"NIFTY MOVE {move:+,.2f} pts | "
                f"09:15 OPEN {snap['nifty_open']:,.2f} | "
                f"ATM {snap['atm_strike']:,.0f} | "
                f"STRIKES {strikes_text} | "
                f"EXPIRY {snap['nearest_expiry']}"
            )
            alert_sent = send_pushover(alert_title, message)

        state.update({
            "session_date": snap["session_date"],
            "net_state": current_net_state,
            "last_gap_raw": gap,
            "last_ce_coi_raw": snap["ce_coi_raw"],
            "last_pe_coi_raw": snap["pe_coi_raw"],
            "last_checked_at_ist": datetime.now(ZERODHA_IST).isoformat(),
            "last_alert_title": alert_title if alert_sent else state.get("last_alert_title"),
        })
        _zerodha_write_nifty_state(state)

        return jsonify({
            "ok": True,
            "mode": "READ_ONLY_NET_COI_MONITOR",
            "threshold_raw": threshold,
            "alert_triggered": bool(net_cross),
            "alert_sent": alert_sent,
            "alert_title": alert_title,
            "winner": winner,
            "net_gap_raw": gap,
            "state": {
                "net_state": current_net_state,
                "ce_net_dominant": current_net_state == 1,
                "pe_net_dominant": current_net_state == -1,
            },
            **snap,
        })

    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/zerodha-nifty-coi-state")
def zerodha_nifty_coi_state():
    """Safe debug state. No API secret/access token is returned."""
    return jsonify({
        "ok": True,
        "threshold_raw": ZERODHA_NIFTY_COI_THRESHOLD,
        "persistent_state_file": ZERODHA_NIFTY_STATE_FILE,
        "persistent_state_file_exists": os.path.exists(ZERODHA_NIFTY_STATE_FILE),
        "state": _zerodha_read_nifty_state(),
    })

@app.get("/")
def home():

    return jsonify({
        "status": "ok",
        "service":
            "NQ + ES + BTC + XAU + MarginPad BTC + XAU Liquidation Backend",

        "coinalyze_min_request_gap_seconds":
            COINALYZE_MIN_REQUEST_GAP_SECONDS,

        "nq_es_threshold": THRESHOLD,

        "btc_liquidation_threshold":
            BTC_LIQ_THRESHOLD,

        "btc_low_move_points":
            BTC_LOW_MOVE_POINTS,

        "btc_long_cumulative":
            round(
                btc_long_cumulative,
                2
            ),

        "btc_short_cumulative":
            round(
                btc_short_cumulative,
                2
            ),

        "btc_cycle_ref_price":
            btc_cycle_ref_price,

        "btc_last_processed_liq_ts":
            btc_last_processed_liq_ts,

        "btc_cached_contracts":
            (
                len(btc_symbol_cache)
                if btc_symbol_cache
                else 0
            ),

        "marginpad_btc_liquidation_threshold":
            MARGINPAD_BTC_LIQ_THRESHOLD,

        "marginpad_btc_long_cumulative":
            round(
                marginpad_btc_long_cumulative,
                2
            ),

        "marginpad_btc_short_cumulative":
            round(
                marginpad_btc_short_cumulative,
                2
            ),

        "marginpad_btc_cycle_ref_price":
            marginpad_btc_cycle_ref_price,

        "marginpad_btc_processed_through_ms":
            marginpad_btc_processed_through_ms,

        "marginpad_seen_event_cache":
            len(
                marginpad_seen_set
            ),

        "marginpad_xau_liquidation_threshold":
            MARGINPAD_XAU_LIQ_THRESHOLD,

        "marginpad_xau_long_cumulative":
            round(
                marginpad_xau_long_cumulative,
                2
            ),

        "marginpad_xau_short_cumulative":
            round(
                marginpad_xau_short_cumulative,
                2
            ),

        "marginpad_xau_cycle_ref_price":
            marginpad_xau_cycle_ref_price,

        "marginpad_xau_processed_through_ms":
            marginpad_xau_processed_through_ms,

        "marginpad_xau_seen_event_cache":
            len(
                marginpad_xau_seen_set
            ),

        "xau_liquidation_threshold":
            XAU_LIQ_THRESHOLD,

        "xau_long_cumulative":
            round(
                xau_long_cumulative,
                2
            ),

        "xau_short_cumulative":
            round(
                xau_short_cumulative,
                2
            ),

        "xau_cycle_ref_price":
            xau_cycle_ref_price,

        "xau_last_processed_liq_ts":
            xau_last_processed_liq_ts,

        "xau_cached_contracts":
            (
                len(xau_symbol_cache)
                if xau_symbol_cache
                else 0
            ),

        "nq_delta":
            latest_delta["NQ"],

        "es_delta":
            latest_delta["ES"],

        "nq_price":
            latest_price["NQ"],

        "es_price":
            latest_price["ES"],

        "jpn_price":
            latest_price["JPN"],

        "nq_es_state":
            state,

        "entry_side":
            entry_side,

        "entry_nq_price":
            entry_nq_price,

        "entry_jpn_price":
            entry_jpn_price
    })


# ==================================================
# TRADINGVIEW WEBHOOK
# ==================================================

@app.post("/webhook")
def webhook():

    global state
    global entry_side
    global entry_nq_price
    global entry_jpn_price

    secret = request.args.get(
        "secret",
        ""
    )

    if (
        not WEBHOOK_SECRET
        or
        secret != WEBHOOK_SECRET
    ):
        return jsonify({
            "ok": False,
            "error": "unauthorized"
        }), 401

    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    if (
        "title" in data
        and
        "message" in data
    ):

        tv_title = str(
            data.get(
                "title",
                "TradingView Alert"
            )
        )
        tv_message = str(
            data.get(
                "message",
                ""
            )
        )

        # Keep the original individual TradingView -> Pushover alert unchanged.
        ok = send_pushover(
            tv_title,
            tv_message
        )

        # Separately consume only NASDAQ TOP5/BOTTOM5 final alerts.
        # This is confirmation/Pushover only; MT5 is intentionally untouched.
        nasdaq_result = _nasdaq_combined_process(
            tv_title,
            tv_message
        )

        return jsonify({
            "ok": ok,
            "mode": "direct_pushover",
            "nasdaq_combined": nasdaq_result
        }), 200 if ok else 500

    symbol = str(
        data.get(
            "symbol",
            ""
        )
    ).upper()

    try:
        price = float(
            data.get("price")
        )

    except (
        TypeError,
        ValueError
    ):
        return jsonify({
            "ok": False,
            "error": "invalid price"
        }), 400

    if (
        "JPN" in symbol
        or
        "NIY" in symbol
    ):

        latest_price["JPN"] = price

        return jsonify({
            "ok": True,
            "instrument_updated": "JPN",
            "jpn_price": price,
            "signal": None
        })

    try:
        delta = float(
            data.get("delta")
        )

    except (
        TypeError,
        ValueError
    ):
        return jsonify({
            "ok": False,
            "error": "invalid delta"
        }), 400

    if "NQ" in symbol:
        instrument = "NQ"

    elif "ES" in symbol:
        instrument = "ES"

    else:
        return jsonify({
            "ok": False,
            "error":
                "symbol must be NQ, ES or JPN"
        }), 400

    latest_delta[instrument] = delta
    latest_price[instrument] = price

    if (
        latest_delta["NQ"] is None
        or
        latest_delta["ES"] is None
    ):
        return jsonify({
            "ok": True,
            "message":
                "waiting for NQ and ES",
            "nq_delta":
                latest_delta["NQ"],
            "es_delta":
                latest_delta["ES"]
        })

    nq_delta = latest_delta["NQ"]
    es_delta = latest_delta["ES"]

    combined = (
        nq_delta
        +
        es_delta
    )

    signal = None

    if (
        combined >= THRESHOLD
        and
        state != 1
    ):
        state = 1
        signal = "BUY"

    elif (
        combined <= -THRESHOLD
        and
        state != -1
    ):
        state = -1
        signal = "SELL"

    if signal:

        current_nq = (
            latest_price["NQ"]
        )

        current_jpn = (
            latest_price["JPN"]
        )

        if (
            entry_side is not None
            and
            entry_nq_price is not None
            and
            entry_jpn_price is not None
            and
            current_nq is not None
            and
            current_jpn is not None
        ):

            if entry_side == "BUY":

                nq_points = (
                    current_nq
                    -
                    entry_nq_price
                )

                jpn_points = (
                    current_jpn
                    -
                    entry_jpn_price
                )

            else:

                nq_points = (
                    entry_nq_price
                    -
                    current_nq
                )

                jpn_points = (
                    entry_jpn_price
                    -
                    current_jpn
                )

            nq_result = (
                "PROFIT"
                if nq_points > 0
                else
                "LOSS"
                if nq_points < 0
                else
                "FLAT"
            )

            jpn_result = (
                "PROFIT"
                if jpn_points > 0
                else
                "LOSS"
                if jpn_points < 0
                else
                "FLAT"
            )

            send_pushover(
                f"NQ + ES CLOSED {entry_side}",
                (
                    f"CLOSED {entry_side} | "
                    f"NQ {nq_result} "
                    f"{nq_points:+.2f} pts | "
                    f"JPN {jpn_result} "
                    f"{jpn_points:+.2f} pts | "
                    f"Exit NQ "
                    f"{current_nq:.2f} | "
                    f"JPN "
                    f"{current_jpn:.2f}"
                )
            )

        nq_price_text = (
            f"{current_nq:.2f}"
            if current_nq is not None
            else "NA"
        )

        jpn_price_text = (
            f"{current_jpn:.2f}"
            if current_jpn is not None
            else "NA"
        )

        send_pushover(
            f"NQ + ES {signal}",
            (
                f"{signal} | "
                f"Combined Delta "
                f"{combined:.0f} | "
                f"NQ Delta "
                f"{nq_delta:.0f} | "
                f"ES Delta "
                f"{es_delta:.0f} | "
                f"NQ "
                f"{nq_price_text} | "
                f"JPN "
                f"{jpn_price_text}"
            )
        )

        entry_side = signal
        entry_nq_price = current_nq
        entry_jpn_price = current_jpn

    return jsonify({
        "ok": True,
        "instrument_updated":
            instrument,
        "nq_delta":
            nq_delta,
        "es_delta":
            es_delta,
        "combined_delta":
            combined,
        "nq_price":
            latest_price["NQ"],
        "jpn_price":
            latest_price["JPN"],
        "signal":
            signal,
        "state":
            state,
        "entry_side":
            entry_side,
        "entry_nq_price":
            entry_nq_price,
        "entry_jpn_price":
            entry_jpn_price
    })


# ==================================================
# GET ALL PERPETUAL SYMBOLS
# ==================================================

def get_perpetual_symbols(asset):

    global btc_symbol_cache
    global xau_symbol_cache
    global xau_price_symbol_cache

    asset = asset.upper()

    if (
        asset == "BTC"
        and
        btc_symbol_cache
    ):
        return (
            btc_symbol_cache,
            None
        )

    if (
        asset == "XAU"
        and
        xau_symbol_cache
    ):
        return (
            xau_symbol_cache,
            None
        )

    markets, error = (
        get_future_markets()
    )

    if error:
        return None, error

    symbols = []
    price_symbol = None

    for market in markets:

        base_asset = str(
            market.get(
                "base_asset",
                ""
            )
        ).upper()

        if (
            base_asset == asset
            and
            market.get(
                "is_perpetual"
            ) is True
        ):

            symbol = (
                market.get(
                    "symbol"
                )
            )

            if symbol:
                symbols.append(
                    symbol
                )

            if (
                asset == "XAU"
                and
                price_symbol is None
                and
                market.get(
                    "has_ohlcv_data"
                ) is True
                and
                symbol
            ):
                price_symbol = symbol

    symbols = list(
        dict.fromkeys(
            symbols
        )
    )

    if not symbols:

        return None, {
            "stage":
                f"{asset.lower()}-symbols",
            "error":
                f"no {asset} perpetual symbols found"
        }

    if asset == "BTC":

        btc_symbol_cache = (
            symbols
        )

        print(
            "BTC ALL CONTRACTS LOADED:",
            len(
                btc_symbol_cache
            )
        )

    elif asset == "XAU":

        xau_symbol_cache = (
            symbols
        )

        print(
            "XAU ALL CONTRACTS LOADED:",
            len(
                xau_symbol_cache
            )
        )

        if price_symbol:

            xau_price_symbol_cache = (
                price_symbol
            )

        elif symbols:

            xau_price_symbol_cache = (
                symbols[0]
            )

    return symbols, None


# ==================================================
# GET CURRENT PRICE FROM COINALYZE
# ==================================================

def get_coinalyze_price(
    symbol,
    stage_name
):

    now = int(
        time.time()
    )

    response, error = (
        coinalyze_get(
            "https://api.coinalyze.net/v1/ohlcv-history",
            params={
                "symbols":
                    symbol,
                "interval":
                    "1min",
                "from":
                    now - 300,
                "to":
                    now
            },
            timeout=10,
            stage=stage_name
        )
    )

    if error:
        return None, error

    try:
        data = response.json()

    except ValueError:

        return None, {
            "stage": stage_name,
            "error": "invalid json"
        }

    if not data:

        return None, {
            "stage": stage_name,
            "error": "empty response"
        }

    history = (
        data[0].get(
            "history",
            []
        )
    )

    if not history:

        return None, {
            "stage": stage_name,
            "error": "no price history"
        }

    try:
        price = float(
            history[-1]["c"]
        )

    except (
        KeyError,
        TypeError,
        ValueError
    ):

        return None, {
            "stage": stage_name,
            "error": "invalid close"
        }

    return price, None


# ==================================================
# BTC PRICE
# ==================================================

def get_btc_price():

    return get_coinalyze_price(
        "BTCUSDT_PERP.A",
        "btc-price"
    )


# ==================================================
# XAU PRICE
# ==================================================

def get_xau_price():

    global xau_price_symbol_cache

    if not xau_price_symbol_cache:

        _, error = (
            get_perpetual_symbols(
                "XAU"
            )
        )

        if error:
            return None, error

    if not xau_price_symbol_cache:

        return None, {
            "stage": "xau-price",
            "error":
                "no XAU OHLCV symbol found"
        }

    return get_coinalyze_price(
        xau_price_symbol_cache,
        "xau-price"
    )


# ==================================================
# GENERIC FULL-CONTRACT FRESH LIQUIDATIONS
# ==================================================

def get_fresh_liquidations(
    asset,
    previous_ts,
    closed_minute_ts
):

    symbols, error = (
        get_perpetual_symbols(
            asset
        )
    )

    if error:
        return None, error

    fresh_long = 0.0
    fresh_short = 0.0
    fresh_rows = {}

    successful_batches = 0
    failed_batches = []

    liquidation_url = (
        "https://api.coinalyze.net/v1/"
        "liquidation-history"
    )

    query_from = max(
        previous_ts,
        closed_minute_ts - 3600
    )

    query_to = (
        closed_minute_ts
        + 59
    )

    for batch_index, i in enumerate(
        range(
            0,
            len(symbols),
            20
        ),
        start=1
    ):

        batch = (
            symbols[
                i:i + 20
            ]
        )

        print(
            f"{asset} BATCH "
            f"{batch_index} | "
            f"contracts={len(batch)}"
        )

        response, batch_error = (
            coinalyze_get(
                liquidation_url,
                params={
                    "symbols":
                        ",".join(
                            batch
                        ),
                    "interval":
                        "1min",
                    "from":
                        query_from,
                    "to":
                        query_to,
                    "convert_to_usd":
                        "true"
                },
                timeout=15,
                stage=(
                    f"{asset.lower()}-"
                    f"liquidation-batch-"
                    f"{batch_index}"
                )
            )
        )

        if batch_error:

            failed_batches.append({
                "batch_index":
                    batch_index,
                "symbols":
                    batch,
                **batch_error
            })

            continue

        successful_batches += 1

        try:
            data = (
                response.json()
            )

        except ValueError:

            failed_batches.append({
                "batch_index":
                    batch_index,
                "symbols":
                    batch,
                "error":
                    "invalid json"
            })

            continue

        for symbol_data in data:

            history = (
                symbol_data.get(
                    "history",
                    []
                )
            )

            for row in history:

                try:
                    row_ts = int(
                        row.get(
                            "t",
                            0
                        )
                    )

                except (
                    TypeError,
                    ValueError
                ):
                    continue

                if not (
                    previous_ts
                    <
                    row_ts
                    <=
                    closed_minute_ts
                ):
                    continue

                try:
                    long_value = float(
                        row.get(
                            "l",
                            0
                        )
                        or 0
                    )

                    short_value = float(
                        row.get(
                            "s",
                            0
                        )
                        or 0
                    )

                    fresh_long += (
                        long_value
                    )

                    fresh_short += (
                        short_value
                    )
                    rb = fresh_rows.setdefault(row_ts, {"long": 0.0, "short": 0.0})
                    rb["long"] += long_value
                    rb["short"] += short_value

                except (
                    TypeError,
                    ValueError
                ):
                    continue

    if failed_batches:

        return None, {
            "stage":
                f"{asset.lower()}-liquidation-history",

            "error":
                "one_or_more_batches_failed",

            "total_contracts":
                len(symbols),

            "expected_batches":
                (
                    len(symbols)
                    + 19
                ) // 20,

            "successful_batches":
                successful_batches,

            "failed_batches":
                failed_batches
        }

    return {
        "asset":
            asset,

        "perpetual_symbols":
            len(symbols),

        "successful_batch_count":
            successful_batches,

        "fresh_long_usd":
            round(
                fresh_long,
                2
            ),

        "fresh_short_usd":
            round(
                fresh_short,
                2
            ),

        "rolling_rows": [
            {"ts": ts, "long": round(v["long"], 2), "short": round(v["short"], 2)}
            for ts, v in sorted(fresh_rows.items())
        ],

        "fresh_net_short_minus_long":
            round(
                fresh_short
                -
                fresh_long,
                2
            )
    }, None


# ==================================================
# BTC READ-ONLY STATE - COINALYZE
# ==================================================

@app.get("/test-btc-aggregate")
def test_btc_aggregate():

    return jsonify({
        "ok": True,
        "read_only": True,

        "btc_long_cumulative":
            round(
                btc_long_cumulative,
                2
            ),

        "btc_short_cumulative":
            round(
                btc_short_cumulative,
                2
            ),

        "threshold_usd":
            BTC_LIQ_THRESHOLD,

        "btc_cycle_ref_price":
            btc_cycle_ref_price,

        "last_processed_liq_ts":
            btc_last_processed_liq_ts,

        "cached_btc_symbols":
            (
                len(btc_symbol_cache)
                if btc_symbol_cache
                else 0
            )
    })


# ==================================================
# BTC READ-ONLY STATE - MARGINPAD
# ==================================================

@app.get("/test-marginpad-btc-aggregate")
def test_marginpad_btc_aggregate():

    return jsonify({
        "ok": True,
        "read_only": True,

        "source": "MarginPad",

        "long_cumulative_usd":
            round(
                marginpad_btc_long_cumulative,
                2
            ),

        "short_cumulative_usd":
            round(
                marginpad_btc_short_cumulative,
                2
            ),

        "threshold_usd":
            MARGINPAD_BTC_LIQ_THRESHOLD,

        "cycle_reference_price":
            marginpad_btc_cycle_ref_price,

        "processed_through_ms":
            marginpad_btc_processed_through_ms,

        "seen_event_cache":
            len(
                marginpad_seen_set
            ),

        "by_exchange":
            marginpad_btc_by_exchange
    })


# ==================================================
# XAU READ-ONLY STATE - MARGINPAD
# ==================================================

@app.get("/test-marginpad-xau-aggregate")
def test_marginpad_xau_aggregate():

    return jsonify({
        "ok": True,
        "read_only": True,
        "source": "MarginPad",
        "asset": "XAU",
        "long_cumulative_usd": round(
            marginpad_xau_long_cumulative,
            2
        ),
        "short_cumulative_usd": round(
            marginpad_xau_short_cumulative,
            2
        ),
        "threshold_usd": MARGINPAD_XAU_LIQ_THRESHOLD,
        "cycle_reference_price": marginpad_xau_cycle_ref_price,
        "processed_through_ms": marginpad_xau_processed_through_ms,
        "seen_event_cache": len(
            marginpad_xau_seen_set
        )
    })


# ==================================================
# XAU READ-ONLY STATE
# ==================================================

@app.get("/test-xau-aggregate")
def test_xau_aggregate():

    return jsonify({
        "ok": True,
        "read_only": True,

        "xau_long_cumulative":
            round(
                xau_long_cumulative,
                2
            ),

        "xau_short_cumulative":
            round(
                xau_short_cumulative,
                2
            ),

        "threshold_usd":
            XAU_LIQ_THRESHOLD,

        "xau_cycle_ref_price":
            xau_cycle_ref_price,

        "last_processed_liq_ts":
            xau_last_processed_liq_ts,

        "cached_xau_symbols":
            (
                len(xau_symbol_cache)
                if xau_symbol_cache
                else 0
            ),

        "xau_price_symbol":
            xau_price_symbol_cache
    })


# ==================================================
# BTC PROCESSOR - COINALYZE
# ==================================================

def _btc_reference_text(reference_source, closed_minute_ts):
    """Return the other provider's CURRENT accumulating BTC cycle.

    This intentionally does NOT use the other provider's previous alert
    snapshot. The goal is same-moment comparison: when MarginPad alerts,
    show Coinalyze's current cycle; when Coinalyze alerts, show MarginPad's
    current cycle.
    """

    if reference_source == "MARGINPAD":
        ref_long = marginpad_btc_long_cumulative
        ref_short = marginpad_btc_short_cumulative
        initialized = marginpad_btc_processed_through_ms is not None
    else:
        ref_long = btc_long_cumulative
        ref_short = btc_short_cumulative
        initialized = btc_last_processed_liq_ts is not None

    if not initialized:
        return f"REF {reference_source} CURRENT CYCLE | NOT INITIALIZED"

    ref_total = ref_long + ref_short
    ref_gap = abs(ref_long - ref_short)
    ref_long_pct = (ref_long / ref_total * 100) if ref_total > 0 else 0
    ref_short_pct = (ref_short / ref_total * 100) if ref_total > 0 else 0

    return (
        f"REF {reference_source} CURRENT CYCLE | "
        f"LONG ${ref_long:,.0f} ({_usd_m(ref_long)}) ({ref_long_pct:.2f}%) | "
        f"SHORT ${ref_short:,.0f} ({_usd_m(ref_short)}) ({ref_short_pct:.2f}%) | "
        f"GAP ${ref_gap:,.0f} ({_usd_m(ref_gap)})"
    )


def process_btc(
    closed_minute_ts
):

    global btc_long_cumulative
    global btc_short_cumulative
    global btc_cycle_ref_price
    global btc_last_processed_liq_ts
    global btc_last_alert_snapshot

    btc_price, price_error = (
        get_btc_price()
    )

    if price_error:

        return {
            "ok": False,
            "asset": "BTC",
            "source": "Coinalyze",
            "alert_sent": False,
            "error": price_error
        }

    if btc_last_processed_liq_ts is None:

        btc_last_processed_liq_ts = (
            closed_minute_ts
        )

        btc_cycle_ref_price = (
            btc_price
        )

        btc_long_cumulative = 0.0
        btc_short_cumulative = 0.0

        return {
            "ok": True,
            "asset": "BTC",
            "source": "Coinalyze",
            "initialized": True,

            "btc_price":
                round(
                    btc_price,
                    2
                ),

            "long_cumulative_usd":
                0,

            "short_cumulative_usd":
                0,

            "cycle_reference_price":
                btc_cycle_ref_price,

            "last_processed_liq_ts":
                btc_last_processed_liq_ts
        }

    if (
        closed_minute_ts
        <=
        btc_last_processed_liq_ts
    ):

        return {
            "ok": True,
            "asset": "BTC",
            "source": "Coinalyze",
            "new_closed_minute": False,

            "btc_price":
                round(
                    btc_price,
                    2
                ),

            "long_cumulative_usd":
                round(
                    btc_long_cumulative,
                    2
                ),

            "short_cumulative_usd":
                round(
                    btc_short_cumulative,
                    2
                ),

            "cycle_reference_price":
                btc_cycle_ref_price,

            "last_processed_liq_ts":
                btc_last_processed_liq_ts
        }

    fresh, error = (
        get_fresh_liquidations(
            "BTC",
            btc_last_processed_liq_ts,
            closed_minute_ts
        )
    )

    if error:

        return {
            "ok": False,
            "asset": "BTC",
            "source": "Coinalyze",
            "alert_sent": False,

            "long_cumulative_usd":
                round(
                    btc_long_cumulative,
                    2
                ),

            "short_cumulative_usd":
                round(
                    btc_short_cumulative,
                    2
                ),

            "last_processed_liq_ts":
                btc_last_processed_liq_ts,

            "error":
                error
        }

    fresh_long = (
        fresh["fresh_long_usd"]
    )

    fresh_short = (
        fresh["fresh_short_usd"]
    )

    for rr in fresh.get("rolling_rows", []):
        _btc_rolling_add_coinalyze_row(
            rr.get("long", 0.0), rr.get("short", 0.0), rr.get("ts"), price=btc_price
        )

    btc_long_cumulative += (
        fresh_long
    )

    btc_short_cumulative += (
        fresh_short
    )

    btc_last_processed_liq_ts = (
        closed_minute_ts
    )

    cycle_long = (
        btc_long_cumulative
    )

    cycle_short = (
        btc_short_cumulative
    )

    cycle_gap = abs(
        cycle_long
        -
        cycle_short
    )

    signed_gap = cycle_long - cycle_short
    long_hit = signed_gap >= BTC_GAP_THRESHOLD and btc_coinalyze_gap_state != "LONG"
    short_hit = signed_gap <= -BTC_GAP_THRESHOLD and btc_coinalyze_gap_state != "SHORT"

    alert_sent = False
    cycle_winner = None

    btc_price_move = None
    low_move = False

    if (
        btc_cycle_ref_price
        is not None
    ):

        btc_price_move = abs(
            btc_price
            -
            btc_cycle_ref_price
        )

        low_move = (
            btc_price_move
            <
            BTC_LOW_MOVE_POINTS
        )

    if (
        long_hit
        or
        short_hit
    ):

        if long_hit:
            cycle_winner = "LONG"
            btc_coinalyze_gap_state = "LONG"
            alert_title = "BTC COINALYZE LONG WINS | +5M GAP"
        else:
            cycle_winner = "SHORT"
            btc_coinalyze_gap_state = "SHORT"
            alert_title = "BTC COINALYZE SHORT WINS | +5M GAP"

        move_text = (
            f"{btc_price_move:,.0f} pts"
            if btc_price_move
            is not None
            else
            "NA"
        )

        low_move_text = (
            " | LOW-MOVE YES"
            if low_move
            else
            ""
        )

        cycle_total = (
            cycle_long
            +
            cycle_short
        )

        long_pct = (
            cycle_long
            /
            cycle_total
            *
            100
        ) if cycle_total > 0 else 0

        short_pct = (
            cycle_short
            /
            cycle_total
            *
            100
        ) if cycle_total > 0 else 0

        # Save this cycle before reset so a MarginPad alert arriving a few
        # seconds/minutes later can still reference the completed Coinalyze cycle.
        btc_last_alert_snapshot = {
            "ts": closed_minute_ts,
            "long": cycle_long,
            "short": cycle_short,
            "winner": cycle_winner,
        }

        reference_text = _btc_reference_text(
            "MARGINPAD",
            closed_minute_ts
        )

        alert_sent = send_pushover(
            alert_title,
            (
                f"SOURCE COINALYZE | "
                f"WINNER "
                f"{cycle_winner} | "
                f"LONG "
                f"${cycle_long:,.0f} ({_usd_m(cycle_long)}) "
                f"({long_pct:.2f}%) | "
                f"SHORT "
                f"${cycle_short:,.0f} ({_usd_m(cycle_short)}) "
                f"({short_pct:.2f}%) | "
                f"GAP "
                f"${cycle_gap:,.0f} ({_usd_m(cycle_gap)}) | "
                f"BTC "
                f"{btc_price:,.0f} | "
                f"BTC MOVE "
                f"{move_text}"
                f"{low_move_text}"
                f"\n{reference_text}"
            )
        )

        btc_long_cumulative = 0.0
        btc_short_cumulative = 0.0

        btc_cycle_ref_price = (
            btc_price
        )

    return {
        "ok": True,
        "asset": "BTC",
        "source": "Coinalyze",
        "initialized": False,

        "perpetual_symbols":
            fresh[
                "perpetual_symbols"
            ],

        "successful_batch_count":
            fresh[
                "successful_batch_count"
            ],

        "price":
            round(
                btc_price,
                2
            ),

        "fresh_long_usd":
            fresh_long,

        "fresh_short_usd":
            fresh_short,

        "cycle_long_before_reset":
            round(
                cycle_long,
                2
            ),

        "cycle_short_before_reset":
            round(
                cycle_short,
                2
            ),

        "cycle_gap_usd":
            round(
                cycle_gap,
                2
            ),

        "long_cumulative_usd":
            round(
                btc_long_cumulative,
                2
            ),

        "short_cumulative_usd":
            round(
                btc_short_cumulative,
                2
            ),

        "threshold_usd":
            BTC_LIQ_THRESHOLD,

        "cycle_winner":
            cycle_winner,

        "alert_sent":
            alert_sent,

        "price_move_points":
            (
                round(
                    btc_price_move,
                    2
                )
                if
                btc_price_move
                is not None
                else
                None
            ),

        "low_move":
            low_move,

        "cycle_reference_price":
            btc_cycle_ref_price,

        "last_processed_liq_ts":
            btc_last_processed_liq_ts
    }


# ==================================================
# BTC PROCESSOR - MARGINPAD
# ==================================================

# MarginPad's nine liquidation venues. Keep all nine visible in every
# standalone MarginPad BTC alert, even when a venue contributes $0.
MARGINPAD_BTC_DISPLAY_EXCHANGES = (
    "binance",
    "bybit",
    "okx",
    "hyperliquid",
    "gate",
    "htx",
    "dydx",
    "bitmex",
    "bitfinex",
)


def _btc_exchange_key(name):
    key = str(name or "unknown").strip().lower() or "unknown"
    aliases = {
        # MarginPad reads Binance USD-M and Coin-M feeds but they belong to
        # the same Binance venue in our 9-exchange alert.
        "binance_coinm": "binance",
        "binance-coinm": "binance",
        "binance coin-m": "binance",
        "binance_coin_m": "binance",
        "binance-futures": "binance",
        "gateio": "gate",
        "gate.io": "gate",
        "hyper_liquid": "hyperliquid",
        "dy/dx": "dydx",
    }
    return aliases.get(key, key)


def _btc_exchange_label(name):
    labels = {
        "binance": "Binance",
        "okx": "OKX",
        "bybit": "Bybit",
        "bitget": "Bitget",
        "aster": "Aster",
        "coinex": "CoinEx",
        "lighter": "Lighter",
        "bitfinex": "Bitfinex",
        "hyperliquid": "Hyperliquid",
        "gate": "Gate",
        "htx": "HTX",
        "dydx": "dYdX",
        "bitmex": "BitMEX",
    }
    key = _btc_exchange_key(name)
    return labels.get(key, key.title())


def _btc_standalone_exchange_lines(
    by_exchange, winner, include_all=False, required_exchanges=None
):
    display_side = "long" if winner == "LONG" else "short" if winner == "SHORT" else None

    # Normalize aliases first so e.g. Binance USD-M / Coin-M contributions
    # appear under one Binance line instead of creating duplicate venue rows.
    normalized = {}
    for ex_name, totals in (by_exchange or {}).items():
        if not isinstance(totals, dict):
            continue
        try:
            ex_long = max(0.0, float(totals.get("long", 0.0) or 0.0))
            ex_short = max(0.0, float(totals.get("short", 0.0) or 0.0))
        except (TypeError, ValueError):
            continue

        key = _btc_exchange_key(ex_name)
        bucket = normalized.setdefault(key, {"long": 0.0, "short": 0.0})
        bucket["long"] += ex_long
        bucket["short"] += ex_short

    # Seed the fixed MarginPad venue list so a quiet exchange still prints $0.
    for ex_name in (required_exchanges or ()):
        key = _btc_exchange_key(ex_name)
        normalized.setdefault(key, {"long": 0.0, "short": 0.0})

    ranked = []
    for ex_name, totals in normalized.items():
        ex_long = totals["long"]
        ex_short = totals["short"]

        if not include_all and ex_long <= 0 and ex_short <= 0:
            continue

        rank_amount = (
            ex_long if display_side == "long"
            else ex_short if display_side == "short"
            else max(ex_long, ex_short)
        )
        ranked.append((rank_amount, ex_name, ex_long, ex_short))

    ranked.sort(key=lambda row: row[0], reverse=True)
    lines = []
    for _, ex_name, ex_long, ex_short in ranked:
        label = _btc_exchange_label(ex_name)
        if display_side == "long":
            lines.append(f"{label}: ${ex_long:,.0f}")
        elif display_side == "short":
            lines.append(f"{label}: ${ex_short:,.0f}")
        else:
            lines.append(f"{label}: L ${ex_long:,.0f} | S ${ex_short:,.0f}")
    return lines


def _usd_m(value):
    """Display USD compactly: <1K normal, 1K-<1M in K, >=1M in M."""
    try:
        amount = abs(float(value))
        if amount >= 1_000_000:
            return f"{amount / 1_000_000:.2f}M"
        if amount >= 1_000:
            return f"{amount / 1_000:.2f}K"
        return f"{amount:,.0f}"
    except (TypeError, ValueError):
        return "0"


def _all_crypto_event_fingerprint(event):
    return "|".join([
        str(event.get("ts", "")),
        str(event.get("exchange", "")),
        str(event.get("symbol", "")),
        str(event.get("side", "")),
        str(event.get("price", "")),
        str(event.get("qty", "")),
        str(event.get("notional", "")),
    ])


def _all_crypto_remember(fingerprint):
    if fingerprint in all_crypto_seen_set:
        return False
    if len(all_crypto_seen_queue) >= ALL_CRYPTO_SEEN_MAX:
        old = all_crypto_seen_queue.popleft()
        all_crypto_seen_set.discard(old)
    all_crypto_seen_queue.append(fingerprint)
    all_crypto_seen_set.add(fingerprint)
    return True


def _all_crypto_refresh_symbol_universe(force=False):
    """Refresh crypto symbols from MarginPad markets; safe fallback keeps core majors."""
    global all_crypto_crypto_symbols, all_crypto_crypto_symbols_refreshed_ts
    now = time.time()
    if not force and all_crypto_crypto_symbols and now - all_crypto_crypto_symbols_refreshed_ts < 3600:
        return True
    fallback = {"BTC","ETH","SOL","XRP","DOGE","BNB","ADA","LINK","AVAX","LTC"}
    try:
        response = requests.get(
            f"{MARGINPAD_BASE_URL}/api/bot/v1/markets",
            params={"class": "crypto"},
            timeout=12,
        )
        if response.ok:
            payload = response.json()
            rows = payload.get("data", payload) if isinstance(payload, dict) else payload
            symbols = set()
            if isinstance(rows, dict):
                rows = rows.get("markets") or rows.get("items") or rows.get("symbols") or []
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict):
                        sym = str(row.get("symbol", "")).upper().strip()
                    else:
                        sym = str(row).upper().strip()
                    if sym:
                        # Normalize common pair forms to base symbol used by liquidation feed.
                        for suffix in ("USDT", "USD", "PERP"):
                            if sym.endswith(suffix) and len(sym) > len(suffix):
                                sym = sym[:-len(suffix)].rstrip("-_/:")
                                break
                        if sym:
                            symbols.add(sym)
            if symbols:
                all_crypto_crypto_symbols = symbols
                all_crypto_crypto_symbols_refreshed_ts = now
                print(f"[ALL CRYPTO SYMBOLS] refreshed={len(symbols)}", flush=True)
                return True
    except Exception as exc:
        print(f"[ALL CRYPTO SYMBOLS ERROR] {exc}", flush=True)
    if not all_crypto_crypto_symbols:
        all_crypto_crypto_symbols = fallback
    all_crypto_crypto_symbols_refreshed_ts = now
    return False


def _all_crypto_rolling_trim(now_ts):
    cutoff = float(now_ts) - ALL_CRYPTO_ROLLING_WINDOW_SECONDS
    while all_crypto_rolling_events and float(all_crypto_rolling_events[0][0]) <= cutoff:
        all_crypto_rolling_events.popleft()


def _all_crypto_rolling_totals():
    long_total = sum(float(r[2]) for r in all_crypto_rolling_events if r[1] == "long")
    short_total = sum(float(r[2]) for r in all_crypto_rolling_events if r[1] == "short")
    return long_total, short_total


def _format_accumulation_duration(start_ts, end_ts=None):
    if start_ts is None:
        return "NA"
    try:
        start_ts = float(start_ts)
        end_ts = float(end_ts) if end_ts is not None else time.time()
        total_seconds = max(0, int(end_ts - start_ts))
    except (TypeError, ValueError):
        return "NA"
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days > 0:
        return f"{days}D {hours}H {minutes}M {seconds}S"
    return f"{hours}H {minutes}M {seconds}S"


def _all_crypto_send_rolling_if_flip(now_ts=None):
    global all_crypto_rolling_state
    now_ts = float(now_ts) if now_ts is not None else time.time()
    _all_crypto_rolling_trim(now_ts)
    long_total, short_total = _all_crypto_rolling_totals()
    signed_gap = long_total - short_total
    state = all_crypto_rolling_state
    new_state = state
    if signed_gap >= ALL_CRYPTO_ROLLING_GAP_THRESHOLD and state != "LONG":
        new_state = "LONG"
    elif signed_gap <= -ALL_CRYPTO_ROLLING_GAP_THRESHOLD and state != "SHORT":
        new_state = "SHORT"
    if new_state == state:
        return False
    all_crypto_rolling_state = new_state
    gap = abs(signed_gap)
    title = f"ALL CRYPTO ROLLING 60M {new_state} | 5M GAP"
    message = (
        "WINDOW: EXACT TRAILING 60 MINUTES | NO RESET\n"
        f"LONG: ${long_total:,.0f} ({_usd_m(long_total)})\n"
        f"SHORT: ${short_total:,.0f} ({_usd_m(short_total)})\n"
        f"GAP: ${gap:,.0f} ({_usd_m(gap)})\n"
        f"STRONGER: {new_state}\n"
        f"STATE: {state or 'NONE'} -> {new_state}"
    )
    sent = send_pushover(title, message)
    print(f"[ALL CRYPTO ROLLING ALERT] {title} sent={sent}", flush=True)
    return bool(sent)


def _all_crypto_send_reset_if_flip():
    global all_crypto_long_cumulative, all_crypto_short_cumulative
    global all_crypto_gap_state, all_crypto_by_symbol
    global all_crypto_cycle_start_ts
    signed_gap = all_crypto_long_cumulative - all_crypto_short_cumulative
    state = all_crypto_gap_state
    new_state = state
    if signed_gap >= ALL_CRYPTO_GAP_THRESHOLD and state != "LONG":
        new_state = "LONG"
    elif signed_gap <= -ALL_CRYPTO_GAP_THRESHOLD and state != "SHORT":
        new_state = "SHORT"
    if new_state == state:
        return False
    signal_ts = time.time()
    accumulation_time = _format_accumulation_duration(all_crypto_cycle_start_ts, signal_ts)
    gap = abs(signed_gap)
    ranked = []
    display_side = "long" if new_state == "LONG" else "short"
    for symbol, totals in all_crypto_by_symbol.items():
        amount = float(totals.get(display_side, 0.0) or 0.0)
        if amount > 0:
            ranked.append((amount, symbol))
    ranked.sort(reverse=True)
    top_lines = [f"{sym}: ${amount:,.0f} ({_usd_m(amount)})" for amount, sym in ranked[:8]]
    title = f"ALL CRYPTO {new_state} WINS | 5M GAP"
    message = (
        "MARKET-WIDE CRYPTO | RESET AFTER VALID FLIP\n"
        f"LONG: ${all_crypto_long_cumulative:,.0f} ({_usd_m(all_crypto_long_cumulative)})\n"
        f"SHORT: ${all_crypto_short_cumulative:,.0f} ({_usd_m(all_crypto_short_cumulative)})\n"
        f"GAP: ${gap:,.0f} ({_usd_m(gap)})\n"
        f"ACCUMULATION TIME: {accumulation_time}\n"
        f"STRONGER: {new_state}\n"
        f"STATE: {state or 'NONE'} -> {new_state}"
    )
    if top_lines:
        message += "\n\nTOP CONTRIBUTORS:\n" + "\n".join(top_lines)
    sent = send_pushover(title, message)
    print(f"[ALL CRYPTO GAP ALERT] {title} accumulation={accumulation_time} sent={sent}", flush=True)
    all_crypto_gap_state = new_state
    # RESET totals only after a valid reverse-only signal. Direction state persists.
    all_crypto_long_cumulative = 0.0
    all_crypto_short_cumulative = 0.0
    all_crypto_by_symbol = {}
    all_crypto_cycle_start_ts = None
    return bool(sent)


def process_marginpad_all_crypto_feed(seed_only=False):
    """Poll MarginPad /api/v1/feed and update the independent ALL CRYPTO test states."""
    global all_crypto_long_cumulative, all_crypto_short_cumulative
    global all_crypto_rolling_events
    global all_crypto_cycle_start_ts
    global all_crypto_last_poll_ts, all_crypto_last_error, all_crypto_first_poll_seeded
    _all_crypto_refresh_symbol_universe()
    try:
        payload, error = marginpad_get(
            "/api/v1/feed",
            timeout=12,
            stage="marginpad-all-crypto-feed",
        )
        if error:
            all_crypto_last_error = str(error)
            return {"ok": False, "error": error}
        events = extract_marginpad_events(payload)
        if not events and isinstance(payload, dict) and isinstance(payload.get("events"), list):
            events = payload.get("events")
        if not isinstance(events, list):
            return {"ok": False, "error": "events payload is not a list"}

        normalized = []
        for event in events:
            if not isinstance(event, dict):
                continue
            ts_ms = normalize_marginpad_ts_ms(event.get("ts"))
            if ts_ms is None:
                continue
            normalized.append((ts_ms, event))
        normalized.sort(key=lambda x: x[0])

        accepted = 0
        seeded = 0
        with _all_crypto_lock:
            for ts_ms, event in normalized:
                fp = _all_crypto_event_fingerprint(event)
                if fp in all_crypto_seen_set:
                    continue
                symbol = str(event.get("symbol", "")).upper().strip()
                # Hard reject known non-crypto instruments; then require crypto universe membership.
                if symbol in {"XAU", "XAG", "GOLD", "SILVER", "NQ", "ES", "SPX", "SP500"}:
                    _all_crypto_remember(fp)
                    continue
                # /api/v1/feed is MarginPad's market-wide crypto liquidation feed.
                # Do not gate on the optional /markets cache: that could silently
                # drop newly tracked coins if the symbol-universe refresh fails.
                try:
                    notional = abs(float(event.get("notional", 0.0) or 0.0))
                except (TypeError, ValueError):
                    _all_crypto_remember(fp)
                    continue
                side_raw = str(event.get("side", "")).lower().strip()
                if notional <= 0 or side_raw not in ("long_liquidated", "short_liquidated"):
                    _all_crypto_remember(fp)
                    continue
                _all_crypto_remember(fp)
                if seed_only or not all_crypto_first_poll_seeded:
                    seeded += 1
                    continue
                side = "long" if side_raw == "long_liquidated" else "short"
                ts_sec = ts_ms / 1000.0
                if all_crypto_cycle_start_ts is None:
                    all_crypto_cycle_start_ts = ts_sec
                if side == "long":
                    all_crypto_long_cumulative += notional
                else:
                    all_crypto_short_cumulative += notional
                bucket = all_crypto_by_symbol.setdefault(symbol or "UNKNOWN", {"long": 0.0, "short": 0.0})
                bucket[side] += notional
                all_crypto_rolling_events.append((ts_sec, side, notional, symbol))
                accepted += 1
                print(
                    f"[ALL CRYPTO ACCEPTED] ts_ms={ts_ms} symbol={symbol or '-'} "
                    f"exchange={event.get('exchange','')} side={side_raw} notional=${notional:,.2f}",
                    flush=True,
                )

            if accepted and len(all_crypto_rolling_events) > 1:
                all_crypto_rolling_events = deque(sorted(all_crypto_rolling_events, key=lambda r: r[0]))
            if not all_crypto_first_poll_seeded:
                all_crypto_first_poll_seeded = True
                print(f"[ALL CRYPTO SEEDED] existing_events={seeded} | live counting starts next poll", flush=True)
            now_ts = time.time()
            _all_crypto_rolling_trim(now_ts)
            reset_alert = _all_crypto_send_reset_if_flip()
            rolling_alert = _all_crypto_send_rolling_if_flip(now_ts)
            all_crypto_last_poll_ts = now_ts
            all_crypto_last_error = None

        _save_runtime_state()
        return {
            "ok": True,
            "events_returned": len(events),
            "events_accepted": accepted,
            "seeded": seeded,
            "long": round(all_crypto_long_cumulative, 2),
            "short": round(all_crypto_short_cumulative, 2),
            "gap": round(all_crypto_long_cumulative - all_crypto_short_cumulative, 2),
            "gap_state": all_crypto_gap_state,
            "rolling_state": all_crypto_rolling_state,
            "reset_alert_sent": reset_alert,
            "rolling_alert_sent": rolling_alert,
        }
    except Exception as exc:
        all_crypto_last_error = str(exc)
        print(f"[ALL CRYPTO POLL ERROR] {exc}", flush=True)
        return {"ok": False, "error": str(exc)}


def _all_crypto_poller_loop():
    print(f"[ALL CRYPTO POLLER] started interval={ALL_CRYPTO_POLL_SECONDS:.0f}s", flush=True)
    while True:
        started = time.monotonic()
        process_marginpad_all_crypto_feed()
        elapsed = time.monotonic() - started
        time.sleep(max(1.0, ALL_CRYPTO_POLL_SECONDS - elapsed))


def _start_all_crypto_poller_once():
    global _all_crypto_poller_started
    enabled = str(os.environ.get("ALL_CRYPTO_POLLER_ENABLED", "1")).strip().lower() not in ("0", "false", "no", "off")
    if not enabled or _all_crypto_poller_started:
        return False
    _all_crypto_poller_started = True
    thread = threading.Thread(target=_all_crypto_poller_loop, name="all-crypto-marginpad-poller", daemon=True)
    thread.start()
    return True


def _xau_rolling_trim(events, now_ts):
    cutoff = float(now_ts) - XAU_ROLLING_WINDOW_SECONDS
    while events and float(events[0][0]) <= cutoff:
        events.popleft()


def _xau_rolling_totals(events):
    long_total = sum(float(r[2]) for r in events if r[1] == "long")
    short_total = sum(float(r[2]) for r in events if r[1] == "short")
    return long_total, short_total


def _xau_rolling_evaluate(source, price=None, now_ts=None):
    global xau_coinalyze_rolling_state, xau_observer_rolling_state
    source = str(source or "").lower().strip()
    if source not in ("coinalyze", "observer"):
        return None
    now_ts = float(now_ts) if now_ts is not None else time.time()
    with _xau_rolling_lock:
        events = xau_coinalyze_rolling_events if source == "coinalyze" else xau_observer_rolling_events
        state = xau_coinalyze_rolling_state if source == "coinalyze" else xau_observer_rolling_state
        _xau_rolling_trim(events, now_ts)
        long_total, short_total = _xau_rolling_totals(events)
        signed_gap = long_total - short_total
        new_state = state
        if signed_gap >= XAU_ROLLING_GAP_THRESHOLD and state != "LONG":
            new_state = "LONG"
        elif signed_gap <= -XAU_ROLLING_GAP_THRESHOLD and state != "SHORT":
            new_state = "SHORT"
        if new_state == state:
            return None
        if source == "coinalyze":
            xau_coinalyze_rolling_state = new_state
            title = f"XAU COINALYZE ROLLING 60M {new_state} | 100K GAP"
        else:
            xau_observer_rolling_state = new_state
            title = f"XAU OBSERVER ROLLING 60M {new_state} | 100K GAP"
        gap = abs(signed_gap)
        try: price_text = f"{float(price):,.2f}" if price is not None else "NA"
        except (TypeError, ValueError): price_text = "NA"
        message = (f"WINDOW: EXACT TRAILING 60 MINUTES | NO RESET\n"
                   f"LONG: ${long_total:,.0f} ({_usd_m(long_total)})\n"
                   f"SHORT: ${short_total:,.0f} ({_usd_m(short_total)})\n"
                   f"GAP: ${gap:,.0f} ({_usd_m(gap)})\n"
                   f"STRONGER: {new_state}\nSTATE: {state or 'NONE'} -> {new_state}\nXAU: {price_text}")
        sent = send_pushover(title, message)
        print(f"[XAU ROLLING 60M] {source.upper()} {new_state} L=${long_total:,.0f} S=${short_total:,.0f} GAP=${gap:,.0f} sent={sent}", flush=True)
        return {"direction":new_state,"long":long_total,"short":short_total,"gap":gap,"alert_sent":bool(sent)}


def _xau_rolling_add(source, side, amount, event_ts=None, exchange=None, price=None):
    source = str(source or "").lower().strip()
    side = str(side or "").lower().strip()
    if source not in ("coinalyze", "observer") or side not in ("long", "short"):
        return None
    try:
        amount = float(amount or 0.0)
        ts = float(event_ts) if event_ts is not None else time.time()
    except (TypeError, ValueError):
        return None
    if amount <= 0:
        return None
    if ts > 10_000_000_000:
        ts /= 1000.0
    with _xau_rolling_lock:
        events = xau_coinalyze_rolling_events if source == "coinalyze" else xau_observer_rolling_events
        events.append((ts, side, amount, str(exchange or "")))
        if len(events) > 1 and events[-2][0] > ts:
            ordered = sorted(events, key=lambda r: r[0]); events.clear(); events.extend(ordered)
    return _xau_rolling_evaluate(source, price=price, now_ts=max(time.time(), ts))


def _xau_rolling_add_coinalyze_row(long_amount, short_amount, event_ts, price=None):
    try:
        long_amount=max(0.0,float(long_amount or 0.0)); short_amount=max(0.0,float(short_amount or 0.0)); ts=float(event_ts)
    except (TypeError,ValueError):
        return None
    if ts > 10_000_000_000: ts /= 1000.0
    with _xau_rolling_lock:
        if long_amount > 0: xau_coinalyze_rolling_events.append((ts,"long",long_amount,"coinalyze"))
        if short_amount > 0: xau_coinalyze_rolling_events.append((ts,"short",short_amount,"coinalyze"))
        if len(xau_coinalyze_rolling_events) > 1:
            ordered=sorted(xau_coinalyze_rolling_events,key=lambda r:r[0]); xau_coinalyze_rolling_events.clear(); xau_coinalyze_rolling_events.extend(ordered)
    return _xau_rolling_evaluate("coinalyze", price=price, now_ts=max(time.time(),ts))


def _btc_rolling_trim(events, now_ts):
    cutoff = float(now_ts) - BTC_ROLLING_WINDOW_SECONDS
    while events and float(events[0][0]) <= cutoff:
        events.popleft()

def _btc_rolling_totals(events):
    long_total = sum(float(r[2]) for r in events if r[1] == "long")
    short_total = sum(float(r[2]) for r in events if r[1] == "short")
    return long_total, short_total

def _btc_rolling_add(source, side, amount, event_ts=None, exchange=None, price=None):
    global btc_coinalyze_rolling_state, btc_observer_rolling_state
    source = str(source or "").lower().strip()
    side = str(side or "").lower().strip()
    if source not in ("coinalyze", "observer") or side not in ("long", "short"):
        return None
    try:
        amount = float(amount or 0.0)
        ts = float(event_ts) if event_ts is not None else time.time()
    except (TypeError, ValueError):
        return None
    if amount <= 0:
        return None
    if ts > 10_000_000_000:
        ts /= 1000.0
    now_ts = max(time.time(), ts)
    with _btc_rolling_lock:
        events = btc_coinalyze_rolling_events if source == "coinalyze" else btc_observer_rolling_events
        state = btc_coinalyze_rolling_state if source == "coinalyze" else btc_observer_rolling_state
        events.append((ts, side, amount, str(exchange or "")))
        if len(events) > 1 and events[-2][0] > ts:
            ordered = sorted(events, key=lambda r: r[0]); events.clear(); events.extend(ordered)
        _btc_rolling_trim(events, now_ts)
        long_total, short_total = _btc_rolling_totals(events)
        signed_gap = long_total - short_total
        new_state = state
        if signed_gap >= BTC_ROLLING_GAP_THRESHOLD and state != "LONG":
            new_state = "LONG"
        elif signed_gap <= -BTC_ROLLING_GAP_THRESHOLD and state != "SHORT":
            new_state = "SHORT"
        if new_state == state:
            return None
        if source == "coinalyze":
            btc_coinalyze_rolling_state = new_state
            title = f"BTC COINALYZE ROLLING 60M {new_state} | +5M GAP"
        else:
            btc_observer_rolling_state = new_state
            title = f"BTC OBSERVER 13EX ROLLING 60M {new_state} | +5M GAP"
        gap = abs(signed_gap)
        try: price_text = f"{float(price):,.0f}" if price is not None else "NA"
        except (TypeError, ValueError): price_text = "NA"
        message = (f"WINDOW: EXACT TRAILING 60 MINUTES | NO RESET\n"
                   f"LONG: ${long_total:,.0f} ({_usd_m(long_total)})\n"
                   f"SHORT: ${short_total:,.0f} ({_usd_m(short_total)})\n"
                   f"GAP: ${gap:,.0f} ({_usd_m(gap)})\n"
                   f"STRONGER: {new_state}\nSTATE: {state or 'NONE'} -> {new_state}\nBTC: {price_text}")
        sent = send_pushover(title, message)
        print(f"[BTC ROLLING 60M] {source.upper()} {new_state} L=${long_total:,.0f} S=${short_total:,.0f} GAP=${gap:,.0f} sent={sent}", flush=True)
        return {"direction":new_state,"long":long_total,"short":short_total,"gap":gap,"alert_sent":bool(sent)}

def _btc_rolling_add_coinalyze_row(long_amount, short_amount, event_ts, price=None):
    global btc_coinalyze_rolling_state
    try:
        long_amount = max(0.0, float(long_amount or 0.0))
        short_amount = max(0.0, float(short_amount or 0.0))
        ts = float(event_ts)
    except (TypeError, ValueError):
        return None
    if ts > 10_000_000_000:
        ts /= 1000.0
    if long_amount <= 0 and short_amount <= 0:
        return None
    with _btc_rolling_lock:
        if long_amount > 0:
            btc_coinalyze_rolling_events.append((ts, "long", long_amount, "coinalyze"))
        if short_amount > 0:
            btc_coinalyze_rolling_events.append((ts, "short", short_amount, "coinalyze"))
        _btc_rolling_trim(btc_coinalyze_rolling_events, max(time.time(), ts))
        long_total, short_total = _btc_rolling_totals(btc_coinalyze_rolling_events)
        signed_gap = long_total - short_total
        state = btc_coinalyze_rolling_state
        new_state = state
        if signed_gap >= BTC_ROLLING_GAP_THRESHOLD and state != "LONG":
            new_state = "LONG"
        elif signed_gap <= -BTC_ROLLING_GAP_THRESHOLD and state != "SHORT":
            new_state = "SHORT"
        if new_state == state:
            return None
        btc_coinalyze_rolling_state = new_state
        gap = abs(signed_gap)
        try: price_text = f"{float(price):,.0f}" if price is not None else "NA"
        except (TypeError, ValueError): price_text = "NA"
        title = f"BTC COINALYZE ROLLING 60M {new_state} | +5M GAP"
        message = (f"WINDOW: EXACT TRAILING 60 MINUTES | NO RESET\n"
                   f"LONG: ${long_total:,.0f} ({_usd_m(long_total)})\n"
                   f"SHORT: ${short_total:,.0f} ({_usd_m(short_total)})\n"
                   f"GAP: ${gap:,.0f} ({_usd_m(gap)})\n"
                   f"STRONGER: {new_state}\nSTATE: {state or 'NONE'} -> {new_state}\nBTC: {price_text}")
        sent = send_pushover(title, message)
        print(f"[BTC ROLLING 60M] COINALYZE {new_state} L=${long_total:,.0f} S=${short_total:,.0f} GAP=${gap:,.0f} sent={sent}", flush=True)
        return {"direction":new_state,"long":long_total,"short":short_total,"gap":gap,"alert_sent":bool(sent)}


def _btc_observer_add(exchange_breakdown, price=None):
    global btc_observer_long_cumulative, btc_observer_short_cumulative
    global btc_observer_cycle_ref_price, btc_observer_last_alert_snapshot
    global btc_observer_by_exchange
    global btc_observer_gap_state

    accepted = {}
    for ex_name, totals in (exchange_breakdown or {}).items():
        if not isinstance(totals, dict):
            continue
        key = _btc_exchange_key(ex_name)
        if key not in BTC_OBSERVER_EXCHANGES:
            continue
        try:
            ex_long = max(0.0, float(totals.get("long", 0.0) or 0.0))
            ex_short = max(0.0, float(totals.get("short", 0.0) or 0.0))
        except (TypeError, ValueError):
            continue
        if ex_long <= 0 and ex_short <= 0:
            continue
        bucket = accepted.setdefault(key, {"long": 0.0, "short": 0.0})
        bucket["long"] += ex_long
        bucket["short"] += ex_short

    if not accepted:
        return None

    alert_snapshot = None
    with _combined_liq_lock:
        current_price = None
        try:
            if price is not None and float(price) > 0:
                current_price = float(price)
        except (TypeError, ValueError):
            pass
        if current_price is None:
            current_price = combined_latest_price.get("BTC")

        if btc_observer_cycle_ref_price is None and current_price is not None:
            btc_observer_cycle_ref_price = current_price

        for ex_name, totals in accepted.items():
            bucket = btc_observer_by_exchange.setdefault(
                ex_name, {"long": 0.0, "short": 0.0}
            )
            bucket["long"] += totals["long"]
            bucket["short"] += totals["short"]
            btc_observer_long_cumulative += totals["long"]
            btc_observer_short_cumulative += totals["short"]

        cycle_long = btc_observer_long_cumulative
        cycle_short = btc_observer_short_cumulative

        # Live observer audit: print every accepted MarginPad/direct update so
        # the 13-exchange wiring can be verified without waiting for +5M.
        update_parts = []
        for ex_name, totals in accepted.items():
            label = _btc_exchange_label(ex_name)
            update_parts.append(
                f"{label}(+L=${totals['long']:,.0f},+S=${totals['short']:,.0f})"
            )
        print(
            f"[BTC OBSERVER] {' | '.join(update_parts)} | "
            f"TOTAL L=${cycle_long:,.0f} S=${cycle_short:,.0f}",
            flush=True,
        )

        signed_gap = cycle_long - cycle_short
        long_hit = signed_gap >= BTC_GAP_THRESHOLD and btc_observer_gap_state != "LONG"
        short_hit = signed_gap <= -BTC_GAP_THRESHOLD and btc_observer_gap_state != "SHORT"

        if long_hit or short_hit:
            if long_hit:
                winner = "LONG"
                btc_observer_gap_state = "LONG"
                title = "BTC OBSERVER LONG WINS | +5M GAP"
            else:
                winner = "SHORT"
                btc_observer_gap_state = "SHORT"
                title = "BTC OBSERVER SHORT WINS | +5M GAP"

            gap = abs(signed_gap)
            move = (
                abs(current_price - btc_observer_cycle_ref_price)
                if current_price is not None and btc_observer_cycle_ref_price is not None
                else None
            )
            # Observer display audit: always show BOTH LONG and SHORT totals
            # for every one of the 13 exchanges. This is display-only; the
            # observer threshold, winner calculation, cycle and reset are unchanged.
            observer_ranked = []
            for ex_name in BTC_OBSERVER_EXCHANGES:
                ex_totals = btc_observer_by_exchange.get(
                    ex_name, {"long": 0.0, "short": 0.0}
                )
                ex_long = max(0.0, float(ex_totals.get("long", 0.0) or 0.0))
                ex_short = max(0.0, float(ex_totals.get("short", 0.0) or 0.0))
                observer_ranked.append(
                    (max(ex_long, ex_short), ex_name, ex_long, ex_short)
                )

            observer_ranked.sort(key=lambda row: row[0], reverse=True)
            exchange_lines = [
                f"{_btc_exchange_label(ex_name)}: LONG ${ex_long:,.0f} ({_usd_m(ex_long)}) | SHORT ${ex_short:,.0f} ({_usd_m(ex_short)})"
                for _, ex_name, ex_long, ex_short in observer_ranked
            ]
            alert_snapshot = {
                "asset": "BTC",
                "winner": winner,
                "title": title,
                "long": cycle_long,
                "short": cycle_short,
                "gap": gap,
                "price": current_price,
                "move": move,
                "exchanges": exchange_lines,
                "ts": int(time.time()),
            }
            btc_observer_last_alert_snapshot = dict(alert_snapshot)

            # Observer has its own cycle/reset only. Existing MarginPad, direct
            # liquidator, Coinalyze and MT5 states are untouched.
            btc_observer_long_cumulative = 0.0
            btc_observer_short_cumulative = 0.0
            btc_observer_by_exchange = {
                ex: {"long": 0.0, "short": 0.0}
                for ex in BTC_OBSERVER_EXCHANGES
            }
            btc_observer_cycle_ref_price = current_price

    if alert_snapshot:
        breakdown = "\n".join(alert_snapshot["exchanges"])
        price_text = (
            f"{alert_snapshot['price']:,.0f}"
            if alert_snapshot["price"] is not None else "NA"
        )
        move_text = (
            f"{alert_snapshot['move']:,.0f} pts"
            if alert_snapshot["move"] is not None else "NA"
        )
        message = (
            f"{breakdown}\n\n"
            f"TOTAL SHORT: ${alert_snapshot['short']:,.0f} ({_usd_m(alert_snapshot['short'])})\n"
            f"TOTAL LONG: ${alert_snapshot['long']:,.0f} ({_usd_m(alert_snapshot['long'])})\n"
            f"GAP: ${alert_snapshot['gap']:,.0f} ({_usd_m(alert_snapshot['gap'])})\n"
            f"BTC {price_text} | BTC MOVE {move_text}"
        )
        sent = send_pushover(alert_snapshot["title"], message)
        print(
            f"[BTC OBSERVER ALERT] {alert_snapshot['title']} "
            f"L=${alert_snapshot['long']:,.0f} S=${alert_snapshot['short']:,.0f} "
            f"sent={sent}",
            flush=True,
        )
        alert_snapshot["alert_sent"] = sent

    return alert_snapshot


def _btc_observer_add_direct(exchange, side, amount, price=None):
    return _btc_observer_add(
        {exchange: {
            "long": amount if side == "long" else 0.0,
            "short": amount if side == "short" else 0.0,
        }},
        price=price,
    )


def process_marginpad_btc(closed_minute_ts):
    global marginpad_btc_long_cumulative, marginpad_btc_short_cumulative
    global marginpad_btc_cycle_ref_price, marginpad_btc_processed_through_ms
    global marginpad_btc_last_alert_snapshot, marginpad_btc_by_exchange

    btc_price, price_error = get_marginpad_btc_price()

    if price_error:
        return {
            "ok": False,
            "asset": "BTC",
            "source": "MarginPad",
            "alert_sent": False,
            "error": price_error,
        }

    # Keep a fresh BTC price available to the standalone direct liquidator
    # without mixing either side's liquidation totals.
    with _combined_liq_lock:
        combined_latest_price["BTC"] = btc_price

    closed_end_ms = (closed_minute_ts + 59) * 1000 + 999

    if marginpad_btc_processed_through_ms is None:
        marginpad_btc_processed_through_ms = closed_end_ms
        marginpad_btc_cycle_ref_price = btc_price
        return {
            "ok": True,
            "asset": "BTC",
            "source": "MarginPad",
            "initialized": True,
            "btc_price": round(btc_price, 2),
            "marginpad_long_usd": round(marginpad_btc_long_cumulative, 2),
            "marginpad_short_usd": round(marginpad_btc_short_cumulative, 2),
            "cycle_reference_price": marginpad_btc_cycle_ref_price,
            "processed_through_ms": marginpad_btc_processed_through_ms,
        }

    if closed_end_ms <= marginpad_btc_processed_through_ms:
        return {
            "ok": True,
            "asset": "BTC",
            "source": "MarginPad",
            "new_closed_minute": False,
            "btc_price": round(btc_price, 2),
            "marginpad_long_usd": round(marginpad_btc_long_cumulative, 2),
            "marginpad_short_usd": round(marginpad_btc_short_cumulative, 2),
            "cycle_reference_price": marginpad_btc_cycle_ref_price,
            "processed_through_ms": marginpad_btc_processed_through_ms,
        }

    fresh, error = get_marginpad_fresh_btc_liquidations(
        marginpad_btc_processed_through_ms,
        closed_minute_ts,
    )

    if error:
        return {
            "ok": False,
            "asset": "BTC",
            "source": "MarginPad",
            "alert_sent": False,
            "marginpad_long_usd": round(marginpad_btc_long_cumulative, 2),
            "marginpad_short_usd": round(marginpad_btc_short_cumulative, 2),
            "processed_through_ms": marginpad_btc_processed_through_ms,
            "error": error,
        }

    fresh_long = float(fresh.get("fresh_long_usd", 0.0) or 0.0)
    fresh_short = float(fresh.get("fresh_short_usd", 0.0) or 0.0)
    fresh_by_exchange = fresh.get("fresh_by_exchange") or {}

    marginpad_btc_processed_through_ms = closed_end_ms

    alert_snapshot = None
    with _combined_liq_lock:
        if marginpad_btc_cycle_ref_price is None:
            marginpad_btc_cycle_ref_price = btc_price

        marginpad_btc_long_cumulative += fresh_long
        marginpad_btc_short_cumulative += fresh_short

        for ex_name, totals in fresh_by_exchange.items():
            if not isinstance(totals, dict):
                continue
            try:
                ex_long = max(0.0, float(totals.get("long", 0.0) or 0.0))
                ex_short = max(0.0, float(totals.get("short", 0.0) or 0.0))
            except (TypeError, ValueError):
                continue
            key = str(ex_name or "unknown").strip().lower() or "unknown"
            bucket = marginpad_btc_by_exchange.setdefault(key, {"long": 0.0, "short": 0.0})
            bucket["long"] += ex_long
            bucket["short"] += ex_short

        cycle_long = marginpad_btc_long_cumulative
        cycle_short = marginpad_btc_short_cumulative
        long_hit = cycle_long >= MARGINPAD_BTC_LIQ_THRESHOLD
        short_hit = cycle_short >= MARGINPAD_BTC_LIQ_THRESHOLD

        if long_hit or short_hit:
            if long_hit and short_hit:
                winner = "BOTH HIT SAME CYCLE"
                title = "BTC MARGINPAD BOTH HIT +5M"
            elif long_hit:
                winner = "LONG"
                title = "BTC MARGINPAD LONG WINS +5M"
            else:
                winner = "SHORT"
                title = "BTC MARGINPAD SHORT WINS +5M"

            gap = abs(cycle_long - cycle_short)
            move = (
                abs(btc_price - marginpad_btc_cycle_ref_price)
                if marginpad_btc_cycle_ref_price is not None
                else None
            )
            exchange_lines = _btc_standalone_exchange_lines(
                marginpad_btc_by_exchange,
                winner,
                include_all=True,
                required_exchanges=MARGINPAD_BTC_DISPLAY_EXCHANGES,
            )

            alert_snapshot = {
                "asset": "BTC",
                "winner": winner,
                "title": title,
                "long": cycle_long,
                "short": cycle_short,
                "gap": gap,
                "price": btc_price,
                "move": move,
                "exchanges": exchange_lines,
                "signal_source": "marginpad_liquidation",
                "ts": int(time.time()),
            }
            marginpad_btc_last_alert_snapshot = dict(alert_snapshot)

            # MarginPad BTC is the standalone execution source. The 4-exchange
            # BTC liquidator below is observation/alert only and cannot publish MT5.
            _publish_mt5_live_signal(alert_snapshot)

            marginpad_btc_long_cumulative = 0.0
            marginpad_btc_short_cumulative = 0.0
            marginpad_btc_by_exchange = {}
            marginpad_btc_cycle_ref_price = btc_price

    # Feed only this newly accepted MarginPad batch into the independent
    # 13-exchange observer. MarginPad's own cycle above remains unchanged.
    _btc_observer_add(fresh_by_exchange, price=btc_price)
    for revent in fresh.get("rolling_events", []):
        _btc_rolling_add("observer", revent.get("side"), revent.get("notional"), revent.get("ts_ms"), exchange=revent.get("exchange"), price=btc_price)

    sent = False
    if alert_snapshot:
        breakdown = "\n".join(alert_snapshot["exchanges"]) or "No exchange breakdown"
        move_text = (
            f"{alert_snapshot['move']:,.0f} pts"
            if alert_snapshot["move"] is not None
            else "NA"
        )
        message = (
            f"{breakdown}\n\n"
            f"MARGINPAD SHORT: ${alert_snapshot['short']:,.0f}\n"
            f"MARGINPAD LONG: ${alert_snapshot['long']:,.0f}\n"
            f"GAP: ${alert_snapshot['gap']:,.0f} ({_usd_m(alert_snapshot['gap'])})\n"
            f"BTC {alert_snapshot['price']:,.0f} | BTC MOVE {move_text}"
        )
        # Standalone MarginPad BTC Pushover intentionally disabled.
        # Calculation/reset + MT5 publication + Observer feed remain unchanged.
        sent = False
        print(
            f"[MARGINPAD BTC ALERT SILENT] {alert_snapshot['title']} "
            f"L=${alert_snapshot['long']:,.0f} S=${alert_snapshot['short']:,.0f}",
            flush=True,
        )

    return {
        "ok": True,
        "asset": "BTC",
        "source": "MarginPad",
        "initialized": False,
        "price": round(btc_price, 2),
        "events_returned": fresh["events_returned"],
        "events_accepted": fresh["events_accepted"],
        "exchanges_seen": fresh["exchanges_seen"],
        "fresh_long_usd": fresh_long,
        "fresh_short_usd": fresh_short,
        "threshold_usd": MARGINPAD_BTC_LIQ_THRESHOLD,
        "cycle_winner": alert_snapshot.get("winner") if alert_snapshot else None,
        "alert_sent": sent,
        "reset": bool(alert_snapshot),
        "marginpad_long_in_current_cycle": round(marginpad_btc_long_cumulative, 2),
        "marginpad_short_in_current_cycle": round(marginpad_btc_short_cumulative, 2),
        "by_exchange": marginpad_btc_by_exchange,
        "cycle_reference_price": marginpad_btc_cycle_ref_price,
        "processed_through_ms": marginpad_btc_processed_through_ms,
        "seen_event_cache": len(marginpad_seen_set),
    }


def add_direct_btc_liquidation_event(exchange, side, amount, event_key, price=None, event_ts=None):
    global direct_btc_long_cumulative, direct_btc_short_cumulative
    global direct_btc_cycle_ref_price, direct_btc_last_alert_snapshot
    global direct_btc_by_exchange

    exchange = str(exchange or "").lower().strip()
    side = str(side or "").lower().strip()

    if exchange not in COMBINED_DIRECT_EXCHANGES:
        return {"ok": False, "error": "invalid_exchange"}
    if side not in ("long", "short"):
        return {"ok": False, "error": "invalid_side"}

    try:
        amount = float(amount or 0.0)
    except (TypeError, ValueError):
        return {"ok": False, "error": "invalid_notional"}
    if amount <= 0:
        return {"ok": False, "error": "invalid_notional"}

    alert_snapshot = None
    with _combined_liq_lock:
        if not _combined_remember_direct_event(str(event_key or "")):
            return {
                "ok": True,
                "duplicate": True,
                "asset": "BTC",
                "source": "direct",
                "long_usd": round(direct_btc_long_cumulative, 2),
                "short_usd": round(direct_btc_short_cumulative, 2),
                "alert_sent": False,
            }

        current_price = combined_latest_price.get("BTC")
        if price is not None:
            try:
                p = float(price)
                if p > 0:
                    current_price = p
                    combined_latest_price["BTC"] = p
            except (TypeError, ValueError):
                pass

        if direct_btc_cycle_ref_price is None and current_price is not None:
            direct_btc_cycle_ref_price = current_price

        if side == "long":
            direct_btc_long_cumulative += amount
        else:
            direct_btc_short_cumulative += amount

        bucket = direct_btc_by_exchange.setdefault(exchange, {"long": 0.0, "short": 0.0})
        bucket[side] += amount

        cycle_long = direct_btc_long_cumulative
        cycle_short = direct_btc_short_cumulative
        long_hit = cycle_long >= DIRECT_BTC_LIQ_THRESHOLD
        short_hit = cycle_short >= DIRECT_BTC_LIQ_THRESHOLD

        if long_hit or short_hit:
            if long_hit and short_hit:
                winner = "BOTH HIT SAME CYCLE"
                title = "BTC LIQUIDATOR BOTH HIT +5M"
            elif long_hit:
                winner = "LONG"
                title = "BTC LIQUIDATOR LONG WINS +5M"
            else:
                winner = "SHORT"
                title = "BTC LIQUIDATOR SHORT WINS +5M"

            gap = abs(cycle_long - cycle_short)
            move = (
                abs(current_price - direct_btc_cycle_ref_price)
                if current_price is not None and direct_btc_cycle_ref_price is not None
                else None
            )
            exchange_lines = _btc_standalone_exchange_lines(
                direct_btc_by_exchange, winner, include_all=True
            )
            alert_snapshot = {
                "asset": "BTC",
                "winner": winner,
                "title": title,
                "long": cycle_long,
                "short": cycle_short,
                "gap": gap,
                "price": current_price,
                "move": move,
                "exchanges": exchange_lines,
                "ts": int(time.time()),
            }
            direct_btc_last_alert_snapshot = dict(alert_snapshot)

            # Observation only: intentionally no MT5 publication here.
            direct_btc_long_cumulative = 0.0
            direct_btc_short_cumulative = 0.0
            direct_btc_by_exchange = {
                ex: {"long": 0.0, "short": 0.0}
                for ex in COMBINED_DIRECT_EXCHANGES
            }
            direct_btc_cycle_ref_price = current_price

        result = {
            "ok": True,
            "duplicate": False,
            "asset": "BTC",
            "source": "direct",
            "exchange": exchange,
            "long_usd": round(cycle_long, 2),
            "short_usd": round(cycle_short, 2),
            "threshold_usd": DIRECT_BTC_LIQ_THRESHOLD,
            "winner": alert_snapshot.get("winner") if alert_snapshot else None,
            "alert_sent": False,
            "reset": bool(alert_snapshot),
        }

    # Event is already de-duplicated/accepted by the direct liquidator. Feed
    # the same contribution into the independent observer only once.
    _btc_observer_add_direct(exchange, side, amount, price=current_price)
    _btc_rolling_add("observer", side, amount, event_ts, exchange=exchange, price=current_price)

    if alert_snapshot:
        breakdown = "\n".join(alert_snapshot["exchanges"]) or "No exchange breakdown"
        price_text = f"{alert_snapshot['price']:,.0f}" if alert_snapshot["price"] is not None else "NA"
        move_text = (
            f"{alert_snapshot['move']:,.0f} pts"
            if alert_snapshot["move"] is not None
            else "NA"
        )
        message = (
            f"{breakdown}\n\n"
            f"LIQUIDATOR SHORT: ${alert_snapshot['short']:,.0f}\n"
            f"LIQUIDATOR LONG: ${alert_snapshot['long']:,.0f}\n"
            f"GAP: ${alert_snapshot['gap']:,.0f} ({_usd_m(alert_snapshot['gap'])})\n"
            f"BTC {price_text} | BTC MOVE {move_text}"
        )
        # Standalone 4-exchange BTC Liquidator Pushover intentionally disabled.
        # Calculation/reset + Observer feed remain unchanged.
        sent = False
        result["alert_sent"] = False
        print(
            f"[BTC LIQUIDATOR ALERT SILENT] {alert_snapshot['title']} "
            f"L=${alert_snapshot['long']:,.0f} S=${alert_snapshot['short']:,.0f}",
            flush=True,
        )
    else:
        print(
            f"[BTC LIQUIDATOR] {exchange.upper()} {side.upper()} +${amount:,.0f} | "
            f"TOTAL L=${result['long_usd']:,.0f} S=${result['short_usd']:,.0f}",
            flush=True,
        )

    return result


# ==================================================
# XAU PROCESSOR - MARGINPAD
# ==================================================

# ==================================================
# XAU PROCESSOR - MARGINPAD
# ==================================================

def process_marginpad_xau(
    closed_minute_ts
):

    global marginpad_xau_cycle_ref_price
    global marginpad_xau_processed_through_ms

    xau_price, price_error = get_marginpad_xau_price()

    if price_error:
        return {
            "ok": False,
            "asset": "XAU",
            "source": "MarginPad",
            "alert_sent": False,
            "error": price_error
        }

    closed_end_ms = (closed_minute_ts + 59) * 1000 + 999

    if marginpad_xau_processed_through_ms is None:
        marginpad_xau_processed_through_ms = closed_end_ms
        marginpad_xau_cycle_ref_price = xau_price

        with _combined_liq_lock:
            combined_latest_price["XAU"] = xau_price
            if combined_cycle_ref_price["XAU"] is None:
                combined_cycle_ref_price["XAU"] = xau_price

        return {
            "ok": True,
            "asset": "XAU",
            "source": "MarginPad",
            "initialized": True,
            "xau_price": round(xau_price, 2),
            "combined_long_usd": round(combined_liq["XAU"]["long"], 2),
            "combined_short_usd": round(combined_liq["XAU"]["short"], 2),
            "cycle_reference_price": combined_cycle_ref_price["XAU"],
            "processed_through_ms": marginpad_xau_processed_through_ms
        }

    if closed_end_ms <= marginpad_xau_processed_through_ms:
        with _combined_liq_lock:
            combined_latest_price["XAU"] = xau_price

        return {
            "ok": True,
            "asset": "XAU",
            "source": "MarginPad",
            "new_closed_minute": False,
            "xau_price": round(xau_price, 2),
            "combined_long_usd": round(combined_liq["XAU"]["long"], 2),
            "combined_short_usd": round(combined_liq["XAU"]["short"], 2),
            "cycle_reference_price": combined_cycle_ref_price["XAU"],
            "processed_through_ms": marginpad_xau_processed_through_ms
        }

    fresh, error = get_marginpad_fresh_xau_liquidations(
        marginpad_xau_processed_through_ms,
        closed_minute_ts
    )

    if error:
        return {
            "ok": False,
            "asset": "XAU",
            "source": "MarginPad",
            "alert_sent": False,
            "combined_long_usd": round(combined_liq["XAU"]["long"], 2),
            "combined_short_usd": round(combined_liq["XAU"]["short"], 2),
            "processed_through_ms": marginpad_xau_processed_through_ms,
            "error": error
        }

    fresh_long = fresh["fresh_long_usd"]
    fresh_short = fresh["fresh_short_usd"]

    # Advance only after a successful MarginPad fetch/parse.
    marginpad_xau_processed_through_ms = closed_end_ms

    for revent in fresh.get("rolling_events", []):
        _xau_rolling_add(
            "observer", revent.get("side"), revent.get("notional"),
            revent.get("ts_ms"), exchange=revent.get("exchange"), price=xau_price
        )
    _xau_rolling_evaluate("observer", price=xau_price)

    combined_result = add_combined_liquidation_batch(
        asset="XAU",
        source="marginpad",
        exchange="marginpad",
        long_usd=fresh_long,
        short_usd=fresh_short,
        event_key=f"marginpad-xau|{closed_end_ms}",
        price=xau_price,
    )

    return {
        "ok": True,
        "asset": "XAU",
        "source": "MarginPad",
        "initialized": False,
        "price": round(xau_price, 2),
        "events_returned": fresh["events_returned"],
        "events_accepted": fresh["events_accepted"],
        "exchanges_seen": fresh["exchanges_seen"],
        "fresh_long_usd": fresh_long,
        "fresh_short_usd": fresh_short,
        "combined_long_before_reset": combined_result["combined_long_usd"],
        "combined_short_before_reset": combined_result["combined_short_usd"],
        "threshold_usd": COMBINED_LIQ_THRESHOLDS["XAU"],
        "cycle_winner": combined_result.get("winner"),
        "alert_sent": combined_result.get("alert_sent", False),
        "combined_reset": combined_result.get("reset", False),
        "current_combined_long_usd": round(combined_liq["XAU"]["long"], 2),
        "current_combined_short_usd": round(combined_liq["XAU"]["short"], 2),
        "marginpad_long_in_current_cycle": round(marginpad_xau_long_cumulative, 2),
        "marginpad_short_in_current_cycle": round(marginpad_xau_short_cumulative, 2),
        "cycle_reference_price": combined_cycle_ref_price["XAU"],
        "processed_through_ms": marginpad_xau_processed_through_ms,
        "seen_event_cache": len(marginpad_xau_seen_set)
    }


# ==================================================
# XAU PROCESSOR
# ==================================================

# ==================================================
# XAU PROCESSOR
# ==================================================

def process_xau(
    closed_minute_ts
):

    global xau_long_cumulative
    global xau_short_cumulative
    global xau_cycle_ref_price
    global xau_last_processed_liq_ts
    global xau_coinalyze_gap_state

    xau_price, price_error = (
        get_xau_price()
    )

    if price_error:

        return {
            "ok": False,
            "asset": "XAU",
            "alert_sent": False,
            "error": price_error
        }

    if (
        xau_last_processed_liq_ts
        is None
    ):

        xau_last_processed_liq_ts = (
            closed_minute_ts
        )

        xau_cycle_ref_price = (
            xau_price
        )

        xau_long_cumulative = 0.0
        xau_short_cumulative = 0.0

        return {
            "ok": True,
            "asset": "XAU",
            "initialized": True,

            "xau_price":
                round(
                    xau_price,
                    2
                ),

            "long_cumulative_usd":
                0,

            "short_cumulative_usd":
                0,

            "cycle_reference_price":
                xau_cycle_ref_price,

            "last_processed_liq_ts":
                xau_last_processed_liq_ts
        }

    if (
        closed_minute_ts
        <=
        xau_last_processed_liq_ts
    ):

        return {
            "ok": True,
            "asset": "XAU",
            "new_closed_minute": False,

            "xau_price":
                round(
                    xau_price,
                    2
                ),

            "long_cumulative_usd":
                round(
                    xau_long_cumulative,
                    2
                ),

            "short_cumulative_usd":
                round(
                    xau_short_cumulative,
                    2
                ),

            "cycle_reference_price":
                xau_cycle_ref_price,

            "last_processed_liq_ts":
                xau_last_processed_liq_ts
        }

    fresh, error = (
        get_fresh_liquidations(
            "XAU",
            xau_last_processed_liq_ts,
            closed_minute_ts
        )
    )

    if error:

        return {
            "ok": False,
            "asset": "XAU",
            "alert_sent": False,

            "long_cumulative_usd":
                round(
                    xau_long_cumulative,
                    2
                ),

            "short_cumulative_usd":
                round(
                    xau_short_cumulative,
                    2
                ),

            "last_processed_liq_ts":
                xau_last_processed_liq_ts,

            "error":
                error
        }

    fresh_long = (
        fresh["fresh_long_usd"]
    )

    fresh_short = (
        fresh["fresh_short_usd"]
    )

    for rr in fresh.get("rolling_rows", []):
        _xau_rolling_add_coinalyze_row(
            rr.get("long", 0.0), rr.get("short", 0.0), rr.get("ts"), price=xau_price
        )
    _xau_rolling_evaluate("coinalyze", price=xau_price)

    xau_long_cumulative += (
        fresh_long
    )

    xau_short_cumulative += (
        fresh_short
    )

    xau_last_processed_liq_ts = (
        closed_minute_ts
    )

    cycle_long = (
        xau_long_cumulative
    )

    cycle_short = (
        xau_short_cumulative
    )

    cycle_gap = abs(
        cycle_long
        -
        cycle_short
    )

    signed_gap = cycle_long - cycle_short
    long_hit = signed_gap >= XAU_GAP_THRESHOLD and xau_coinalyze_gap_state != "LONG"
    short_hit = signed_gap <= -XAU_GAP_THRESHOLD and xau_coinalyze_gap_state != "SHORT"

    alert_sent = False
    cycle_winner = None
    xau_price_move = None

    if (
        xau_cycle_ref_price
        is not None
    ):

        xau_price_move = abs(
            xau_price
            -
            xau_cycle_ref_price
        )

    if (
        long_hit
        or
        short_hit
    ):

        if long_hit:
            cycle_winner = "LONG"
            xau_coinalyze_gap_state = "LONG"
            alert_title = "XAU COINALYZE LONG WINS | 100K GAP"
        else:
            cycle_winner = "SHORT"
            xau_coinalyze_gap_state = "SHORT"
            alert_title = "XAU COINALYZE SHORT WINS | 100K GAP"

        move_text = (
            f"{xau_price_move:,.2f} pts"
            if
            xau_price_move
            is not None
            else
            "NA"
        )

        cycle_total = (
            cycle_long
            +
            cycle_short
        )

        long_pct = (
            cycle_long
            /
            cycle_total
            *
            100
        ) if cycle_total > 0 else 0

        short_pct = (
            cycle_short
            /
            cycle_total
            *
            100
        ) if cycle_total > 0 else 0

        alert_sent = send_pushover(
            alert_title,
            (
                f"WINNER "
                f"{cycle_winner} | "
                f"LONG "
                f"${cycle_long:,.0f} ({_usd_m(cycle_long)}) "
                f"({long_pct:.2f}%) | "
                f"SHORT "
                f"${cycle_short:,.0f} ({_usd_m(cycle_short)}) "
                f"({short_pct:.2f}%) | "
                f"GAP "
                f"${cycle_gap:,.0f} ({_usd_m(cycle_gap)}) | "
                f"XAU "
                f"{xau_price:,.2f} | "
                f"XAU MOVE "
                f"{move_text}"
            )
        )

        xau_long_cumulative = 0.0
        xau_short_cumulative = 0.0

        xau_cycle_ref_price = (
            xau_price
        )

    return {
        "ok": True,
        "asset": "XAU",
        "initialized": False,

        "perpetual_symbols":
            fresh[
                "perpetual_symbols"
            ],

        "successful_batch_count":
            fresh[
                "successful_batch_count"
            ],

        "price":
            round(
                xau_price,
                2
            ),

        "price_symbol":
            xau_price_symbol_cache,

        "fresh_long_usd":
            fresh_long,

        "fresh_short_usd":
            fresh_short,

        "cycle_long_before_reset":
            round(
                cycle_long,
                2
            ),

        "cycle_short_before_reset":
            round(
                cycle_short,
                2
            ),

        "cycle_gap_usd":
            round(
                cycle_gap,
                2
            ),

        "long_cumulative_usd":
            round(
                xau_long_cumulative,
                2
            ),

        "short_cumulative_usd":
            round(
                xau_short_cumulative,
                2
            ),

        "threshold_usd":
            XAU_LIQ_THRESHOLD,

        "cycle_winner":
            cycle_winner,

        "alert_sent":
            alert_sent,

        "price_move_points":
            (
                round(
                    xau_price_move,
                    2
                )
                if
                xau_price_move
                is not None
                else
                None
            ),

        "cycle_reference_price":
            xau_cycle_ref_price,

        "last_processed_liq_ts":
            xau_last_processed_liq_ts
    }


# ==================================================
# HELPER: LAST CLOSED MINUTE
# ==================================================

def get_closed_minute_ts():

    now = int(
        time.time()
    )

    current_minute_start = (
        now // 60
    ) * 60

    return (
        current_minute_start
        -
        60
    )


# ==================================================
# CRON AUTHORIZATION
# ==================================================

def cron_authorized():

    supplied = (
        request.headers.get(
            "X-Cron-Secret",
            ""
        )
    )

    return (
        bool(CRON_SECRET)
        and supplied == CRON_SECRET
    )


# ==================================================
# BTC ONLY ENDPOINT - COINALYZE
# ==================================================

@app.get("/btc-minute-alert")
def btc_minute_alert():

    if not cron_authorized():

        return jsonify({
            "ok": False,
            "error": "unauthorized"
        }), 403

    try:

        closed_minute_ts = (
            get_closed_minute_ts()
        )

        btc_result = (
            process_btc(
                closed_minute_ts
            )
        )

        if not btc_result.get(
            "ok",
            False
        ):

            print(
                "BTC PROCESS ERROR:",
                btc_result
            )

        return jsonify({
            "ok":
                btc_result.get(
                    "ok",
                    False
                ),

            "retry_needed":
                not btc_result.get(
                    "ok",
                    False
                ),

            "closed_minute_ts":
                closed_minute_ts,

            "btc":
                btc_result
        }), 200

    except Exception as e:

        print(
            "BTC-MINUTE-ALERT ERROR:",
            str(e)
        )

        return jsonify({
            "ok": False,
            "retry_needed": True,
            "alert_sent": False,
            "error": str(e)
        }), 200


# ==================================================
# COMBINED LIQUIDATION ENDPOINTS
# ==================================================

@app.post("/direct-liquidation-event")
def direct_liquidation_event():
    supplied_secret = request.headers.get("X-Direct-Liq-Secret", "").strip()

    if not DIRECT_LIQ_SECRET or supplied_secret != DIRECT_LIQ_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 403

    data = request.get_json(silent=True) or {}
    asset = str(data.get("asset", "")).upper().strip()
    exchange = str(data.get("exchange", "")).lower().strip()
    side = str(data.get("side", "")).lower().strip()
    event_key = str(data.get("event_key") or data.get("event_id") or "").strip()

    if asset not in ("BTC", "XAU"):
        return jsonify({"ok": False, "error": "invalid_asset"}), 400

    if exchange not in COMBINED_DIRECT_EXCHANGES:
        return jsonify({"ok": False, "error": "invalid_exchange"}), 400

    if side not in ("long", "short"):
        return jsonify({"ok": False, "error": "invalid_side"}), 400

    if not event_key:
        return jsonify({"ok": False, "error": "missing_event_key"}), 400

    try:
        amount = float(data.get("notional_usd", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "invalid_notional"}), 400

    if amount <= 0:
        return jsonify({"ok": False, "error": "invalid_notional"}), 400

    if asset == "BTC":
        result = add_direct_btc_liquidation_event(
            exchange=exchange,
            side=side,
            amount=amount,
            event_key=event_key,
            price=data.get("price"),
            event_ts=(data.get("ts_ms") or data.get("timestamp_ms") or data.get("ts") or data.get("timestamp")),
        )
    else:
        event_ts = (data.get("ts_ms") or data.get("timestamp_ms") or data.get("ts") or data.get("timestamp"))
        result = add_combined_liquidation_batch(
            asset=asset,
            source="direct",
            exchange=exchange,
            long_usd=amount if side == "long" else 0.0,
            short_usd=amount if side == "short" else 0.0,
            event_key=event_key,
            price=data.get("price"),
        )
        if not result.get("duplicate"):
            _xau_rolling_add(
                "observer", side, amount, event_ts, exchange=exchange, price=data.get("price")
            )

    return jsonify(result), 200


@app.get("/combined-liquidation-state")
def combined_liquidation_state():
    if not cron_authorized():
        return jsonify({"ok": False, "error": "unauthorized"}), 403

    with _combined_liq_lock:
        return jsonify({
            "ok": True,
            "BTC": {
                "mode": "SEPARATE_MARGINPAD_AND_DIRECT",
                "marginpad": {
                    "threshold_usd": MARGINPAD_BTC_LIQ_THRESHOLD,
                    "long_usd": round(marginpad_btc_long_cumulative, 2),
                    "short_usd": round(marginpad_btc_short_cumulative, 2),
                    "by_exchange": marginpad_btc_by_exchange,
                    "last_alert": marginpad_btc_last_alert_snapshot,
                },
                "liquidator": {
                    "threshold_usd": DIRECT_BTC_LIQ_THRESHOLD,
                    "long_usd": round(direct_btc_long_cumulative, 2),
                    "short_usd": round(direct_btc_short_cumulative, 2),
                    "by_exchange": direct_btc_by_exchange,
                    "last_alert": direct_btc_last_alert_snapshot,
                },
            },
            "XAU": {
                "threshold_usd": COMBINED_LIQ_THRESHOLDS["XAU"],
                "long_usd": round(combined_liq["XAU"]["long"], 2),
                "short_usd": round(combined_liq["XAU"]["short"], 2),
                "by_source": combined_by_source["XAU"],
                "last_alert": combined_last_alert["XAU"],
            },
            "direct_seen_count": len(combined_direct_seen_set),
        }), 200


# ==================================================
# MT5 DEMO SIGNAL BRIDGE ENDPOINTS
# ==================================================

def mt5_bridge_authorized():
    supplied = request.headers.get("X-MT5-Secret", "").strip()
    return bool(MT5_BRIDGE_SECRET) and supplied == MT5_BRIDGE_SECRET


@app.get("/mt5-signal")
def mt5_signal():
    if not mt5_bridge_authorized():
        return jsonify({"ok": False, "error": "unauthorized"}), 403

    mode = str(request.args.get("mode", "live")).strip().lower()

    with _mt5_signal_lock:
        if mode == "test":
            signals = {
                "BTC": mt5_test_signals["BTC"],
                "XAU": mt5_test_signals["XAU"],
            }
            signal_mode = "TEST_ONLY"
        else:
            signals = {
                "BTC": mt5_latest_signals["BTC"],
                "XAU": mt5_latest_signals["XAU"],
            }
            signal_mode = "LIVE_COMBINED"

    return jsonify({
        "ok": True,
        "mode": signal_mode,
        "server_ts": int(time.time()),
        "signals": signals,
    }), 200


@app.post("/mt5-test-signal")
def mt5_test_signal():
    if not mt5_bridge_authorized():
        return jsonify({"ok": False, "error": "unauthorized"}), 403

    data = request.get_json(silent=True) or {}
    asset = str(data.get("asset", "")).upper().strip()
    side = str(data.get("side", "")).upper().strip()

    if asset not in ("BTC", "XAU"):
        return jsonify({"ok": False, "error": "invalid_asset"}), 400

    if side not in ("BUY", "SELL"):
        return jsonify({"ok": False, "error": "invalid_side"}), 400

    winner = "SHORT" if side == "BUY" else "LONG"

    signal = _mt5_make_signal(
        asset=asset,
        winner=winner,
        side=side,
        source="synthetic_test_only",
        mode="TEST_ONLY",
    )

    with _mt5_signal_lock:
        mt5_test_signals[asset] = signal

    print(
        f"[MT5 BRIDGE] TEST ONLY {asset} {winner} -> {side} | id={signal['id']}",
        flush=True,
    )

    return jsonify({
        "ok": True,
        "test_only": True,
        "combined_totals_untouched": True,
        "pushover_untouched": True,
        "signal": signal,
    }), 200


# ==================================================
# BTC ONLY ENDPOINT - MARGINPAD
# ==================================================

@app.get("/marginpad-btc-minute-alert")
def marginpad_btc_minute_alert():

    if not cron_authorized():

        return jsonify({
            "ok": False,
            "error": "unauthorized"
        }), 403

    try:

        closed_minute_ts = (
            get_closed_minute_ts()
        )

        btc_result = (
            process_marginpad_btc(
                closed_minute_ts
            )
        )

        if not btc_result.get(
            "ok",
            False
        ):

            print(
                "MARGINPAD BTC PROCESS ERROR:",
                btc_result
            )

        return jsonify({
            "ok":
                btc_result.get(
                    "ok",
                    False
                ),

            "retry_needed":
                not btc_result.get(
                    "ok",
                    False
                ),

            "closed_minute_ts":
                closed_minute_ts,

            "marginpad_btc":
                btc_result
        }), 200

    except Exception as e:

        print(
            "MARGINPAD-BTC-MINUTE-ALERT ERROR:",
            str(e)
        )

        return jsonify({
            "ok": False,
            "retry_needed": True,
            "alert_sent": False,
            "error": str(e)
        }), 200


# ==================================================
# XAU ONLY ENDPOINT - MARGINPAD
# ==================================================

@app.get("/marginpad-xau-minute-alert")
def marginpad_xau_minute_alert():

    if not cron_authorized():

        return jsonify({
            "ok": False,
            "error": "unauthorized"
        }), 403

    try:

        closed_minute_ts = (
            get_closed_minute_ts()
        )

        xau_result = (
            process_marginpad_xau(
                closed_minute_ts
            )
        )

        if not xau_result.get(
            "ok",
            False
        ):

            print(
                "MARGINPAD XAU PROCESS ERROR:",
                xau_result
            )

        return jsonify({
            "ok": xau_result.get(
                "ok",
                False
            ),
            "retry_needed": not xau_result.get(
                "ok",
                False
            ),
            "closed_minute_ts": closed_minute_ts,
            "marginpad_xau": xau_result
        }), 200

    except Exception as e:

        print(
            "MARGINPAD-XAU-MINUTE-ALERT ERROR:",
            str(e)
        )

        return jsonify({
            "ok": False,
            "retry_needed": True,
            "alert_sent": False,
            "error": str(e)
        }), 200


# ==================================================
# XAU ONLY ENDPOINT
# ==================================================

@app.get("/xau-minute-alert")
def xau_minute_alert():

    try:

        closed_minute_ts = (
            get_closed_minute_ts()
        )

        xau_result = (
            process_xau(
                closed_minute_ts
            )
        )

        if not xau_result.get(
            "ok",
            False
        ):

            print(
                "XAU PROCESS ERROR:",
                xau_result
            )

        return jsonify({
            "ok":
                xau_result.get(
                    "ok",
                    False
                ),

            "retry_needed":
                not xau_result.get(
                    "ok",
                    False
                ),

            "closed_minute_ts":
                closed_minute_ts,

            "xau":
                xau_result
        }), 200

    except Exception as e:

        print(
            "XAU-MINUTE-ALERT ERROR:",
            str(e)
        )

        return jsonify({
            "ok": False,
            "retry_needed": True,
            "alert_sent": False,
            "error": str(e)
        }), 200


# ==================================================
# ALL CRYPTO TEST ENDPOINT - MARGINPAD /api/v1/feed
# ==================================================

@app.get("/all-crypto-status")
def all_crypto_status():
    with _all_crypto_lock:
        _all_crypto_rolling_trim(time.time())
        rolling_long, rolling_short = _all_crypto_rolling_totals()
        return jsonify({
            "ok": True,
            "threshold_usd": ALL_CRYPTO_GAP_THRESHOLD,
            "reset_cycle": {
                "long": round(all_crypto_long_cumulative, 2),
                "short": round(all_crypto_short_cumulative, 2),
                "gap": round(all_crypto_long_cumulative - all_crypto_short_cumulative, 2),
                "state": all_crypto_gap_state,
            },
            "rolling_60m": {
                "long": round(rolling_long, 2),
                "short": round(rolling_short, 2),
                "gap": round(rolling_long - rolling_short, 2),
                "state": all_crypto_rolling_state,
                "events": len(all_crypto_rolling_events),
            },
            "crypto_symbols": len(all_crypto_crypto_symbols),
            "seen_events": len(all_crypto_seen_set),
            "last_poll_ts": all_crypto_last_poll_ts,
            "last_error": all_crypto_last_error,
        }), 200


@app.get("/all-crypto-poll-now")
def all_crypto_poll_now():
    if not cron_authorized():
        return jsonify({"ok": False, "error": "unauthorized"}), 403
    return jsonify(process_marginpad_all_crypto_feed()), 200


# Start the independent 5-second market-wide liquidation collector.
# Existing Render config is documented as one worker in this app's persistence section.
_start_all_crypto_poller_once()


# ==================================================
# START SERVER
# ==================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
