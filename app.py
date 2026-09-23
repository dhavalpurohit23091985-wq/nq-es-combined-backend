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
# NASDAQ / NQ GENERATION SWITCHES
# ==================================================
# CURRENT setup kept ON:
#   NASDAQ 10-STOCK | QQQ WEIGHTED | ROLLING LAST-4 | +/-0.100%
#
# OLD setups disabled:
#   - NQ + ES combined-delta +/-1000 engine
#   - NASDAQ TOP5/BOTTOM5 combined confirmation engine
LATEST_QQQ_LAST4_ENABLED = True
OLD_NQ_ES_DELTA_ENABLED = False
OLD_NASDAQ_TOP5_BOTTOM5_ENABLED = False


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
# Display-only timestamps: when the current rolling direction was first triggered.
# Used only to show CHANGE TIME on the next reverse alert.
btc_coinalyze_rolling_state_ts = None
btc_observer_rolling_state_ts = None
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
XAU_NORMAL_OBSERVER_STATE_EPOCH = 2  # one-time stale-lock migration
xau_coinalyze_rolling_events = deque()
xau_observer_rolling_events = deque()
xau_coinalyze_rolling_state = None
xau_observer_rolling_state = None
# Display-only timestamps for rolling direction changes.
xau_coinalyze_rolling_state_ts = None
xau_observer_rolling_state_ts = None
_xau_rolling_lock = threading.RLock()

# XAU Observer mirrors the BTC 13EX universe:
# MarginPad 9 approved exchanges + Direct 4 exchanges.
XAU_MARGINPAD_EXCHANGES = (
    "binance", "bybit", "okx", "hyperliquid", "gate", "htx",
    "dydx", "bitmex", "bitfinex",
)
XAU_OBSERVER_EXCHANGES = (
    *XAU_MARGINPAD_EXCHANGES,
    "bitget", "aster", "coinex", "lighter",
)


# ==================================================
# ALL CRYPTO LIQUIDATION - 13EX (MARGINPAD 9 + DIRECT 4)
# ==================================================
# Independent test setup. Existing BTC/XAU logic is untouched.
# Sources: MarginPad GET /api/v1/feed filtered to 9 exchanges + Direct 4 worker.
# Only symbols classified as crypto are accepted. XAU/metals/indices are excluded.
#
# Setup A: actual LONG-SHORT GAP +/-$5M, RESET after valid reverse-only alert.
# Setup B: exact trailing 60m GAP +/-$5M, NO RESET, reverse-only.

ALL_CRYPTO_GAP_THRESHOLD = 5_000_000.0
ALL_CRYPTO_ROLLING_WINDOW_SECONDS = 3600
ALL_CRYPTO_ROLLING_GAP_THRESHOLD = 5_000_000.0
ALL_CRYPTO_POLL_SECONDS = 5.0
ALL_CRYPTO_SEEN_MAX = 50_000

# Fixed ALL Crypto 13EX universe: MarginPad 9 + Direct 4.
ALL_CRYPTO_MARGINPAD_EXCHANGES = (
    "binance", "bybit", "okx", "hyperliquid", "gate", "htx",
    "dydx", "bitmex", "bitfinex",
)
ALL_CRYPTO_DIRECT_EXCHANGES = ("bitget", "aster", "coinex", "lighter")
ALL_CRYPTO_OBSERVER_EXCHANGES = (
    *ALL_CRYPTO_MARGINPAD_EXCHANGES,
    *ALL_CRYPTO_DIRECT_EXCHANGES,
)

all_crypto_long_cumulative = 0.0
all_crypto_short_cumulative = 0.0
all_crypto_gap_state = None
# Display-only timestamp of the last valid +/-$5M cumulative state change.
all_crypto_gap_state_ts = None
# Start timestamp of the current cumulative RESET cycle.
# Starts on the first accepted liquidation after reset and persists across restarts.
all_crypto_cycle_start_ts = None
all_crypto_rolling_events = deque()
all_crypto_rolling_state = None
# Display-only timestamp for the current rolling direction.
all_crypto_rolling_state_ts = None

# Read-only /gap-check snapshots.
# Updated ONLY when the corresponding existing alert condition actually fires.
# These snapshots do not participate in alert calculations, reset logic, MT5 or Pushover.
gap_check_last_alerts = {
    "all_crypto_normal": None,
    "all_crypto_rolling": None,
    "btc_observer_rolling": None,
    "btc_coinalyze_rolling": None,
    "eth_observer_rolling": None,
    "eth_coinalyze_rolling": None,
    "sol_observer_rolling": None,
    "sol_coinalyze_rolling": None,
    "xau_observer_normal": None,
    "xau_coinalyze_normal": None,
    "xau_observer_rolling": None,
    "xau_coinalyze_rolling": None,
}
all_crypto_seen_queue = deque()
all_crypto_seen_set = set()
all_crypto_by_symbol = {}
# Read-only unusual-strength ledger: lifetime/no-reset per-symbol totals.
# It receives the same already-accepted ALL CRYPTO 13EX events, sends NO alerts,
# and remembers the first time each symbol reaches an absolute $5M GAP.
ALL_CRYPTO_UNUSUAL_GAP_THRESHOLD = 5_000_000.0
all_crypto_unusual_by_symbol = {}
all_crypto_last_poll_ts = None
all_crypto_last_error = None
all_crypto_crypto_symbols = set()
all_crypto_crypto_symbols_refreshed_ts = 0.0
all_crypto_first_poll_seeded = False
_all_crypto_lock = threading.RLock()
_all_crypto_poller_started = False

# ETH / SOL 13EX NORMAL GAP observers.
# Same reverse-only +/-$5M GAP behavior as the dedicated BTC Observer,
# fed from the same accepted MarginPad-9 + Direct-4 crypto events.
ALT_GAP_ASSETS = ("ETH", "SOL")
alt_gap_observer = {
    asset: {
        "long": 0.0,
        "short": 0.0,
        "state": None,
        "by_exchange": {ex: {"long": 0.0, "short": 0.0} for ex in ALL_CRYPTO_OBSERVER_EXCHANGES},
    }
    for asset in ALT_GAP_ASSETS
}
# Hourly discovery report: exact trailing 60m, sent on each IST clock-hour.
# Independent from GAP/rolling trigger state; never resets liquidation data.
ALL_CRYPTO_HOURLY_TOP_N = 12
_all_crypto_hourly_reporter_started = False

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
# Exchange-level audit for the current XAU Coinalyze cumulative GAP cycle.
# Display/audit only: does not change totals, threshold, direction state or reset logic.
xau_coinalyze_by_exchange = {}

xau_cycle_ref_price = None
xau_last_processed_liq_ts = None

xau_symbol_cache = None
xau_price_symbol_cache = None
coinalyze_symbol_exchange_cache = {}


# ==================================================
# FUTURE MARKETS CACHE
# ==================================================

future_markets_cache = None


# ==================================================
# PUSHOVER
# ==================================================

def send_pushover(title, message):

    # ==================================================
    # FINAL PUSHOVER SCOPE
    # ==================================================
    # Phone alerts allowed ONLY for:
    #   1) Current NASDAQ QQQ WEIGHTED ROLLING LAST-4 setup
    #   2) BTC OBSERVER 13EX normal GAP alert ($5M)
    #   3) XAU OBSERVER 13EX normal GAP alert ($100K)
    #
    # IMPORTANT:
    #   - ROLLING 60M stays phone-silent.
    #   - ALL CRYPTO 13EX stays phone-silent.
    #   - Coinalyze/MarginPad/Direct/old setups stay phone-silent.
    #   - CoinGlass BTC/XAU runs in its own separate service and is unaffected.

    title_upper = str(title or "").upper()
    message_upper = str(message or "").upper()

    latest_qqq_last4 = (
        "NASDAQ" in title_upper
        and (
            "QQQ WEIGHTED" in title_upper
            or "QQQ WEIGHTED" in message_upper
            or "ROLLING LAST-4" in title_upper
            or "ROLLING LAST-4" in message_upper
            or "LAST-4" in title_upper
            or "LAST-4" in message_upper
        )
    )

    btc_observer_13ex_normal = (
        "BTC OBSERVER 13EX" in title_upper
        and "ROLLING" not in title_upper
        and "5M GAP" in title_upper
    )

    xau_observer_13ex_normal = (
        "XAU OBSERVER 13EX" in title_upper
        and "ROLLING" not in title_upper
        and "100K GAP" in title_upper
    )

    allowed = (
        latest_qqq_last4
        or btc_observer_13ex_normal
        or xau_observer_13ex_normal
    )

    if not allowed:
        print(
            f"[PUSHOVER SILENT - NQ + BTC13EX + XAU13EX ONLY] {title}",
            flush=True,
        )
        return False

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
    global gap_check_last_alerts

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
                f"SETUP: {setup}\n"
                "ALERT 1:\n"
                f"{first['source']} {first['direction']} | {first['time']}\n"
                "ALERT 2:\n"
                f"{source} {direction} | {event_time}\n"
                "SEQUENCE:\n"
                f"{first['direction']} -> {direction}\n"
                "RESULT:\n"
                f"COMBINED {combined_direction}\n"
                f"NQ: {nq_display}\n"
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

        # Preserve real exchange-level contributions for BTC + XAU auditing.
        # Audit state only: combined totals / thresholds / reset logic are unchanged.
        if asset in ("BTC", "XAU"):
            if source == "marginpad" and isinstance(exchange_breakdown, dict):
                for ex_name, ex_totals in exchange_breakdown.items():
                    ex_key = _btc_exchange_key(ex_name)
                    if asset == "XAU" and ex_key not in XAU_MARGINPAD_EXCHANGES:
                        continue
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
                ex_key = _btc_exchange_key(exchange or source_key)
                if asset != "XAU" or ex_key in XAU_OBSERVER_EXCHANGES:
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

            # XAU 13EX NORMAL GAP: alert only on a true direction flip.
            # Use BOTH the dedicated direction state and the persisted last-alert
            # winner as a restart/reload-safe dedup guard. This prevents the same
            # LONG (or SHORT) from being re-sent on every ~5 minute accumulation
            # cycle if one state field is temporarily restored as None.
            _xau_last_winner = None
            _xau_last_alert = combined_last_alert.get("XAU")
            if isinstance(_xau_last_alert, dict):
                _candidate = str(_xau_last_alert.get("winner") or "").upper().strip()
                if _candidate in ("LONG", "SHORT"):
                    _xau_last_winner = _candidate

            _xau_effective_state = (
                xau_observer_gap_state
                if xau_observer_gap_state in ("LONG", "SHORT")
                else _xau_last_winner
            )

            long_hit = (
                signed_gap >= XAU_GAP_THRESHOLD
                and _xau_effective_state != "LONG"
            )
            short_hit = (
                signed_gap <= -XAU_GAP_THRESHOLD
                and _xau_effective_state != "SHORT"
            )
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
                    title = "XAU OBSERVER 13EX LONG WINS | 100K GAP"
                else:
                    winner = "SHORT"
                    xau_observer_gap_state = "SHORT"
                    title = "XAU OBSERVER 13EX SHORT WINS | 100K GAP"
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
                        f"{label}: L ${_usd_m(src_long)} | S ${_usd_m(src_short)}"
                    )

            exchange_lines = []
            exchange_labels = {
                "binance": "Binance", "bybit": "Bybit", "okx": "OKX",
                "hyperliquid": "Hyperliquid", "gate": "Gate", "htx": "HTX",
                "dydx": "dYdX", "bitmex": "BitMEX", "bitfinex": "Bitfinex",
                "bitget": "Bitget", "aster": "Aster", "coinex": "CoinEx",
                "lighter": "Lighter",
            }

            if asset == "BTC":
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
                    rank_amount = (ex_long if display_side == "long" else ex_short if display_side == "short" else max(ex_long, ex_short))
                    ranked.append((rank_amount, ex_name, ex_long, ex_short))
                ranked.sort(key=lambda row: row[0], reverse=True)
                for _, ex_name, ex_long, ex_short in ranked:
                    label = exchange_labels.get(ex_name, ex_name.title())
                    if display_side == "long":
                        exchange_lines.append(f"{label}: ${_usd_m(ex_long)}")
                    elif display_side == "short":
                        exchange_lines.append(f"{label}: ${_usd_m(ex_short)}")
                    else:
                        exchange_lines.append(f"{label}: L ${_usd_m(ex_long)} | S ${_usd_m(ex_short)}")

            elif asset == "XAU":
                audit_long = 0.0
                audit_short = 0.0
                for ex_name in XAU_OBSERVER_EXCHANGES:
                    ex_totals = combined_by_exchange["XAU"].get(
                        ex_name, {"long": 0.0, "short": 0.0}
                    )
                    ex_long = float(ex_totals.get("long", 0.0) or 0.0)
                    ex_short = float(ex_totals.get("short", 0.0) or 0.0)
                    audit_long += ex_long
                    audit_short += ex_short
                    label = exchange_labels.get(ex_name, ex_name.title())
                    exchange_lines.append(
                        f"{label}: L ${_usd_m(ex_long)} | S ${_usd_m(ex_short)}"
                    )
                exchange_lines.append("")
                exchange_lines.append(f"13EX SUM LONG: ${_usd_m(audit_long)}")
                exchange_lines.append(f"13EX SUM SHORT: ${_usd_m(audit_short)}")
                exchange_lines.append(f"TOTAL LONG: ${_usd_m(cycle_long)}")
                exchange_lines.append(f"TOTAL SHORT: ${_usd_m(cycle_short)}")
                audit_status = (
                    "MATCH"
                    if abs(audit_long - cycle_long) < 0.01
                    and abs(audit_short - cycle_short) < 0.01
                    else "MISMATCH"
                )
                exchange_lines.append(f"AUDIT: {audit_status}")

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
            if asset == "XAU":
                _gap_check_capture(
                    "xau_observer_normal",
                    "SHORT" if winner == "LONG" else "LONG",
                    winner, cycle_long, cycle_short,
                    XAU_GAP_THRESHOLD, alert_snapshot["ts"], title
                )

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

        if asset in ("BTC", "XAU") and alert_snapshot.get("exchanges"):
            breakdown = "\n".join(alert_snapshot["exchanges"])
            message = (
                f"{breakdown}\n"
                f"COMBINED SHORT: ${_usd_m(alert_snapshot['short'])}\n"
                f"COMBINED LONG: ${_usd_m(alert_snapshot['long'])}\n"
                f"GAP: ${_usd_m(alert_snapshot['gap'])}\n"
                f"{asset} {price_text} | {asset} MOVE {move_text}"
            )
        else:
            breakdown = "\n".join(alert_snapshot["sources"]) or "No source breakdown"
            message = (
                f"SOURCE COMBINED | WINNER {alert_snapshot['winner']} | "
                f"LONG ${_usd_m(alert_snapshot['long'])} ({alert_snapshot['long_pct']:.2f}%) | "
                f"SHORT ${_usd_m(alert_snapshot['short'])} ({alert_snapshot['short_pct']:.2f}%) | "
                f"GAP ${_usd_m(alert_snapshot['gap'])} | "
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
            f"L=${_usd_m(alert_snapshot['long'])} S=${_usd_m(alert_snapshot['short'])} "
            f"sent={sent}",
            flush=True,
        )

    else:
        print(
            f"[COMBINED {asset}] {source_key.upper()} "
            f"+L=${_usd_m(long_usd)} +S=${_usd_m(short_usd)} | "
            f"TOTAL L=${_usd_m(result['combined_long_usd'])} "
            f"S=${_usd_m(result['combined_short_usd'])}",
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

    # Do not wait indefinitely behind another MarginPad request. A long retry
    # chain in one poll must not hold a Gunicorn worker until its timeout.
    lock_acquired = _marginpad_request_lock.acquire(timeout=2.0)
    if not lock_acquired:
        return None, {
            "stage": stage,
            "error": "marginpad request busy; retry on next poll"
        }

    try:
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
    finally:
        _marginpad_request_lock.release()


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
    # GOLD FAMILY: combine MarginPad XAU + XAUT when each symbol is available.
    # A failure/unsupported response for one symbol does not discard the other.
    events = []
    source_errors = []

    for requested_symbol in ("XAU", "XAUT"):
        payload, error = marginpad_get(
            "/api/v1/liquidations/live",
            params={
                "symbol": requested_symbol,
                "limit": MARGINPAD_LIVE_LIMIT
            },
            timeout=12,
            stage=f"marginpad-{requested_symbol.lower()}-liquidations"
        )

        if error:
            source_errors.append({
                "symbol": requested_symbol,
                "error": error,
            })
            print(
                f"[MARGINPAD GOLD SOURCE SKIP] {requested_symbol} | {error}",
                flush=True,
            )
            continue

        symbol_events = extract_marginpad_events(payload)

        # Some MarginPad responses can also be flat:
        # {"symbol":"XAU","events":[...]} / {"symbol":"XAUT","events":[...]}.
        if not symbol_events and isinstance(payload, dict):
            value = payload.get("events")
            if isinstance(value, list):
                symbol_events = value

        if not isinstance(symbol_events, list):
            source_errors.append({
                "symbol": requested_symbol,
                "error": "events payload is not a list",
            })
            continue

        # If an upstream row omits symbol, stamp the requested symbol so
        # XAU and XAUT remain independently fingerprinted/auditable.
        for raw_event in symbol_events:
            if not isinstance(raw_event, dict):
                continue
            event = dict(raw_event)
            if not str(event.get("symbol", "")).strip():
                event["symbol"] = requested_symbol
            events.append(event)

    if not events and len(source_errors) >= 2:
        return None, {
            "stage": "marginpad-xau-xaut-liquidations",
            "error": "both XAU and XAUT sources unavailable",
            "sources": source_errors,
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
    fresh_by_exchange = {
        ex: {"long": 0.0, "short": 0.0}
        for ex in XAU_MARGINPAD_EXCHANGES
    }
    rolling_events = []
    normalized_events = []

    for event in events:
        if not isinstance(event, dict):
            continue

        event_symbol = str(
            event.get("symbol", "")
        ).strip().upper()

        # GOLD FAMILY only.
        if event_symbol and event_symbol not in ("XAU", "XAUT"):
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

        if side not in ("long_liquidated", "short_liquidated"):
            continue

        exchange = str(
            event.get("exchange", "")
        ).strip()
        exchange_key = _btc_exchange_key(exchange)

        if exchange_key not in XAU_MARGINPAD_EXCHANGES:
            print(
                "[XAU/XAUT 13EX EXCHANGE REJECT] "
                f"exchange={exchange or '-'} | "
                f"normalized={exchange_key or '-'} | "
                f"side={side} | "
                f"notional=${notional:,.2f}",
                flush=True,
            )
            continue

        if side == "long_liquidated":
            fresh_long += notional
            fresh_by_exchange[exchange_key]["long"] += notional
        else:
            fresh_short += notional
            fresh_by_exchange[exchange_key]["short"] += notional

        exchanges.add(exchange_key)

        remember_marginpad_xau_event(
            fingerprint
        )

        rolling_events.append({
            "ts_ms": event_ts_ms,
            "exchange": exchange_key,
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
        "MARGINPAD XAU+XAUT | "
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
        "fresh_by_exchange": {
            ex: {
                "long": round(fresh_by_exchange[ex]["long"], 2),
                "short": round(fresh_by_exchange[ex]["short"], 2),
            }
            for ex in XAU_MARGINPAD_EXCHANGES
        },
        "newest_event_ts_ms": newest_event_ms,
        "rolling_events": rolling_events,
        "closed_end_ms": closed_end_ms,
        "gold_symbols": ["XAU", "XAUT"],
        "source_errors": source_errors,
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
# ETH + SOL ROLLING 60M - BTC LOGIC CLONES
# ==================================================
ALT_ROLLING_ASSETS = ("ETH", "SOL")
ALT_ROLLING_GAP_THRESHOLD = 5_000_000.0
ALT_ROLLING_WINDOW_SECONDS = 3600
_alt_rolling_lock = threading.RLock()
alt_rolling = {a: {
    "coinalyze_events": deque(), "observer_events": deque(),
    "coinalyze_state": None, "observer_state": None,
    "coinalyze_state_ts": None, "observer_state_ts": None,
    "coinalyze_last_processed_ts": None, "marginpad_processed_through_ms": None,
    "marginpad_seen_queue": deque(), "marginpad_seen_set": set(),
    "direct_seen_queue": deque(), "direct_seen_set": set(),
} for a in ALT_ROLLING_ASSETS}

def _alt_price(asset):
    return get_coinalyze_price(f"{asset}USDT_PERP.A", f"{asset.lower()}-price")

def _alt_trim(events, now_ts):
    cutoff=float(now_ts)-ALT_ROLLING_WINDOW_SECONDS
    while events and float(events[0][0]) <= cutoff: events.popleft()

def _alt_totals(events):
    return (sum(float(r[2]) for r in events if r[1]=="long"), sum(float(r[2]) for r in events if r[1]=="short"))

def _rolling_exchange_breakdown_lines(events, total_signed_gap, stronger, exchange_order=None):
    """Display-only trailing-window exchange audit. Does not affect alert logic."""
    by_exchange = {}
    for row in events:
        if len(row) < 4:
            continue
        try:
            side = str(row[1]).lower()
            amount = float(row[2] or 0.0)
            ex = _btc_exchange_key(row[3])
        except (TypeError, ValueError):
            continue
        if side not in ("long", "short") or amount <= 0:
            continue
        ex = ex or "unknown"
        bucket = by_exchange.setdefault(ex, {"long": 0.0, "short": 0.0})
        bucket[side] += amount

    labels = {
        "binance": "Binance", "bybit": "Bybit", "okx": "OKX",
        "hyperliquid": "Hyperliquid", "gate": "Gate", "htx": "HTX",
        "dydx": "dYdX", "bitmex": "BitMEX", "bitfinex": "Bitfinex",
        "bitget": "Bitget", "aster": "Aster", "coinex": "CoinEx",
        "lighter": "Lighter", "coinalyze": "Coinalyze", "unknown": "Unknown",
    }
    if exchange_order:
        names = [x for x in exchange_order if x in by_exchange]
        names += [x for x in by_exchange if x not in names]
    else:
        names = list(by_exchange)

    # Rank by absolute exchange net GAP so the biggest contributors are easiest to see.
    names.sort(key=lambda x: abs(by_exchange[x]["long"] - by_exchange[x]["short"]), reverse=True)
    denom = abs(float(total_signed_gap or 0.0))
    lines = ["", "EXCHANGE BREAKDOWN — TRAILING 60M"]
    for ex in names:
        L = by_exchange[ex]["long"]; S = by_exchange[ex]["short"]
        signed = L - S
        ex_gap = abs(signed)
        ex_side = "LONG" if signed > 0 else "SHORT" if signed < 0 else "EVEN"
        aligned = signed if stronger == "LONG" else -signed
        pct = (aligned / denom * 100.0) if denom > 0 else 0.0
        label = labels.get(ex, ex.title())
        lines.append(f"{label}: L ${_usd_m(L)} | S ${_usd_m(S)} | GAP ${_usd_m(ex_gap)} {ex_side} | {pct:+.1f}%")
    lines.append(f"TOTAL GAP: ${_usd_m(denom)} {stronger} | 100.0%")
    return lines


def _alt_evaluate(asset, source, price=None, now_ts=None):
    asset=str(asset).upper(); source=str(source).lower(); now_ts=float(now_ts or time.time())
    s=alt_rolling[asset]
    with _alt_rolling_lock:
        ev=s[f"{source}_events"]; _alt_trim(ev,now_ts); L,S=_alt_totals(ev); gap_signed=L-S
        old=s[f"{source}_state"]; new=old
        if gap_signed >= ALT_ROLLING_GAP_THRESHOLD and old != "LONG": new="LONG"
        elif gap_signed <= -ALT_ROLLING_GAP_THRESHOLD and old != "SHORT": new="SHORT"
        if new==old: return None
        change=_format_accumulation_duration(s[f"{source}_state_ts"],now_ts)
        s[f"{source}_state"]=new; s[f"{source}_state_ts"]=now_ts
        try: px=f"{float(price):,.2f}" if price is not None else "NA"
        except (TypeError,ValueError): px="NA"
        title=(f"{asset} COINALYZE ROLLING 60M {new} | +5M GAP" if source=="coinalyze" else f"{asset} OBSERVER 13EX ROLLING 60M {new} | +5M GAP")
        msg=(f"WINDOW: EXACT TRAILING 60 MINUTES | NO RESET\nLONG: ${_usd_m(L)}\nSHORT: ${_usd_m(S)}\nGAP: ${_usd_m(abs(gap_signed))}\nSTRONGER: {new}\nSTATE: {old or 'NONE'} -> {new}\nCHANGE TIME: {change}\n{asset}: {px}")
        if asset == "ETH" and source == "observer":
            msg += "\n" + "\n".join(_rolling_exchange_breakdown_lines(
                ev, gap_signed, new, BTC_OBSERVER_EXCHANGES
            ))
        sent=send_pushover(title,msg)

        # Mirror a VALID ETH/SOL 13EX Observer +/-$5M rolling alert into the
        # read-only ALL COINS $5M+ no-reset ledger.  This does not change the
        # rolling calculation, state, threshold or Pushover logic.
        #
        # The market-wide ALL CRYPTO feed intentionally seeds old events on
        # startup/deploy, while the per-asset rolling observer can already have
        # enough trailing-60m history to fire.  In that case the alert existed
        # but the unusual ledger had no $5M-hit timestamp yet.
        if source == "observer":
            with _all_crypto_lock:
                unusual_bucket = all_crypto_unusual_by_symbol.setdefault(
                    asset, {"long": 0.0, "short": 0.0, "hit_5m_ts": None}
                )

                # Preserve any no-reset totals already collected by the ALL
                # CRYPTO feed.  If it started later than this rolling observer,
                # seed only the missing historical floor from the valid alert
                # snapshot instead of adding the snapshot again.
                unusual_bucket["long"] = max(
                    float(unusual_bucket.get("long", 0.0) or 0.0), float(L)
                )
                unusual_bucket["short"] = max(
                    float(unusual_bucket.get("short", 0.0) or 0.0), float(S)
                )

                if unusual_bucket.get("hit_5m_ts") is None:
                    unusual_bucket["hit_5m_ts"] = now_ts

        _gap_check_capture(
            f"{asset.lower()}_coinalyze_rolling" if source == "coinalyze" else f"{asset.lower()}_observer_rolling",
            old, new, L, S, ALT_ROLLING_GAP_THRESHOLD, now_ts, title
        )
        print(f"[{asset} ROLLING 60M] {source.upper()} {new} L=${_usd_m(L)} S=${_usd_m(S)} GAP=${_usd_m(abs(gap_signed))} sent={sent}",flush=True)
        return {"direction":new,"long":L,"short":S,"gap":abs(gap_signed),"alert_sent":bool(sent)}

def _alt_add(asset,source,side,amount,event_ts=None,exchange=None,price=None):
    asset=str(asset).upper(); source=str(source).lower(); side=str(side).lower()
    if asset not in ALT_ROLLING_ASSETS or source not in ("coinalyze","observer") or side not in ("long","short"): return None
    try: amount=float(amount or 0); ts=float(event_ts) if event_ts is not None else time.time()
    except (TypeError,ValueError): return None
    if amount<=0: return None
    if ts>10_000_000_000: ts/=1000.0
    with _alt_rolling_lock:
        ev=alt_rolling[asset][f"{source}_events"]; ev.append((ts,side,amount,str(exchange or "")))
        if len(ev)>1 and ev[-2][0]>ts:
            ordered=sorted(ev,key=lambda r:r[0]); ev.clear(); ev.extend(ordered)
    return _alt_evaluate(asset,source,price,max(time.time(),ts))

def _alt_add_coinalyze_row(asset,L,S,ts,exchange=None,price=None):
    try: L=max(0,float(L or 0)); S=max(0,float(S or 0)); ts=float(ts)
    except (TypeError,ValueError): return None
    if L>0: _alt_add(asset,"coinalyze","long",L,ts,exchange,price)
    if S>0: return _alt_add(asset,"coinalyze","short",S,ts,exchange,price)

def process_alt_coinalyze(asset,closed_minute_ts):
    asset=str(asset).upper(); s=alt_rolling[asset]; price,err=_alt_price(asset)
    if err: return {"ok":False,"asset":asset,"source":"Coinalyze","error":err}
    prev=s["coinalyze_last_processed_ts"]
    if prev is None: s["coinalyze_last_processed_ts"]=closed_minute_ts; return {"ok":True,"asset":asset,"source":"Coinalyze","initialized":True}
    if closed_minute_ts<=prev: return {"ok":True,"asset":asset,"source":"Coinalyze","new_closed_minute":False}
    fresh,err=get_fresh_liquidations(asset,prev,closed_minute_ts)
    if err: return {"ok":False,"asset":asset,"source":"Coinalyze","error":err}
    for r in fresh.get("rolling_rows",[]): _alt_add_coinalyze_row(asset,r.get("long",0),r.get("short",0),r.get("ts"),r.get("exchange"),price)
    s["coinalyze_last_processed_ts"]=closed_minute_ts
    return {"ok":True,"asset":asset,"source":"Coinalyze","fresh_long_usd":fresh.get("fresh_long_usd",0),"fresh_short_usd":fresh.get("fresh_short_usd",0)}

def process_alt_marginpad(asset,closed_minute_ts):
    asset=str(asset).upper(); s=alt_rolling[asset]
    pp,pe=marginpad_get("/api/v1/price",params={"symbol":asset},timeout=10,stage=f"marginpad-{asset.lower()}-price"); price=None
    if not pe:
        try: price=float((pp.get("data") or {}).get("price"))
        except (TypeError,ValueError,AttributeError): pass
    end=(closed_minute_ts+59)*1000+999; prev=s["marginpad_processed_through_ms"]; low=(end-MARGINPAD_OVERLAP_MS if prev is None else max(0,int(prev)-MARGINPAD_OVERLAP_MS))
    payload,err=marginpad_get("/api/v1/liquidations/live",params={"symbol":asset,"limit":MARGINPAD_LIVE_LIMIT},timeout=12,stage=f"marginpad-{asset.lower()}-liquidations")
    if err: return {"ok":False,"asset":asset,"source":"MarginPad","error":err}
    accepted=0
    for e in sorted(extract_marginpad_events(payload),key=lambda x:normalize_marginpad_ts_ms(x.get("ts")) or 0):
        if not isinstance(e,dict): continue
        ets=normalize_marginpad_ts_ms(e.get("ts"))
        if ets is None or ets>end or ets<=low: continue
        if str(e.get("symbol",asset)).upper().strip() not in ("",asset): continue
        ex=_btc_exchange_key(e.get("exchange",""))
        if ex not in BTC_OBSERVER_EXCHANGES[:9]: continue
        fp=marginpad_event_fingerprint(e)
        if fp in s["marginpad_seen_set"]: continue
        try: amt=abs(float(e.get("notional",0) or 0))
        except (TypeError,ValueError): continue
        raw=str(e.get("side","")).lower(); side="long" if raw=="long_liquidated" else "short" if raw=="short_liquidated" else None
        if not side or amt<=0: continue
        s["marginpad_seen_set"].add(fp); s["marginpad_seen_queue"].append(fp)
        while len(s["marginpad_seen_queue"])>MARGINPAD_SEEN_MAX: s["marginpad_seen_set"].discard(s["marginpad_seen_queue"].popleft())
        _alt_add(asset,"observer",side,amt,ets,ex,price); accepted+=1
    s["marginpad_processed_through_ms"]=end
    return {"ok":True,"asset":asset,"source":"MarginPad","events_accepted":accepted}


# ==================================================
# Core strategy logic is unchanged. This only preserves in-memory runtime
# state across Render deploys/restarts using the existing /var/data disk.

RUNTIME_STATE_FILE = os.path.join('/var/data', 'backend_runtime_state.json')
_runtime_state_lock = threading.Lock()


# Must be defined before _load_runtime_state() is called during module startup.
def _btc_exchange_key(name):
    key = str(name or "unknown").strip().lower() or "unknown"
    aliases = {
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


def _runtime_state_payload():
    return {
        'version': 1,
        'gap_check_last_alerts': gap_check_last_alerts,
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
            'coinalyze_state_ts': btc_coinalyze_rolling_state_ts,
            'observer_state_ts': btc_observer_rolling_state_ts,
            'coinalyze_events': list(btc_coinalyze_rolling_events),
            'observer_events': list(btc_observer_rolling_events),
        },

        'alt_crypto_rolling_60m': {a: {
            'coinalyze_state': alt_rolling[a]['coinalyze_state'], 'observer_state': alt_rolling[a]['observer_state'],
            'coinalyze_state_ts': alt_rolling[a]['coinalyze_state_ts'], 'observer_state_ts': alt_rolling[a]['observer_state_ts'],
            'coinalyze_last_processed_ts': alt_rolling[a]['coinalyze_last_processed_ts'], 'marginpad_processed_through_ms': alt_rolling[a]['marginpad_processed_through_ms'],
            'coinalyze_events': list(alt_rolling[a]['coinalyze_events']), 'observer_events': list(alt_rolling[a]['observer_events']),
            'marginpad_seen_queue': list(alt_rolling[a]['marginpad_seen_queue']), 'direct_seen_queue': list(alt_rolling[a]['direct_seen_queue']),
        } for a in ALT_ROLLING_ASSETS},

        'alt_gap_observer': {a: {
            'long': alt_gap_observer[a]['long'],
            'short': alt_gap_observer[a]['short'],
            'state': alt_gap_observer[a]['state'],
            'by_exchange': alt_gap_observer[a]['by_exchange'],
        } for a in ALT_GAP_ASSETS},

        'all_crypto_marginpad': {
            'long_cumulative': all_crypto_long_cumulative,
            'short_cumulative': all_crypto_short_cumulative,
            'gap_state': all_crypto_gap_state,
            'gap_state_ts': all_crypto_gap_state_ts,
            'cycle_start_ts': all_crypto_cycle_start_ts,
            'rolling_state': all_crypto_rolling_state,
            'rolling_state_ts': all_crypto_rolling_state_ts,
            'rolling_events': list(all_crypto_rolling_events),
            'seen_queue': list(all_crypto_seen_queue),
            'by_symbol': all_crypto_by_symbol,
            'unusual_by_symbol': all_crypto_unusual_by_symbol,
            'last_poll_ts': all_crypto_last_poll_ts,
            'crypto_symbols': sorted(all_crypto_crypto_symbols),
            'crypto_symbols_refreshed_ts': all_crypto_crypto_symbols_refreshed_ts,
            'first_poll_seeded': all_crypto_first_poll_seeded,
        },

        'coinalyze_xau': {
            'long_cumulative': xau_long_cumulative,
            'short_cumulative': xau_short_cumulative,
            'by_exchange': xau_coinalyze_by_exchange,
            'cycle_ref_price': xau_cycle_ref_price,
            'last_processed_liq_ts': xau_last_processed_liq_ts,
        },

        'xau_gap_direction_states': {
            'coinalyze': xau_coinalyze_gap_state,
            'observer': xau_observer_gap_state,
            'observer_state_epoch': XAU_NORMAL_OBSERVER_STATE_EPOCH,
        },
        'xau_rolling_60m': {
            'coinalyze_state': xau_coinalyze_rolling_state,
            'observer_state': xau_observer_rolling_state,
            'coinalyze_state_ts': xau_coinalyze_rolling_state_ts,
            'observer_state_ts': xau_observer_rolling_state_ts,
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
        # Unique temp file prevents concurrent Gunicorn/process saves from
        # racing on the same .tmp pathname during deploy/restart overlap.
        tmp_path = (
            RUNTIME_STATE_FILE
            + f'.{os.getpid()}.{threading.get_ident()}.tmp'
        )
        with _runtime_state_lock:
            try:
                with open(tmp_path, 'w', encoding='utf-8') as f:
                    json.dump(payload, f, separators=(',', ':'), sort_keys=True)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, RUNTIME_STATE_FILE)
            finally:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except OSError:
                    pass
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
    global btc_coinalyze_rolling_state_ts, btc_observer_rolling_state_ts
    global all_crypto_long_cumulative, all_crypto_short_cumulative
    global all_crypto_gap_state, all_crypto_gap_state_ts, all_crypto_rolling_events, all_crypto_rolling_state
    global all_crypto_rolling_state_ts
    global all_crypto_cycle_start_ts
    global all_crypto_seen_queue, all_crypto_seen_set, all_crypto_by_symbol
    global all_crypto_unusual_by_symbol
    global all_crypto_last_poll_ts, all_crypto_crypto_symbols
    global all_crypto_crypto_symbols_refreshed_ts, all_crypto_first_poll_seeded
    global xau_long_cumulative, xau_short_cumulative, xau_coinalyze_by_exchange
    global xau_cycle_ref_price, xau_last_processed_liq_ts
    global xau_coinalyze_gap_state, xau_observer_gap_state
    global xau_coinalyze_rolling_events, xau_observer_rolling_events
    global xau_coinalyze_rolling_state, xau_observer_rolling_state
    global xau_coinalyze_rolling_state_ts, xau_observer_rolling_state_ts
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

        saved_gap_check = data.get('gap_check_last_alerts') or {}
        if isinstance(saved_gap_check, dict):
            for _key in gap_check_last_alerts:
                _value = saved_gap_check.get(_key)
                gap_check_last_alerts[_key] = dict(_value) if isinstance(_value, dict) else None

        # Backward-compatible recovery for the XAU Observer normal alert:
        # combined_last_alert is restored below and remains the fallback in /gap-check.
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
        try:
            btc_coinalyze_rolling_state_ts = float(rolling.get('coinalyze_state_ts')) if rolling.get('coinalyze_state_ts') is not None else None
        except (TypeError, ValueError):
            btc_coinalyze_rolling_state_ts = None
        try:
            btc_observer_rolling_state_ts = float(rolling.get('observer_state_ts')) if rolling.get('observer_state_ts') is not None else None
        except (TypeError, ValueError):
            btc_observer_rolling_state_ts = None
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

        _alt_saved=data.get('alt_crypto_rolling_60m') or {}
        for _a in ALT_ROLLING_ASSETS:
            _src=_alt_saved.get(_a) or {}; _st=alt_rolling[_a]
            _st['coinalyze_state']=_src.get('coinalyze_state') if _src.get('coinalyze_state') in ('LONG','SHORT') else None
            _st['observer_state']=_src.get('observer_state') if _src.get('observer_state') in ('LONG','SHORT') else None
            for _k in ('coinalyze_state_ts','observer_state_ts','coinalyze_last_processed_ts','marginpad_processed_through_ms'):
                try: _st[_k]=float(_src.get(_k)) if _src.get(_k) is not None else None
                except (TypeError,ValueError): _st[_k]=None
            _st['coinalyze_events']=_restore_roll(_src.get('coinalyze_events')); _st['observer_events']=_restore_roll(_src.get('observer_events'))
            _seen=list(_src.get('marginpad_seen_queue') or [])[-MARGINPAD_SEEN_MAX:]; _st['marginpad_seen_queue']=deque(_seen); _st['marginpad_seen_set']=set(_seen)
            _dseen=list(_src.get('direct_seen_queue') or [])[-COMBINED_DIRECT_SEEN_MAX:]; _st['direct_seen_queue']=deque(_dseen); _st['direct_seen_set']=set(_dseen)

        _alt_gap_saved = data.get('alt_gap_observer') or {}
        for _asset in ALT_GAP_ASSETS:
            _src = _alt_gap_saved.get(_asset) or {}
            _st = alt_gap_observer[_asset]
            _st['long'] = float(_src.get('long', 0.0) or 0.0)
            _st['short'] = float(_src.get('short', 0.0) or 0.0)
            _st['state'] = _src.get('state') if _src.get('state') in ('LONG', 'SHORT') else None
            _st['by_exchange'] = {ex: {'long': 0.0, 'short': 0.0} for ex in ALL_CRYPTO_OBSERVER_EXCHANGES}
            for _ex, _totals in (_src.get('by_exchange') or {}).items():
                _key = _btc_exchange_key(_ex)
                if _key in _st['by_exchange'] and isinstance(_totals, dict):
                    _st['by_exchange'][_key] = {
                        'long': float(_totals.get('long', 0.0) or 0.0),
                        'short': float(_totals.get('short', 0.0) or 0.0),
                    }

        allc = data.get('all_crypto_marginpad') or {}
        all_crypto_long_cumulative = float(allc.get('long_cumulative', 0.0) or 0.0)
        all_crypto_short_cumulative = float(allc.get('short_cumulative', 0.0) or 0.0)
        all_crypto_gap_state = allc.get('gap_state') if allc.get('gap_state') in ('LONG','SHORT') else None
        try:
            all_crypto_gap_state_ts = float(allc.get('gap_state_ts')) if allc.get('gap_state_ts') is not None else None
        except (TypeError, ValueError):
            all_crypto_gap_state_ts = None
        _saved_all_crypto_cycle_start = allc.get('cycle_start_ts')
        try:
            all_crypto_cycle_start_ts = float(_saved_all_crypto_cycle_start) if _saved_all_crypto_cycle_start is not None else None
        except (TypeError, ValueError):
            all_crypto_cycle_start_ts = None
        # Backward compatibility for a cycle already in progress before this field existed.
        if all_crypto_cycle_start_ts is None and (all_crypto_long_cumulative > 0 or all_crypto_short_cumulative > 0):
            all_crypto_cycle_start_ts = time.time()
        all_crypto_rolling_state = allc.get('rolling_state') if allc.get('rolling_state') in ('LONG','SHORT') else None
        try:
            all_crypto_rolling_state_ts = float(allc.get('rolling_state_ts')) if allc.get('rolling_state_ts') is not None else None
        except (TypeError, ValueError):
            all_crypto_rolling_state_ts = None
        _all_cutoff = time.time() - ALL_CRYPTO_ROLLING_WINDOW_SECONDS
        _all_rows = deque()
        for row in (allc.get('rolling_events') or []):
            try:
                ts=float(row[0]); side=str(row[1]); amount=float(row[2]); symbol=str(row[3]) if len(row)>3 else ""; exchange=str(row[4]) if len(row)>4 else ""
                if ts > 10_000_000_000: ts /= 1000.0
                if ts > _all_cutoff and side in ('long','short') and amount > 0:
                    _all_rows.append((ts,side,amount,symbol,exchange))
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
        all_crypto_unusual_by_symbol = {}
        for sym, totals in (allc.get('unusual_by_symbol') or {}).items():
            if isinstance(totals, dict):
                try:
                    hit_ts = float(totals.get('hit_5m_ts')) if totals.get('hit_5m_ts') is not None else None
                except (TypeError, ValueError):
                    hit_ts = None
                all_crypto_unusual_by_symbol[str(sym).upper()] = {
                    'long': float(totals.get('long', 0.0) or 0.0),
                    'short': float(totals.get('short', 0.0) or 0.0),
                    'hit_5m_ts': hit_ts,
                }
        # One-time migration for installs where the unusual ledger was added
        # after accepted ALL-CRYPTO events already existed.  Use only the
        # persisted accepted rolling rows; after this bootstrap the ledger
        # remains independent/no-reset and is persisted normally.
        if not all_crypto_unusual_by_symbol and all_crypto_rolling_events:
            for _row in all_crypto_rolling_events:
                try:
                    _ts = float(_row[0])
                    _side = str(_row[1]).lower().strip()
                    _amount = float(_row[2])
                    _symbol = str(_row[3] if len(_row) > 3 else "UNKNOWN").upper().strip() or "UNKNOWN"
                except (TypeError, ValueError, IndexError):
                    continue
                if _side not in ('long', 'short') or _amount <= 0:
                    continue
                _bucket = all_crypto_unusual_by_symbol.setdefault(
                    _symbol, {'long': 0.0, 'short': 0.0, 'hit_5m_ts': None}
                )
                _bucket[_side] += _amount
                _signed = _bucket['long'] - _bucket['short']
                if _bucket['hit_5m_ts'] is None and abs(_signed) >= ALL_CRYPTO_UNUSUAL_GAP_THRESHOLD:
                    _bucket['hit_5m_ts'] = _ts
        all_crypto_last_poll_ts = allc.get('last_poll_ts')
        all_crypto_crypto_symbols = set(str(x).upper() for x in (allc.get('crypto_symbols') or []) if x)
        all_crypto_crypto_symbols_refreshed_ts = float(allc.get('crypto_symbols_refreshed_ts', 0.0) or 0.0)
        all_crypto_first_poll_seeded = bool(allc.get('first_poll_seeded', False))

        cxau = data.get('coinalyze_xau') or {}
        xau_long_cumulative = float(cxau.get('long_cumulative', 0.0) or 0.0)
        xau_short_cumulative = float(cxau.get('short_cumulative', 0.0) or 0.0)
        xau_coinalyze_by_exchange = {}
        for ex_name, ex_totals in (cxau.get('by_exchange') or {}).items():
            if isinstance(ex_totals, dict):
                xau_coinalyze_by_exchange[str(ex_name)] = {
                    'long': float(ex_totals.get('long', 0.0) or 0.0),
                    'short': float(ex_totals.get('short', 0.0) or 0.0),
                }
        xau_cycle_ref_price = cxau.get('cycle_ref_price')
        xau_last_processed_liq_ts = cxau.get('last_processed_liq_ts')

        xgap = data.get('xau_gap_direction_states') or {}
        xau_coinalyze_gap_state = xgap.get('coinalyze') if xgap.get('coinalyze') in ('LONG','SHORT') else None
        xau_observer_gap_state = xgap.get('observer') if xgap.get('observer') in ('LONG','SHORT') else None
        try:
            _saved_xau_observer_state_epoch = int(xgap.get('observer_state_epoch', 0) or 0)
        except (TypeError, ValueError):
            _saved_xau_observer_state_epoch = 0
        xroll = data.get('xau_rolling_60m') or {}
        xau_coinalyze_rolling_state = xroll.get('coinalyze_state') if xroll.get('coinalyze_state') in ('LONG','SHORT') else None
        xau_observer_rolling_state = xroll.get('observer_state') if xroll.get('observer_state') in ('LONG','SHORT') else None
        try:
            xau_coinalyze_rolling_state_ts = float(xroll.get('coinalyze_state_ts')) if xroll.get('coinalyze_state_ts') is not None else None
        except (TypeError, ValueError):
            xau_coinalyze_rolling_state_ts = None
        try:
            xau_observer_rolling_state_ts = float(xroll.get('observer_state_ts')) if xroll.get('observer_state_ts') is not None else None
        except (TypeError, ValueError):
            xau_observer_rolling_state_ts = None
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

            # XAU restore safety: the 13EX exchange audit is the canonical source
            # for the XAU Observer cycle. Older runtime-state files could contain
            # combined XAU totals without the matching per-exchange audit buckets.
            # On restore, rebuild XAU totals/source buckets from the persisted 13EX
            # audit so an unaudited legacy amount can never create TOTAL vs 13EX
            # SUM mismatches after a deploy/restart. Live accumulation is unchanged.
            if asset == "XAU":
                old_long = combined_liq[asset]["long"]
                old_short = combined_liq[asset]["short"]

                audited_long = 0.0
                audited_short = 0.0
                marginpad_long = 0.0
                marginpad_short = 0.0

                for ex_name in XAU_OBSERVER_EXCHANGES:
                    ex_totals = combined_by_exchange[asset].get(
                        ex_name, {"long": 0.0, "short": 0.0}
                    )
                    ex_long = float(ex_totals.get("long", 0.0) or 0.0)
                    ex_short = float(ex_totals.get("short", 0.0) or 0.0)
                    audited_long += ex_long
                    audited_short += ex_short
                    if ex_name in XAU_MARGINPAD_EXCHANGES:
                        marginpad_long += ex_long
                        marginpad_short += ex_short

                # Rebuild the source audit from the same exchange-level truth.
                combined_by_source[asset]["marginpad"]["long"] = marginpad_long
                combined_by_source[asset]["marginpad"]["short"] = marginpad_short
                for direct_name in COMBINED_DIRECT_EXCHANGES:
                    direct_totals = combined_by_exchange[asset].get(
                        direct_name, {"long": 0.0, "short": 0.0}
                    )
                    combined_by_source[asset][direct_name]["long"] = float(
                        direct_totals.get("long", 0.0) or 0.0
                    )
                    combined_by_source[asset][direct_name]["short"] = float(
                        direct_totals.get("short", 0.0) or 0.0
                    )

                combined_liq[asset]["long"] = audited_long
                combined_liq[asset]["short"] = audited_short
                marginpad_xau_long_cumulative = marginpad_long
                marginpad_xau_short_cumulative = marginpad_short

                if (
                    abs(old_long - audited_long) >= 0.01
                    or abs(old_short - audited_short) >= 0.01
                ):
                    print(
                        "[XAU RESTORE AUDIT RECONCILED] "
                        f"saved L=${old_long:,.2f} S=${old_short:,.2f} -> "
                        f"13EX L=${audited_long:,.2f} S=${audited_short:,.2f}",
                        flush=True,
                    )

        saved_ref = comb.get('cycle_ref_price') or {}
        saved_latest = comb.get('latest_price') or {}
        saved_last_alert = comb.get('last_alert') or {}
        for asset in ("BTC", "XAU"):
            combined_cycle_ref_price[asset] = saved_ref.get(asset)
            combined_latest_price[asset] = saved_latest.get(asset)
            combined_last_alert[asset] = saved_last_alert.get(asset)

        # One-time XAU normal-observer migration:
        # old deployments could preserve a stale LONG/SHORT dedupe lock forever.
        # Clear ONLY that normal XAU lock once. Totals, 13EX audit, rolling-60m,
        # BTC and NQ state are untouched. After the first new normal XAU alert,
        # reverse-only dedupe works normally and the epoch prevents future resets.
        if _saved_xau_observer_state_epoch < XAU_NORMAL_OBSERVER_STATE_EPOCH:
            xau_observer_gap_state = None
            combined_last_alert["XAU"] = None
            print(
                f"[XAU NORMAL STATE MIGRATION] epoch "
                f"{_saved_xau_observer_state_epoch}->{XAU_NORMAL_OBSERVER_STATE_EPOCH} "
                "| cleared stale normal XAU direction lock only",
                flush=True,
            )

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
        'combined_xau': {
            'long': combined_liq['XAU']['long'],
            'short': combined_liq['XAU']['short'],
            'gap': combined_liq['XAU']['long'] - combined_liq['XAU']['short'],
            'by_source': combined_by_source['XAU'],
            'by_exchange': combined_by_exchange['XAU'],
            'audit_13ex_long': sum(
                float((combined_by_exchange['XAU'].get(ex) or {}).get('long', 0.0) or 0.0)
                for ex in XAU_OBSERVER_EXCHANGES
            ),
            'audit_13ex_short': sum(
                float((combined_by_exchange['XAU'].get(ex) or {}).get('short', 0.0) or 0.0)
                for ex in XAU_OBSERVER_EXCHANGES
            ),
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
# NASDAQ QQQ-WEIGHTED PUSHOVER AUDIT SPLITTER
# ==================================================
# TradingView can send a longer audit than Pushover's 1024-character
# message limit. For the QQQ-weighted LAST-4 alert only, preserve the
# complete 10-stock audit and split it into TWO Pushover messages:
#   PART 1/2 = trigger metadata + first 5 stock rows
#   PART 2/2 = remaining 5 stock rows + final basket/NQ summary
# All Pine calculations, trigger logic, state, threshold and trading logic
# remain untouched here. This is display-only.

def _qqq_weighted_audit_pushover_parts(title, message):
    title_u = str(title or "").upper()
    text = str(message or "")

    # Every other TradingView alert remains exactly one message.
    if "NASDAQ 10-STOCK" not in title_u or "QQQ WEIGHTED" not in title_u:
        return [(str(title or ""), text)]

    stock_names = {
        "NVDA", "AAPL", "MSFT", "MU", "AMZN",
        "AMD", "GOOGL", "META", "GOOG", "TSLA"
    }

    normalized = text.replace("\r", "").replace("\n", " | ")
    tokens = [part.strip() for part in normalized.split("|") if part.strip()]

    metadata = []
    wanted_prefixes = (
        "TRIGGER #",
        "TRIGGER BASE:",
        "WEIGHTED BASKET NET:",
        "STATE:",
        "BASE 15:30 ENTRY CACHE:",
        "POSITION AFTER ALERT:",
    )
    for token in tokens:
        if token.upper().startswith(wanted_prefixes):
            if token not in metadata:
                metadata.append(token)

    stock_rows = []
    i = 0
    while i < len(tokens):
        symbol = tokens[i].upper()
        if symbol not in stock_names:
            i += 1
            continue

        vals = {}
        j = i + 1
        while j < len(tokens) and tokens[j].upper() not in stock_names:
            p = tokens[j]
            up = p.upper()

            if up.startswith("OPEN "):
                vals["O"] = p[5:].strip()
            elif up.startswith("LIVE "):
                vals["T"] = p[5:].strip()
            elif up.startswith("TRIGGER "):
                vals["T"] = p[8:].strip()
            elif up.startswith("RAW "):
                vals["R"] = p[4:].strip()
            elif up.startswith("W "):
                vals["W"] = p[2:].strip()
            elif up.startswith("CONTR "):
                vals["C"] = p[6:].strip()
                j += 1
                break
            j += 1

        if "O" in vals and "T" in vals:
            row = f"{symbol} | O {vals['O']} | T {vals['T']}"
            if "R" in vals:
                row += f" | R {vals['R']}"
            if "W" in vals:
                row += f" | W {vals['W']}"
            if "C" in vals:
                row += f" | C {vals['C']}"
            stock_rows.append(row)

        i = max(j, i + 1)

    summary = []
    for token in tokens:
        u = token.upper()
        if (
            u.startswith("WEIGHTED NET =")
            or u.startswith("NQ AT TRIGGER:")
        ):
            if token not in summary:
                summary.append(token)

    # Preserve source order exactly; only split after row 5.
    first_rows = stock_rows[:5]
    second_rows = stock_rows[5:]

    part1_lines = []
    part1_lines.extend(metadata)
    part1_lines.append("AUDIT 1/2: O=OPEN | T=LIVE | R=RAW | W=WEIGHT | C=CONTR")
    part1_lines.extend(first_rows)

    part2_lines = [
        "AUDIT 2/2: O=OPEN | T=LIVE | R=RAW | W=WEIGHT | C=CONTR"
    ]
    part2_lines.extend(second_rows)
    part2_lines.extend(summary)

    part1 = "\n".join(part1_lines).strip()
    part2 = "\n".join(part2_lines).strip()

    # Defensive protection only. With 5 rows per part both messages should
    # normally be comfortably below Pushover's 1024-character message limit.
    if len(part1) > 1024:
        part1 = part1[:1024]
    if len(part2) > 1024:
        part2 = part2[:1024]

    return [
        (f"{title} | PART 1/2", part1 or text),
        (f"{title} | PART 2/2", part2 or text),
    ]



# ==================================================
# NQ / NASDAQ LONG PUSHOVER SPLITTER
# ==================================================
# Display-only protection for long TradingView NQ/NASDAQ alerts.
# Existing QQQ-weighted audit splitting remains unchanged.
# If an NQ/NASDAQ TradingView message is too large for one Pushover message,
# split it into exactly TWO messages at a line boundary.
# No calculation, threshold, state, TradingView alert logic or MT5 logic changes.

def _nq_long_pushover_parts(title, message):
    title_text = str(title or "")
    message_text = str(message or "")
    title_u = title_text.upper()

    # Only NQ/NASDAQ-family TradingView alerts are eligible.
    if "NQ" not in title_u and "NASDAQ" not in title_u:
        return [(title_text, message_text)]

    # Short messages remain exactly one notification.
    if len(message_text) <= 950:
        return [(title_text, message_text)]

    normalized = message_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")

    # Choose a line-boundary split closest to the middle while keeping
    # both parts comfortably below Pushover's 1024-character message limit.
    best = None
    for cut in range(1, len(lines)):
        p1 = "\n".join(lines[:cut]).strip()
        p2 = "\n".join(lines[cut:]).strip()
        if len(p1) <= 950 and len(p2) <= 950:
            score = abs(len(p1) - len(p2))
            if best is None or score < best[0]:
                best = (score, p1, p2)

    if best is not None:
        _, part1, part2 = best
    else:
        # Defensive fallback for a message containing very long single lines.
        midpoint = len(normalized) // 2
        left_break = normalized.rfind("\n", 0, midpoint + 1)
        right_break = normalized.find("\n", midpoint)

        if left_break > 0:
            cut = left_break
        elif right_break != -1:
            cut = right_break
        else:
            cut = midpoint

        part1 = normalized[:cut].strip()
        part2 = normalized[cut:].strip()

        # Final defensive cap only; normal structured TV messages should
        # always split at line boundaries above without reaching this path.
        part1 = part1[:1024]
        part2 = part2[:1024]

    return [
        (f"{title_text} | PART 1/2", part1),
        (f"{title_text} | PART 2/2", part2),
    ]


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

        # Keep normal TradingView alerts unchanged. For the QQQ-weighted
        # LAST-4 audit only, split the complete 10-stock audit into two
        # Pushover messages so no stock row is lost to the 1024-char limit.
        pushover_parts = _qqq_weighted_audit_pushover_parts(
            tv_title,
            tv_message
        )

        # QQQ weighted audit already has its own exact 5+5 stock split.
        # For every other long NQ/NASDAQ TradingView message, apply the
        # generic two-part display splitter so the full alert reaches Pushover.
        if len(pushover_parts) == 1:
            pushover_parts = _nq_long_pushover_parts(
                tv_title,
                tv_message
            )

        def _send_pushover_parts_background(parts):
            try:
                for part_title, part_message in parts:
                    send_pushover(
                        part_title,
                        part_message
                    )
            except Exception as e:
                print(
                    f"[PUSHOVER BACKGROUND ERROR] {e}",
                    flush=True
                )

        threading.Thread(
            target=_send_pushover_parts_background,
            args=(list(pushover_parts),),
            daemon=True
        ).start()

        pushover_results = ["queued"] * len(pushover_parts)

        ok = all(pushover_results)

        # OLD NASDAQ TOP5/BOTTOM5 combined confirmation is disabled.
        # The current QQQ-weighted Rolling Last-4 TradingView alert above
        # still goes directly to Pushover, including its existing 5+5 split.
        nasdaq_result = {
            "handled": False,
            "disabled": True,
            "reason": "old_nasdaq_top5_bottom5_off"
        }

        return jsonify({
            "ok": ok,
            "mode": "direct_pushover",
            "latest_qqq_last4_enabled": LATEST_QQQ_LAST4_ENABLED,
            "nasdaq_combined": nasdaq_result
        }), 200 if ok else 500

    symbol = str(
        data.get(
            "symbol",
            ""
        )
    ).upper()

    if (
        not OLD_NQ_ES_DELTA_ENABLED
        and symbol in ("NQ", "ES", "JPN")
    ):
        print(
            f"[OLD NQ+ES OFF] ignored structured webhook | symbol={symbol}",
            flush=True
        )
        return jsonify({
            "ok": True,
            "ignored": True,
            "reason": "old_nq_es_delta_off",
            "symbol": symbol
        }), 200

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
    global coinalyze_symbol_exchange_cache

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

        asset_match = (
            base_asset == asset
            or (
                asset == "XAU"
                and base_asset == "XAUT"
            )
        )

        if (
            asset_match
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
                # Read-only audit mapping: Coinalyze contract -> exchange.
                # This does not change which symbols/totals are processed.
                exchange_name = str(market.get("exchange", "") or "").strip()
                if exchange_name:
                    coinalyze_symbol_exchange_cache[str(symbol)] = exchange_name

            if (
                asset == "XAU"
                and
                base_asset == "XAU"
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

            coinalyze_symbol = str(symbol_data.get("symbol", "") or "").strip()
            coinalyze_exchange = str(
                coinalyze_symbol_exchange_cache.get(coinalyze_symbol, "") or ""
            ).strip()
            if not coinalyze_exchange:
                # Defensive fallback: keep the contract visible instead of
                # silently losing its contribution from the exchange audit.
                coinalyze_exchange = coinalyze_symbol or "Unknown"

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
                    # Preserve exchange identity for rolling-window audit.
                    # Totals above remain exactly unchanged.
                    rb_key = (row_ts, coinalyze_exchange)
                    rb = fresh_rows.setdefault(rb_key, {"long": 0.0, "short": 0.0})
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
            {
                "ts": ts,
                "exchange": exchange,
                "long": round(v["long"], 2),
                "short": round(v["short"], 2),
            }
            for (ts, exchange), v in sorted(fresh_rows.items())
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
        f"LONG ${_usd_m(ref_long)} ({ref_long_pct:.2f}%) | "
        f"SHORT ${_usd_m(ref_short)} ({ref_short_pct:.2f}%) | "
        f"GAP ${_usd_m(ref_gap)}"
    )


def process_btc(
    closed_minute_ts
):

    global btc_long_cumulative
    global btc_short_cumulative
    global btc_cycle_ref_price
    global btc_last_processed_liq_ts
    global btc_last_alert_snapshot
    global btc_coinalyze_gap_state

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
            rr.get("long", 0.0), rr.get("short", 0.0), rr.get("ts"),
            exchange=rr.get("exchange"), price=btc_price
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

        _xau_previous_gap_state = "SHORT" if cycle_winner == "LONG" else "LONG"
        alert_sent = send_pushover(
            alert_title,
            (
                f"SOURCE COINALYZE | "
                f"WINNER "
                f"{cycle_winner} | "
                f"LONG "
                f"${_usd_m(cycle_long)} "
                f"({long_pct:.2f}%) | "
                f"SHORT "
                f"${_usd_m(cycle_short)} "
                f"({short_pct:.2f}%) | "
                f"GAP "
                f"${_usd_m(cycle_gap)} | "
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
            lines.append(f"{label}: ${_usd_m(ex_long)}")
        elif display_side == "short":
            lines.append(f"{label}: ${_usd_m(ex_short)}")
        else:
            lines.append(f"{label}: L ${_usd_m(ex_long)} | S ${_usd_m(ex_short)}")
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


def _gap_check_capture(key, previous_state, new_state, long_total, short_total, threshold, ts=None, title=None):
    """Save a display-only frozen copy of an already-triggered alert."""
    try:
        long_total = float(long_total or 0.0)
        short_total = float(short_total or 0.0)
        ts = float(ts) if ts is not None else time.time()
    except (TypeError, ValueError):
        return
    signed_gap = long_total - short_total
    gap_check_last_alerts[key] = {
        "ts": ts,
        "previous_state": previous_state or "NONE",
        "state": new_state or "NONE",
        "long": long_total,
        "short": short_total,
        "signed_gap": signed_gap,
        "gap": abs(signed_gap),
        "threshold": float(threshold),
        "title": str(title or ""),
    }


def _all_crypto_send_rolling_if_flip(now_ts=None):
    global all_crypto_rolling_state, all_crypto_rolling_state_ts
    now_ts = float(now_ts) if now_ts is not None else time.time()
    _all_crypto_rolling_trim(now_ts)
    long_total, short_total = _all_crypto_rolling_totals()
    rolling_total = long_total + short_total
    long_pct = (long_total / rolling_total * 100.0) if rolling_total > 0 else 0.0
    short_pct = (short_total / rolling_total * 100.0) if rolling_total > 0 else 0.0
    signed_gap = long_total - short_total
    state = all_crypto_rolling_state
    new_state = state
    if signed_gap >= ALL_CRYPTO_ROLLING_GAP_THRESHOLD and state != "LONG":
        new_state = "LONG"
    elif signed_gap <= -ALL_CRYPTO_ROLLING_GAP_THRESHOLD and state != "SHORT":
        new_state = "SHORT"
    if new_state == state:
        return False
    change_time = _format_accumulation_duration(all_crypto_rolling_state_ts, now_ts)
    all_crypto_rolling_state = new_state
    all_crypto_rolling_state_ts = now_ts
    gap = abs(signed_gap)

    # Display-only exchange audit for the SAME exact trailing 60-minute rows.
    # Trigger, threshold, reverse-only state and NO RESET behavior are unchanged.
    by_exchange = {
        ex: {"long": 0.0, "short": 0.0}
        for ex in ALL_CRYPTO_OBSERVER_EXCHANGES
    }
    legacy_unknown = {"long": 0.0, "short": 0.0}
    for row in all_crypto_rolling_events:
        try:
            side = str(row[1]).lower().strip()
            amount = float(row[2])
            exchange = _btc_exchange_key(row[4]) if len(row) > 4 and row[4] else ""
        except (TypeError, ValueError, IndexError):
            continue
        if side not in ("long", "short") or amount <= 0:
            continue
        if exchange in by_exchange:
            by_exchange[exchange][side] += amount
        else:
            # Backward compatibility: rows persisted before exchange was stored.
            legacy_unknown[side] += amount

    exchange_labels = {
        "binance": "Binance", "bybit": "Bybit", "okx": "OKX",
        "hyperliquid": "Hyperliquid", "gate": "Gate", "htx": "HTX",
        "dydx": "dYdX", "bitmex": "BitMEX", "bitfinex": "Bitfinex",
        "bitget": "Bitget", "aster": "Aster", "coinex": "CoinEx",
        "lighter": "Lighter",
    }
    exchange_lines = []
    for ex in ALL_CRYPTO_OBSERVER_EXCHANGES:
        ex_long = by_exchange[ex]["long"]
        ex_short = by_exchange[ex]["short"]
        if ex_long > 0 or ex_short > 0:
            ex_signed_gap = ex_long - ex_short
            ex_gap = abs(ex_signed_gap)
            ex_stronger = "L" if ex_signed_gap > 0 else "S" if ex_signed_gap < 0 else "EVEN"
            exchange_lines.append(
                f"{exchange_labels.get(ex, ex.title())}: "
                f"L ${_usd_m(ex_long)} | S ${_usd_m(ex_short)} | "
                f"GAP ${_usd_m(ex_gap)} {ex_stronger}"
            )
    if legacy_unknown["long"] > 0 or legacy_unknown["short"] > 0:
        legacy_signed_gap = legacy_unknown["long"] - legacy_unknown["short"]
        legacy_gap = abs(legacy_signed_gap)
        legacy_stronger = "L" if legacy_signed_gap > 0 else "S" if legacy_signed_gap < 0 else "EVEN"
        exchange_lines.append(
            f"Legacy/Unknown: L ${_usd_m(legacy_unknown['long'])} | "
            f"S ${_usd_m(legacy_unknown['short'])} | "
            f"GAP ${_usd_m(legacy_gap)} {legacy_stronger}"
        )

    title = f"ALL CRYPTO 13EX ROLLING 60M {new_state} | 5M GAP"
    _gap_check_capture(
        "all_crypto_rolling", state, new_state, long_total, short_total,
        ALL_CRYPTO_ROLLING_GAP_THRESHOLD, now_ts, title
    )
    message = (
        "13EX: MARGINPAD 9 + DIRECT 4 | EXACT TRAILING 60 MINUTES | NO RESET\n"
        f"LONG: ${_usd_m(long_total)} ({long_pct:.2f}%)\n"
        f"SHORT: ${_usd_m(short_total)} ({short_pct:.2f}%)\n"
        f"GAP: ${_usd_m(gap)}\n"
        f"STRONGER: {new_state}\n"
        f"STATE: {state or 'NONE'} -> {new_state}\n"
        f"CHANGE TIME: {change_time}"
    )
    if exchange_lines:
        message += "\nEXCHANGE BREAKDOWN (LAST 60M):\n" + "\n".join(exchange_lines)
    # Pushover intentionally disabled for ALL CRYPTO 13EX Rolling 60M.
    # Keep rolling calculations/state/data collection unchanged for gap-check/audit.
    sent = False
    print(f"[ALL CRYPTO ROLLING ALERT SILENT] {title} sent={sent}", flush=True)
    return False


def _alt_gap_observer_add(asset, exchange, side, amount):
    """ETH/SOL dedicated 13EX cumulative GAP observer; reverse-only, reset after alert."""
    asset = str(asset or "").upper().strip()
    exchange = _btc_exchange_key(exchange)
    side = str(side or "").lower().strip()
    if asset not in ALT_GAP_ASSETS or exchange not in ALL_CRYPTO_OBSERVER_EXCHANGES or side not in ("long", "short"):
        return False
    try:
        amount = abs(float(amount or 0.0))
    except (TypeError, ValueError):
        return False
    if amount <= 0:
        return False

    st = alt_gap_observer[asset]
    st[side] += amount
    bucket = st["by_exchange"].setdefault(exchange, {"long": 0.0, "short": 0.0})
    bucket[side] += amount

    signed_gap = st["long"] - st["short"]
    old_state = st["state"]
    new_state = old_state
    if signed_gap >= BTC_GAP_THRESHOLD and old_state != "LONG":
        new_state = "LONG"
    elif signed_gap <= -BTC_GAP_THRESHOLD and old_state != "SHORT":
        new_state = "SHORT"
    if new_state == old_state:
        return False

    gap = abs(signed_gap)
    ranked = []
    for ex in ALL_CRYPTO_OBSERVER_EXCHANGES:
        totals = st["by_exchange"].get(ex, {"long": 0.0, "short": 0.0})
        ex_long = float(totals.get("long", 0.0) or 0.0)
        ex_short = float(totals.get("short", 0.0) or 0.0)
        ranked.append((max(ex_long, ex_short), ex, ex_long, ex_short))
    ranked.sort(key=lambda row: row[0], reverse=True)
    lines = [
        f"{_btc_exchange_label(ex)}: LONG ${_usd_m(ex_long)} | SHORT ${_usd_m(ex_short)}"
        for _, ex, ex_long, ex_short in ranked
    ]
    title = f"{asset} OBSERVER {new_state} WINS | +5M GAP"
    message = (
        "\n".join(lines)
        + f"\nTOTAL SHORT: ${_usd_m(st['short'])}"
        + f"\nTOTAL LONG: ${_usd_m(st['long'])}"
        + f"\nGAP: ${_usd_m(gap)}"
        + f"\nSTATE: {old_state or 'NONE'} -> {new_state}"
    )
    sent = send_pushover(title, message)
    print(f"[{asset} OBSERVER ALERT] {title} L=${_usd_m(st['long'])} S=${_usd_m(st['short'])} sent={sent}", flush=True)

    # Same behavior as BTC Observer: preserve direction state, reset only cumulative cycle totals.
    st["state"] = new_state
    st["long"] = 0.0
    st["short"] = 0.0
    st["by_exchange"] = {ex: {"long": 0.0, "short": 0.0} for ex in ALL_CRYPTO_OBSERVER_EXCHANGES}
    return bool(sent)


def _all_crypto_unusual_add(symbol, side, amount, event_ts):
    """No-reset, no-alert per-symbol ledger fed only by accepted 13EX events."""
    symbol = str(symbol or "UNKNOWN").upper().strip() or "UNKNOWN"
    side = str(side or "").lower().strip()
    if side not in ("long", "short"):
        return
    try:
        amount = float(amount or 0.0)
        event_ts = float(event_ts or time.time())
    except (TypeError, ValueError):
        return
    if amount <= 0:
        return

    bucket = all_crypto_unusual_by_symbol.setdefault(
        symbol, {"long": 0.0, "short": 0.0, "hit_5m_ts": None}
    )
    bucket[side] = float(bucket.get(side, 0.0) or 0.0) + amount
    signed_gap = float(bucket.get("long", 0.0) or 0.0) - float(bucket.get("short", 0.0) or 0.0)
    if bucket.get("hit_5m_ts") is None and abs(signed_gap) >= ALL_CRYPTO_UNUSUAL_GAP_THRESHOLD:
        bucket["hit_5m_ts"] = event_ts


def _all_crypto_send_reset_if_flip():
    global all_crypto_long_cumulative, all_crypto_short_cumulative
    global all_crypto_gap_state, all_crypto_gap_state_ts, all_crypto_by_symbol
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
    top_lines = [f"{sym}: ${_usd_m(amount)}" for amount, sym in ranked[:8]]
    title = f"ALL CRYPTO 13EX {new_state} WINS | 5M GAP"
    message = (
        "MARKET-WIDE CRYPTO 13EX | MARGINPAD 9 + DIRECT 4 | RESET AFTER VALID FLIP\n"
        f"LONG: ${_usd_m(all_crypto_long_cumulative)}\n"
        f"SHORT: ${_usd_m(all_crypto_short_cumulative)}\n"
        f"GAP: ${_usd_m(gap)}\n"
        f"ACCUMULATION TIME: {accumulation_time}\n"
        f"STRONGER: {new_state}\n"
        f"STATE: {state or 'NONE'} -> {new_state}"
    )
    if top_lines:
        message += "\nTOP CONTRIBUTORS:\n" + "\n".join(top_lines)
    sent = send_pushover(title, message)
    _gap_check_capture(
        "all_crypto_normal", state, new_state,
        all_crypto_long_cumulative, all_crypto_short_cumulative,
        ALL_CRYPTO_GAP_THRESHOLD, signal_ts, title
    )
    print(f"[ALL CRYPTO GAP ALERT] {title} accumulation={accumulation_time} sent={sent}", flush=True)
    all_crypto_gap_state = new_state
    all_crypto_gap_state_ts = signal_ts
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
            # Repair/bootstrap for the read-only unusual-strength ledger.
            # Older deployments could mark the current MarginPad feed as SEEN
            # before those events were ever copied into all_crypto_unusual_by_symbol.
            # If the ledger is completely empty, rebuild it ONCE from the current
            # valid MarginPad 9-exchange feed.  This does NOT touch the normal
            # ALL-CRYPTO cumulative totals, rolling-60m state, alerts or seen cache.
            if not all_crypto_unusual_by_symbol:
                unusual_seeded = 0
                unusual_seed_seen = set()
                for seed_ts_ms, seed_event in normalized:
                    seed_fp = _all_crypto_event_fingerprint(seed_event)
                    if seed_fp in unusual_seed_seen:
                        continue
                    unusual_seed_seen.add(seed_fp)

                    seed_exchange = _btc_exchange_key(seed_event.get("exchange", ""))
                    if seed_exchange not in ALL_CRYPTO_MARGINPAD_EXCHANGES:
                        continue

                    seed_symbol = str(seed_event.get("symbol", "")).upper().strip()
                    if seed_symbol in {"XAU", "XAUT", "XAG", "GOLD", "SILVER", "NQ", "ES", "SPX", "SP500"}:
                        continue

                    try:
                        seed_notional = abs(float(seed_event.get("notional", 0.0) or 0.0))
                    except (TypeError, ValueError):
                        continue

                    seed_side_raw = str(seed_event.get("side", "")).lower().strip()
                    if seed_notional <= 0 or seed_side_raw not in ("long_liquidated", "short_liquidated"):
                        continue

                    seed_side = "long" if seed_side_raw == "long_liquidated" else "short"
                    _all_crypto_unusual_add(
                        seed_symbol or "UNKNOWN",
                        seed_side,
                        seed_notional,
                        seed_ts_ms / 1000.0,
                    )
                    unusual_seeded += 1

                print(
                    f"[ALL CRYPTO UNUSUAL BOOTSTRAP] events={unusual_seeded} | "
                    f"symbols={len(all_crypto_unusual_by_symbol)}",
                    flush=True,
                )

            for ts_ms, event in normalized:
                fp = _all_crypto_event_fingerprint(event)
                if fp in all_crypto_seen_set:
                    continue
                exchange_key = _btc_exchange_key(event.get("exchange", ""))
                if exchange_key not in ALL_CRYPTO_MARGINPAD_EXCHANGES:
                    _all_crypto_remember(fp)
                    print(
                        f"[ALL CRYPTO MARGINPAD EXCHANGE REJECT] exchange={event.get('exchange','')} normalized={exchange_key}",
                        flush=True,
                    )
                    continue
                symbol = str(event.get("symbol", "")).upper().strip()
                # Hard reject known non-crypto instruments; then require crypto universe membership.
                if symbol in {"XAU", "XAUT", "XAG", "GOLD", "SILVER", "NQ", "ES", "SPX", "SP500"}:
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
                _all_crypto_unusual_add(symbol or "UNKNOWN", side, notional, ts_sec)
                if symbol in ALT_GAP_ASSETS:
                    _alt_gap_observer_add(symbol, exchange_key, side, notional)
                all_crypto_rolling_events.append((ts_sec, side, notional, symbol, exchange_key))
                accepted += 1
                print(
                    f"[ALL CRYPTO MARGINPAD ACCEPTED] ts_ms={ts_ms} symbol={symbol or '-'} "
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



def add_all_crypto_direct_event(exchange, symbol, side, amount, event_key, event_ts=None, verified_crypto=False):
    """Add one Direct-4 crypto liquidation to the ALL Crypto 13EX state only."""
    global all_crypto_long_cumulative, all_crypto_short_cumulative
    global all_crypto_rolling_events, all_crypto_cycle_start_ts
    global all_crypto_last_poll_ts, all_crypto_last_error

    exchange = str(exchange or "").lower().strip()
    symbol = str(symbol or "").upper().strip()
    side = str(side or "").lower().strip()
    event_key = str(event_key or "").strip()

    if exchange not in ALL_CRYPTO_DIRECT_EXCHANGES:
        return {"ok": False, "error": "invalid_all_crypto_exchange"}
    if not symbol or symbol in {"XAU", "XAUT", "XAG", "GOLD", "SILVER", "NQ", "ES", "SPX", "SP500"}:
        return {"ok": False, "error": "invalid_all_crypto_symbol"}

    # CRYPTO-ONLY SAFETY GATE:
    # Bitget/Aster/CoinEx ALL events come from crypto-futures-specific venue paths
    # and are explicitly marked verified_crypto by the worker. This avoids false
    # negatives for genuine coins (e.g. UNI/ZEC/G/CROSS) that may be missing from
    # the cached MarginPad market universe.
    #
    # Lighter is a mixed-asset venue, so it is NOT trusted by symbol alone and must
    # still exist in the MarginPad crypto universe before entering the $5M totals.
    verified_crypto = bool(verified_crypto)
    if not verified_crypto:
        _all_crypto_refresh_symbol_universe()
        if symbol not in all_crypto_crypto_symbols:
            print(
                f"[ALL CRYPTO DIRECT REJECTED NONCRYPTO] exchange={exchange} symbol={symbol}",
                flush=True,
            )
            return {
                "ok": True,
                "accepted": False,
                "filtered_noncrypto": True,
                "source": "direct",
                "exchange": exchange,
                "symbol": symbol,
            }
    else:
        print(
            f"[ALL CRYPTO DIRECT VERIFIED CRYPTO] exchange={exchange} symbol={symbol}",
            flush=True,
        )

    if side not in ("long", "short"):
        return {"ok": False, "error": "invalid_all_crypto_side"}
    if not event_key:
        return {"ok": False, "error": "missing_event_key"}
    try:
        amount = abs(float(amount or 0.0))
    except (TypeError, ValueError):
        return {"ok": False, "error": "invalid_notional"}
    if amount <= 0:
        return {"ok": False, "error": "invalid_notional"}

    ts_ms = normalize_marginpad_ts_ms(event_ts) if event_ts is not None else None
    ts_sec = (ts_ms / 1000.0) if ts_ms is not None else time.time()
    fp = f"direct|{exchange}|{event_key}"

    with _all_crypto_lock:
        if fp in all_crypto_seen_set:
            return {"ok": True, "duplicate": True, "source": "direct", "exchange": exchange, "symbol": symbol}
        _all_crypto_remember(fp)

        if all_crypto_cycle_start_ts is None:
            all_crypto_cycle_start_ts = ts_sec
        if side == "long":
            all_crypto_long_cumulative += amount
        else:
            all_crypto_short_cumulative += amount

        bucket = all_crypto_by_symbol.setdefault(symbol, {"long": 0.0, "short": 0.0})
        bucket[side] += amount
        _all_crypto_unusual_add(symbol, side, amount, ts_sec)
        if symbol in ALT_GAP_ASSETS:
            _alt_gap_observer_add(symbol, exchange, side, amount)
        all_crypto_rolling_events.append((ts_sec, side, amount, symbol, exchange))
        if len(all_crypto_rolling_events) > 1 and ts_sec < float(all_crypto_rolling_events[-2][0]):
            all_crypto_rolling_events = deque(sorted(all_crypto_rolling_events, key=lambda r: r[0]))

        now_ts = time.time()
        _all_crypto_rolling_trim(now_ts)
        reset_alert = _all_crypto_send_reset_if_flip()
        rolling_alert = _all_crypto_send_rolling_if_flip(now_ts)
        all_crypto_last_poll_ts = now_ts
        all_crypto_last_error = None

        result = {
            "ok": True,
            "duplicate": False,
            "source": "direct",
            "exchange": exchange,
            "symbol": symbol,
            "long": round(all_crypto_long_cumulative, 2),
            "short": round(all_crypto_short_cumulative, 2),
            "gap": round(all_crypto_long_cumulative - all_crypto_short_cumulative, 2),
            "gap_state": all_crypto_gap_state,
            "rolling_state": all_crypto_rolling_state,
            "reset_alert_sent": reset_alert,
            "rolling_alert_sent": rolling_alert,
        }

    _save_runtime_state()
    print(
        f"[ALL CRYPTO DIRECT ACCEPTED] exchange={exchange} symbol={symbol} "
        f"side={side} notional=${amount:,.2f}",
        flush=True,
    )
    return result

def _all_crypto_hourly_snapshot(now_ts=None):
    """Build a read-only exact last-60m symbol leaderboard for the hourly update."""
    now_ts = float(now_ts) if now_ts is not None else time.time()
    with _all_crypto_lock:
        _all_crypto_rolling_trim(now_ts)
        by_symbol = {}
        for row in all_crypto_rolling_events:
            try:
                _, side, amount, symbol = row[:4]
                side = str(side).lower().strip()
                amount = float(amount)
                symbol = str(symbol or "UNKNOWN").upper().strip() or "UNKNOWN"
            except (TypeError, ValueError, IndexError):
                continue
            if side not in ("long", "short") or amount <= 0:
                continue
            bucket = by_symbol.setdefault(symbol, {"long": 0.0, "short": 0.0})
            bucket[side] += amount

    ranked = []
    total_long = 0.0
    total_short = 0.0

    for symbol, totals in by_symbol.items():
        long_usd = float(totals.get("long", 0.0) or 0.0)
        short_usd = float(totals.get("short", 0.0) or 0.0)
        gap = abs(long_usd - short_usd)
        total_long += long_usd
        total_short += short_usd
        if long_usd > 0 or short_usd > 0:
            ranked.append((gap, symbol, long_usd, short_usd))

    # Hourly Top 12 = biggest LONG/SHORT GAP first.
    ranked.sort(key=lambda row: row[0], reverse=True)
    return ranked, total_long, total_short


def _all_crypto_send_hourly_report(boundary_ts=None):
    """Send the fixed hourly 13EX update; no threshold, signal, or reset."""
    boundary_ts = float(boundary_ts) if boundary_ts is not None else time.time()
    ranked, total_long, total_short = _all_crypto_hourly_snapshot(boundary_ts)

    total_gap = abs(total_long - total_short)
    total_winner = (
        "LONG WINS" if total_long > total_short
        else "SHORT WINS" if total_short > total_long
        else "TIE"
    )

    ist_dt = datetime.fromtimestamp(boundary_ts, tz=NASDAQ_COMBINED_IST)
    title = "ALL CRYPTO 13EX | HOURLY UPDATE"
    lines = [f"{ist_dt.strftime('%d-%m-%Y | %H:%M')} IST"]

    if ranked:
        for idx, (gap, symbol, long_usd, short_usd) in enumerate(
            ranked[:ALL_CRYPTO_HOURLY_TOP_N], 1
        ):
            winner = (
                "LONG WINS" if long_usd > short_usd
                else "SHORT WINS" if short_usd > long_usd
                else "TIE"
            )
            coin_total = long_usd + short_usd
            long_pct = (long_usd / coin_total * 100.0) if coin_total > 0 else 0.0
            short_pct = (short_usd / coin_total * 100.0) if coin_total > 0 else 0.0
            lines.extend([
                f"{idx}. {symbol}",
                f"L ${_usd_m(long_usd)} ({long_pct:.1f}%) | S ${_usd_m(short_usd)} ({short_pct:.1f}%)",
                f"GAP ${_usd_m(gap)} | {winner}",
            ])
    else:
        lines.append("No accepted crypto liquidations in the last 60 minutes.")

    grand_total = total_long + total_short
    total_long_pct = (total_long / grand_total * 100.0) if grand_total > 0 else 0.0
    total_short_pct = (total_short / grand_total * 100.0) if grand_total > 0 else 0.0
    lines.extend([
        "ALL CRYPTO TOTAL",
        f"L ${_usd_m(total_long)} ({total_long_pct:.1f}%) | S ${_usd_m(total_short)} ({total_short_pct:.1f}%)",
        f"GAP ${_usd_m(total_gap)} | {total_winner}",
    ])

    message = "\n".join(lines)
    sent = send_pushover(title, message)

    print(
        f"[ALL CRYPTO HOURLY] boundary={ist_dt.isoformat()} active={len(ranked)} "
        f"L=${_usd_m(total_long)} S=${_usd_m(total_short)} "
        f"GAP=${_usd_m(total_gap)} winner={total_winner} sent={sent}",
        flush=True,
    )
    return bool(sent)


def _all_crypto_hourly_reporter_loop():
    print("[ALL CRYPTO HOURLY] reporter started | IST clock-hour", flush=True)
    while True:
        now = datetime.now(NASDAQ_COMBINED_IST)
        next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        sleep_seconds = max(0.25, (next_hour - now).total_seconds())
        time.sleep(sleep_seconds)
        # Boundary timestamp is used as the exact trailing-60m endpoint.
        boundary = datetime.now(NASDAQ_COMBINED_IST).replace(minute=0, second=0, microsecond=0)
        try:
            _all_crypto_send_hourly_report(boundary.timestamp())
        except Exception as exc:
            print(f"[ALL CRYPTO HOURLY ERROR] {exc}", flush=True)


def _start_all_crypto_hourly_reporter_once():
    """ALL CRYPTO 13EX hourly Top-12 Pushover report intentionally disabled."""
    global _all_crypto_hourly_reporter_started
    _all_crypto_hourly_reporter_started = False
    print("[ALL CRYPTO HOURLY] REPORTER OFF", flush=True)
    return False

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
    # FINAL SETUP: rolling 60M is intentionally disabled.
    # Keep legacy function present so old callers are harmless.
    return None
    global xau_coinalyze_rolling_state, xau_observer_rolling_state
    global xau_coinalyze_rolling_state_ts, xau_observer_rolling_state_ts
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
        previous_state_ts = (
            xau_coinalyze_rolling_state_ts
            if source == "coinalyze"
            else xau_observer_rolling_state_ts
        )
        change_time = _format_accumulation_duration(previous_state_ts, now_ts)
        if source == "coinalyze":
            xau_coinalyze_rolling_state = new_state
            xau_coinalyze_rolling_state_ts = now_ts
            title = f"XAU COINALYZE ROLLING 60M {new_state} | 100K GAP"
        else:
            xau_observer_rolling_state = new_state
            xau_observer_rolling_state_ts = now_ts
            title = f"XAU OBSERVER 13EX ROLLING 60M {new_state} | 100K GAP"
        gap = abs(signed_gap)
        rolling_total = long_total + short_total
        long_pct = (long_total / rolling_total * 100.0) if rolling_total > 0 else 0.0
        short_pct = (short_total / rolling_total * 100.0) if rolling_total > 0 else 0.0
        try:
            price_text = f"{float(price):,.2f}" if price is not None else "NA"
        except (TypeError, ValueError):
            price_text = "NA"

        if source == "observer":
            labels = {
                "binance": "Binance", "bybit": "Bybit", "okx": "OKX",
                "hyperliquid": "Hyperliquid", "gate": "Gate", "htx": "HTX",
                "dydx": "dYdX", "bitmex": "BitMEX", "bitfinex": "Bitfinex",
                "bitget": "Bitget", "aster": "Aster", "coinex": "CoinEx",
                "lighter": "Lighter",
            }
            by_exchange = {
                ex: {"long": 0.0, "short": 0.0}
                for ex in XAU_OBSERVER_EXCHANGES
            }
            for row in events:
                if len(row) < 4:
                    continue
                _, event_side, amount, event_exchange = row[:4]
                ex_key = _btc_exchange_key(event_exchange)
                if ex_key not in by_exchange:
                    continue
                try:
                    amt = max(0.0, float(amount or 0.0))
                except (TypeError, ValueError):
                    continue
                if event_side in ("long", "short"):
                    by_exchange[ex_key][event_side] += amt

            audit_long = sum(v["long"] for v in by_exchange.values())
            audit_short = sum(v["short"] for v in by_exchange.values())
            audit_status = (
                "MATCH"
                if abs(audit_long - long_total) < 0.01
                and abs(audit_short - short_total) < 0.01
                else "MISMATCH"
            )
            exchange_lines = [
                f"{labels.get(ex, ex.title())}: L ${_usd_m(by_exchange[ex]['long'])} | S ${_usd_m(by_exchange[ex]['short'])}"
                for ex in XAU_OBSERVER_EXCHANGES
            ]
            message = (
                "WINDOW: EXACT TRAILING 60 MINUTES | NO RESET\n"
                f"LONG: ${_usd_m(long_total)} ({long_pct:.2f}%)\n"
                f"SHORT: ${_usd_m(short_total)} ({short_pct:.2f}%)\n"
                f"GAP: ${_usd_m(gap)}\n"
                f"STRONGER: {new_state}\nSTATE: {state or 'NONE'} -> {new_state}\nCHANGE TIME: {change_time}\n"
                f"XAU: {price_text}\n13EX AUDIT:\n"
                + "\n".join(exchange_lines)
                + f"\n13EX SUM LONG: ${_usd_m(audit_long)}"
                + f"\n13EX SUM SHORT: ${_usd_m(audit_short)}"
                + f"\nTOTAL LONG: ${_usd_m(long_total)}"
                + f"\nTOTAL SHORT: ${_usd_m(short_total)}"
                + f"\nAUDIT: {audit_status}"
            )
        else:
            # Coinalyze audit uses the real exchange attached to each contract
            # in /future-markets. It intentionally does NOT pretend Coinalyze
            # has the Observer's fixed 13-exchange universe.
            by_exchange = {}
            for row in events:
                if len(row) < 4:
                    continue
                _, event_side, amount, event_exchange = row[:4]
                ex_name = str(event_exchange or "Unknown").strip() or "Unknown"
                bucket = by_exchange.setdefault(ex_name, {"long": 0.0, "short": 0.0})
                try:
                    amt = max(0.0, float(amount or 0.0))
                except (TypeError, ValueError):
                    continue
                if event_side in ("long", "short"):
                    bucket[event_side] += amt

            audit_long = sum(v["long"] for v in by_exchange.values())
            audit_short = sum(v["short"] for v in by_exchange.values())
            audit_status = (
                "MATCH"
                if abs(audit_long - long_total) < 0.01
                and abs(audit_short - short_total) < 0.01
                else "MISMATCH"
            )
            exchange_lines = [
                f"{ex}: L ${_usd_m(vals['long'])} | S ${_usd_m(vals['short'])}"
                for ex, vals in sorted(
                    by_exchange.items(),
                    key=lambda item: item[1]["long"] + item[1]["short"],
                    reverse=True,
                )
            ]
            exchange_block = "\n".join(exchange_lines) or "No exchange contributions in window"

            message = (
                "WINDOW: EXACT TRAILING 60 MINUTES | NO RESET\n"
                f"LONG: ${_usd_m(long_total)}\n"
                f"SHORT: ${_usd_m(short_total)}\n"
                f"GAP: ${_usd_m(gap)}\n"
                f"STRONGER: {new_state}\nSTATE: {state or 'NONE'} -> {new_state}\nCHANGE TIME: {change_time}\n"
                f"XAU: {price_text}\nCOINALYZE EXCHANGE AUDIT:\n"
                + exchange_block
                + f"\nEXCHANGE SUM LONG: ${_usd_m(audit_long)}"
                + f"\nEXCHANGE SUM SHORT: ${_usd_m(audit_short)}"
                + f"\nTOTAL LONG: ${_usd_m(long_total)}"
                + f"\nTOTAL SHORT: ${_usd_m(short_total)}"
                + f"\nAUDIT: {audit_status}"
            )

        sent = send_pushover(title, message)
        _gap_check_capture(
            "xau_coinalyze_rolling" if source == "coinalyze" else "xau_observer_rolling",
            state, new_state, long_total, short_total,
            XAU_ROLLING_GAP_THRESHOLD, now_ts, title
        )
        print(
            f"[XAU ROLLING 60M] {source.upper()} {new_state} "
            f"L=${_usd_m(long_total)} S=${_usd_m(short_total)} GAP=${_usd_m(gap)} sent={sent}",
            flush=True,
        )
        return {
            "direction": new_state, "long": long_total, "short": short_total,
            "gap": gap, "alert_sent": bool(sent)
        }


def _xau_rolling_add(source, side, amount, event_ts=None, exchange=None, price=None):
    # FINAL SETUP: rolling 60M is intentionally disabled.
    # Keep legacy function present so old callers are harmless.
    return None
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


def _xau_rolling_add_coinalyze_row(long_amount, short_amount, event_ts, exchange=None, price=None):
    # FINAL SETUP: rolling 60M is intentionally disabled.
    # Keep legacy function present so old callers are harmless.
    return None
    try:
        long_amount=max(0.0,float(long_amount or 0.0)); short_amount=max(0.0,float(short_amount or 0.0)); ts=float(event_ts)
    except (TypeError,ValueError):
        return None
    if ts > 10_000_000_000: ts /= 1000.0
    with _xau_rolling_lock:
        exchange_name = str(exchange or "Unknown").strip() or "Unknown"
        if long_amount > 0: xau_coinalyze_rolling_events.append((ts,"long",long_amount,exchange_name))
        if short_amount > 0: xau_coinalyze_rolling_events.append((ts,"short",short_amount,exchange_name))
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
    # FINAL SETUP: rolling 60M is intentionally disabled.
    # Keep legacy function present so old callers are harmless.
    return None
    global btc_coinalyze_rolling_state, btc_observer_rolling_state
    global btc_coinalyze_rolling_state_ts, btc_observer_rolling_state_ts
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
        previous_state_ts = (
            btc_coinalyze_rolling_state_ts
            if source == "coinalyze"
            else btc_observer_rolling_state_ts
        )
        change_time = _format_accumulation_duration(previous_state_ts, now_ts)
        if source == "coinalyze":
            btc_coinalyze_rolling_state = new_state
            btc_coinalyze_rolling_state_ts = now_ts
            title = f"BTC COINALYZE ROLLING 60M {new_state} | +5M GAP"
        else:
            btc_observer_rolling_state = new_state
            btc_observer_rolling_state_ts = now_ts
            title = f"BTC OBSERVER 13EX ROLLING 60M {new_state} | +5M GAP"
        gap = abs(signed_gap)
        try: price_text = f"{float(price):,.0f}" if price is not None else "NA"
        except (TypeError, ValueError): price_text = "NA"
        message = (f"WINDOW: EXACT TRAILING 60 MINUTES | NO RESET\n"
                   f"LONG: ${_usd_m(long_total)}\n"
                   f"SHORT: ${_usd_m(short_total)}\n"
                   f"GAP: ${_usd_m(gap)}\n"
                   f"STRONGER: {new_state}\nSTATE: {state or 'NONE'} -> {new_state}\nCHANGE TIME: {change_time}\nBTC: {price_text}")
        message += "\n" + "\n".join(_rolling_exchange_breakdown_lines(
            events, signed_gap, new_state, BTC_OBSERVER_EXCHANGES if source == "observer" else None
        ))
        sent = send_pushover(title, message)
        _gap_check_capture(
            "btc_coinalyze_rolling" if source == "coinalyze" else "btc_observer_rolling",
            state, new_state, long_total, short_total,
            BTC_ROLLING_GAP_THRESHOLD, now_ts, title
        )
        print(f"[BTC ROLLING 60M] {source.upper()} {new_state} L=${_usd_m(long_total)} S=${_usd_m(short_total)} GAP=${_usd_m(gap)} sent={sent}", flush=True)
        return {"direction":new_state,"long":long_total,"short":short_total,"gap":gap,"alert_sent":bool(sent)}

def _btc_rolling_add_coinalyze_row(long_amount, short_amount, event_ts, exchange=None, price=None):
    # FINAL SETUP: rolling 60M is intentionally disabled.
    # Keep legacy function present so old callers are harmless.
    return None
    global btc_coinalyze_rolling_state, btc_coinalyze_rolling_state_ts
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
            btc_coinalyze_rolling_events.append((ts, "long", long_amount, str(exchange or "coinalyze")))
        if short_amount > 0:
            btc_coinalyze_rolling_events.append((ts, "short", short_amount, str(exchange or "coinalyze")))
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
        now_ts = max(time.time(), ts)
        change_time = _format_accumulation_duration(btc_coinalyze_rolling_state_ts, now_ts)
        btc_coinalyze_rolling_state = new_state
        btc_coinalyze_rolling_state_ts = now_ts
        gap = abs(signed_gap)
        try: price_text = f"{float(price):,.0f}" if price is not None else "NA"
        except (TypeError, ValueError): price_text = "NA"
        title = f"BTC COINALYZE ROLLING 60M {new_state} | +5M GAP"
        message = (f"WINDOW: EXACT TRAILING 60 MINUTES | NO RESET\n"
                   f"LONG: ${_usd_m(long_total)}\n"
                   f"SHORT: ${_usd_m(short_total)}\n"
                   f"GAP: ${_usd_m(gap)}\n"
                   f"STRONGER: {new_state}\nSTATE: {state or 'NONE'} -> {new_state}\nCHANGE TIME: {change_time}\nBTC: {price_text}")
        message += "\n" + "\n".join(_rolling_exchange_breakdown_lines(
            btc_coinalyze_rolling_events, signed_gap, new_state
        ))
        sent = send_pushover(title, message)
        _gap_check_capture(
            "btc_coinalyze_rolling", state, new_state, long_total, short_total,
            BTC_ROLLING_GAP_THRESHOLD, now_ts, title
        )
        print(f"[BTC ROLLING 60M] COINALYZE {new_state} L=${_usd_m(long_total)} S=${_usd_m(short_total)} GAP=${_usd_m(gap)} sent={sent}", flush=True)
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
                f"{label}(+L=${_usd_m(totals['long'])},+S=${_usd_m(totals['short'])})"
            )
        print(
            f"[BTC OBSERVER] {' | '.join(update_parts)} | "
            f"TOTAL L=${_usd_m(cycle_long)} S=${_usd_m(cycle_short)}",
            flush=True,
        )

        signed_gap = cycle_long - cycle_short

        # BTC OBSERVER 13EX NORMAL GAP:
        # Alert whenever the current fresh observer cycle reaches +/-$5M GAP.
        # The observer resets its own LONG/SHORT totals after each valid alert,
        # so the next alert is based on a new accumulation cycle.
        # No rolling-60m state is used for this normal GAP trigger.
        # Reverse-only normal GAP trigger:
        # one LONG alert remains active until a valid SHORT reversal,
        # and one SHORT alert remains active until a valid LONG reversal.
        # This prevents the same-side snapshot/cycle from alerting repeatedly.
        long_hit = (
            signed_gap >= BTC_OBSERVER_THRESHOLD
            and btc_observer_gap_state != "LONG"
        )
        short_hit = (
            signed_gap <= -BTC_OBSERVER_THRESHOLD
            and btc_observer_gap_state != "SHORT"
        )

        if long_hit or short_hit:
            if long_hit:
                winner = "LONG"
                btc_observer_gap_state = "LONG"
                title = "BTC OBSERVER 13EX LONG WINS | +5M GAP"
            else:
                winner = "SHORT"
                btc_observer_gap_state = "SHORT"
                title = "BTC OBSERVER 13EX SHORT WINS | +5M GAP"

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
                f"{_btc_exchange_label(ex_name)}: LONG ${_usd_m(ex_long)} | SHORT ${_usd_m(ex_short)}"
                for _, ex_name, ex_long, ex_short in observer_ranked
            ]
            # Build an exact per-exchange fingerprint for this completed cycle.
            # If the upstream source replays the same already-counted batch after
            # our reset, do NOT send the same alert again. A genuinely fresh
            # cycle will have a different 13-exchange fingerprint.
            exchange_totals_snapshot = {
                ex_name: {
                    "long": round(float(btc_observer_by_exchange.get(ex_name, {}).get("long", 0.0) or 0.0), 2),
                    "short": round(float(btc_observer_by_exchange.get(ex_name, {}).get("short", 0.0) or 0.0), 2),
                }
                for ex_name in BTC_OBSERVER_EXCHANGES
            }

            previous_exchange_totals = (
                btc_observer_last_alert_snapshot.get("exchange_totals")
                if isinstance(btc_observer_last_alert_snapshot, dict)
                else None
            )
            duplicate_completed_cycle = (
                isinstance(previous_exchange_totals, dict)
                and exchange_totals_snapshot == previous_exchange_totals
            )

            if duplicate_completed_cycle:
                print(
                    f"[BTC OBSERVER DUPLICATE SUPPRESSED] {title} "
                    f"L=${_usd_m(cycle_long)} S=${_usd_m(cycle_short)}",
                    flush=True,
                )
            else:
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
                    "exchange_totals": exchange_totals_snapshot,
                    "ts": int(time.time()),
                }
                btc_observer_last_alert_snapshot = dict(alert_snapshot)

            # Observer has its own cycle/reset only. Existing MarginPad, direct
            # liquidator, Coinalyze and MT5 states are untouched. Reset even when
            # an upstream replay is suppressed so replayed totals cannot linger.
            btc_observer_long_cumulative = 0.0
            btc_observer_short_cumulative = 0.0
            btc_observer_by_exchange = {
                ex: {"long": 0.0, "short": 0.0}
                for ex in BTC_OBSERVER_EXCHANGES
            }
            btc_observer_cycle_ref_price = current_price

    # Persist observer direction + reset state so a restart/reload keeps the
    # same-side lock. This does not change the $5M GAP calculation.
    if long_hit or short_hit:
        _save_runtime_state()

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
            f"{breakdown}\n"
            f"TOTAL SHORT: ${_usd_m(alert_snapshot['short'])}\n"
            f"TOTAL LONG: ${_usd_m(alert_snapshot['long'])}\n"
            f"GAP: ${_usd_m(alert_snapshot['gap'])}\n"
            f"BTC {price_text} | BTC MOVE {move_text}"
        )
        sent = send_pushover(alert_snapshot["title"], message)
        print(
            f"[BTC OBSERVER ALERT] {alert_snapshot['title']} "
            f"L=${_usd_m(alert_snapshot['long'])} S=${_usd_m(alert_snapshot['short'])} "
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
            f"{breakdown}\n"
            f"MARGINPAD SHORT: ${_usd_m(alert_snapshot['short'])}\n"
            f"MARGINPAD LONG: ${_usd_m(alert_snapshot['long'])}\n"
            f"GAP: ${_usd_m(alert_snapshot['gap'])}\n"
            f"BTC {alert_snapshot['price']:,.0f} | BTC MOVE {move_text}"
        )
        # Standalone MarginPad BTC Pushover intentionally disabled.
        # Calculation/reset + MT5 publication + Observer feed remain unchanged.
        sent = False
        print(
            f"[MARGINPAD BTC ALERT SILENT] {alert_snapshot['title']} "
            f"L=${_usd_m(alert_snapshot['long'])} S=${_usd_m(alert_snapshot['short'])}",
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
            f"{breakdown}\n"
            f"LIQUIDATOR SHORT: ${_usd_m(alert_snapshot['short'])}\n"
            f"LIQUIDATOR LONG: ${_usd_m(alert_snapshot['long'])}\n"
            f"GAP: ${_usd_m(alert_snapshot['gap'])}\n"
            f"BTC {price_text} | BTC MOVE {move_text}"
        )
        # Standalone 4-exchange BTC Liquidator Pushover intentionally disabled.
        # Calculation/reset + Observer feed remain unchanged.
        sent = False
        result["alert_sent"] = False
        print(
            f"[BTC LIQUIDATOR ALERT SILENT] {alert_snapshot['title']} "
            f"L=${_usd_m(alert_snapshot['long'])} S=${_usd_m(alert_snapshot['short'])}",
            flush=True,
        )
    else:
        print(
            f"[BTC LIQUIDATOR] {exchange.upper()} {side.upper()} +${_usd_m(amount)} | "
            f"TOTAL L=${_usd_m(result['long_usd'])} S=${_usd_m(result['short_usd'])}",
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

    # XAU price is display/reference data only. Do NOT block liquidation
    # ingestion when the shared MarginPad request lock is temporarily busy.
    xau_price, price_error = get_marginpad_xau_price()

    if price_error:
        with _combined_liq_lock:
            cached_xau_price = combined_latest_price.get("XAU")
        if cached_xau_price is not None:
            xau_price = cached_xau_price
            print(f"[MARGINPAD XAU PRICE FALLBACK] cached={xau_price} | error={price_error}", flush=True)
        else:
            xau_price = None
            print(f"[MARGINPAD XAU PRICE SKIP] ingestion continues | error={price_error}", flush=True)

    closed_end_ms = (closed_minute_ts + 59) * 1000 + 999

    if marginpad_xau_processed_through_ms is None:
        marginpad_xau_processed_through_ms = closed_end_ms
        if xau_price is not None:
            marginpad_xau_cycle_ref_price = xau_price

        with _combined_liq_lock:
            if xau_price is not None:
                combined_latest_price["XAU"] = xau_price
                if combined_cycle_ref_price["XAU"] is None:
                    combined_cycle_ref_price["XAU"] = xau_price

        return {
            "ok": True,
            "asset": "XAU",
            "source": "MarginPad",
            "initialized": True,
            "xau_price": round(xau_price, 2) if xau_price is not None else None,
            "combined_long_usd": round(combined_liq["XAU"]["long"], 2),
            "combined_short_usd": round(combined_liq["XAU"]["short"], 2),
            "cycle_reference_price": combined_cycle_ref_price["XAU"],
            "processed_through_ms": marginpad_xau_processed_through_ms
        }

    if closed_end_ms <= marginpad_xau_processed_through_ms:
        with _combined_liq_lock:
            if xau_price is not None:
                combined_latest_price["XAU"] = xau_price

        return {
            "ok": True,
            "asset": "XAU",
            "source": "MarginPad",
            "new_closed_minute": False,
            "xau_price": round(xau_price, 2) if xau_price is not None else None,
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
        exchange_breakdown=fresh.get("fresh_by_exchange", {}),
    )

    return {
        "ok": True,
        "asset": "XAU",
        "source": "MarginPad",
        "initialized": False,
        "price": round(xau_price, 2) if xau_price is not None else None,
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
    global xau_coinalyze_by_exchange
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
        xau_coinalyze_by_exchange = {}

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
            rr.get("long", 0.0), rr.get("short", 0.0), rr.get("ts"),
            exchange=rr.get("exchange"), price=xau_price
        )
    _xau_rolling_evaluate("coinalyze", price=xau_price)

    # Build the exchange audit from the exact same fresh Coinalyze rows that
    # feed this cumulative GAP cycle. This is audit/display state only.
    for rr in fresh.get("rolling_rows", []):
        ex_name = str(rr.get("exchange", "") or "Unknown").strip() or "Unknown"
        bucket = xau_coinalyze_by_exchange.setdefault(
            ex_name, {"long": 0.0, "short": 0.0}
        )
        bucket["long"] += float(rr.get("long", 0.0) or 0.0)
        bucket["short"] += float(rr.get("short", 0.0) or 0.0)

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

        exchange_lines = []
        audit_long = 0.0
        audit_short = 0.0
        ranked_exchanges = []
        for ex_name, ex_totals in xau_coinalyze_by_exchange.items():
            ex_long = float(ex_totals.get("long", 0.0) or 0.0)
            ex_short = float(ex_totals.get("short", 0.0) or 0.0)
            audit_long += ex_long
            audit_short += ex_short
            if ex_long > 0 or ex_short > 0:
                ranked_exchanges.append((max(ex_long, ex_short), ex_name, ex_long, ex_short))

        ranked_exchanges.sort(key=lambda row: row[0], reverse=True)
        for _, ex_name, ex_long, ex_short in ranked_exchanges:
            exchange_lines.append(
                f"{ex_name}: L ${_usd_m(ex_long)} | S ${_usd_m(ex_short)}"
            )

        audit_status = (
            "MATCH"
            if abs(audit_long - cycle_long) < 0.01
            and abs(audit_short - cycle_short) < 0.01
            else "MISMATCH"
        )
        audit_text = "\n".join(exchange_lines) or "No exchange contribution"

        alert_sent = send_pushover(
            alert_title,
            (
                f"WINNER "
                f"{cycle_winner} | "
                f"LONG "
                f"${_usd_m(cycle_long)} "
                f"({long_pct:.2f}%) | "
                f"SHORT "
                f"${_usd_m(cycle_short)} "
                f"({short_pct:.2f}%) | "
                f"GAP "
                f"${_usd_m(cycle_gap)} | "
                f"XAU "
                f"{xau_price:,.2f} | "
                f"XAU MOVE "
                f"{move_text}\n"
                f"COINALYZE EXCHANGE AUDIT:\n"
                f"{audit_text}\n"
                f"EXCHANGE SUM LONG: ${_usd_m(audit_long)}\n"
                f"EXCHANGE SUM SHORT: ${_usd_m(audit_short)}\n"
                f"TOTAL LONG: ${_usd_m(cycle_long)}\n"
                f"TOTAL SHORT: ${_usd_m(cycle_short)}\n"
                f"AUDIT: {audit_status}"
            )
        )

        xau_long_cumulative = 0.0
        xau_short_cumulative = 0.0
        xau_coinalyze_by_exchange = {}

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

        eth_result = process_alt_coinalyze("ETH", closed_minute_ts)
        sol_result = process_alt_coinalyze("SOL", closed_minute_ts)

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

            "btc": btc_result,
            "eth": eth_result,
            "sol": sol_result
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
    if asset == "XAUT":
        asset = "XAU"
    exchange = str(data.get("exchange", "")).lower().strip()
    side = str(data.get("side", "")).lower().strip()
    event_key = str(data.get("event_key") or data.get("event_id") or "").strip()

    if asset not in ("BTC", "ETH", "SOL", "XAU", "ALL"):
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

    if asset == "ALL":
        symbol = str(data.get("symbol", "")).upper().strip()
        if not symbol:
            return jsonify({"ok": False, "error": "missing_symbol"}), 400
        result = add_all_crypto_direct_event(
            exchange=exchange,
            symbol=symbol,
            side=side,
            amount=amount,
            event_key=event_key,
            event_ts=(data.get("ts_ms") or data.get("timestamp_ms") or data.get("ts") or data.get("timestamp")),
            verified_crypto=bool(data.get("verified_crypto", False)),
        )
    elif asset == "BTC":
        result = add_direct_btc_liquidation_event(
            exchange=exchange,
            side=side,
            amount=amount,
            event_key=event_key,
            price=data.get("price"),
            event_ts=(data.get("ts_ms") or data.get("timestamp_ms") or data.get("ts") or data.get("timestamp")),
        )
    elif asset in ("ETH", "SOL"):
        event_ts = (data.get("ts_ms") or data.get("timestamp_ms") or data.get("ts") or data.get("timestamp"))
        _st = alt_rolling[asset]
        if event_key in _st["direct_seen_set"]:
            result = {"ok": True, "duplicate": True, "asset": asset, "source": "direct", "exchange": exchange, "rolling_observer": True}
        else:
            _st["direct_seen_set"].add(event_key); _st["direct_seen_queue"].append(event_key)
            while len(_st["direct_seen_queue"]) > COMBINED_DIRECT_SEEN_MAX:
                _st["direct_seen_set"].discard(_st["direct_seen_queue"].popleft())
            _alt_add(asset, "observer", side, amount, event_ts, exchange=exchange, price=data.get("price"))
            result = {"ok": True, "duplicate": False, "asset": asset, "source": "direct", "exchange": exchange, "rolling_observer": True}
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

        eth_result = process_alt_marginpad("ETH", closed_minute_ts)
        sol_result = process_alt_marginpad("SOL", closed_minute_ts)

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

            "marginpad_btc": btc_result,
            "marginpad_eth": eth_result,
            "marginpad_sol": sol_result
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
# ALL CRYPTO 13EX STATUS / POLL ENDPOINTS
# ==================================================

@app.get("/all-crypto-status")
def all_crypto_status():
    with _all_crypto_lock:
        _all_crypto_rolling_trim(time.time())
        rolling_long, rolling_short = _all_crypto_rolling_totals()
        return jsonify({
            "ok": True,
            "threshold_usd": ALL_CRYPTO_GAP_THRESHOLD,
            "mode": "13EX_MARGINPAD9_PLUS_DIRECT4",
            "exchanges": list(ALL_CRYPTO_OBSERVER_EXCHANGES),
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



@app.get("/unusual-liquidations")
def unusual_liquidations():
    """Read-only no-reset $5M+ per-coin liquidation GAP table. No alerts."""
    from html import escape

    with _all_crypto_lock:
        rows = []
        for symbol, totals in all_crypto_unusual_by_symbol.items():
            long_total = float(totals.get("long", 0.0) or 0.0)
            short_total = float(totals.get("short", 0.0) or 0.0)
            signed_gap = long_total - short_total
            hit_ts = totals.get("hit_5m_ts")
            # Once a coin has crossed the $5M absolute GAP threshold, keep it
            # visible permanently in this no-reset ledger even if its later
            # LONG/SHORT totals offset and the current GAP falls back below $5M.
            if hit_ts is None:
                continue
            rows.append({
                "symbol": str(symbol),
                "long": long_total,
                "short": short_total,
                "gap": abs(signed_gap),
                "side": "LONG" if signed_gap > 0 else "SHORT" if signed_gap < 0 else "EVEN",
                "hit_ts": hit_ts,
            })

    rows.sort(key=lambda row: row["gap"], reverse=True)
    total_long = sum(row["long"] for row in rows)
    total_short = sum(row["short"] for row in rows)
    total_signed_gap = total_long - total_short
    total_side = "LONG" if total_signed_gap > 0 else "SHORT" if total_signed_gap < 0 else "EVEN"

    ist = ZoneInfo("Asia/Kolkata")
    def money(value):
        return "$" + _usd_m(abs(float(value or 0.0)))
    def hit_time(value):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).astimezone(ist).strftime("%d-%m %H:%M")
        except (TypeError, ValueError, OSError):
            return "NA"

    body_rows = "".join(
        "<tr>"
        f"<td>{escape(row['symbol'])}</td>"
        f"<td>{money(row['long'])}</td>"
        f"<td>{money(row['short'])}</td>"
        f"<td>{money(row['gap'])}</td>"
        f"<td class='{row['side'].lower()}'>{row['side']}</td>"
        f"<td>{hit_time(row['hit_ts'])}</td>"
        "</tr>"
        for row in rows
    )
    if not body_rows:
        body_rows = '<tr><td colspan="6" class="empty">No coin has crossed a $5M GAP yet.</td></tr>'

    html = f"""<!doctype html>
<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Unusual Liquidations</title>
<style>
body{{font-family:Arial,sans-serif;margin:16px;background:#fff;color:#111}}
h2{{margin:0 0 5px;font-size:20px}} .sub{{font-size:12px;color:#666;margin-bottom:14px}}
.wrap{{overflow-x:auto}} table{{border-collapse:collapse;width:100%;min-width:620px;font-size:14px}}
th,td{{padding:9px 8px;border-bottom:1px solid #ddd;text-align:right;white-space:nowrap}}
th:first-child,td:first-child{{text-align:left;font-weight:700}} th{{background:#f5f5f5;position:sticky;top:0}}
tfoot td{{font-weight:700;border-top:2px solid #111;border-bottom:0}} .long{{font-weight:700}} .short{{font-weight:700}}
.empty{{text-align:center!important;color:#666;font-weight:400!important;padding:18px}}
.note{{font-size:11px;color:#777;margin-top:10px}}
</style></head><body>
<h2>ALL COINS — $5M+ LIQUIDATION GAP</h2>
<div class="sub">13EX • NO RESET • NO ALERT • Largest GAP first • $5M HIT = first threshold-cross time (IST)</div>
<div class="sub" id="last-checked">LAST CHECKED: NEVER</div>
<div class="wrap"><table>
<thead><tr><th>COIN</th><th>LONG</th><th>SHORT</th><th>GAP</th><th>SIDE</th><th>$5M HIT</th></tr></thead>
<tbody>{body_rows}</tbody>
<tfoot><tr><td>TOTAL</td><td>{money(total_long)}</td><td>{money(total_short)}</td><td>{money(total_signed_gap)}</td><td>{total_side}</td><td>—</td></tr></tfoot>
</table></div>
<div class="note">TOTAL includes all coins that have ever crossed the $5M GAP threshold in this no-reset ledger.</div>
<script>
(function() {{
  const key = "unusual_liquidations_last_checked_ist";
  const previous = localStorage.getItem(key);
  document.getElementById("last-checked").textContent = "LAST CHECKED: " + (previous || "NEVER");
  const now = new Date();
  const current = new Intl.DateTimeFormat("en-GB", {{
    timeZone: "Asia/Kolkata",
    day: "2-digit", month: "2-digit", year: "numeric",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false
  }}).format(now).replace(",", "") + " IST";
  localStorage.setItem(key, current);
}})();
</script>
</body></html>"""
    return app.response_class(response=html, status=200, mimetype="text/html")


@app.get("/gap-check")
def gap_check():
    """
    Read-only LAST VALID ALERT dashboard.
    Values change ONLY when one of the existing individual alert conditions fires.
    No live rolling/cumulative movement is shown here.
    """
    ist = ZoneInfo("Asia/Kolkata")

    def fmt_money(value):
        value = float(value or 0.0)
        sign = "-" if value < 0 else ""
        return sign + "$" + _usd_m(abs(value))

    def fmt_time(value):
        if value is None:
            return "NA"
        try:
            return datetime.fromtimestamp(float(value), timezone.utc).astimezone(ist).strftime(
                "%d/%m/%Y %H:%M:%S IST"
            )
        except (TypeError, ValueError, OSError):
            return "NA"

    def normalized_snapshot(key, name, fallback=None):
        snap = gap_check_last_alerts.get(key)
        if not isinstance(snap, dict) and isinstance(fallback, dict):
            # Backward-compatible display only. This does not create/change alert state.
            long_total = float(fallback.get("long", 0.0) or 0.0)
            short_total = float(fallback.get("short", 0.0) or 0.0)
            winner = str(fallback.get("winner") or "NONE")
            signed_gap = long_total - short_total
            snap = {
                "ts": fallback.get("ts"),
                "previous_state": "NA",
                "state": winner,
                "long": long_total,
                "short": short_total,
                "signed_gap": signed_gap,
                "gap": abs(signed_gap),
                "title": fallback.get("title", ""),
            }

        if not isinstance(snap, dict):
            return {"name": name, "empty": True}

        long_total = float(snap.get("long", 0.0) or 0.0)
        short_total = float(snap.get("short", 0.0) or 0.0)
        signed_gap = float(snap.get("signed_gap", long_total - short_total) or 0.0)
        state = str(snap.get("state") or "NONE")
        previous = str(snap.get("previous_state") or "NA")
        return {
            "name": name,
            "empty": False,
            "long": long_total,
            "short": short_total,
            "signed_gap": signed_gap,
            "gap": abs(signed_gap),
            "state": state,
            "previous_state": previous,
            "ts": snap.get("ts"),
        }

    # XAU Observer already had a persistent canonical last-alert snapshot before
    # this dashboard feature existed, so it can be shown immediately as fallback.
    with _combined_liq_lock:
        xau_observer_fallback = (
            dict(combined_last_alert["XAU"])
            if isinstance(combined_last_alert.get("XAU"), dict)
            else None
        )

    rows = [
        normalized_snapshot("all_crypto_normal", "ALL CRYPTO NORMAL"),
        normalized_snapshot("all_crypto_rolling", "ALL CRYPTO ROLLING 60M"),
        normalized_snapshot("btc_observer_rolling", "BTC OBSERVER 13EX ROLLING 60M"),
        normalized_snapshot("btc_coinalyze_rolling", "BTC COINALYZE ROLLING 60M"),
        normalized_snapshot("eth_observer_rolling", "ETH OBSERVER 13EX ROLLING 60M"),
        normalized_snapshot("eth_coinalyze_rolling", "ETH COINALYZE ROLLING 60M"),
        normalized_snapshot("sol_observer_rolling", "SOL OBSERVER 13EX ROLLING 60M"),
        normalized_snapshot("sol_coinalyze_rolling", "SOL COINALYZE ROLLING 60M"),
        normalized_snapshot("xau_observer_normal", "XAU OBSERVER 13EX", xau_observer_fallback),
        normalized_snapshot("xau_coinalyze_normal", "XAU COINALYZE"),
        normalized_snapshot("xau_observer_rolling", "XAU OBSERVER 13EX ROLLING 60M"),
        normalized_snapshot("xau_coinalyze_rolling", "XAU COINALYZE ROLLING 60M"),
    ]

    lines = [
        "GAP ALERT COMBO CHECK",
        "LAST COMPLETED VALID ALERTS ONLY",
        "",
    ]

    for row in rows:
        lines.append(row["name"])
        if row["empty"]:
            lines.extend([
                "LAST ALERT: WAITING FOR NEXT VALID ALERT",
                "",
            ])
            continue

        sign = "+" if row["signed_gap"] > 0 else "-" if row["signed_gap"] < 0 else ""
        lines.extend([
            f"L: {fmt_money(row['long'])} | S: {fmt_money(row['short'])}",
            f"GAP: {sign}{fmt_money(abs(row['signed_gap']))} {row['state']}",
            f"STATE: {row['previous_state']} -> {row['state']}",
            f"ALERT TIME: {fmt_time(row['ts'])}",
            "",
        ])

    return app.response_class(
        response="\n".join(lines),
        status=200,
        mimetype="text/plain",
    )
# ==================================================
# START ALL-CRYPTO BACKGROUND POLLER
# ==================================================
# Start only after every function/route in this module has been defined.
# The poller's first successful MarginPad feed pass also performs the
# read-only unusual-strength ledger bootstrap when that ledger is empty.
_start_all_crypto_poller_once()
