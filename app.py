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


# ==================================================
# XAU FRESH LIQUIDATION SETTINGS - MARGINPAD
# ==================================================
# Completely separate from both BTC MarginPad and XAU Coinalyze.

MARGINPAD_XAU_LIQ_THRESHOLD = 1_000_000

marginpad_xau_long_cumulative = 0.0
marginpad_xau_short_cumulative = 0.0

marginpad_xau_cycle_ref_price = None
marginpad_xau_processed_through_ms = None

# Separate de-duplication cache for XAU MarginPad events.
marginpad_xau_seen_queue = deque()
marginpad_xau_seen_set = set()


# ==================================================
# XAU FRESH LIQUIDATION SETTINGS
# ==================================================

XAU_LIQ_THRESHOLD = 1_000_000

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

        "newest_event_ts_ms":
            newest_event_ms,

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
zerodha_access_token = None
zerodha_access_token_created_at = None


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
    return jsonify({
        "ok": True,
        "message": "Zerodha login successful. Access token stored in this web-service process.",
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

        ok = send_pushover(
            str(
                data.get(
                    "title",
                    "TradingView Alert"
                )
            ),
            str(
                data.get(
                    "message",
                    ""
                )
            )
        )

        return jsonify({
            "ok": ok,
            "mode": "direct_pushover"
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
            )
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
        f"LONG ${ref_long:,.0f} ({ref_long_pct:.2f}%) | "
        f"SHORT ${ref_short:,.0f} ({ref_short_pct:.2f}%) | "
        f"GAP ${ref_gap:,.0f}"
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

    long_hit = (
        cycle_long
        >=
        BTC_LIQ_THRESHOLD
    )

    short_hit = (
        cycle_short
        >=
        BTC_LIQ_THRESHOLD
    )

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

        if (
            long_hit
            and
            short_hit
        ):

            cycle_winner = (
                "BOTH HIT SAME MINUTE"
            )

            alert_title = (
                "BTC COINALYZE BOTH HIT +5M"
            )

        elif long_hit:

            cycle_winner = "LONG"
            alert_title = (
                "BTC COINALYZE LONG WINS +5M"
            )

        else:

            cycle_winner = "SHORT"
            alert_title = (
                "BTC COINALYZE SHORT WINS +5M"
            )

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
                f"${cycle_long:,.0f} "
                f"({long_pct:.2f}%) | "
                f"SHORT "
                f"${cycle_short:,.0f} "
                f"({short_pct:.2f}%) | "
                f"GAP "
                f"${cycle_gap:,.0f} | "
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

def process_marginpad_btc(
    closed_minute_ts
):

    global marginpad_btc_long_cumulative
    global marginpad_btc_short_cumulative
    global marginpad_btc_cycle_ref_price
    global marginpad_btc_processed_through_ms
    global marginpad_btc_last_alert_snapshot

    btc_price, price_error = (
        get_marginpad_btc_price()
    )

    if price_error:

        return {
            "ok": False,
            "asset": "BTC",
            "source": "MarginPad",
            "alert_sent": False,
            "error": price_error
        }

    closed_end_ms = (
        closed_minute_ts
        + 59
    ) * 1000 + 999

    if (
        marginpad_btc_processed_through_ms
        is None
    ):

        # Initialize at the current closed minute so old
        # events are not counted on first deployment/restart.
        marginpad_btc_processed_through_ms = (
            closed_end_ms
        )

        marginpad_btc_cycle_ref_price = (
            btc_price
        )

        marginpad_btc_long_cumulative = 0.0
        marginpad_btc_short_cumulative = 0.0

        return {
            "ok": True,
            "asset": "BTC",
            "source": "MarginPad",
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
                marginpad_btc_cycle_ref_price,

            "processed_through_ms":
                marginpad_btc_processed_through_ms
        }

    if (
        closed_end_ms
        <=
        marginpad_btc_processed_through_ms
    ):

        return {
            "ok": True,
            "asset": "BTC",
            "source": "MarginPad",
            "new_closed_minute": False,

            "btc_price":
                round(
                    btc_price,
                    2
                ),

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

            "cycle_reference_price":
                marginpad_btc_cycle_ref_price,

            "processed_through_ms":
                marginpad_btc_processed_through_ms
        }

    fresh, error = (
        get_marginpad_fresh_btc_liquidations(
            marginpad_btc_processed_through_ms,
            closed_minute_ts
        )
    )

    if error:

        return {
            "ok": False,
            "asset": "BTC",
            "source": "MarginPad",
            "alert_sent": False,

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

            "processed_through_ms":
                marginpad_btc_processed_through_ms,

            "error":
                error
        }

    fresh_long = (
        fresh["fresh_long_usd"]
    )

    fresh_short = (
        fresh["fresh_short_usd"]
    )

    marginpad_btc_long_cumulative += (
        fresh_long
    )

    marginpad_btc_short_cumulative += (
        fresh_short
    )

    # Advance only after a successful MarginPad fetch/parse.
    marginpad_btc_processed_through_ms = (
        closed_end_ms
    )

    cycle_long = (
        marginpad_btc_long_cumulative
    )

    cycle_short = (
        marginpad_btc_short_cumulative
    )

    cycle_gap = abs(
        cycle_long
        -
        cycle_short
    )

    long_hit = (
        cycle_long
        >=
        MARGINPAD_BTC_LIQ_THRESHOLD
    )

    short_hit = (
        cycle_short
        >=
        MARGINPAD_BTC_LIQ_THRESHOLD
    )

    alert_sent = False
    cycle_winner = None

    btc_price_move = None
    low_move = False

    if (
        marginpad_btc_cycle_ref_price
        is not None
    ):

        btc_price_move = abs(
            btc_price
            -
            marginpad_btc_cycle_ref_price
        )

        low_move = (
            btc_price_move
            <
            MARGINPAD_BTC_LOW_MOVE_POINTS
        )

    if (
        long_hit
        or
        short_hit
    ):

        if (
            long_hit
            and
            short_hit
        ):

            cycle_winner = (
                "BOTH HIT SAME MINUTE"
            )

            alert_title = (
                "BTC MARGINPAD BOTH HIT +5M"
            )

        elif long_hit:

            cycle_winner = "LONG"

            alert_title = (
                "BTC MARGINPAD LONG WINS +5M"
            )

        else:

            cycle_winner = "SHORT"

            alert_title = (
                "BTC MARGINPAD SHORT WINS +5M"
            )

        move_text = (
            f"{btc_price_move:,.0f} pts"
            if
            btc_price_move
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

        # Save this cycle before reset so a Coinalyze alert arriving shortly
        # afterwards can reference the completed MarginPad cycle.
        marginpad_btc_last_alert_snapshot = {
            "ts": closed_minute_ts,
            "long": cycle_long,
            "short": cycle_short,
            "winner": cycle_winner,
        }

        reference_text = _btc_reference_text(
            "COINALYZE",
            closed_minute_ts
        )

        alert_sent = send_pushover(
            alert_title,
            (
                f"SOURCE MARGINPAD | "
                f"WINNER "
                f"{cycle_winner} | "
                f"LONG "
                f"${cycle_long:,.0f} "
                f"({long_pct:.2f}%) | "
                f"SHORT "
                f"${cycle_short:,.0f} "
                f"({short_pct:.2f}%) | "
                f"GAP "
                f"${cycle_gap:,.0f} | "
                f"BTC "
                f"{btc_price:,.0f} | "
                f"BTC MOVE "
                f"{move_text}"
                f"{low_move_text}"
                f"\n{reference_text}"
            )
        )

        marginpad_btc_long_cumulative = 0.0
        marginpad_btc_short_cumulative = 0.0

        marginpad_btc_cycle_ref_price = (
            btc_price
        )

    return {
        "ok": True,
        "asset": "BTC",
        "source": "MarginPad",
        "initialized": False,

        "price":
            round(
                btc_price,
                2
            ),

        "events_returned":
            fresh[
                "events_returned"
            ],

        "events_accepted":
            fresh[
                "events_accepted"
            ],

        "exchanges_seen":
            fresh[
                "exchanges_seen"
            ],

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
            marginpad_btc_cycle_ref_price,

        "processed_through_ms":
            marginpad_btc_processed_through_ms,

        "seen_event_cache":
            len(
                marginpad_seen_set
            )
    }


# ==================================================
# XAU PROCESSOR - MARGINPAD
# ==================================================

def process_marginpad_xau(
    closed_minute_ts
):

    global marginpad_xau_long_cumulative
    global marginpad_xau_short_cumulative
    global marginpad_xau_cycle_ref_price
    global marginpad_xau_processed_through_ms

    xau_price, price_error = (
        get_marginpad_xau_price()
    )

    if price_error:

        return {
            "ok": False,
            "asset": "XAU",
            "source": "MarginPad",
            "alert_sent": False,
            "error": price_error
        }

    closed_end_ms = (
        closed_minute_ts
        + 59
    ) * 1000 + 999

    if marginpad_xau_processed_through_ms is None:

        # Start clean at the current closed minute so a deploy/restart
        # cannot replay old XAU liquidation events into the new cycle.
        marginpad_xau_processed_through_ms = (
            closed_end_ms
        )

        marginpad_xau_cycle_ref_price = (
            xau_price
        )

        marginpad_xau_long_cumulative = 0.0
        marginpad_xau_short_cumulative = 0.0

        return {
            "ok": True,
            "asset": "XAU",
            "source": "MarginPad",
            "initialized": True,
            "xau_price": round(xau_price, 2),
            "long_cumulative_usd": 0,
            "short_cumulative_usd": 0,
            "cycle_reference_price": marginpad_xau_cycle_ref_price,
            "processed_through_ms": marginpad_xau_processed_through_ms
        }

    if closed_end_ms <= marginpad_xau_processed_through_ms:

        return {
            "ok": True,
            "asset": "XAU",
            "source": "MarginPad",
            "new_closed_minute": False,
            "xau_price": round(xau_price, 2),
            "long_cumulative_usd": round(
                marginpad_xau_long_cumulative,
                2
            ),
            "short_cumulative_usd": round(
                marginpad_xau_short_cumulative,
                2
            ),
            "cycle_reference_price": marginpad_xau_cycle_ref_price,
            "processed_through_ms": marginpad_xau_processed_through_ms
        }

    fresh, error = (
        get_marginpad_fresh_xau_liquidations(
            marginpad_xau_processed_through_ms,
            closed_minute_ts
        )
    )

    if error:

        return {
            "ok": False,
            "asset": "XAU",
            "source": "MarginPad",
            "alert_sent": False,
            "long_cumulative_usd": round(
                marginpad_xau_long_cumulative,
                2
            ),
            "short_cumulative_usd": round(
                marginpad_xau_short_cumulative,
                2
            ),
            "processed_through_ms": marginpad_xau_processed_through_ms,
            "error": error
        }

    fresh_long = fresh["fresh_long_usd"]
    fresh_short = fresh["fresh_short_usd"]

    marginpad_xau_long_cumulative += fresh_long
    marginpad_xau_short_cumulative += fresh_short

    # Advance only after a successful MarginPad fetch/parse.
    marginpad_xau_processed_through_ms = (
        closed_end_ms
    )

    cycle_long = marginpad_xau_long_cumulative
    cycle_short = marginpad_xau_short_cumulative
    cycle_gap = abs(cycle_long - cycle_short)

    long_hit = (
        cycle_long >= MARGINPAD_XAU_LIQ_THRESHOLD
    )

    short_hit = (
        cycle_short >= MARGINPAD_XAU_LIQ_THRESHOLD
    )

    alert_sent = False
    cycle_winner = None

    xau_price_move = None

    if marginpad_xau_cycle_ref_price is not None:
        xau_price_move = abs(
            xau_price
            - marginpad_xau_cycle_ref_price
        )

    if long_hit or short_hit:

        if long_hit and short_hit:
            cycle_winner = "BOTH HIT SAME MINUTE"
            alert_title = "XAU MARGINPAD BOTH HIT +1M"

        elif long_hit:
            cycle_winner = "LONG"
            alert_title = "XAU MARGINPAD LONG WINS +1M"

        else:
            cycle_winner = "SHORT"
            alert_title = "XAU MARGINPAD SHORT WINS +1M"

        move_text = (
            f"{xau_price_move:,.2f} pts"
            if xau_price_move is not None
            else "NA"
        )

        cycle_total = cycle_long + cycle_short

        long_pct = (
            cycle_long / cycle_total * 100
        ) if cycle_total > 0 else 0

        short_pct = (
            cycle_short / cycle_total * 100
        ) if cycle_total > 0 else 0

        alert_sent = send_pushover(
            alert_title,
            (
                f"SOURCE MARGINPAD | "
                f"WINNER {cycle_winner} | "
                f"LONG ${cycle_long:,.0f} "
                f"({long_pct:.2f}%) | "
                f"SHORT ${cycle_short:,.0f} "
                f"({short_pct:.2f}%) | "
                f"GAP ${cycle_gap:,.0f} | "
                f"XAU {xau_price:,.2f} | "
                f"XAU MOVE {move_text}"
            )
        )

        marginpad_xau_long_cumulative = 0.0
        marginpad_xau_short_cumulative = 0.0
        marginpad_xau_cycle_ref_price = xau_price

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
        "cycle_long_before_reset": round(cycle_long, 2),
        "cycle_short_before_reset": round(cycle_short, 2),
        "cycle_gap_usd": round(cycle_gap, 2),
        "long_cumulative_usd": round(
            marginpad_xau_long_cumulative,
            2
        ),
        "short_cumulative_usd": round(
            marginpad_xau_short_cumulative,
            2
        ),
        "threshold_usd": MARGINPAD_XAU_LIQ_THRESHOLD,
        "cycle_winner": cycle_winner,
        "alert_sent": alert_sent,
        "price_move_points": (
            round(xau_price_move, 2)
            if xau_price_move is not None
            else None
        ),
        "cycle_reference_price": marginpad_xau_cycle_ref_price,
        "processed_through_ms": marginpad_xau_processed_through_ms,
        "seen_event_cache": len(
            marginpad_xau_seen_set
        )
    }


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

    long_hit = (
        cycle_long
        >=
        XAU_LIQ_THRESHOLD
    )

    short_hit = (
        cycle_short
        >=
        XAU_LIQ_THRESHOLD
    )

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

        if (
            long_hit
            and
            short_hit
        ):

            cycle_winner = (
                "BOTH HIT SAME MINUTE"
            )

            alert_title = (
                "XAU COINALYZE BOTH HIT +1M"
            )

        elif long_hit:

            cycle_winner = "LONG"

            alert_title = (
                "XAU COINALYZE LONG WINS +1M"
            )

        else:

            cycle_winner = "SHORT"

            alert_title = (
                "XAU COINALYZE SHORT WINS +1M"
            )

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
                f"${cycle_long:,.0f} "
                f"({long_pct:.2f}%) | "
                f"SHORT "
                f"${cycle_short:,.0f} "
                f"({short_pct:.2f}%) | "
                f"GAP "
                f"${cycle_gap:,.0f} | "
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
