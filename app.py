import os
import time

from flask import Flask, request, jsonify
import requests


app = Flask(__name__)


# ==================================================
# ENVIRONMENT
# ==================================================

PUSHOVER_TOKEN = os.environ.get("PUSHOVER_TOKEN")
PUSHOVER_USER = os.environ.get("PUSHOVER_USER")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")
COINALYZE_API_KEY = os.environ.get("COINALYZE_API_KEY")

PUSHOVER_URL = "https://api.pushover.net/1/messages.json"


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
# BTC CUMULATIVE LIQUIDATION SETTINGS
# ==================================================

BTC_LIQ_THRESHOLD = 10_000_000
BTC_LOW_MOVE_POINTS = 500

# Fresh liquidation accumulated AFTER latest reset
btc_cumulative_net = 0.0
btc_cumulative_long = 0.0
btc_cumulative_short = 0.0

# Price when current $10M block started
btc_event_ref_price = None

# Last fully processed 1-minute liquidation timestamp
last_processed_liq_ts = None

# Cache BTC perpetual symbols so future-markets
# does not need to be called every minute
btc_symbols_cache = None


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
# HOME
# ==================================================

@app.get("/")
def home():

    return jsonify({
        "status": "ok",
        "service": "NQ + ES + BTC Cumulative Liquidation Backend",

        "nq_es_threshold": THRESHOLD,

        "btc_liquidation_mode": "fresh_cumulative_no_time_limit",
        "btc_liquidation_threshold": BTC_LIQ_THRESHOLD,
        "btc_low_move_points": BTC_LOW_MOVE_POINTS,

        "btc_cumulative_net": round(
            btc_cumulative_net,
            2
        ),

        "btc_cumulative_long": round(
            btc_cumulative_long,
            2
        ),

        "btc_cumulative_short": round(
            btc_cumulative_short,
            2
        ),

        "btc_event_ref_price": btc_event_ref_price,
        "last_processed_liq_ts": last_processed_liq_ts,

        "nq_delta": latest_delta["NQ"],
        "es_delta": latest_delta["ES"],

        "nq_price": latest_price["NQ"],
        "es_price": latest_price["ES"],
        "jpn_price": latest_price["JPN"],

        "nq_es_state": state,

        "entry_side": entry_side,
        "entry_nq_price": entry_nq_price,
        "entry_jpn_price": entry_jpn_price
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

    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "error": "unauthorized"
        }), 401

    data = request.get_json(silent=True) or {}

    # DIRECT PUSHOVER MODE
    if "title" in data and "message" in data:

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
            data.get(
                "price"
            )
        )

    except (TypeError, ValueError):

        return jsonify({
            "ok": False,
            "error": "invalid price"
        }), 400

    # JPN PRICE UPDATE
    if "JPN" in symbol or "NIY" in symbol:

        latest_price["JPN"] = price

        return jsonify({
            "ok": True,
            "instrument_updated": "JPN",
            "jpn_price": price,
            "signal": None
        })

    try:
        delta = float(
            data.get(
                "delta"
            )
        )

    except (TypeError, ValueError):

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
            "error": "symbol must be NQ, ES or JPN"
        }), 400

    latest_delta[instrument] = delta
    latest_price[instrument] = price

    if (
        latest_delta["NQ"] is None
        or latest_delta["ES"] is None
    ):

        return jsonify({
            "ok": True,
            "message": "waiting for NQ and ES",
            "nq_delta": latest_delta["NQ"],
            "es_delta": latest_delta["ES"]
        })

    nq_delta = latest_delta["NQ"]
    es_delta = latest_delta["ES"]

    combined = nq_delta + es_delta

    signal = None

    if (
        combined >= THRESHOLD
        and state != 1
    ):
        state = 1
        signal = "BUY"

    elif (
        combined <= -THRESHOLD
        and state != -1
    ):
        state = -1
        signal = "SELL"

    if signal:

        current_nq = latest_price["NQ"]
        current_jpn = latest_price["JPN"]

        # CLOSE PREVIOUS
        if (
            entry_side is not None
            and entry_nq_price is not None
            and entry_jpn_price is not None
            and current_nq is not None
            and current_jpn is not None
        ):

            if entry_side == "BUY":

                nq_points = (
                    current_nq
                    - entry_nq_price
                )

                jpn_points = (
                    current_jpn
                    - entry_jpn_price
                )

            else:

                nq_points = (
                    entry_nq_price
                    - current_nq
                )

                jpn_points = (
                    entry_jpn_price
                    - current_jpn
                )

            nq_result = (
                "PROFIT"
                if nq_points > 0
                else "LOSS"
                if nq_points < 0
                else "FLAT"
            )

            jpn_result = (
                "PROFIT"
                if jpn_points > 0
                else "LOSS"
                if jpn_points < 0
                else "FLAT"
            )

            send_pushover(
                f"NQ + ES CLOSED {entry_side}",
                (
                    f"CLOSED {entry_side} | "
                    f"NQ {nq_result} "
                    f"{nq_points:+.2f} pts | "
                    f"JPN {jpn_result} "
                    f"{jpn_points:+.2f} pts | "
                    f"Exit NQ {current_nq:.2f} | "
                    f"JPN {current_jpn:.2f}"
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
                f"Combined Delta {combined:.0f} | "
                f"NQ Delta {nq_delta:.0f} | "
                f"ES Delta {es_delta:.0f} | "
                f"NQ {nq_price_text} | "
                f"JPN {jpn_price_text}"
            )
        )

        entry_side = signal
        entry_nq_price = current_nq
        entry_jpn_price = current_jpn

    return jsonify({
        "ok": True,
        "instrument_updated": instrument,

        "nq_delta": nq_delta,
        "es_delta": es_delta,
        "combined_delta": combined,

        "nq_price": latest_price["NQ"],
        "jpn_price": latest_price["JPN"],

        "signal": signal,
        "state": state,

        "entry_side": entry_side,
        "entry_nq_price": entry_nq_price,
        "entry_jpn_price": entry_jpn_price
    })


# ==================================================
# GET BTC PERPETUAL SYMBOLS
# ==================================================

def get_btc_perpetual_symbols():

    global btc_symbols_cache

    if btc_symbols_cache:
        return btc_symbols_cache, None

    response = requests.get(
        "https://api.coinalyze.net/v1/future-markets",
        headers={
            "api_key": COINALYZE_API_KEY
        },
        timeout=10
    )

    if response.status_code != 200:

        return None, {
            "stage": "future-markets",
            "status_code": response.status_code,
            "response": response.text[:500]
        }

    markets = response.json()

    btc_symbols = []

    for market in markets:

        if (
            market.get("base_asset") == "BTC"
            and market.get("is_perpetual") is True
        ):

            symbol = market.get("symbol")

            if symbol:
                btc_symbols.append(symbol)

    btc_symbols = list(
        dict.fromkeys(
            btc_symbols
        )
    )

    btc_symbols_cache = btc_symbols

    return btc_symbols, None


# ==================================================
# GET BTC CURRENT PRICE
# ==================================================

def get_btc_price():

    now = int(
        time.time()
    )

    response = requests.get(
        "https://api.coinalyze.net/v1/ohlcv-history",
        params={
            "symbols": "BTCUSDT_PERP.A",
            "interval": "1min",
            "from": now - 300,
            "to": now
        },
        headers={
            "api_key": COINALYZE_API_KEY
        },
        timeout=10
    )

    if response.status_code != 200:

        return None, {
            "stage": "btc-price",
            "status_code": response.status_code,
            "response": response.text[:500]
        }

    data = response.json()

    if not data:

        return None, {
            "stage": "btc-price",
            "error": "empty response"
        }

    history = data[0].get(
        "history",
        []
    )

    if not history:

        return None, {
            "stage": "btc-price",
            "error": "no price history"
        }

    try:
        btc_price = float(
            history[-1]["c"]
        )

    except (
        KeyError,
        TypeError,
        ValueError
    ):

        return None, {
            "stage": "btc-price",
            "error": "invalid BTC close"
        }

    return btc_price, None


# ==================================================
# FETCH ONLY NEW CLOSED 1-MIN LIQUIDATIONS
# ==================================================

def get_fresh_btc_liquidations(
    from_timestamp,
    to_timestamp
):

    btc_symbols, error = (
        get_btc_perpetual_symbols()
    )

    if error:
        return None, error

    long_total = 0.0
    short_total = 0.0

    successful_batches = 0
    failed_batches = []

    liquidation_url = (
        "https://api.coinalyze.net/v1/"
        "liquidation-history"
    )

    for i in range(
        0,
        len(btc_symbols),
        20
    ):

        batch = btc_symbols[
            i:i + 20
        ]

        response = requests.get(
            liquidation_url,
            params={
                "symbols": ",".join(batch),
                "interval": "1min",
                "from": from_timestamp,
                "to": to_timestamp,
                "convert_to_usd": "true"
            },
            headers={
                "api_key": COINALYZE_API_KEY
            },
            timeout=15
        )

        if response.status_code != 200:

            failed_batches.append({
                "status_code":
                    response.status_code,

                "symbols":
                    batch,

                "response":
                    response.text[:500]
            })

            continue

        successful_batches += 1

        data = response.json()

        for symbol_data in data:

            history = symbol_data.get(
                "history",
                []
            )

            for row in history:

                try:
                    row_time = int(
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

                # ONLY requested new minutes
                if (
                    row_time
                    < from_timestamp
                    or row_time
                    > to_timestamp
                ):
                    continue

                long_total += float(
                    row.get(
                        "l",
                        0
                    ) or 0
                )

                short_total += float(
                    row.get(
                        "s",
                        0
                    ) or 0
                )

    if failed_batches:

        return None, {
            "stage": "liquidation-history",
            "error": "one_or_more_batches_failed",
            "successful_batches":
                successful_batches,
            "failed_batches":
                failed_batches
        }

    net = (
        short_total
        - long_total
    )

    return {
        "btc_perpetual_symbols":
            len(btc_symbols),

        "successful_batch_count":
            successful_batches,

        "fresh_long_usd":
            round(long_total, 2),

        "fresh_short_usd":
            round(short_total, 2),

        "fresh_net_usd":
            round(net, 2),

        "from_timestamp":
            from_timestamp,

        "to_timestamp":
            to_timestamp
    }, None


# ==================================================
# BTC STATUS / MANUAL TEST
# DOES NOT CONSUME LIQUIDATION DATA
# ==================================================

@app.get("/test-btc-aggregate")
def test_btc_aggregate():

    try:

        btc_price, error = (
            get_btc_price()
        )

        if error:

            return jsonify({
                "ok": False,
                "error": error
            }), 429

        if btc_cumulative_net >= 0:
            direction = "SHORT_LIQ_NET"
        else:
            direction = "LONG_LIQ_NET"

        return jsonify({
            "ok": True,

            "mode":
                "fresh_cumulative_no_time_limit",

            "threshold_usd":
                BTC_LIQ_THRESHOLD,

            "low_move_points":
                BTC_LOW_MOVE_POINTS,

            "btc_price":
                round(
                    btc_price,
                    2
                ),

            "event_reference_price":
                btc_event_ref_price,

            "cumulative_long_usd":
                round(
                    btc_cumulative_long,
                    2
                ),

            "cumulative_short_usd":
                round(
                    btc_cumulative_short,
                    2
                ),

            "cumulative_net_usd":
                round(
                    btc_cumulative_net,
                    2
                ),

            "current_direction":
                direction,

            "last_processed_liq_ts":
                last_processed_liq_ts
        })

    except Exception as e:

        return jsonify({
            "ok": False,
            "error": str(e)
        }), 500


# ==================================================
# BTC EVERY-MINUTE CUMULATIVE ALERT
# ==================================================

@app.get("/btc-minute-alert")
def btc_minute_alert():

    global btc_cumulative_net
    global btc_cumulative_long
    global btc_cumulative_short

    global btc_event_ref_price
    global last_processed_liq_ts

    try:

        now = int(
            time.time()
        )

        # Last FULLY CLOSED 1-minute candle
        current_minute = (
            now // 60
        ) * 60

        closed_minute = (
            current_minute
            - 60
        )

        # Get current BTC price
        current_price, price_error = (
            get_btc_price()
        )

        if price_error:

            return jsonify({
                "ok": False,
                "alert_sent": False,
                "error": price_error
            }), 429

        # ==================================================
        # FIRST RUN AFTER DEPLOY / RESTART
        # Initialize only.
        # Do NOT count historical liquidations.
        # ==================================================

        if last_processed_liq_ts is None:

            last_processed_liq_ts = (
                closed_minute
            )

            btc_event_ref_price = (
                current_price
            )

            return jsonify({
                "ok": True,

                "initialized": True,

                "message":
                    "BTC cumulative counter initialized",

                "mode":
                    "fresh_cumulative_no_time_limit",

                "btc_price":
                    round(
                        current_price,
                        2
                    ),

                "event_reference_price":
                    round(
                        btc_event_ref_price,
                        2
                    ),

                "cumulative_net_usd":
                    round(
                        btc_cumulative_net,
                        2
                    ),

                "last_processed_liq_ts":
                    last_processed_liq_ts,

                "alert_sent":
                    False
            })

        # ==================================================
        # NOTHING NEW YET
        # ==================================================

        if (
            closed_minute
            <= last_processed_liq_ts
        ):

            price_move = abs(
                current_price
                - btc_event_ref_price
            )

            return jsonify({
                "ok": True,

                "mode":
                    "fresh_cumulative_no_time_limit",

                "message":
                    "no new closed minute yet",

                "btc_price":
                    round(
                        current_price,
                        2
                    ),

                "event_reference_price":
                    round(
                        btc_event_ref_price,
                        2
                    ),

                "price_move_points":
                    round(
                        price_move,
                        2
                    ),

                "cumulative_long_usd":
                    round(
                        btc_cumulative_long,
                        2
                    ),

                "cumulative_short_usd":
                    round(
                        btc_cumulative_short,
                        2
                    ),

                "cumulative_net_usd":
                    round(
                        btc_cumulative_net,
                        2
                    ),

                "alert_sent":
                    False
            })

        # ==================================================
        # FETCH ONLY NEW MINUTES
        # ==================================================

        from_timestamp = (
            last_processed_liq_ts
            + 60
        )

        fresh, error = (
            get_fresh_btc_liquidations(
                from_timestamp,
                closed_minute
            )
        )

        if error:

            # IMPORTANT:
            # Do not advance last_processed timestamp
            # if API failed.
            return jsonify({
                "ok": False,
                "alert_sent": False,
                "error": error
            }), 429

        # API succeeded, safe to advance
        last_processed_liq_ts = (
            closed_minute
        )

        fresh_long = (
            fresh["fresh_long_usd"]
        )

        fresh_short = (
            fresh["fresh_short_usd"]
        )

        fresh_net = (
            fresh["fresh_net_usd"]
        )

        # ==================================================
        # ADD NEW LIQUIDATION FLOW TO CURRENT EVENT
        # ==================================================

        btc_cumulative_long += (
            fresh_long
        )

        btc_cumulative_short += (
            fresh_short
        )

        btc_cumulative_net += (
            fresh_net
        )

        # Price movement since CURRENT event started
        price_move = abs(
            current_price
            - btc_event_ref_price
        )

        alert_sent = False
        event_triggered = False
        event_direction = None
        low_move = False

        completed_net = None
        completed_long = None
        completed_short = None
        completed_ref_price = None
        completed_price_move = None

        # ==================================================
        # +$10M NET SHORT LIQUIDATION
        # SHORTS FORCE-CLOSED = BUY-SIDE EVENT
        # ==================================================

        if (
            btc_cumulative_net
            >= BTC_LIQ_THRESHOLD
        ):

            event_triggered = True
            event_direction = "BUY"

            completed_net = (
                btc_cumulative_net
            )

            completed_long = (
                btc_cumulative_long
            )

            completed_short = (
                btc_cumulative_short
            )

            completed_ref_price = (
                btc_event_ref_price
            )

            completed_price_move = (
                price_move
            )

            low_move = (
                price_move
                < BTC_LOW_MOVE_POINTS
            )

            if low_move:

                title = (
                    "BTC 10M LOW-MOVE BUY"
                )

            else:

                title = (
                    "BTC 10M LIQUIDATION BUY"
                )

            alert_sent = send_pushover(
                title,
                (
                    f"SHORT LIQ NET "
                    f"+${completed_net:,.0f} | "
                    f"SHORT ${completed_short:,.0f} | "
                    f"LONG ${completed_long:,.0f} | "
                    f"BTC MOVE "
                    f"{completed_price_move:,.0f} pts | "
                    f"REF BTC "
                    f"{completed_ref_price:,.0f} | "
                    f"NOW BTC "
                    f"{current_price:,.0f} | "
                    f"LOW MOVE "
                    f"{'YES' if low_move else 'NO'}"
                )
            )

            # RESET FOR NEXT FRESH $10M EVENT
            btc_cumulative_net = 0.0
            btc_cumulative_long = 0.0
            btc_cumulative_short = 0.0

            btc_event_ref_price = (
                current_price
            )

        # ==================================================
        # -$10M NET LONG LIQUIDATION
        # LONGS FORCE-CLOSED = SELL-SIDE EVENT
        # ==================================================

        elif (
            btc_cumulative_net
            <= -BTC_LIQ_THRESHOLD
        ):

            event_triggered = True
            event_direction = "SELL"

            completed_net = (
                btc_cumulative_net
            )

            completed_long = (
                btc_cumulative_long
            )

            completed_short = (
                btc_cumulative_short
            )

            completed_ref_price = (
                btc_event_ref_price
            )

            completed_price_move = (
                price_move
            )

            low_move = (
                price_move
                < BTC_LOW_MOVE_POINTS
            )

            if low_move:

                title = (
                    "BTC 10M LOW-MOVE SELL"
                )

            else:

                title = (
                    "BTC 10M LIQUIDATION SELL"
                )

            alert_sent = send_pushover(
                title,
                (
                    f"LONG LIQ NET "
                    f"-${abs(completed_net):,.0f} | "
                    f"LONG ${completed_long:,.0f} | "
                    f"SHORT ${completed_short:,.0f} | "
                    f"BTC MOVE "
                    f"{completed_price_move:,.0f} pts | "
                    f"REF BTC "
                    f"{completed_ref_price:,.0f} | "
                    f"NOW BTC "
                    f"{current_price:,.0f} | "
                    f"LOW MOVE "
                    f"{'YES' if low_move else 'NO'}"
                )
            )

            # RESET FOR NEXT FRESH $10M EVENT
            btc_cumulative_net = 0.0
            btc_cumulative_long = 0.0
            btc_cumulative_short = 0.0

            btc_event_ref_price = (
                current_price
            )

        # ==================================================
        # RESPONSE
        # ==================================================

        return jsonify({
            "ok": True,

            "mode":
                "fresh_cumulative_no_time_limit",

            "btc_perpetual_symbols":
                fresh[
                    "btc_perpetual_symbols"
                ],

            "fresh_long_usd":
                fresh_long,

            "fresh_short_usd":
                fresh_short,

            "fresh_net_usd":
                fresh_net,

            "btc_price":
                round(
                    current_price,
                    2
                ),

            "event_reference_price":
                round(
                    btc_event_ref_price,
                    2
                ),

            "current_price_move_points":
                round(
                    abs(
                        current_price
                        - btc_event_ref_price
                    ),
                    2
                ),

            "cumulative_long_usd":
                round(
                    btc_cumulative_long,
                    2
                ),

            "cumulative_short_usd":
                round(
                    btc_cumulative_short,
                    2
                ),

            "cumulative_net_usd":
                round(
                    btc_cumulative_net,
                    2
                ),

            "threshold_usd":
                BTC_LIQ_THRESHOLD,

            "event_triggered":
                event_triggered,

            "event_direction":
                event_direction,

            "low_move":
                low_move,

            "completed_event_net":
                completed_net,

            "completed_event_long":
                completed_long,

            "completed_event_short":
                completed_short,

            "completed_event_ref_price":
                completed_ref_price,

            "completed_event_price_move":
                completed_price_move,

            "alert_sent":
                alert_sent,

            "last_processed_liq_ts":
                last_processed_liq_ts
        })

    except Exception as e:

        return jsonify({
            "ok": False,
            "alert_sent": False,
            "error": str(e)
        }), 500


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
