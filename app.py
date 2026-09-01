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
# BTC FRESH LIQUIDATION SETTINGS
# ==================================================

BTC_LIQ_THRESHOLD = 5_000_000
BTC_LOW_MOVE_POINTS = 500


# Fresh cycle counters
btc_long_cumulative = 0.0
btc_short_cumulative = 0.0


# One common cycle reference price
btc_cycle_ref_price = None


# Last fully processed 1-minute liquidation timestamp
last_processed_liq_ts = None


# Cache BTC perpetual symbols
btc_symbol_cache = None


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
        "service": "NQ + ES + BTC Fresh Liquidation Backend",

        "nq_es_threshold": THRESHOLD,

        "btc_liquidation_threshold": BTC_LIQ_THRESHOLD,
        "btc_low_move_points": BTC_LOW_MOVE_POINTS,

        "btc_long_cumulative": round(
            btc_long_cumulative,
            2
        ),

        "btc_short_cumulative": round(
            btc_short_cumulative,
            2
        ),

        "btc_cycle_ref_price": btc_cycle_ref_price,

        "last_processed_liq_ts":
            last_processed_liq_ts,

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


    # ----------------------------------------------
    # DIRECT PUSHOVER MODE
    # ----------------------------------------------

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


    # ----------------------------------------------
    # PRICE
    # ----------------------------------------------

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

    except (TypeError, ValueError):

        return jsonify({
            "ok": False,
            "error": "invalid price"
        }), 400


    # ----------------------------------------------
    # JPN UPDATE
    # ----------------------------------------------

    if "JPN" in symbol or "NIY" in symbol:

        latest_price["JPN"] = price

        return jsonify({
            "ok": True,
            "instrument_updated": "JPN",
            "jpn_price": price,
            "signal": None
        })


    # ----------------------------------------------
    # DELTA
    # ----------------------------------------------

    try:
        delta = float(
            data.get("delta")
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


    if combined >= THRESHOLD and state != 1:
        state = 1
        signal = "BUY"

    elif combined <= -THRESHOLD and state != -1:
        state = -1
        signal = "SELL"


    # ----------------------------------------------
    # SIGNAL FLIP
    # ----------------------------------------------

    if signal:

        current_nq = latest_price["NQ"]
        current_jpn = latest_price["JPN"]


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

    global btc_symbol_cache


    if btc_symbol_cache:
        return btc_symbol_cache, None


    try:

        response = requests.get(
            "https://api.coinalyze.net/v1/future-markets",
            headers={
                "api_key": COINALYZE_API_KEY
            },
            timeout=10
        )

    except requests.RequestException as e:

        return None, {
            "stage": "future-markets",
            "error": str(e)
        }


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


    btc_symbol_cache = btc_symbols

    return btc_symbols, None


# ==================================================
# GET CURRENT BTC PRICE
# ==================================================

def get_btc_price():

    now = int(time.time())


    try:

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

    except requests.RequestException as e:

        return None, {
            "stage": "btc-price",
            "error": str(e)
        }


    if response.status_code != 200:

        return None, {
            "stage": "btc-price",
            "status_code":
                response.status_code,
            "response":
                response.text[:500]
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
# GET FRESH CLOSED-MINUTE LIQUIDATIONS
# ==================================================

def get_fresh_btc_liquidations(
    previous_ts,
    closed_minute_ts
):

    btc_symbols, error = (
        get_btc_perpetual_symbols()
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

    query_to = closed_minute_ts + 59


    for i in range(
        0,
        len(btc_symbols),
        20
    ):

        batch = btc_symbols[
            i:i + 20
        ]


        try:

            response = requests.get(
                liquidation_url,
                params={
                    "symbols":
                        ",".join(batch),

                    "interval":
                        "1min",

                    "from":
                        query_from,

                    "to":
                        query_to,

                    "convert_to_usd":
                        "true"
                },
                headers={
                    "api_key":
                        COINALYZE_API_KEY
                },
                timeout=15
            )

        except requests.RequestException as e:

            failed_batches.append({
                "symbols": batch,
                "error": str(e)
            })

            continue


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
                    < row_ts
                    <= closed_minute_ts
                ):
                    continue


                try:

                    fresh_long += float(
                        row.get(
                            "l",
                            0
                        ) or 0
                    )

                    fresh_short += float(
                        row.get(
                            "s",
                            0
                        ) or 0
                    )

                except (
                    TypeError,
                    ValueError
                ):
                    continue


    if failed_batches:

        return None, {
            "stage":
                "liquidation-history",

            "error":
                "one_or_more_batches_failed",

            "successful_batches":
                successful_batches,

            "failed_batches":
                failed_batches
        }


    return {
        "btc_perpetual_symbols":
            len(btc_symbols),

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
                - fresh_long,
                2
            )
    }, None


# ==================================================
# BTC READ-ONLY STATE
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
            last_processed_liq_ts,

        "cached_btc_symbols":
            (
                len(btc_symbol_cache)
                if btc_symbol_cache
                else 0
            )
    })


# ==================================================
# BTC EVERY-MINUTE FRESH LIQUIDATION ALERT
# ==================================================

@app.get("/btc-minute-alert")
def btc_minute_alert():

    global btc_long_cumulative
    global btc_short_cumulative

    global btc_cycle_ref_price

    global last_processed_liq_ts


    try:

        now = int(time.time())


        # ------------------------------------------
        # Latest FULLY CLOSED 1-minute candle
        # ------------------------------------------

        current_minute_start = (
            now // 60
        ) * 60

        closed_minute_ts = (
            current_minute_start
            - 60
        )


        # ------------------------------------------
        # BTC PRICE
        # ------------------------------------------

        btc_price, price_error = (
            get_btc_price()
        )


        if price_error:

            return jsonify({
                "ok": False,
                "alert_sent": False,
                "error": price_error
            }), 429


        # ------------------------------------------
        # FIRST RUN AFTER DEPLOY
        # ------------------------------------------

        if last_processed_liq_ts is None:

            last_processed_liq_ts = (
                closed_minute_ts
            )

            btc_cycle_ref_price = (
                btc_price
            )


            return jsonify({
                "ok": True,

                "initialized": True,

                "message":
                    "BTC fresh liquidation cycle initialized",

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
                    last_processed_liq_ts
            })


        # ------------------------------------------
        # NOTHING NEW YET
        # ------------------------------------------

        if (
            closed_minute_ts
            <= last_processed_liq_ts
        ):

            return jsonify({
                "ok": True,

                "new_closed_minute":
                    False,

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
                    last_processed_liq_ts
            })


        # ------------------------------------------
        # GET FRESH CLOSED MINUTE DATA
        # ------------------------------------------

        fresh, error = (
            get_fresh_btc_liquidations(
                last_processed_liq_ts,
                closed_minute_ts
            )
        )


        if error:

            return jsonify({
                "ok": False,

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
                    last_processed_liq_ts,

                "error":
                    error
            }), 429


        fresh_long = (
            fresh["fresh_long_usd"]
        )

        fresh_short = (
            fresh["fresh_short_usd"]
        )


        # ------------------------------------------
        # ADD BOTH SIDES TO CURRENT CYCLE
        # ------------------------------------------

        btc_long_cumulative += (
            fresh_long
        )

        btc_short_cumulative += (
            fresh_short
        )


        last_processed_liq_ts = (
            closed_minute_ts
        )


        # Save cycle values BEFORE any reset
        cycle_long = (
            btc_long_cumulative
        )

        cycle_short = (
            btc_short_cumulative
        )


        # ------------------------------------------
        # CHECK WHICH SIDE HIT 5M
        # ------------------------------------------

        long_hit = (
            cycle_long
            >= BTC_LIQ_THRESHOLD
        )

        short_hit = (
            cycle_short
            >= BTC_LIQ_THRESHOLD
        )


        alert_sent = False
        cycle_winner = None

        btc_price_move = None
        low_move = False


        if btc_cycle_ref_price is not None:

            btc_price_move = abs(
                btc_price
                - btc_cycle_ref_price
            )

            low_move = (
                btc_price_move
                < BTC_LOW_MOVE_POINTS
            )


        # ------------------------------------------
        # A CYCLE HAS COMPLETED
        # ------------------------------------------

        if long_hit or short_hit:

            if long_hit and short_hit:

                cycle_winner = (
                    "BOTH HIT SAME MINUTE"
                )

                alert_title = (
                    "BTC BOTH LIQ +5M"
                )

            elif long_hit:

                cycle_winner = (
                    "LONG"
                )

                alert_title = (
                    "BTC LONG LIQ +5M"
                )

            else:

                cycle_winner = (
                    "SHORT"
                )

                alert_title = (
                    "BTC SHORT LIQ +5M"
                )


            move_text = (
                f"{btc_price_move:,.0f} pts"
                if btc_price_move
                is not None
                else "NA"
            )


            low_move_text = (
                " | LOW-MOVE YES"
                if low_move
                else ""
            )


            alert_sent = send_pushover(
                alert_title,
                (
                    f"WINNER {cycle_winner} | "
                    f"LONG ${cycle_long:,.0f} | "
                    f"SHORT ${cycle_short:,.0f} | "
                    f"BTC {btc_price:,.0f} | "
                    f"BTC MOVE {move_text}"
                    f"{low_move_text}"
                )
            )


            # --------------------------------------
            # RESET BOTH SIDES = NEW FRESH CYCLE
            # --------------------------------------

            btc_long_cumulative = 0.0
            btc_short_cumulative = 0.0

            btc_cycle_ref_price = (
                btc_price
            )


        # ------------------------------------------
        # RESPONSE
        # ------------------------------------------

        return jsonify({
            "ok": True,

            "initialized":
                False,

            "btc_perpetual_symbols":
                fresh[
                    "btc_perpetual_symbols"
                ],

            "successful_batch_count":
                fresh[
                    "successful_batch_count"
                ],

            "btc_price":
                round(
                    btc_price,
                    2
                ),

            "fresh_long_usd":
                fresh_long,

            "fresh_short_usd":
                fresh_short,

            "fresh_net_short_minus_long":
                fresh[
                    "fresh_net_short_minus_long"
                ],

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

            "btc_price_move_points":
                (
                    round(
                        btc_price_move,
                        2
                    )
                    if btc_price_move
                    is not None
                    else None
                ),

            "low_move":
                low_move,

            "cycle_reference_price":
                btc_cycle_ref_price,

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
