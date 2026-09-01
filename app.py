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
# BTC LIQUIDATION SETTINGS
# ==================================================

# Main trend alert
BTC_LIQ_THRESHOLD = 10_000_000

# Special alert:
# liquidation net changes by $10M
# but BTC price moves less than 500 points
BTC_SPECIAL_LIQ_CHANGE = 10_000_000
BTC_SPECIAL_MAX_PRICE_MOVE = 500


# Main BTC state
# 0  = no valid signal yet
# 1  = BUY
# -1 = SELL
btc_liq_state = 0


# Special alert reference
special_ref_net = None
special_ref_price = None


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
        "service": "NQ + ES + BTC Liquidation Backend",

        "nq_es_threshold": THRESHOLD,
        "btc_liquidation_threshold": BTC_LIQ_THRESHOLD,

        "btc_special_liq_change": BTC_SPECIAL_LIQ_CHANGE,
        "btc_special_max_price_move": BTC_SPECIAL_MAX_PRICE_MOVE,

        "nq_delta": latest_delta["NQ"],
        "es_delta": latest_delta["ES"],

        "nq_price": latest_price["NQ"],
        "es_price": latest_price["ES"],
        "jpn_price": latest_price["JPN"],

        "nq_es_state": state,
        "btc_liq_state": btc_liq_state,

        "special_ref_net": special_ref_net,
        "special_ref_price": special_ref_price,

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


    # --------------------------------------------------
    # DIRECT PUSHOVER MODE
    # --------------------------------------------------

    if "title" in data and "message" in data:

        ok = send_pushover(
            str(data.get("title", "TradingView Alert")),
            str(data.get("message", ""))
        )

        return jsonify({
            "ok": ok,
            "mode": "direct_pushover"
        }), 200 if ok else 500


    symbol = str(
        data.get("symbol", "")
    ).upper()


    # --------------------------------------------------
    # PRICE
    # --------------------------------------------------

    try:

        price = float(
            data.get("price")
        )

    except (TypeError, ValueError):

        return jsonify({
            "ok": False,
            "error": "invalid price"
        }), 400


    # --------------------------------------------------
    # JPN UPDATE ONLY
    # --------------------------------------------------

    if "JPN" in symbol or "NIY" in symbol:

        latest_price["JPN"] = price

        return jsonify({
            "ok": True,
            "instrument_updated": "JPN",
            "jpn_price": price,
            "signal": None
        })


    # --------------------------------------------------
    # NQ / ES DELTA
    # --------------------------------------------------

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


    # --------------------------------------------------
    # COMBINED DELTA
    # --------------------------------------------------

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


    # --------------------------------------------------
    # SIGNAL CHANGE
    # --------------------------------------------------

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

                nq_points = current_nq - entry_nq_price
                jpn_points = current_jpn - entry_jpn_price

            else:

                nq_points = entry_nq_price - current_nq
                jpn_points = entry_jpn_price - current_jpn


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

                f"CLOSED {entry_side} | "
                f"NQ {nq_result} {nq_points:+.2f} pts | "
                f"JPN {jpn_result} {jpn_points:+.2f} pts | "
                f"Exit NQ {current_nq:.2f} | "
                f"JPN {current_jpn:.2f}"
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

            f"{signal} | "
            f"Combined Delta {combined:.0f} | "
            f"NQ Delta {nq_delta:.0f} | "
            f"ES Delta {es_delta:.0f} | "
            f"NQ {nq_price_text} | "
            f"JPN {jpn_price_text}"
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
        dict.fromkeys(btc_symbols)
    )


    return btc_symbols, None


# ==================================================
# GET BTC PRICE
# ==================================================

def get_btc_price():

    now = int(time.time())

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

    except (KeyError, TypeError, ValueError):

        return None, {
            "stage": "btc-price",
            "error": "invalid BTC close"
        }


    return btc_price, None


# ==================================================
# CALCULATE ROLLING LAST 60 MIN BTC LIQUIDATIONS
# ==================================================

def calculate_btc_liquidations():

    now = int(time.time())
    one_hour_ago = now - 3600


    btc_symbols, error = get_btc_perpetual_symbols()

    if error:
        return None, error


    long_total = 0.0
    short_total = 0.0

    successful_batches = 0
    failed_batches = []


    liquidation_url = (
        "https://api.coinalyze.net/v1/liquidation-history"
    )


    # --------------------------------------------------
    # MAX 20 SYMBOLS PER REQUEST
    # --------------------------------------------------

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
                "from": one_hour_ago,
                "to": now,
                "convert_to_usd": "true"
            },

            headers={
                "api_key": COINALYZE_API_KEY
            },

            timeout=15
        )


        if response.status_code != 200:

            failed_batches.append({
                "status_code": response.status_code,
                "symbols": batch,
                "response": response.text[:500]
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

                long_total += float(
                    row.get("l", 0) or 0
                )

                short_total += float(
                    row.get("s", 0) or 0
                )


    # --------------------------------------------------
    # DO NOT USE PARTIAL DATA
    # --------------------------------------------------

    if failed_batches:

        return None, {
            "stage": "liquidation-history",
            "error": "one_or_more_batches_failed",
            "successful_batches": successful_batches,
            "failed_batches": failed_batches
        }


    net = short_total - long_total


    if net >= BTC_LIQ_THRESHOLD:

        signal = "BUY"

    elif net <= -BTC_LIQ_THRESHOLD:

        signal = "SELL"

    else:

        signal = "WAIT"


    # --------------------------------------------------
    # GET BTC PRICE FOR SPECIAL ALERT
    # --------------------------------------------------

    btc_price, price_error = get_btc_price()

    if price_error:
        return None, price_error


    result = {

        "window":
            "rolling_last_60_minutes",

        "btc_perpetual_symbols":
            len(btc_symbols),

        "successful_batch_count":
            successful_batches,

        "long_liquidations_usd":
            round(long_total, 2),

        "short_liquidations_usd":
            round(short_total, 2),

        "net_short_minus_long_usd":
            round(net, 2),

        "threshold_usd":
            BTC_LIQ_THRESHOLD,

        "signal":
            signal,

        "btc_price":
            round(btc_price, 2)
    }


    return result, None


# ==================================================
# BTC MANUAL TEST
# ==================================================

@app.get("/test-btc-aggregate")
def test_btc_aggregate():

    try:

        result, error = calculate_btc_liquidations()


        if error:

            return jsonify({
                "ok": False,
                "error": error
            }), 429


        return jsonify({
            "ok": True,
            **result
        })


    except Exception as e:

        return jsonify({
            "ok": False,
            "error": str(e)
        }), 500


# ==================================================
# BTC EVERY-MINUTE ALERT
#
# SAME CRON HANDLES:
#
# 1. NORMAL +/- $10M BUY / SELL
#
# 2. SPECIAL:
#    $10M NET LIQUIDATION CHANGE
#    + BTC PRICE MOVE < 500 POINTS
# ==================================================

@app.get("/btc-minute-alert")
def btc_minute_alert():

    global btc_liq_state
    global special_ref_net
    global special_ref_price


    try:

        result, error = calculate_btc_liquidations()


        # --------------------------------------------------
        # API ERROR
        # --------------------------------------------------

        if error:

            return jsonify({
                "ok": False,
                "alert_sent": False,
                "btc_liq_state": btc_liq_state,
                "error": error
            }), 429


        current_signal = result[
            "signal"
        ]

        long_total = result[
            "long_liquidations_usd"
        ]

        short_total = result[
            "short_liquidations_usd"
        ]

        current_net = result[
            "net_short_minus_long_usd"
        ]

        current_price = result[
            "btc_price"
        ]


        # ==================================================
        # MAIN +/- $10M BUY / SELL ALERT
        # ==================================================

        normal_alert_sent = False
        new_signal = None


        if current_signal == "BUY":

            if btc_liq_state != 1:

                btc_liq_state = 1
                new_signal = "BUY"


        elif current_signal == "SELL":

            if btc_liq_state != -1:

                btc_liq_state = -1
                new_signal = "SELL"


        # WAIT = HOLD PREVIOUS STATE


        if btc_liq_state == 1:

            state_text = "BUY"

        elif btc_liq_state == -1:

            state_text = "SELL"

        else:

            state_text = "NONE"


        # --------------------------------------------------
        # MAIN PUSHOVER
        # --------------------------------------------------

        if new_signal == "BUY":

            normal_alert_sent = send_pushover(

                "BTC LIQUIDATION BUY",

                (
                    f"BUY | ROLLING 60 MIN | "
                    f"SHORT ${short_total:,.0f} | "
                    f"LONG ${long_total:,.0f} | "
                    f"NET +${current_net:,.0f} | "
                    f"BTC {current_price:,.0f}"
                )
            )


        elif new_signal == "SELL":

            normal_alert_sent = send_pushover(

                "BTC LIQUIDATION SELL",

                (
                    f"SELL | ROLLING 60 MIN | "
                    f"LONG ${long_total:,.0f} | "
                    f"SHORT ${short_total:,.0f} | "
                    f"NET -${abs(current_net):,.0f} | "
                    f"BTC {current_price:,.0f}"
                )
            )


        # ==================================================
        # SPECIAL ALERT
        #
        # LAST REFERENCE NET -> CURRENT NET
        #
        # +/- $10M CHANGE
        #
        # AND
        #
        # BTC PRICE MOVE < 500 POINTS
        # ==================================================

        special_alert_sent = False
        special_direction = None

        liq_change = 0.0
        price_move = 0.0


        # --------------------------------------------------
        # FIRST RUN:
        # SAVE STARTING REFERENCE
        # --------------------------------------------------

        if (
            special_ref_net is None
            or special_ref_price is None
        ):

            special_ref_net = current_net
            special_ref_price = current_price


        else:

            liq_change = (
                current_net
                - special_ref_net
            )

            price_move = abs(
                current_price
                - special_ref_price
            )


            # --------------------------------------------------
            # +$10M NET CHANGE
            # SHORT LIQUIDATIONS DOMINATING
            # --------------------------------------------------

            if (
                liq_change
                >= BTC_SPECIAL_LIQ_CHANGE
            ):

                special_direction = "SHORT LIQ +10M"


                if (
                    price_move
                    < BTC_SPECIAL_MAX_PRICE_MOVE
                ):

                    special_alert_sent = send_pushover(

                        "BTC 10M LIQ / <500 MOVE",

                        (
                            f"SHORT LIQ CHANGE "
                            f"+${liq_change:,.0f} | "
                            f"BTC MOVE {price_move:,.0f} pts | "
                            f"REF {special_ref_price:,.0f} | "
                            f"NOW {current_price:,.0f}"
                        )
                    )


                # Start tracking next $10M block
                special_ref_net = current_net
                special_ref_price = current_price


            # --------------------------------------------------
            # -$10M NET CHANGE
            # LONG LIQUIDATIONS DOMINATING
            # --------------------------------------------------

            elif (
                liq_change
                <= -BTC_SPECIAL_LIQ_CHANGE
            ):

                special_direction = "LONG LIQ +10M"


                if (
                    price_move
                    < BTC_SPECIAL_MAX_PRICE_MOVE
                ):

                    special_alert_sent = send_pushover(

                        "BTC 10M LIQ / <500 MOVE",

                        (
                            f"LONG LIQ CHANGE "
                            f"-${abs(liq_change):,.0f} | "
                            f"BTC MOVE {price_move:,.0f} pts | "
                            f"REF {special_ref_price:,.0f} | "
                            f"NOW {current_price:,.0f}"
                        )
                    )


                # Start tracking next $10M block
                special_ref_net = current_net
                special_ref_price = current_price


        # ==================================================
        # RESPONSE
        # ==================================================

        return jsonify({

            "ok": True,

            "window":
                "rolling_last_60_minutes",

            "current_1h_signal":
                current_signal,

            "held_state":
                state_text,

            "new_signal":
                new_signal,

            "normal_alert_sent":
                normal_alert_sent,

            "btc_liq_state":
                btc_liq_state,

            "btc_perpetual_symbols":
                result["btc_perpetual_symbols"],

            "long_liquidations_usd":
                long_total,

            "short_liquidations_usd":
                short_total,

            "net_short_minus_long_usd":
                current_net,

            "threshold_usd":
                BTC_LIQ_THRESHOLD,

            "btc_price":
                current_price,

            # ------------------------------------------
            # SPECIAL TRACKER
            # ------------------------------------------

            "special_reference_net":
                special_ref_net,

            "special_reference_price":
                special_ref_price,

            "special_liquidation_change":
                round(liq_change, 2),

            "special_price_move_points":
                round(price_move, 2),

            "special_direction":
                special_direction,

            "special_alert_sent":
                special_alert_sent
        })


    except Exception as e:

        return jsonify({
            "ok": False,
            "normal_alert_sent": False,
            "special_alert_sent": False,
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
