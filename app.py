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

BTC_LIQ_THRESHOLD = 1_000_000

# 0 = neutral / WAIT
# 1 = BUY already alerted
# -1 = SELL already alerted

btc_liq_state = 0


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

        "nq_delta": latest_delta["NQ"],
        "es_delta": latest_delta["ES"],

        "nq_price": latest_price["NQ"],
        "es_price": latest_price["ES"],
        "jpn_price": latest_price["JPN"],

        "nq_es_state": state,
        "btc_liq_state": btc_liq_state,

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
    # JPN
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


    if signal:

        current_nq = latest_price["NQ"]
        current_jpn = latest_price["JPN"]


        # --------------------------------------------------
        # CLOSE PREVIOUS
        # --------------------------------------------------

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
            "response": response.text
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
                "response": response.text
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


    net = (
        short_total
        -
        long_total
    )


    if net >= BTC_LIQ_THRESHOLD:

        signal = "BUY"

    elif net <= -BTC_LIQ_THRESHOLD:

        signal = "SELL"

    else:

        signal = "WAIT"


    result = {
        "window": "rolling_last_60_minutes",

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
            signal
    }


    return result, None


# ==================================================
# MANUAL BTC TEST
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
# BTC EVERY-MINUTE ALERT ENDPOINT
# ==================================================

@app.get("/btc-minute-alert")
def btc_minute_alert():

    global btc_liq_state


    try:

        result, error = calculate_btc_liquidations()


        # --------------------------------------------------
        # API ERROR = DO NOTHING
        # --------------------------------------------------

        if error:

            return jsonify({
                "ok": False,
                "pushover_sent": False,
                "btc_liq_state": btc_liq_state,
                "error": error
            }), 429


        signal = result["signal"]

        long_total = result[
            "long_liquidations_usd"
        ]

        short_total = result[
            "short_liquidations_usd"
        ]

        net = result[
            "net_short_minus_long_usd"
        ]


        pushover_sent = False
        new_alert = False


        # --------------------------------------------------
        # BUY
        # --------------------------------------------------

        if signal == "BUY":

            if btc_liq_state != 1:

                btc_liq_state = 1
                new_alert = True


        # --------------------------------------------------
        # SELL
        # --------------------------------------------------

        elif signal == "SELL":

            if btc_liq_state != -1:

                btc_liq_state = -1
                new_alert = True


        # --------------------------------------------------
        # WAIT = RESET
        # --------------------------------------------------

        else:

            btc_liq_state = 0


        # --------------------------------------------------
        # SEND ONLY ON NEW CROSS / FLIP
        # --------------------------------------------------

        if new_alert:

            title = (
                f"BTC 1H LIQUIDATION {signal}"
            )


            message = (
                f"{signal} | ROLLING 60 MIN | "
                f"LONG ${long_total:,.0f} | "
                f"SHORT ${short_total:,.0f} | "
                f"NET ${net:,.0f} | "
                f"{result['btc_perpetual_symbols']} BTC PERP MARKETS"
            )


            pushover_sent = send_pushover(
                title,
                message
            )


        return jsonify({
            "ok": True,

            "new_alert":
                new_alert,

            "pushover_sent":
                pushover_sent,

            "btc_liq_state":
                btc_liq_state,

            **result
        })


    except Exception as e:

        return jsonify({
            "ok": False,
            "pushover_sent": False,
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
