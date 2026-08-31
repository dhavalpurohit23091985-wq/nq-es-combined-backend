import os
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

PUSHOVER_TOKEN = os.environ.get("PUSHOVER_TOKEN")
PUSHOVER_USER = os.environ.get("PUSHOVER_USER")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")
COINALYZE_API_KEY = os.environ.get("COINALYZE_API_KEY")
PUSHOVER_URL = "https://api.pushover.net/1/messages.json"

# --------------------------------------------------
# LIVE DATA
# --------------------------------------------------

latest_delta = {
    "NQ": None,
    "ES": None
}

latest_price = {
    "NQ": None,
    "ES": None,
    "JPN": None
}

# --------------------------------------------------
# COMBINED SIGNAL STATE
# --------------------------------------------------

state = 0
THRESHOLD = 1000

# --------------------------------------------------
# TRADE JOURNAL STATE
# --------------------------------------------------

entry_side = None
entry_nq_price = None
entry_jpn_price = None


def send_pushover(title, message):

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


@app.get("/")
def home():

    return jsonify({
        "status": "ok",
        "service": "NQ + ES Combined Delta Backend",
        "threshold": THRESHOLD,

        "nq_delta": latest_delta["NQ"],
        "es_delta": latest_delta["ES"],

        "nq_price": latest_price["NQ"],
        "es_price": latest_price["ES"],
        "jpn_price": latest_price["JPN"],

        "state": state,

        "entry_side": entry_side,
        "entry_nq_price": entry_nq_price,
        "entry_jpn_price": entry_jpn_price
    })


@app.post("/webhook")
def webhook():

    global state
    global entry_side
    global entry_nq_price
    global entry_jpn_price

    # --------------------------------------------------
    # SECURITY
    # --------------------------------------------------

    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:

        return jsonify({
            "ok": False,
            "error": "unauthorized"
        }), 401

    # --------------------------------------------------
    # READ JSON
    # --------------------------------------------------

    data = request.get_json(silent=True) or {}

    # --------------------------------------------------
    # DIRECT PUSHOVER MODE
    #
    # Accepts TradingView payload like:
    # {"title":"NQ CLUSTER SELL","message":"SELL ENTRY | ..."}
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

    # --------------------------------------------------
    # COMBINED NQ / ES / JPN MODE
    # --------------------------------------------------

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
    # JPN PRICE UPDATE ONLY
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
    # NQ / ES NEED DELTA
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

    # --------------------------------------------------
    # IDENTIFY INSTRUMENT
    # --------------------------------------------------

    if "NQ" in symbol:

        instrument = "NQ"

    elif "ES" in symbol:

        instrument = "ES"

    else:

        return jsonify({
            "ok": False,
            "error": "symbol must be NQ, ES or JPN"
        }), 400

    # --------------------------------------------------
    # SAVE LATEST VALUES
    # --------------------------------------------------

    latest_delta[instrument] = delta
    latest_price[instrument] = price

    # --------------------------------------------------
    # WAIT UNTIL BOTH DELTAS EXIST
    # --------------------------------------------------

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

    # --------------------------------------------------
    # SIGNAL CHANGE ONLY
    # --------------------------------------------------

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

    # --------------------------------------------------
    # IF SIGNAL CHANGED
    # --------------------------------------------------

    if signal:

        current_nq = latest_price["NQ"]
        current_jpn = latest_price["JPN"]

        # ==================================================
        # CLOSE PREVIOUS TRADE
        # ==================================================

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

            close_title = (
                f"NQ + ES CLOSED {entry_side}"
            )

            close_message = (
                f"CLOSED {entry_side} | "
                f"NQ {nq_result} {nq_points:+.2f} pts | "
                f"JPN {jpn_result} {jpn_points:+.2f} pts | "
                f"Exit NQ {current_nq:.2f} | "
                f"JPN {current_jpn:.2f}"
            )

            send_pushover(
                close_title,
                close_message
            )

        # ==================================================
        # SEND NEW BUY / SELL SIGNAL
        # ==================================================

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

        signal_title = (
            f"NQ + ES {signal}"
        )

        signal_message = (
            f"{signal} | "
            f"Combined Delta {combined:.0f} | "
            f"NQ Delta {nq_delta:.0f} | "
            f"ES Delta {es_delta:.0f} | "
            f"NQ {nq_price_text} | "
            f"JPN {jpn_price_text}"
        )

        send_pushover(
            signal_title,
            signal_message
        )

        # ==================================================
        # SAVE NEW ENTRY
        # ==================================================

        entry_side = signal
        entry_nq_price = current_nq
        entry_jpn_price = current_jpn

    # --------------------------------------------------
    # RESPONSE TO TRADINGVIEW
    # --------------------------------------------------

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
# --------------------------------------------------
# COINALYZE API TEST
# --------------------------------------------------

@app.route("/test-coinalyze", methods=["GET"])
def test_coinalyze():

    url = "https://api.coinalyze.net/v1/exchanges"

    headers = {
        "api_key": COINALYZE_API_KEY
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=10
        )

        return jsonify({
            "status_code": response.status_code,
            "response": response.json()
        })

    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500



@app.route("/test-btc-liquidation", methods=["GET"])
def test_btc_liquidation():
    import time

    now = int(time.time())
    one_hour_ago = now - 3600

    url = "https://api.coinalyze.net/v1/liquidation-history"

    params = {
        "symbols": "BTCUSDT_PERP.A",
        "interval": "1min",
        "from": one_hour_ago,
        "to": now,
        "convert_to_usd": "true"
    }

    headers = {
        "api_key": COINALYZE_API_KEY
    }

    try:
        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=10
        )

        data = response.json()

        long_total = 0.0
        short_total = 0.0

        if response.status_code == 200 and data:
            history = data[0].get("history", [])

            for row in history:
                long_total += float(row.get("l", 0) or 0)
                short_total += float(row.get("s", 0) or 0)

        net = short_total - long_total

        if net >= 1_000_000:
            signal = "BUY"
        elif net <= -1_000_000:
            signal = "SELL"
        else:
            signal = "WAIT"

        return jsonify({
            "status_code": response.status_code,
            "window": "rolling_last_60_minutes",
            "long_liquidations_usd": round(long_total, 2),
            "short_liquidations_usd": round(short_total, 2),
            "net_short_minus_long_usd": round(net, 2),
            "signal": signal
        })

    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500   



@app.route("/test-btc-symbols", methods=["GET"])
def test_btc_symbols():
    url = "https://api.coinalyze.net/v1/future-markets"

    headers = {
        "api_key": COINALYZE_API_KEY
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=10
        )

        data = response.json()

        btc_markets = []

        if response.status_code == 200:
            for market in data:
                if market.get("base_asset") == "BTC":
                    btc_markets.append({
                        "symbol": market.get("symbol"),
                        "exchange": market.get("exchange"),
                        "is_perpetual": market.get("is_perpetual")
                    })

        return jsonify({
            "status_code": response.status_code,
            "count": len(btc_markets),
            "btc_markets": btc_markets
        })

    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500
@app.route("/test-btc-aggregate", methods=["GET"])
def test_btc_aggregate():
    import time

    now = int(time.time())
    one_hour_ago = now - 3600

    headers = {
        "api_key": COINALYZE_API_KEY
    }

    try:
        # 1) Get all supported futures markets
        markets_response = requests.get(
            "https://api.coinalyze.net/v1/future-markets",
            headers=headers,
            timeout=10
        )

        markets = markets_response.json()

        # 2) Keep only BTC perpetual contracts
        btc_symbols = []

        if markets_response.status_code == 200:
            for market in markets:
                if (
                    market.get("base_asset") == "BTC"
                    and market.get("is_perpetual") is True
                ):
                    symbol = market.get("symbol")
                    if symbol:
                        btc_symbols.append(symbol)

        # Remove duplicates
        btc_symbols = list(dict.fromkeys(btc_symbols))

        long_total = 0.0
        short_total = 0.0

        liquidation_url = "https://api.coinalyze.net/v1/liquidation-history"

        # Coinalyze allows max 20 symbols per request
        for i in range(0, len(btc_symbols), 20):
            batch = btc_symbols[i:i + 20]

            params = {
                "symbols": ",".join(batch),
                "interval": "1min",
                "from": one_hour_ago,
                "to": now,
                "convert_to_usd": "true"
            }

            response = requests.get(
                liquidation_url,
                params=params,
                headers=headers,
                timeout=15
            )

            if response.status_code != 200:
                continue

            data = response.json()

            for symbol_data in data:
                history = symbol_data.get("history", [])

                for row in history:
                    long_total += float(row.get("l", 0) or 0)
                    short_total += float(row.get("s", 0) or 0)

        # 3) Net difference
        net = short_total - long_total

        if net >= 1_000_000:
            signal = "BUY"
        elif net <= -1_000_000:
            signal = "SELL"
        else:
            signal = "WAIT"

        return jsonify({
            "status_code": 200,
            "window": "rolling_last_60_minutes",
            "btc_perpetual_symbols": len(btc_symbols),
            "long_liquidations_usd": round(long_total, 2),
            "short_liquidations_usd": round(short_total, 2),
            "net_short_minus_long_usd": round(net, 2),
            "signal": signal
        })

    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500
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
