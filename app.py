import os
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

PUSHOVER_TOKEN = os.environ.get("PUSHOVER_TOKEN")
PUSHOVER_USER = os.environ.get("PUSHOVER_USER")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")

PUSHOVER_URL = "https://api.pushover.net/1/messages.json"

latest_delta = {
    "NQ": None,
    "ES": None
}

latest_price = {
    "NQ": None,
    "ES": None,
    "JPN": None
}

state = 0

THRESHOLD = 1000


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

    response = requests.post(
        PUSHOVER_URL,
        data=payload,
        timeout=10
    )

    return response.ok


@app.get("/")
def home():
    return jsonify({
        "status": "ok",
        "service": "NQ + ES Combined Delta Backend",
        "threshold": THRESHOLD,
        "nq_delta": latest_delta["NQ"],
        "es_delta": latest_delta["ES"],
        "nq_price": latest_price["NQ"],
        "jpn_price": latest_price["JPN"],
        "state": state
    })


@app.post("/webhook")
def webhook():
    global state

    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "error": "unauthorized"
        }), 401

    data = request.get_json(silent=True) or {}

    symbol = str(data.get("symbol", "")).upper()

    try:
        price = float(data.get("price"))
    except (TypeError, ValueError):
        return jsonify({
            "ok": False,
            "error": "invalid price"
        }), 400

    # -------------------------
    # JPN PRICE UPDATE ONLY
    # -------------------------

    if "JPN" in symbol or "NIY" in symbol:

        latest_price["JPN"] = price

        return jsonify({
            "ok": True,
            "instrument_updated": "JPN",
            "jpn_price": price,
            "signal": None
        })

    # -------------------------
    # NQ / ES DELTA UPDATE
    # -------------------------

    try:
        delta = float(data.get("delta"))
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

    # Wait until both NQ and ES have values
    if (
        latest_delta["NQ"] is None
        or latest_delta["ES"] is None
    ):
        return jsonify({
            "ok": True,
            "message": "waiting for NQ and ES"
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

        nq_price = latest_price["NQ"]
        jpn_price = latest_price["JPN"]

        nq_price_text = (
            f"{nq_price:.2f}"
            if nq_price is not None
            else "NA"
        )

        jpn_price_text = (
            f"{jpn_price:.2f}"
            if jpn_price is not None
            else "NA"
        )

        title = f"NQ + ES {signal}"

        message = (
            f"{signal} | "
            f"Combined Delta {combined:.0f} | "
            f"NQ Delta {nq_delta:.0f} | "
            f"ES Delta {es_delta:.0f} | "
            f"NQ {nq_price_text} | "
            f"JPN {jpn_price_text}"
        )

        send_pushover(title, message)

    return jsonify({
        "ok": True,
        "instrument_updated": instrument,
        "nq_delta": nq_delta,
        "es_delta": es_delta,
        "combined_delta": combined,
        "nq_price": latest_price["NQ"],
        "jpn_price": latest_price["JPN"],
        "signal": signal,
        "state": state
    })


if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )
