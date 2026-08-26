import os
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

PUSHOVER_TOKEN = os.environ.get("PUSHOVER_TOKEN")
PUSHOVER_USER = os.environ.get("PUSHOVER_USER")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")

PUSHOVER_URL = "https://api.pushover.net/1/messages.json"

# Latest delta received from each instrument
latest_delta = {
    "NQ": None,
    "ES": None
}

# Current combined trading state
# 0 = neutral
# 1 = BUY
# -1 = SELL
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
        delta = float(data.get("delta"))
    except (TypeError, ValueError):
        return jsonify({
            "ok": False,
            "error": "invalid delta"
        }), 400

    # Accept NQ / NQ1! and ES / ES1!
    if "NQ" in symbol:
        instrument = "NQ"
    elif "ES" in symbol:
        instrument = "ES"
    else:
        return jsonify({
            "ok": False,
            "error": "symbol must be NQ or ES"
        }), 400

    latest_delta[instrument] = delta

    # Wait until both have sent at least one update
    if (
        latest_delta["NQ"] is None
        or latest_delta["ES"] is None
    ):
        return jsonify({
            "ok": True,
            "message": "waiting for both instruments",
            "nq_delta": latest_delta["NQ"],
            "es_delta": latest_delta["ES"]
        })

    nq_delta = latest_delta["NQ"]
    es_delta = latest_delta["ES"]

    combined = nq_delta + es_delta

    signal = None

    # BUY only when combined crosses/qualifies +1000
    if combined >= THRESHOLD and state != 1:
        state = 1
        signal = "BUY"

    # SELL only when combined crosses/qualifies -1000
    elif combined <= -THRESHOLD and state != -1:
        state = -1
        signal = "SELL"

    if signal:
        title = f"NQ + ES {signal}"

        message = (
            f"{signal} | Combined Delta {combined:.0f} | "
            f"NQ {nq_delta:.0f} | ES {es_delta:.0f}"
        )

        send_pushover(title, message)

    return jsonify({
        "ok": True,
        "instrument_updated": instrument,
        "nq_delta": nq_delta,
        "es_delta": es_delta,
        "combined_delta": combined,
        "signal": signal,
        "state": state
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )
