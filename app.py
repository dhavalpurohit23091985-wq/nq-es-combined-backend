        print("[BTC NETFLOW PUSHOVER] Missing PUSHOVER_TOKEN/PUSHOVER_USER", flush=True)
        return False

    side = str(side or "").strip().upper()
    trigger_time = str(trigger_time or "").strip()

    try:
        trigger_total = float(trigger_total)
    except (TypeError, ValueError):
        return False

    title = f"BTC 5M NETFLOW | {side}"
    message = (
        f"{side} CONDITION\n"
        f"TIME: {trigger_time}\n"
        f"RUNNING: {trigger_total:+.3f}M\n"
        f"THRESHOLD: ±$100M\n"
        f"STRICT ALTERNATION"
    )

    try:
        r = requests.post(
            PUSHOVER_URL,
            data={
                "token": PUSHOVER_TOKEN,
                "user": PUSHOVER_USER,
                "title": title,
                "message": message,
                "priority": 1,
            },
            timeout=10,
        )

        if not r.ok:
            print(
                f"[BTC NETFLOW PUSHOVER ERROR] HTTP {r.status_code} | {r.text[:300]}",
                flush=True,
            )

        return r.ok

    except requests.RequestException as exc:
        print(f"[BTC NETFLOW PUSHOVER ERROR] {exc}", flush=True)
        return False


def _btc_cors_response(response):
    response.headers["Access-Control-Allow-Origin"] = "https://www.coinglass.com"
    response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Cache-Control"] = "no-store"
    return response


@app.route("/btc-netflow-alert", methods=["POST", "OPTIONS"])
def btc_netflow_alert():
    if request.method == "OPTIONS":
        response = app.make_response(("", 204))
        return _btc_cors_response(response)

    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        response = jsonify({"ok": False, "error": "unauthorized"})
        response.status_code = 401
        return _btc_cors_response(response)

    data = request.get_json(silent=True) or {}
    side = str(data.get("side", "")).strip().upper()
    trigger_time = str(data.get("time", "")).strip()

    try:
        trigger_total = float(data.get("total"))
    except (TypeError, ValueError):
        trigger_total = None

    if side not in ("BUY", "SELL"):
        response = jsonify({"ok": False, "error": "side_must_be_BUY_or_SELL"})
        response.status_code = 400
        return _btc_cors_response(response)

    if not trigger_time:
        response = jsonify({"ok": False, "error": "time_required"})
        response.status_code = 400
        return _btc_cors_response(response)

    if trigger_total is None:
        response = jsonify({"ok": False, "error": "numeric_total_required"})
        response.status_code = 400
        return _btc_cors_response(response)

    if side == "BUY" and trigger_total < 100:
        response = jsonify({"ok": False, "error": "BUY_total_below_100M"})
        response.status_code = 400
        return _btc_cors_response(response)

    if side == "SELL" and trigger_total > -100:
        response = jsonify({"ok": False, "error": "SELL_total_above_minus_100M"})
        response.status_code = 400
        return _btc_cors_response(response)

    print(
        f"[BTC NETFLOW ALERT] {side} | {trigger_time} | {trigger_total:+.3f}M",
        flush=True,
    )

    def _send():
        ok = _send_btc_netflow_pushover(side, trigger_time, trigger_total)
        print(f"[BTC NETFLOW PUSHOVER QUEUED] side={side} ok={ok}", flush=True)

    threading.Thread(target=_send, daemon=True).start()

    response = jsonify({
        "ok": True,
        "mode": "btc_5m_netflow_pushover",
        "side": side,
        "time": trigger_time,
        "total": trigger_total,
    })
    response.status_code = 200
    return _btc_cors_response(response)
