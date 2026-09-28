        ok = _send_btc_netflow_pushover(
            side=side,
            trigger_time=trigger_time,
            trigger_total=trigger_total,
            current_time=current_time,
            last_data=last_data,
            data_delay=data_delay,
            value=value,
            running=running,
            threshold=threshold,
            state=state,
            next_valid=next_valid,
            last_trigger_type=last_trigger_type,
            last_trigger_time=last_trigger_time,
            last_trigger_total=last_trigger_total,
            history_5m=history_5m,
            auto_refresh=auto_refresh,
        )

        print(
            f"[BTC NETFLOW PUSHOVER] side={side} ok={ok}",
            flush=True,
        )

    threading.Thread(
        target=_send,
        daemon=True,
    ).start()

    response = jsonify({
        "ok": True,
        "mode": "btc_5m_netflow_pushover",
        "side": side,
        "time": trigger_time,
        "total": trigger_total,
        "current_time": current_time,
        "last_data": last_data,
        "data_delay": data_delay,
        "value": value,
        "running": running,
        "state": state,
        "next_valid": next_valid,
    })

    response.status_code = 200
    return _btc_cors_response(response)

# ============================================================
# COINGLASS BTC 5M API DIAGNOSTIC
# Temporary test only - does not affect existing systems.
# ============================================================

@app.get("/coinglass-btc5m-test")
def coinglass_btc5m_test():
    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    url = "https://capi.coinglass.com/api/moneyFlow/history"

    params = {
        "range": "5m",
        "symbol": "BTC",
        "type": "FUTURES",
    }

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Language": "en",
        "Origin": "https://www.coinglass.com",
        "Referer": "https://www.coinglass.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/153.0.0.0 Safari/537.36"
        ),
    }

    try:
        r = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=20,
        )

        content_type = r.headers.get("Content-Type", "")
        body_preview = r.text[:2000]

        parsed = None
        try:
            parsed = r.json()
        except Exception:
            pass

        return jsonify({
            "ok": r.ok,
            "http_status": r.status_code,
            "content_type": content_type,
            "final_url": r.url,
            "json": parsed,
            "body_preview": None if parsed is not None else body_preview,
        }), 200

    except requests.RequestException as exc:
        return jsonify({
            "ok": False,
            "error": "request_failed",
            "detail": str(exc),
        }), 500
