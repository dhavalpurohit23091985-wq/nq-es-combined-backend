import asyncio
import json
import os
import time
from collections import deque

import requests
import websockets

PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY", "").strip()
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN", "").strip()
THRESHOLD_USD = float(os.getenv("DIRECT_BTC_THRESHOLD_USD", "5000000"))

BITGET_WS = "wss://ws.bitget.com/v3/ws/public"
ASTER_WS = "wss://fstream.asterdex.com/ws/btcusdt@forceOrder"
COINEX_URL = "https://api.coinex.com/v2/futures/liquidation-history"
LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
LIGHTER_ORDERBOOKS_URL = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"

COINEX_POLL_SECONDS = 5
COINEX_LOOKBACK_MS = 60_000
SEEN_LIMIT = 20_000

totals = {"long": 0.0, "short": 0.0}
by_exchange = {
    "bitget": {"long": 0.0, "short": 0.0},
    "aster": {"long": 0.0, "short": 0.0},
    "coinex": {"long": 0.0, "short": 0.0},
    "lighter": {"long": 0.0, "short": 0.0},
}

lock = asyncio.Lock()
seen_queue = deque(maxlen=SEEN_LIMIT)
seen_set = set()


def remember_event(key):
    if key in seen_set:
        return False
    if len(seen_queue) == seen_queue.maxlen:
        old = seen_queue.popleft()
        seen_set.discard(old)
    seen_queue.append(key)
    seen_set.add(key)
    return True


def usd(x):
    return f"${x:,.2f}"


def send_pushover(title, message):
    if not PUSHOVER_USER_KEY or not PUSHOVER_API_TOKEN:
        print("[PUSHOVER] Missing credentials", flush=True)
        return False
    try:
        r = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": PUSHOVER_API_TOKEN,
                "user": PUSHOVER_USER_KEY,
                "title": title,
                "message": message,
                "priority": 1,
            },
            timeout=20,
        )
        print(f"[PUSHOVER] status={r.status_code} body={r.text[:300]}", flush=True)
        return r.ok
    except Exception as e:
        print(f"[PUSHOVER ERROR] {type(e).__name__}: {e}", flush=True)
        return False


async def add_liquidation(exchange, side, notional_usd, event_key):
    if side not in ("long", "short") or notional_usd <= 0:
        return
    if not remember_event(event_key):
        return

    async with lock:
        totals[side] += notional_usd
        by_exchange[exchange][side] += notional_usd

        print(
            f"[EVENT] {exchange.upper()} {side.upper()} {usd(notional_usd)} | "
            f"TOTAL L={usd(totals['long'])} S={usd(totals['short'])}",
            flush=True,
        )

        long_hit = totals["long"] >= THRESHOLD_USD
        short_hit = totals["short"] >= THRESHOLD_USD
        if not (long_hit or short_hit):
            return

        long_total = totals["long"]
        short_total = totals["short"]
        gap = abs(long_total - short_total)

        if long_hit and short_hit:
            winner = "LONG" if long_total >= short_total else "SHORT"
            title = f"BTC DIRECT BOTH HIT +{THRESHOLD_USD/1_000_000:g}M ({winner} HIGHER)"
        elif long_hit:
            title = f"BTC DIRECT LONG WINS +{THRESHOLD_USD/1_000_000:g}M"
        else:
            title = f"BTC DIRECT SHORT WINS +{THRESHOLD_USD/1_000_000:g}M"

        message = (
            f"LONG: {usd(long_total)}\n"
            f"SHORT: {usd(short_total)}\n"
            f"GAP: {usd(gap)}\n\n"
            f"Bitget  L {usd(by_exchange['bitget']['long'])} | S {usd(by_exchange['bitget']['short'])}\n"
            f"Aster   L {usd(by_exchange['aster']['long'])} | S {usd(by_exchange['aster']['short'])}\n"
            f"CoinEx  L {usd(by_exchange['coinex']['long'])} | S {usd(by_exchange['coinex']['short'])}\n"
            f"Lighter L {usd(by_exchange['lighter']['long'])} | S {usd(by_exchange['lighter']['short'])}"
        )
        await asyncio.to_thread(send_pushover, title, message)

        totals["long"] = 0.0
        totals["short"] = 0.0
        for ex in by_exchange:
            by_exchange[ex]["long"] = 0.0
            by_exchange[ex]["short"] = 0.0
        print("[CYCLE RESET] cumulative totals reset after alert", flush=True)


async def bitget_heartbeat(ws):
    while True:
        await asyncio.sleep(25)
        try:
            await ws.send("ping")
        except Exception:
            return


async def bitget_loop():
    subscribe = {
        "op": "subscribe",
        "args": [{"instType": "usdt-futures", "topic": "liquidation"}],
    }
    while True:
        try:
            print("[BITGET] connecting...", flush=True)
            async with websockets.connect(
                BITGET_WS, open_timeout=20, close_timeout=10,
                ping_interval=None, max_size=4_000_000
            ) as ws:
                await ws.send(json.dumps(subscribe))
                print("[BITGET] subscribed", flush=True)
                hb = asyncio.create_task(bitget_heartbeat(ws))
                try:
                    async for raw in ws:
                        if raw == "pong":
                            continue
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        for event in msg.get("data") or []:
                            if str(event.get("symbol", "")).upper() != "BTCUSDT":
                                continue
                            raw_side = str(event.get("side", "")).lower()
                            side = "long" if raw_side == "buy" else "short" if raw_side == "sell" else None
                            if not side:
                                continue

                            # Bitget says liquidation amount is in quote coin.
                            # For BTCUSDT that means USDT-like USD notional.
                            amount = float(event.get("amount") or 0)
                            ts = str(event.get("ts") or "")
                            price = str(event.get("price") or "")
                            key = f"bitget|{ts}|{side}|{price}|{amount}"
                            await add_liquidation("bitget", side, amount, key)
                finally:
                    hb.cancel()
        except Exception as e:
            print(f"[BITGET ERROR] {type(e).__name__}: {e}", flush=True)
            await asyncio.sleep(5)


async def aster_loop():
    while True:
        try:
            print("[ASTER] connecting...", flush=True)
            async with websockets.connect(
                ASTER_WS, open_timeout=20, close_timeout=10,
                ping_interval=180, ping_timeout=30, max_size=4_000_000
            ) as ws:
                print("[ASTER] subscribed btcusdt@forceOrder", flush=True)
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    order = msg.get("o") or {}
                    if str(order.get("s", "")).upper() != "BTCUSDT":
                        continue

                    forced_side = str(order.get("S", "")).upper()
                    side = "long" if forced_side == "SELL" else "short" if forced_side == "BUY" else None
                    if not side:
                        continue

                    avg_price = float(order.get("ap") or 0)
                    price = avg_price if avg_price > 0 else float(order.get("p") or 0)
                    filled_qty = float(order.get("z") or 0)
                    if filled_qty <= 0:
                        filled_qty = float(order.get("l") or 0)
                    if filled_qty <= 0:
                        filled_qty = float(order.get("q") or 0)

                    notional = price * filled_qty
                    ts = str(order.get("T") or msg.get("E") or "")
                    key = f"aster|{ts}|{forced_side}|{price}|{filled_qty}"
                    await add_liquidation("aster", side, notional, key)
        except Exception as e:
            print(f"[ASTER ERROR] {type(e).__name__}: {e}", flush=True)
            await asyncio.sleep(5)


async def coinex_poll_once(session):
    now_ms = int(time.time() * 1000)
    params = {
        "market": "BTCUSDT",
        "start_time": now_ms - COINEX_LOOKBACK_MS,
        "end_time": now_ms,
        "page": 1,
        "limit": 100,
    }

    r = await asyncio.to_thread(session.get, COINEX_URL, params=params, timeout=20)
    r.raise_for_status()
    payload = r.json()
    if payload.get("code") != 0:
        raise RuntimeError(f"CoinEx response: {payload}")

    for event in payload.get("data") or []:
        if str(event.get("market", "")).strip().upper() != "BTCUSDT":
            continue
        side = str(event.get("side", "")).lower()
        if side not in ("long", "short"):
            continue

        price = float(event.get("liq_price") or 0)
        amount = float(event.get("liq_amount") or 0)
        notional = price * amount

        ts = str(event.get("created_at") or "")
        bkr = str(event.get("bkr_price") or "")
        key = f"coinex|{ts}|{side}|{price}|{amount}|{bkr}"
        await add_liquidation("coinex", side, notional, key)


async def coinex_loop():
    session = requests.Session()
    while True:
        try:
            await coinex_poll_once(session)
        except Exception as e:
            print(f"[COINEX ERROR] {type(e).__name__}: {e}", flush=True)
        await asyncio.sleep(COINEX_POLL_SECONDS)


def _lighter_find_btc_market_id(payload):
    """Return BTC perpetual market_id from Lighter orderBooks metadata."""
    books = payload.get("order_books") or payload.get("orderBooks") or payload.get("data") or []
    if isinstance(books, dict):
        books = books.get("order_books") or books.get("orderBooks") or books.get("data") or []

    for item in books:
        if not isinstance(item, dict):
            continue

        symbol = str(item.get("symbol") or "").upper().strip()
        market_type = str(item.get("market_type") or "").lower().strip()

        # Lighter perpetual metadata uses raw symbol such as BTC.
        # Accept common symbol variants but avoid spot-style BTC/...
        is_btc = symbol in {"BTC", "BTC-USD", "BTCUSD", "BTC-PERP"} or (
            symbol.startswith("BTC") and "/" not in symbol
        )
        if not is_btc:
            continue

        # Prefer perpetual market when market_type is present.
        if market_type and "spot" in market_type:
            continue

        market_id = item.get("market_id")
        if market_id is None:
            market_id = item.get("market_index")
        if market_id is not None:
            return int(market_id)

    raise RuntimeError("Could not discover Lighter BTC perpetual market_id")


async def lighter_get_btc_market_id():
    def fetch():
        r = requests.get(LIGHTER_ORDERBOOKS_URL, timeout=20)
        r.raise_for_status()
        return _lighter_find_btc_market_id(r.json())

    return await asyncio.to_thread(fetch)


async def lighter_heartbeat(ws):
    while True:
        await asyncio.sleep(60)
        try:
            await ws.send(json.dumps({"type": "ping"}))
        except Exception:
            return


async def lighter_loop():
    while True:
        try:
            market_id = await lighter_get_btc_market_id()
            print(f"[LIGHTER] BTC market_id={market_id}", flush=True)
            print("[LIGHTER] connecting...", flush=True)

            async with websockets.connect(
                LIGHTER_WS,
                open_timeout=20,
                close_timeout=10,
                ping_interval=None,
                max_size=4_000_000,
            ) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "channel": f"trade/{market_id}",
                }))
                print(f"[LIGHTER] subscribed trade/{market_id}", flush=True)

                hb = asyncio.create_task(lighter_heartbeat(ws))
                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if msg.get("type") == "pong":
                            continue

                        for trade in msg.get("liquidation_trades") or []:
                            if not isinstance(trade, dict):
                                continue

                            if int(trade.get("market_id", market_id)) != market_id:
                                continue

                            # Official Trade payload provides usd_amount directly.
                            notional = float(trade.get("usd_amount") or 0)
                            if notional <= 0:
                                continue

                            # For liquidation trades, use the taker's pre-trade
                            # position sign when present:
                            #   positive -> long position being liquidated
                            #   negative -> short position being liquidated
                            #
                            # If Lighter omits this field, skip rather than guess
                            # the side from is_maker_ask. The raw event is logged
                            # so we can inspect the live payload and safely add a
                            # fallback later if needed.
                            before_raw = trade.get("taker_position_size_before")
                            try:
                                before = float(before_raw)
                            except (TypeError, ValueError):
                                before = 0.0

                            if before > 0:
                                side = "long"
                            elif before < 0:
                                side = "short"
                            else:
                                print(
                                    "[LIGHTER SIDE UNRESOLVED] "
                                    f"trade_id={trade.get('trade_id_str') or trade.get('trade_id')} "
                                    f"usd={usd(notional)} "
                                    f"is_maker_ask={trade.get('is_maker_ask')} "
                                    f"taker_position_size_before={before_raw}",
                                    flush=True,
                                )
                                continue

                            trade_id = str(trade.get("trade_id_str") or trade.get("trade_id") or "")
                            ts = str(trade.get("timestamp") or "")
                            tx_hash = str(trade.get("tx_hash") or "")
                            key = f"lighter|{market_id}|{trade_id}|{ts}|{tx_hash}"

                            await add_liquidation("lighter", side, notional, key)
                finally:
                    hb.cancel()

        except Exception as e:
            print(f"[LIGHTER ERROR] {type(e).__name__}: {e}", flush=True)
            await asyncio.sleep(5)


async def status_loop():
    while True:
        await asyncio.sleep(60)
        async with lock:
            print(
                "[STATUS] "
                f"threshold={usd(THRESHOLD_USD)} "
                f"LONG={usd(totals['long'])} SHORT={usd(totals['short'])} | "
                f"Bitget(L={usd(by_exchange['bitget']['long'])},S={usd(by_exchange['bitget']['short'])}) "
                f"Aster(L={usd(by_exchange['aster']['long'])},S={usd(by_exchange['aster']['short'])}) "
                f"CoinEx(L={usd(by_exchange['coinex']['long'])},S={usd(by_exchange['coinex']['short'])}) "
                f"Lighter(L={usd(by_exchange['lighter']['long'])},S={usd(by_exchange['lighter']['short'])})",
                flush=True,
            )


async def main():
    print("DIRECT BTC LIQUIDATION WORKER STARTING", flush=True)
    print(f"Threshold: {usd(THRESHOLD_USD)}", flush=True)
    print("Sources: Bitget + Aster + CoinEx + Lighter", flush=True)
    await asyncio.gather(
        bitget_loop(),
        aster_loop(),
        coinex_loop(),
        lighter_loop(),
        status_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
