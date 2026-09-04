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

COINEX_POLL_SECONDS = 5
COINEX_LOOKBACK_MS = 60_000
SEEN_LIMIT = 20_000

totals = {"long": 0.0, "short": 0.0}
by_exchange = {
    "bitget": {"long": 0.0, "short": 0.0},
    "aster": {"long": 0.0, "short": 0.0},
    "coinex": {"long": 0.0, "short": 0.0},
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
            f"CoinEx  L {usd(by_exchange['coinex']['long'])} | S {usd(by_exchange['coinex']['short'])}"
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
                f"CoinEx(L={usd(by_exchange['coinex']['long'])},S={usd(by_exchange['coinex']['short'])})",
                flush=True,
            )


async def main():
    print("DIRECT BTC LIQUIDATION WORKER STARTING", flush=True)
    print(f"Threshold: {usd(THRESHOLD_USD)}", flush=True)
    print("Sources: Bitget + Aster + CoinEx", flush=True)
    await asyncio.gather(
        bitget_loop(),
        aster_loop(),
        coinex_loop(),
        status_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
