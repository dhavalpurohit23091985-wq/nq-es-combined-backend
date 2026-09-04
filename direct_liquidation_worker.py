import asyncio
import json
import os
import time
from collections import deque

import requests
import websockets

# ============================================================
# DIRECT BTC + XAU LIQUIDATION WORKER
#
# BTC direct sources:
#   Bitget + Aster + CoinEx + Lighter
#
# XAU direct sources:
#   Bitget XAUUSDT when available
#   Aster XAU/GOLD symbols from all-market forceOrder stream
#   CoinEx ONLY if a true XAU/GOLD futures market exists
#   Lighter XAU/GOLD market when available
#
# IMPORTANT:
# - CoinEx XAUTUSDT (Tether Gold token) is NOT mixed into XAU.
# - Unsupported XAU markets are skipped instead of guessed.
# ============================================================

PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY", "").strip()
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN", "").strip()

BTC_THRESHOLD_USD = float(os.getenv("DIRECT_BTC_THRESHOLD_USD", "5000000"))
XAU_THRESHOLD_USD = float(os.getenv("DIRECT_XAU_THRESHOLD_USD", "1000000"))

BITGET_WS = "wss://ws.bitget.com/v3/ws/public"

# Aster all-market force liquidation stream.
ASTER_WS = "wss://fstream.asterdex.com/ws/!forceOrder@arr"

COINEX_LIQ_URL = "https://api.coinex.com/v2/futures/liquidation-history"
COINEX_MARKETS_URL = "https://api.coinex.com/v2/futures/market"

LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
LIGHTER_ORDERBOOKS_URL = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"

COINEX_POLL_SECONDS = 5
COINEX_LOOKBACK_MS = 60_000
SEEN_LIMIT = 40_000

ASSETS = ("BTC", "XAU")
EXCHANGES = ("bitget", "aster", "coinex", "lighter")

totals = {
    "BTC": {"long": 0.0, "short": 0.0},
    "XAU": {"long": 0.0, "short": 0.0},
}

by_exchange = {
    asset: {
        ex: {"long": 0.0, "short": 0.0}
        for ex in EXCHANGES
    }
    for asset in ASSETS
}

lock = asyncio.Lock()

seen_queue = deque(maxlen=SEEN_LIMIT)
seen_set = set()

coinex_markets = {
    "BTC": "BTCUSDT",
    "XAU": None,
}


# ============================================================
# HELPERS
# ============================================================

def usd(x):
    return f"${x:,.2f}"


def threshold_for(asset):
    return BTC_THRESHOLD_USD if asset == "BTC" else XAU_THRESHOLD_USD


def remember_event(key):
    if key in seen_set:
        return False

    if len(seen_queue) == seen_queue.maxlen:
        old = seen_queue.popleft()
        seen_set.discard(old)

    seen_queue.append(key)
    seen_set.add(key)
    return True


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
        print(
            f"[PUSHOVER] status={r.status_code} body={r.text[:300]}",
            flush=True,
        )
        return r.ok
    except Exception as e:
        print(
            f"[PUSHOVER ERROR] {type(e).__name__}: {e}",
            flush=True,
        )
        return False


def reset_asset_cycle(asset):
    totals[asset]["long"] = 0.0
    totals[asset]["short"] = 0.0

    for ex in EXCHANGES:
        by_exchange[asset][ex]["long"] = 0.0
        by_exchange[asset][ex]["short"] = 0.0


async def add_liquidation(asset, exchange, side, notional_usd, event_key):
    if asset not in ASSETS:
        return

    if exchange not in EXCHANGES:
        return

    if side not in ("long", "short"):
        return

    try:
        notional_usd = float(notional_usd)
    except Exception:
        return

    if notional_usd <= 0:
        return

    if not remember_event(event_key):
        return

    async with lock:
        totals[asset][side] += notional_usd
        by_exchange[asset][exchange][side] += notional_usd

        print(
            f"[EVENT] {asset} {exchange.upper()} {side.upper()} "
            f"{usd(notional_usd)} | "
            f"TOTAL L={usd(totals[asset]['long'])} "
            f"S={usd(totals[asset]['short'])}",
            flush=True,
        )

        threshold = threshold_for(asset)
        long_hit = totals[asset]["long"] >= threshold
        short_hit = totals[asset]["short"] >= threshold

        if not (long_hit or short_hit):
            return

        long_total = totals[asset]["long"]
        short_total = totals[asset]["short"]
        gap = abs(long_total - short_total)
        threshold_m = threshold / 1_000_000

        if long_hit and short_hit:
            winner = "LONG" if long_total >= short_total else "SHORT"
            title = (
                f"{asset} DIRECT BOTH HIT +{threshold_m:g}M "
                f"({winner} HIGHER)"
            )
        elif long_hit:
            title = f"{asset} DIRECT LONG WINS +{threshold_m:g}M"
        else:
            title = f"{asset} DIRECT SHORT WINS +{threshold_m:g}M"

        lines = [
            f"LONG: {usd(long_total)}",
            f"SHORT: {usd(short_total)}",
            f"GAP: {usd(gap)}",
            "",
        ]

        for ex in EXCHANGES:
            lines.append(
                f"{ex.title():7s} "
                f"L {usd(by_exchange[asset][ex]['long'])} | "
                f"S {usd(by_exchange[asset][ex]['short'])}"
            )

        await asyncio.to_thread(
            send_pushover,
            title,
            "\n".join(lines),
        )

        reset_asset_cycle(asset)

        print(
            f"[CYCLE RESET] {asset} cumulative totals reset after alert",
            flush=True,
        )


# ============================================================
# SYMBOL CLASSIFICATION
# ============================================================

def classify_symbol(symbol):
    s = str(symbol or "").upper().replace("-", "").replace("_", "")

    if s == "BTCUSDT" or s.startswith("BTCUSDT"):
        return "BTC"

    # XAU only. We deliberately do NOT classify XAUTUSDT as XAU.
    if s.startswith("XAUT"):
        return None

    if s.startswith("XAUUSDT") or s.startswith("GOLDUSDT"):
        return "XAU"

    return None


# ============================================================
# BITGET
# ============================================================

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
        "args": [
            {
                "instType": "usdt-futures",
                "topic": "liquidation",
            }
        ],
    }

    while True:
        try:
            print("[BITGET] connecting...", flush=True)

            async with websockets.connect(
                BITGET_WS,
                open_timeout=20,
                close_timeout=10,
                ping_interval=None,
                max_size=4_000_000,
            ) as ws:
                await ws.send(json.dumps(subscribe))
                print(
                    "[BITGET] subscribed liquidation/usdt-futures "
                    "(BTC + XAU filter)",
                    flush=True,
                )

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
                            symbol = str(event.get("symbol") or "").upper()
                            asset = classify_symbol(symbol)

                            if not asset:
                                continue

                            raw_side = str(event.get("side") or "").lower()

                            # Official Bitget liquidation channel:
                            # buy  = long position liquidation
                            # sell = short position liquidation
                            side = (
                                "long"
                                if raw_side == "buy"
                                else "short"
                                if raw_side == "sell"
                                else None
                            )

                            if not side:
                                continue

                            # Bitget documents amount in quote coin.
                            # BTCUSDT / XAUUSDT quote coin is USDT,
                            # therefore amount is directly USD-like notional.
                            amount = float(event.get("amount") or 0)

                            ts = str(event.get("ts") or "")
                            price = str(event.get("price") or "")

                            key = (
                                f"bitget|{asset}|{symbol}|{ts}|"
                                f"{side}|{price}|{amount}"
                            )

                            await add_liquidation(
                                asset,
                                "bitget",
                                side,
                                amount,
                                key,
                            )
                finally:
                    hb.cancel()

        except Exception as e:
            print(
                f"[BITGET ERROR] {type(e).__name__}: {e}",
                flush=True,
            )
            await asyncio.sleep(5)


# ============================================================
# ASTER
# ============================================================

def iter_aster_force_orders(msg):
    """
    Aster all-market forceOrder stream may arrive as:
    - list of forceOrder payloads
    - one forceOrder payload
    - wrapper containing order 'o'
    This helper accepts all common shapes.
    """
    if isinstance(msg, list):
        for item in msg:
            if isinstance(item, dict):
                yield item
        return

    if isinstance(msg, dict):
        yield msg


async def aster_loop():
    while True:
        try:
            print("[ASTER] connecting all-market forceOrder...", flush=True)

            async with websockets.connect(
                ASTER_WS,
                open_timeout=20,
                close_timeout=10,
                ping_interval=180,
                ping_timeout=30,
                max_size=4_000_000,
            ) as ws:
                print(
                    "[ASTER] subscribed !forceOrder@arr "
                    "(BTC + XAU/GOLD filter)",
                    flush=True,
                )

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    for item in iter_aster_force_orders(msg):
                        order = item.get("o") or item

                        symbol = str(order.get("s") or "").upper()
                        asset = classify_symbol(symbol)

                        if not asset:
                            continue

                        forced_side = str(order.get("S") or "").upper()

                        # Forced SELL closes a long.
                        # Forced BUY closes a short.
                        side = (
                            "long"
                            if forced_side == "SELL"
                            else "short"
                            if forced_side == "BUY"
                            else None
                        )

                        if not side:
                            continue

                        avg_price = float(order.get("ap") or 0)
                        price = (
                            avg_price
                            if avg_price > 0
                            else float(order.get("p") or 0)
                        )

                        filled_qty = float(order.get("z") or 0)

                        if filled_qty <= 0:
                            filled_qty = float(order.get("l") or 0)

                        if filled_qty <= 0:
                            filled_qty = float(order.get("q") or 0)

                        notional = price * filled_qty

                        ts = str(
                            order.get("T")
                            or item.get("E")
                            or ""
                        )

                        key = (
                            f"aster|{asset}|{symbol}|{ts}|"
                            f"{forced_side}|{price}|{filled_qty}"
                        )

                        await add_liquidation(
                            asset,
                            "aster",
                            side,
                            notional,
                            key,
                        )

        except Exception as e:
            print(
                f"[ASTER ERROR] {type(e).__name__}: {e}",
                flush=True,
            )
            await asyncio.sleep(5)


# ============================================================
# COINEX MARKET DISCOVERY
# ============================================================

def discover_coinex_markets():
    """
    BTCUSDT is used for BTC.

    For XAU, only a true base currency XAU/GOLD market is accepted.
    XAUTUSDT is Tether Gold token and is deliberately excluded.
    """
    try:
        r = requests.get(
            COINEX_MARKETS_URL,
            timeout=20,
        )
        r.raise_for_status()
        payload = r.json()

        if payload.get("code") != 0:
            raise RuntimeError(payload)

        btc_market = None
        xau_market = None

        for item in payload.get("data") or []:
            if not isinstance(item, dict):
                continue

            market = str(item.get("market") or "").upper()
            base = str(item.get("base_ccy") or "").upper()
            quote = str(item.get("quote_ccy") or "").upper()
            available = item.get("is_market_available")

            if available is False:
                continue

            if market == "BTCUSDT":
                btc_market = market

            if (
                quote == "USDT"
                and base in {"XAU", "GOLD"}
                and not market.startswith("XAUT")
            ):
                xau_market = market

        coinex_markets["BTC"] = btc_market or "BTCUSDT"
        coinex_markets["XAU"] = xau_market

        print(
            f"[COINEX] BTC market={coinex_markets['BTC']} | "
            f"XAU market={coinex_markets['XAU'] or 'NOT FOUND / SKIPPED'}",
            flush=True,
        )

    except Exception as e:
        print(
            f"[COINEX MARKET ERROR] {type(e).__name__}: {e}",
            flush=True,
        )


async def coinex_poll_market(session, asset, market):
    now_ms = int(time.time() * 1000)

    params = {
        "market": market,
        "start_time": now_ms - COINEX_LOOKBACK_MS,
        "end_time": now_ms,
        "page": 1,
        "limit": 100,
    }

    r = await asyncio.to_thread(
        session.get,
        COINEX_LIQ_URL,
        params=params,
        timeout=20,
    )

    r.raise_for_status()
    payload = r.json()

    if payload.get("code") != 0:
        raise RuntimeError(
            f"CoinEx {asset} response: {payload}"
        )

    for event in payload.get("data") or []:
        if str(event.get("market") or "").upper() != market:
            continue

        side = str(event.get("side") or "").lower()

        if side not in ("long", "short"):
            continue

        price = float(event.get("liq_price") or 0)
        amount = float(event.get("liq_amount") or 0)

        # CoinEx linear USDT futures amount is base-asset quantity.
        notional = price * amount

        ts = str(event.get("created_at") or "")
        bkr = str(event.get("bkr_price") or "")

        key = (
            f"coinex|{asset}|{market}|{ts}|{side}|"
            f"{price}|{amount}|{bkr}"
        )

        await add_liquidation(
            asset,
            "coinex",
            side,
            notional,
            key,
        )


async def coinex_loop():
    session = requests.Session()

    await asyncio.to_thread(discover_coinex_markets)

    refresh_counter = 0

    while True:
        try:
            btc_market = coinex_markets.get("BTC")

            if btc_market:
                await coinex_poll_market(
                    session,
                    "BTC",
                    btc_market,
                )

            xau_market = coinex_markets.get("XAU")

            if xau_market:
                await coinex_poll_market(
                    session,
                    "XAU",
                    xau_market,
                )

        except Exception as e:
            print(
                f"[COINEX ERROR] {type(e).__name__}: {e}",
                flush=True,
            )

        refresh_counter += 1

        # Re-check XAU market periodically in case exchange adds it later.
        if refresh_counter >= 720:
            refresh_counter = 0
            await asyncio.to_thread(discover_coinex_markets)

        await asyncio.sleep(COINEX_POLL_SECONDS)


# ============================================================
# LIGHTER
# ============================================================

def _lighter_books(payload):
    books = (
        payload.get("order_books")
        or payload.get("orderBooks")
        or payload.get("data")
        or []
    )

    if isinstance(books, dict):
        books = (
            books.get("order_books")
            or books.get("orderBooks")
            or books.get("data")
            or []
        )

    return books if isinstance(books, list) else []


def _lighter_market_id_for_asset(payload, asset):
    for item in _lighter_books(payload):
        if not isinstance(item, dict):
            continue

        symbol = str(item.get("symbol") or "").upper().strip()
        market_type = str(
            item.get("market_type") or ""
        ).lower().strip()

        if market_type and "spot" in market_type:
            continue

        if asset == "BTC":
            matched = (
                symbol in {
                    "BTC",
                    "BTC-USD",
                    "BTCUSD",
                    "BTC-PERP",
                }
                or (
                    symbol.startswith("BTC")
                    and "/" not in symbol
                )
            )
        else:
            # XAU/GOLD only. Do not accept XAUT token.
            matched = (
                (
                    symbol.startswith("XAU")
                    and not symbol.startswith("XAUT")
                )
                or symbol.startswith("GOLD")
            )

        if not matched:
            continue

        market_id = item.get("market_id")

        if market_id is None:
            market_id = item.get("market_index")

        if market_id is not None:
            return int(market_id)

    return None


async def lighter_get_market_ids():
    def fetch():
        r = requests.get(
            LIGHTER_ORDERBOOKS_URL,
            timeout=20,
        )
        r.raise_for_status()
        payload = r.json()

        return {
            "BTC": _lighter_market_id_for_asset(
                payload,
                "BTC",
            ),
            "XAU": _lighter_market_id_for_asset(
                payload,
                "XAU",
            ),
        }

    return await asyncio.to_thread(fetch)


async def lighter_heartbeat(ws):
    while True:
        await asyncio.sleep(60)
        try:
            await ws.send(
                json.dumps({"type": "ping"})
            )
        except Exception:
            return


async def lighter_asset_loop(asset):
    while True:
        try:
            ids = await lighter_get_market_ids()
            market_id = ids.get(asset)

            if market_id is None:
                print(
                    f"[LIGHTER] {asset} market not found; "
                    f"skipping and rechecking later",
                    flush=True,
                )
                await asyncio.sleep(300)
                continue

            print(
                f"[LIGHTER] {asset} market_id={market_id}",
                flush=True,
            )

            async with websockets.connect(
                LIGHTER_WS,
                open_timeout=20,
                close_timeout=10,
                ping_interval=None,
                max_size=4_000_000,
            ) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "type": "subscribe",
                            "channel": f"trade/{market_id}",
                        }
                    )
                )

                print(
                    f"[LIGHTER] {asset} subscribed "
                    f"trade/{market_id}",
                    flush=True,
                )

                hb = asyncio.create_task(
                    lighter_heartbeat(ws)
                )

                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if msg.get("type") == "pong":
                            continue

                        for trade in (
                            msg.get("liquidation_trades")
                            or []
                        ):
                            if not isinstance(trade, dict):
                                continue

                            try:
                                event_market_id = int(
                                    trade.get(
                                        "market_id",
                                        market_id,
                                    )
                                )
                            except Exception:
                                continue

                            if event_market_id != market_id:
                                continue

                            notional = float(
                                trade.get("usd_amount")
                                or 0
                            )

                            if notional <= 0:
                                continue

                            before_raw = trade.get(
                                "taker_position_size_before"
                            )

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
                                    f"[LIGHTER SIDE UNRESOLVED] "
                                    f"{asset} "
                                    f"trade_id="
                                    f"{trade.get('trade_id_str') or trade.get('trade_id')} "
                                    f"usd={usd(notional)} "
                                    f"taker_position_size_before={before_raw}",
                                    flush=True,
                                )
                                continue

                            trade_id = str(
                                trade.get("trade_id_str")
                                or trade.get("trade_id")
                                or ""
                            )

                            ts = str(
                                trade.get("timestamp")
                                or ""
                            )

                            tx_hash = str(
                                trade.get("tx_hash")
                                or ""
                            )

                            key = (
                                f"lighter|{asset}|{market_id}|"
                                f"{trade_id}|{ts}|{tx_hash}"
                            )

                            await add_liquidation(
                                asset,
                                "lighter",
                                side,
                                notional,
                                key,
                            )
                finally:
                    hb.cancel()

        except Exception as e:
            print(
                f"[LIGHTER {asset} ERROR] "
                f"{type(e).__name__}: {e}",
                flush=True,
            )
            await asyncio.sleep(5)


# ============================================================
# STATUS
# ============================================================

async def status_loop():
    while True:
        await asyncio.sleep(60)

        async with lock:
            for asset in ASSETS:
                print(
                    f"[STATUS] {asset} "
                    f"threshold={usd(threshold_for(asset))} "
                    f"LONG={usd(totals[asset]['long'])} "
                    f"SHORT={usd(totals[asset]['short'])} | "
                    f"Bitget("
                    f"L={usd(by_exchange[asset]['bitget']['long'])},"
                    f"S={usd(by_exchange[asset]['bitget']['short'])}) "
                    f"Aster("
                    f"L={usd(by_exchange[asset]['aster']['long'])},"
                    f"S={usd(by_exchange[asset]['aster']['short'])}) "
                    f"CoinEx("
                    f"L={usd(by_exchange[asset]['coinex']['long'])},"
                    f"S={usd(by_exchange[asset]['coinex']['short'])}) "
                    f"Lighter("
                    f"L={usd(by_exchange[asset]['lighter']['long'])},"
                    f"S={usd(by_exchange[asset]['lighter']['short'])})",
                    flush=True,
                )


# ============================================================
# MAIN
# ============================================================

async def main():
    print(
        "DIRECT BTC + XAU LIQUIDATION WORKER STARTING",
        flush=True,
    )

    print(
        f"BTC Threshold: {usd(BTC_THRESHOLD_USD)}",
        flush=True,
    )

    print(
        f"XAU Threshold: {usd(XAU_THRESHOLD_USD)}",
        flush=True,
    )

    print(
        "BTC Sources: Bitget + Aster + CoinEx + Lighter",
        flush=True,
    )

    print(
        "XAU Sources: exchange-by-exchange auto-detect; "
        "unsupported markets skipped",
        flush=True,
    )

    await asyncio.gather(
        bitget_loop(),
        aster_loop(),
        coinex_loop(),
        lighter_asset_loop("BTC"),
        lighter_asset_loop("XAU"),
        status_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
