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

# Combined backend owns thresholding, alerting, and cycle resets.
COMBINED_BACKEND_URL = os.getenv(
    "COMBINED_BACKEND_URL",
    "https://nq-es-combined-backend.onrender.com",
).rstrip("/")
DIRECT_LIQ_SECRET = os.getenv("DIRECT_LIQ_SECRET", "").strip()
DIRECT_EVENT_URL = f"{COMBINED_BACKEND_URL}/direct-liquidation-event"
FORWARD_RETRIES = int(os.getenv("DIRECT_FORWARD_RETRIES", "3"))
FORWARD_TIMEOUT_SECONDS = float(os.getenv("DIRECT_FORWARD_TIMEOUT_SECONDS", "20"))

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
coinex_all_markets = []


# ============================================================
# HELPERS
# ============================================================

def usd(x):
    return f"${x:,.2f}"


def remember_event(key):
    if key in seen_set:
        return False

    if len(seen_queue) == seen_queue.maxlen:
        old = seen_queue.popleft()
        seen_set.discard(old)

    seen_queue.append(key)
    seen_set.add(key)
    return True


def crypto_base_symbol(symbol):
    """Normalize a futures pair to a base crypto symbol for ALL Crypto."""
    s = str(symbol or "").upper().strip().replace("-", "").replace("_", "").replace("/", "")
    for suffix in ("USDT", "USDC", "USD", "PERP"):
        if s.endswith(suffix) and len(s) > len(suffix):
            s = s[:-len(suffix)]
            break
    return s.strip()


def is_all_crypto_symbol(symbol):
    base = crypto_base_symbol(symbol)
    if not base:
        return False

    # Worker-side first filter only. app.py applies the authoritative MarginPad
    # crypto-universe whitelist before any Direct-4 amount reaches ALL Crypto.
    # Keep obvious traditional markets from generating unnecessary HTTP traffic.
    noncrypto = {
        # Metals / energy / indices
        "XAU", "GOLD", "XAG", "SILVER", "NQ", "ES", "SPX", "SP500",
        "DOW", "DJI", "NDX", "NASDAQ", "WTI", "BRENT", "CL", "NG",
        # Fiat currencies and common FX pair bases after quote stripping
        "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD",
        "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD",
        "NZDUSD", "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "CADJPY",
        # Known tokenized/traditional-equity symbols seen on multi-asset venues
        "CRCL",
    }
    if base in noncrypto:
        return False
    return True


def forward_direct_event(asset, exchange, side, notional_usd, event_key, *, symbol=None, ts_ms=None, price=None, verified_crypto=None):
    if not DIRECT_LIQ_SECRET:
        print(
            "[FORWARD ERROR] DIRECT_LIQ_SECRET is missing",
            flush=True,
        )
        return False

    payload = {
        "asset": asset,
        "exchange": exchange,
        "side": side,
        "notional_usd": float(notional_usd),
        "event_key": str(event_key),
    }
    if symbol is not None:
        payload["symbol"] = str(symbol)
    if ts_ms is not None:
        payload["ts_ms"] = ts_ms
    if price is not None:
        payload["price"] = price
    if verified_crypto is not None:
        payload["verified_crypto"] = bool(verified_crypto)
    headers = {
        "X-Direct-Liq-Secret": DIRECT_LIQ_SECRET,
        "Content-Type": "application/json",
    }

    attempts = max(1, FORWARD_RETRIES)
    for attempt in range(1, attempts + 1):
        try:
            r = requests.post(
                DIRECT_EVENT_URL,
                json=payload,
                headers=headers,
                timeout=FORWARD_TIMEOUT_SECONDS,
            )

            if r.ok:
                try:
                    body = r.json()
                except Exception:
                    body = {}

                duplicate = bool(body.get("duplicate"))
                print(
                    f"[FORWARDED] {asset} {exchange.upper()} {side.upper()} "
                    f"{usd(notional_usd)} | status={r.status_code}"
                    + (" | backend_duplicate" if duplicate else ""),
                    flush=True,
                )
                return True

            print(
                f"[FORWARD ERROR] {asset} {exchange.upper()} "
                f"attempt={attempt}/{attempts} status={r.status_code} "
                f"body={r.text[:300]}",
                flush=True,
            )

            # Authentication/validation errors will not improve with retry.
            if 400 <= r.status_code < 500:
                return False

        except Exception as e:
            print(
                f"[FORWARD ERROR] {asset} {exchange.upper()} "
                f"attempt={attempt}/{attempts} "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        if attempt < attempts:
            time.sleep(min(2 ** (attempt - 1), 5))

    return False


async def add_all_crypto_liquidation(symbol, exchange, side, notional_usd, event_key, *, ts_ms=None, price=None):
    """Forward one crypto liquidation to app.py's independent ALL Crypto 13EX state."""
    if exchange not in EXCHANGES or side not in ("long", "short"):
        return
    if not is_all_crypto_symbol(symbol):
        return
    try:
        notional_usd = float(notional_usd)
    except Exception:
        return
    if notional_usd <= 0:
        return
    base = crypto_base_symbol(symbol)
    all_key = f"all|{event_key}"
    await asyncio.to_thread(
        forward_direct_event,
        "ALL",
        exchange,
        side,
        notional_usd,
        all_key,
        symbol=base,
        ts_ms=ts_ms,
        price=price,
        verified_crypto=(exchange in {"bitget", "aster", "coinex", "lighter"}),
    )


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

    # Avoid local duplicate traffic. The app.py endpoint also keeps its own
    # persistent dedupe, so HTTP retries/restarts cannot double-count.
    if event_key in seen_set:
        return

    ok = await asyncio.to_thread(
        forward_direct_event,
        asset,
        exchange,
        side,
        notional_usd,
        event_key,
    )

    if not ok:
        # Do not mark failed forwards as seen. Polling/stream replay can retry.
        return

    if not remember_event(event_key):
        return

    async with lock:
        # Local totals are STATUS ONLY (forwarded since worker start).
        # They never trigger alerts or resets. app.py is the canonical state.
        totals[asset][side] += notional_usd
        by_exchange[asset][exchange][side] += notional_usd

        print(
            f"[LOCAL FORWARDED TOTAL] {asset} "
            f"L={usd(totals[asset]['long'])} "
            f"S={usd(totals[asset]['short'])}",
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
                    "(ALL crypto + BTC/XAU canonical paths)",
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

                            await add_all_crypto_liquidation(
                                symbol, "bitget", side, amount, key, ts_ms=ts, price=price
                            )
                            if asset:
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
                    "(ALL crypto + BTC/XAU canonical paths)",
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

                        await add_all_crypto_liquidation(
                            symbol, "aster", side, notional, key, ts_ms=ts, price=price
                        )
                        if asset:
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
    global coinex_all_markets
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
        all_markets = []

        for item in payload.get("data") or []:
            if not isinstance(item, dict):
                continue

            market = str(item.get("market") or "").upper()
            base = str(item.get("base_ccy") or "").upper()
            quote = str(item.get("quote_ccy") or "").upper()
            available = item.get("is_market_available")

            if available is False:
                continue

            if quote == "USDT" and market:
                all_markets.append((base or crypto_base_symbol(market), market))

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
        coinex_all_markets = all_markets

        print(
            f"[COINEX] BTC market={coinex_markets['BTC']} | "
            f"XAU market={coinex_markets['XAU'] or 'NOT FOUND / SKIPPED'} | ALL crypto markets={len(coinex_all_markets)}",
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

        await add_all_crypto_liquidation(
            market, "coinex", side, notional, key, ts_ms=ts, price=price
        )
        canonical_asset = classify_symbol(market)
        if canonical_asset:
            await add_liquidation(
                canonical_asset,
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
            # CoinEx requires a market on liquidation-history, so scan every
            # available USDT futures market. The endpoint is public and the
            # event-key dedupe prevents overlap from the rolling lookback.
            for base, market in list(coinex_all_markets):
                # One unsupported/invalid CoinEx market must NOT abort the
                # complete ALL-Crypto scan. Skip only that market and keep
                # polling the rest of the discovered futures universe.
                try:
                    await coinex_poll_market(
                        session,
                        base or crypto_base_symbol(market),
                        market,
                    )
                except Exception as market_error:
                    print(
                        f"[COINEX MARKET SKIP] market={market} "
                        f"{type(market_error).__name__}: {market_error}",
                        flush=True,
                    )
                    continue

        except Exception as e:
            print(
                f"[COINEX ERROR] {type(e).__name__}: {e}",
                flush=True,
            )

        refresh_counter += 1

        # Refresh the full CoinEx futures universe periodically.
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


async def lighter_get_all_crypto_markets():
    def fetch():
        r = requests.get(LIGHTER_ORDERBOOKS_URL, timeout=20)
        r.raise_for_status()
        payload = r.json()
        result = {}
        for item in _lighter_books(payload):
            if not isinstance(item, dict):
                continue
            market_type = str(item.get("market_type") or "").lower().strip()
            if market_type and "spot" in market_type:
                continue
            symbol = str(item.get("symbol") or "").upper().strip()
            if not is_all_crypto_symbol(symbol):
                continue
            market_id = item.get("market_id")
            if market_id is None:
                market_id = item.get("market_index")
            if market_id is None:
                continue
            result[int(market_id)] = crypto_base_symbol(symbol)
        return result
    return await asyncio.to_thread(fetch)


async def lighter_all_crypto_loop():
    while True:
        try:
            market_map = await lighter_get_all_crypto_markets()
            if not market_map:
                print("[LIGHTER ALL] no crypto markets found; rechecking", flush=True)
                await asyncio.sleep(300)
                continue
            print(f"[LIGHTER ALL] crypto markets={len(market_map)}", flush=True)
            async with websockets.connect(
                LIGHTER_WS,
                open_timeout=20,
                close_timeout=10,
                ping_interval=None,
                max_size=8_000_000,
            ) as ws:
                for market_id in market_map:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{market_id}"}))
                    await asyncio.sleep(0.02)
                hb = asyncio.create_task(lighter_heartbeat(ws))
                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if msg.get("type") == "pong":
                            continue
                        for trade in (msg.get("liquidation_trades") or []):
                            if not isinstance(trade, dict):
                                continue
                            try:
                                market_id = int(trade.get("market_id"))
                            except Exception:
                                continue
                            symbol = market_map.get(market_id)
                            if not symbol:
                                continue
                            try:
                                notional = float(trade.get("usd_amount") or 0)
                            except Exception:
                                continue
                            if notional <= 0:
                                continue
                            try:
                                before = float(trade.get("taker_position_size_before") or 0)
                            except Exception:
                                before = 0.0
                            side = "long" if before > 0 else "short" if before < 0 else None
                            if not side:
                                continue
                            trade_id = str(trade.get("trade_id_str") or trade.get("trade_id") or "")
                            ts = str(trade.get("timestamp") or "")
                            tx_hash = str(trade.get("tx_hash") or "")
                            key = f"lighter|ALL|{market_id}|{trade_id}|{ts}|{tx_hash}"
                            await add_all_crypto_liquidation(
                                symbol, "lighter", side, notional, key, ts_ms=ts
                            )
                finally:
                    hb.cancel()
        except Exception as e:
            print(f"[LIGHTER ALL ERROR] {type(e).__name__}: {e}", flush=True)
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
                    f"[STATUS] {asset} FORWARDED-SINCE-START "
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
        "DIRECT BTC + XAU + ALL CRYPTO LIQUIDATION WORKER STARTING",
        flush=True,
    )

    print(
        f"Combined backend: {DIRECT_EVENT_URL}",
        flush=True,
    )
    print(
        "Thresholds/alerts/resets are owned by app.py combined accumulator",
        flush=True,
    )

    print(
        "BTC Sources: Bitget + Aster + CoinEx + Lighter",
        flush=True,
    )

    print(
        "XAU Sources: exchange-by-exchange auto-detect; unsupported markets skipped",
        flush=True,
    )
    print(
        "ALL Crypto Direct Sources: Bitget + Aster + CoinEx + Lighter",
        flush=True,
    )

    await asyncio.gather(
        bitget_loop(),
        aster_loop(),
        coinex_loop(),
        lighter_asset_loop("BTC"),
        lighter_asset_loop("XAU"),
        lighter_all_crypto_loop(),
        status_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
