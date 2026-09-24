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
# Sources:
#   Bitget + Aster + CoinEx + Lighter
#
# IMPORTANT:
# - ONLY BTC + XAU are processed/forwarded.
# - XAU / XAUT / GOLD are combined into canonical XAU.
# - No ETH.
# - No SOL.
# - No ALL-CRYPTO forwarding.
# - Backend owns thresholding, alerts and cycle resets.
# ============================================================


COMBINED_BACKEND_URL = os.getenv(
    "COMBINED_BACKEND_URL",
    "https://nq-es-combined-backend.onrender.com",
).rstrip("/")

DIRECT_LIQ_SECRET = os.getenv(
    "DIRECT_LIQ_SECRET",
    "",
).strip()

DIRECT_EVENT_URL = (
    f"{COMBINED_BACKEND_URL}/direct-liquidation-event"
)

FORWARD_RETRIES = int(
    os.getenv("DIRECT_FORWARD_RETRIES", "3")
)

FORWARD_TIMEOUT_SECONDS = float(
    os.getenv("DIRECT_FORWARD_TIMEOUT_SECONDS", "20")
)


# ============================================================
# EXCHANGE ENDPOINTS
# ============================================================

BITGET_WS = "wss://ws.bitget.com/v3/ws/public"

ASTER_WS = (
    "wss://fstream.asterdex.com/ws/"
    "!forceOrder@arr"
)

COINEX_LIQ_URL = (
    "https://api.coinex.com/v2/"
    "futures/liquidation-history"
)

COINEX_MARKETS_URL = (
    "https://api.coinex.com/v2/"
    "futures/market"
)

LIGHTER_WS = (
    "wss://mainnet.zklighter.elliot.ai/"
    "stream?readonly=true"
)

LIGHTER_ORDERBOOKS_URL = (
    "https://mainnet.zklighter.elliot.ai/"
    "api/v1/orderBooks"
)


# ============================================================
# CONFIG
# ============================================================

COINEX_POLL_SECONDS = 5
COINEX_LOOKBACK_MS = 60_000

SEEN_LIMIT = 40_000

ASSETS = (
    "BTC",
    "XAU",
)

EXCHANGES = (
    "bitget",
    "aster",
    "coinex",
    "lighter",
)


# ============================================================
# LOCAL STATUS
# ============================================================

totals = {
    "BTC": {
        "long": 0.0,
        "short": 0.0,
    },
    "XAU": {
        "long": 0.0,
        "short": 0.0,
    },
}

by_exchange = {
    asset: {
        exchange: {
            "long": 0.0,
            "short": 0.0,
        }
        for exchange in EXCHANGES
    }
    for asset in ASSETS
}

lock = asyncio.Lock()

seen_queue = deque(
    maxlen=SEEN_LIMIT
)

seen_set = set()


# CoinEx:
# BTC is one market.
# Gold can have XAU / XAUT / GOLD family markets.
coinex_btc_market = "BTCUSDT"
coinex_gold_markets = []


# ============================================================
# HELPERS
# ============================================================

def usd(value):
    return f"${value:,.2f}"


def remember_event(key):
    if key in seen_set:
        return False

    if len(seen_queue) == seen_queue.maxlen:
        old = seen_queue.popleft()
        seen_set.discard(old)

    seen_queue.append(key)
    seen_set.add(key)

    return True


def classify_symbol(symbol):
    """
    ONLY BTC and gold-family symbols are accepted.
    Everything else returns None.
    """

    s = (
        str(symbol or "")
        .upper()
        .strip()
        .replace("-", "")
        .replace("_", "")
        .replace("/", "")
    )

    if (
        s == "BTC"
        or s.startswith("BTCUSDT")
        or s.startswith("BTCUSD")
        or s.startswith("BTCPERP")
    ):
        return "BTC"

    if (
        s.startswith("XAUT")
        or s.startswith("XAU")
        or s.startswith("GOLD")
    ):
        return "XAU"

    return None


# ============================================================
# BACKEND FORWARDING
# ============================================================

def forward_direct_event(
    asset,
    exchange,
    side,
    notional_usd,
    event_key,
):
    if not DIRECT_LIQ_SECRET:
        print(
            "[FORWARD ERROR] "
            "DIRECT_LIQ_SECRET is missing",
            flush=True,
        )
        return False

    payload = {
        "asset": asset,
        "exchange": exchange,
        "side": side,
        "notional_usd": float(
            notional_usd
        ),
        "event_key": str(
            event_key
        ),
    }

    headers = {
        "X-Direct-Liq-Secret":
            DIRECT_LIQ_SECRET,
        "Content-Type":
            "application/json",
    }

    attempts = max(
        1,
        FORWARD_RETRIES,
    )

    for attempt in range(
        1,
        attempts + 1,
    ):
        try:
            response = requests.post(
                DIRECT_EVENT_URL,
                json=payload,
                headers=headers,
                timeout=FORWARD_TIMEOUT_SECONDS,
            )

            if response.ok:
                try:
                    body = response.json()
                except Exception:
                    body = {}

                duplicate = bool(
                    body.get("duplicate")
                )

                print(
                    f"[FORWARDED] "
                    f"{asset} "
                    f"{exchange.upper()} "
                    f"{side.upper()} "
                    f"{usd(notional_usd)} | "
                    f"status={response.status_code}"
                    + (
                        " | backend_duplicate"
                        if duplicate
                        else ""
                    ),
                    flush=True,
                )

                return True

            print(
                f"[FORWARD ERROR] "
                f"{asset} "
                f"{exchange.upper()} "
                f"attempt={attempt}/{attempts} "
                f"status={response.status_code} "
                f"body={response.text[:300]}",
                flush=True,
            )

            # Validation/authentication errors
            # will not improve with retry.
            if 400 <= response.status_code < 500:
                return False

        except Exception as exc:
            print(
                f"[FORWARD ERROR] "
                f"{asset} "
                f"{exchange.upper()} "
                f"attempt={attempt}/{attempts} "
                f"{type(exc).__name__}: "
                f"{exc}",
                flush=True,
            )

        if attempt < attempts:
            time.sleep(
                min(
                    2 ** (attempt - 1),
                    5,
                )
            )

    return False


async def add_liquidation(
    asset,
    exchange,
    side,
    notional_usd,
    event_key,
):
    if asset not in ASSETS:
        return

    if exchange not in EXCHANGES:
        return

    if side not in (
        "long",
        "short",
    ):
        return

    try:
        notional_usd = float(
            notional_usd
        )
    except Exception:
        return

    if notional_usd <= 0:
        return

    # Local duplicate protection.
    # Backend also owns persistent dedupe.
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
        # Failed forwards are deliberately
        # NOT marked seen so replay/polling
        # can retry them.
        return

    if not remember_event(event_key):
        return

    async with lock:
        totals[asset][side] += (
            notional_usd
        )

        by_exchange[
            asset
        ][
            exchange
        ][
            side
        ] += notional_usd

        print(
            f"[LOCAL FORWARDED TOTAL] "
            f"{asset} "
            f"L={usd(totals[asset]['long'])} "
            f"S={usd(totals[asset]['short'])}",
            flush=True,
        )


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
                "instType":
                    "usdt-futures",
                "topic":
                    "liquidation",
            }
        ],
    }

    while True:
        try:
            print(
                "[BITGET] connecting...",
                flush=True,
            )

            async with websockets.connect(
                BITGET_WS,
                open_timeout=20,
                close_timeout=10,
                ping_interval=None,
                max_size=4_000_000,
            ) as ws:

                await ws.send(
                    json.dumps(subscribe)
                )

                print(
                    "[BITGET] subscribed "
                    "liquidation/usdt-futures | "
                    "processing BTC/XAU only",
                    flush=True,
                )

                hb = asyncio.create_task(
                    bitget_heartbeat(ws)
                )

                try:
                    async for raw in ws:
                        if raw == "pong":
                            continue

                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        for event in (
                            msg.get("data")
                            or []
                        ):
                            symbol = str(
                                event.get(
                                    "symbol"
                                )
                                or ""
                            ).upper()

                            asset = (
                                classify_symbol(
                                    symbol
                                )
                            )

                            # Ignore everything
                            # except BTC/XAU.
                            if asset is None:
                                continue

                            raw_side = str(
                                event.get(
                                    "side"
                                )
                                or ""
                            ).lower()

                            # Existing mapping preserved:
                            # buy = long liquidation
                            # sell = short liquidation
                            side = (
                                "long"
                                if raw_side == "buy"
                                else "short"
                                if raw_side == "sell"
                                else None
                            )

                            if not side:
                                continue

                            try:
                                amount = float(
                                    event.get(
                                        "amount"
                                    )
                                    or 0
                                )
                            except Exception:
                                continue

                            if amount <= 0:
                                continue

                            ts = str(
                                event.get("ts")
                                or ""
                            )

                            price = str(
                                event.get("price")
                                or ""
                            )

                            key = (
                                f"bitget|"
                                f"{asset}|"
                                f"{symbol}|"
                                f"{ts}|"
                                f"{side}|"
                                f"{price}|"
                                f"{amount}"
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

        except Exception as exc:
            print(
                f"[BITGET ERROR] "
                f"{type(exc).__name__}: "
                f"{exc}",
                flush=True,
            )

            await asyncio.sleep(5)


# ============================================================
# ASTER
# ============================================================

def iter_aster_force_orders(msg):
    if isinstance(msg, list):
        for item in msg:
            if isinstance(
                item,
                dict,
            ):
                yield item

        return

    if isinstance(msg, dict):
        yield msg


async def aster_loop():
    while True:
        try:
            print(
                "[ASTER] connecting "
                "all-market forceOrder | "
                "processing BTC/XAU only...",
                flush=True,
            )

            async with websockets.connect(
                ASTER_WS,
                open_timeout=20,
                close_timeout=10,
                ping_interval=180,
                ping_timeout=30,
                max_size=4_000_000,
            ) as ws:

                print(
                    "[ASTER] subscribed "
                    "!forceOrder@arr | "
                    "BTC/XAU filter active",
                    flush=True,
                )

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    for item in (
                        iter_aster_force_orders(
                            msg
                        )
                    ):
                        order = (
                            item.get("o")
                            or item
                        )

                        symbol = str(
                            order.get("s")
                            or ""
                        ).upper()

                        asset = (
                            classify_symbol(
                                symbol
                            )
                        )

                        # Ignore ETH/SOL/all others.
                        if asset is None:
                            continue

                        forced_side = str(
                            order.get("S")
                            or ""
                        ).upper()

                        # Existing mapping preserved:
                        # forced SELL closes long
                        # forced BUY closes short
                        side = (
                            "long"
                            if forced_side
                            == "SELL"
                            else "short"
                            if forced_side
                            == "BUY"
                            else None
                        )

                        if not side:
                            continue

                        try:
                            avg_price = float(
                                order.get("ap")
                                or 0
                            )

                            price = (
                                avg_price
                                if avg_price > 0
                                else float(
                                    order.get("p")
                                    or 0
                                )
                            )

                            filled_qty = float(
                                order.get("z")
                                or 0
                            )

                            if filled_qty <= 0:
                                filled_qty = float(
                                    order.get("l")
                                    or 0
                                )

                            if filled_qty <= 0:
                                filled_qty = float(
                                    order.get("q")
                                    or 0
                                )

                        except Exception:
                            continue

                        if (
                            price <= 0
                            or filled_qty <= 0
                        ):
                            continue

                        notional = (
                            price
                            * filled_qty
                        )

                        ts = str(
                            order.get("T")
                            or item.get("E")
                            or ""
                        )

                        key = (
                            f"aster|"
                            f"{asset}|"
                            f"{symbol}|"
                            f"{ts}|"
                            f"{forced_side}|"
                            f"{price}|"
                            f"{filled_qty}"
                        )

                        await add_liquidation(
                            asset,
                            "aster",
                            side,
                            notional,
                            key,
                        )

        except Exception as exc:
            print(
                f"[ASTER ERROR] "
                f"{type(exc).__name__}: "
                f"{exc}",
                flush=True,
            )

            await asyncio.sleep(5)


# ============================================================
# COINEX DISCOVERY
# ============================================================

def discover_coinex_markets():
    global coinex_btc_market
    global coinex_gold_markets

    try:
        response = requests.get(
            COINEX_MARKETS_URL,
            timeout=20,
        )

        response.raise_for_status()

        payload = response.json()

        if payload.get("code") != 0:
            raise RuntimeError(
                payload
            )

        btc_market = None
        gold_markets = []

        for item in (
            payload.get("data")
            or []
        ):
            if not isinstance(
                item,
                dict,
            ):
                continue

            market = str(
                item.get("market")
                or ""
            ).upper()

            base = str(
                item.get("base_ccy")
                or ""
            ).upper()

            quote = str(
                item.get("quote_ccy")
                or ""
            ).upper()

            available = item.get(
                "is_market_available"
            )

            if available is False:
                continue

            if market == "BTCUSDT":
                btc_market = market

            if (
                quote == "USDT"
                and base in {
                    "XAU",
                    "XAUT",
                    "GOLD",
                }
                and market
            ):
                gold_markets.append(
                    market
                )

        coinex_btc_market = (
            btc_market
            or "BTCUSDT"
        )

        coinex_gold_markets = list(
            dict.fromkeys(
                gold_markets
            )
        )

        print(
            "[COINEX] "
            f"BTC={coinex_btc_market} | "
            f"GOLD FAMILY="
            f"{coinex_gold_markets or 'NOT FOUND / SKIPPED'}",
            flush=True,
        )

    except Exception as exc:
        print(
            f"[COINEX MARKET ERROR] "
            f"{type(exc).__name__}: "
            f"{exc}",
            flush=True,
        )


async def coinex_poll_market(
    session,
    asset,
    market,
):
    now_ms = int(
        time.time() * 1000
    )

    params = {
        "market": market,
        "start_time":
            now_ms
            - COINEX_LOOKBACK_MS,
        "end_time": now_ms,
        "page": 1,
        "limit": 100,
    }

    response = await asyncio.to_thread(
        session.get,
        COINEX_LIQ_URL,
        params=params,
        timeout=20,
    )

    response.raise_for_status()

    payload = response.json()

    if payload.get("code") != 0:
        raise RuntimeError(
            f"CoinEx {asset} response: "
            f"{payload}"
        )

    for event in (
        payload.get("data")
        or []
    ):
        if (
            str(
                event.get("market")
                or ""
            ).upper()
            != market
        ):
            continue

        side = str(
            event.get("side")
            or ""
        ).lower()

        if side not in (
            "long",
            "short",
        ):
            continue

        try:
            price = float(
                event.get("liq_price")
                or 0
            )

            amount = float(
                event.get("liq_amount")
                or 0
            )

        except Exception:
            continue

        if (
            price <= 0
            or amount <= 0
        ):
            continue

        # Existing CoinEx calculation:
        # notional = price * base qty
        notional = (
            price
            * amount
        )

        ts = str(
            event.get("created_at")
            or ""
        )

        bkr = str(
            event.get("bkr_price")
            or ""
        )

        key = (
            f"coinex|"
            f"{asset}|"
            f"{market}|"
            f"{ts}|"
            f"{side}|"
            f"{price}|"
            f"{amount}|"
            f"{bkr}"
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

    await asyncio.to_thread(
        discover_coinex_markets
    )

    refresh_counter = 0

    while True:
        try:
            # BTC ONLY
            if coinex_btc_market:
                try:
                    await coinex_poll_market(
                        session,
                        "BTC",
                        coinex_btc_market,
                    )

                except Exception as exc:
                    print(
                        "[COINEX MARKET SKIP] "
                        f"BTC "
                        f"{coinex_btc_market} | "
                        f"{type(exc).__name__}: "
                        f"{exc}",
                        flush=True,
                    )

            # XAU/XAUT/GOLD ONLY
            for market in list(
                coinex_gold_markets
            ):
                try:
                    await coinex_poll_market(
                        session,
                        "XAU",
                        market,
                    )

                except Exception as exc:
                    print(
                        "[COINEX MARKET SKIP] "
                        f"XAU {market} | "
                        f"{type(exc).__name__}: "
                        f"{exc}",
                        flush=True,
                    )

        except Exception as exc:
            print(
                f"[COINEX ERROR] "
                f"{type(exc).__name__}: "
                f"{exc}",
                flush=True,
            )

        refresh_counter += 1

        # 720 × 5 sec ≈ 1 hour
        if refresh_counter >= 720:
            refresh_counter = 0

            await asyncio.to_thread(
                discover_coinex_markets
            )

        await asyncio.sleep(
            COINEX_POLL_SECONDS
        )


# ============================================================
# LIGHTER HELPERS
# ============================================================

def _lighter_books(payload):
    books = (
        payload.get("order_books")
        or payload.get("orderBooks")
        or payload.get("data")
        or []
    )

    if isinstance(
        books,
        dict,
    ):
        books = (
            books.get("order_books")
            or books.get("orderBooks")
            or books.get("data")
            or []
        )

    return (
        books
        if isinstance(books, list)
        else []
    )


def _lighter_market_id_for_btc(
    payload
):
    for item in _lighter_books(
        payload
    ):
        if not isinstance(
            item,
            dict,
        ):
            continue

        market_type = str(
            item.get("market_type")
            or ""
        ).lower().strip()

        if (
            market_type
            and "spot" in market_type
        ):
            continue

        symbol = str(
            item.get("symbol")
            or ""
        ).upper().strip()

        compact = (
            symbol
            .replace("-", "")
            .replace("_", "")
            .replace("/", "")
        )

        if not (
            compact == "BTC"
            or compact.startswith(
                "BTCUSD"
            )
            or compact.startswith(
                "BTCPERP"
            )
        ):
            continue

        market_id = item.get(
            "market_id"
        )

        if market_id is None:
            market_id = item.get(
                "market_index"
            )

        if market_id is not None:
            return int(
                market_id
            )

    return None


async def lighter_get_btc_market_id():
    def fetch():
        response = requests.get(
            LIGHTER_ORDERBOOKS_URL,
            timeout=20,
        )

        response.raise_for_status()

        payload = response.json()

        return (
            _lighter_market_id_for_btc(
                payload
            )
        )

    return await asyncio.to_thread(
        fetch
    )


async def lighter_get_gold_market_ids():
    def fetch():
        response = requests.get(
            LIGHTER_ORDERBOOKS_URL,
            timeout=20,
        )

        response.raise_for_status()

        payload = response.json()

        ids = []

        for item in _lighter_books(
            payload
        ):
            if not isinstance(
                item,
                dict,
            ):
                continue

            market_type = str(
                item.get("market_type")
                or ""
            ).lower().strip()

            if (
                market_type
                and "spot" in market_type
            ):
                continue

            symbol = str(
                item.get("symbol")
                or ""
            ).upper().strip()

            compact = (
                symbol
                .replace("-", "")
                .replace("_", "")
                .replace("/", "")
            )

            if not (
                compact.startswith("XAU")
                or compact.startswith("XAUT")
                or compact.startswith("GOLD")
            ):
                continue

            market_id = item.get(
                "market_id"
            )

            if market_id is None:
                market_id = item.get(
                    "market_index"
                )

            if market_id is not None:
                ids.append(
                    int(market_id)
                )

        return list(
            dict.fromkeys(ids)
        )

    return await asyncio.to_thread(
        fetch
    )


async def lighter_heartbeat(ws):
    while True:
        await asyncio.sleep(60)

        try:
            await ws.send(
                json.dumps(
                    {"type": "ping"}
                )
            )

        except Exception:
            return


# ============================================================
# LIGHTER BTC
# ============================================================

async def lighter_btc_loop():
    while True:
        try:
            market_id = (
                await lighter_get_btc_market_id()
            )

            if market_id is None:
                print(
                    "[LIGHTER] "
                    "BTC market not found; "
                    "rechecking later",
                    flush=True,
                )

                await asyncio.sleep(300)
                continue

            print(
                f"[LIGHTER] BTC "
                f"market_id={market_id}",
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
                            "type":
                                "subscribe",
                            "channel":
                                f"trade/{market_id}",
                        }
                    )
                )

                print(
                    "[LIGHTER] "
                    f"BTC subscribed "
                    f"trade/{market_id}",
                    flush=True,
                )

                hb = asyncio.create_task(
                    lighter_heartbeat(ws)
                )

                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(
                                raw
                            )
                        except Exception:
                            continue

                        if (
                            msg.get("type")
                            == "pong"
                        ):
                            continue

                        for trade in (
                            msg.get(
                                "liquidation_trades"
                            )
                            or []
                        ):
                            if not isinstance(
                                trade,
                                dict,
                            ):
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

                            if (
                                event_market_id
                                != market_id
                            ):
                                continue

                            try:
                                notional = float(
                                    trade.get(
                                        "usd_amount"
                                    )
                                    or 0
                                )
                            except Exception:
                                continue

                            if notional <= 0:
                                continue

                            before_raw = (
                                trade.get(
                                    "taker_position_size_before"
                                )
                            )

                            try:
                                before = float(
                                    before_raw
                                )
                            except (
                                TypeError,
                                ValueError,
                            ):
                                before = 0.0

                            if before > 0:
                                side = "long"

                            elif before < 0:
                                side = "short"

                            else:
                                continue

                            trade_id = str(
                                trade.get(
                                    "trade_id_str"
                                )
                                or trade.get(
                                    "trade_id"
                                )
                                or ""
                            )

                            ts = str(
                                trade.get(
                                    "timestamp"
                                )
                                or ""
                            )

                            tx_hash = str(
                                trade.get(
                                    "tx_hash"
                                )
                                or ""
                            )

                            key = (
                                f"lighter|BTC|"
                                f"{market_id}|"
                                f"{trade_id}|"
                                f"{ts}|"
                                f"{tx_hash}"
                            )

                            await add_liquidation(
                                "BTC",
                                "lighter",
                                side,
                                notional,
                                key,
                            )

                finally:
                    hb.cancel()

        except Exception as exc:
            print(
                f"[LIGHTER BTC ERROR] "
                f"{type(exc).__name__}: "
                f"{exc}",
                flush=True,
            )

            await asyncio.sleep(5)


# ============================================================
# LIGHTER XAU / XAUT / GOLD
# ============================================================

async def lighter_gold_family_loop():
    while True:
        try:
            market_ids = (
                await lighter_get_gold_market_ids()
            )

            if not market_ids:
                print(
                    "[LIGHTER] "
                    "XAU/XAUT/GOLD markets "
                    "not found; "
                    "rechecking later",
                    flush=True,
                )

                await asyncio.sleep(300)
                continue

            market_id_set = set(
                market_ids
            )

            print(
                "[LIGHTER] "
                f"GOLD FAMILY "
                f"market_ids={market_ids}",
                flush=True,
            )

            async with websockets.connect(
                LIGHTER_WS,
                open_timeout=20,
                close_timeout=10,
                ping_interval=None,
                max_size=4_000_000,
            ) as ws:

                for market_id in market_ids:
                    await ws.send(
                        json.dumps(
                            {
                                "type":
                                    "subscribe",
                                "channel":
                                    f"trade/{market_id}",
                            }
                        )
                    )

                print(
                    "[LIGHTER] "
                    "GOLD FAMILY subscribed "
                    f"markets={market_ids}",
                    flush=True,
                )

                hb = asyncio.create_task(
                    lighter_heartbeat(ws)
                )

                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(
                                raw
                            )
                        except Exception:
                            continue

                        if (
                            msg.get("type")
                            == "pong"
                        ):
                            continue

                        for trade in (
                            msg.get(
                                "liquidation_trades"
                            )
                            or []
                        ):
                            if not isinstance(
                                trade,
                                dict,
                            ):
                                continue

                            try:
                                event_market_id = int(
                                    trade.get(
                                        "market_id"
                                    )
                                )
                            except Exception:
                                continue

                            if (
                                event_market_id
                                not in market_id_set
                            ):
                                continue

                            try:
                                notional = float(
                                    trade.get(
                                        "usd_amount"
                                    )
                                    or 0
                                )
                            except Exception:
                                continue

                            if notional <= 0:
                                continue

                            before_raw = (
                                trade.get(
                                    "taker_position_size_before"
                                )
                            )

                            try:
                                before = float(
                                    before_raw
                                )
                            except (
                                TypeError,
                                ValueError,
                            ):
                                before = 0.0

                            if before > 0:
                                side = "long"

                            elif before < 0:
                                side = "short"

                            else:
                                continue

                            trade_id = str(
                                trade.get(
                                    "trade_id_str"
                                )
                                or trade.get(
                                    "trade_id"
                                )
                                or ""
                            )

                            ts = str(
                                trade.get(
                                    "timestamp"
                                )
                                or ""
                            )

                            tx_hash = str(
                                trade.get(
                                    "tx_hash"
                                )
                                or ""
                            )

                            key = (
                                f"lighter|"
                                f"XAU_GOLD_FAMILY|"
                                f"{event_market_id}|"
                                f"{trade_id}|"
                                f"{ts}|"
                                f"{tx_hash}"
                            )

                            await add_liquidation(
                                "XAU",
                                "lighter",
                                side,
                                notional,
                                key,
                            )

                finally:
                    hb.cancel()

        except Exception as exc:
            print(
                "[LIGHTER GOLD FAMILY ERROR] "
                f"{type(exc).__name__}: "
                f"{exc}",
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
                    f"[STATUS] "
                    f"{asset} "
                    f"FORWARDED-SINCE-START "
                    f"LONG="
                    f"{usd(totals[asset]['long'])} "
                    f"SHORT="
                    f"{usd(totals[asset]['short'])} | "
                    f"Bitget("
                    f"L="
                    f"{usd(by_exchange[asset]['bitget']['long'])},"
                    f"S="
                    f"{usd(by_exchange[asset]['bitget']['short'])}) "
                    f"Aster("
                    f"L="
                    f"{usd(by_exchange[asset]['aster']['long'])},"
                    f"S="
                    f"{usd(by_exchange[asset]['aster']['short'])}) "
                    f"CoinEx("
                    f"L="
                    f"{usd(by_exchange[asset]['coinex']['long'])},"
                    f"S="
                    f"{usd(by_exchange[asset]['coinex']['short'])}) "
                    f"Lighter("
                    f"L="
                    f"{usd(by_exchange[asset]['lighter']['long'])},"
                    f"S="
                    f"{usd(by_exchange[asset]['lighter']['short'])})",
                    flush=True,
                )


# ============================================================
# MAIN
# ============================================================

async def main():
    print(
        "DIRECT BTC + XAU "
        "LIQUIDATION WORKER STARTING",
        flush=True,
    )

    print(
        f"Combined backend: "
        f"{DIRECT_EVENT_URL}",
        flush=True,
    )

    print(
        "MODE: BTC + XAU ONLY",
        flush=True,
    )

    print(
        "Sources: "
        "Bitget + Aster + "
        "CoinEx + Lighter",
        flush=True,
    )

    print(
        "XAU family: "
        "XAU + XAUT + GOLD "
        "-> canonical XAU",
        flush=True,
    )

    print(
        "ETH: OFF | "
        "SOL: OFF | "
        "ALL CRYPTO: OFF",
        flush=True,
    )

    print(
        "Thresholds/alerts/resets "
        "remain owned by app.py "
        "combined accumulator",
        flush=True,
    )

    await asyncio.gather(
        bitget_loop(),
        aster_loop(),
        coinex_loop(),
        lighter_btc_loop(),
        lighter_gold_family_loop(),
        status_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
