# FINAL CLEAN BACKEND: NQ + NIFTY + BANKNIFTY ONLY
# BTC/XAU/ETH/SOL/13EX/MarginPad/Coinalyze/direct-liquidation/MT5 liquidation code removed.

import os
import re
import json
import fcntl
import threading
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

PUSHOVER_TOKEN = os.environ.get("PUSHOVER_TOKEN")
PUSHOVER_USER = os.environ.get("PUSHOVER_USER")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")
PUSHOVER_URL = "https://api.pushover.net/1/messages.json"
LATEST_QQQ_LAST4_ENABLED = True

RUNTIME_STATE_FILE = os.path.join("/var/data", "backend_runtime_state.json")
NQ_TRIGGER_SERIAL_FILE = RUNTIME_STATE_FILE + ".nq_trigger_serial.json"
NIFTY_TRIGGER_SERIAL_FILE = RUNTIME_STATE_FILE + ".nifty_trigger_serial.json"
BANKNIFTY_TRIGGER_SERIAL_FILE = RUNTIME_STATE_FILE + ".banknifty_trigger_serial.json"

# ============================================================
# NQ LIVE 4-BASE DASHBOARD STATE
# ============================================================

NQ_DASHBOARD_STATE_FILE = os.path.join(
    "/var/data",
    "nq_live_4base_dashboard.json"
)

NQ_DASHBOARD_LOCK = threading.Lock()


def send_pushover(title, message):
    title_upper = str(title or "").upper()
    message_upper = str(message or "").upper()

    latest_qqq_last4 = (
        "NASDAQ" in title_upper
        and (
            "QQQ WEIGHTED" in title_upper
            or "QQQ WEIGHTED" in message_upper
            or "ROLLING LAST-4" in title_upper
            or "ROLLING LAST-4" in message_upper
            or "LAST-4" in title_upper
            or "LAST-4" in message_upper
            or "NASDAQ 10-STOCK" in title_upper
            or "FIXED 1H OPEN BASE" in message_upper
        )
    )

    # INDIA: allow BOTH existing LAST-4 alerts and current FIXED-1H
    # NIFTY/BANKNIFTY signal alerts through the main /webhook route.
    # Dashboard V2 uses /india-fixed1h-dashboard-webhook separately,
    # so its 1-minute UPDATE payloads do not create Pushover alerts here.
    nifty_allowed = (
        "NIFTY 10-STOCK" in title_upper
        and "BANKNIFTY" not in title_upper
    )

    banknifty_allowed = (
        "BANKNIFTY TOP-5" in title_upper
    )

    # NIY1: allow the dedicated 15-minute sequential alerts
    # through the existing main /webhook -> Pushover path.
    niy1_allowed = (
        "NIY1" in title_upper
    )

    if not (latest_qqq_last4 or nifty_allowed or banknifty_allowed or niy1_allowed):
        print(
            f"[PUSHOVER SILENT - NQ + NIFTY + BANKNIFTY + NIY1 ONLY] {title}",
            flush=True,
        )
        return False

    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        return False

    try:
        r = requests.post(
            PUSHOVER_URL,
            data={
                "token": PUSHOVER_TOKEN,
                "user": PUSHOVER_USER,
                "title": title,
                "message": message,
                "priority": 2,
                "retry": 30,
                "expire": 3600,
            },
            timeout=10,
        )
        return r.ok

    except requests.RequestException as exc:
        print(f"[PUSHOVER ERROR] {exc}", flush=True)
        return False


# ============================================================
# INDIA PERSISTENT TRIGGER NUMBER
# ============================================================

def _india_persistent_trigger_number(title, message):
    title_text = str(title or "")
    message_text = str(message or "")
    title_u = title_text.upper()
    message_u = message_text.upper()

    is_nifty = (
        "NIFTY 10-STOCK" in title_u
        and "NIFTY WEIGHTED" in title_u
        and (
            "LAST-4" in title_u
            or "LAST-4" in message_u
        )
    )

    is_banknifty = (
        "BANKNIFTY TOP-5" in title_u
        and "WEIGHTED" in title_u
        and (
            "LAST-4" in title_u
            or "LAST-4" in message_u
        )
    )

    if is_banknifty:
        asset = "BANKNIFTY"
        serial_file = BANKNIFTY_TRIGGER_SERIAL_FILE

    elif is_nifty:
        asset = "NIFTY"
        serial_file = NIFTY_TRIGGER_SERIAL_FILE

    else:
        return message_text, None, False

    match = re.search(
        r"(?i)\bTRIGGER\s*#\s*(\d+)",
        message_text,
    )

    incoming_serial = (
        int(match.group(1))
        if match
        else None
    )

    fingerprint_message = re.sub(
        r"(?i)\bTRIGGER\s*#\s*\d+",
        "TRIGGER #",
        message_text,
        count=1,
    )

    fingerprint = (
        title_text
        + "\n"
        + fingerprint_message
    )

    # Daily Indian-market backend cycle boundary: 09:15 IST.
    now_ist = datetime.now(
        ZoneInfo("Asia/Kolkata")
    )

    boundary = now_ist.replace(
        hour=9,
        minute=15,
        second=0,
        microsecond=0,
    )

    if now_ist < boundary:
        boundary -= timedelta(days=1)

    reset_cycle = boundary.strftime(
        "%Y-%m-%dT%H:%M%z"
    )

    os.makedirs(
        os.path.dirname(serial_file),
        exist_ok=True,
    )

    lock_path = serial_file + ".lock"

    with open(
        lock_path,
        "a+",
        encoding="utf-8",
    ) as lock_file:

        fcntl.flock(
            lock_file.fileno(),
            fcntl.LOCK_EX,
        )

        try:
            saved = {}

            try:
                with open(
                    serial_file,
                    "r",
                    encoding="utf-8",
                ) as f:
                    loaded = json.load(f)

                    if isinstance(loaded, dict):
                        saved = loaded

            except FileNotFoundError:
                pass

            except Exception as exc:
                print(
                    f"[{asset} TRIGGER DISK READ ERROR] {exc}",
                    flush=True,
                )

            saved_cycle = str(
                saved.get("reset_cycle")
                or ""
            )

            daily_reset = bool(
                saved_cycle
                and saved_cycle != reset_cycle
            )

            try:
                previous_serial = max(
                    0,
                    int(
                        saved.get(
                            "serial",
                            0,
                        )
                        or 0
                    ),
                )

            except (TypeError, ValueError):
                previous_serial = 0

            previous_fingerprint = str(
                saved.get(
                    "last_fingerprint"
                )
                or ""
            )

            if daily_reset:
                previous_serial = 0
                previous_fingerprint = ""

                print(
                    f"[{asset} TRIGGER DAILY RESET] cycle={reset_cycle}",
                    flush=True,
                )

            duplicate = bool(
                previous_fingerprint
                and previous_fingerprint
                == fingerprint
            )

            if duplicate:
                serial = previous_serial

            elif previous_serial <= 0:
                if daily_reset:
                    serial = 1

                else:
                    serial = (
                        incoming_serial
                        if incoming_serial
                        and incoming_serial > 0
                        else 1
                    )

            else:
                serial = previous_serial + 1

            if not duplicate:
                payload = {
                    "asset": asset,
                    "serial": serial,
                    "last_fingerprint": fingerprint,
                    "reset_cycle": reset_cycle,
                    "saved_at_utc": datetime.now(
                        timezone.utc
                    ).isoformat(),
                }

                tmp = (
                    serial_file
                    + f".{os.getpid()}.{threading.get_ident()}.tmp"
                )

                try:
                    with open(
                        tmp,
                        "w",
                        encoding="utf-8",
                    ) as f:
                        json.dump(
                            payload,
                            f,
                            separators=(",", ":"),
                            sort_keys=True,
                        )

                        f.flush()
                        os.fsync(f.fileno())

                    os.replace(
                        tmp,
                        serial_file,
                    )

                finally:
                    try:
                        if os.path.exists(tmp):
                            os.remove(tmp)

                    except OSError:
                        pass

            if match:
                rewritten = (
                    message_text[:match.start()]
                    + f"TRIGGER #{serial}"
                    + message_text[match.end():]
                )

            else:
                rewritten = (
                    f"TRIGGER #{serial} | "
                    + message_text
                )

            print(
                f"[{asset} PERSISTENT TRIGGER] #{serial} "
                f"| pine={incoming_serial if incoming_serial is not None else 'NA'} "
                f"| duplicate={duplicate} "
                f"| cycle={reset_cycle}",
                flush=True,
            )

            return (
                rewritten,
                serial,
                duplicate,
            )

        finally:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_UN,
            )


# ============================================================
# NQ PERSISTENT TRIGGER NUMBER
# ============================================================

def _nq_persistent_trigger_number(title, message):
    title_text = str(title or "")
    message_text = str(message or "")
    title_u = title_text.upper()
    message_u = message_text.upper()

    is_qqq_last4 = (
        "NASDAQ 10-STOCK" in title_u
        and "QQQ WEIGHTED" in title_u
        and (
            "LAST-4" in title_u
            or "LAST-4" in message_u
        )
    )

    if not is_qqq_last4:
        return message_text, None, False

    match = re.search(
        r"(?i)\bTRIGGER\s*#\s*(\d+)",
        message_text,
    )

    incoming_serial = (
        int(match.group(1))
        if match
        else None
    )

    fingerprint_message = re.sub(
        r"(?i)\bTRIGGER\s*#\s*\d+",
        "TRIGGER #",
        message_text,
        count=1,
    )

    fingerprint = (
        title_text
        + "\n"
        + fingerprint_message
    )

    os.makedirs(
        os.path.dirname(
            NQ_TRIGGER_SERIAL_FILE
        ),
        exist_ok=True,
    )

    lock_path = (
        NQ_TRIGGER_SERIAL_FILE
        + ".lock"
    )

    with open(
        lock_path,
        "a+",
        encoding="utf-8",
    ) as lock_file:

        fcntl.flock(
            lock_file.fileno(),
            fcntl.LOCK_EX,
        )

        try:
            saved = {}

            try:
                with open(
                    NQ_TRIGGER_SERIAL_FILE,
                    "r",
                    encoding="utf-8",
                ) as f:

                    loaded = json.load(f)

                    if isinstance(
                        loaded,
                        dict,
                    ):
                        saved = loaded

            except FileNotFoundError:
                pass

            except Exception as exc:
                print(
                    f"[NQ TRIGGER DISK READ ERROR] {exc}",
                    flush=True,
                )

            # Weekly NQ trigger cycle (IST):
            # Saturday 02:30 -> reset
            # Monday 05:30 -> first valid alert starts again at #1.
            now_ist = datetime.now(
                ZoneInfo("Asia/Kolkata")
            )

            days_since_saturday = (
                now_ist.weekday() - 5
            ) % 7

            reset_date = (
                now_ist
                - timedelta(
                    days=days_since_saturday
                )
            ).date()

            reset_ist = datetime.combine(
                reset_date,
                datetime.min.time(),
                tzinfo=ZoneInfo(
                    "Asia/Kolkata"
                ),
            ).replace(
                hour=2,
                minute=30,
            )

            if now_ist < reset_ist:
                reset_ist -= timedelta(
                    days=7
                )

            reset_cycle = reset_ist.strftime(
                "%Y-%m-%dT%H:%M%z"
            )

            saved_cycle = str(
                saved.get("reset_cycle")
                or ""
            )

            weekly_reset = bool(
                saved_cycle
                and saved_cycle
                != reset_cycle
            )

            try:
                previous_serial = max(
                    0,
                    int(
                        saved.get(
                            "serial",
                            0,
                        )
                        or 0
                    ),
                )

            except (TypeError, ValueError):
                previous_serial = 0

            previous_fingerprint = str(
                saved.get(
                    "last_fingerprint"
                )
                or ""
            )

            if weekly_reset:
                previous_serial = 0
                previous_fingerprint = ""

                print(
                    f"[NQ TRIGGER WEEKLY RESET] cycle={reset_cycle}",
                    flush=True,
                )

            duplicate = bool(
                previous_fingerprint
                and previous_fingerprint
                == fingerprint
            )

            if duplicate:
                serial = previous_serial

            elif previous_serial <= 0:
                serial = (
                    incoming_serial
                    if incoming_serial
                    and incoming_serial > 0
                    else 1
                )

            else:
                serial = (
                    previous_serial + 1
                )

            if not duplicate:
                payload = {
                    "serial": serial,
                    "last_fingerprint": fingerprint,
                    "reset_cycle": reset_cycle,
                    "saved_at_utc": datetime.now(
                        timezone.utc
                    ).isoformat(),
                }

                tmp = (
                    NQ_TRIGGER_SERIAL_FILE
                    + f".{os.getpid()}.{threading.get_ident()}.tmp"
                )

                try:
                    with open(
                        tmp,
                        "w",
                        encoding="utf-8",
                    ) as f:

                        json.dump(
                            payload,
                            f,
                            separators=(",", ":"),
                            sort_keys=True,
                        )

                        f.flush()
                        os.fsync(
                            f.fileno()
                        )

                    os.replace(
                        tmp,
                        NQ_TRIGGER_SERIAL_FILE,
                    )

                finally:
                    try:
                        if os.path.exists(tmp):
                            os.remove(tmp)

                    except OSError:
                        pass

            if match:
                rewritten = (
                    message_text[:match.start()]
                    + f"TRIGGER #{serial}"
                    + message_text[match.end():]
                )

            else:
                rewritten = (
                    f"TRIGGER #{serial} | "
                    + message_text
                )

            print(
                f"[NQ PERSISTENT TRIGGER] #{serial} "
                f"| pine={incoming_serial if incoming_serial is not None else 'NA'} "
                f"| duplicate={duplicate}",
                flush=True,
            )

            return (
                rewritten,
                serial,
                duplicate,
            )

        finally:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_UN,
            )


# ============================================================
# NQ DASHBOARD TRIGGER HISTORY
# Uses the EXISTING persistent NQ trigger serial.
# Does not change alert logic, threshold, or Pine trigger behavior.
# ============================================================

def _nq_dashboard_record_trigger(title, message, serial, duplicate=False):
    if serial is None or duplicate:
        return

    title_u = str(title or "").upper()
    text = str(message or "")

    if not (
        "NASDAQ 10-STOCK" in title_u
        and "QQQ WEIGHTED" in title_u
        and "LAST-4" in title_u
    ):
        return

    base_match = re.search(
        r"(?i)\bTRIGGER\s+BASE:\s*(?:\d{2}-\d{2}-\d{4}\s+)?(\d{1,2}:\d{2})",
        text,
    )

    if not base_match:
        print("[NQ DASHBOARD TRIGGER] Trigger base not found", flush=True)
        return

    base_time = base_match.group(1)

    trigger_match = re.search(
        r"(?i)\bTRIGGER:\s*([^|\n]+)",
        text,
    )

    trigger_text = (
        trigger_match.group(1).upper()
        if trigger_match
        else ""
    )

    # The new position/direction is the ENTRY side.
    if "BUY ENTRY" in trigger_text:
        direction = "BUY"
    elif "SELL ENTRY" in trigger_text:
        direction = "SELL"
    else:
        state_match = re.search(
            r"(?i)\bPOSITION\s+AFTER\s+ALERT:\s*(BUY|SELL)",
            text,
        )
        if state_match:
            direction = state_match.group(1).upper()
        elif " BUY" in (" " + title_u):
            direction = "BUY"
        elif " SELL" in (" " + title_u):
            direction = "SELL"
        else:
            print("[NQ DASHBOARD TRIGGER] Direction not found", flush=True)
            return

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ZoneInfo("Asia/Kolkata"))

    with NQ_DASHBOARD_LOCK:
        try:
            with open(
                NQ_DASHBOARD_STATE_FILE,
                "r",
                encoding="utf-8",
            ) as f:
                state = json.load(f)
                if not isinstance(state, dict):
                    state = {}
        except FileNotFoundError:
            state = {}
        except Exception as exc:
            print(f"[NQ DASHBOARD TRIGGER READ ERROR] {exc}", flush=True)
            state = {}

        history = state.get("trigger_history")
        if not isinstance(history, list):
            history = []

        # Avoid adding the same backend serial twice.
        if any(
            isinstance(item, dict)
            and int(item.get("serial", -1)) == int(serial)
            for item in history
            if str(item.get("serial", "")).isdigit()
        ):
            return

        history.append({
            "serial": int(serial),
            "direction": direction,
            "base_time": base_time,
            "created_at_utc": now_utc.isoformat(),
            "created_at_ist": now_ist.strftime("%d-%m-%Y %H:%M:%S"),
        })

        # Keep enough history for the current weekly cycle without
        # letting the dashboard file grow forever.
        history = history[-200:]
        state["trigger_history"] = history

        os.makedirs(
            os.path.dirname(NQ_DASHBOARD_STATE_FILE),
            exist_ok=True,
        )

        tmp = (
            NQ_DASHBOARD_STATE_FILE
            + f".{os.getpid()}.{threading.get_ident()}.tmp"
        )

        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    state,
                    f,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp, NQ_DASHBOARD_STATE_FILE)
        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass

    print(
        f"[NQ DASHBOARD TRIGGER] #{serial} {direction} | BASE {base_time}",
        flush=True,
    )


# ============================================================
# QQQ WEIGHTED AUDIT PUSHOVER SPLITTER
# ============================================================

def _qqq_weighted_audit_pushover_parts(title, message):
    title_u = str(
        title or ""
    ).upper()

    text = str(
        message or ""
    )

    if (
        "NASDAQ 10-STOCK"
        not in title_u
        or "QQQ WEIGHTED"
        not in title_u
    ):
        return [
            (
                str(title or ""),
                text,
            )
        ]

    stock_names = {
        "NVDA",
        "AAPL",
        "MSFT",
        "MU",
        "AMZN",
        "AMD",
        "GOOGL",
        "META",
        "GOOG",
        "TSLA",
    }

    normalized = (
        text
        .replace("\r", "")
        .replace("\n", " | ")
    )

    tokens = [
        part.strip()
        for part
        in normalized.split("|")
        if part.strip()
    ]

    metadata = []

    wanted_prefixes = (
        "TRIGGER #",
        "TRIGGER BASE:",
        "WEIGHTED BASKET NET:",
        "STATE:",
        "BASE 15:30 ENTRY CACHE:",
        "POSITION AFTER ALERT:",
    )

    for token in tokens:
        if token.upper().startswith(
            wanted_prefixes
        ):
            if token not in metadata:
                metadata.append(token)

    stock_rows = []
    i = 0

    while i < len(tokens):
        symbol = tokens[i].upper()

        if symbol not in stock_names:
            i += 1
            continue

        vals = {}
        j = i + 1

        while (
            j < len(tokens)
            and tokens[j].upper()
            not in stock_names
        ):
            p = tokens[j]
            up = p.upper()

            if up.startswith("OPEN "):
                vals["O"] = p[5:].strip()

            elif up.startswith("LIVE "):
                vals["T"] = p[5:].strip()

            elif up.startswith("TRIGGER "):
                vals["T"] = p[8:].strip()

            elif up.startswith("RAW "):
                vals["R"] = p[4:].strip()

            elif up.startswith("W "):
                vals["W"] = p[2:].strip()

            elif up.startswith("CONTR "):
                vals["C"] = p[6:].strip()
                j += 1
                break

            j += 1

        if (
            "O" in vals
            and "T" in vals
        ):
            row = (
                f"{symbol} "
                f"| O {vals['O']} "
                f"| T {vals['T']}"
            )

            if "R" in vals:
                row += (
                    f" | R {vals['R']}"
                )

            if "W" in vals:
                row += (
                    f" | W {vals['W']}"
                )

            if "C" in vals:
                row += (
                    f" | C {vals['C']}"
                )

            stock_rows.append(row)

        i = max(
            j,
            i + 1,
        )

    summary = []

    for token in tokens:
        u = token.upper()

        if (
            u.startswith(
                "WEIGHTED NET ="
            )
            or u.startswith(
                "NQ AT TRIGGER:"
            )
        ):
            if token not in summary:
                summary.append(token)

    first_rows = stock_rows[:5]
    second_rows = stock_rows[5:]

    part1_lines = []
    part1_lines.extend(metadata)

    part1_lines.append(
        "AUDIT 1/2: "
        "O=OPEN | T=LIVE | "
        "R=RAW | W=WEIGHT | "
        "C=CONTR"
    )

    part1_lines.extend(
        first_rows
    )

    part2_lines = [
        "AUDIT 2/2: "
        "O=OPEN | T=LIVE | "
        "R=RAW | W=WEIGHT | "
        "C=CONTR"
    ]

    part2_lines.extend(
        second_rows
    )

    part2_lines.extend(
        summary
    )

    part1 = "\n".join(
        part1_lines
    ).strip()

    part2 = "\n".join(
        part2_lines
    ).strip()

    if len(part1) > 1024:
        part1 = part1[:1024]

    if len(part2) > 1024:
        part2 = part2[:1024]

    return [
        (
            f"{title} | PART 1/2",
            part1 or text,
        ),
        (
            f"{title} | PART 2/2",
            part2 or text,
        ),
    ]


# ============================================================
# NQ LONG PUSHOVER SPLITTER
# ============================================================

def _nq_long_pushover_parts(title, message):
    title_text = str(title or "")
    message_text = str(message or "")
    title_u = title_text.upper()

    if (
        "NQ" not in title_u
        and "NASDAQ" not in title_u
    ):
        return [
            (
                title_text,
                message_text,
            )
        ]

    if len(message_text) <= 950:
        return [
            (
                title_text,
                message_text,
            )
        ]

    normalized = (
        message_text
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )

    lines = normalized.split("\n")

    best = None

    for cut in range(
        1,
        len(lines),
    ):
        p1 = "\n".join(
            lines[:cut]
        ).strip()

        p2 = "\n".join(
            lines[cut:]
        ).strip()

        if (
            len(p1) <= 950
            and len(p2) <= 950
        ):
            score = abs(
                len(p1)
                - len(p2)
            )

            if (
                best is None
                or score < best[0]
            ):
                best = (
                    score,
                    p1,
                    p2,
                )

    if best is not None:
        _, part1, part2 = best

    else:
        midpoint = (
            len(normalized) // 2
        )

        left_break = normalized.rfind(
            "\n",
            0,
            midpoint + 1,
        )

        right_break = normalized.find(
            "\n",
            midpoint,
        )

        if left_break > 0:
            cut = left_break

        elif right_break != -1:
            cut = right_break

        else:
            cut = midpoint

        part1 = normalized[
            :cut
        ].strip()

        part2 = normalized[
            cut:
        ].strip()

        part1 = part1[:1024]
        part2 = part2[:1024]

    return [
        (
            f"{title_text} | PART 1/2",
            part1,
        ),
        (
            f"{title_text} | PART 2/2",
            part2,
        ),
    ]


# ============================================================
# INDIA WEIGHTED PUSHOVER SPLITTER
# ============================================================

def _india_weighted_pushover_parts(title, message):
    title_text = str(title or "")
    message_text = str(message or "")
    title_u = title_text.upper()

    is_nifty = (
        "NIFTY 10-STOCK" in title_u
        and "NIFTY WEIGHTED" in title_u
    )

    is_banknifty = (
        "BANKNIFTY TOP-5" in title_u
        and "WEIGHTED" in title_u
    )

    if (
        not (
            is_nifty
            or is_banknifty
        )
        or len(message_text) <= 950
    ):
        return [
            (
                title_text,
                message_text,
            )
        ]

    normalized = (
        message_text
        .replace("\r", "")
        .replace("\n", " | ")
    )

    fields = [
        field.strip()
        for field
        in normalized.split("|")
        if field.strip()
    ]

    best = None

    for cut in range(
        1,
        len(fields),
    ):
        p1 = " | ".join(
            fields[:cut]
        ).strip()

        p2 = " | ".join(
            fields[cut:]
        ).strip()

        if (
            len(p1) <= 950
            and len(p2) <= 950
        ):
            score = abs(
                len(p1)
                - len(p2)
            )

            if (
                best is None
                or score < best[0]
            ):
                best = (
                    score,
                    p1,
                    p2,
                )

    if best is None:
        midpoint = (
            len(normalized) // 2
        )

        left = normalized.rfind(
            " | ",
            0,
            midpoint + 1,
        )

        right = normalized.find(
            " | ",
            midpoint,
        )

        if left > 0:
            cut = left

        elif right != -1:
            cut = right

        else:
            cut = midpoint

        part1 = normalized[
            :cut
        ].strip()

        part2 = normalized[
            cut:
        ].strip()

    else:
        _, part1, part2 = best

    return [
        (
            f"{title_text} | PART 1/2",
            part1,
        ),
        (
            f"{title_text} | PART 2/2",
            part2,
        ),
    ]


# ============================================================
# EXISTING HOME
# ============================================================

@app.get("/")
def home():
    return jsonify({
        "ok": True,
        "service": "NQ + NIFTY + BANKNIFTY backend",
        "nq": "NASDAQ 10-STOCK QQQ WEIGHTED LAST-4",
        "nifty": "NIFTY 10-STOCK WEIGHTED LAST-4",
        "banknifty": "BANKNIFTY TOP-5 WEIGHTED LAST-4",
        "crypto_liquidation_code": "removed",
        "nq_dashboard": "/nq-dashboard",
    })


# ============================================================
# EXISTING TRADINGVIEW ALERT WEBHOOK
# ============================================================

@app.post("/webhook")
def webhook():
    secret = request.args.get(
        "secret",
        "",
    )

    if (
        not WEBHOOK_SECRET
        or secret != WEBHOOK_SECRET
    ):
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    if (
        "title" not in data
        or "message" not in data
    ):
        return jsonify({
            "ok": False,
            "error": "title_and_message_required",
        }), 400

    tv_title = str(
        data.get(
            "title",
            "TradingView Alert",
        )
    )

    tv_message = str(
        data.get(
            "message",
            "",
        )
    )

    # Keep the Fixed-1H browser dashboard synced to the REAL MAIN alert.
    # This is separate from the old LAST-4 dashboard history helper below.
    fixed1h_main_trigger = _fixed1h_record_main_trigger(
        tv_title,
        tv_message,
    )

    (
        tv_message,
        nq_serial,
        nq_duplicate,
    ) = _nq_persistent_trigger_number(
        tv_title,
        tv_message,
    )

    # Mirror the already-created NQ trigger into the browser dashboard.
    # This does NOT create a trigger and does NOT alter alert logic.
    _nq_dashboard_record_trigger(
        tv_title,
        tv_message,
        nq_serial,
        nq_duplicate,
    )

    (
        tv_message,
        india_serial,
        india_duplicate,
    ) = _india_persistent_trigger_number(
        tv_title,
        tv_message,
    )

    parts = (
        _qqq_weighted_audit_pushover_parts(
            tv_title,
            tv_message,
        )
    )

    if len(parts) == 1:
        parts = (
            _nq_long_pushover_parts(
                tv_title,
                tv_message,
            )
        )

    if len(parts) == 1:
        parts = (
            _india_weighted_pushover_parts(
                tv_title,
                tv_message,
            )
        )

    def _send(parts_to_send):
        try:
            for (
                part_title,
                part_message,
            ) in parts_to_send:
                send_pushover(
                    part_title,
                    part_message,
                )

        except Exception as exc:
            print(
                f"[PUSHOVER BACKGROUND ERROR] {exc}",
                flush=True,
            )

    threading.Thread(
        target=_send,
        args=(list(parts),),
        daemon=True,
    ).start()

    return jsonify({
        "ok": True,
        "mode": "direct_pushover",
        "latest_qqq_last4_enabled": True,
        "nq_trigger": nq_serial,
        "nq_duplicate": nq_duplicate,
        "india_trigger": india_serial,
        "india_duplicate": india_duplicate,
        "fixed1h_dashboard_synced": bool(fixed1h_main_trigger),
        "parts_queued": len(parts),
    }), 200


# ============================================================
# NQ DASHBOARD HELPERS
# ============================================================

def _safe_float(value):
    try:
        if value is None:
            return None

        return float(value)

    except (
        TypeError,
        ValueError,
    ):
        return None


def _dashboard_state_text(net):
    if net is None:
        return "--"

    if net >= 0.100:
        return "BUY SIDE"

    if net <= -0.100:
        return "SELL SIDE"

    return "NEUTRAL"


def _dashboard_load():
    try:
        with NQ_DASHBOARD_LOCK:
            with open(
                NQ_DASHBOARD_STATE_FILE,
                "r",
                encoding="utf-8",
            ) as f:
                data = json.load(f)

        if isinstance(
            data,
            dict,
        ):
            return data

    except FileNotFoundError:
        pass

    except Exception as exc:
        print(
            f"[NQ DASHBOARD READ ERROR] {exc}",
            flush=True,
        )

    return {
        "updated_at_utc": None,
        "updated_at_ist": None,
        "threshold": 0.100,
        "bases": [],
        "trigger_history": [],
    }


def _dashboard_save(data):
    os.makedirs(
        os.path.dirname(
            NQ_DASHBOARD_STATE_FILE
        ),
        exist_ok=True,
    )

    tmp = (
        NQ_DASHBOARD_STATE_FILE
        + f".{os.getpid()}.{threading.get_ident()}.tmp"
    )

    with NQ_DASHBOARD_LOCK:
        try:
            with open(
                tmp,
                "w",
                encoding="utf-8",
            ) as f:
                json.dump(
                    data,
                    f,
                    separators=(",", ":"),
                    sort_keys=True,
                )

                f.flush()
                os.fsync(
                    f.fileno()
                )

            os.replace(
                tmp,
                NQ_DASHBOARD_STATE_FILE,
            )

        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)

            except OSError:
                pass


# ============================================================
# NQ DASHBOARD WEBHOOK
# TradingView dashboard indicator sends current 4 bases here.
# ============================================================

@app.post("/nq-dashboard-webhook")
def nq_dashboard_webhook():
    secret = request.args.get(
        "secret",
        "",
    )

    if (
        not WEBHOOK_SECRET
        or secret != WEBHOOK_SECRET
    ):
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    bases = []

    for i in range(1, 5):
        time_value = str(
            data.get(
                f"base{i}_time",
                "--",
            )
        ).strip()

        net_value = _safe_float(
            data.get(
                f"base{i}_net"
            )
        )

        bases.append({
            "number": i,
            "time": time_value,
            "net": net_value,
            "state": _dashboard_state_text(
                net_value
            ),
        })

    now_utc = datetime.now(
        timezone.utc
    )

    now_ist = (
        now_utc.astimezone(
            ZoneInfo(
                "Asia/Kolkata"
            )
        )
    )

    # Preserve alert trigger history when live base values refresh.
    previous_state = _dashboard_load()
    trigger_history = previous_state.get("trigger_history", [])
    if not isinstance(trigger_history, list):
        trigger_history = []

    state = {
        "updated_at_utc": (
            now_utc.isoformat()
        ),
        "updated_at_ist": (
            now_ist.strftime(
                "%d-%m-%Y %H:%M:%S"
            )
        ),
        "threshold": 0.100,
        "bases": bases,
        "trigger_history": trigger_history,
    }

    try:
        _dashboard_save(
            state
        )

    except Exception as exc:
        print(
            f"[NQ DASHBOARD SAVE ERROR] {exc}",
            flush=True,
        )

        return jsonify({
            "ok": False,
            "error": "save_failed",
        }), 500

    print(
        "[NQ DASHBOARD UPDATE] "
        + " | ".join(
            f"B{x['number']}={x['net']}"
            for x in bases
        ),
        flush=True,
    )

    return jsonify({
        "ok": True,
        "mode": "nq_live_4base_dashboard",
        "updated_at_ist": state[
            "updated_at_ist"
        ],
        "bases": bases,
    }), 200


# ============================================================
# NQ DASHBOARD JSON DATA
# ============================================================

@app.get("/nq-dashboard-data")
def nq_dashboard_data():
    state = _dashboard_load()

    return jsonify({
        "ok": True,
        **state,
    })


# ============================================================
# NQ LIVE 4-BASE BROWSER DASHBOARD
# ============================================================

@app.get("/nq-dashboard")
def nq_dashboard():
    html = """
<!DOCTYPE html>
<html lang="en">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>NQ LIVE 4-BASE DASHBOARD</title>

<style>

body {
    margin: 0;
    padding: 20px;
    background: #0d1117;
    color: #f0f6fc;
    font-family: Arial, Helvetica, sans-serif;
}

.container {
    max-width: 850px;
    margin: 0 auto;
}

h1 {
    text-align: center;
    margin-bottom: 5px;
}

.subtitle {
    text-align: center;
    color: #8b949e;
    margin-bottom: 25px;
}

.card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 12px;
    overflow: hidden;
}

table {
    width: 100%;
    border-collapse: collapse;
}

th {
    background: #21262d;
    padding: 15px 10px;
    font-size: 14px;
}

td {
    padding: 18px 10px;
    text-align: center;
    border-top: 1px solid #30363d;
    font-size: 18px;
}

.net {
    font-weight: bold;
    font-size: 22px;
}

.buy {
    color: #3fb950;
    font-weight: bold;
}

.sell {
    color: #f85149;
    font-weight: bold;
}

.neutral {
    color: #d29922;
    font-weight: bold;
}

.trigger-cell {
    font-weight: bold;
    font-size: 15px;
    line-height: 1.6;
}

.trigger-buy {
    color: #3fb950;
}

.trigger-sell {
    color: #f85149;
}

.footer {
    margin-top: 18px;
    text-align: center;
    color: #8b949e;
    line-height: 1.7;
}

.status {
    margin-top: 10px;
    text-align: center;
    font-size: 13px;
    color: #8b949e;
}

@media (max-width: 600px) {

    body {
        padding: 10px;
    }

    h1 {
        font-size: 22px;
    }

    th {
        font-size: 12px;
    }

    td {
        font-size: 15px;
        padding: 15px 5px;
    }

    .net {
        font-size: 18px;
    }
}

</style>

</head>

<body>

<div class="container">

    <h1>NQ LIVE 4-BASE DASHBOARD</h1>

    <div class="subtitle">
        NASDAQ 10-STOCK | QQQ WEIGHTED | ROLLING LAST-4
    </div>

    <div class="card">

        <table>

            <thead>

                <tr>
                    <th>BASE</th>
                    <th>TIME</th>
                    <th>CURRENT NET</th>
                    <th>STATE</th>
                    <th>TRIGGER</th>
                </tr>

            </thead>

            <tbody id="rows">

                <tr>
                    <td colspan="5">
                        Waiting for TradingView data...
                    </td>
                </tr>

            </tbody>

        </table>

    </div>

    <div class="footer">

        Threshold:
        <strong>+0.100% BUY</strong>
        /
        <strong>-0.100% SELL</strong>

        <br>

        QQQ Top-10 Weight:
        <strong>46.76%</strong>

        <br>

        Last Update:
        <span id="updated">--</span>
        IST

    </div>

    <div
        class="status"
        id="connection"
    >
        Loading...
    </div>

</div>

<script>

function escapeHtml(value) {

    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}


function formatNet(value) {

    if (
        value === null ||
        value === undefined ||
        Number.isNaN(
            Number(value)
        )
    ) {
        return "--";
    }

    const n = Number(value);

    const sign =
        n >= 0
        ? "+"
        : "";

    return (
        sign
        + n.toFixed(3)
        + "%"
    );
}


function stateClass(state) {

    if (
        state === "BUY SIDE"
    ) {
        return "buy";
    }

    if (
        state === "SELL SIDE"
    ) {
        return "sell";
    }

    return "neutral";
}


async function refreshDashboard() {

    try {

        const response =
            await fetch(
                "/nq-dashboard-data?ts="
                + Date.now(),
                {
                    cache: "no-store"
                }
            );

        const data =
            await response.json();

        const rows =
            document.getElementById(
                "rows"
            );

        if (
            !data.bases ||
            data.bases.length === 0
        ) {

            rows.innerHTML =
                '<tr>'
                + '<td colspan="5">'
                + 'Waiting for TradingView data...'
                + '</td>'
                + '</tr>';

            document.getElementById(
                "connection"
            ).textContent =
                "No dashboard data received yet.";

            return;
        }

        const triggerMap = {};

        if (Array.isArray(data.trigger_history)) {
            for (const item of data.trigger_history) {
                const key = String(item.base_time || "");
                if (!key) {
                    continue;
                }
                if (!triggerMap[key]) {
                    triggerMap[key] = [];
                }
                triggerMap[key].push(item);
            }
        }

        let html = "";

        for (
            const base
            of data.bases
        ) {

            const cls =
                stateClass(
                    base.state
                );

            html +=
                "<tr>"

                + "<td><strong>#"
                + escapeHtml(
                    base.number
                )
                + "</strong></td>"

                + "<td>"
                + escapeHtml(
                    base.time || "--"
                )
                + "</td>"

                + '<td class="net '
                + cls
                + '">'
                + escapeHtml(
                    formatNet(
                        base.net
                    )
                )
                + "</td>"

                + '<td class="'
                + cls
                + '">'
                + escapeHtml(
                    base.state || "--"
                )
                + "</td>"

                + '<td class="trigger-cell">'
                + (() => {
                    const items = triggerMap[String(base.time || "")] || [];
                    if (items.length === 0) {
                        return "--";
                    }
                    return items.map((item) => {
                        const direction = String(item.direction || "").toUpperCase();
                        const tcls = direction === "BUY" ? "trigger-buy" : "trigger-sell";
                        return '<span class="' + tcls + '">#'
                            + escapeHtml(item.serial)
                            + ' ' + escapeHtml(direction)
                            + '</span>';
                    }).join("<br>");
                })()
                + "</td>"

                + "</tr>";
        }

        rows.innerHTML =
            html;

        document.getElementById(
            "updated"
        ).textContent =
            data.updated_at_ist
            || "--";

        document.getElementById(
            "connection"
        ).textContent =
            "LIVE • Auto refresh every 5 seconds";

    }

    catch (error) {

        document.getElementById(
            "connection"
        ).textContent =
            "Waiting for server...";

    }
}


refreshDashboard();

setInterval(
    refreshDashboard,
    5000
);

</script>

</body>

</html>
"""

    return html, 200, {
        "Content-Type":
            "text/html; charset=utf-8",

        "Cache-Control":
            "no-store, no-cache, must-revalidate",
    }
# ============================================================
# NQ FIXED 1H DASHBOARD
# SEPARATE FROM EXISTING LAST-4 DASHBOARD
# ============================================================

NQ_FIXED1H_DASHBOARD_STATE_FILE = os.path.join(
    "/var/data",
    "nq_fixed1h_dashboard.json",
)

NQ_FIXED1H_DASHBOARD_LOCK = threading.Lock()

# MAIN /webhook trigger state is stored separately so the 1-minute
# dashboard feed can NEVER overwrite a newer real BUY/SELL trigger
# with stale Pine metadata.
NQ_FIXED1H_MAIN_TRIGGER_FILE = os.path.join(
    "/var/data",
    "nq_fixed1h_main_trigger.json",
)
NQ_FIXED1H_MAIN_TRIGGER_LOCK = threading.Lock()


def _fixed1h_safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _fixed1h_load():
    try:
        with NQ_FIXED1H_DASHBOARD_LOCK:
            with open(
                NQ_FIXED1H_DASHBOARD_STATE_FILE,
                "r",
                encoding="utf-8",
            ) as f:
                data = json.load(f)

        if isinstance(data, dict):
            return data

    except FileNotFoundError:
        pass

    except Exception as exc:
        print(
            f"[FIXED1H DASHBOARD READ ERROR] {exc}",
            flush=True,
        )

    return {
        "updated_at_utc": None,
        "updated_at_ist": None,
        "base_time": None,
        "update_time": None,
        "direct": None,
        "state": "NONE",
        "threshold": 0.50,
        "total_weight": 47.03,
        "last_trigger": "NONE",
        "last_trigger_type": "NONE",
        "last_trigger_value": None,
        "last_trigger_time": "NONE",
        "stocks": [],
    }


def _fixed1h_save(data):
    os.makedirs(
        os.path.dirname(NQ_FIXED1H_DASHBOARD_STATE_FILE),
        exist_ok=True,
    )

    tmp = (
        NQ_FIXED1H_DASHBOARD_STATE_FILE
        + f".{os.getpid()}.{threading.get_ident()}.tmp"
    )

    with NQ_FIXED1H_DASHBOARD_LOCK:
        try:
            with open(
                tmp,
                "w",
                encoding="utf-8",
            ) as f:
                json.dump(
                    data,
                    f,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                f.flush()
                os.fsync(f.fileno())

            os.replace(
                tmp,
                NQ_FIXED1H_DASHBOARD_STATE_FILE,
            )

        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass


def _fixed1h_main_trigger_load():
    try:
        with NQ_FIXED1H_MAIN_TRIGGER_LOCK:
            with open(
                NQ_FIXED1H_MAIN_TRIGGER_FILE,
                "r",
                encoding="utf-8",
            ) as f:
                data = json.load(f)

        if isinstance(data, dict):
            return data

    except FileNotFoundError:
        pass

    except Exception as exc:
        print(
            f"[FIXED1H MAIN TRIGGER READ ERROR] {exc}",
            flush=True,
        )

    return {}


def _fixed1h_main_trigger_save(data):
    os.makedirs(
        os.path.dirname(NQ_FIXED1H_MAIN_TRIGGER_FILE),
        exist_ok=True,
    )

    tmp = (
        NQ_FIXED1H_MAIN_TRIGGER_FILE
        + f".{os.getpid()}.{threading.get_ident()}.tmp"
    )

    with NQ_FIXED1H_MAIN_TRIGGER_LOCK:
        try:
            with open(
                tmp,
                "w",
                encoding="utf-8",
            ) as f:
                json.dump(
                    data,
                    f,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                f.flush()
                os.fsync(f.fileno())

            os.replace(
                tmp,
                NQ_FIXED1H_MAIN_TRIGGER_FILE,
            )

        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass


def _fixed1h_record_main_trigger(title, message):
    """Mirror a real NQ Fixed-1H MAIN alert into the browser dashboard.

    MAIN /webhook is authoritative for BUY/SELL state.  The dashboard Pine
    remains data-only and is not allowed to overwrite this trigger metadata.
    """
    title_text = str(title or "")
    message_text = str(message or "")
    title_u = title_text.upper()
    message_u = message_text.upper()

    if (
        "NASDAQ 10-STOCK" not in title_u
        or "FIXED 1H OPEN BASE" not in message_u
    ):
        return None

    if "SELL" in title_u:
        direction = "SELL"
    elif "BUY" in title_u:
        direction = "BUY"
    else:
        return None

    direct_value = None
    match = re.search(
        r"(?i)\\bDIRECT\\s*:\\s*([+-]?\\d+(?:\\.\\d+)?)\\s*%",
        message_text,
    )
    if match:
        direct_value = _fixed1h_safe_float(match.group(1))

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ZoneInfo("Asia/Kolkata"))

    trigger = {
        "state": direction,
        "state_source": "MAIN",
        "last_trigger": direction,
        "last_trigger_type": "DIRECT",
        "last_trigger_value": direct_value,
        "last_trigger_time": now_ist.strftime("%Y-%m-%d %H:%M:%S"),
        "last_trigger_source": "MAIN",
        "saved_at_utc": now_utc.isoformat(),
        "saved_at_ist": now_ist.strftime("%d-%m-%Y %H:%M:%S"),
    }

    # Authoritative copy: the dashboard feed cannot overwrite this file.
    _fixed1h_main_trigger_save(trigger)

    # Immediate browser update as well.  Even if a simultaneous dashboard
    # update races with this write, the NEXT dashboard update re-applies the
    # authoritative MAIN trigger from the separate file above.
    try:
        state = _fixed1h_load()
        state.update(trigger)
        state["updated_at_utc"] = now_utc.isoformat()
        state["updated_at_ist"] = now_ist.strftime("%d-%m-%Y %H:%M:%S")
        _fixed1h_save(state)
    except Exception as exc:
        print(
            f"[FIXED1H MAIN -> DASHBOARD SYNC ERROR] {exc}",
            flush=True,
        )

    print(
        "[FIXED1H MAIN -> DASHBOARD] "
        f"{direction} | DIRECT={direct_value} "
        f"| TIME={trigger['last_trigger_time']}",
        flush=True,
    )

    return trigger


def _fixed1h_send_pushover(title, message):
    # Fixed-1H is intentionally separate from the existing Last-4
    # send_pushover() filter, so Rolling/NIFTY/BankNifty behavior stays untouched.
    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        return False

    try:
        r = requests.post(
            PUSHOVER_URL,
            data={
                "token": PUSHOVER_TOKEN,
                "user": PUSHOVER_USER,
                "title": str(title or "NASDAQ FIXED 1H"),
                "message": str(message or ""),
                "priority": 2,
                "retry": 30,
                "expire": 3600,
            },
            timeout=10,
        )
        return r.ok

    except requests.RequestException as exc:
        print(
            f"[FIXED1H PUSHOVER ERROR] {exc}",
            flush=True,
        )
        return False



# ============================================================
# TEMP FIXED-1H PUSHOVER TEST
# Protected by existing WEBHOOK_SECRET.
# ============================================================

@app.get("/fixed1h-pushover-test")
def fixed1h_pushover_test():
    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    ok = _fixed1h_send_pushover(
        "NASDAQ FIXED 1H TEST",
        "Master Pushover connection test",
    )

    print(f"[FIXED1H PUSHOVER TEST] ok={ok}", flush=True)

    return jsonify({
        "ok": bool(ok),
        "mode": "fixed1h_pushover_test",
    }), 200 if ok else 502


@app.post("/fixed1h-dashboard-webhook")
def fixed1h_dashboard_webhook():
    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    data = request.get_json(silent=True) or {}
    payload_type = str(data.get("type", "")).strip()

    # DASHBOARD ROUTE IS DATA-ONLY.
    # Never send Pushover from dashboard-origin payloads.
    # MAIN /webhook remains the only alert/Pushover path.
    if payload_type == "NASDAQ_FIXED_1H_SIGNAL":
        signal = str(data.get("signal", "")).strip().upper()
        print(
            f"[FIXED1H DASHBOARD SIGNAL IGNORED FOR PUSHOVER] {signal}",
            flush=True,
        )
        return jsonify({
            "ok": True,
            "mode": "nq_fixed1h_dashboard_signal_ignored",
            "signal": signal,
            "pushover": False,
        }), 200

    if payload_type not in {
        "NASDAQ_FIXED_1H_MASTER",
        "NASDAQ_FIXED_1H_DASHBOARD",
    }:
        return jsonify({
            "ok": False,
            "error": "invalid_fixed1h_payload",
        }), 400

    stocks_raw = data.get("stocks", [])
    stocks = []

    if isinstance(stocks_raw, list):
        for item in stocks_raw:
            if not isinstance(item, dict):
                continue

            symbol = str(
                item.get("symbol", "")
            ).strip().upper()

            if not symbol:
                continue

            stocks.append({
                "symbol": symbol,
                "weight": _fixed1h_safe_float(
                    item.get("weight")
                ),
                "open": _fixed1h_safe_float(
                    item.get("open")
                ),
                "live": _fixed1h_safe_float(
                    item.get("live")
                ),
                "move_pct": _fixed1h_safe_float(
                    item.get("move_pct")
                ),
                "weighted": _fixed1h_safe_float(
                    item.get("weighted")
                ),
            })

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(
        ZoneInfo("Asia/Kolkata")
    )

    # MAIN /webhook is authoritative for signal state + LAST TRIGGER.
    # The dashboard Pine only supplies live DIRECT / stock values.
    # This prevents an old Pine last_trigger (for example 21-Sep) from
    # overwriting a newer real MAIN BUY/SELL on every dashboard refresh.
    main_trigger = _fixed1h_main_trigger_load()

    incoming_state = str(
        data.get("state", "NONE")
    ).upper()

    state_value = str(
        main_trigger.get("state")
        or incoming_state
    ).upper()

    last_trigger_value = (
        _fixed1h_safe_float(main_trigger.get("last_trigger_value"))
        if main_trigger
        else _fixed1h_safe_float(data.get("last_trigger_value"))
    )

    state = {
        "updated_at_utc": now_utc.isoformat(),
        "updated_at_ist": now_ist.strftime(
            "%d-%m-%Y %H:%M:%S"
        ),
        "base_time": str(
            data.get("base_time", "--")
        ),
        "update_time": str(
            data.get("update_time", "--")
        ),
        "direct": _fixed1h_safe_float(
            data.get("direct")
        ),
        "state": state_value,
        "state_source": (
            "MAIN" if main_trigger else "DASHBOARD"
        ),
        "threshold": _fixed1h_safe_float(
            data.get("threshold")
        ),
        "total_weight": _fixed1h_safe_float(
            data.get("total_weight")
        ),
        "last_trigger": str(
            main_trigger.get("last_trigger")
            if main_trigger
            else data.get("last_trigger", "NONE")
        ).upper(),
        "last_trigger_type": str(
            main_trigger.get("last_trigger_type")
            if main_trigger
            else data.get("last_trigger_type", "NONE")
        ).upper(),
        "last_trigger_value": last_trigger_value,
        "last_trigger_time": str(
            main_trigger.get("last_trigger_time")
            if main_trigger
            else data.get("last_trigger_time", "NONE")
        ),
        "last_trigger_source": (
            "MAIN" if main_trigger else "DASHBOARD"
        ),
        "stocks": stocks,
    }

    if state["threshold"] is None:
        state["threshold"] = 0.50

    if state["total_weight"] is None:
        state["total_weight"] = 47.03

    try:
        _fixed1h_save(state)

    except Exception as exc:
        print(
            f"[FIXED1H DASHBOARD SAVE ERROR] {exc}",
            flush=True,
        )
        return jsonify({
            "ok": False,
            "error": "save_failed",
        }), 500

    print(
        "[FIXED1H DASHBOARD UPDATE] "
        f"BASE={state['base_time']} "
        f"| DIRECT={state['direct']} "
        f"| STATE={state['state']} "
        f"| TRIGGER={state['last_trigger']} "
        f"| SOURCE={state.get('last_trigger_source')} "
        f"| STOCKS={len(stocks)}",
        flush=True,
    )

    return jsonify({
        "ok": True,
        "mode": "nq_fixed1h_dashboard",
        "updated_at_ist": state["updated_at_ist"],
        "base_time": state["base_time"],
        "direct": state["direct"],
        "state": state["state"],
        "stocks_received": len(stocks),
    }), 200


@app.get("/fixed1h-dashboard-data")
def fixed1h_dashboard_data():
    state = _fixed1h_load()
    return jsonify({
        "ok": True,
        **state,
    })


@app.get("/fixed1h-dashboard")
def fixed1h_dashboard():
    html = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>
<title>NQ FIXED 1H DASHBOARD</title>

<style>
body {
    margin: 0;
    padding: 20px;
    background: #0d1117;
    color: #f0f6fc;
    font-family: Arial, Helvetica, sans-serif;
}

.container {
    max-width: 1150px;
    margin: 0 auto;
}

h1 {
    text-align: center;
    margin: 0 0 5px 0;
}

.subtitle {
    text-align: center;
    color: #8b949e;
    margin-bottom: 20px;
}

.summary {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 10px;
    margin-bottom: 14px;
}

.card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 12px;
}

.metric {
    padding: 14px;
    text-align: center;
}

.metric-label {
    color: #8b949e;
    font-size: 12px;
    margin-bottom: 7px;
}

.metric-value {
    font-size: 20px;
    font-weight: bold;
}

.table-card {
    overflow-x: auto;
}

table {
    width: 100%;
    border-collapse: collapse;
    min-width: 800px;
}

th {
    background: #21262d;
    padding: 12px 8px;
    font-size: 13px;
}

td {
    padding: 12px 8px;
    text-align: center;
    border-top: 1px solid #30363d;
    font-size: 15px;
}

.buy {
    color: #3fb950;
    font-weight: bold;
}

.sell {
    color: #f85149;
    font-weight: bold;
}

.neutral {
    color: #d29922;
    font-weight: bold;
}

.trigger {
    margin-top: 14px;
    padding: 14px;
    line-height: 1.7;
}

.footer {
    margin-top: 15px;
    text-align: center;
    color: #8b949e;
    line-height: 1.7;
    font-size: 13px;
}

@media (max-width: 850px) {
    body {
        padding: 10px;
    }

    .summary {
        grid-template-columns: repeat(2, 1fr);
    }
}

@media (max-width: 500px) {
    .summary {
        grid-template-columns: 1fr;
    }
}
</style>
</head>

<body>
<div class="container">

    <h1>NQ FIXED 1H DASHBOARD</h1>

    <div class="subtitle">
        NASDAQ 10-STOCK | FIXED 1H OPEN | DIRECT ONLY | ±0.50%
    </div>

    <div class="summary">
        <div class="card metric">
            <div class="metric-label">FIXED 1H BASE</div>
            <div class="metric-value" id="baseTime">--</div>
        </div>

        <div class="card metric">
            <div class="metric-label">1H DIRECT</div>
            <div class="metric-value" id="direct">--</div>
        </div>

        <div class="card metric">
            <div class="metric-label">STATE</div>
            <div class="metric-value" id="state">NONE</div>
        </div>

        <div class="card metric">
            <div class="metric-label">THRESHOLD</div>
            <div class="metric-value" id="threshold">±0.50%</div>
        </div>

        <div class="card metric">
            <div class="metric-label">TOP-10 WEIGHT</div>
            <div class="metric-value" id="weight">47.03%</div>
        </div>

        <div class="card metric">
            <div class="metric-label">TRADINGVIEW UPDATE</div>
            <div class="metric-value" id="updateTime">--</div>
        </div>
    </div>

    <div class="card table-card">
        <table>
            <thead>
                <tr>
                    <th>STOCK</th>
                    <th>WEIGHT</th>
                    <th>1H OPEN</th>
                    <th>LIVE</th>
                    <th>OPEN→LIVE</th>
                    <th>WEIGHTED</th>
                </tr>
            </thead>
            <tbody id="stockRows">
                <tr>
                    <td colspan="6">
                        Waiting for TradingView data...
                    </td>
                </tr>
            </tbody>
        </table>
    </div>

    <div class="card trigger">
        <strong>LAST TRIGGER:</strong>
        <span id="lastTrigger">NONE</span>
        &nbsp; | &nbsp;
        <strong>TYPE:</strong>
        <span id="lastTriggerType">NONE</span>
        &nbsp; | &nbsp;
        <strong>VALUE:</strong>
        <span id="lastTriggerValue">--</span>
        &nbsp; | &nbsp;
        <strong>TIME:</strong>
        <span id="lastTriggerTime">NONE</span>
    </div>

    <div class="footer">
        Backend update:
        <span id="updated">--</span>
        IST
        <br>
        <span id="connection">
            Loading...
        </span>
    </div>

</div>

<script>
function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function numberText(value, decimals) {
    if (
        value === null ||
        value === undefined ||
        Number.isNaN(Number(value))
    ) {
        return "--";
    }

    return Number(value).toFixed(decimals);
}

function percentText(value, decimals) {
    if (
        value === null ||
        value === undefined ||
        Number.isNaN(Number(value))
    ) {
        return "--";
    }

    const n = Number(value);
    const sign = n > 0 ? "+" : "";
    return sign + n.toFixed(decimals) + "%";
}

function sideClass(value) {
    const n = Number(value);

    if (Number.isNaN(n)) {
        return "neutral";
    }

    if (n > 0) {
        return "buy";
    }

    if (n < 0) {
        return "sell";
    }

    return "neutral";
}

function stateClass(value) {
    const s = String(value || "").toUpperCase();

    if (s === "BUY") {
        return "buy";
    }

    if (s === "SELL") {
        return "sell";
    }

    return "neutral";
}

async function refreshDashboard() {
    try {
        const response = await fetch(
            "/fixed1h-dashboard-data?ts=" + Date.now(),
            {
                cache: "no-store"
            }
        );

        const data = await response.json();

        document.getElementById(
            "baseTime"
        ).textContent = data.base_time || "--";

        const directEl = document.getElementById("direct");
        directEl.textContent = percentText(data.direct, 3);
        directEl.className =
            "metric-value " + sideClass(data.direct);

        const stateEl = document.getElementById("state");
        stateEl.textContent = data.state || "NONE";
        stateEl.className =
            "metric-value " + stateClass(data.state);

        document.getElementById(
            "threshold"
        ).textContent =
            "±" + numberText(data.threshold, 2) + "%";

        document.getElementById(
            "weight"
        ).textContent =
            numberText(data.total_weight, 2) + "%";

        document.getElementById(
            "updateTime"
        ).textContent =
            data.update_time || "--";

        document.getElementById(
            "updated"
        ).textContent =
            data.updated_at_ist || "--";

        const rows = document.getElementById(
            "stockRows"
        );

        if (
            !Array.isArray(data.stocks) ||
            data.stocks.length === 0
        ) {
            rows.innerHTML =
                '<tr><td colspan="6">'
                + 'Waiting for TradingView data...'
                + '</td></tr>';
        } else {
            let html = "";

            for (const stock of data.stocks) {
                html +=
                    "<tr>"
                    + "<td><strong>"
                    + escapeHtml(stock.symbol || "--")
                    + "</strong></td>"
                    + "<td>"
                    + escapeHtml(
                        numberText(stock.weight, 2) + "%"
                    )
                    + "</td>"
                    + "<td>"
                    + escapeHtml(
                        numberText(stock.open, 2)
                    )
                    + "</td>"
                    + "<td>"
                    + escapeHtml(
                        numberText(stock.live, 2)
                    )
                    + "</td>"
                    + '<td class="'
                    + sideClass(stock.move_pct)
                    + '">'
                    + escapeHtml(
                        percentText(stock.move_pct, 3)
                    )
                    + "</td>"
                    + '<td class="'
                    + sideClass(stock.weighted)
                    + '">'
                    + escapeHtml(
                        percentText(stock.weighted, 4)
                    )
                    + "</td>"
                    + "</tr>";
            }

            rows.innerHTML = html;
        }

        const lastTriggerEl =
            document.getElementById("lastTrigger");

        lastTriggerEl.textContent =
            data.last_trigger || "NONE";

        lastTriggerEl.className =
            stateClass(data.last_trigger);

        document.getElementById(
            "lastTriggerType"
        ).textContent =
            data.last_trigger_type || "NONE";

        const lastValueEl =
            document.getElementById("lastTriggerValue");

        lastValueEl.textContent =
            percentText(
                data.last_trigger_value,
                3
            );

        lastValueEl.className =
            sideClass(data.last_trigger_value);

        document.getElementById(
            "lastTriggerTime"
        ).textContent =
            data.last_trigger_time || "NONE";

        document.getElementById(
            "connection"
        ).textContent =
            "LIVE • Auto refresh every 5 seconds";

    } catch (error) {
        document.getElementById(
            "connection"
        ).textContent =
            "Waiting for server...";
    }
}

refreshDashboard();

setInterval(
    refreshDashboard,
    5000
);
</script>

</body>
</html>
"""

    return html, 200, {
        "Content-Type":
            "text/html; charset=utf-8",

        "Cache-Control":
            "no-store, no-cache, must-revalidate",
    }

# ============================================================
# COINGLASS ISOLATED DIAGNOSTIC V8
# Correct current CoinGlass two-stage AES-ECB/PKCS7 response flow
# reconstructed from the uploaded _app-cd1f34fb3d7c610a.js.
# Does not change NQ / NIFTY / BANKNIFTY / FIXED-1H logic.
# ============================================================

@app.get("/coinglass-test")
def coinglass_test():
    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    import base64
    import time

    url = "https://capi.coinglass.com/api/coin/liquidation"

    # Browser-like request. cache-ts-v2 is generated fresh each request.
    cache_ts_v2 = str(int(time.time() * 1000))
    headers = {
        "accept": "application/json",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "cache-ts-v2": cache_ts_v2,
        "encryption": "true",
        "language": "en",
        "obe": "s_009b65e04f6f431599afef84fa3fbf8f",
        "origin": "https://www.coinglass.com",
        "priority": "u=1, i",
        "referer": "https://www.coinglass.com/",
        "sec-ch-ua": '"Google Chrome";v="153", "Not_A Brand";v="8", "Chromium";v="153"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/153.0.0.0 Safari/537.36"
        ),
    }

    # Constants decoded from the current CoinGlass JS mn() v-branches.
    # v=55 -> 170b070da9654622
    # v=66 -> d6537d845a964081
    # v=77 -> 863f08689c97435b
    FIXED_V_KEYS = {
        "55": "170b070da9654622",
        "66": "d6537d845a964081",
        "77": "863f08689c97435b",
    }

    def _pkcs7_unpad(raw):
        if not raw:
            raise ValueError("empty plaintext")
        pad = raw[-1]
        if pad < 1 or pad > 16:
            raise ValueError("invalid padding")
        if raw[-pad:] != bytes([pad]) * pad:
            raise ValueError("invalid padding bytes")
        return raw[:-pad]

    def _coinglass_wn_decode(compressed_bytes):
        """Exact role of CoinGlass wn(): pako inflate bytes, then UTF-8 text."""
        import zlib

        # The uploaded bundle imports pako and wn() passes the AES-decrypted
        # byte array into pako's inflate path before converting it to UTF-8.
        # pako inflate accepts zlib/gzip-wrapped streams; keep raw-deflate as
        # a compatibility fallback without changing the cryptographic flow.
        last_error = None
        for wbits in (47, 15, -15):
            try:
                inflated = zlib.decompress(compressed_bytes, wbits)
                return inflated.decode("utf-8")
            except Exception as exc:
                last_error = exc

        raise ValueError("CoinGlass wn()/inflate failed: " + str(last_error))

    def _aes_ecb_decrypt_cryptojs(ciphertext_b64, key_text):
        """CoinGlass Sn(): AES-ECB/PKCS7 -> hex bytes -> wn()/inflate -> UTF-8."""
        from Crypto.Cipher import AES

        key = str(key_text).encode("utf-8")
        if len(key) not in (16, 24, 32):
            raise ValueError(f"invalid AES key length: {len(key)}")

        ctext = str(ciphertext_b64)
        cipher_bytes = base64.b64decode(
            ctext + "=" * (-len(ctext) % 4),
            validate=False,
        )
        if not cipher_bytes or len(cipher_bytes) % 16 != 0:
            raise ValueError(
                f"cipher length not AES block aligned: {len(cipher_bytes)}"
            )

        raw = AES.new(key, AES.MODE_ECB).decrypt(cipher_bytes)
        plain = _pkcs7_unpad(raw)

        # IMPORTANT: CoinGlass does NOT UTF-8 decode AES plaintext directly.
        # JS Sn() converts the decrypted Hex WordArray back to bytes and wn()
        # inflates those bytes first. V7 missed this inflate step.
        decoded = _coinglass_wn_decode(plain)

        # CoinGlass Sn() strips a surrounding double quote if present.
        if decoded.startswith('"'):
            decoded = decoded[1:]
        if decoded.endswith('"'):
            decoded = decoded[:-1]

        return decoded

    def _stage1_seed(response_obj, v_value):
        """Equivalent to the decoded mn(response, Mn(config.url)) branch."""
        v = str(v_value or "")

        if v in FIXED_V_KEYS:
            return FIXED_V_KEYS[v], f"fixed_v{v}"

        if v == "0":
            # JS uses response.config.headers['cache-ts-v2'].
            return cache_ts_v2, "cache-ts-v2"

        if v == "2":
            return response_obj.headers.get("time") or "", "response_time_header"

        if v == "1":
            # Current endpoint normally returns fixed-key versions (55/66/77).
            # Keep v=1 explicit rather than guessing Mn(url)'s transformed value.
            raise ValueError("v=1 URL-derived seed not implemented in V8")

        raise ValueError(f"unsupported CoinGlass v header: {v!r}")

    def _exact_two_stage_decrypt(response_obj, encrypted_data, v_value):
        report = {
            "crypto_available": False,
            "decrypt_success": False,
            "v": v_value,
            "seed_source": None,
            "stage1_success": False,
            "stage1_key_length": None,
            "stage2_success": False,
            "plaintext_preview": None,
            "plaintext_json": None,
        }

        try:
            from Crypto.Cipher import AES  # noqa: F401
            report["crypto_available"] = True
        except Exception as exc:
            report["error"] = "PyCryptodome unavailable: " + str(exc)
            return report

        try:
            seed, source = _stage1_seed(response_obj, v_value)
            report["seed_source"] = source
            report["seed_length_before_b64"] = len(seed)

            # JS: a = btoa(seed); a = a.substring(0, 16)
            stage1_key = base64.b64encode(
                seed.encode("utf-8")
            ).decode("ascii")[:16]
            report["stage1_key_length"] = len(stage1_key)

            # Exact JS interceptor order:
            #   a = Sn(t.headers.user, a)
            #   o = Sn(t.data.data, a)
            # Stage 1 therefore decrypts the RESPONSE HEADER `user`,
            # not the encrypted data payload.
            encrypted_user = response_obj.headers.get("user") or ""
            if not encrypted_user:
                raise ValueError("missing CoinGlass user response header")

            stage2_key = _aes_ecb_decrypt_cryptojs(
                encrypted_user,
                stage1_key,
            )
            report["stage1_success"] = True
            report["encrypted_user_length"] = len(encrypted_user)
            report["stage2_key_length"] = len(stage2_key.encode("utf-8"))
            # Do not expose the derived key itself in the diagnostic response.

            # Stage 2 decrypts data.data with the key derived from `user`.
            plaintext = _aes_ecb_decrypt_cryptojs(
                encrypted_data,
                stage2_key,
            )
            report["stage2_success"] = True
            report["plaintext_preview"] = plaintext[:1500]

            try:
                obj = json.loads(plaintext)
                report["plaintext_json"] = obj
                report["json_success"] = True
            except Exception as exc:
                report["json_success"] = False
                report["json_error"] = str(exc)[:200]
                obj = None

            # CoinGlass JS accepts either parsed JSON or the plaintext string.
            report["decrypt_success"] = True

        except Exception as exc:
            report["error"] = str(exc)[:300]

        return report

    try:
        r = requests.get(
            url,
            headers=headers,
            timeout=20,
        )

        try:
            parsed = r.json()
            json_parse_ok = True
        except Exception:
            parsed = None
            json_parse_ok = False

        v_value = r.headers.get("v")
        user_value = r.headers.get("user")
        time_value = r.headers.get("time")

        result = {
            "ok": bool(r.ok),
            "diagnostic_version": "COINGLASS_V8_HEADER_USER_AES_PAKO_INFLATE",
            "http_status": r.status_code,
            "final_url": r.url,
            "json_parse_ok": json_parse_ok,
            "KEY_HEADERS": {
                "v": v_value,
                "user_present": bool(user_value),
                "user_length": len(user_value) if user_value else 0,
                "time": time_value,
                "encryption": r.headers.get("encryption"),
                "content-type": r.headers.get("content-type"),
            },
        }

        if isinstance(parsed, dict):
            data = parsed.get("data")
            result.update({
                "json_keys": list(parsed.keys()),
                "coinglass_code": parsed.get("code"),
                "coinglass_msg": parsed.get("msg"),
                "coinglass_success": parsed.get("success"),
                "data_present": "data" in parsed,
                "data_type": type(data).__name__,
                "data_length": (
                    len(data)
                    if isinstance(data, (str, list, dict))
                    else None
                ),
            })

            if isinstance(data, str):
                result["decrypt"] = _exact_two_stage_decrypt(
                    r,
                    data,
                    v_value,
                )
            else:
                result["decrypt"] = {
                    "decrypt_success": False,
                    "reason": "data is not encrypted string",
                }
        else:
            result.update({
                "body_length": len(r.text),
                "body_preview": r.text[:1000],
            })

        decrypt_info = result.get("decrypt", {})
        print(
            "[COINGLASS V8] "
            f"status={r.status_code} "
            f"v={v_value} "
            f"seed_source={decrypt_info.get('seed_source')} "
            f"stage1={decrypt_info.get('stage1_success')} "
            f"stage2={decrypt_info.get('stage2_success')} "
            f"json={decrypt_info.get('json_success')} "
            f"success={decrypt_info.get('decrypt_success')}",
            flush=True,
        )

        return jsonify(result), 200

    except requests.RequestException as exc:
        print(
            f"[COINGLASS V8 REQUEST ERROR] {exc}",
            flush=True,
        )
        return jsonify({
            "ok": False,
            "diagnostic_version": "COINGLASS_V8_HEADER_USER_AES_PAKO_INFLATE",
            "error": "request_failed",
            "detail": str(exc),
        }), 502


# ============================================================
# COINGLASS V11 - INDIVIDUAL LIQUIDATION ORDER ENDPOINT TEST
# Source endpoint: /api/futures/liquidation/order
# Reuses the verified V8 AES + pako/inflate decryption flow.
# This is intentionally a schema-discovery endpoint first; once the
# live order fields are confirmed, the trailing-60m counter can be
# wired without guessing field names or timestamps.
# ============================================================

@app.get("/coinglass-orders-test")
def coinglass_orders_test():
    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    import base64
    import time

    url = "https://capi.coinglass.com/api/futures/liquidation/order"

    # Exact individual liquidation-order endpoint found in the uploaded CoinGlass JS.
    # Keep filters configurable until we inspect the live decrypted schema.
    symbol = (request.args.get("symbol") or "BTC").strip().upper()
    exchange = (request.args.get("exchange") or "").strip()
    limit_text = (request.args.get("limit") or "").strip()
    page_size_text = (request.args.get("pageSize") or "100").strip()
    page_num_text = (request.args.get("pageNum") or "1").strip()

    params = {"symbol": symbol}
    if exchange:
        params["exchange"] = exchange
    if limit_text.isdigit():
        params["limit"] = int(limit_text)

    # CoinGlass requires pageSize as an integer for this endpoint.
    # Default to 100 so the browser test works even if pageSize is omitted.
    if page_size_text.isdigit() and int(page_size_text) > 0:
        params["pageSize"] = int(page_size_text)
    else:
        params["pageSize"] = 100

    # CoinGlass also requires pageNum as an integer. Default to first page.
    if page_num_text.isdigit() and int(page_num_text) > 0:
        params["pageNum"] = int(page_num_text)
    else:
        params["pageNum"] = 1

    # Browser-like request. cache-ts-v2 is generated fresh each request.
    cache_ts_v2 = str(int(time.time() * 1000))
    headers = {
        "accept": "application/json",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "cache-ts-v2": cache_ts_v2,
        "encryption": "true",
        "language": "en",
        "obe": "s_009b65e04f6f431599afef84fa3fbf8f",
        "origin": "https://www.coinglass.com",
        "priority": "u=1, i",
        "referer": "https://www.coinglass.com/",
        "sec-ch-ua": '"Google Chrome";v="153", "Not_A Brand";v="8", "Chromium";v="153"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/153.0.0.0 Safari/537.36"
        ),
    }

    # Constants decoded from the current CoinGlass JS mn() v-branches.
    # v=55 -> 170b070da9654622
    # v=66 -> d6537d845a964081
    # v=77 -> 863f08689c97435b
    FIXED_V_KEYS = {
        "55": "170b070da9654622",
        "66": "d6537d845a964081",
        "77": "863f08689c97435b",
    }

    def _pkcs7_unpad(raw):
        if not raw:
            raise ValueError("empty plaintext")
        pad = raw[-1]
        if pad < 1 or pad > 16:
            raise ValueError("invalid padding")
        if raw[-pad:] != bytes([pad]) * pad:
            raise ValueError("invalid padding bytes")
        return raw[:-pad]

    def _coinglass_wn_decode(compressed_bytes):
        """Exact role of CoinGlass wn(): pako inflate bytes, then UTF-8 text."""
        import zlib

        # The uploaded bundle imports pako and wn() passes the AES-decrypted
        # byte array into pako's inflate path before converting it to UTF-8.
        # pako inflate accepts zlib/gzip-wrapped streams; keep raw-deflate as
        # a compatibility fallback without changing the cryptographic flow.
        last_error = None
        for wbits in (47, 15, -15):
            try:
                inflated = zlib.decompress(compressed_bytes, wbits)
                return inflated.decode("utf-8")
            except Exception as exc:
                last_error = exc

        raise ValueError("CoinGlass wn()/inflate failed: " + str(last_error))

    def _aes_ecb_decrypt_cryptojs(ciphertext_b64, key_text):
        """CoinGlass Sn(): AES-ECB/PKCS7 -> hex bytes -> wn()/inflate -> UTF-8."""
        from Crypto.Cipher import AES

        key = str(key_text).encode("utf-8")
        if len(key) not in (16, 24, 32):
            raise ValueError(f"invalid AES key length: {len(key)}")

        ctext = str(ciphertext_b64)
        cipher_bytes = base64.b64decode(
            ctext + "=" * (-len(ctext) % 4),
            validate=False,
        )
        if not cipher_bytes or len(cipher_bytes) % 16 != 0:
            raise ValueError(
                f"cipher length not AES block aligned: {len(cipher_bytes)}"
            )

        raw = AES.new(key, AES.MODE_ECB).decrypt(cipher_bytes)
        plain = _pkcs7_unpad(raw)

        # IMPORTANT: CoinGlass does NOT UTF-8 decode AES plaintext directly.
        # JS Sn() converts the decrypted Hex WordArray back to bytes and wn()
        # inflates those bytes first. V7 missed this inflate step.
        decoded = _coinglass_wn_decode(plain)

        # CoinGlass Sn() strips a surrounding double quote if present.
        if decoded.startswith('"'):
            decoded = decoded[1:]
        if decoded.endswith('"'):
            decoded = decoded[:-1]

        return decoded

    def _stage1_seed(response_obj, v_value):
        """Equivalent to the decoded mn(response, Mn(config.url)) branch."""
        v = str(v_value or "")

        if v in FIXED_V_KEYS:
            return FIXED_V_KEYS[v], f"fixed_v{v}"

        if v == "0":
            # JS uses response.config.headers['cache-ts-v2'].
            return cache_ts_v2, "cache-ts-v2"

        if v == "2":
            return response_obj.headers.get("time") or "", "response_time_header"

        if v == "1":
            # Current endpoint normally returns fixed-key versions (55/66/77).
            # Keep v=1 explicit rather than guessing Mn(url)'s transformed value.
            raise ValueError("v=1 URL-derived seed not implemented in V10")

        raise ValueError(f"unsupported CoinGlass v header: {v!r}")

    def _exact_two_stage_decrypt(response_obj, encrypted_data, v_value):
        report = {
            "crypto_available": False,
            "decrypt_success": False,
            "v": v_value,
            "seed_source": None,
            "stage1_success": False,
            "stage1_key_length": None,
            "stage2_success": False,
            "plaintext_preview": None,
            "plaintext_json": None,
        }

        try:
            from Crypto.Cipher import AES  # noqa: F401
            report["crypto_available"] = True
        except Exception as exc:
            report["error"] = "PyCryptodome unavailable: " + str(exc)
            return report

        try:
            seed, source = _stage1_seed(response_obj, v_value)
            report["seed_source"] = source
            report["seed_length_before_b64"] = len(seed)

            # JS: a = btoa(seed); a = a.substring(0, 16)
            stage1_key = base64.b64encode(
                seed.encode("utf-8")
            ).decode("ascii")[:16]
            report["stage1_key_length"] = len(stage1_key)

            # Exact JS interceptor order:
            #   a = Sn(t.headers.user, a)
            #   o = Sn(t.data.data, a)
            # Stage 1 therefore decrypts the RESPONSE HEADER `user`,
            # not the encrypted data payload.
            encrypted_user = response_obj.headers.get("user") or ""
            if not encrypted_user:
                raise ValueError("missing CoinGlass user response header")

            stage2_key = _aes_ecb_decrypt_cryptojs(
                encrypted_user,
                stage1_key,
            )
            report["stage1_success"] = True
            report["encrypted_user_length"] = len(encrypted_user)
            report["stage2_key_length"] = len(stage2_key.encode("utf-8"))
            # Do not expose the derived key itself in the diagnostic response.

            # Stage 2 decrypts data.data with the key derived from `user`.
            plaintext = _aes_ecb_decrypt_cryptojs(
                encrypted_data,
                stage2_key,
            )
            report["stage2_success"] = True
            report["plaintext_preview"] = plaintext[:1500]

            try:
                obj = json.loads(plaintext)
                report["plaintext_json"] = obj
                report["json_success"] = True
            except Exception as exc:
                report["json_success"] = False
                report["json_error"] = str(exc)[:200]
                obj = None

            # CoinGlass JS accepts either parsed JSON or the plaintext string.
            report["decrypt_success"] = True

        except Exception as exc:
            report["error"] = str(exc)[:300]

        return report

    try:
        r = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=20,
        )

        try:
            parsed = r.json()
            json_parse_ok = True
        except Exception:
            parsed = None
            json_parse_ok = False

        v_value = r.headers.get("v")
        user_value = r.headers.get("user")
        time_value = r.headers.get("time")

        result = {
            "ok": bool(r.ok),
            "diagnostic_version": "COINGLASS_V11_LIQUIDATION_ORDER_PAGESIZE_PAGENUM",
            "requested_symbol": symbol,
            "requested_exchange": exchange or None,
            "requested_limit": params.get("limit"),
            "requested_pageSize": params.get("pageSize"),
            "requested_pageNum": params.get("pageNum"),
            "http_status": r.status_code,
            "final_url": r.url,
            "json_parse_ok": json_parse_ok,
            "KEY_HEADERS": {
                "v": v_value,
                "user_present": bool(user_value),
                "user_length": len(user_value) if user_value else 0,
                "time": time_value,
                "encryption": r.headers.get("encryption"),
                "content-type": r.headers.get("content-type"),
            },
        }

        if isinstance(parsed, dict):
            data = parsed.get("data")
            result.update({
                "json_keys": list(parsed.keys()),
                "coinglass_code": parsed.get("code"),
                "coinglass_msg": parsed.get("msg"),
                "coinglass_success": parsed.get("success"),
                "data_present": "data" in parsed,
                "data_type": type(data).__name__,
                "data_length": (
                    len(data)
                    if isinstance(data, (str, list, dict))
                    else None
                ),
            })

            if isinstance(data, str):
                result["decrypt"] = _exact_two_stage_decrypt(
                    r,
                    data,
                    v_value,
                )
            else:
                result["decrypt"] = {
                    "decrypt_success": False,
                    "reason": "data is not encrypted string",
                }
        else:
            result.update({
                "body_length": len(r.text),
                "body_preview": r.text[:1000],
            })

        decrypt_info = result.get("decrypt", {})
        print(
            "[COINGLASS V10 ORDERS] "
            f"status={r.status_code} "
            f"v={v_value} "
            f"seed_source={decrypt_info.get('seed_source')} "
            f"stage1={decrypt_info.get('stage1_success')} "
            f"stage2={decrypt_info.get('stage2_success')} "
            f"json={decrypt_info.get('json_success')} "
            f"success={decrypt_info.get('decrypt_success')}",
            flush=True,
        )

        return jsonify(result), 200

    except requests.RequestException as exc:
        print(
            f"[COINGLASS V10 ORDERS REQUEST ERROR] {exc}",
            flush=True,
        )
        return jsonify({
            "ok": False,
            "diagnostic_version": "COINGLASS_V10_LIQUIDATION_ORDER_PAGESIZE",
            "error": "request_failed",
            "detail": str(exc),
        }), 502

# ============================================================
# COINGLASS V15 - BTC 1H LIQUIDATION TRADES ALERT ONLY
# Uses CoinGlass's own pre-calculated 1h liquidation trade counts.
# Frozen signal logic:
#   SHORT - LONG >= 50 -> BUY
#   LONG - SHORT >= 50 -> SELL
# Same signal never repeats; only an opposite qualifying state can alert next.
# Poll interval: 60 seconds. State persists on Render disk.
# ============================================================

BTC_1H_STATE_FILE = os.path.join('/var/data', 'btc_coinglass_1h_trades_state.json')
BTC_1H_MONITOR_LOCK_FILE = os.path.join('/var/data', 'btc_coinglass_1h_trades.lock')
BTC_1H_LOCK = threading.Lock()
BTC_1H_START_LOCK = threading.Lock()
BTC_1H_MONITOR_STARTED = False
BTC_1H_MONITOR_HANDLE = None
BTC_1H_THRESHOLD = 50

BTC_1H_STATE = {
    'long_count': None,
    'short_count': None,
    'diff_short_minus_long': None,
    'state': None,
    'last_change_ms': None,
    'last_update_ms': None,
    'status': 'STARTING',
    'error': None,
}


def _btc1h_now_ms():
    import time
    return int(time.time() * 1000)


def _btc1h_atomic_write(payload):
    try:
        os.makedirs('/var/data', exist_ok=True)
        tmp = BTC_1H_STATE_FILE + f'.{os.getpid()}.{threading.get_ident()}.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, BTC_1H_STATE_FILE)
    except Exception as exc:
        print(f'[BTC 1H STATE WRITE ERROR] {exc}', flush=True)


def _btc1h_load_state():
    try:
        with open(BTC_1H_STATE_FILE, 'r', encoding='utf-8') as f:
            saved = json.load(f)
        if isinstance(saved, dict):
            with BTC_1H_LOCK:
                for key in BTC_1H_STATE:
                    if key in saved:
                        BTC_1H_STATE[key] = saved[key]
            print(f'[BTC 1H DISK] state loaded path={BTC_1H_STATE_FILE}', flush=True)
            return True
    except FileNotFoundError:
        print(f'[BTC 1H DISK] no previous state yet path={BTC_1H_STATE_FILE}', flush=True)
    except Exception as exc:
        print(f'[BTC 1H STATE READ ERROR] {exc}', flush=True)
    return False


def _btc1h_send_pushover(title, message):
    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        print(f'[BTC 1H PUSHOVER NOT CONFIGURED] {title} | {message}', flush=True)
        return False
    try:
        r = requests.post(
            PUSHOVER_URL,
            data={
                'token': PUSHOVER_TOKEN,
                'user': PUSHOVER_USER,
                'title': title,
                'message': message,
                'priority': 1,
            },
            timeout=10,
        )
        print(f'[BTC 1H PUSHOVER] status={r.status_code} ok={r.ok}', flush=True)
        return r.ok
    except requests.RequestException as exc:
        print(f'[BTC 1H PUSHOVER ERROR] {exc}', flush=True)
        return False


def _btc1h_inflate(compressed_bytes):
    import zlib
    last_error = None
    for wbits in (47, 15, -15):
        try:
            return zlib.decompress(compressed_bytes, wbits).decode('utf-8')
        except Exception as exc:
            last_error = exc
    raise ValueError('CoinGlass inflate failed: ' + str(last_error))


def _btc1h_unpad(raw):
    if not raw:
        raise ValueError('empty plaintext')
    pad = raw[-1]
    if pad < 1 or pad > 16 or raw[-pad:] != bytes([pad]) * pad:
        raise ValueError('invalid PKCS7 padding')
    return raw[:-pad]


def _btc1h_aes_decrypt(ciphertext_b64, key_text):
    import base64
    from Crypto.Cipher import AES
    key = str(key_text).encode('utf-8')
    if len(key) not in (16, 24, 32):
        raise ValueError(f'invalid AES key length: {len(key)}')
    text_value = str(ciphertext_b64)
    cipher_bytes = base64.b64decode(text_value + '=' * (-len(text_value) % 4), validate=False)
    raw = AES.new(key, AES.MODE_ECB).decrypt(cipher_bytes)
    decoded = _btc1h_inflate(_btc1h_unpad(raw))
    if decoded.startswith('"'):
        decoded = decoded[1:]
    if decoded.endswith('"'):
        decoded = decoded[:-1]
    return decoded


def _btc1h_decrypt_response(resp, encrypted_data, cache_ts_v2):
    import base64
    fixed = {
        '55': '170b070da9654622',
        '66': 'd6537d845a964081',
        '77': '863f08689c97435b',
    }
    v = str(resp.headers.get('v') or '')
    if v in fixed:
        seed = fixed[v]
    elif v == '0':
        seed = cache_ts_v2
    elif v == '2':
        seed = resp.headers.get('time') or ''
    else:
        raise ValueError(f'unsupported CoinGlass v header: {v!r}')

    stage1_key = base64.b64encode(seed.encode('utf-8')).decode('ascii')[:16]
    encrypted_user = resp.headers.get('user') or ''
    if not encrypted_user:
        raise ValueError('missing CoinGlass user response header')
    stage2_key = _btc1h_aes_decrypt(encrypted_user, stage1_key)
    plaintext = _btc1h_aes_decrypt(encrypted_data, stage2_key)
    return json.loads(plaintext)


def _btc1h_headers(cache_ts_v2):
    return {
        'accept': 'application/json',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-ts-v2': cache_ts_v2,
        'encryption': 'true',
        'language': 'en',
        'obe': 's_009b65e04f6f431599afef84fa3fbf8f',
        'origin': 'https://www.coinglass.com',
        'referer': 'https://www.coinglass.com/',
        'sec-ch-ua': '"Google Chrome";v="153", "Not_A Brand";v="8", "Chromium";v="153"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/153.0.0.0 Safari/537.36'
        ),
    }


def _btc1h_number(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(round(float(value)))
    except Exception:
        return None


def _btc1h_pair_from_dict(d):
    if not isinstance(d, dict):
        return None

    # Exact field names expected from CoinGlass Liquidation Trades mode.
    pairs = [
        ('longLiquidationCount', 'shortLiquidationCount'),
        ('long_liquidation_count', 'short_liquidation_count'),
        ('longCount', 'shortCount'),
        ('long_count', 'short_count'),
    ]
    for lk, sk in pairs:
        if lk in d and sk in d:
            long_n = _btc1h_number(d.get(lk))
            short_n = _btc1h_number(d.get(sk))
            if long_n is not None and short_n is not None:
                return long_n, short_n

    # Some CoinGlass payloads place 1h fields directly on the BTC row.
    direct_pairs = [
        ('h1LongLiquidationCount', 'h1ShortLiquidationCount'),
        ('h1LongCount', 'h1ShortCount'),
        ('longLiquidationCount1h', 'shortLiquidationCount1h'),
    ]
    for lk, sk in direct_pairs:
        if lk in d and sk in d:
            long_n = _btc1h_number(d.get(lk))
            short_n = _btc1h_number(d.get(sk))
            if long_n is not None and short_n is not None:
                return long_n, short_n

    for hkey in ('h1', 'H1', '1h', '1H'):
        nested = d.get(hkey)
        pair = _btc1h_pair_from_dict(nested) if isinstance(nested, dict) else None
        if pair:
            return pair
    return None


def _btc1h_find_btc_counts(obj):
    # Find a BTC row first, then read its CoinGlass 1h trade-count fields.
    if isinstance(obj, dict):
        symbol = str(obj.get('symbol') or obj.get('coin') or obj.get('name') or '').upper()
        if symbol in ('BTC', 'BITCOIN'):
            pair = _btc1h_pair_from_dict(obj)
            if pair:
                return pair
        for value in obj.values():
            found = _btc1h_find_btc_counts(value)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _btc1h_find_btc_counts(item)
            if found:
                return found
    return None


def _btc1h_fetch_counts():
    import time
    cache_ts_v2 = str(int(time.time() * 1000))
    resp = requests.get(
        'https://capi.coinglass.com/api/coin/liquidation',
        headers=_btc1h_headers(cache_ts_v2),
        timeout=20,
    )
    resp.raise_for_status()
    outer = resp.json()
    if str(outer.get('code')) != '0':
        raise ValueError(f"CoinGlass code={outer.get('code')} msg={outer.get('msg')}")
    encrypted = outer.get('data')
    if not isinstance(encrypted, str):
        raise ValueError('CoinGlass encrypted data missing')
    inner = _btc1h_decrypt_response(resp, encrypted, cache_ts_v2)
    pair = _btc1h_find_btc_counts(inner)
    if not pair:
        raise ValueError('BTC 1h Liquidation Trades count fields not found in CoinGlass response')
    return pair


def _btc1h_evaluate_once(send_alert=True):
    long_n, short_n = _btc1h_fetch_counts()
    diff = int(short_n - long_n)
    now_ms = _btc1h_now_ms()

    desired = None
    if diff >= BTC_1H_THRESHOLD:
        desired = 'BUY'
    elif diff <= -BTC_1H_THRESHOLD:
        desired = 'SELL'

    alert_state = None
    with BTC_1H_LOCK:
        previous = BTC_1H_STATE.get('state')
        if desired is not None and desired != previous:
            BTC_1H_STATE['state'] = desired
            BTC_1H_STATE['last_change_ms'] = now_ms
            alert_state = desired

        BTC_1H_STATE['long_count'] = long_n
        BTC_1H_STATE['short_count'] = short_n
        BTC_1H_STATE['diff_short_minus_long'] = diff
        BTC_1H_STATE['last_update_ms'] = now_ms
        BTC_1H_STATE['status'] = 'LIVE'
        BTC_1H_STATE['error'] = None
        state_copy = dict(BTC_1H_STATE)
        _btc1h_atomic_write(state_copy)

    if alert_state and send_alert:
        if alert_state == 'BUY':
            reason = f'SHORT liquidation trades stronger by {abs(diff)}'
        else:
            reason = f'LONG liquidation trades stronger by {abs(diff)}'
        _btc1h_send_pushover(
            f'BTC 1H {alert_state}',
            f'{reason}\nLONG={long_n} | SHORT={short_n} | DIFF={diff:+d}\nCoinGlass Liquidation Trades',
        )
        print(f'[BTC 1H SIGNAL] {alert_state} LONG={long_n} SHORT={short_n} DIFF={diff:+d}', flush=True)

    return state_copy


def _btc1h_monitor_loop():
    import time
    print('[BTC 1H MONITOR] started', flush=True)
    while True:
        started = time.time()
        try:
            _btc1h_evaluate_once(send_alert=True)
        except Exception as exc:
            with BTC_1H_LOCK:
                BTC_1H_STATE['last_update_ms'] = _btc1h_now_ms()
                BTC_1H_STATE['status'] = 'ERROR'
                BTC_1H_STATE['error'] = str(exc)[:500]
                _btc1h_atomic_write(dict(BTC_1H_STATE))
            print(f'[BTC 1H MONITOR ERROR] {exc}', flush=True)
        elapsed = time.time() - started
        time.sleep(max(5.0, 60.0 - elapsed))


def _btc1h_start_monitor():
    global BTC_1H_MONITOR_STARTED, BTC_1H_MONITOR_HANDLE
    with BTC_1H_START_LOCK:
        if BTC_1H_MONITOR_STARTED:
            return True
        try:
            os.makedirs('/var/data', exist_ok=True)
            handle = open(BTC_1H_MONITOR_LOCK_FILE, 'a+')
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception as exc:
            print(f'[BTC 1H MONITOR] another worker owns lock: {exc}', flush=True)
            return False

        BTC_1H_MONITOR_HANDLE = handle
        BTC_1H_MONITOR_STARTED = True
        thread = threading.Thread(target=_btc1h_monitor_loop, daemon=True, name='btc-coinglass-1h-trades')
        thread.start()
        print(f'[BTC 1H MONITOR] thread launched pid={os.getpid()}', flush=True)
        return True


@app.get('/btc-liquidation-data')
def btc_liquidation_data():
    if os.environ.get('COINGLASS_BTC_MONITOR', '1') == '1':
        _btc1h_start_monitor()
    with BTC_1H_LOCK:
        return jsonify(dict(BTC_1H_STATE)), 200


@app.get('/btc-liquidation-refresh')
def btc_liquidation_refresh():
    if os.environ.get('COINGLASS_BTC_MONITOR', '1') == '1':
        _btc1h_start_monitor()
    secret = request.args.get('secret', '')
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({'ok': False, 'error': 'unauthorized'}), 401
    try:
        return jsonify({'ok': True, 'state': _btc1h_evaluate_once(send_alert=False)}), 200
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 502


@app.get('/btc-pushover-test')
def btc_pushover_test():
    secret = request.args.get('secret', '')
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({'ok': False, 'error': 'unauthorized'}), 401
    ok = _btc1h_send_pushover(
        'BTC 1H ALERT TEST',
        'CoinGlass Liquidation Trades alert connection is working.',
    )
    return jsonify({'ok': bool(ok), 'mode': 'btc_1h_pushover_test'}), 200 if ok else 502


_btc1h_load_state()
if os.environ.get('COINGLASS_BTC_MONITOR', '1') == '1':
    _btc1h_start_monitor()


# ============================================================
# INDIA FIXED-1H LIVE DASHBOARD
# NIFTY 10-STOCK + BANKNIFTY TOP-5
# NQ-STYLE stock-level display.
# Separate from all existing alert / NQ / BTC logic.
# ============================================================
INDIA_FIXED1H_DASHBOARD_STATE_FILE = os.path.join('/var/data', 'india_fixed1h_dashboard.json')
INDIA_FIXED1H_DASHBOARD_LOCK = threading.Lock()

def _india_fixed1h_default_state():
    def blank(weight):
        return {'received': False, 'direct': None, 'state': 'NONE', 'threshold': 0.50, 'weight': weight, 'hour_open_time': None, 'pine_update_time': None, 'updated_at_utc': None, 'updated_at_ist': None, 'stocks': []}
    return {'nifty': blank(52.87), 'banknifty': blank(61.23)}

def _india_fixed1h_load():
    state = _india_fixed1h_default_state()
    try:
        with INDIA_FIXED1H_DASHBOARD_LOCK:
            with open(INDIA_FIXED1H_DASHBOARD_STATE_FILE, 'r', encoding='utf-8') as f:
                saved = json.load(f)
        if isinstance(saved, dict):
            for key in ('nifty', 'banknifty'):
                if isinstance(saved.get(key), dict):
                    state[key].update(saved[key])
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f'[INDIA DASHBOARD READ ERROR] {exc}', flush=True)
    return state

def _india_fixed1h_save(state):
    os.makedirs(os.path.dirname(INDIA_FIXED1H_DASHBOARD_STATE_FILE), exist_ok=True)
    tmp = INDIA_FIXED1H_DASHBOARD_STATE_FILE + f'.{os.getpid()}.{threading.get_ident()}.tmp'
    with INDIA_FIXED1H_DASHBOARD_LOCK:
        try:
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(state, f, separators=(',', ':'), sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, INDIA_FIXED1H_DASHBOARD_STATE_FILE)
        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass

def _india_fixed1h_float(value):
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None

@app.post('/india-fixed1h-dashboard-webhook')
def india_fixed1h_dashboard_webhook():
    secret = request.args.get('secret', '')
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({'ok': False, 'error': 'unauthorized'}), 401
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        try:
            data = json.loads(request.get_data(as_text=True) or '{}')
        except Exception:
            data = {}

    payload_type = str(data.get('type', '')).strip().upper()
    if payload_type in {'NIFTY_DASHBOARD', 'NIFTY_FIXED_1H_DASHBOARD'}:
        key, expected_weight = 'nifty', 52.87
    elif payload_type in {'BANKNIFTY_DASHBOARD', 'BANKNIFTY_FIXED_1H_DASHBOARD'}:
        key, expected_weight = 'banknifty', 61.23
    else:
        return jsonify({'ok': False, 'error': 'unsupported_dashboard_type'}), 400

    stocks = []
    incoming_stocks = data.get('stocks', [])
    if isinstance(incoming_stocks, list):
        for item in incoming_stocks:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get('symbol', '')).strip().upper()
            if not symbol:
                continue
            stocks.append({
                'symbol': symbol,
                'weight': _india_fixed1h_float(item.get('weight')),
                'open': _india_fixed1h_float(item.get('open')),
                'live': _india_fixed1h_float(item.get('live')),
                'move': _india_fixed1h_float(
                    item.get('move') if item.get('move') is not None else item.get('move_pct')
                ),
                'contribution': _india_fixed1h_float(
                    item.get('contribution') if item.get('contribution') is not None else item.get('weighted')
                ),
            })

    direct = _india_fixed1h_float(data.get('direct'))
    threshold_value = _india_fixed1h_float(data.get('threshold'))
    weight_value = _india_fixed1h_float(
        data.get('weight') if data.get('weight') is not None else data.get('total_weight')
    )
    state_text = str(data.get('state', 'NONE')).strip().upper()
    if state_text not in {'BUY', 'SELL', 'NONE'}:
        state_text = 'NONE'

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ZoneInfo('Asia/Kolkata'))
    state = _india_fixed1h_load()
    state[key] = {
        'received': True, 'direct': direct,
        'state': state_text,
        'threshold': threshold_value if threshold_value is not None else 0.50,
        'weight': weight_value if weight_value is not None else expected_weight,
        'hour_open_time': data.get('hour_open_time') or data.get('base_time'),
        'pine_update_time': data.get('update_time'),
        'updated_at_utc': now_utc.isoformat(),
        'updated_at_ist': now_ist.strftime('%d-%m-%Y %H:%M:%S'),
        'stocks': stocks,
    }
    try:
        _india_fixed1h_save(state)
    except Exception as exc:
        print(f'[INDIA DASHBOARD SAVE ERROR] {exc}', flush=True)
        return jsonify({'ok': False, 'error': 'save_failed'}), 500

    print(f'[INDIA DASHBOARD UPDATE] {key.upper()} | DIRECT={direct} | STATE={state_text} | STOCKS={len(stocks)}', flush=True)
    return jsonify({'ok': True, 'mode': 'india_fixed1h_dashboard', 'asset': key, 'updated_at_ist': state[key]['updated_at_ist'], 'stocks_received': len(stocks)}), 200

@app.get('/india-fixed1h-dashboard-data')
def india_fixed1h_dashboard_data():
    return jsonify({'ok': True, **_india_fixed1h_load()}), 200

@app.get('/india-fixed1h-dashboard')
def india_fixed1h_dashboard():
    html = """<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NIFTY + BANKNIFTY FIXED 1H</title>
<style>
*{box-sizing:border-box}
body{margin:0;padding:20px;background:#0d1117;color:#f0f6fc;font-family:Arial,Helvetica,sans-serif}
.wrap{max-width:1180px;margin:auto}
h1{text-align:center;margin:4px 0 5px}
.sub{text-align:center;color:#8b949e;margin-bottom:22px}
.section{margin-bottom:24px}
.head{display:flex;justify-content:space-between;align-items:end;gap:12px;margin-bottom:10px}
.head h2{margin:0}
.meta{color:#8b949e;font-size:13px}
.summary{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px}
.card{background:#161b22;border:1px solid #30363d;border-radius:12px}
.metric{padding:13px 8px;text-align:center}
.label{color:#8b949e;font-size:11px;margin-bottom:6px}
.value{font-size:18px;font-weight:bold}
.table-card{overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:780px}
th{background:#21262d;padding:11px 8px;font-size:12px}
td{padding:11px 8px;text-align:center;border-top:1px solid #30363d;font-size:14px}
.buy{color:#3fb950;font-weight:bold}
.sell{color:#f85149;font-weight:bold}
.none{color:#d29922;font-weight:bold}
.footer{text-align:center;color:#8b949e;line-height:1.7;font-size:13px;margin-top:10px}
.live{color:#3fb950;font-weight:bold}
.waiting{color:#d29922;font-weight:bold}
@media(max-width:900px){.summary{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){body{padding:10px}.summary{grid-template-columns:1fr}.head{display:block}.meta{margin-top:5px}}
</style>
</head>
<body>
<div class="wrap">
<h1>NIFTY + BANKNIFTY FIXED 1H</h1>
<div class="sub">NQ-STYLE LIVE STOCK DASHBOARD • FIXED 1H OPEN • DIRECT ONLY • ±0.50%</div>

<div class="section">
<div class="head"><h2>NIFTY 10-STOCK</h2><div class="meta">TOP-10 ACTUAL WEIGHT • 52.87%</div></div>
<div class="summary">
<div class="card metric"><div class="label">1H DIRECT</div><div class="value" id="niftyDirect">--</div></div>
<div class="card metric"><div class="label">STATE</div><div class="value none" id="niftyState">NONE</div></div>
<div class="card metric"><div class="label">THRESHOLD</div><div class="value" id="niftyThreshold">±0.50%</div></div>
<div class="card metric"><div class="label">TOP-10 WEIGHT</div><div class="value" id="niftyWeight">52.87%</div></div>
</div>
<div class="card table-card"><table><thead><tr><th>STOCK</th><th>WEIGHT</th><th>1H OPEN</th><th>LIVE</th><th>OPEN→LIVE</th><th>WEIGHTED</th></tr></thead><tbody id="niftyRows"><tr><td colspan="6">Waiting for TradingView data...</td></tr></tbody></table></div>
<div class="footer">Last Update: <span id="niftyUpdated">--</span></div>
</div>

<div class="section">
<div class="head"><h2>BANKNIFTY TOP-5</h2><div class="meta">TOP-5 ACTUAL WEIGHT • 61.23%</div></div>
<div class="summary">
<div class="card metric"><div class="label">1H DIRECT</div><div class="value" id="bankDirect">--</div></div>
<div class="card metric"><div class="label">STATE</div><div class="value none" id="bankState">NONE</div></div>
<div class="card metric"><div class="label">THRESHOLD</div><div class="value" id="bankThreshold">±0.50%</div></div>
<div class="card metric"><div class="label">TOP-5 WEIGHT</div><div class="value" id="bankWeight">61.23%</div></div>
</div>
<div class="card table-card"><table><thead><tr><th>STOCK</th><th>WEIGHT</th><th>1H OPEN</th><th>LIVE</th><th>OPEN→LIVE</th><th>WEIGHTED</th></tr></thead><tbody id="bankRows"><tr><td colspan="6">Waiting for TradingView data...</td></tr></tbody></table></div>
<div class="footer">Last Update: <span id="bankUpdated">--</span></div>
</div>

<div class="footer" id="status">Loading...</div>
</div>
<script>
function esc(v){return String(v).replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#039;")}
function num(v,d){if(v===null||v===undefined||Number.isNaN(Number(v)))return"--";return Number(v).toFixed(d)}
function pct(v,d=3){if(v===null||v===undefined||Number.isNaN(Number(v)))return"--";let n=Number(v);return(n>=0?"+":"")+n.toFixed(d)+"%"}
function stateCls(v){v=String(v||"NONE").toUpperCase();return v==="BUY"?"buy":v==="SELL"?"sell":"none"}
function valueCls(v){if(v===null||v===undefined||Number.isNaN(Number(v)))return"";let n=Number(v);return n>0?"buy":n<0?"sell":"none"}
function stockRows(stocks){if(!Array.isArray(stocks)||stocks.length===0)return'<tr><td colspan="6">Waiting for stock-level TradingView data...</td></tr>';return stocks.map(s=>'<tr><td><strong>'+esc(s.symbol||"--")+'</strong></td><td>'+esc(num(s.weight,2))+'%</td><td>'+esc(num(s.open,2))+'</td><td>'+esc(num(s.live,2))+'</td><td class="'+valueCls(s.move)+'">'+esc(pct(s.move))+'</td><td class="'+valueCls(s.contribution)+'">'+esc(pct(s.contribution))+'</td></tr>').join("")}
function paint(asset,x){let p=asset==="nifty"?"nifty":"bank";document.getElementById(p+"Direct").textContent=pct(x.direct);let st=document.getElementById(p+"State");st.textContent=x.state||"NONE";st.className="value "+stateCls(x.state);document.getElementById(p+"Threshold").textContent="±"+num(x.threshold??0.50,2)+"%";document.getElementById(p+"Weight").textContent=num(x.weight??(asset==="nifty"?52.87:61.23),2)+"%";document.getElementById(p+"Updated").textContent=x.updated_at_ist?x.updated_at_ist+" IST":"--";document.getElementById(p+"Rows").innerHTML=stockRows(x.stocks)}
async function refresh(){try{let r=await fetch("/india-fixed1h-dashboard-data?ts="+Date.now(),{cache:"no-store"}),d=await r.json();paint("nifty",d.nifty||{});paint("banknifty",d.banknifty||{});let n=d.nifty&&d.nifty.received,b=d.banknifty&&d.banknifty.received,s=document.getElementById("status");s.innerHTML=n&&b?'<span class="live">LIVE</span> • Both TradingView feeds received • Browser refresh every 5 seconds':'<span class="waiting">WAITING</span> • '+(!n?"NIFTY ":"")+(!b?"BANKNIFTY ":"")+"feed not received yet"}catch(e){document.getElementById("status").textContent="Waiting for server..."}}
refresh();setInterval(refresh,5000)
</script>
</body>
</html>"""
    return html, 200, {'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store, no-cache, must-revalidate'}


# ============================================================
# COINGLASS LIQUIDATION TRADES DASHBOARD
# BTC + XAU always included + 8 highest-ranked unique assets.
# DATA ONLY: this route never sends Pushover.
# ============================================================

COINGLASS_DASHBOARD_STATE_FILE = os.path.join(
    "/tmp",
    "coinglass_top10_dashboard.json",
)
COINGLASS_DASHBOARD_LOCK = threading.Lock()


def _coinglass_dashboard_load():
    try:
        with COINGLASS_DASHBOARD_LOCK:
            with open(
                COINGLASS_DASHBOARD_STATE_FILE,
                "r",
                encoding="utf-8",
            ) as f:
                data = json.load(f)
        if isinstance(data, dict):
            return data
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f"[COINGLASS DASHBOARD READ ERROR] {exc}", flush=True)

    return {
        "updated_at_utc": None,
        "updated_at_ist": None,
        "assets": [],
    }


def _coinglass_dashboard_save(data):
    os.makedirs(
        os.path.dirname(COINGLASS_DASHBOARD_STATE_FILE),
        exist_ok=True,
    )
    tmp = (
        COINGLASS_DASHBOARD_STATE_FILE
        + f".{os.getpid()}.{threading.get_ident()}.tmp"
    )

    with COINGLASS_DASHBOARD_LOCK:
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    data,
                    f,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp, COINGLASS_DASHBOARD_STATE_FILE)
        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass


@app.post("/coinglass-dashboard-webhook")
def coinglass_dashboard_webhook():
    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    data = request.get_json(silent=True) or {}
    raw_assets = data.get("assets", [])

    if not isinstance(raw_assets, list):
        return jsonify({
            "ok": False,
            "error": "assets_list_required",
        }), 400

    assets = []
    seen = set()

    for item in raw_assets:
        if not isinstance(item, dict):
            continue

        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol or symbol in seen:
            continue

        try:
            rank = int(item.get("rank", 999999))
        except (TypeError, ValueError):
            rank = 999999

        try:
            long_count = float(item.get("long"))
            short_count = float(item.get("short"))
        except (TypeError, ValueError):
            continue

        difference = abs(long_count - short_count)

        if long_count > short_count:
            stronger = "LONG"
        elif short_count > long_count:
            stronger = "SHORT"
        else:
            stronger = "EQUAL"

        assets.append({
            "rank": rank,
            "symbol": symbol,
            "long": long_count,
            "short": short_count,
            "difference": difference,
            "stronger": stronger,
        })
        seen.add(symbol)

    # Monkey sends the already-selected 10 rows:
    # BTC + XAU compulsory, remaining 8 by CoinGlass ranking.
    assets = assets[:10]

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ZoneInfo("Asia/Kolkata"))

    state = {
        "updated_at_utc": now_utc.isoformat(),
        "updated_at_ist": now_ist.strftime("%d-%m-%Y %H:%M:%S"),
        "assets": assets,
    }

    try:
        _coinglass_dashboard_save(state)
    except Exception as exc:
        print(f"[COINGLASS DASHBOARD SAVE ERROR] {exc}", flush=True)
        return jsonify({
            "ok": False,
            "error": "save_failed",
        }), 500

    print(
        "[COINGLASS DASHBOARD UPDATE] "
        + ", ".join(x["symbol"] for x in assets),
        flush=True,
    )

    return jsonify({
        "ok": True,
        "mode": "coinglass_liquidation_trades_dashboard",
        "count": len(assets),
        "updated_at_ist": state["updated_at_ist"],
        "pushover": False,
    }), 200


@app.get("/coinglass-dashboard-data")
def coinglass_dashboard_data():
    return jsonify({
        "ok": True,
        **_coinglass_dashboard_load(),
    })


@app.get("/coinglass-dashboard")
def coinglass_dashboard():
    html = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CoinGlass Top-10 Dashboard</title>
<style>
body {
    margin: 0;
    padding: 22px;
    background: #0d1117;
    color: #f0f6fc;
    font-family: Arial, Helvetica, sans-serif;
}
.container {
    max-width: 1050px;
    margin: 0 auto;
}
h1 {
    text-align: center;
    margin: 0 0 7px;
}
.subtitle {
    text-align: center;
    color: #8b949e;
    margin-bottom: 22px;
}
.card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 12px;
    overflow-x: auto;
}
table {
    width: 100%;
    border-collapse: collapse;
}
th {
    background: #21262d;
    padding: 14px 10px;
    font-size: 13px;
}
td {
    padding: 14px 10px;
    text-align: center;
    border-top: 1px solid #30363d;
    font-size: 16px;
}
.asset {
    font-weight: 800;
    font-size: 17px;
}
.long {
    color: #3fb950;
    font-weight: 700;
}
.short {
    color: #f85149;
    font-weight: 700;
}
.equal {
    color: #d29922;
    font-weight: 700;
}
.fixed {
    color: #58a6ff;
}
.footer {
    text-align: center;
    color: #8b949e;
    margin-top: 18px;
    line-height: 1.8;
}
@media (max-width: 650px) {
    body { padding: 10px; }
    h1 { font-size: 21px; }
    th { font-size: 11px; }
    td { font-size: 13px; padding: 12px 5px; }
}
</style>
</head>
<body>
<div class="container">
    <h1>COINGLASS TOP-10 DASHBOARD</h1>
    <div class="subtitle">
        LIQUIDATION TRADES • 1H LONG vs 1H SHORT • BTC + XAU ALWAYS INCLUDED
    </div>

    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>RANK</th>
                    <th>ASSET</th>
                    <th>1H LONG</th>
                    <th>1H SHORT</th>
                    <th>DIFFERENCE</th>
                    <th>STRONGER</th>
                </tr>
            </thead>
            <tbody id="rows">
                <tr>
                    <td colspan="6">Waiting for CoinGlass feed...</td>
                </tr>
            </tbody>
        </table>
    </div>

    <div class="footer">
        BTC + XAU fixed • Remaining 8 follow CoinGlass ranking<br>
        Feed update: <span id="updated">--</span> IST<br>
        <span id="status">Loading...</span>
    </div>
</div>

<script>
function esc(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function fmt(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return "--";
    if (Number.isInteger(n)) return String(n);
    return n.toFixed(2).replace(/\.?0+$/, "");
}

async function refreshDashboard() {
    const status = document.getElementById("status");

    try {
        const response = await fetch(
            "/coinglass-dashboard-data?ts=" + Date.now(),
            {cache: "no-store"}
        );
        const data = await response.json();
        const rows = document.getElementById("rows");

        if (!Array.isArray(data.assets) || data.assets.length === 0) {
            rows.innerHTML =
                '<tr><td colspan="6">Waiting for CoinGlass feed...</td></tr>';
            status.textContent = "No feed received yet.";
            return;
        }

        let html = "";

        data.assets.forEach((item, index) => {
            const stronger = String(item.stronger || "EQUAL").toUpperCase();
            const cls =
                stronger === "LONG" ? "long" :
                stronger === "SHORT" ? "short" : "equal";
            const fixed =
                item.symbol === "BTC" || item.symbol === "XAU"
                ? " fixed" : "";

            html += "<tr>"
                + "<td>" + esc(index + 1) + "</td>"
                + '<td class="asset' + fixed + '">' + esc(item.symbol) + "</td>"
                + '<td class="long">' + esc(fmt(item.long)) + "</td>"
                + '<td class="short">' + esc(fmt(item.short)) + "</td>"
                + "<td><strong>" + esc(fmt(item.difference)) + "</strong></td>"
                + '<td class="' + cls + '">' + esc(stronger) + "</td>"
                + "</tr>";
        });

        rows.innerHTML = html;
        document.getElementById("updated").textContent =
            data.updated_at_ist || "--";
        status.textContent = "LIVE • Browser refresh every 5 seconds";
    } catch (error) {
        status.textContent = "Waiting for server...";
    }
}

refreshDashboard();
setInterval(refreshDashboard, 5000);
</script>
</body>
</html>
"""
    return html, 200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
    }


# ============================================================
# COINGLASS LIQUIDATION TRADES DASHBOARD — 4H
# Separate from existing 1H dashboard.
# BTC + XAU always included + remaining 8 CoinGlass-ranked assets.
# DATA ONLY: this route never sends Pushover.
# ============================================================

COINGLASS_4H_DASHBOARD_STATE_FILE = os.path.join(
    "/tmp",
    "coinglass_top10_dashboard_4h.json",
)
COINGLASS_4H_DASHBOARD_LOCK = threading.Lock()


def _coinglass_4h_dashboard_load():
    try:
        with COINGLASS_4H_DASHBOARD_LOCK:
            with open(
                COINGLASS_4H_DASHBOARD_STATE_FILE,
                "r",
                encoding="utf-8",
            ) as f:
                data = json.load(f)

        if isinstance(data, dict):
            return data

    except FileNotFoundError:
        pass

    except Exception as exc:
        print(
            f"[COINGLASS 4H DASHBOARD READ ERROR] {exc}",
            flush=True,
        )

    return {
        "updated_at_utc": None,
        "updated_at_ist": None,
        "assets": [],
    }


def _coinglass_4h_dashboard_save(data):
    os.makedirs(
        os.path.dirname(COINGLASS_4H_DASHBOARD_STATE_FILE),
        exist_ok=True,
    )

    tmp = (
        COINGLASS_4H_DASHBOARD_STATE_FILE
        + f".{os.getpid()}.{threading.get_ident()}.tmp"
    )

    with COINGLASS_4H_DASHBOARD_LOCK:
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    data,
                    f,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                f.flush()
                os.fsync(f.fileno())

            os.replace(
                tmp,
                COINGLASS_4H_DASHBOARD_STATE_FILE,
            )

        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass


@app.post("/coinglass-dashboard-4h-webhook")
def coinglass_dashboard_4h_webhook():
    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    data = request.get_json(silent=True) or {}
    raw_assets = data.get("assets", [])

    if not isinstance(raw_assets, list):
        return jsonify({
            "ok": False,
            "error": "assets_list_required",
        }), 400

    assets = []
    seen = set()

    for item in raw_assets:
        if not isinstance(item, dict):
            continue

        symbol = str(item.get("symbol", "")).strip().upper()

        if not symbol or symbol in seen:
            continue

        try:
            rank = int(item.get("rank", 999999))
        except (TypeError, ValueError):
            rank = 999999

        try:
            long_count = float(item.get("long"))
            short_count = float(item.get("short"))
        except (TypeError, ValueError):
            continue

        difference = abs(long_count - short_count)

        if long_count > short_count:
            stronger = "LONG"
        elif short_count > long_count:
            stronger = "SHORT"
        else:
            stronger = "EQUAL"

        assets.append({
            "rank": rank,
            "symbol": symbol,
            "long": long_count,
            "short": short_count,
            "difference": difference,
            "stronger": stronger,
        })

        seen.add(symbol)

    assets = assets[:10]

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(
        ZoneInfo("Asia/Kolkata")
    )

    state = {
        "updated_at_utc": now_utc.isoformat(),
        "updated_at_ist": now_ist.strftime(
            "%d-%m-%Y %H:%M:%S"
        ),
        "assets": assets,
    }

    try:
        _coinglass_4h_dashboard_save(state)

    except Exception as exc:
        print(
            f"[COINGLASS 4H DASHBOARD SAVE ERROR] {exc}",
            flush=True,
        )

        return jsonify({
            "ok": False,
            "error": "save_failed",
        }), 500

    print(
        "[COINGLASS 4H DASHBOARD UPDATE] "
        + ", ".join(x["symbol"] for x in assets),
        flush=True,
    )

    return jsonify({
        "ok": True,
        "mode": "coinglass_4h_liquidation_trades_dashboard",
        "count": len(assets),
        "updated_at_ist": state["updated_at_ist"],
        "pushover": False,
    }), 200


@app.get("/coinglass-dashboard-4h-data")
def coinglass_dashboard_4h_data():
    return jsonify({
        "ok": True,
        **_coinglass_4h_dashboard_load(),
    })


@app.get("/coinglass-dashboard-4h")
def coinglass_dashboard_4h():
    html = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CoinGlass Top-10 Dashboard 4H</title>

<style>
body {
    margin: 0;
    padding: 22px;
    background: #0d1117;
    color: #f0f6fc;
    font-family: Arial, Helvetica, sans-serif;
}
.container {
    max-width: 1050px;
    margin: 0 auto;
}
h1 {
    text-align: center;
    margin: 0 0 7px;
}
.subtitle {
    text-align: center;
    color: #8b949e;
    margin-bottom: 22px;
}
.card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 12px;
    overflow-x: auto;
}
table {
    width: 100%;
    border-collapse: collapse;
}
th {
    background: #21262d;
    padding: 14px 10px;
    font-size: 13px;
}
td {
    padding: 14px 10px;
    text-align: center;
    border-top: 1px solid #30363d;
    font-size: 16px;
}
.asset {
    font-weight: 800;
    font-size: 17px;
}
.long {
    color: #3fb950;
    font-weight: 700;
}
.short {
    color: #f85149;
    font-weight: 700;
}
.equal {
    color: #d29922;
    font-weight: 700;
}
.fixed {
    color: #58a6ff;
}
.footer {
    text-align: center;
    color: #8b949e;
    margin-top: 18px;
    line-height: 1.8;
}
@media (max-width: 650px) {
    body { padding: 10px; }
    h1 { font-size: 21px; }
    th { font-size: 11px; }
    td { font-size: 13px; padding: 12px 5px; }
}
</style>
</head>

<body>
<div class="container">

    <h1>COINGLASS TOP-10 DASHBOARD — 4H</h1>

    <div class="subtitle">
        LIQUIDATION TRADES • 4H LONG vs 4H SHORT • BTC + XAU ALWAYS INCLUDED
    </div>

    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>RANK</th>
                    <th>ASSET</th>
                    <th>4H LONG</th>
                    <th>4H SHORT</th>
                    <th>DIFFERENCE</th>
                    <th>STRONGER</th>
                </tr>
            </thead>

            <tbody id="rows">
                <tr>
                    <td colspan="6">
                        Waiting for CoinGlass 4H feed...
                    </td>
                </tr>
            </tbody>
        </table>
    </div>

    <div class="footer">
        BTC + XAU fixed • Remaining 8 follow CoinGlass ranking<br>
        Feed update: <span id="updated">--</span> IST<br>
        <span id="status">Loading...</span>
    </div>

</div>

<script>
function esc(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function fmt(value) {
    const n = Number(value);

    if (!Number.isFinite(n)) {
        return "--";
    }

    if (Number.isInteger(n)) {
        return String(n);
    }

    return n
        .toFixed(2)
        .replace(/\.?0+$/, "");
}

async function refreshDashboard() {
    const status =
        document.getElementById("status");

    try {
        const response = await fetch(
            "/coinglass-dashboard-4h-data?ts="
            + Date.now(),
            {cache: "no-store"}
        );

        const data =
            await response.json();

        const rows =
            document.getElementById("rows");

        if (
            !Array.isArray(data.assets)
            || data.assets.length === 0
        ) {
            rows.innerHTML =
                '<tr><td colspan="6">'
                + 'Waiting for CoinGlass 4H feed...'
                + '</td></tr>';

            status.textContent =
                "No 4H feed received yet.";

            return;
        }

        let html = "";

        data.assets.forEach(
            (item, index) => {

                const stronger =
                    String(
                        item.stronger || "EQUAL"
                    ).toUpperCase();

                const cls =
                    stronger === "LONG"
                    ? "long"
                    : stronger === "SHORT"
                    ? "short"
                    : "equal";

                const fixed =
                    item.symbol === "BTC"
                    || item.symbol === "XAU"
                    ? " fixed"
                    : "";

                html +=
                    "<tr>"
                    + "<td>"
                    + esc(index + 1)
                    + "</td>"
                    + '<td class="asset'
                    + fixed
                    + '">'
                    + esc(item.symbol)
                    + "</td>"
                    + '<td class="long">'
                    + esc(fmt(item.long))
                    + "</td>"
                    + '<td class="short">'
                    + esc(fmt(item.short))
                    + "</td>"
                    + "<td><strong>"
                    + esc(fmt(item.difference))
                    + "</strong></td>"
                    + '<td class="'
                    + cls
                    + '">'
                    + esc(stronger)
                    + "</td>"
                    + "</tr>";
            }
        );

        rows.innerHTML = html;

        document.getElementById(
            "updated"
        ).textContent =
            data.updated_at_ist || "--";

        status.textContent =
            "LIVE • Browser refresh every 5 seconds";

    } catch (error) {
        status.textContent =
            "Waiting for server...";
    }
}

refreshDashboard();

setInterval(
    refreshDashboard,
    5000
);
</script>

</body>
</html>
"""

    return html, 200, {
        "Content-Type":
            "text/html; charset=utf-8",
        "Cache-Control":
            "no-store, no-cache, must-revalidate",
    }

# ============================================================
# COINGLASS FEED WATCHDOG — 1H + 4H
# ============================================================
# Independent of Chrome/Tampermonkey execution.
# Checks the timestamps already stored by the 1H and 4H dashboard feeds.
# If a feed is older than 3 minutes, one Pushover STALE alert is sent.
# No repeat STALE spam while it remains stale.
# When the feed becomes fresh again, one RECOVERED alert is sent.

COINGLASS_WATCHDOG_CHECK_SECONDS = 60
COINGLASS_WATCHDOG_STALE_SECONDS = 180
COINGLASS_WATCHDOG_STARTUP_GRACE_SECONDS = 180
COINGLASS_WATCHDOG_LOCK_FILE = "/tmp/coinglass_feed_watchdog.lock"

_COINGLASS_WATCHDOG_STARTED_MONO = time.monotonic()


def _coinglass_watchdog_send_pushover(title, message):
    """Dedicated watchdog Pushover sender; bypasses trading-alert filters."""
    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        print(
            "[COINGLASS WATCHDOG] Pushover not configured",
            flush=True,
        )
        return False

    try:
        response = requests.post(
            PUSHOVER_URL,
            data={
                "token": PUSHOVER_TOKEN,
                "user": PUSHOVER_USER,
                "title": title,
                "message": message,
                "priority": 0,
            },
            timeout=10,
        )

        if not response.ok:
            print(
                f"[COINGLASS WATCHDOG PUSHOVER FAILED] HTTP {response.status_code}",
                flush=True,
            )

        return response.ok

    except requests.RequestException as exc:
        print(
            f"[COINGLASS WATCHDOG PUSHOVER ERROR] {exc}",
            flush=True,
        )
        return False


def _coinglass_watchdog_parse_utc(value):
    if not value:
        return None

    try:
        text = str(value).strip()

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        parsed = datetime.fromisoformat(text)

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.astimezone(timezone.utc)

    except (TypeError, ValueError):
        return None


def _coinglass_watchdog_feed_info(label, loader):
    try:
        data = loader()
    except Exception as exc:
        return {
            "label": label,
            "fresh": False,
            "age_seconds": None,
            "updated_at_ist": None,
            "reason": f"loader error: {type(exc).__name__}",
        }

    if not isinstance(data, dict):
        data = {}

    updated_utc = _coinglass_watchdog_parse_utc(
        data.get("updated_at_utc")
    )

    updated_ist = data.get("updated_at_ist")

    if updated_utc is None:
        return {
            "label": label,
            "fresh": False,
            "age_seconds": None,
            "updated_at_ist": updated_ist,
            "reason": "no valid feed timestamp",
        }

    age_seconds = max(
        0.0,
        (datetime.now(timezone.utc) - updated_utc).total_seconds(),
    )

    return {
        "label": label,
        "fresh": age_seconds <= COINGLASS_WATCHDOG_STALE_SECONDS,
        "age_seconds": age_seconds,
        "updated_at_ist": updated_ist,
        "reason": None,
    }


def _coinglass_watchdog_age_text(age_seconds):
    if age_seconds is None:
        return "unknown"

    total = int(round(age_seconds))
    minutes, seconds = divmod(total, 60)

    if minutes:
        return f"{minutes}m {seconds}s"

    return f"{seconds}s"


def _coinglass_feed_watchdog_loop():
    # Prevent duplicate watchdog threads if Gunicorn runs more than one worker.
    try:
        lock_handle = open(
            COINGLASS_WATCHDOG_LOCK_FILE,
            "a+",
            encoding="utf-8",
        )
        fcntl.flock(
            lock_handle.fileno(),
            fcntl.LOCK_EX | fcntl.LOCK_NB,
        )
    except (OSError, BlockingIOError):
        print(
            "[COINGLASS WATCHDOG] Another worker already owns watchdog lock",
            flush=True,
        )
        return

    print(
        "[COINGLASS WATCHDOG] Started — checking 1H + 4H every 60s; stale > 180s",
        flush=True,
    )

    last_state = {
        "1H": "UNKNOWN",
        "4H": "UNKNOWN",
    }

    while True:
        try:
            uptime_seconds = (
                time.monotonic()
                - _COINGLASS_WATCHDOG_STARTED_MONO
            )

            feeds = [
                _coinglass_watchdog_feed_info(
                    "1H",
                    _coinglass_dashboard_load,
                ),
                _coinglass_watchdog_feed_info(
                    "4H",
                    _coinglass_4h_dashboard_load,
                ),
            ]

            for info in feeds:
                label = info["label"]
                state = "FRESH" if info["fresh"] else "STALE"
                previous = last_state.get(label, "UNKNOWN")

                # Give Render/feed time to repopulate /tmp after restart/redeploy.
                if (
                    state == "STALE"
                    and uptime_seconds
                    < COINGLASS_WATCHDOG_STARTUP_GRACE_SECONDS
                ):
                    last_state[label] = "UNKNOWN"
                    continue

                if state == "STALE" and previous != "STALE":
                    age_text = _coinglass_watchdog_age_text(
                        info["age_seconds"]
                    )
                    updated_text = (
                        info["updated_at_ist"]
                        or "not available"
                    )
                    reason = info.get("reason")

                    message = (
                        f"CoinGlass {label} feed has stopped updating.\n"
                        f"Feed age: {age_text}\n"
                        f"Last update: {updated_text} IST\n"
                        f"Stale limit: {COINGLASS_WATCHDOG_STALE_SECONDS // 60} minutes"
                    )

                    if reason:
                        message += f"\nReason: {reason}"

                    _coinglass_watchdog_send_pushover(
                        f"COINGLASS {label} FEED STALE",
                        message,
                    )

                    print(
                        f"[COINGLASS WATCHDOG] {label} STALE | age={age_text}",
                        flush=True,
                    )

                elif state == "FRESH" and previous == "STALE":
                    updated_text = (
                        info["updated_at_ist"]
                        or "available"
                    )

                    _coinglass_watchdog_send_pushover(
                        f"COINGLASS {label} FEED RECOVERED",
                        (
                            f"CoinGlass {label} feed is updating again.\n"
                            f"Latest update: {updated_text} IST"
                        ),
                    )

                    print(
                        f"[COINGLASS WATCHDOG] {label} RECOVERED",
                        flush=True,
                    )

                last_state[label] = state

        except Exception as exc:
            print(
                f"[COINGLASS WATCHDOG LOOP ERROR] {type(exc).__name__}: {exc}",
                flush=True,
            )

        time.sleep(COINGLASS_WATCHDOG_CHECK_SECONDS)


def _start_coinglass_feed_watchdog():
    thread = threading.Thread(
        target=_coinglass_feed_watchdog_loop,
        name="coinglass-feed-watchdog",
        daemon=True,
    )
    thread.start()


_start_coinglass_feed_watchdog()

