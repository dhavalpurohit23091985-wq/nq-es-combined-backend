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

.combined-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 13px;
    padding: 15px;
    margin-bottom: 14px;
    text-align: center;
}

.combined-title {
    color: #58a6ff;
    font-size: 18px;
    font-weight: 900;
    margin-bottom: 4px;
}

.combined-rule {
    color: #8b949e;
    font-size: 12px;
    font-weight: 700;
    margin-bottom: 12px;
}

.combined-coins {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 8px;
    margin-bottom: 12px;
}

.combined-coin {
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 9px;
    padding: 9px 6px;
    font-size: 13px;
    font-weight: 800;
}

.combined-main {
    border-top: 1px solid #30363d;
    padding-top: 11px;
}

.combined-signal {
    font-size: 24px;
    font-weight: 900;
}

.combined-counts,
.combined-last,
.combined-engine {
    margin-top: 5px;
    color: #8b949e;
    font-size: 12px;
    font-weight: 700;
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
# FIXED 8 always included + next 2 highest-ranked non-fixed assets.
# DATA ONLY: this route never sends Pushover.
# ============================================================

COINGLASS_DASHBOARD_STATE_FILE = os.path.join(
    "/tmp",
    "coinglass_top10_dashboard.json",
)
COINGLASS_DASHBOARD_LOCK = threading.Lock()


# ============================================================
# COINGLASS FIXED-8 + NEXT-2 SELECTION
# Shared by BOTH 1H and 4H dashboard webhooks.
# Bad/old payloads that omit any fixed asset are rejected so
# they can never overwrite a good dashboard state.
# ============================================================

COINGLASS_FIXED_ASSETS = (
    "BTC",
    "XAU",
    "ETH",
    "SOL",
    "XRP",
    "NEAR",
    "DOGE",
    "ZEC",
)
COINGLASS_FIXED_ASSET_SET = set(COINGLASS_FIXED_ASSETS)


def _select_coinglass_fixed8_plus2(parsed_assets):
    by_symbol = {
        str(item.get("symbol", "")).upper(): item
        for item in parsed_assets
        if isinstance(item, dict)
        and str(item.get("symbol", "")).strip()
    }

    missing_fixed = [
        symbol
        for symbol in COINGLASS_FIXED_ASSETS
        if symbol not in by_symbol
    ]

    if missing_fixed:
        return None, {
            "error": "fixed_assets_missing",
            "missing": missing_fixed,
        }

    ranked_non_fixed = sorted(
        (
            item
            for item in parsed_assets
            if str(item.get("symbol", "")).upper()
            not in COINGLASS_FIXED_ASSET_SET
        ),
        key=lambda item: (
            int(item.get("rank", 999999)),
            str(item.get("symbol", "")),
        ),
    )

    if len(ranked_non_fixed) < 2:
        return None, {
            "error": "ranked_assets_missing",
            "available_non_fixed": len(ranked_non_fixed),
        }

    selected = [
        by_symbol[symbol]
        for symbol in COINGLASS_FIXED_ASSETS
    ] + ranked_non_fixed[:2]

    # Keep browser order aligned with CoinGlass ranking while
    # guaranteeing the fixed eight can never disappear.
    selected.sort(
        key=lambda item: (
            int(item.get("rank", 999999)),
            str(item.get("symbol", "")),
        )
    )

    return selected, None


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

    selected_assets, selection_error = (
        _select_coinglass_fixed8_plus2(assets)
    )

    if selection_error:
        print(
            "[COINGLASS DASHBOARD REJECTED] "
            + json.dumps(selection_error, sort_keys=True),
            flush=True,
        )
        return jsonify({
            "ok": False,
            **selection_error,
            "required_fixed": list(COINGLASS_FIXED_ASSETS),
        }), 422

    assets = selected_assets

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
        LIQUIDATION TRADES • 1H LONG vs 1H SHORT • FIXED 8 + NEXT 2 RANKED
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
        FIXED: BTC XAU ETH SOL XRP NEAR DOGE ZEC • NEXT 2 follow CoinGlass ranking<br>
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
                [
                    "BTC", "XAU", "ETH", "SOL",
                    "XRP", "NEAR", "DOGE", "ZEC"
                ].includes(item.symbol)
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
# FIXED 8 always included + next 2 CoinGlass-ranked non-fixed assets.
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

    selected_assets, selection_error = (
        _select_coinglass_fixed8_plus2(assets)
    )

    if selection_error:
        print(
            "[COINGLASS 4H DASHBOARD REJECTED] "
            + json.dumps(selection_error, sort_keys=True),
            flush=True,
        )
        return jsonify({
            "ok": False,
            **selection_error,
            "required_fixed": list(COINGLASS_FIXED_ASSETS),
        }), 422

    assets = selected_assets

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
        LIQUIDATION TRADES • 4H LONG vs 4H SHORT • FIXED 8 + NEXT 2 RANKED
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
        FIXED: BTC XAU ETH SOL XRP NEAR DOGE ZEC • NEXT 2 follow CoinGlass ranking<br>
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
                    [
                        "BTC", "XAU", "ETH", "SOL",
                        "XRP", "NEAR", "DOGE", "ZEC"
                    ].includes(item.symbol)
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

# ============================================================
# COINGLASS LIQUIDATION VALUE DASHBOARD — FINAL 4H
# FINAL 7 ONLY: BTC, ETH, SOL, XRP, NEAR, XAU, DOGE.
# DATA ONLY: these routes NEVER send Pushover.
#
# Thresholds:
#   BTC / ETH / SOL -> $1,000,000 gap
#   XRP / NEAR / XAU / DOGE -> $100,000 gap
#
# Signal convention:
#   SHORT liquidation value - LONG liquidation value >= threshold -> BUY
#   LONG liquidation value - SHORT liquidation value >= threshold -> SELL
# ============================================================

COINGLASS_VALUE_4H_DASHBOARD_STATE_FILE = os.path.join(
    "/tmp",
    "coinglass_liquidation_value_4h.json",
)
COINGLASS_VALUE_4H_DASHBOARD_LOCK = threading.Lock()

COINGLASS_VALUE_FIXED_ASSETS = (
    "BTC",
    "ETH",
    "SOL",
    "XRP",
    "NEAR",
    "DOGE",
    "ZEC",
    "XAU",
)
COINGLASS_VALUE_FIXED_ASSET_SET = set(
    COINGLASS_VALUE_FIXED_ASSETS
)

COINGLASS_VALUE_TOP3 = {
    "BTC",
    "ETH",
    "SOL",
}


def _coinglass_value_4h_threshold(symbol):
    symbol = str(symbol or "").strip().upper()

    if symbol in COINGLASS_VALUE_TOP3:
        return 1_000_000.0

    if symbol in {"XRP", "NEAR", "XAU", "DOGE"}:
        return 100_000.0

    # Final 4H feed should contain only the seven configured assets.
    # Keep unknown assets non-triggering by assigning an effectively
    # unreachable threshold instead of accidentally using a low default.
    return float("inf")


def _select_coinglass_value_fixed8_plus2(parsed_assets):
    by_symbol = {
        str(item.get("symbol", "")).strip().upper(): item
        for item in parsed_assets
        if isinstance(item, dict)
        and str(item.get("symbol", "")).strip()
    }

    missing_fixed = [
        symbol
        for symbol in COINGLASS_VALUE_FIXED_ASSETS
        if symbol not in by_symbol
    ]

    if missing_fixed:
        return None, {
            "error": "fixed_assets_missing",
            "missing": missing_fixed,
        }

    ranked_non_fixed = sorted(
        (
            item
            for item in parsed_assets
            if str(item.get("symbol", "")).strip().upper()
            not in COINGLASS_VALUE_FIXED_ASSET_SET
        ),
        key=lambda item: (
            int(item.get("rank", 999999)),
            str(item.get("symbol", "")),
        ),
    )

    if len(ranked_non_fixed) < 2:
        return None, {
            "error": "ranked_assets_missing",
            "available_non_fixed": len(ranked_non_fixed),
        }

    selected = [
        by_symbol[symbol]
        for symbol in COINGLASS_VALUE_FIXED_ASSETS
    ] + ranked_non_fixed[:2]

    # Display in CoinGlass ranking order while guaranteeing
    # that all eight fixed assets are always present.
    selected.sort(
        key=lambda item: (
            int(item.get("rank", 999999)),
            str(item.get("symbol", "")),
        )
    )

    return selected, None


def _coinglass_value_4h_dashboard_load():
    try:
        with COINGLASS_VALUE_4H_DASHBOARD_LOCK:
            with open(
                COINGLASS_VALUE_4H_DASHBOARD_STATE_FILE,
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
            f"[COINGLASS VALUE 4H DASHBOARD READ ERROR] {exc}",
            flush=True,
        )

    return {
        "updated_at_utc": None,
        "updated_at_ist": None,
        "assets": [],
    }


def _coinglass_value_4h_dashboard_save(data):
    os.makedirs(
        os.path.dirname(
            COINGLASS_VALUE_4H_DASHBOARD_STATE_FILE
        ),
        exist_ok=True,
    )

    tmp = (
        COINGLASS_VALUE_4H_DASHBOARD_STATE_FILE
        + f".{os.getpid()}.{threading.get_ident()}.tmp"
    )

    with COINGLASS_VALUE_4H_DASHBOARD_LOCK:
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
                COINGLASS_VALUE_4H_DASHBOARD_STATE_FILE,
            )

        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass


@app.post("/coinglass-liquidation-value-4h-webhook")
def coinglass_liquidation_value_4h_webhook():
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

        symbol = str(
            item.get("symbol", "")
        ).strip().upper()

        if not symbol or symbol in seen:
            continue

        try:
            rank = int(
                item.get("rank", 999999)
            )
        except (TypeError, ValueError):
            rank = 999999

        try:
            price_raw = item.get("price")
            price_value = (
                float(price_raw)
                if price_raw is not None
                else None
            )
        except (TypeError, ValueError):
            price_value = None

        try:
            long_value = float(item.get("long"))
            short_value = float(item.get("short"))
        except (TypeError, ValueError):
            continue

        if long_value < 0 or short_value < 0:
            continue

        if price_value is not None and price_value < 0:
            price_value = None

        difference = abs(
            long_value - short_value
        )

        if long_value > short_value:
            stronger = "LONG"
        elif short_value > long_value:
            stronger = "SHORT"
        else:
            stronger = "EQUAL"

        threshold = _coinglass_value_4h_threshold(
            symbol
        )

        if short_value - long_value >= threshold:
            signal = "BUY"
        elif long_value - short_value >= threshold:
            signal = "SELL"
        else:
            signal = "NONE"

        assets.append({
            "rank": rank,
            "symbol": symbol,
            "price": price_value,
            "long": long_value,
            "short": short_value,
            "difference": difference,
            "stronger": stronger,
            "threshold": threshold,
            "signal": signal,
        })

        seen.add(symbol)

    final_4h_symbols = (
        "BTC",
        "ETH",
        "SOL",
        "XRP",
        "NEAR",
        "XAU",
        "DOGE",
    )

    by_symbol = {
        str(item.get("symbol", "")).strip().upper(): item
        for item in assets
        if isinstance(item, dict)
    }

    missing = [
        symbol
        for symbol in final_4h_symbols
        if symbol not in by_symbol
    ]

    if missing:
        print(
            "[COINGLASS VALUE 4H FINAL7 REJECTED] "
            + json.dumps({"missing": missing}, sort_keys=True),
            flush=True,
        )

        return jsonify({
            "ok": False,
            "error": "final_4h_assets_missing",
            "missing": missing,
            "required": list(final_4h_symbols),
        }), 422

    assets = [
        by_symbol[symbol]
        for symbol in final_4h_symbols
    ]

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
        _coinglass_value_4h_dashboard_save(
            state
        )

    except Exception as exc:
        print(
            f"[COINGLASS VALUE 4H DASHBOARD SAVE ERROR] {exc}",
            flush=True,
        )

        return jsonify({
            "ok": False,
            "error": "save_failed",
        }), 500

    print(
        "[COINGLASS VALUE 4H DASHBOARD UPDATE] "
        + ", ".join(
            x["symbol"]
            for x in assets
        ),
        flush=True,
    )

    return jsonify({
        "ok": True,
        "mode": "coinglass_4h_liquidation_value_dashboard",
        "count": len(assets),
        "updated_at_ist": state[
            "updated_at_ist"
        ],
        "pushover": False,
    }), 200


# ============================================================
# TRADINGVIEW CRYPTOCAP:TOTAL — 4H LEGEND CHANGE +/-10B
# Telemetry only. Existing CoinGlass FINAL7 remains untouched.
# Pushover is sent by the TradingView Tampermonkey userscript only.
# ============================================================

TRADINGVIEW_TOTAL_4H_STATE_FILE = os.path.join(
    "/tmp", "tradingview_total_4h_value.json"
)
TRADINGVIEW_TOTAL_4H_LOCK = threading.Lock()


def _tradingview_total_4h_load():
    try:
        with TRADINGVIEW_TOTAL_4H_LOCK:
            with open(TRADINGVIEW_TOTAL_4H_STATE_FILE, "r", encoding="utf-8") as file:
                payload = json.load(file)
        if isinstance(payload, dict):
            return payload
    except FileNotFoundError:
        pass
    except Exception as exc:
        print("[TV TOTAL 4H STATE READ ERROR] " + str(exc), flush=True)
    return {
        "open_display": None,
        "close_display": None,
        "change_b": None,
        "change_text": None,
        "signal": "WAITING",
        "updated_at_utc": None,
        "updated_at_ist": None,
        "last_alert_signal": None,
        "last_alert_ist": None,
    }


def _tradingview_total_4h_save(payload):
    temp = (TRADINGVIEW_TOTAL_4H_STATE_FILE +
            f".{os.getpid()}.{threading.get_ident()}.tmp")
    with TRADINGVIEW_TOTAL_4H_LOCK:
        try:
            with open(temp, "w", encoding="utf-8") as file:
                json.dump(payload, file, separators=(",", ":"), sort_keys=True)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp, TRADINGVIEW_TOTAL_4H_STATE_FILE)
        finally:
            try:
                if os.path.exists(temp):
                    os.remove(temp)
            except OSError:
                pass


@app.post("/tradingview-total-4h-webhook")
def tradingview_total_4h_webhook():
    secret = request.args.get("secret", "")
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    incoming = request.get_json(silent=True)
    if not isinstance(incoming, dict):
        return jsonify({"ok": False, "error": "json_object_required"}), 400

    if incoming.get("symbol") != "CRYPTOCAP:TOTAL" or incoming.get("timeframe") != "4h":
        return jsonify({"ok": False, "error": "wrong_symbol_or_timeframe"}), 422

    try:
        change_b = float(incoming["change_b"])
    except (TypeError, ValueError, KeyError, OverflowError):
        return jsonify({"ok": False, "error": "invalid_change_b"}), 400

    # Bound obviously corrupt readings (NaN/inf or impossible market-cap changes).
    import math
    if not math.isfinite(change_b) or abs(change_b) > 10000:
        return jsonify({"ok": False, "error": "invalid_change_b"}), 400

    open_text = str(incoming.get("open_display") or "")[:32]
    close_text = str(incoming.get("close_display") or "")[:32]
    change_text = str(incoming.get("change_text") or "")[:32]

    if not re.fullmatch(r"[0-9][0-9,.]*\s*[TBMK]", open_text, flags=re.I):
        return jsonify({"ok": False, "error": "bad_open"}), 400
    if not re.fullmatch(r"[0-9][0-9,.]*\s*[TBMK]", close_text, flags=re.I):
        return jsonify({"ok": False, "error": "bad_close"}), 400

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ZoneInfo("Asia/Kolkata"))

    # Signal is computed independently server-side from the reported legend change.
    signal = "BUY" if change_b >= 10 else "SELL" if change_b <= -10 else "NONE"
    last_alert_signal = str(incoming.get("last_alert_signal") or "").upper()
    if last_alert_signal not in {"BUY", "SELL"}:
        last_alert_signal = None
    last_alert_ist = str(incoming.get("last_alert_ist") or "")[:32] or None

    payload = {
        "open_display": open_text,
        "close_display": close_text,
        "change_b": round(change_b, 6),
        "change_text": change_text,
        "signal": signal,
        "updated_at_utc": now_utc.isoformat(),
        "updated_at_ist": now_ist.strftime("%d-%m-%Y %H:%M:%S"),
        "last_alert_signal": last_alert_signal,
        "last_alert_ist": last_alert_ist,
    }
    try:
        _tradingview_total_4h_save(payload)
    except Exception as exc:
        print("[TV TOTAL 4H STATE WRITE ERROR] " + str(exc), flush=True)
        return jsonify({"ok": False, "error": "save_failed"}), 500

    return jsonify({"ok": True, "signal": signal, "pushover": False}), 200


# ============================================================
# TRADINGVIEW CRYPTOCAP:XAUT — 4H LEGEND CHANGE +/-10M
# Dashboard telemetry only; Tampermonkey sends Pushover independently.
# This does not change the CoinGlass FINAL7 or TOTAL feed/alert logic.
# ============================================================

TRADINGVIEW_XAUT_4H_STATE_FILE = os.path.join(
    "/tmp", "tradingview_xaut_4h_value.json"
)
TRADINGVIEW_XAUT_4H_LOCK = threading.Lock()


def _tradingview_xaut_4h_load():
    try:
        with TRADINGVIEW_XAUT_4H_LOCK:
            with open(TRADINGVIEW_XAUT_4H_STATE_FILE, "r", encoding="utf-8") as file:
                payload = json.load(file)
        if isinstance(payload, dict):
            return payload
    except FileNotFoundError:
        pass
    except Exception as exc:
        print("[TV XAUT 4H STATE READ ERROR] " + str(exc), flush=True)
    return {
        "open_display": None,
        "close_display": None,
        "change_m": None,
        "change_text": None,
        "signal": "WAITING",
        "updated_at_utc": None,
        "updated_at_ist": None,
        "last_alert_signal": None,
        "last_alert_ist": None,
    }


def _tradingview_xaut_4h_save(payload):
    temp = (TRADINGVIEW_XAUT_4H_STATE_FILE +
            f".{os.getpid()}.{threading.get_ident()}.tmp")
    with TRADINGVIEW_XAUT_4H_LOCK:
        try:
            with open(temp, "w", encoding="utf-8") as file:
                json.dump(payload, file, separators=(",", ":"), sort_keys=True)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp, TRADINGVIEW_XAUT_4H_STATE_FILE)
        finally:
            try:
                if os.path.exists(temp):
                    os.remove(temp)
            except OSError:
                pass


@app.post("/tradingview-xaut-4h-webhook")
def tradingview_xaut_4h_webhook():
    # Unlike older feed endpoints, keep the secret out of the URL/query logs.
    import hmac
    secret = request.headers.get("X-Webhook-Secret", "")
    if not WEBHOOK_SECRET or not hmac.compare_digest(secret, WEBHOOK_SECRET):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    incoming = request.get_json(silent=True)
    if not isinstance(incoming, dict):
        return jsonify({"ok": False, "error": "json_object_required"}), 400

    if incoming.get("symbol") != "CRYPTOCAP:XAUT" or incoming.get("timeframe") != "4h":
        return jsonify({"ok": False, "error": "wrong_symbol_or_timeframe"}), 422

    try:
        change_m = float(incoming["change_m"])
    except (TypeError, ValueError, KeyError, OverflowError):
        return jsonify({"ok": False, "error": "invalid_change_m"}), 400

    import math
    if not math.isfinite(change_m) or abs(change_m) > 1000000:
        return jsonify({"ok": False, "error": "invalid_change_m"}), 400

    open_text = str(incoming.get("open_display") or "")[:32]
    close_text = str(incoming.get("close_display") or "")[:32]
    change_text = str(incoming.get("change_text") or "")[:32]

    if not re.fullmatch(r"[0-9][0-9,.]*\s*[TBMK]", open_text, flags=re.I):
        return jsonify({"ok": False, "error": "bad_open"}), 400
    if not re.fullmatch(r"[0-9][0-9,.]*\s*[TBMK]", close_text, flags=re.I):
        return jsonify({"ok": False, "error": "bad_close"}), 400

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ZoneInfo("Asia/Kolkata"))
    signal = "BUY" if change_m >= 10 else "SELL" if change_m <= -10 else "NONE"
    last_alert_signal = str(incoming.get("last_alert_signal") or "").upper()
    if last_alert_signal not in {"BUY", "SELL"}:
        last_alert_signal = None
    last_alert_ist = str(incoming.get("last_alert_ist") or "")[:40] or None

    payload = {
        "open_display": open_text,
        "close_display": close_text,
        "change_m": round(change_m, 6),
        "change_text": change_text,
        "signal": signal,
        "updated_at_utc": now_utc.isoformat(),
        "updated_at_ist": now_ist.strftime("%d-%m-%Y %H:%M:%S"),
        "last_alert_signal": last_alert_signal,
        "last_alert_ist": last_alert_ist,
    }

    try:
        _tradingview_xaut_4h_save(payload)
    except Exception as exc:
        print("[TV XAUT 4H STATE WRITE ERROR] " + str(exc), flush=True)
        return jsonify({"ok": False, "error": "save_failed"}), 500

    return jsonify({"ok": True, "signal": signal, "pushover": False}), 200


@app.get("/tradingview-xaut-4h-data")
def tradingview_xaut_4h_data():
    return jsonify({"ok": True, **_tradingview_xaut_4h_load()})


# ============================================================
# TRADINGVIEW CRYPTOCAP:QQQB — 4H LEGEND CHANGE +/-10K
# Dashboard telemetry only; Tampermonkey sends Pushover independently.
# This does not change the CoinGlass FINAL7 or TOTAL feed/alert logic.
# ============================================================

TRADINGVIEW_QQQB_4H_STATE_FILE = os.path.join(
    "/tmp", "tradingview_qqqb_4h_value.json"
)
TRADINGVIEW_QQQB_4H_LOCK = threading.Lock()


def _tradingview_qqqb_4h_load():
    try:
        with TRADINGVIEW_QQQB_4H_LOCK:
            with open(TRADINGVIEW_QQQB_4H_STATE_FILE, "r", encoding="utf-8") as file:
                payload = json.load(file)
        if isinstance(payload, dict):
            return payload
    except FileNotFoundError:
        pass
    except Exception as exc:
        print("[TV QQQB 4H STATE READ ERROR] " + str(exc), flush=True)
    return {
        "open_display": None,
        "close_display": None,
        "change_k": None,
        "change_text": None,
        "signal": "WAITING",
        "updated_at_utc": None,
        "updated_at_ist": None,
        "last_alert_signal": None,
        "last_alert_ist": None,
    }


def _tradingview_qqqb_4h_save(payload):
    temp = (TRADINGVIEW_QQQB_4H_STATE_FILE +
            f".{os.getpid()}.{threading.get_ident()}.tmp")
    with TRADINGVIEW_QQQB_4H_LOCK:
        try:
            with open(temp, "w", encoding="utf-8") as file:
                json.dump(payload, file, separators=(",", ":"), sort_keys=True)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp, TRADINGVIEW_QQQB_4H_STATE_FILE)
        finally:
            try:
                if os.path.exists(temp):
                    os.remove(temp)
            except OSError:
                pass


@app.post("/tradingview-qqqb-4h-webhook")
def tradingview_qqqb_4h_webhook():
    # Unlike older feed endpoints, keep the secret out of the URL/query logs.
    import hmac
    secret = request.headers.get("X-Webhook-Secret", "")
    if not WEBHOOK_SECRET or not hmac.compare_digest(secret, WEBHOOK_SECRET):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    incoming = request.get_json(silent=True)
    if not isinstance(incoming, dict):
        return jsonify({"ok": False, "error": "json_object_required"}), 400

    if incoming.get("symbol") != "CRYPTOCAP:QQQB" or incoming.get("timeframe") != "4h":
        return jsonify({"ok": False, "error": "wrong_symbol_or_timeframe"}), 422

    try:
        change_k = float(incoming["change_k"])
    except (TypeError, ValueError, KeyError, OverflowError):
        return jsonify({"ok": False, "error": "invalid_change_k"}), 400

    import math
    if not math.isfinite(change_k) or abs(change_k) > 1000000000:
        return jsonify({"ok": False, "error": "invalid_change_k"}), 400

    open_text = str(incoming.get("open_display") or "")[:32]
    close_text = str(incoming.get("close_display") or "")[:32]
    change_text = str(incoming.get("change_text") or "")[:32]

    if not re.fullmatch(r"[0-9][0-9,.]*\s*[TBMK]", open_text, flags=re.I):
        return jsonify({"ok": False, "error": "bad_open"}), 400
    if not re.fullmatch(r"[0-9][0-9,.]*\s*[TBMK]", close_text, flags=re.I):
        return jsonify({"ok": False, "error": "bad_close"}), 400

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ZoneInfo("Asia/Kolkata"))
    signal = "BUY" if change_k >= 10 else "SELL" if change_k <= -10 else "NONE"
    last_alert_signal = str(incoming.get("last_alert_signal") or "").upper()
    if last_alert_signal not in {"BUY", "SELL"}:
        last_alert_signal = None
    last_alert_ist = str(incoming.get("last_alert_ist") or "")[:40] or None

    payload = {
        "open_display": open_text,
        "close_display": close_text,
        "change_k": round(change_k, 6),
        "change_text": change_text,
        "signal": signal,
        "updated_at_utc": now_utc.isoformat(),
        "updated_at_ist": now_ist.strftime("%d-%m-%Y %H:%M:%S"),
        "last_alert_signal": last_alert_signal,
        "last_alert_ist": last_alert_ist,
    }

    try:
        _tradingview_qqqb_4h_save(payload)
    except Exception as exc:
        print("[TV QQQB 4H STATE WRITE ERROR] " + str(exc), flush=True)
        return jsonify({"ok": False, "error": "save_failed"}), 500

    return jsonify({"ok": True, "signal": signal, "pushover": False}), 200


@app.get("/tradingview-qqqb-4h-data")
def tradingview_qqqb_4h_data():
    return jsonify({"ok": True, **_tradingview_qqqb_4h_load()})


@app.get("/coinglass-liquidation-value-4h-data")
def coinglass_liquidation_value_4h_data():
    state = _coinglass_value_4h_dashboard_load()
    engine = _coinglass_value_alert_engine_load().get("4H", {})

    return jsonify({
        "ok": True,
        **state,
        "alert_engine_4h": engine,
        "tradingview_total_4h": _tradingview_total_4h_load(),
        "tradingview_xaut_4h": _tradingview_xaut_4h_load(),
        "tradingview_qqqb_4h": _tradingview_qqqb_4h_load(),
    })


@app.get("/coinglass-liquidation-value")
@app.get("/coinglass-liquidation-value-4h")
def coinglass_liquidation_value_4h():
    html = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CoinGlass Final 4H Liquidation Dominance</title>
<style>
:root { color-scheme: dark; }
* { box-sizing: border-box; }
body {
    margin: 0;
    padding: 18px;
    background: #0d1117;
    color: #f0f6fc;
    font-family: Arial, Helvetica, sans-serif;
}
.container { max-width: 1050px; margin: 0 auto; }
h1 { text-align: center; margin: 0 0 6px; font-size: 28px; }
.subtitle { text-align: center; color: #8b949e; line-height: 1.55; margin-bottom: 15px; }
.status-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 14px; }
.status-box {
    background: #161b22; border: 1px solid #30363d; border-radius: 10px;
    padding: 11px; text-align: center; color: #8b949e; line-height: 1.5; font-size: 13px;
}
.status-box strong { color: #f0f6fc; }
.card { background: #161b22; border: 1px solid #30363d; border-radius: 12px; overflow-x: auto; }
table { width: 100%; border-collapse: collapse; min-width: 780px; }
th { background: #21262d; padding: 13px 8px; font-size: 12px; white-space: nowrap; }
td { padding: 13px 8px; text-align: center; border-top: 1px solid #30363d; font-size: 14px; }
.asset { font-weight: 900; }
.price { color: #f0f6fc; font-weight: 900; }
.long { color: #f85149; }
.short { color: #3fb950; }
.buy { color: #3fb950; font-weight: 900; }
.sell { color: #f85149; font-weight: 900; }
.none, .equal { color: #d29922; font-weight: 900; }
/* GAP COLOR ONLY: SHORT dominance = red, LONG dominance = green */
.gap-short { color: #f85149; font-weight: 900; }
.gap-long { color: #3fb950; font-weight: 900; }
.gap-equal { color: #d29922; font-weight: 900; }
.live { color: #3fb950; font-weight: 900; }
.stale { color: #f85149; font-weight: 900; }
.footer { text-align: center; color: #8b949e; font-size: 12px; line-height: 1.6; margin-top: 14px; }

/* TradingView TOTAL card appears above the seven CoinGlass coin cards. */
.tv-total-card {
    background: #161b22; border: 1px solid #30363d;
    border-radius: 12px; padding: 14px; margin-bottom: 14px;
}
.tv-total-head { display: flex; justify-content: space-between; align-items: center;
    gap: 12px; font-weight: 900; font-size: 18px; margin-bottom: 12px; }
.tv-total-metrics { display: grid; grid-template-columns: 1fr 1fr; gap: 12px 18px; }
.tv-total-metric { display: flex; justify-content: space-between; gap: 10px;
    padding-bottom: 8px; border-bottom: 1px solid #30363d; font-size: 14px; }
.tv-total-metric span { color: #8b949e; }
.tv-total-metric strong { text-align: right; }
.tv-total-caption { color: #8b949e; font-size: 12px; line-height: 1.5; margin-top: 12px; }
@media (max-width: 720px) {
    .tv-total-head { font-size: 16px; }
    .tv-total-metric { font-size: 12px; }
    .tv-total-metrics { gap: 10px; }
}
.mobile-list { display: none; }
@media (max-width: 720px) {
    body { padding: 10px; }
    h1 { font-size: 22px; }
    .status-grid { grid-template-columns: 1fr; }
    .desktop-card { display: none; }
    .mobile-list { display: block; }
    .asset-card {
        background: #161b22; border: 1px solid #30363d; border-radius: 11px;
        margin-bottom: 9px; padding: 12px;
    }
    .asset-head { display:flex; justify-content:space-between; gap:10px; font-weight:900; margin-bottom:8px; }
    .metrics { display:grid; grid-template-columns:1fr 1fr; gap:6px 10px; font-size:12px; }
    .metric { display:flex; justify-content:space-between; gap:8px; }
    .metric span:first-child { color:#8b949e; }
    .metric.price-row { grid-column: 1 / -1; border-bottom: 1px solid #30363d; padding-bottom: 7px; margin-bottom: 1px; }
    .metric.price-row strong { color:#f0f6fc; font-size:14px; }
}
</style>
</head>
<body>
<div class="container">
    <h1>COINGLASS LIQUIDATION VALUE — FINAL 4H</h1>
    <div class="subtitle">
        BTC / ETH / SOL: minimum gap $1M<br>
        XRP / NEAR / XAU / DOGE: minimum gap $100K<br>
        SHORT &gt; LONG = BUY • LONG &gt; SHORT = SELL • Strict BUY → SELL → BUY
    </div>

    <div class="status-grid">
        <div class="status-box">
            <strong>4H FEED</strong><br>
            <span id="updated">--</span><br>
            <span id="feedAge">--</span>
        </div>
        <div class="status-box">
            <strong>4H ALERT ENGINE</strong><br>
            <span id="engineUpdated">--</span><br>
            <span id="engineAge">--</span>
        </div>
    </div>

    <div class="tv-total-card" id="tvTotalCard">
        <div class="tv-total-head">
            <span>TRADINGVIEW TOTAL — 4H</span>
            <span id="tvTotalSignal" class="none">WAITING</span>
        </div>
        <div class="tv-total-metrics">
            <div class="tv-total-metric"><span>OPEN</span><strong id="tvTotalOpen">--</strong></div>
            <div class="tv-total-metric"><span>CURRENT</span><strong id="tvTotalCurrent">--</strong></div>
            <div class="tv-total-metric"><span>CHANGE</span><strong id="tvTotalChange">--</strong></div>
            <div class="tv-total-metric"><span>TRIGGER</span><strong>±$10B</strong></div>
            <div class="tv-total-metric"><span>LAST ALERT</span><strong id="tvTotalLast">--</strong></div>
            <div class="tv-total-metric"><span>STATUS</span><strong id="tvTotalStatus" class="stale">WAITING</strong></div>
        </div>
        <div class="tv-total-caption">Source: TradingView CRYPTOCAP:TOTAL 4H chart legend change • +10B BUY / −10B SELL. Display only; Pushover sent by Tampermonkey.</div>
    </div>

    <div class="tv-total-card" id="tvXautCard">
        <div class="tv-total-head">
            <span>TRADINGVIEW XAUT — 4H</span>
            <span id="tvXautSignal" class="none">WAITING</span>
        </div>
        <div class="tv-total-metrics">
            <div class="tv-total-metric"><span>OPEN</span><strong id="tvXautOpen">--</strong></div>
            <div class="tv-total-metric"><span>CURRENT</span><strong id="tvXautCurrent">--</strong></div>
            <div class="tv-total-metric"><span>CHANGE</span><strong id="tvXautChange">--</strong></div>
            <div class="tv-total-metric"><span>TRIGGER</span><strong>±$10M</strong></div>
            <div class="tv-total-metric"><span>LAST ALERT</span><strong id="tvXautLast">--</strong></div>
            <div class="tv-total-metric"><span>STATUS</span><strong id="tvXautStatus" class="stale">WAITING</strong></div>
        </div>
        <div class="tv-total-caption">Source: TradingView CRYPTOCAP:XAUT 4H displayed legend change • +10M BUY / −10M SELL. Display only; Pushover sent by Tampermonkey. The legend change may use previous candle close, not exact current candle open.</div>
    </div>

    <div class="tv-total-card" id="tvQqqbCard">
        <div class="tv-total-head">
            <span>TRADINGVIEW QQQB — 4H</span>
            <span id="tvQqqbSignal" class="none">WAITING</span>
        </div>
        <div class="tv-total-metrics">
            <div class="tv-total-metric"><span>OPEN</span><strong id="tvQqqbOpen">--</strong></div>
            <div class="tv-total-metric"><span>CURRENT</span><strong id="tvQqqbCurrent">--</strong></div>
            <div class="tv-total-metric"><span>CHANGE</span><strong id="tvQqqbChange">--</strong></div>
            <div class="tv-total-metric"><span>TRIGGER</span><strong>±$10K</strong></div>
            <div class="tv-total-metric"><span>LAST ALERT</span><strong id="tvQqqbLast">--</strong></div>
            <div class="tv-total-metric"><span>STATUS</span><strong id="tvQqqbStatus" class="stale">WAITING</strong></div>
        </div>
        <div class="tv-total-caption">Source: TradingView CRYPTOCAP:QQQB 4H displayed legend change • +10K BUY / −10K SELL. Display only; Pushover sent by Tampermonkey. The legend change may use previous candle close, not exact current candle open.</div>
    </div>

    <div class="card desktop-card">
        <table>
            <thead>
                <tr>
                    <th>ASSET</th>
                    <th>PRICE</th>
                    <th>LONG</th>
                    <th>SHORT</th>
                    <th>GAP</th>
                    <th>MIN GAP</th>
                    <th>DOMINANT</th>
                    <th>SIGNAL</th>
                    <th>LAST ALERT</th>
                </tr>
            </thead>
            <tbody id="rows">
                <tr><td colspan="9">Waiting for 4H Value feed...</td></tr>
            </tbody>
        </table>
    </div>

    <div class="mobile-list" id="mobileRows">
        <div class="asset-card">Waiting for 4H Value feed...</div>
    </div>

    <div class="footer">
        FINAL 4H ONLY • 7 COINS • Browser refresh every 5 seconds<br>
        Last Alert updates only after successful Pushover HTTP 2xx.
    </div>
</div>

<script>
function esc(v) {
    return String(v ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}
function money(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "--";
    const a = Math.abs(n);
    if (a >= 1e9) return "$" + (n/1e9).toFixed(2) + "B";
    if (a >= 1e6) return "$" + (n/1e6).toFixed(2) + "M";
    if (a >= 1e3) return "$" + (n/1e3).toFixed(1) + "K";
    return "$" + n.toFixed(0);
}
function price(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "--";
    let maxDecimals = 2;
    if (Math.abs(n) < 1) maxDecimals = 6;
    else if (Math.abs(n) < 100) maxDecimals = 4;
    return "$" + n.toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: maxDecimals
    });
}
function signalClass(s) {
    s = String(s || "NONE").toUpperCase();
    return s === "BUY" ? "buy" : s === "SELL" ? "sell" : "none";
}
function gapClass(dom) {
    dom = String(dom || "EQUAL").toUpperCase();
    return dom === "SHORT" ? "gap-short" : dom === "LONG" ? "gap-long" : "gap-equal";
}
function ageInfo(iso) {
    if (!iso) return {text:"NO DATA", cls:"stale"};
    const t = new Date(iso).getTime();
    if (!Number.isFinite(t)) return {text:"NO DATA", cls:"stale"};
    const m = Math.max(0, (Date.now() - t) / 60000);
    return { text: (m < 5 ? "LIVE ✅ • " : "STALE ❌ • ") + m.toFixed(1) + " min", cls: m < 5 ? "live" : "stale" };
}
function compactTime(s) {
    const m = String(s || "").match(/(\d{1,2}:\d{2})(?::\d{2})?$/);
    return m ? m[1] : (s || "--");
}
function lastAlert(engine, symbol) {
    const map = engine && typeof engine.last_alert_by_asset === "object" ? engine.last_alert_by_asset : {};
    const x = map[symbol];
    if (!x || typeof x !== "object") return "--";
    const sig = String(x.signal || "--").toUpperCase();
    return '<span class="' + signalClass(sig) + '">' + esc(sig) + '</span> • ' + esc(compactTime(x.sent_at_ist));
}
function renderTradingViewTotal(t) {
    t = t && typeof t === 'object' ? t : {};
    const status = document.getElementById('tvTotalStatus');
    const time = t.updated_at_utc ? new Date(t.updated_at_utc).getTime() : NaN;
    const elapsed = Number.isFinite(time) ? Math.max(0, (Date.now() - time)/60000) : null;
    const fresh = elapsed !== null && elapsed < 1;
    const sig = fresh ? String(t.signal || 'NONE').toUpperCase() : 'WAITING';
    const sigEl = document.getElementById('tvTotalSignal');
    sigEl.textContent = sig;
    sigEl.className = signalClass(sig);
    // Don't present old prices or a stale BUY/SELL signal as live.
    document.getElementById('tvTotalOpen').textContent = fresh ? String(t.open_display || '--') : '--';
    document.getElementById('tvTotalCurrent').textContent = fresh ? String(t.close_display || '--') : '--';
    const ch = document.getElementById('tvTotalChange');
    ch.textContent = fresh && Number.isFinite(Number(t.change_b))
        ? (Number(t.change_b) >= 0 ? '+' : '') + Number(t.change_b).toFixed(2) + 'B'
        : '--';
    ch.className = fresh ? (Number(t.change_b) >= 0 ? 'buy' : 'sell') : 'none';
    const last = t.last_alert_signal === 'BUY' || t.last_alert_signal === 'SELL'
        ? t.last_alert_signal + (t.last_alert_ist ? ' • ' + t.last_alert_ist : '')
        : '--';
    document.getElementById('tvTotalLast').textContent = last;
    status.textContent = elapsed === null ? 'WAITING FOR TV FEED'
        : (fresh ? 'LIVE ✅ • ' : 'STALE ❌ • ') + elapsed.toFixed(1) + ' min';
    status.className = fresh ? 'live' : 'stale';
}

function renderTradingViewXaut(t) {
    t = t && typeof t === 'object' ? t : {};
    const time = t.updated_at_utc ? new Date(t.updated_at_utc).getTime() : NaN;
    const elapsed = Number.isFinite(time) ? Math.max(0, (Date.now() - time)/60000) : null;
    const fresh = elapsed !== null && elapsed < 1;
    const sig = fresh ? String(t.signal || 'NONE').toUpperCase() : 'WAITING';
    const sigEl = document.getElementById('tvXautSignal');
    sigEl.textContent = sig;
    sigEl.className = signalClass(sig);
    document.getElementById('tvXautOpen').textContent = fresh ? String(t.open_display || '--') : '--';
    document.getElementById('tvXautCurrent').textContent = fresh ? String(t.close_display || '--') : '--';
    const changeEl = document.getElementById('tvXautChange');
    changeEl.textContent = fresh && Number.isFinite(Number(t.change_m))
        ? (Number(t.change_m) >= 0 ? '+' : '') + Number(t.change_m).toFixed(2) + 'M'
        : '--';
    changeEl.className = fresh ? (Number(t.change_m) >= 0 ? 'buy' : 'sell') : 'none';
    const last = t.last_alert_signal === 'BUY' || t.last_alert_signal === 'SELL'
        ? t.last_alert_signal + (t.last_alert_ist ? ' • ' + t.last_alert_ist : '')
        : '--';
    document.getElementById('tvXautLast').textContent = last;
    const status = document.getElementById('tvXautStatus');
    status.textContent = elapsed === null ? 'WAITING FOR XAUT FEED'
        : (fresh ? 'LIVE ✅ • ' : 'STALE ❌ • ') + elapsed.toFixed(1) + ' min';
    status.className = fresh ? 'live' : 'stale';
}

function renderTradingViewQqqb(t) {
    t = t && typeof t === 'object' ? t : {};
    const time = t.updated_at_utc ? new Date(t.updated_at_utc).getTime() : NaN;
    const elapsed = Number.isFinite(time) ? Math.max(0, (Date.now() - time)/60000) : null;
    const fresh = elapsed !== null && elapsed < 1;
    const sig = fresh ? String(t.signal || 'NONE').toUpperCase() : 'WAITING';
    const sigEl = document.getElementById('tvQqqbSignal');
    sigEl.textContent = sig;
    sigEl.className = signalClass(sig);
    document.getElementById('tvQqqbOpen').textContent = fresh ? String(t.open_display || '--') : '--';
    document.getElementById('tvQqqbCurrent').textContent = fresh ? String(t.close_display || '--') : '--';
    const changeEl = document.getElementById('tvQqqbChange');
    changeEl.textContent = fresh && Number.isFinite(Number(t.change_k))
        ? (Number(t.change_k) >= 0 ? '+' : '') + Number(t.change_k).toFixed(2) + 'K'
        : '--';
    changeEl.className = fresh ? (Number(t.change_k) >= 0 ? 'buy' : 'sell') : 'none';
    const last = t.last_alert_signal === 'BUY' || t.last_alert_signal === 'SELL'
        ? t.last_alert_signal + (t.last_alert_ist ? ' • ' + t.last_alert_ist : '')
        : '--';
    document.getElementById('tvQqqbLast').textContent = last;
    const status = document.getElementById('tvQqqbStatus');
    status.textContent = elapsed === null ? 'WAITING FOR QQQB FEED'
        : (fresh ? 'LIVE ✅ • ' : 'STALE ❌ • ') + elapsed.toFixed(1) + ' min';
    status.className = fresh ? 'live' : 'stale';
}

async function refresh() {
    try {
        const r = await fetch('/coinglass-liquidation-value-4h-data?ts=' + Date.now(), {cache:'no-store'});
        const d = await r.json();
        const assets = Array.isArray(d.assets) ? d.assets : [];
        const engine = d.alert_engine_4h && typeof d.alert_engine_4h === 'object' ? d.alert_engine_4h : {};
        renderTradingViewTotal(d.tradingview_total_4h);
        renderTradingViewXaut(d.tradingview_xaut_4h);
        renderTradingViewQqqb(d.tradingview_qqqb_4h);

        document.getElementById('updated').textContent = d.updated_at_ist || '--';
        const fa = ageInfo(d.updated_at_utc);
        document.getElementById('feedAge').textContent = fa.text;
        document.getElementById('feedAge').className = fa.cls;

        document.getElementById('engineUpdated').textContent = engine.heartbeat_at_ist || '--';
        const ea = ageInfo(engine.heartbeat_at_utc);
        document.getElementById('engineAge').textContent = ea.text;
        document.getElementById('engineAge').className = ea.cls;

        if (!assets.length) return;

        let html = '';
        let mobile = '';
        for (const x of assets) {
            const sig = String(x.signal || 'NONE').toUpperCase();
            const dom = String(x.stronger || 'EQUAL').toUpperCase();
            html += '<tr>'
                + '<td class="asset">' + esc(x.symbol) + '</td>'
                + '<td class="price">' + esc(price(x.price)) + '</td>'
                + '<td class="long">' + esc(money(x.long)) + '</td>'
                + '<td class="short">' + esc(money(x.short)) + '</td>'
                + '<td class="' + gapClass(dom) + '"><strong>' + esc(money(x.difference)) + '</strong></td>'
                + '<td>' + esc(money(x.threshold)) + '</td>'
                + '<td class="' + (dom === 'SHORT' ? 'buy' : dom === 'LONG' ? 'sell' : 'equal') + '">' + esc(dom) + '</td>'
                + '<td class="' + signalClass(sig) + '">' + esc(sig) + '</td>'
                + '<td>' + lastAlert(engine, String(x.symbol || '').toUpperCase()) + '</td>'
                + '</tr>';

            mobile += '<div class="asset-card">'
                + '<div class="asset-head"><span>' + esc(x.symbol) + '</span><span class="' + signalClass(sig) + '">' + esc(sig) + '</span></div>'
                + '<div class="metrics">'
                + '<div class="metric price-row"><span>PRICE</span><strong>' + esc(price(x.price)) + '</strong></div>'
                + '<div class="metric"><span>LONG</span><strong>' + esc(money(x.long)) + '</strong></div>'
                + '<div class="metric"><span>SHORT</span><strong>' + esc(money(x.short)) + '</strong></div>'
                + '<div class="metric"><span>GAP</span><strong class="' + gapClass(dom) + '">' + esc(money(x.difference)) + '</strong></div>'
                + '<div class="metric"><span>MIN GAP</span><strong>' + esc(money(x.threshold)) + '</strong></div>'
                + '<div class="metric"><span>DOM</span><strong>' + esc(dom) + '</strong></div>'
                + '<div class="metric"><span>LAST</span><strong>' + lastAlert(engine, String(x.symbol || '').toUpperCase()) + '</strong></div>'
                + '</div></div>';
        }
        document.getElementById('rows').innerHTML = html;
        document.getElementById('mobileRows').innerHTML = mobile;
    } catch (e) {
        document.getElementById('feedAge').textContent = 'Waiting for server...';
        document.getElementById('feedAge').className = 'stale';
    }
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""

    return html, 200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
    }

# ============================================================
# COINGLASS LIQUIDATION VALUE DASHBOARD — 1H
# COMPLETELY SEPARATE from existing 4H Liquidation Value and
# existing Liquidation Trades dashboards.
# FIXED 8 always included + next 2 CoinGlass-ranked non-fixed assets.
# DATA ONLY: these routes NEVER send Pushover.
#
# Thresholds — SAME AS 4H VALUE:
#   BTC / ETH / SOL -> $1,000,000 gap
#   Every other asset -> $500,000 gap
#
# Signal convention — SAME AS 4H VALUE:
#   SHORT liquidation value - LONG liquidation value >= threshold -> BUY
#   LONG liquidation value - SHORT liquidation value >= threshold -> SELL
# ============================================================

COINGLASS_VALUE_1H_DASHBOARD_STATE_FILE = os.path.join(
    "/tmp",
    "coinglass_liquidation_value_1h.json",
)
COINGLASS_VALUE_1H_DASHBOARD_LOCK = threading.Lock()


def _coinglass_value_1h_threshold(symbol):
    symbol = str(symbol or "").strip().upper()

    if symbol in COINGLASS_VALUE_TOP3:
        return 1_000_000.0

    return 500_000.0


def _coinglass_value_1h_dashboard_load():
    try:
        with COINGLASS_VALUE_1H_DASHBOARD_LOCK:
            with open(
                COINGLASS_VALUE_1H_DASHBOARD_STATE_FILE,
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
            f"[COINGLASS VALUE 1H DASHBOARD READ ERROR] {exc}",
            flush=True,
        )

    return {
        "updated_at_utc": None,
        "updated_at_ist": None,
        "assets": [],
    }


def _coinglass_value_1h_dashboard_save(data):
    os.makedirs(
        os.path.dirname(
            COINGLASS_VALUE_1H_DASHBOARD_STATE_FILE
        ),
        exist_ok=True,
    )

    tmp = (
        COINGLASS_VALUE_1H_DASHBOARD_STATE_FILE
        + f".{os.getpid()}.{threading.get_ident()}.tmp"
    )

    with COINGLASS_VALUE_1H_DASHBOARD_LOCK:
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
                COINGLASS_VALUE_1H_DASHBOARD_STATE_FILE,
            )

        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass


@app.post("/coinglass-liquidation-value-1h-webhook")
def coinglass_liquidation_value_1h_webhook():
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

        symbol = str(
            item.get("symbol", "")
        ).strip().upper()

        if not symbol or symbol in seen:
            continue

        try:
            rank = int(
                item.get("rank", 999999)
            )
        except (TypeError, ValueError):
            rank = 999999

        try:
            long_value = float(item.get("long"))
            short_value = float(item.get("short"))
        except (TypeError, ValueError):
            continue

        if long_value < 0 or short_value < 0:
            continue

        difference = abs(
            long_value - short_value
        )

        if long_value > short_value:
            stronger = "LONG"
        elif short_value > long_value:
            stronger = "SHORT"
        else:
            stronger = "EQUAL"

        threshold = _coinglass_value_1h_threshold(
            symbol
        )

        if short_value - long_value >= threshold:
            signal = "BUY"
        elif long_value - short_value >= threshold:
            signal = "SELL"
        else:
            signal = "NONE"

        assets.append({
            "rank": rank,
            "symbol": symbol,
            "long": long_value,
            "short": short_value,
            "difference": difference,
            "stronger": stronger,
            "threshold": threshold,
            "signal": signal,
        })

        seen.add(symbol)

    selected_assets, selection_error = (
        _select_coinglass_value_fixed8_plus2(
            assets
        )
    )

    if selection_error:
        print(
            "[COINGLASS VALUE 1H DASHBOARD REJECTED] "
            + json.dumps(
                selection_error,
                sort_keys=True,
            ),
            flush=True,
        )

        return jsonify({
            "ok": False,
            **selection_error,
            "required_fixed": list(
                COINGLASS_VALUE_FIXED_ASSETS
            ),
        }), 422

    assets = selected_assets

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
        _coinglass_value_1h_dashboard_save(
            state
        )

    except Exception as exc:
        print(
            f"[COINGLASS VALUE 1H DASHBOARD SAVE ERROR] {exc}",
            flush=True,
        )

        return jsonify({
            "ok": False,
            "error": "save_failed",
        }), 500

    print(
        "[COINGLASS VALUE 1H DASHBOARD UPDATE] "
        + ", ".join(
            x["symbol"]
            for x in assets
        ),
        flush=True,
    )

    return jsonify({
        "ok": True,
        "mode": "coinglass_1h_liquidation_value_dashboard",
        "count": len(assets),
        "updated_at_ist": state[
            "updated_at_ist"
        ],
        "pushover": False,
    }), 200


@app.get("/coinglass-liquidation-value-1h-data")
def coinglass_liquidation_value_1h_data():
    return jsonify({
        "ok": True,
        **_coinglass_value_1h_dashboard_load(),
    })


@app.get("/coinglass-liquidation-value-1h")
def coinglass_liquidation_value_1h():
    html = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CoinGlass 1H Liquidation Value Dashboard</title>

<style>
body {
    margin: 0;
    padding: 22px;
    background: #0d1117;
    color: #f0f6fc;
    font-family: Arial, Helvetica, sans-serif;
}
.container {
    max-width: 1180px;
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
    line-height: 1.6;
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
    font-size: 15px;
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
.equal,
.none {
    color: #d29922;
    font-weight: 700;
}
.fixed {
    color: #58a6ff;
}
.buy {
    color: #3fb950;
    font-weight: 900;
}
.sell {
    color: #f85149;
    font-weight: 900;
}
.footer {
    text-align: center;
    color: #8b949e;
    margin-top: 18px;
    line-height: 1.8;
}
@media (max-width: 700px) {
    body { padding: 10px; }
    h1 { font-size: 20px; }
    th { font-size: 10px; }
    td { font-size: 12px; padding: 11px 4px; }
}
</style>
</head>

<body>
<div class="container">

    <h1>COINGLASS LIQUIDATION VALUE — 1H</h1>

    <div class="subtitle">
        DEFAULT LIQUIDATION VALUE MODE • FIXED 8 + NEXT 2 RANKED<br>
        BTC / ETH / SOL = $1M GAP • ALL OTHERS = $500K GAP
    </div>

    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>RANK</th>
                    <th>ASSET</th>
                    <th>1H LONG VALUE</th>
                    <th>1H SHORT VALUE</th>
                    <th>GAP</th>
                    <th>THRESHOLD</th>
                    <th>STRONGER</th>
                    <th>SIGNAL</th>
                </tr>
            </thead>

            <tbody id="rows">
                <tr>
                    <td colspan="8">
                        Waiting for CoinGlass 1H Liquidation Value feed...
                    </td>
                </tr>
            </tbody>
        </table>
    </div>

    <div class="footer">
        FIXED: BTC ETH SOL XRP NEAR DOGE ZEC XAU • NEXT 2 follow CoinGlass ranking<br>
        SHORT-LONG threshold = BUY • LONG-SHORT threshold = SELL<br>
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

function money(value) {
    const n = Number(value);

    if (!Number.isFinite(n)) {
        return "--";
    }

    const abs = Math.abs(n);

    if (abs >= 1000000000) {
        return "$" + (n / 1000000000).toFixed(2) + "B";
    }

    if (abs >= 1000000) {
        return "$" + (n / 1000000).toFixed(2) + "M";
    }

    if (abs >= 1000) {
        return "$" + (n / 1000).toFixed(1) + "K";
    }

    return "$" + n.toFixed(2);
}

async function refreshDashboard() {
    const status =
        document.getElementById("status");

    try {
        const response = await fetch(
            "/coinglass-liquidation-value-1h-data?ts="
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
                '<tr><td colspan="8">'
                + 'Waiting for CoinGlass 1H Liquidation Value feed...'
                + '</td></tr>';

            status.textContent =
                "No Liquidation Value feed received yet.";

            return;
        }

        let html = "";

        data.assets.forEach(
            (item, index) => {

                const stronger =
                    String(
                        item.stronger || "EQUAL"
                    ).toUpperCase();

                const signal =
                    String(
                        item.signal || "NONE"
                    ).toUpperCase();

                const strongerCls =
                    stronger === "LONG"
                    ? "long"
                    : stronger === "SHORT"
                    ? "short"
                    : "equal";

                const signalCls =
                    signal === "BUY"
                    ? "buy"
                    : signal === "SELL"
                    ? "sell"
                    : "none";

                const fixed =
                    [
                        "BTC", "ETH", "SOL", "XRP", "NEAR",
                        "DOGE", "ZEC", "XAU"
                    ].includes(item.symbol)
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
                    + esc(money(item.long))
                    + "</td>"
                    + '<td class="short">'
                    + esc(money(item.short))
                    + "</td>"
                    + "<td><strong>"
                    + esc(money(item.difference))
                    + "</strong></td>"
                    + "<td>"
                    + esc(money(item.threshold))
                    + "</td>"
                    + '<td class="'
                    + strongerCls
                    + '">'
                    + esc(stronger)
                    + "</td>"
                    + '<td class="'
                    + signalCls
                    + '">'
                    + esc(signal)
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
# COINGLASS FEED DIAGNOSTICS — 1H + 4H
# ============================================================
# Browser/Tampermonkey feeds report their current stage here.
# Watchdog uses this to distinguish parser/feed failures from a
# missing browser heartbeat. This does not change trading alerts.

COINGLASS_DIAGNOSTIC_STATE_FILE = "/tmp/coinglass_feed_diagnostics.json"
COINGLASS_DIAGNOSTIC_LOCK = threading.Lock()


def _coinglass_diagnostic_load():
    try:
        with COINGLASS_DIAGNOSTIC_LOCK:
            with open(
                COINGLASS_DIAGNOSTIC_STATE_FILE,
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
            f"[COINGLASS DIAGNOSTIC READ ERROR] {type(exc).__name__}: {exc}",
            flush=True,
        )
    return {}


def _coinglass_diagnostic_save(data):
    tmp = (
        COINGLASS_DIAGNOSTIC_STATE_FILE
        + f".{os.getpid()}.{threading.get_ident()}.tmp"
    )

    with COINGLASS_DIAGNOSTIC_LOCK:
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, COINGLASS_DIAGNOSTIC_STATE_FILE)
        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass


@app.post("/coinglass-feed-diagnostic-webhook")
def coinglass_feed_diagnostic_webhook():
    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    timeframe = str(data.get("timeframe", "")).strip().upper()

    if timeframe not in {"1H", "4H"}:
        return jsonify({
            "ok": False,
            "error": "timeframe_must_be_1H_or_4H",
        }), 400

    stage = str(data.get("stage", "unknown")).strip()[:80]
    status = str(data.get("status", "OK")).strip().upper()[:20]
    detail = str(data.get("detail", "")).strip()[:500]
    version = str(data.get("version", "")).strip()[:40]
    client_time = str(data.get("client_time", "")).strip()[:80]

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ZoneInfo("Asia/Kolkata"))

    state = _coinglass_diagnostic_load()
    state[timeframe] = {
        "received_at_utc": now_utc.isoformat(),
        "received_at_ist": now_ist.strftime("%d-%m-%Y %H:%M:%S"),
        "stage": stage,
        "status": status,
        "detail": detail,
        "version": version,
        "client_time": client_time,
    }

    try:
        _coinglass_diagnostic_save(state)
    except Exception as exc:
        print(
            f"[COINGLASS DIAGNOSTIC SAVE ERROR] {type(exc).__name__}: {exc}",
            flush=True,
        )
        return jsonify({"ok": False, "error": "save_failed"}), 500

    print(
        f"[COINGLASS DIAGNOSTIC] {timeframe} | {status} | {stage} | {detail[:160]}",
        flush=True,
    )

    return jsonify({
        "ok": True,
        "timeframe": timeframe,
        "stage": stage,
        "status": status,
        "received_at_ist": state[timeframe]["received_at_ist"],
    }), 200


@app.get("/coinglass-feed-diagnostic-data")
def coinglass_feed_diagnostic_data():
    return jsonify({
        "ok": True,
        **_coinglass_diagnostic_load(),
    })





# ============================================================
# COINGLASS LIQUIDATION VALUE ALERT ENGINE HEARTBEAT — 1H + 4H
# Tracks whether the separate Tampermonkey VALUE ALERT scripts are alive.
# Also stores LAST ALERT only after the browser script confirms Pushover 2xx.
# This does NOT create signals and does NOT change thresholds/alternation.
# ============================================================

COINGLASS_VALUE_ALERT_ENGINE_STATE_FILE = os.path.join(
    "/tmp",
    "coinglass_value_alert_engine.json",
)
COINGLASS_VALUE_ALERT_ENGINE_LOCK = threading.Lock()


def _coinglass_value_alert_engine_default():
    return {
        "1H": {
            "heartbeat_at_utc": None,
            "heartbeat_at_ist": None,
            "version": None,
            "pushover_configured": False,
            "last_alert": None,
            "last_alert_by_asset": {},
        },
        "4H": {
            "heartbeat_at_utc": None,
            "heartbeat_at_ist": None,
            "version": None,
            "pushover_configured": False,
            "last_alert": None,
            "last_alert_by_asset": {},
        },
    }


def _coinglass_value_alert_engine_load():
    try:
        with COINGLASS_VALUE_ALERT_ENGINE_LOCK:
            with open(
                COINGLASS_VALUE_ALERT_ENGINE_STATE_FILE,
                "r",
                encoding="utf-8",
            ) as f:
                data = json.load(f)

        if not isinstance(data, dict):
            data = {}

    except FileNotFoundError:
        data = {}

    except Exception as exc:
        print(
            f"[VALUE ALERT ENGINE READ ERROR] {type(exc).__name__}: {exc}",
            flush=True,
        )
        data = {}

    default = _coinglass_value_alert_engine_default()

    for timeframe in ("1H", "4H"):
        item = data.get(timeframe)
        if not isinstance(item, dict):
            item = {}

        merged = dict(default[timeframe])
        merged.update(item)

        last_alert_by_asset = merged.get(
            "last_alert_by_asset"
        )
        if not isinstance(last_alert_by_asset, dict):
            last_alert_by_asset = {}

        # Backward-compatible migration:
        # preserve the old single last_alert as the first per-asset entry.
        legacy_last_alert = merged.get("last_alert")
        if isinstance(legacy_last_alert, dict):
            legacy_asset = str(
                legacy_last_alert.get("asset", "")
            ).strip().upper()
            if (
                legacy_asset
                and legacy_asset not in last_alert_by_asset
            ):
                last_alert_by_asset[
                    legacy_asset
                ] = legacy_last_alert

        merged[
            "last_alert_by_asset"
        ] = last_alert_by_asset
        default[timeframe] = merged

    return default


def _coinglass_value_alert_engine_save(data):
    os.makedirs(
        os.path.dirname(
            COINGLASS_VALUE_ALERT_ENGINE_STATE_FILE
        ),
        exist_ok=True,
    )

    tmp = (
        COINGLASS_VALUE_ALERT_ENGINE_STATE_FILE
        + f".{os.getpid()}.{threading.get_ident()}.tmp"
    )

    with COINGLASS_VALUE_ALERT_ENGINE_LOCK:
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
                COINGLASS_VALUE_ALERT_ENGINE_STATE_FILE,
            )

        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass


@app.post("/coinglass-liquidation-value-alert-heartbeat")
def coinglass_liquidation_value_alert_heartbeat():
    secret = request.args.get("secret", "")

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

    timeframe = str(
        data.get("timeframe", "")
    ).strip().upper()

    if timeframe not in {"1H", "4H"}:
        return jsonify({
            "ok": False,
            "error": "timeframe_must_be_1H_or_4H",
        }), 400

    event = str(
        data.get("event", "heartbeat")
    ).strip().lower()

    if event not in {"heartbeat", "alert_sent"}:
        return jsonify({
            "ok": False,
            "error": "event_must_be_heartbeat_or_alert_sent",
        }), 400

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(
        ZoneInfo("Asia/Kolkata")
    )

    state = _coinglass_value_alert_engine_load()
    item = state.get(timeframe)

    if not isinstance(item, dict):
        item = {}

    item["heartbeat_at_utc"] = now_utc.isoformat()
    item["heartbeat_at_ist"] = now_ist.strftime(
        "%d-%m-%Y %H:%M:%S"
    )
    item["version"] = str(
        data.get("version", "")
    ).strip()[:40]
    item["pushover_configured"] = bool(
        data.get("pushover_configured", False)
    )

    if event == "alert_sent":
        asset = str(
            data.get("asset", "")
        ).strip().upper()[:20]

        signal = str(
            data.get("signal", "")
        ).strip().upper()[:10]

        if (
            asset
            and signal in {"BUY", "SELL"}
        ):
            alert_record = {
                "asset": asset,
                "signal": signal,
                "sent_at_utc": now_utc.isoformat(),
                "sent_at_ist": now_ist.strftime(
                    "%d-%m-%Y %H:%M:%S"
                ),
            }

            # Keep the existing global latest alert for compatibility.
            item["last_alert"] = alert_record

            # Also remember the latest successful alert for EACH coin.
            last_alert_by_asset = item.get(
                "last_alert_by_asset"
            )
            if not isinstance(last_alert_by_asset, dict):
                last_alert_by_asset = {}

            last_alert_by_asset[
                asset
            ] = alert_record

            item[
                "last_alert_by_asset"
            ] = last_alert_by_asset

    state[timeframe] = item

    try:
        _coinglass_value_alert_engine_save(
            state
        )

    except Exception as exc:
        print(
            f"[VALUE ALERT ENGINE SAVE ERROR] {type(exc).__name__}: {exc}",
            flush=True,
        )

        return jsonify({
            "ok": False,
            "error": "save_failed",
        }), 500

    print(
        f"[VALUE ALERT ENGINE] {timeframe} | {event.upper()} | "
        f"PUSHOVER={'YES' if item['pushover_configured'] else 'NO'}",
        flush=True,
    )

    return jsonify({
        "ok": True,
        "timeframe": timeframe,
        "event": event,
        "received_at_ist": item[
            "heartbeat_at_ist"
        ],
        "last_alert": item.get(
            "last_alert"
        ),
    }), 200


@app.get("/coinglass-liquidation-value-alert-engine-data")
def coinglass_liquidation_value_alert_engine_data():
    return jsonify({
        "ok": True,
        **_coinglass_value_alert_engine_load(),
    })



# ============================================================
# COINGLASS BTC + ETH + SOL 2-OF-3 COMBINED ALERT STATE
# Separate from the existing individual 1H alert-engine heartbeat.
# The browser script posts a heartbeat every 30 seconds and posts
# alert_sent ONLY after Pushover confirms HTTP 2xx.
# ============================================================

COINGLASS_VALUE_COMBINED_ALERT_STATE_FILE = os.path.join(
    "/tmp",
    "coinglass_value_combined_2of3_alert.json",
)
COINGLASS_VALUE_COMBINED_ALERT_LOCK = threading.Lock()


def _coinglass_value_combined_alert_default():
    return {
        "heartbeat_at_utc": None,
        "heartbeat_at_ist": None,
        "version": None,
        "pushover_configured": False,
        "last_alert": None,
    }


def _coinglass_value_combined_alert_load_unlocked():
    try:
        with open(
            COINGLASS_VALUE_COMBINED_ALERT_STATE_FILE,
            "r",
            encoding="utf-8",
        ) as f:
            data = json.load(f)

        if isinstance(data, dict):
            state = _coinglass_value_combined_alert_default()
            state.update(data)
            return state

    except FileNotFoundError:
        pass

    except Exception as exc:
        print(
            f"[VALUE 2OF3 ALERT READ ERROR] {type(exc).__name__}: {exc}",
            flush=True,
        )

    return _coinglass_value_combined_alert_default()


def _coinglass_value_combined_alert_load():
    with COINGLASS_VALUE_COMBINED_ALERT_LOCK:
        return _coinglass_value_combined_alert_load_unlocked()


def _coinglass_value_combined_alert_save_unlocked(data):
    os.makedirs(
        os.path.dirname(
            COINGLASS_VALUE_COMBINED_ALERT_STATE_FILE
        ),
        exist_ok=True,
    )

    tmp = (
        COINGLASS_VALUE_COMBINED_ALERT_STATE_FILE
        + f".{os.getpid()}.{threading.get_ident()}.tmp"
    )

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
            COINGLASS_VALUE_COMBINED_ALERT_STATE_FILE,
        )

    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


@app.route(
    "/coinglass-liquidation-value-combined-alert-heartbeat",
    methods=["GET", "POST"],
)
def coinglass_liquidation_value_combined_alert_heartbeat():
    secret = request.args.get("secret", "")

    if (
        not WEBHOOK_SECRET
        or secret != WEBHOOK_SECRET
    ):
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    # GET is supported for the Tampermonkey combined heartbeat because
    # the same browser already reaches the Render 1H data endpoint by GET.
    # POST remains supported for backward compatibility.
    if request.method == "GET":
        data = {
            "event": request.args.get("event", "heartbeat"),
            "version": request.args.get("version", ""),
            "signal": request.args.get("signal", ""),
            "pushover_configured": str(
                request.args.get("pushover_configured", "")
            ).strip().lower() in {"1", "true", "yes", "on"},
        }
    else:
        data = (
            request.get_json(
                silent=True
            )
            or {}
        )

    event = str(
        data.get("event", "heartbeat")
    ).strip().lower()

    if event not in {"heartbeat", "alert_sent"}:
        return jsonify({
            "ok": False,
            "error": "event_must_be_heartbeat_or_alert_sent",
        }), 400

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(
        ZoneInfo("Asia/Kolkata")
    )

    try:
        # Atomic read -> update -> save so a heartbeat can never erase
        # a newly saved Last Combined alert.
        with COINGLASS_VALUE_COMBINED_ALERT_LOCK:
            state = _coinglass_value_combined_alert_load_unlocked()

            state["heartbeat_at_utc"] = now_utc.isoformat()
            state["heartbeat_at_ist"] = now_ist.strftime(
                "%d-%m-%Y %H:%M:%S"
            )
            state["version"] = str(
                data.get("version", "")
            ).strip()[:40]
            state["pushover_configured"] = bool(
                data.get("pushover_configured", False)
            )

            if event == "alert_sent":
                signal = str(
                    data.get("signal", "")
                ).strip().upper()[:10]

                if signal not in {"BUY", "SELL"}:
                    return jsonify({
                        "ok": False,
                        "error": "signal_must_be_BUY_or_SELL",
                    }), 400

                state["last_alert"] = {
                    "signal": signal,
                    "sent_at_utc": now_utc.isoformat(),
                    "sent_at_ist": now_ist.strftime(
                        "%d-%m-%Y %H:%M:%S"
                    ),
                }

            _coinglass_value_combined_alert_save_unlocked(
                state
            )

    except Exception as exc:
        print(
            f"[VALUE 2OF3 ALERT SAVE ERROR] {type(exc).__name__}: {exc}",
            flush=True,
        )

        return jsonify({
            "ok": False,
            "error": "save_failed",
        }), 500

    print(
        f"[VALUE 2OF3 ALERT ENGINE] {event.upper()} | "
        f"PUSHOVER={'YES' if state['pushover_configured'] else 'NO'}",
        flush=True,
    )

    return jsonify({
        "ok": True,
        "event": event,
        "received_at_ist": state[
            "heartbeat_at_ist"
        ],
        "last_alert": state.get(
            "last_alert"
        ),
    }), 200


@app.get("/coinglass-liquidation-value-combined-alert-data")
def coinglass_liquidation_value_combined_alert_data():
    return jsonify({
        "ok": True,
        **_coinglass_value_combined_alert_load(),
    })


# ============================================================
# COINGLASS 10-COIN TOTAL DOMINANCE ALERT STATE
# Separate from individual $100K alert engine and BTC/ETH/SOL 2OF3.
# NO GAP FILTER for the Total Dominance vote:
# SHORT > LONG = BUY | LONG > SHORT = SELL | EQUAL = NONE.
# Browser script sends heartbeat every 30 sec and alert_sent only
# after Pushover confirms HTTP 2xx.
# ============================================================

COINGLASS_VALUE_TOTAL_DOMINANCE_ALERT_STATE_FILE = os.path.join(
    "/tmp",
    "coinglass_value_total_dominance_alert.json",
)
COINGLASS_VALUE_TOTAL_DOMINANCE_ALERT_LOCK = threading.Lock()


def _coinglass_value_total_dominance_alert_default():
    return {
        "heartbeat_at_utc": None,
        "heartbeat_at_ist": None,
        "version": None,
        "pushover_configured": False,
        "last_alert": None,
    }


def _coinglass_value_total_dominance_alert_load_unlocked():
    try:
        with open(
            COINGLASS_VALUE_TOTAL_DOMINANCE_ALERT_STATE_FILE,
            "r",
            encoding="utf-8",
        ) as f:
            data = json.load(f)

        if isinstance(data, dict):
            state = _coinglass_value_total_dominance_alert_default()
            state.update(data)
            return state

    except FileNotFoundError:
        pass

    except Exception as exc:
        print(
            f"[VALUE TOTAL DOMINANCE READ ERROR] {type(exc).__name__}: {exc}",
            flush=True,
        )

    return _coinglass_value_total_dominance_alert_default()


def _coinglass_value_total_dominance_alert_load():
    with COINGLASS_VALUE_TOTAL_DOMINANCE_ALERT_LOCK:
        return _coinglass_value_total_dominance_alert_load_unlocked()


def _coinglass_value_total_dominance_alert_save_unlocked(data):
    os.makedirs(
        os.path.dirname(
            COINGLASS_VALUE_TOTAL_DOMINANCE_ALERT_STATE_FILE
        ),
        exist_ok=True,
    )

    tmp = (
        COINGLASS_VALUE_TOTAL_DOMINANCE_ALERT_STATE_FILE
        + f".{os.getpid()}.{threading.get_ident()}.tmp"
    )

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
            COINGLASS_VALUE_TOTAL_DOMINANCE_ALERT_STATE_FILE,
        )

    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


@app.route(
    "/coinglass-liquidation-value-total-dominance-heartbeat",
    methods=["GET", "POST"],
)
def coinglass_liquidation_value_total_dominance_heartbeat():
    secret = request.args.get("secret", "")

    if (
        not WEBHOOK_SECRET
        or secret != WEBHOOK_SECRET
    ):
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    if request.method == "GET":
        data = {
            "event": request.args.get("event", "heartbeat"),
            "version": request.args.get("version", ""),
            "signal": request.args.get("signal", ""),
            "pushover_configured": str(
                request.args.get("pushover_configured", "")
            ).strip().lower() in {"1", "true", "yes", "on"},
        }
    else:
        data = (
            request.get_json(
                silent=True
            )
            or {}
        )

    event = str(
        data.get("event", "heartbeat")
    ).strip().lower()

    if event not in {"heartbeat", "alert_sent"}:
        return jsonify({
            "ok": False,
            "error": "event_must_be_heartbeat_or_alert_sent",
        }), 400

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(
        ZoneInfo("Asia/Kolkata")
    )

    try:
        with COINGLASS_VALUE_TOTAL_DOMINANCE_ALERT_LOCK:
            state = _coinglass_value_total_dominance_alert_load_unlocked()

            state["heartbeat_at_utc"] = now_utc.isoformat()
            state["heartbeat_at_ist"] = now_ist.strftime(
                "%d-%m-%Y %H:%M:%S"
            )
            state["version"] = str(
                data.get("version", "")
            ).strip()[:40]
            state["pushover_configured"] = bool(
                data.get("pushover_configured", False)
            )

            if event == "alert_sent":
                signal = str(
                    data.get("signal", "")
                ).strip().upper()[:10]

                if signal not in {"BUY", "SELL"}:
                    return jsonify({
                        "ok": False,
                        "error": "signal_must_be_BUY_or_SELL",
                    }), 400

                state["last_alert"] = {
                    "signal": signal,
                    "sent_at_utc": now_utc.isoformat(),
                    "sent_at_ist": now_ist.strftime(
                        "%d-%m-%Y %H:%M:%S"
                    ),
                }

            _coinglass_value_total_dominance_alert_save_unlocked(
                state
            )

    except Exception as exc:
        print(
            f"[VALUE TOTAL DOMINANCE SAVE ERROR] {type(exc).__name__}: {exc}",
            flush=True,
        )

        return jsonify({
            "ok": False,
            "error": "save_failed",
        }), 500

    print(
        f"[VALUE TOTAL DOMINANCE ENGINE] {event.upper()} | "
        f"PUSHOVER={'YES' if state['pushover_configured'] else 'NO'}",
        flush=True,
    )

    return jsonify({
        "ok": True,
        "event": event,
        "received_at_ist": state[
            "heartbeat_at_ist"
        ],
        "last_alert": state.get(
            "last_alert"
        ),
    }), 200


@app.get("/coinglass-liquidation-value-total-dominance-alert-data")
def coinglass_liquidation_value_total_dominance_alert_data():
    return jsonify({
        "ok": True,
        **_coinglass_value_total_dominance_alert_load(),
    })


# ============================================================
# COINGLASS LIQUIDATION VALUE — 1H DOMINANCE DASHBOARD
# DISPLAY ONLY.
# Reads the EXISTING 1H Liquidation Value feed and 1H alert-engine heartbeat.
# Dashboard signal uses 1H liquidation dominance with a $100K minimum gap:
# SHORT > LONG and GAP >= $100K = BUY
# LONG > SHORT and GAP >= $100K = SELL
# GAP < $100K = NONE (dominant side is still shown).
# 4H backend/feed routes are left untouched; they are simply not shown here.
# ============================================================

COINGLASS_VALUE_DOMINANCE_MIN_GAP = 100_000.0


def _coinglass_value_combined_state():
    state_1h = _coinglass_value_1h_dashboard_load()
    alert_state = _coinglass_value_alert_engine_load()
    combined_alert_state = (
        _coinglass_value_combined_alert_load()
    )
    total_dominance_alert_state = (
        _coinglass_value_total_dominance_alert_load()
    )

    assets_1h = state_1h.get("assets", [])
    if not isinstance(assets_1h, list):
        assets_1h = []

    rows = []

    for index, item in enumerate(assets_1h[:10], start=1):
        if not isinstance(item, dict):
            continue

        symbol = str(
            item.get("symbol", "")
        ).strip().upper()

        if not symbol:
            continue

        try:
            long_value = float(item.get("long"))
            short_value = float(item.get("short"))
        except (TypeError, ValueError):
            long_value = None
            short_value = None

        if (
            long_value is not None
            and short_value is not None
        ):
            difference = abs(
                long_value - short_value
            )

            if short_value > long_value:
                stronger = "SHORT"
            elif long_value > short_value:
                stronger = "LONG"
            else:
                stronger = "EQUAL"

            if difference >= COINGLASS_VALUE_DOMINANCE_MIN_GAP:
                if stronger == "SHORT":
                    signal = "BUY"
                elif stronger == "LONG":
                    signal = "SELL"
                else:
                    signal = "NONE"
            else:
                signal = "NONE"
        else:
            difference = None
            stronger = "EQUAL"
            signal = "NONE"

        try:
            rank = int(item.get("rank", index))
        except (TypeError, ValueError):
            rank = index

        rows.append({
            "rank": rank,
            "symbol": symbol,
            "is_fixed": symbol in COINGLASS_VALUE_FIXED_ASSET_SET,
            "one_hour": {
                **item,
                "long": long_value,
                "short": short_value,
                "difference": difference,
                "stronger": stronger,
                "signal": signal,
            },
        })

    target_symbols = ("BTC", "ETH", "SOL")
    target_rows = {
        str(row.get("symbol", "")).upper(): row
        for row in rows
        if isinstance(row, dict)
    }

    combined_components = []
    buy_count = 0
    sell_count = 0

    for symbol in target_symbols:
        row = target_rows.get(symbol, {})
        one_hour = (
            row.get("one_hour", {})
            if isinstance(row, dict)
            else {}
        )
        if not isinstance(one_hour, dict):
            one_hour = {}

        component_signal = str(
            one_hour.get("signal", "NONE")
        ).strip().upper()

        if component_signal == "BUY":
            buy_count += 1
        elif component_signal == "SELL":
            sell_count += 1
        else:
            component_signal = "NONE"

        combined_components.append({
            "symbol": symbol,
            "signal": component_signal,
            "long": one_hour.get("long"),
            "short": one_hour.get("short"),
            "difference": one_hour.get("difference"),
        })

    if buy_count >= 2:
        combined_signal = "BUY"
    elif sell_count >= 2:
        combined_signal = "SELL"
    else:
        combined_signal = "NONE"

    # 10-coin TOTAL DOMINANCE uses ONLY dominant side, with NO gap filter.
    total_components = []
    total_buy_count = 0
    total_sell_count = 0
    total_none_count = 0

    for row in rows:
        one_hour = (
            row.get("one_hour", {})
            if isinstance(row, dict)
            else {}
        )
        if not isinstance(one_hour, dict):
            one_hour = {}

        dominant_side = str(
            one_hour.get("stronger", "EQUAL")
        ).strip().upper()

        if dominant_side == "SHORT":
            total_vote = "BUY"
            total_buy_count += 1
        elif dominant_side == "LONG":
            total_vote = "SELL"
            total_sell_count += 1
        else:
            dominant_side = "EQUAL"
            total_vote = "NONE"
            total_none_count += 1

        total_components.append({
            "symbol": str(row.get("symbol", "")).upper(),
            "dominant": dominant_side,
            "signal": total_vote,
            "long": one_hour.get("long"),
            "short": one_hour.get("short"),
            "difference": one_hour.get("difference"),
        })

    if total_buy_count > total_sell_count:
        total_dominance_signal = "BUY"
    elif total_sell_count > total_buy_count:
        total_dominance_signal = "SELL"
    else:
        total_dominance_signal = "NONE"

    return {
        "updated_at_1h_utc": state_1h.get("updated_at_utc"),
        "updated_at_1h_ist": state_1h.get("updated_at_ist"),
        "alert_engine_1h": alert_state.get("1H", {}),
        "combined_2of3": {
            "signal": combined_signal,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "components": combined_components,
            "alert_engine": combined_alert_state,
        },
        "total_dominance": {
            "signal": total_dominance_signal,
            "buy_count": total_buy_count,
            "sell_count": total_sell_count,
            "none_count": total_none_count,
            "components": total_components,
            "alert_engine": total_dominance_alert_state,
        },
        "assets": rows,
    }


@app.get("/coinglass-liquidation-value-combined-data")
def coinglass_liquidation_value_combined_data():
    return jsonify({
        "ok": True,
        **_coinglass_value_combined_state(),
    })


@app.get("/coinglass-liquidation-value-combined")
def coinglass_liquidation_value_combined():
    html = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CoinGlass 1H Liquidation Dominance Dashboard</title>

<style>
:root {
    color-scheme: dark;
}

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 18px;
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
    font-size: 28px;
}

.subtitle {
    text-align: center;
    color: #8b949e;
    margin-bottom: 16px;
    line-height: 1.55;
}

.update-strip {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
    margin-bottom: 14px;
}

.update-box {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 10px 12px;
    text-align: center;
    color: #8b949e;
    font-size: 13px;
    line-height: 1.5;
}

.update-box strong {
    color: #f0f6fc;
}

.card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 12px;
    overflow-x: auto;
}

table {
    width: 100%;
    min-width: 760px;
    border-collapse: collapse;
}

th {
    background: #21262d;
    padding: 13px 8px;
    font-size: 12px;
    white-space: nowrap;
}

th.group-1h {
    box-shadow: inset 0 -3px 0 #58a6ff;
}

td {
    padding: 13px 8px;
    text-align: center;
    border-top: 1px solid #30363d;
    font-size: 14px;
    white-space: nowrap;
}

.asset {
    font-weight: 800;
    font-size: 17px;
}

.fixed {
    color: #58a6ff;
}

.long {
    color: #3fb950;
    font-weight: 700;
}

.short {
    color: #f85149;
    font-weight: 700;
}

.buy {
    color: #3fb950;
    font-weight: 800;
}

.sell {
    color: #f85149;
    font-weight: 800;
}

.none,
.equal {
    color: #8b949e;
    font-weight: 700;
}

.gap {
    font-weight: 800;
}

.mobile-list {
    display: none;
}

.asset-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 13px;
    padding: 12px;
    margin-bottom: 10px;
}

.asset-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 10px;
}

.asset-name {
    font-size: 21px;
    font-weight: 900;
}

.mode-label {
    color: #8b949e;
    font-size: 12px;
    text-align: right;
    font-weight: 700;
}

.tf-box {
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 12px;
    background: #0d1117;
}

.tf-title {
    color: #58a6ff;
    font-weight: 900;
    text-align: center;
    margin-bottom: 9px;
    font-size: 17px;
}

.metric {
    display: flex;
    justify-content: space-between;
    gap: 8px;
    padding: 4px 0;
    font-size: 14px;
}

.metric-label {
    color: #8b949e;
}

.signal-line {
    text-align: center;
    margin-top: 8px;
    padding-top: 8px;
    border-top: 1px solid #30363d;
    font-size: 17px;
}

.footer {
    text-align: center;
    color: #8b949e;
    margin-top: 16px;
    line-height: 1.65;
    font-size: 13px;
}

.status-live {
    color: #3fb950;
    font-weight: 800;
}

.status-stale {
    color: #f85149;
    font-weight: 800;
}

.status-warn {
    color: #d29922;
    font-weight: 800;
}

.coin-last-alert {
    text-align: center;
    margin-top: 5px;
    min-height: 14px;
    color: #8b949e;
    font-size: 11px;
    font-weight: 700;
    line-height: 1.25;
}

@media (max-width: 780px) {
    body {
        padding: 10px;
    }

    h1 {
        font-size: 21px;
    }

    .subtitle {
        font-size: 13px;
    }

    .desktop-card {
        display: none;
    }

    .mobile-list {
        display: block;
    }

    .update-strip {
        grid-template-columns: 1fr 1fr;
        gap: 7px;
    }

    .update-box {
        padding: 8px 5px;
        font-size: 11px;
    }

    .combined-card {
        padding: 12px 9px;
    }

    .combined-title {
        font-size: 16px;
    }

    .combined-coins {
        gap: 5px;
    }

    .combined-coin {
        padding: 8px 3px;
        font-size: 11px;
    }

    .combined-signal {
        font-size: 21px;
    }
}
</style>
</head>

<body>
<div class="container">

    <h1>COINGLASS LIQUIDATION VALUE — 1H</h1>

    <div class="subtitle">
        FIXED 8 + NEXT 2 COINGLASS RANKED<br>
        DOMINANCE + MINIMUM $100K GAP<br>
        SHORT &gt; LONG + GAP ≥ $100K = BUY • LONG &gt; SHORT + GAP ≥ $100K = SELL
    </div>

    <div class="update-strip">
        <div class="update-box">
            <strong>1H FEED</strong><br>
            <span id="updated1h">--</span><br>
            <span id="age1h">--</span>
        </div>

        <div class="update-box">
            <strong>1H ALERT ENGINE</strong><br>
            <span id="alertUpdated1h">--</span><br>
            <span id="alertAge1h">--</span>
        </div>
    </div>

    <div class="combined-card">
        <div class="combined-title">
            BTC + ETH + SOL • 2 OF 3 COMBINED
        </div>

        <div class="combined-rule">
            EACH COIN MUST PASS MINIMUM $100K GAP
        </div>

        <div class="combined-coins" id="combinedCoins">
            <div class="combined-coin">BTC: --</div>
            <div class="combined-coin">ETH: --</div>
            <div class="combined-coin">SOL: --</div>
        </div>

        <div class="combined-main">
            <div
                class="combined-signal none"
                id="combinedSignal"
            >
                NONE
            </div>

            <div
                class="combined-counts"
                id="combinedCounts"
            >
                BUY 0/3 • SELL 0/3
            </div>

            <div
                class="combined-last"
                id="combinedLast"
            >
                Last Combined: --
            </div>

            <div
                class="combined-engine"
                id="combinedEngine"
            >
                2OF3 Engine: --
            </div>
        </div>
    </div>

    <div class="combined-card">
        <div class="combined-title">
            10-COIN TOTAL DOMINANCE
        </div>

        <div class="combined-rule">
            NO GAP FILTER • SHORT &gt; LONG = BUY • LONG &gt; SHORT = SELL
        </div>

        <div class="combined-main">
            <div
                class="combined-signal none"
                id="totalDominanceSignal"
            >
                TOTAL NONE
            </div>

            <div
                class="combined-counts"
                id="totalDominanceCounts"
            >
                BUY 0/10 • SELL 0/10 • NONE 0/10
            </div>

            <div
                class="combined-last"
                id="totalDominanceLast"
            >
                Last Total: --
            </div>

            <div
                class="combined-engine"
                id="totalDominanceEngine"
            >
                TOTAL Engine: --
            </div>
        </div>
    </div>

    <div class="card desktop-card">
        <table>
            <thead>
                <tr>
                    <th rowspan="2">#</th>
                    <th rowspan="2">ASSET</th>
                    <th colspan="5" class="group-1h">1 HOUR</th>
                </tr>
                <tr>
                    <th>LONG</th>
                    <th>SHORT</th>
                    <th>GAP</th>
                    <th>DOMINANT</th>
                    <th>SIGNAL</th>
                </tr>
            </thead>
            <tbody id="desktopRows">
                <tr>
                    <td colspan="7">Waiting for 1H Value feed...</td>
                </tr>
            </tbody>
        </table>
    </div>

    <div class="mobile-list" id="mobileRows">
        <div class="asset-card">
            Waiting for 1H Value feed...
        </div>
    </div>

    <div class="footer">
        Existing 1H feed + individual alert-engine protection remain unchanged.<br>
        BTC/ETH/SOL 2-of-3 uses the $100K per-coin rule.<br>
        10-coin Total Dominance uses dominant side only — NO GAP FILTER.<br>
        LIVE • Browser refresh every 5 seconds
    </div>

</div>

<script>
function esc(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function money(value) {
    const n = Number(value);

    if (!Number.isFinite(n)) {
        return "--";
    }

    const abs = Math.abs(n);

    if (abs >= 1000000) {
        return "$" + (n / 1000000).toFixed(2) + "M";
    }

    if (abs >= 1000) {
        return "$" + (n / 1000).toFixed(1) + "K";
    }

    return "$" + n.toFixed(0);
}

function signalClass(signal) {
    const s = String(signal || "NONE").toUpperCase();

    if (s === "BUY") {
        return "buy";
    }

    if (s === "SELL") {
        return "sell";
    }

    return "none";
}

function dominantClass(side) {
    const s = String(side || "EQUAL").toUpperCase();

    if (s === "LONG") {
        return "long";
    }

    if (s === "SHORT") {
        return "short";
    }

    return "equal";
}

function valueOrNone(item, key) {
    if (!item || item[key] === null || item[key] === undefined) {
        return "--";
    }

    return money(item[key]);
}

function signalOrNone(item) {
    if (!item) {
        return "--";
    }

    return String(item.signal || "NONE").toUpperCase();
}

function dominantOrEqual(item) {
    if (!item) {
        return "EQUAL";
    }

    return String(item.stronger || "EQUAL").toUpperCase();
}

function ageText(utcText) {
    if (!utcText) {
        return {
            text: "NO DATA",
            cls: "status-stale"
        };
    }

    const t = Date.parse(utcText);

    if (!Number.isFinite(t)) {
        return {
            text: "UNKNOWN",
            cls: "status-stale"
        };
    }

    const minutes = Math.max(
        0,
        (Date.now() - t) / 60000
    );

    if (minutes >= 5) {
        return {
            text: "STALE • " + minutes.toFixed(1) + " min",
            cls: "status-stale"
        };
    }

    return {
        text: "LIVE • " + minutes.toFixed(1) + " min",
        cls: "status-live"
    };
}

function alertEngineStatus(engine) {
    if (!engine || !engine.heartbeat_at_utc) {
        return {
            text: "NO HEARTBEAT",
            cls: "status-stale"
        };
    }

    const t = Date.parse(engine.heartbeat_at_utc);

    if (!Number.isFinite(t)) {
        return {
            text: "UNKNOWN",
            cls: "status-stale"
        };
    }

    const minutes = Math.max(
        0,
        (Date.now() - t) / 60000
    );

    if (minutes >= 5) {
        return {
            text: "STALE • " + minutes.toFixed(1) + " min",
            cls: "status-stale"
        };
    }

    if (!engine.pushover_configured) {
        return {
            text: "LIVE • PUSHOVER NOT CONFIGURED",
            cls: "status-warn"
        };
    }

    return {
        text: "LIVE ✅ • " + minutes.toFixed(1) + " min",
        cls: "status-live"
    };
}

function coinLastAlertHtml(engine, symbol) {
    const byAsset =
        engine
        && engine.last_alert_by_asset
        && typeof engine.last_alert_by_asset === "object"
        ? engine.last_alert_by_asset
        : {};

    const key = String(symbol || "").toUpperCase();
    const item = byAsset[key] || null;

    if (!item) {
        return '<div class="coin-last-alert">Last: --</div>';
    }

    const signal =
        String(item.signal || "--").toUpperCase();

    const sent =
        String(item.sent_at_ist || "");

    const match = sent.match(
        /(\d{1,2}:\d{2})(?::\d{2})?$/
    );

    const timeText =
        match
        ? match[1]
        : (sent || "--");

    return (
        '<div class="coin-last-alert">'
        + 'Last: <span class="'
        + signalClass(signal)
        + '">'
        + esc(signal)
        + '</span> • '
        + esc(timeText)
        + '</div>'
    );
}

function compactTime(sentAtIst) {
    const sent = String(sentAtIst || "");
    const match = sent.match(
        /(\d{1,2}:\d{2})(?::\d{2})?$/
    );

    return match
        ? match[1]
        : (sent || "--");
}

function renderCombined(combined) {
    const block =
        combined
        && typeof combined === "object"
        ? combined
        : {};

    const components =
        Array.isArray(block.components)
        ? block.components
        : [];

    const bySymbol = {};

    components.forEach((item) => {
        const symbol =
            String(item.symbol || "").toUpperCase();

        if (symbol) {
            bySymbol[symbol] = item;
        }
    });

    const coinHtml = ["BTC", "ETH", "SOL"]
        .map((symbol) => {
            const item = bySymbol[symbol] || {};
            const sig =
                String(item.signal || "NONE").toUpperCase();

            return (
                '<div class="combined-coin">'
                + esc(symbol)
                + ': <span class="'
                + signalClass(sig)
                + '">'
                + esc(sig)
                + '</span>'
                + '</div>'
            );
        })
        .join("");

    document.getElementById(
        "combinedCoins"
    ).innerHTML = coinHtml;

    const signal =
        String(block.signal || "NONE").toUpperCase();

    const signalEl =
        document.getElementById(
            "combinedSignal"
        );

    signalEl.textContent =
        "COMBINED " + signal;

    signalEl.className =
        "combined-signal "
        + signalClass(signal);

    document.getElementById(
        "combinedCounts"
    ).textContent =
        "BUY "
        + Number(block.buy_count || 0)
        + "/3 • SELL "
        + Number(block.sell_count || 0)
        + "/3";

    const engine =
        block.alert_engine
        && typeof block.alert_engine === "object"
        ? block.alert_engine
        : {};

    const last =
        engine.last_alert
        && typeof engine.last_alert === "object"
        ? engine.last_alert
        : null;

    if (last) {
        const lastSignal =
            String(last.signal || "--").toUpperCase();

        document.getElementById(
            "combinedLast"
        ).innerHTML =
            'Last Combined: <span class="'
            + signalClass(lastSignal)
            + '">'
            + esc(lastSignal)
            + '</span> • '
            + esc(
                compactTime(
                    last.sent_at_ist
                )
            );
    } else {
        document.getElementById(
            "combinedLast"
        ).textContent =
            "Last Combined: --";
    }

    const status =
        alertEngineStatus(
            engine
        );

    const engineEl =
        document.getElementById(
            "combinedEngine"
        );

    engineEl.textContent =
        "2OF3 Engine: "
        + status.text;

    engineEl.className =
        "combined-engine "
        + status.cls;
}

function renderTotalDominance(total) {
    const block =
        total
        && typeof total === "object"
        ? total
        : {};

    const signal =
        String(block.signal || "NONE").toUpperCase();

    const signalEl =
        document.getElementById(
            "totalDominanceSignal"
        );

    signalEl.textContent =
        "TOTAL " + signal;

    signalEl.className =
        "combined-signal "
        + signalClass(signal);

    document.getElementById(
        "totalDominanceCounts"
    ).textContent =
        "BUY "
        + Number(block.buy_count || 0)
        + "/10 • SELL "
        + Number(block.sell_count || 0)
        + "/10 • NONE "
        + Number(block.none_count || 0)
        + "/10";

    const engine =
        block.alert_engine
        && typeof block.alert_engine === "object"
        ? block.alert_engine
        : {};

    const last =
        engine.last_alert
        && typeof engine.last_alert === "object"
        ? engine.last_alert
        : null;

    if (last) {
        const lastSignal =
            String(last.signal || "--").toUpperCase();

        document.getElementById(
            "totalDominanceLast"
        ).innerHTML =
            'Last Total: <span class="'
            + signalClass(lastSignal)
            + '">'
            + esc(lastSignal)
            + '</span> • '
            + esc(
                compactTime(
                    last.sent_at_ist
                )
            );
    } else {
        document.getElementById(
            "totalDominanceLast"
        ).textContent =
            "Last Total: --";
    }

    const status =
        alertEngineStatus(
            engine
        );

    const engineEl =
        document.getElementById(
            "totalDominanceEngine"
        );

    engineEl.textContent =
        "TOTAL Engine: "
        + status.text;

    engineEl.className =
        "combined-engine "
        + status.cls;
}

function metricHtml(label, value, cls) {
    return (
        '<div class="metric">'
        + '<span class="metric-label">'
        + esc(label)
        + '</span>'
        + '<span class="'
        + esc(cls || "")
        + '">'
        + esc(value)
        + '</span>'
        + '</div>'
    );
}

async function refreshDashboard() {
    try {
        const response = await fetch(
            "/coinglass-liquidation-value-combined-data?ts="
            + Date.now(),
            {
                cache: "no-store"
            }
        );

        const data = await response.json();
        const assets = Array.isArray(data.assets)
            ? data.assets
            : [];

        const updated1h = document.getElementById("updated1h");
        const age1h = document.getElementById("age1h");
        const alertUpdated1h = document.getElementById("alertUpdated1h");
        const alertAge1h = document.getElementById("alertAge1h");

        updated1h.textContent = data.updated_at_1h_ist || "--";

        const age1 = ageText(data.updated_at_1h_utc);
        age1h.textContent = age1.text;
        age1h.className = age1.cls;

        const engine1 = data.alert_engine_1h || {};
        const engineStatus1 = alertEngineStatus(engine1);

        alertUpdated1h.textContent =
            engine1.heartbeat_at_ist || "--";

        alertAge1h.textContent = engineStatus1.text;
        alertAge1h.className = engineStatus1.cls;

        renderCombined(
            data.combined_2of3 || {}
        );

        renderTotalDominance(
            data.total_dominance || {}
        );

        if (assets.length === 0) {
            document.getElementById("desktopRows").innerHTML =
                '<tr><td colspan="7">Waiting for 1H Value feed...</td></tr>';

            document.getElementById("mobileRows").innerHTML =
                '<div class="asset-card">Waiting for 1H Value feed...</div>';

            return;
        }

        let desktop = "";
        let mobile = "";

        assets.forEach((row, index) => {
            const one = row.one_hour || null;
            const symbol = String(row.symbol || "--").toUpperCase();
            const fixedClass = row.is_fixed ? " fixed" : "";

            const oneSignal = signalOrNone(one);
            const dominant = dominantOrEqual(one);

            const oneLastAlert =
                coinLastAlertHtml(
                    engine1,
                    symbol
                );

            desktop +=
                "<tr>"
                + "<td>" + esc(index + 1) + "</td>"
                + '<td class="asset' + fixedClass + '">' + esc(symbol) + "</td>"
                + '<td class="long">' + esc(valueOrNone(one, "long")) + "</td>"
                + '<td class="short">' + esc(valueOrNone(one, "short")) + "</td>"
                + '<td class="gap">' + esc(valueOrNone(one, "difference")) + "</td>"
                + '<td class="' + dominantClass(dominant) + '">' + esc(dominant) + "</td>"
                + '<td class="' + signalClass(oneSignal) + '">'
                + esc(oneSignal)
                + oneLastAlert
                + "</td>"
                + "</tr>";

            mobile +=
                '<div class="asset-card">'
                + '<div class="asset-head">'
                + '<div class="asset-name' + fixedClass + '">#'
                + esc(index + 1)
                + " "
                + esc(symbol)
                + "</div>"
                + '<div class="mode-label">1H • MIN GAP $100K</div>'
                + "</div>"

                + '<div class="tf-box">'
                + '<div class="tf-title">1 HOUR</div>'
                + metricHtml("LONG", valueOrNone(one, "long"), "long")
                + metricHtml("SHORT", valueOrNone(one, "short"), "short")
                + metricHtml("GAP", valueOrNone(one, "difference"), "gap")
                + metricHtml("DOMINANT", dominant, dominantClass(dominant))
                + '<div class="signal-line ' + signalClass(oneSignal) + '">'
                + esc(oneSignal)
                + oneLastAlert
                + "</div>"
                + "</div>"

                + "</div>";
        });

        document.getElementById("desktopRows").innerHTML = desktop;
        document.getElementById("mobileRows").innerHTML = mobile;

    } catch (error) {
        document.getElementById("age1h").textContent = "SERVER WAIT";
        document.getElementById("age1h").className = "status-stale";
        document.getElementById("alertAge1h").textContent = "SERVER WAIT";
        document.getElementById("alertAge1h").className = "status-stale";
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
# COINGLASS LIQUIDATION TRADES — COMBINED 1H + 4H DASHBOARD
# DISPLAY ONLY.
# Reads the EXISTING 1H and 4H Liquidation Trades dashboard state.
# Does NOT change webhook logic, 50-gap alert logic, strict alternation,
# Pushover, Tampermonkey feeds, or auto-recovery.
# ============================================================


def _coinglass_trades_combined_parse_time(value):
    try:
        text = str(value or "").strip()
        if not text:
            raise ValueError("empty")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def _coinglass_trades_combined_state():
    state_1h = _coinglass_dashboard_load()
    state_4h = _coinglass_4h_dashboard_load()

    assets_1h = state_1h.get("assets", [])
    assets_4h = state_4h.get("assets", [])

    if not isinstance(assets_1h, list):
        assets_1h = []
    if not isinstance(assets_4h, list):
        assets_4h = []

    map_1h = {
        str(item.get("symbol", "")).strip().upper(): item
        for item in assets_1h
        if isinstance(item, dict)
        and str(item.get("symbol", "")).strip()
    }

    map_4h = {
        str(item.get("symbol", "")).strip().upper(): item
        for item in assets_4h
        if isinstance(item, dict)
        and str(item.get("symbol", "")).strip()
    }

    time_1h = _coinglass_trades_combined_parse_time(
        state_1h.get("updated_at_utc")
    )
    time_4h = _coinglass_trades_combined_parse_time(
        state_4h.get("updated_at_utc")
    )

    # Use the freshest feed's current FIXED8+2 ranking as display order.
    # If one feed is temporarily empty, fall back to the other one.
    if assets_1h and (
        not assets_4h
        or time_1h >= time_4h
    ):
        primary_assets = assets_1h
        secondary_assets = assets_4h
    else:
        primary_assets = assets_4h
        secondary_assets = assets_1h

    symbols = []
    seen = set()

    for source in (primary_assets, secondary_assets):
        for item in source:
            if not isinstance(item, dict):
                continue

            symbol = str(
                item.get("symbol", "")
            ).strip().upper()

            if not symbol or symbol in seen:
                continue

            symbols.append(symbol)
            seen.add(symbol)

            if len(symbols) >= 10:
                break

        if len(symbols) >= 10:
            break

    rows = []

    for symbol in symbols:
        item_1h = map_1h.get(symbol)
        item_4h = map_4h.get(symbol)

        rank_values = []
        for item in (item_1h, item_4h):
            if not isinstance(item, dict):
                continue
            try:
                rank_values.append(
                    int(item.get("rank", 999999))
                )
            except (TypeError, ValueError):
                pass

        rank = min(rank_values) if rank_values else 999999

        rows.append({
            "rank": rank,
            "symbol": symbol,
            "is_fixed": symbol in COINGLASS_FIXED_ASSET_SET,
            "one_hour": item_1h,
            "four_hour": item_4h,
        })

    return {
        "updated_at_1h_utc": state_1h.get("updated_at_utc"),
        "updated_at_1h_ist": state_1h.get("updated_at_ist"),
        "updated_at_4h_utc": state_4h.get("updated_at_utc"),
        "updated_at_4h_ist": state_4h.get("updated_at_ist"),
        "gap_threshold": 50,
        "assets": rows,
    }


@app.get("/coinglass-liquidation-trades-combined-data")
def coinglass_liquidation_trades_combined_data():
    return jsonify({
        "ok": True,
        **_coinglass_trades_combined_state(),
    })


@app.get("/coinglass-liquidation-trades")
@app.get("/coinglass-liquidation-trades-combined")
def coinglass_liquidation_trades_combined():
    html = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CoinGlass 1H + 4H Liquidation Trades Dashboard</title>

<style>
:root {
    color-scheme: dark;
}

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 18px;
    background: #0d1117;
    color: #f0f6fc;
    font-family: Arial, Helvetica, sans-serif;
}

.container {
    max-width: 1400px;
    margin: 0 auto;
}

h1 {
    text-align: center;
    margin: 0 0 7px;
    font-size: 28px;
}

.subtitle {
    text-align: center;
    color: #8b949e;
    margin-bottom: 16px;
    line-height: 1.55;
}

.update-strip {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
    margin-bottom: 14px;
}

.update-box {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 10px 12px;
    text-align: center;
    color: #8b949e;
    font-size: 13px;
    line-height: 1.5;
}

.update-box strong {
    color: #f0f6fc;
}

.card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 12px;
    overflow-x: auto;
}

table {
    width: 100%;
    min-width: 1030px;
    border-collapse: collapse;
}

th {
    background: #21262d;
    padding: 13px 8px;
    font-size: 12px;
    white-space: nowrap;
}

th.group-1h {
    box-shadow: inset 0 -3px 0 #58a6ff;
}

th.group-4h {
    box-shadow: inset 0 -3px 0 #a371f7;
}

td {
    padding: 13px 8px;
    text-align: center;
    border-top: 1px solid #30363d;
    font-size: 14px;
    white-space: nowrap;
}

.asset {
    font-weight: 800;
    font-size: 17px;
}

.fixed {
    color: #58a6ff;
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

.gap {
    font-weight: 800;
}

.sep-left {
    border-left: 2px solid #30363d;
}

.mobile-list {
    display: none;
}

.asset-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 13px;
    padding: 12px;
    margin-bottom: 10px;
}

.asset-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 10px;
}

.asset-name {
    font-size: 21px;
    font-weight: 900;
}

.rank-note {
    color: #8b949e;
    font-size: 12px;
    text-align: right;
}

.tf-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 9px;
}

.tf-box {
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 10px;
    background: #0d1117;
}

.tf-title {
    font-weight: 900;
    text-align: center;
    margin-bottom: 9px;
    font-size: 16px;
}

.tf-title.one {
    color: #58a6ff;
}

.tf-title.four {
    color: #a371f7;
}

.metric {
    display: flex;
    justify-content: space-between;
    gap: 8px;
    padding: 3px 0;
    font-size: 13px;
}

.metric-label {
    color: #8b949e;
}

.stronger-line {
    text-align: center;
    margin-top: 7px;
    padding-top: 7px;
    border-top: 1px solid #30363d;
    font-size: 15px;
}

.footer {
    text-align: center;
    color: #8b949e;
    margin-top: 16px;
    line-height: 1.65;
    font-size: 13px;
}

.status-live {
    color: #3fb950;
    font-weight: 800;
}

.status-stale {
    color: #f85149;
    font-weight: 800;
}

@media (max-width: 780px) {
    body {
        padding: 10px;
    }

    h1 {
        font-size: 21px;
    }

    .subtitle {
        font-size: 13px;
    }

    .desktop-card {
        display: none;
    }

    .mobile-list {
        display: block;
    }

    .update-strip {
        grid-template-columns: 1fr 1fr;
        gap: 7px;
    }

    .update-box {
        padding: 8px 5px;
        font-size: 11px;
    }
}
</style>
</head>

<body>
<div class="container">

    <h1>COINGLASS LIQUIDATION TRADES — 1H + 4H</h1>

    <div class="subtitle">
        FIXED 8 + NEXT 2 COINGLASS RANKED<br>
        CURRENT LONG / SHORT TRADE COUNTS • 50-TRADE ALERT LOGIC UNCHANGED<br>
        EXISTING STRICT BUY→SELL→BUY ALTERNATION REMAINS UNCHANGED
    </div>

    <div class="update-strip">
        <div class="update-box">
            <strong>1H FEED</strong><br>
            <span id="updated1h">--</span><br>
            <span id="age1h">--</span>
        </div>
        <div class="update-box">
            <strong>4H FEED</strong><br>
            <span id="updated4h">--</span><br>
            <span id="age4h">--</span>
        </div>
    </div>

    <div class="card desktop-card">
        <table>
            <thead>
                <tr>
                    <th rowspan="2">#</th>
                    <th rowspan="2">ASSET</th>
                    <th colspan="4" class="group-1h">1 HOUR</th>
                    <th colspan="4" class="group-4h sep-left">4 HOUR</th>
                </tr>
                <tr>
                    <th>LONG</th>
                    <th>SHORT</th>
                    <th>GAP</th>
                    <th>STRONGER</th>
                    <th class="sep-left">LONG</th>
                    <th>SHORT</th>
                    <th>GAP</th>
                    <th>STRONGER</th>
                </tr>
            </thead>
            <tbody id="desktopRows">
                <tr>
                    <td colspan="10">Waiting for 1H + 4H Trades feeds...</td>
                </tr>
            </tbody>
        </table>
    </div>

    <div class="mobile-list" id="mobileRows">
        <div class="asset-card">
            Waiting for 1H + 4H Trades feeds...
        </div>
    </div>

    <div class="footer">
        Fixed: BTC XAU ETH SOL XRP NEAR DOGE ZEC • Next 2 follow CoinGlass ranking.<br>
        Existing 1H / 4H alerts and auto-recovery remain unchanged.<br>
        LIVE • Browser refresh every 5 seconds
    </div>

</div>

<script>
function esc(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function countFmt(value) {
    const n = Number(value);

    if (!Number.isFinite(n)) {
        return "--";
    }

    if (Number.isInteger(n)) {
        return String(n);
    }

    return n.toFixed(2).replace(/\.?0+$/, "");
}

function strongerText(item) {
    if (!item) {
        return "--";
    }

    return String(item.stronger || "EQUAL").toUpperCase();
}

function strongerClass(item) {
    const s = strongerText(item);

    if (s === "LONG") {
        return "long";
    }

    if (s === "SHORT") {
        return "short";
    }

    return "equal";
}

function valueOrNone(item, key) {
    if (!item || item[key] === null || item[key] === undefined) {
        return "--";
    }

    return countFmt(item[key]);
}

function ageText(utcText) {
    if (!utcText) {
        return {
            text: "NO DATA",
            cls: "status-stale"
        };
    }

    const t = Date.parse(utcText);

    if (!Number.isFinite(t)) {
        return {
            text: "UNKNOWN",
            cls: "status-stale"
        };
    }

    const minutes = Math.max(
        0,
        (Date.now() - t) / 60000
    );

    if (minutes >= 5) {
        return {
            text: "STALE • " + minutes.toFixed(1) + " min",
            cls: "status-stale"
        };
    }

    return {
        text: "LIVE • " + minutes.toFixed(1) + " min",
        cls: "status-live"
    };
}

function metricHtml(label, value, cls) {
    return (
        '<div class="metric">'
        + '<span class="metric-label">'
        + esc(label)
        + '</span>'
        + '<span class="'
        + esc(cls || "")
        + '">'
        + esc(value)
        + '</span>'
        + '</div>'
    );
}

async function refreshDashboard() {
    try {
        const response = await fetch(
            "/coinglass-liquidation-trades-combined-data?ts="
            + Date.now(),
            {
                cache: "no-store"
            }
        );

        const data = await response.json();
        const assets = Array.isArray(data.assets)
            ? data.assets
            : [];

        document.getElementById("updated1h").textContent =
            data.updated_at_1h_ist || "--";
        document.getElementById("updated4h").textContent =
            data.updated_at_4h_ist || "--";

        const age1 = ageText(data.updated_at_1h_utc);
        const age4 = ageText(data.updated_at_4h_utc);

        const age1h = document.getElementById("age1h");
        const age4h = document.getElementById("age4h");

        age1h.textContent = age1.text;
        age1h.className = age1.cls;
        age4h.textContent = age4.text;
        age4h.className = age4.cls;

        if (assets.length === 0) {
            document.getElementById("desktopRows").innerHTML =
                '<tr><td colspan="10">Waiting for 1H + 4H Trades feeds...</td></tr>';

            document.getElementById("mobileRows").innerHTML =
                '<div class="asset-card">Waiting for 1H + 4H Trades feeds...</div>';

            return;
        }

        let desktop = "";
        let mobile = "";

        assets.forEach((row, index) => {
            const one = row.one_hour || null;
            const four = row.four_hour || null;
            const symbol = String(row.symbol || "--").toUpperCase();
            const fixedClass = row.is_fixed ? " fixed" : "";

            desktop +=
                "<tr>"
                + "<td>" + esc(index + 1) + "</td>"
                + '<td class="asset' + fixedClass + '">' + esc(symbol) + "</td>"
                + '<td class="long">' + esc(valueOrNone(one, "long")) + "</td>"
                + '<td class="short">' + esc(valueOrNone(one, "short")) + "</td>"
                + '<td class="gap">' + esc(valueOrNone(one, "difference")) + "</td>"
                + '<td class="' + strongerClass(one) + '">' + esc(strongerText(one)) + "</td>"
                + '<td class="long sep-left">' + esc(valueOrNone(four, "long")) + "</td>"
                + '<td class="short">' + esc(valueOrNone(four, "short")) + "</td>"
                + '<td class="gap">' + esc(valueOrNone(four, "difference")) + "</td>"
                + '<td class="' + strongerClass(four) + '">' + esc(strongerText(four)) + "</td>"
                + "</tr>";

            mobile +=
                '<div class="asset-card">'
                + '<div class="asset-head">'
                + '<div class="asset-name' + fixedClass + '">' + esc(symbol) + '</div>'
                + '<div class="rank-note">#' + esc(index + 1) + '<br>50 GAP</div>'
                + '</div>'
                + '<div class="tf-grid">'
                + '<div class="tf-box">'
                + '<div class="tf-title one">1H</div>'
                + metricHtml("LONG", valueOrNone(one, "long"), "long")
                + metricHtml("SHORT", valueOrNone(one, "short"), "short")
                + metricHtml("GAP", valueOrNone(one, "difference"), "gap")
                + '<div class="stronger-line ' + strongerClass(one) + '">'
                + esc(strongerText(one))
                + '</div>'
                + '</div>'
                + '<div class="tf-box">'
                + '<div class="tf-title four">4H</div>'
                + metricHtml("LONG", valueOrNone(four, "long"), "long")
                + metricHtml("SHORT", valueOrNone(four, "short"), "short")
                + metricHtml("GAP", valueOrNone(four, "difference"), "gap")
                + '<div class="stronger-line ' + strongerClass(four) + '">'
                + esc(strongerText(four))
                + '</div>'
                + '</div>'
                + '</div>'
                + '</div>';
        });

        document.getElementById("desktopRows").innerHTML = desktop;
        document.getElementById("mobileRows").innerHTML = mobile;

    } catch (error) {
        document.getElementById("desktopRows").innerHTML =
            '<tr><td colspan="10">Waiting for server...</td></tr>';

        document.getElementById("mobileRows").innerHTML =
            '<div class="asset-card">Waiting for server...</div>';
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
# COINGLASS FEED WATCHDOG — 1H + 4H
# ============================================================
# Independent of Chrome/Tampermonkey execution.
# Checks the timestamps already stored by the 1H and 4H dashboard feeds.
# If a feed is older than 5 minutes, one Pushover STALE alert is sent.
# No repeat STALE spam while it remains stale.
# When the feed becomes fresh again, one RECOVERED alert is sent.

COINGLASS_WATCHDOG_CHECK_SECONDS = 60
COINGLASS_WATCHDOG_STALE_SECONDS = 300
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


def _coinglass_watchdog_diagnostic_reason(label):
    diagnostics = _coinglass_diagnostic_load()
    info = diagnostics.get(label, {}) if isinstance(diagnostics, dict) else {}

    if not isinstance(info, dict) or not info:
        return (
            "No browser heartbeat received. "
            "Possible causes: feed script off/not loaded, CoinGlass tab closed, "
            "Chrome/RDP session stopped, VPS/network issue."
        )

    received_utc = _coinglass_watchdog_parse_utc(
        info.get("received_at_utc")
    )

    if received_utc is None:
        return "Diagnostic heartbeat exists but its timestamp is invalid."

    heartbeat_age = max(
        0.0,
        (datetime.now(timezone.utc) - received_utc).total_seconds(),
    )

    if heartbeat_age > COINGLASS_WATCHDOG_STALE_SECONDS:
        return (
            "No recent browser heartbeat "
            f"({_coinglass_watchdog_age_text(heartbeat_age)} old). "
            "Possible causes: feed script off/not loaded, CoinGlass tab closed, "
            "Chrome/RDP session stopped, VPS/network issue."
        )

    stage = str(info.get("stage", "unknown") or "unknown")
    status = str(info.get("status", "UNKNOWN") or "UNKNOWN").upper()
    detail = str(info.get("detail", "") or "").strip()
    version = str(info.get("version", "") or "").strip()

    prefix = "Browser/script heartbeat is alive. "
    if version:
        prefix += f"Feed version {version}. "

    if status == "ERROR":
        if detail:
            return prefix + f"Failure stage: {stage}. Detail: {detail}"
        return prefix + f"Failure stage: {stage}."

    if stage == "cycle_start":
        return prefix + "Feed cycle started but did not reach data collection/post."

    if stage in {"trades_verified", "collect_ok"}:
        return prefix + f"Last successful stage: {stage}; data POST did not complete."

    if stage == "data_post_ok":
        return (
            prefix
            + "Feed reports a successful Render POST, but dashboard timestamp is stale; "
            "check backend save/state handling."
        )

    return prefix + f"Last reported stage: {stage} ({status})."


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
        "[COINGLASS WATCHDOG] Started — checking 1H + 4H every 60s; stale > 300s",
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
                    if not reason:
                        reason = _coinglass_watchdog_diagnostic_reason(label)

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
