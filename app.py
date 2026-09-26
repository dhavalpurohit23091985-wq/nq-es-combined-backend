# FINAL CLEAN BACKEND: NQ + NIFTY + BANKNIFTY ONLY
# BTC/XAU/ETH/SOL/13EX/MarginPad/Coinalyze/direct-liquidation/MT5 liquidation code removed.

import os
import re
import json
import fcntl
import threading
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
        )
    )

    nifty_last4 = (
        "NIFTY 10-STOCK" in title_upper
        and (
            "NIFTY WEIGHTED" in title_upper
            or "NIFTY WEIGHTED" in message_upper
        )
        and (
            "LAST-4" in title_upper
            or "LAST-4" in message_upper
        )
    )

    banknifty_last4 = (
        "BANKNIFTY TOP-5" in title_upper
        and (
            "WEIGHTED" in title_upper
            or "WEIGHTED" in message_upper
        )
        and (
            "LAST-4" in title_upper
            or "LAST-4" in message_upper
        )
    )

    if not (latest_qqq_last4 or nifty_last4 or banknifty_last4):
        print(
            f"[PUSHOVER SILENT - NQ + NIFTY + BANKNIFTY ONLY] {title}",
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
        "carry": None,
        "added": None,
        "state": "NONE",
        "threshold": 0.100,
        "total_weight": 47.00,
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


@app.post("/fixed1h-dashboard-webhook")
def fixed1h_dashboard_webhook():
    secret = request.args.get("secret", "")

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "error": "unauthorized",
        }), 401

    data = request.get_json(silent=True) or {}

    if (
        str(data.get("type", "")).strip()
        != "NASDAQ_FIXED_1H_DASHBOARD"
    ):
        return jsonify({
            "ok": False,
            "error": "invalid_dashboard_payload",
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
        "carry": _fixed1h_safe_float(
            data.get("carry")
        ),
        "added": _fixed1h_safe_float(
            data.get("added")
        ),
        "state": str(
            data.get("state", "NONE")
        ).upper(),
        "threshold": _fixed1h_safe_float(
            data.get("threshold")
        ),
        "total_weight": _fixed1h_safe_float(
            data.get("total_weight")
        ),
        "last_trigger": str(
            data.get("last_trigger", "NONE")
        ).upper(),
        "last_trigger_type": str(
            data.get("last_trigger_type", "NONE")
        ).upper(),
        "last_trigger_value": _fixed1h_safe_float(
            data.get("last_trigger_value")
        ),
        "last_trigger_time": str(
            data.get("last_trigger_time", "NONE")
        ),
        "stocks": stocks,
    }

    if state["threshold"] is None:
        state["threshold"] = 0.100

    if state["total_weight"] is None:
        state["total_weight"] = 47.00

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
        f"| CARRY={state['carry']} "
        f"| ADDED={state['added']} "
        f"| STATE={state['state']} "
        f"| STOCKS={len(stocks)}",
        flush=True,
    )

    return jsonify({
        "ok": True,
        "mode": "nq_fixed1h_dashboard",
        "updated_at_ist": state["updated_at_ist"],
        "base_time": state["base_time"],
        "direct": state["direct"],
        "carry": state["carry"],
        "added": state["added"],
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
        NASDAQ 10-STOCK | FIXED 1H OPEN + CONTRIBUTION ADD | ±0.100%
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
            <div class="metric-label">CARRY</div>
            <div class="metric-value" id="carry">--</div>
        </div>

        <div class="card metric">
            <div class="metric-label">ADDED</div>
            <div class="metric-value" id="added">--</div>
        </div>

        <div class="card metric">
            <div class="metric-label">STATE</div>
            <div class="metric-value" id="state">NONE</div>
        </div>

        <div class="card metric">
            <div class="metric-label">THRESHOLD</div>
            <div class="metric-value" id="threshold">±0.100%</div>
        </div>

        <div class="card metric">
            <div class="metric-label">TOP-10 WEIGHT</div>
            <div class="metric-value" id="weight">47.00%</div>
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

        const carryEl = document.getElementById("carry");
        carryEl.textContent = percentText(data.carry, 3);
        carryEl.className =
            "metric-value " + sideClass(data.carry);

        const addedEl = document.getElementById("added");
        addedEl.textContent = percentText(data.added, 3);
        addedEl.className =
            "metric-value " + sideClass(data.added);

        const stateEl = document.getElementById("state");
        stateEl.textContent = data.state || "NONE";
        stateEl.className =
            "metric-value " + stateClass(data.state);

        document.getElementById(
            "threshold"
        ).textContent =
            "±" + numberText(data.threshold, 3) + "%";

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
