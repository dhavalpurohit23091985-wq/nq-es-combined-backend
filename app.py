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

def send_pushover(title, message):
    title_upper = str(title or "").upper()
    message_upper = str(message or "").upper()

    latest_qqq_last4 = (
        "NASDAQ" in title_upper
        and ("QQQ WEIGHTED" in title_upper or "QQQ WEIGHTED" in message_upper
             or "ROLLING LAST-4" in title_upper or "ROLLING LAST-4" in message_upper
             or "LAST-4" in title_upper or "LAST-4" in message_upper)
    )
    nifty_last4 = (
        "NIFTY 10-STOCK" in title_upper
        and ("NIFTY WEIGHTED" in title_upper or "NIFTY WEIGHTED" in message_upper)
        and ("LAST-4" in title_upper or "LAST-4" in message_upper)
    )
    banknifty_last4 = (
        "BANKNIFTY TOP-5" in title_upper
        and ("WEIGHTED" in title_upper or "WEIGHTED" in message_upper)
        and ("LAST-4" in title_upper or "LAST-4" in message_upper)
    )

    if not (latest_qqq_last4 or nifty_last4 or banknifty_last4):
        print(f"[PUSHOVER SILENT - NQ + NIFTY + BANKNIFTY ONLY] {title}", flush=True)
        return False
    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        return False
    try:
        r = requests.post(PUSHOVER_URL, data={
            "token": PUSHOVER_TOKEN, "user": PUSHOVER_USER,
            "title": title, "message": message,
            "priority": 2, "retry": 30, "expire": 3600,
        }, timeout=10)
        return r.ok
    except requests.RequestException as exc:
        print(f"[PUSHOVER ERROR] {exc}", flush=True)
        return False


def _india_persistent_trigger_number(title, message):
    title_text = str(title or "")
    message_text = str(message or "")
    title_u = title_text.upper()
    message_u = message_text.upper()

    is_nifty = (
        "NIFTY 10-STOCK" in title_u
        and "NIFTY WEIGHTED" in title_u
        and ("LAST-4" in title_u or "LAST-4" in message_u)
    )
    is_banknifty = (
        "BANKNIFTY TOP-5" in title_u
        and "WEIGHTED" in title_u
        and ("LAST-4" in title_u or "LAST-4" in message_u)
    )

    if is_banknifty:
        asset = "BANKNIFTY"
        serial_file = BANKNIFTY_TRIGGER_SERIAL_FILE
    elif is_nifty:
        asset = "NIFTY"
        serial_file = NIFTY_TRIGGER_SERIAL_FILE
    else:
        return message_text, None, False

    match = re.search(r"(?i)\bTRIGGER\s*#\s*(\d+)", message_text)
    incoming_serial = int(match.group(1)) if match else None

    # Ignore Pine's local serial when identifying an exact resend/retry.
    fingerprint_message = re.sub(
        r"(?i)\bTRIGGER\s*#\s*\d+",
        "TRIGGER #",
        message_text,
        count=1,
    )
    fingerprint = title_text + "\n" + fingerprint_message

    # Daily Indian-market backend cycle boundary: 09:15 IST.
    now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
    boundary = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    if now_ist < boundary:
        boundary -= timedelta(days=1)
    reset_cycle = boundary.strftime("%Y-%m-%dT%H:%M%z")

    os.makedirs(os.path.dirname(serial_file), exist_ok=True)
    lock_path = serial_file + ".lock"

    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            saved = {}
            try:
                with open(serial_file, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, dict):
                        saved = loaded
            except FileNotFoundError:
                pass
            except Exception as exc:
                print(f"[{asset} TRIGGER DISK READ ERROR] {exc}", flush=True)

            saved_cycle = str(saved.get("reset_cycle") or "")
            daily_reset = bool(saved_cycle and saved_cycle != reset_cycle)

            try:
                previous_serial = max(0, int(saved.get("serial", 0) or 0))
            except (TypeError, ValueError):
                previous_serial = 0
            previous_fingerprint = str(saved.get("last_fingerprint") or "")

            if daily_reset:
                previous_serial = 0
                previous_fingerprint = ""
                print(f"[{asset} TRIGGER DAILY RESET] cycle={reset_cycle}", flush=True)

            duplicate = bool(
                previous_fingerprint and previous_fingerprint == fingerprint
            )
            if duplicate:
                serial = previous_serial
            elif previous_serial <= 0:
                # On a brand-new disk file, preserve today's Pine serial if present.
                # After a detected 09:15 cycle change, start the new day at #1.
                if daily_reset:
                    serial = 1
                else:
                    serial = incoming_serial if incoming_serial and incoming_serial > 0 else 1
            else:
                serial = previous_serial + 1

            if not duplicate:
                payload = {
                    "asset": asset,
                    "serial": serial,
                    "last_fingerprint": fingerprint,
                    "reset_cycle": reset_cycle,
                    "saved_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                tmp = serial_file + f".{os.getpid()}.{threading.get_ident()}.tmp"
                try:
                    with open(tmp, "w", encoding="utf-8") as f:
                        json.dump(payload, f, separators=(",", ":"), sort_keys=True)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp, serial_file)
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
                rewritten = f"TRIGGER #{serial} | {message_text}"

            print(
                f"[{asset} PERSISTENT TRIGGER] #{serial} "
                f"| pine={incoming_serial if incoming_serial is not None else 'NA'} "
                f"| duplicate={duplicate} | cycle={reset_cycle}",
                flush=True,
            )
            return rewritten, serial, duplicate
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

def _nq_persistent_trigger_number(title, message):
    title_text = str(title or "")
    message_text = str(message or "")
    title_u = title_text.upper()
    message_u = message_text.upper()

    is_qqq_last4 = (
        "NASDAQ 10-STOCK" in title_u
        and "QQQ WEIGHTED" in title_u
        and ("LAST-4" in title_u or "LAST-4" in message_u)
    )
    if not is_qqq_last4:
        return message_text, None, False

    # Pine serial is used only to seed the disk counter on the very first alert.
    # After that, the backend disk serial is authoritative.
    match = re.search(r"(?i)\bTRIGGER\s*#\s*(\d+)", message_text)
    incoming_serial = int(match.group(1)) if match else None

    # Fingerprint excludes Pine's local serial so a resend/retry of the same
    # TradingView event cannot consume another backend trigger number.
    fingerprint_message = re.sub(
        r"(?i)\bTRIGGER\s*#\s*\d+",
        "TRIGGER #",
        message_text,
        count=1,
    )
    fingerprint = title_text + "\n" + fingerprint_message

    os.makedirs(os.path.dirname(NQ_TRIGGER_SERIAL_FILE), exist_ok=True)
    lock_path = NQ_TRIGGER_SERIAL_FILE + ".lock"

    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            saved = {}
            try:
                with open(NQ_TRIGGER_SERIAL_FILE, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, dict):
                        saved = loaded
            except FileNotFoundError:
                pass
            except Exception as exc:
                print(f"[NQ TRIGGER DISK READ ERROR] {exc}", flush=True)

            # Weekly NQ trigger cycle (IST):
            #   Saturday 02:30 -> reset
            #   Monday 05:30 -> first valid alert starts again at TRIGGER #1.
            # The LAST-4 Pine calculation itself is untouched; this is only the
            # persistent backend display/sequence number.
            now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
            days_since_saturday = (now_ist.weekday() - 5) % 7
            reset_date = (now_ist - timedelta(days=days_since_saturday)).date()
            reset_ist = datetime.combine(
                reset_date,
                datetime.min.time(),
                tzinfo=ZoneInfo("Asia/Kolkata"),
            ).replace(hour=2, minute=30)
            if now_ist < reset_ist:
                reset_ist -= timedelta(days=7)
            reset_cycle = reset_ist.strftime("%Y-%m-%dT%H:%M%z")

            saved_cycle = str(saved.get("reset_cycle") or "")
            weekly_reset = bool(saved_cycle and saved_cycle != reset_cycle)

            try:
                previous_serial = max(0, int(saved.get("serial", 0) or 0))
            except (TypeError, ValueError):
                previous_serial = 0
            previous_fingerprint = str(saved.get("last_fingerprint") or "")

            if weekly_reset:
                previous_serial = 0
                previous_fingerprint = ""
                print(
                    f"[NQ TRIGGER WEEKLY RESET] cycle={reset_cycle}",
                    flush=True,
                )

            duplicate = bool(previous_fingerprint and previous_fingerprint == fingerprint)
            if duplicate:
                serial = previous_serial
            elif previous_serial <= 0:
                serial = incoming_serial if incoming_serial and incoming_serial > 0 else 1
            else:
                serial = previous_serial + 1

            if not duplicate:
                payload = {
                    "serial": serial,
                    "last_fingerprint": fingerprint,
                    "reset_cycle": reset_cycle,
                    "saved_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                tmp = (
                    NQ_TRIGGER_SERIAL_FILE
                    + f".{os.getpid()}.{threading.get_ident()}.tmp"
                )
                try:
                    with open(tmp, "w", encoding="utf-8") as f:
                        json.dump(payload, f, separators=(",", ":"), sort_keys=True)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp, NQ_TRIGGER_SERIAL_FILE)
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
                rewritten = f"TRIGGER #{serial} | {message_text}"

            print(
                f"[NQ PERSISTENT TRIGGER] #{serial} "
                f"| pine={incoming_serial if incoming_serial is not None else 'NA'} "
                f"| duplicate={duplicate}",
                flush=True,
            )
            return rewritten, serial, duplicate
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

def _qqq_weighted_audit_pushover_parts(title, message):
    title_u = str(title or "").upper()
    text = str(message or "")

    # Every other TradingView alert remains exactly one message.
    if "NASDAQ 10-STOCK" not in title_u or "QQQ WEIGHTED" not in title_u:
        return [(str(title or ""), text)]

    stock_names = {
        "NVDA", "AAPL", "MSFT", "MU", "AMZN",
        "AMD", "GOOGL", "META", "GOOG", "TSLA"
    }

    normalized = text.replace("\r", "").replace("\n", " | ")
    tokens = [part.strip() for part in normalized.split("|") if part.strip()]

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
        if token.upper().startswith(wanted_prefixes):
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
        while j < len(tokens) and tokens[j].upper() not in stock_names:
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

        if "O" in vals and "T" in vals:
            row = f"{symbol} | O {vals['O']} | T {vals['T']}"
            if "R" in vals:
                row += f" | R {vals['R']}"
            if "W" in vals:
                row += f" | W {vals['W']}"
            if "C" in vals:
                row += f" | C {vals['C']}"
            stock_rows.append(row)

        i = max(j, i + 1)

    summary = []
    for token in tokens:
        u = token.upper()
        if (
            u.startswith("WEIGHTED NET =")
            or u.startswith("NQ AT TRIGGER:")
        ):
            if token not in summary:
                summary.append(token)

    # Preserve source order exactly; only split after row 5.
    first_rows = stock_rows[:5]
    second_rows = stock_rows[5:]

    part1_lines = []
    part1_lines.extend(metadata)
    part1_lines.append("AUDIT 1/2: O=OPEN | T=LIVE | R=RAW | W=WEIGHT | C=CONTR")
    part1_lines.extend(first_rows)

    part2_lines = [
        "AUDIT 2/2: O=OPEN | T=LIVE | R=RAW | W=WEIGHT | C=CONTR"
    ]
    part2_lines.extend(second_rows)
    part2_lines.extend(summary)

    part1 = "\n".join(part1_lines).strip()
    part2 = "\n".join(part2_lines).strip()

    # Defensive protection only. With 5 rows per part both messages should
    # normally be comfortably below Pushover's 1024-character message limit.
    if len(part1) > 1024:
        part1 = part1[:1024]
    if len(part2) > 1024:
        part2 = part2[:1024]

    return [
        (f"{title} | PART 1/2", part1 or text),
        (f"{title} | PART 2/2", part2 or text),
    ]

def _nq_long_pushover_parts(title, message):
    title_text = str(title or "")
    message_text = str(message or "")
    title_u = title_text.upper()

    # Only NQ/NASDAQ-family TradingView alerts are eligible.
    if "NQ" not in title_u and "NASDAQ" not in title_u:
        return [(title_text, message_text)]

    # Short messages remain exactly one notification.
    if len(message_text) <= 950:
        return [(title_text, message_text)]

    normalized = message_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")

    # Choose a line-boundary split closest to the middle while keeping
    # both parts comfortably below Pushover's 1024-character message limit.
    best = None
    for cut in range(1, len(lines)):
        p1 = "\n".join(lines[:cut]).strip()
        p2 = "\n".join(lines[cut:]).strip()
        if len(p1) <= 950 and len(p2) <= 950:
            score = abs(len(p1) - len(p2))
            if best is None or score < best[0]:
                best = (score, p1, p2)

    if best is not None:
        _, part1, part2 = best
    else:
        # Defensive fallback for a message containing very long single lines.
        midpoint = len(normalized) // 2
        left_break = normalized.rfind("\n", 0, midpoint + 1)
        right_break = normalized.find("\n", midpoint)

        if left_break > 0:
            cut = left_break
        elif right_break != -1:
            cut = right_break
        else:
            cut = midpoint

        part1 = normalized[:cut].strip()
        part2 = normalized[cut:].strip()

        # Final defensive cap only; normal structured TV messages should
        # always split at line boundaries above without reaching this path.
        part1 = part1[:1024]
        part2 = part2[:1024]

    return [
        (f"{title_text} | PART 1/2", part1),
        (f"{title_text} | PART 2/2", part2),
    ]

def _india_weighted_pushover_parts(title, message):
    title_text = str(title or "")
    message_text = str(message or "")
    title_u = title_text.upper()

    is_nifty = "NIFTY 10-STOCK" in title_u and "NIFTY WEIGHTED" in title_u
    is_banknifty = "BANKNIFTY TOP-5" in title_u and "WEIGHTED" in title_u

    if not (is_nifty or is_banknifty) or len(message_text) <= 950:
        return [(title_text, message_text)]

    # TradingView audit is pipe-delimited. Turn each field into a line so the
    # split happens only at a clean audit-field boundary.
    normalized = message_text.replace("\r", "").replace("\n", " | ")
    fields = [field.strip() for field in normalized.split("|") if field.strip()]

    best = None
    for cut in range(1, len(fields)):
        p1 = " | ".join(fields[:cut]).strip()
        p2 = " | ".join(fields[cut:]).strip()
        if len(p1) <= 950 and len(p2) <= 950:
            score = abs(len(p1) - len(p2))
            if best is None or score < best[0]:
                best = (score, p1, p2)

    if best is None:
        # Defensive fallback: keep two parts and prefer a pipe boundary nearest
        # the middle. This should not be needed for the current NIFTY/BANKNIFTY
        # payload sizes.
        midpoint = len(normalized) // 2
        left = normalized.rfind(" | ", 0, midpoint + 1)
        right = normalized.find(" | ", midpoint)
        if left > 0:
            cut = left
        elif right != -1:
            cut = right
        else:
            cut = midpoint
        part1 = normalized[:cut].strip()
        part2 = normalized[cut:].strip()
    else:
        _, part1, part2 = best

    return [
        (f"{title_text} | PART 1/2", part1),
        (f"{title_text} | PART 2/2", part2),
    ]

@app.get("/")
def home():
    return jsonify({
        "ok": True,
        "service": "NQ + NIFTY + BANKNIFTY backend",
        "nq": "NASDAQ 10-STOCK QQQ WEIGHTED LAST-4",
        "nifty": "NIFTY 10-STOCK WEIGHTED LAST-4",
        "banknifty": "BANKNIFTY TOP-5 WEIGHTED LAST-4",
        "crypto_liquidation_code": "removed",
    })

@app.post("/webhook")
def webhook():
    secret = request.args.get("secret", "")
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    if "title" not in data or "message" not in data:
        return jsonify({"ok": False, "error": "title_and_message_required"}), 400

    tv_title = str(data.get("title", "TradingView Alert"))
    tv_message = str(data.get("message", ""))

    tv_message, nq_serial, nq_duplicate = _nq_persistent_trigger_number(tv_title, tv_message)
    tv_message, india_serial, india_duplicate = _india_persistent_trigger_number(tv_title, tv_message)

    parts = _qqq_weighted_audit_pushover_parts(tv_title, tv_message)
    if len(parts) == 1:
        parts = _nq_long_pushover_parts(tv_title, tv_message)
    if len(parts) == 1:
        parts = _india_weighted_pushover_parts(tv_title, tv_message)

    def _send(parts_to_send):
        try:
            for part_title, part_message in parts_to_send:
                send_pushover(part_title, part_message)
        except Exception as exc:
            print(f"[PUSHOVER BACKGROUND ERROR] {exc}", flush=True)

    threading.Thread(target=_send, args=(list(parts),), daemon=True).start()
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

