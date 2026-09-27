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
