#!/usr/bin/env python3
"""Outbound voice call: dial a number, play TTS message, hang up.

Uses a cloudflared/ngrok tunnel to expose the local callback server via HTTPS
(required by Azure Communication Services).
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

from flask import Flask, request, Response
from azure.communication.callautomation import (
    CallAutomationClient,
    PhoneNumberIdentifier,
    TextSource,
)
from azure.core.messaging import CloudEvent

# Load .env from the same directory as this script
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

app = Flask(__name__)

CONN_STR = os.environ["ACS_CONNECTION_STRING"]
ACS_PHONE = os.environ["ACS_PHONE_NUMBER"]
COGNITIVE_ENDPOINT = os.environ["COGNITIVE_SERVICES_ENDPOINT"]
CALLBACK_HOST = os.environ.get("CALLBACK_HOST", "")
CALLBACK_PORT = int(os.environ.get("CALLBACK_PORT", "9090"))

client = CallAutomationClient.from_connection_string(CONN_STR)
call_completed = threading.Event()
call_result = {"success": False, "message": ""}


def start_tunnel(port: int) -> tuple[str, subprocess.Popen | None]:
    """Start a tunnel and return (https_url, process).

    If CALLBACK_HOST already starts with https://, uses it directly (no tunnel).
    Otherwise tries cloudflared, then ngrok.
    """
    if CALLBACK_HOST.startswith("https://"):
        return CALLBACK_HOST.rstrip("/"), None

    # Try cloudflared
    if shutil.which("cloudflared"):
        proc = subprocess.Popen(
            ["cloudflared", "tunnel", "--url", f"http://localhost:{port}", "--no-autoupdate"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.time() + 30
        while time.time() < deadline:
            line = proc.stdout.readline()
            if not line:
                break
            m = re.search(r"(https://[a-z0-9-]+\.trycloudflare\.com)", line)
            if m:
                url = m.group(1)
                print(f"Tunnel ready: {url}")
                return url, proc
            time.sleep(0.1)
        proc.kill()
        raise RuntimeError("cloudflared failed to start within 30s")

    # Try ngrok
    if shutil.which("ngrok"):
        proc = subprocess.Popen(
            ["ngrok", "http", str(port), "--log", "stdout", "--log-format", "json"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.time() + 30
        while time.time() < deadline:
            line = proc.stdout.readline()
            if not line:
                break
            try:
                data = json.loads(line)
                url = data.get("url", "")
                if url.startswith("https://"):
                    print(f"Tunnel ready: {url}")
                    return url, proc
            except json.JSONDecodeError:
                pass
            time.sleep(0.1)
        proc.kill()
        raise RuntimeError("ngrok failed to start within 30s")

    raise RuntimeError(
        "No HTTPS callback available. Set CALLBACK_HOST=https://... "
        "or install cloudflared/ngrok."
    )


def make_call(callback_base: str, phone_number: str, message: str, voice: str):
    callback_url = f"{callback_base}/api/callbacks"
    print(f"Callback URL: {callback_url}")
    target = PhoneNumberIdentifier(phone_number)
    caller = PhoneNumberIdentifier(ACS_PHONE)

    props = client.create_call(
        target,
        callback_url,
        source_caller_id_number=caller,
        cognitive_services_endpoint=COGNITIVE_ENDPOINT,
    )
    print(f"Call created: connection_id={props.call_connection_id}")

    app.config["MESSAGE"] = message
    app.config["VOICE"] = voice


@app.route("/api/callbacks", methods=["POST"])
def handle_callbacks():
    for event_dict in request.json:
        event = CloudEvent.from_dict(event_dict)
        event_type = event.type

        if event_type == "Microsoft.Communication.CallConnected":
            call_connection_id = event.data["callConnectionId"]
            conn = client.get_call_connection(call_connection_id)
            play_source = TextSource(
                text=app.config["MESSAGE"],
                voice_name=app.config["VOICE"],
            )
            conn.play_media_to_all(play_source)
            print("Playing message...")

        elif event_type == "Microsoft.Communication.PlayCompleted":
            call_connection_id = event.data["callConnectionId"]
            conn = client.get_call_connection(call_connection_id)
            conn.hang_up(is_for_everyone=True)
            call_result["success"] = True
            call_result["message"] = "Call completed successfully"
            print("Message played, hanging up.")
            call_completed.set()

        elif event_type == "Microsoft.Communication.PlayFailed":
            call_connection_id = event.data["callConnectionId"]
            conn = client.get_call_connection(call_connection_id)
            conn.hang_up(is_for_everyone=True)
            call_result["success"] = False
            call_result["message"] = "Failed to play message"
            call_completed.set()

        elif event_type == "Microsoft.Communication.CreateCallFailed":
            call_result["success"] = False
            call_result["message"] = f"Call failed: {event.data.get('resultInformation', {}).get('message', 'unknown error')}"
            call_completed.set()

        elif event_type == "Microsoft.Communication.CallDisconnected":
            if not call_completed.is_set():
                call_result["success"] = False
                call_result["message"] = "Call disconnected (recipient may not have answered). Do NOT retry automatically."
                call_completed.set()

    return Response(status=200)


def run_server():
    app.run(host="0.0.0.0", port=CALLBACK_PORT, threaded=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Make an outbound voice call")
    parser.add_argument("--phone", required=True, help="Target phone number (E.164)")
    parser.add_argument("--message", required=True, help="Message to speak")
    parser.add_argument("--voice", default="zh-TW-HsiaoChenNeural", help="TTS voice name")
    parser.add_argument("--timeout", type=int, default=60, help="Timeout in seconds")
    args = parser.parse_args()

    # Start local callback server
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    time.sleep(1)

    # Start tunnel for HTTPS callback
    tunnel_proc = None
    try:
        callback_base, tunnel_proc = start_tunnel(CALLBACK_PORT)
        make_call(callback_base, args.phone, args.message, args.voice)

        if call_completed.wait(timeout=args.timeout):
            if call_result["success"]:
                print(f"SUCCESS: {call_result['message']}")
                sys.exit(0)
            else:
                print(f"FAILED: {call_result['message']}", file=sys.stderr)
                sys.exit(1)
        else:
            print("FAILED: Timeout - recipient did not answer. Do NOT retry automatically.", file=sys.stderr)
            sys.exit(1)
    finally:
        if tunnel_proc:
            tunnel_proc.kill()
