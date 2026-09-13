#!/usr/bin/env python3
"""Prepare a fresh local Rocket.Chat (scripts/smoke/compose.yml) for the smoke test.

Creates the bot user and a test human, generates Personal Access Tokens for both,
points file storage at MinIO (S3 API, proxying off), and writes the env file that
``scripts/smoke_test.py --env-file`` reads.  Idempotent: re-running reuses users
and regenerates tokens.
"""

from __future__ import annotations

import argparse
import json
import secrets
import sys
import time
import urllib.error
import urllib.request

ADMIN_USER = "smoke-admin"
ADMIN_PASS = "SmokeAdmin2026!"
BOT_USER = "hermes-bot"
HUMAN_USER = "smoke-human"
PASSWORD = "SmokeUser2026!"


def call(base: str, method: str, path: str, *, auth: dict | None = None, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(f"{base}/api/v1/{path}", data=data, method=method)
    request.add_header("Content-Type", "application/json")
    for key, value in (auth or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode() or "{}")
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode(errors="replace")
        raise SystemExit(f"{method} {path} -> HTTP {exc.code}: {payload[:400]}") from None


def wait_for_server(base: str, timeout: float = 600.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base}/api/info", timeout=5) as response:
                info = json.loads(response.read().decode())
                print(f"Rocket.Chat {info.get('version')} is up")
                return
        except Exception:
            time.sleep(3)
    raise SystemExit("Rocket.Chat did not come up in time")


def login(base: str, user: str, password: str) -> dict:
    data = call(base, "POST", "login", body={"user": user, "password": password})["data"]
    return {"X-Auth-Token": data["authToken"], "X-User-Id": data["userId"]}


def ensure_user(base: str, admin: dict, username: str, name: str, roles: list[str]) -> str:
    try:
        existing = call(base, "GET", f"users.info?username={username}", auth=admin)
        return existing["user"]["_id"]
    except SystemExit:
        pass
    created = call(
        base, "POST", "users.create", auth=admin,
        body={
            "username": username, "name": name, "password": PASSWORD,
            "email": f"{username}@smoke.invalid", "roles": roles, "verified": True,
            "requirePasswordChange": False, "joinDefaultChannels": False, "sendWelcomeEmail": False,
        },
    )
    return created["user"]["_id"]


def personal_token(base: str, username: str) -> str:
    auth = login(base, username, PASSWORD)
    name = f"smoke-{secrets.token_hex(3)}"
    result = call(base, "POST", "users.generatePersonalAccessToken", auth=auth,
                  body={"tokenName": name, "bypassTwoFactor": True})
    return result["token"]


def set_setting(base: str, admin: dict, key: str, value) -> None:
    """Write one setting; a setting unknown to this Rocket.Chat release is reported and skipped."""
    try:
        call(base, "POST", f"settings/{key}", auth=admin, body={"value": value})
    except SystemExit as exc:
        if "HTTP 400" in str(exc):
            print(f"warning: setting {key} not accepted by this release, skipped")
            return
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:3000")
    parser.add_argument("--lan-ip", required=True, help="host address the Rocket.Chat container can reach MinIO on")
    parser.add_argument("--out", required=True, help="env file to write for smoke_test.py")
    parser.add_argument("--storage", choices=["minio", "gridfs"], default="minio")
    args = parser.parse_args()

    wait_for_server(args.url)
    admin = login(args.url, ADMIN_USER, ADMIN_PASS)
    bot_id = ensure_user(args.url, admin, BOT_USER, "Hermes Bot", ["user", "bot"])
    human_id = ensure_user(args.url, admin, HUMAN_USER, "Smoke Human", ["user"])
    bot_token = personal_token(args.url, BOT_USER)
    human_token = personal_token(args.url, HUMAN_USER)
    admin_token = call(args.url, "POST", "users.generatePersonalAccessToken", auth=admin,
                       body={"tokenName": f"smoke-admin-{secrets.token_hex(3)}", "bypassTwoFactor": True})["token"]

    set_setting(args.url, admin, "Message_AllowUnrecognizedSlashCommand", True)
    set_setting(args.url, admin, "API_Enable_Rate_Limiter", False)
    if args.storage == "minio":
        bucket_url = f"http://{args.lan_ip}:9000"
        for key, value in (
            ("FileUpload_S3_Bucket", "rc-uploads"),
            ("FileUpload_S3_AWSAccessKeyId", "smokeminio"),
            ("FileUpload_S3_AWSSecretAccessKey", "smokeminio2026"),
            ("FileUpload_S3_BucketURL", bucket_url),
            ("FileUpload_S3_Region", "us-east-1"),
            ("FileUpload_S3_ForcePathStyle", True),
            ("FileUpload_S3_Proxy_Uploads", False),
            ("FileUpload_Storage_Type", "AmazonS3"),
        ):
            set_setting(args.url, admin, key, value)
        print(f"file storage: MinIO via {bucket_url}, proxy off (redirect path)")
    else:
        set_setting(args.url, admin, "FileUpload_Storage_Type", "GridFS")
        print("file storage: GridFS (no redirects)")

    lines = [
        f"RC_SMOKE_URL={args.url}",
        f"RC_SMOKE_BOT_USER_ID={bot_id}",
        f"RC_SMOKE_BOT_TOKEN={bot_token}",
        f"RC_SMOKE_HUMAN_USER_ID={human_id}",
        f"RC_SMOKE_HUMAN_USERNAME={HUMAN_USER}",
        f"RC_SMOKE_HUMAN_TOKEN={human_token}",
        f"RC_SMOKE_ADMIN_USER_ID={admin['X-User-Id']}",
        f"RC_SMOKE_ADMIN_TOKEN={admin_token}",
        "RC_SMOKE_ALLOW_INSECURE_HTTP=true",
        "RC_SMOKE_ALLOW_PRIVATE_FILE_REDIRECTS=true",
        f"RC_SMOKE_EXPECT_REDIRECT={'true' if args.storage == 'minio' else 'false'}",
    ]
    with open(args.out, "w") as handle:
        handle.write("\n".join(lines) + "\n")
    print(f"wrote {args.out} (bot={BOT_USER}/{bot_id}, human={HUMAN_USER}/{human_id})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
