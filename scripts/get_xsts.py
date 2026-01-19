#!/usr/bin/env python3
import os, json, requests

# 1) Paste the *live.com* access_token (scope: service::user.auth.xboxlive.com::MBI_SSL)
MSA_ACCESS_TOKEN = os.environ.get("MSA_ACCESS_TOKEN", "").strip()
if not MSA_ACCESS_TOKEN:
    raise SystemExit("Set MSA_ACCESS_TOKEN to the live.com access_token first")

def post_json(url, payload):
    r = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=30)
    return r.status_code, r.headers, r.text

# 2) XBL user token
xbl_url = "https://user.auth.xboxlive.com/user/authenticate"
xbl_payload = {
    "Properties": {
        "AuthMethod": "RPS",
        "SiteName": "user.auth.xboxlive.com",
        # Critical: many flows require the "d=" prefix for RPS tickets
        "RpsTicket": "d=" + MSA_ACCESS_TOKEN,
    },
    "RelyingParty": "http://auth.xboxlive.com",
    "TokenType": "JWT",
}

status, headers, body = post_json(xbl_url, xbl_payload)
if status != 200:
    print("XBL FAIL", status)
    print("headers:", dict(headers))
    print("body:", body[:400])
    raise SystemExit(1)

xbl = json.loads(body)
xbl_token = xbl["Token"]
uhs = xbl["DisplayClaims"]["xui"][0]["uhs"]

# 3) XSTS token (RETAIL sandbox)
# RelyingParty depends on what you will call next.
# For many Xbox consumer APIs, "http://xboxlive.com" is the common choice.
# Allow override via env var.
xsts_url = "https://xsts.auth.xboxlive.com/xsts/authorize"
RELYING_PARTY = os.environ.get("XSTS_RELYING_PARTY", "http://xboxlive.com").strip() or "http://xboxlive.com"

xsts_payload = {
    "Properties": {
        "SandboxId": "RETAIL",
        "UserTokens": [xbl_token],
    },
    "RelyingParty": RELYING_PARTY,
    "TokenType": "JWT",
}

status, headers, body = post_json(xsts_url, xsts_payload)
if status != 200:
    print("XSTS FAIL", status)
    print("headers:", dict(headers))
    print("body:", body[:400])
    raise SystemExit(1)

xsts = json.loads(body)
xsts_token = xsts["Token"]

# Print shell-safe exports so you can do: eval "$(python src/get_xsts.py)"
print(f'export XSTS_UHS="{uhs}"')
print(f'export XSTS_TOKEN="{xsts_token}"')
print(f'export XBL3_AUTH="XBL3.0 x={uhs};{xsts_token}"')
print(f'export XSTS_RELYING_PARTY="{RELYING_PARTY}"')
