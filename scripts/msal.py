#!/usr/bin/env python3
import time, requests

# Known-good public client (Azure CLI)
CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
TENANT = "consumers"

DEVICE_CODE_URL = f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/devicecode"
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/token"

SCOPES = "openid profile offline_access"

dc = requests.post(DEVICE_CODE_URL, data={"client_id": CLIENT_ID, "scope": SCOPES}, timeout=30)
dc.raise_for_status()
j = dc.json()

print(j["message"])
interval = int(j.get("interval", 5))
deadline = time.time() + int(j.get("expires_in", 900))

while time.time() < deadline:
    tok = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "client_id": CLIENT_ID,
            "device_code": j["device_code"],
        },
        timeout=30,
    )
    if tok.status_code == 200:
        t = tok.json()
        print("SUCCESS")
        print("scope:", t.get("scope"))
        print("has_refresh_token:", "refresh_token" in t)
        break

    e = tok.json()
    if e.get("error") in ("authorization_pending", "slow_down"):
        time.sleep(interval + (5 if e.get("error") == "slow_down" else 0))
        continue
    raise RuntimeError(e)
else:
    raise TimeoutError("Device code expired.")