#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import requests

DEBUG = os.environ.get("MS_DEBUG", "").strip().lower() in ("1", "true", "yes", "y", "on")


def _log(msg: str) -> None:
    if DEBUG:
        print(f"[debug] {msg}")


def _safe_prefix(s: str, n: int = 400) -> str:
    s = s or ""
    return s[:n].replace("\n", "\\n")


def _decode_jwt_claims(token: Optional[str]) -> Dict[str, Any]:
    """Best-effort decode of a JWT payload without verifying signature.

    Useful for debugging which account/token you received.
    """
    if not token:
        return {}
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    import base64

    payload_b64 = parts[1]
    # base64url pad
    pad = "=" * ((4 - (len(payload_b64) % 4)) % 4)
    try:
        payload = base64.urlsafe_b64decode((payload_b64 + pad).encode("utf-8"))
        return json.loads(payload.decode("utf-8"))
    except Exception:
        return {}

TENANT = "consumers"
AUTH_BASE = f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0"

DEVICE_CODE_URL = f"{AUTH_BASE}/devicecode"
TOKEN_URL = f"{AUTH_BASE}/token"

# Use the client_id you just proved works (the script that printed microsoft.com/link)
# If you used a different one, paste it here.
CLIENT_ID = os.environ.get("MS_CLIENT_ID", "").strip()
if not CLIENT_ID:
    raise SystemExit("Set MS_CLIENT_ID env var to the working client_id you used.")

# Minimal identity scopes for “always succeeds”
SCOPES = "openid profile offline_access"

CACHE_PATH = Path(os.environ.get("MS_TOKEN_CACHE", "~/.config/msauth/device_token.json")).expanduser()


@dataclass
class TokenSet:
    access_token: Optional[str]
    refresh_token: Optional[str]
    id_token: Optional[str]
    expires_at: Optional[int]  # unix seconds
    scope: Optional[str]
    token_type: Optional[str]

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "TokenSet":
        return TokenSet(
            access_token=d.get("access_token"),
            refresh_token=d.get("refresh_token"),
            id_token=d.get("id_token"),
            expires_at=d.get("expires_at"),
            scope=d.get("scope"),
            token_type=d.get("token_type"),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "id_token": self.id_token,
            "expires_at": self.expires_at,
            "scope": self.scope,
            "token_type": self.token_type,
        }


def _mkdir_secure(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Best-effort: ensure directory is user-only
    try:
        os.chmod(path.parent, 0o700)
    except Exception:
        pass


def _write_secure(path: Path, data: Dict[str, Any]) -> None:
    _mkdir_secure(path)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    # Ensure file mode 600
    os.chmod(tmp, stat.S_IRUSR | stat.S_IWUSR)
    tmp.replace(path)


def load_cache() -> Optional[TokenSet]:
    if not CACHE_PATH.exists():
        return None
    try:
        d = json.loads(CACHE_PATH.read_text())
        return TokenSet.from_json(d)
    except Exception:
        return None


def save_cache(tok: TokenSet) -> None:
    _write_secure(CACHE_PATH, tok.to_json())


def is_access_token_valid(tok: TokenSet, skew_seconds: int = 60) -> bool:
    if not tok.access_token or not tok.expires_at:
        return False
    return int(time.time()) + skew_seconds < int(tok.expires_at)


def device_code_login() -> TokenSet:
    _log(f"DEVICE_CODE_URL={DEVICE_CODE_URL} tenant={TENANT} client_id={CLIENT_ID!r} scopes={SCOPES!r}")
    r = requests.post(
        DEVICE_CODE_URL,
        data={"client_id": CLIENT_ID, "scope": SCOPES},
        timeout=30,
    )
    _log(f"devicecode HTTP={r.status_code} content-type={r.headers.get('content-type')!r} body_prefix={_safe_prefix(r.text)!r}")

    if r.status_code != 200:
        ct = (r.headers.get("content-type") or "").lower()
        body_prefix = (r.text or "")[:800].replace("\n", "\\n")
        # Microsoft usually returns JSON with fields like error/error_description.
        if "application/json" in ct:
            try:
                j = r.json()
            except Exception:
                j = {"_parse_error": True, "body_prefix": body_prefix}
            raise RuntimeError(
                "Device code request failed. "
                f"HTTP={r.status_code} content_type={r.headers.get('content-type')} "
                f"client_id={CLIENT_ID!r} scopes={SCOPES!r} response={j!r}"
            )
        raise RuntimeError(
            "Device code request failed. "
            f"HTTP={r.status_code} content_type={r.headers.get('content-type')} "
            f"client_id={CLIENT_ID!r} scopes={SCOPES!r} body_prefix={body_prefix!r}"
        )

    dc = r.json()

    # Microsoft returns a user-friendly message already
    print(dc.get("message") or f"Go to {dc['verification_uri']} and enter {dc['user_code']}")

    interval = int(dc.get("interval", 5))
    deadline = time.time() + int(dc.get("expires_in", 900))

    while time.time() < deadline:
        time.sleep(interval)

        _log(f"polling token endpoint {TOKEN_URL} (interval={interval}s)")
        tr = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "client_id": CLIENT_ID,
                "device_code": dc["device_code"],
            },
            timeout=30,
        )
        _log(f"token HTTP={tr.status_code} content-type={tr.headers.get('content-type')!r} body_prefix={_safe_prefix(tr.text)!r}")

        if tr.status_code == 200:
            t = tr.json()
            expires_in = int(t.get("expires_in", 3600))
            tok = TokenSet(
                access_token=t.get("access_token"),
                refresh_token=t.get("refresh_token"),
                id_token=t.get("id_token"),
                expires_at=int(time.time()) + expires_in,
                scope=t.get("scope"),
                token_type=t.get("token_type"),
            )
            save_cache(tok)
            return tok

        body = tr.json()
        err = body.get("error")
        if err in ("authorization_pending", "slow_down"):
            if err == "slow_down":
                interval += 5
            continue

        raise RuntimeError(f"Device code token failed: {body}")

    raise TimeoutError("Device code expired before you finished signing in.")


def refresh(tok: TokenSet) -> TokenSet:
    if not tok.refresh_token:
        raise RuntimeError("No refresh_token in cache; need interactive device login.")

    _log(f"refresh token endpoint {TOKEN_URL} client_id={CLIENT_ID!r} scope={SCOPES!r}")
    r = requests.post(
        TOKEN_URL,
        data={
            "client_id": CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": tok.refresh_token,
            # scope optional in v2 refresh; including it can restrict/shape returned tokens.
            # Keep minimal and stable:
            "scope": SCOPES,
        },
        timeout=30,
    )
    _log(f"refresh HTTP={r.status_code} content-type={r.headers.get('content-type')!r} body_prefix={_safe_prefix(r.text)!r}")

    # Helpful debugging if refresh fails
    if r.status_code != 200:
        try:
            j = r.json()
        except Exception:
            raise RuntimeError(f"Refresh failed: HTTP {r.status_code} {r.text[:300]!r}")
        raise RuntimeError(f"Refresh failed: {j}")

    t = r.json()
    expires_in = int(t.get("expires_in", 3600))

    # Microsoft may rotate refresh tokens — always persist the newest one.
    new_tok = TokenSet(
        access_token=t.get("access_token"),
        refresh_token=t.get("refresh_token") or tok.refresh_token,
        id_token=t.get("id_token") or tok.id_token,
        expires_at=int(time.time()) + expires_in,
        scope=t.get("scope") or tok.scope,
        token_type=t.get("token_type") or tok.token_type,
    )
    save_cache(new_tok)
    return new_tok


def get_token() -> tuple[TokenSet, str]:
    """Return (TokenSet, mode) where mode is one of: cached_access, refreshed, device_login."""
    tok = load_cache()

    if tok and is_access_token_valid(tok):
        return tok, "cached_access"

    if tok and tok.refresh_token:
        try:
            return refresh(tok), "refreshed"
        except Exception as e:
            _log(f"refresh failed; falling back to device login: {e!r}")

    return device_code_login(), "device_login"


def main() -> None:
    tok, mode = get_token()
    print("OK")
    print("mode:", mode)
    print("cache:", str(CACHE_PATH))
    print("expires_at:", tok.expires_at)
    print("scope:", tok.scope)
    print("has_refresh_token:", bool(tok.refresh_token))
    print("client_id:", CLIENT_ID)
    # Do not print tokens by default.

    # Always show which account you authenticated as (best-effort; derived from id_token).
    claims = _decode_jwt_claims(tok.id_token)
    if claims:
        account = (
            claims.get("preferred_username")
            or claims.get("email")
            or claims.get("upn")
            or "(unknown)"
        )
        name = claims.get("name") or "(no name)"
        tid = claims.get("tid") or "(no tid)"
        oid = claims.get("oid") or "(no oid)"
        iss = claims.get("iss") or "(no iss)"
        aud = claims.get("aud") or "(no aud)"
        print("account:", account)
        print("name:", name)
        print("tenant_id(tid):", tid)
        print("object_id(oid):", oid)
        print("issuer:", iss)
        print("audience(aud):", aud)

        if DEBUG:
            interesting = {
                k: claims.get(k)
                for k in (
                    "name",
                    "preferred_username",
                    "email",
                    "upn",
                    "oid",
                    "tid",
                    "sub",
                    "iss",
                    "aud",
                    "iat",
                    "exp",
                )
                if k in claims
            }
            _log(f"id_token claims: {interesting}")
    else:
        if DEBUG:
            _log("no id_token claims decoded")


if __name__ == "__main__":
    main()