#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.parse import urlparse, unquote, quote

import requests

# Xbox Live uses Microsoft Account (login.live.com) tokens ("service::user.auth.xboxlive.com::MBI_SSL").
LIVE_CLIENT_ID = "0000000048093EE3"  # official Xbox/Microsoft Account client id used by xbox-webapi-ex
LIVE_REDIRECT_URI = "https://login.live.com/oauth20_desktop.srf"
LIVE_SCOPE = "service::user.auth.xboxlive.com::MBI_SSL"
LIVE_AUTHORIZE_URL = "https://login.live.com/oauth20_authorize.srf"

CACHE = Path(os.environ.get("XBOX_TOKEN_OUT", "./xbox_tokens.json")).expanduser()

# ---- Xbox endpoints ----
XBL_AUTH_URL = "https://user.auth.xboxlive.com/user/authenticate"
XSTS_AUTH_URL = "https://xsts.auth.xboxlive.com/xsts/authorize"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) xbox-webapi-ex/diagnostic"


def log_resp(prefix: str, r: requests.Response) -> None:
    ct = r.headers.get("content-type")
    # Helpful for XBL errors (they often return empty body but include correlation headers)
    hdrs = {k: v for k, v in r.headers.items() if k.lower() in ("ms-cv", "x-xblcorrelationid", "date", "server")}
    body = (r.text or "")
    print(f"{prefix} STATUS: {r.status_code}")
    print(f"{prefix} CONTENT-TYPE: {ct}")
    print(f"{prefix} HEADERS: {hdrs}")
    print(f"{prefix} BODY_LEN: {len(body)}")
    print(f"{prefix} BODY_PREFIX: {body[:400].replace(chr(10),'\\n')}")


def live_msa_tokens_via_playwright(timeout_seconds: int = 600) -> dict:
    """Launch a browser and capture the final redirect URL containing #access_token=... from login.live.com.

    Requires:
      python -m pip install playwright
      python -m playwright install chromium
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        raise RuntimeError(
            "Playwright is required for programmatic capture. Install with:\n"
            "  python -m pip install playwright\n"
            "  python -m playwright install chromium\n"
            f"Original import error: {e!r}"
        )

    params = {
        "client_id": LIVE_CLIENT_ID,
        "redirect_uri": LIVE_REDIRECT_URI,
        "response_type": "token",
        "display": "touch",
        "scope": LIVE_SCOPE,
        "locale": "en",
    }

    q = "&".join([f"{k}={requests.utils.quote(str(v), safe='')}" for k, v in params.items()])
    auth_url = f"{LIVE_AUTHORIZE_URL}?{q}"

    print("Open/sign-in in the browser window. Waiting for redirect to:", LIVE_REDIRECT_URI)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context()
        page = ctx.new_page()

        def scan_page_for_token(p):
            # window.location.href includes the fragment; page.url often does not.
            try:
                href = p.evaluate("() => window.location.href")
            except Exception:
                href = p.url
            if (
                captured["url"] is None
                and href
                and href.startswith(LIVE_REDIRECT_URI)
                and "#" in href
                and "access_token=" in href
            ):
                captured["url"] = href
            return href

        # Some auth flows briefly hit the redirect URL and then immediately navigate away.
        # Also, the redirect can happen in a sub-frame. So we capture the FIRST redirect
        # URL we see from ANY frame navigation, instead of assuming the page "ends" there.
        captured = {"url": None}

        def on_nav(frame):
            u = frame.url
            print("NAV:", u)
            if captured["url"] is None and u.startswith(LIVE_REDIRECT_URI) and "access_token=" in u and "#" in u:
                captured["url"] = u

        page.on("framenavigated", on_nav)

        def on_popup(popup):
            # Ensure we watch navigations in the popup too.
            try:
                popup.on("framenavigated", lambda frame: on_nav(frame))
            except Exception:
                pass

        page.on("popup", on_popup)
        # Also watch any new pages created at the context level.
        ctx.on("page", lambda p: p.on("framenavigated", lambda frame: on_nav(frame)))

        page.goto(auth_url)

        deadline = time.time() + timeout_seconds
        last_url: str | None = None

        while time.time() < deadline:
            # Prefer a captured redirect URL from any frame. This avoids missing the token
            # when the browser immediately navigates away after hitting oauth20_desktop.srf.
            url = captured.get("url")

            if not url:
                # Scan the current page quickly; this is critical because the redirect
                # with the fragment can appear and disappear very quickly.
                url = scan_page_for_token(page)

                if url != last_url:
                    print("URL:", url)
                    last_url = url

                # Additionally, try a very short wait for the token to appear.
                # This avoids missing a transient fragment between polls.
                if captured["url"] is None:
                    try:
                        page.wait_for_function(
                            """(redirect) => {
                                try {
                                    const href = window.location.href || '';
                                    return href.startsWith(redirect) && href.includes('#') && href.includes('access_token=');
                                } catch (e) { return false; }
                            }""",
                            arg=LIVE_REDIRECT_URI,
                            timeout=200,
                        )
                        url = scan_page_for_token(page)
                    except Exception:
                        pass

            # Accept querystring variants (e.g. oauth20_desktop.srf?lc=1033#access_token=...)
            if url and url.startswith(LIVE_REDIRECT_URI) and "access_token=" in url and "#" in url:
                parsed = urlparse(url)

                # IMPORTANT: do NOT use parse_qs here; it uses unquote_plus, which can corrupt tokens.
                # We decode using unquote so '+' remains '+' (not space).
                frag_raw: dict[str, str] = {}
                frag_decoded: dict[str, str] = {}

                for part in (parsed.fragment or "").split("&"):
                    if not part:
                        continue
                    if "=" not in part:
                        frag_raw[part] = ""
                        frag_decoded[part] = ""
                        continue
                    k, v = part.split("=", 1)
                    # Keep the raw fragment value EXACTLY as provided by the browser
                    # (it may already contain '/' and other non-escaped characters).
                    frag_raw[k] = v
                    # Also keep a percent-decoded version. Use unquote (NOT unquote_plus)
                    # so '+' remains '+' (not space).
                    frag_decoded[k] = unquote(v)
                print("Captured fragment keys:", sorted(frag_decoded.keys()))

                if "access_token" not in frag_raw and "access_token" not in frag_decoded:
                    raise RuntimeError(f"Redirect reached but missing access_token fragment: {url}")

                token = {
                    # For XBL, we'll try multiple variants later.
                    "access_token_raw": frag_raw.get("access_token"),
                    "access_token": frag_decoded.get("access_token") or frag_raw.get("access_token"),
                    "refresh_token_raw": frag_raw.get("refresh_token"),
                    "refresh_token": frag_decoded.get("refresh_token") or frag_raw.get("refresh_token"),
                    "token_type": frag_decoded.get("token_type") or frag_raw.get("token_type"),
                    "expires_in": frag_decoded.get("expires_in") or frag_raw.get("expires_in"),
                    "scope": frag_decoded.get("scope") or frag_raw.get("scope"),
                    "user_id": frag_decoded.get("user_id") or frag_raw.get("user_id"),
                    "redirect_url": url,
                }

                ctx.close()
                browser.close()
                return token

            time.sleep(0.02)

        ctx.close()
        browser.close()

    raise TimeoutError(f"Timed out waiting for login.live.com redirect with access_token. last_url={last_url!r}")


def xbl_user_authenticate(ms_access_token: str, ms_access_token_raw: str | None = None) -> dict:
    # RpsTicket formatting is unfortunately inconsistent across samples and accounts.
    # We will try a small matrix:
    #   - raw token vs percent-decoded token
    #   - with and without d=/t= prefixes
    #   - with and without x-xbl-contract-version header

    relying_parties = ["http://auth.xboxlive.com", "https://auth.xboxlive.com"]
    def attempt(rps_ticket: str, relying_party: str, with_contract_header: bool = True) -> requests.Response:
        payload = {
            "Properties": {
                "AuthMethod": "RPS",
                "SiteName": "user.auth.xboxlive.com",
                "RpsTicket": rps_ticket,
            },
            # NOTE: samples vary between http/https; we will try both via outer loop.
            "RelyingParty": relying_party,
            "TokenType": "JWT",
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            # Be closer to real traffic; some backends behave differently with bare UA.
            "User-Agent": UA,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "DNT": "1",
        }
        if with_contract_header:
            headers["x-xbl-contract-version"] = "1"
        return requests.post(XBL_AUTH_URL, headers=headers, json=payload, timeout=30)

    # We may have two representations:
    #   - ms_access_token: decoded (percent-decoded) token
    #   - ms_access_token_raw: raw fragment value from the browser (may still contain %XX)
    decoded = ms_access_token
    raw_fragment = ms_access_token_raw or ms_access_token

    # Derive a decoded version of the raw fragment (if it has %XX escapes)
    try:
        raw_fragment_decoded = unquote(raw_fragment)
    except Exception:
        raw_fragment_decoded = raw_fragment

    def pct_encode(s: str) -> str:
        """Percent-encode the token for use inside RpsTicket.

        Some Xbox auth backends are picky and expect the RpsTicket token portion to be fully
        URL-encoded (e.g. '/' -> %2F, '+' -> %2B). We therefore try fully-encoded variants.
        """
        # Important: do not treat '+' as space. Use quote() on the exact string.
        return quote(s, safe='')

    decoded_enc = pct_encode(decoded)
    rawfrag_enc = pct_encode(raw_fragment)
    rawfrag_decoded_enc = pct_encode(raw_fragment_decoded)

    tickets: list[tuple[str, str]] = []

    def add(label: str, ticket: str | None) -> None:
        if not ticket:
            return
        tickets.append((label, ticket))

    # Plain tokens
    add("decoded", decoded)
    add("rawfrag", raw_fragment)
    add("rawfrag_decoded", raw_fragment_decoded)

    # Fully percent-encoded tokens (sometimes required by XBL)
    add("decoded_enc", decoded_enc)
    add("rawfrag_enc", rawfrag_enc)
    add("rawfrag_decoded_enc", rawfrag_decoded_enc)

    # Prefixed variants
    for base_label, base in [
        ("decoded", decoded),
        ("rawfrag", raw_fragment),
        ("rawfrag_decoded", raw_fragment_decoded),
        ("decoded_enc", decoded_enc),
        ("rawfrag_enc", rawfrag_enc),
        ("rawfrag_decoded_enc", rawfrag_decoded_enc),
    ]:
        add(f"d={base_label}", f"d={base}")
        add(f"t={base_label}", f"t={base}")

    # De-dup while preserving order
    seen = set()
    uniq: list[tuple[str, str]] = []
    for lbl, t in tickets:
        if t in seen:
            continue
        seen.add(t)
        uniq.append((lbl, t))

    # Try with contract header first, then without.
    attempts: list[tuple[str, requests.Response]] = []

    for relying_party in relying_parties:
        for with_contract in (True, False):
            for lbl, t in uniq:
                r = attempt(t, relying_party=relying_party, with_contract_header=with_contract)
                if r.status_code == 200:
                    return r.json()
                attempts.append((f"XBL({lbl},rp={relying_party},contract={with_contract})", r))

    # Log the most informative failures (up to 12)
    for lbl, r in attempts[:12]:
        log_resp(lbl, r)

    # If none succeeded, raise a compact summary
    summary = ", ".join([f"{lbl}={r.status_code}" for lbl, r in attempts[:12]])
    raise RuntimeError(f"XBL user/authenticate failed. First attempts: {summary}")


def xsts_authorize(xbl_token: str) -> dict:
    payload = {
        "Properties": {
            "SandboxId": "RETAIL",
            "UserTokens": [xbl_token],
        },
        "RelyingParty": "http://xboxlive.com",
        "TokenType": "JWT",
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": UA,
        "x-xbl-contract-version": "1",
    }
    r = requests.post(XSTS_AUTH_URL, headers=headers, json=payload, timeout=30)
    if r.status_code != 200:
        log_resp("XSTS", r)
        raise RuntimeError(f"XSTS authorize failed: {r.status_code}")
    return r.json()


def main() -> None:
    ms = live_msa_tokens_via_playwright()
    # Prefer decoded token, but keep raw available for debug.
    ms_access_token = ms.get("access_token")
    ms_access_token_raw = ms.get("access_token_raw")
    if not ms_access_token:
        raise RuntimeError("No access_token from login.live.com token response")

    print("MS token scope:", ms.get("scope"))
    print("MS access_token len(dec):", len(ms_access_token))
    if ms_access_token_raw:
        print("MS access_token len(raw):", len(ms_access_token_raw))
    print("MS access_token startswith:", (ms_access_token[:6] + "..." if ms_access_token else None))
    if ms_access_token_raw:
        print("MS access_token_raw startswith:", (ms_access_token_raw[:6] + "..."))

    xbl = xbl_user_authenticate(ms_access_token, ms_access_token_raw)
    xbl_token = xbl["Token"]
    uhs = xbl["DisplayClaims"]["xui"][0]["uhs"]

    xsts = xsts_authorize(xbl_token)
    xsts_token = xsts["Token"]
    xid = xsts["DisplayClaims"]["xui"][0].get("xid")

    out = {
        "ms": {
            "token_type": ms.get("token_type"),
            "scope": ms.get("scope"),
            "expires_in": ms.get("expires_in"),
            "user_id": ms.get("user_id"),
            "has_refresh_token": bool(ms.get("refresh_token")),
            "access_token_len": len(ms_access_token),
            "access_token_raw_len": (len(ms_access_token_raw) if ms_access_token_raw else None),
            "redirect_url_prefix": (ms.get("redirect_url") or "")[:80],
        },
        "xbl": {"uhs": uhs, "token": xbl_token},
        "xsts": {"uhs": xsts["DisplayClaims"]["xui"][0]["uhs"], "xid": xid, "token": xsts_token},
        "authorization_header": f"XBL3.0 x={uhs};{xsts_token}",
    }

    CACHE.write_text(json.dumps(out, indent=2))
    print("OK: wrote", str(CACHE))
    print("uhs:", uhs)
    print("auth header ready in xbox_tokens.json as authorization_header")


if __name__ == "__main__":
    main()