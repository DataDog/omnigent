"""Conservative classification of completed, unsuccessful token responses."""

import httpx


def refresh_retry_safe(response: httpx.Response) -> bool:
    """Allow rate-limit rejection or an explicit OAuth temporary error.

    A bare 503 may follow an upstream rotation, so status alone is insufficient.
    Transport errors and malformed/success payloads never establish safe retry.
    """
    if response.status_code not in (429, 503):
        return False
    try:
        payload = response.json()
    except ValueError:
        return response.status_code == 429
    if isinstance(payload, dict) and ("access_token" in payload or "refresh_token" in payload):
        return False
    if isinstance(payload, dict) and "error" in payload:
        return payload["error"] in ("temporarily_unavailable", "server_error")
    return response.status_code == 429
