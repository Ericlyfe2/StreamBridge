import hashlib
import hmac
import os
import time


SECRET = os.environ.get("STREAMBRIDGE_SIGNING_SECRET", "dev-signing-secret")


def sign_download(file_id: int, expires_in_seconds: int = 900) -> str:
    exp = int(time.time()) + expires_in_seconds
    payload = f"{file_id}:{exp}"
    signature = hmac.new(SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{file_id}:{exp}:{signature}"


def verify_download_token(token: str) -> int | None:
    try:
        file_id_str, exp_str, provided_sig = token.split(":")
        payload = f"{file_id_str}:{exp_str}"
        expected_sig = hmac.new(
            SECRET.encode(), payload.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected_sig, provided_sig):
            return None
        if int(exp_str) < int(time.time()):
            return None
        return int(file_id_str)
    except ValueError:
        return None
