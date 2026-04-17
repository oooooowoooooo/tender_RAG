import json
import socket
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


SSL_CONTEXT = ssl._create_unverified_context()
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
}


def _request(
    url: str,
    method: str = "GET",
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 60,
) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url, data=data, method=method)
    for key, value in {**DEFAULT_HEADERS, **(headers or {})}.items():
        request.add_header(key, value)
    with urllib.request.urlopen(request, context=SSL_CONTEXT, timeout=timeout) as response:
        return response.status, dict(response.headers.items()), response.read()


def get_bytes(url: str, headers: dict[str, str] | None = None, timeout: int = 60) -> tuple[int, dict[str, str], bytes]:
    return _request(url=url, method="GET", headers=headers, timeout=timeout)


def get_text(url: str, headers: dict[str, str] | None = None, timeout: int = 60) -> tuple[int, dict[str, str], str]:
    status, response_headers, payload = get_bytes(url, headers=headers, timeout=timeout)
    charset = "utf-8"
    content_type = response_headers.get("Content-Type", "")
    if "charset=" in content_type:
        charset = content_type.split("charset=", 1)[1].split(";", 1)[0].strip()
    return status, response_headers, payload.decode(charset, "ignore")


def post_form_json(
    url: str,
    form_data: dict[str, str],
    headers: dict[str, str] | None = None,
    timeout: int = 60,
) -> dict[str, Any]:
    payload = urllib.parse.urlencode(form_data).encode("utf-8")
    _, _, text = _request(
        url=url,
        method="POST",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded", **(headers or {})},
        timeout=timeout,
    )
    return json.loads(text)


def classify_fetch_error(error: Exception) -> str:
    if isinstance(error, urllib.error.HTTPError):
        if error.code == 403:
            return "http_403"
        if error.code == 404:
            return "http_404"
        return f"http_{error.code}"
    if isinstance(error, urllib.error.URLError):
        if isinstance(error.reason, socket.timeout):
            return "timeout"
        return "network_error"
    if isinstance(error, TimeoutError):
        return "timeout"
    return "fetch_error"
