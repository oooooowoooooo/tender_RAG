from __future__ import annotations

import csv
import json
import re
import ssl
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
ACCEPTANCE_DIR = ROOT / "acceptance_assets" / "raw_sources"
POLICY_HTML_DIR = ACCEPTANCE_DIR / "01_policy" / "html_samples"
TENDER_HTML_DIR = ACCEPTANCE_DIR / "02_tender" / "html_samples"
TENDER_PDF_DIR = ACCEPTANCE_DIR / "02_tender" / "pdf_samples"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def ensure_dirs() -> None:
    for path in (POLICY_HTML_DIR, TENDER_HTML_DIR, TENDER_PDF_DIR):
        path.mkdir(parents=True, exist_ok=True)


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def fetch_bytes(url: str) -> bytes:
    parsed = urlparse(url)
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": f"{parsed.scheme}://{parsed.netloc}/",
        },
    )
    context = ssl._create_unverified_context()
    with urlopen(request, timeout=30, context=context) as response:
        return response.read()


def safe_name(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return value.strip("._") or "sample"


def save_download(url: str, target: Path) -> dict[str, str]:
    try:
        raw = fetch_bytes(url)
        target.write_bytes(raw)
        return {"url": url, "path": str(target.relative_to(ROOT)).replace("\\", "/"), "status": "success"}
    except Exception as exc:  # noqa: BLE001
        return {"url": url, "path": str(target.relative_to(ROOT)).replace("\\", "/"), "status": f"failed: {exc}"}


def collect_policy_html_samples(limit: int = 3) -> list[dict[str, str]]:
    rows = read_rows(ROOT / "data_new" / "01_policy" / "policy_local_docs_ah_hf.csv")
    results: list[dict[str, str]] = []
    for index, row in enumerate(rows[:limit], start=1):
        filename = f"policy_{index:02d}_{safe_name(row.get('id', 'sample'))}.html"
        results.append(save_download(row["url"], POLICY_HTML_DIR / filename))
    return results


def collect_tender_html_samples(limit: int = 3) -> list[dict[str, str]]:
    rows = read_rows(ACCEPTANCE_DIR / "02_tender" / "tender_notice_raw_1000.csv")
    results: list[dict[str, str]] = []
    for index, row in enumerate(rows[:limit], start=1):
        filename = f"tender_{index:02d}_{safe_name(row.get('doc_id', 'sample'))}.html"
        results.append(save_download(row["link"], TENDER_HTML_DIR / filename))
    return results


def extract_attachment_urls(limit: int = 3) -> list[str]:
    rows = read_rows(ROOT / "data_new" / "02_tender" / "tender_notices_procurement.csv")
    urls: list[str] = []
    for row in rows:
        raw_links = row.get("attachment_file_link", "")
        for line in raw_links.splitlines():
            if ":" not in line:
                continue
            _, url = line.split(":", 1)
            if url.strip().lower().endswith(".pdf") or "downloadZtbAttach.jspx" in url:
                urls.append(url.strip())
            if len(urls) >= limit:
                return urls
    return urls


def collect_tender_pdf_samples(limit: int = 3) -> list[dict[str, str]]:
    urls = extract_attachment_urls(limit=limit)
    results: list[dict[str, str]] = []
    for index, url in enumerate(urls, start=1):
        parsed = urlparse(url)
        stem = Path(parsed.path).name or f"sample_{index}"
        filename = f"attachment_{index:02d}_{safe_name(stem)}.pdf"
        results.append(save_download(url, TENDER_PDF_DIR / filename))
    return results


def main() -> None:
    ensure_dirs()
    manifest = {
        "policy_html_samples": collect_policy_html_samples(),
        "tender_html_samples": collect_tender_html_samples(),
        "tender_pdf_samples": collect_tender_pdf_samples(),
    }
    output = ACCEPTANCE_DIR / "original_file_samples_manifest.json"
    output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
