import argparse
import json
from pathlib import Path

from lib.browser_utils import dump_dom_with_chrome
from lib.fetch_utils import classify_fetch_error, get_text
from lib.hash_utils import sha256_file
from lib.io_utils import ROOT, append_jsonl, now_ts, read_jsonl
from lib.manifest_utils import build_attempt_row


FETCH_QUEUE_PATH = ROOT / "data" / "manifests" / "fetch_queue_v1.jsonl"
FETCH_ATTEMPTS_PATH = ROOT / "data" / "manifests" / "fetch_attempts.jsonl"
TENDER_HTML_DIR = ROOT / "data" / "raw" / "tender" / "html"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=f"tender_detail_{now_ts().replace(' ', '_').replace(':', '')}")
    parser.add_argument("--limit", type=int, default=20)
    return parser.parse_args()


def looks_like_block_page(text: str) -> bool:
    lowered = text.lower()
    return "403 forbidden" in lowered or "client ip" in lowered or "eventid:" in lowered


def main() -> None:
    args = parse_args()
    tasks = [
        task
        for task in read_jsonl(FETCH_QUEUE_PATH)
        if task.get("dataset") == "tender" and task.get("asset_role") == "detail_html"
    ]
    if args.limit > 0:
        tasks = tasks[: args.limit]

    success = 0
    failure = 0

    for task in tasks:
        project_id = str(task["entity_id"])
        source_url = str(task.get("source_url", "")).strip()
        target_path = TENDER_HTML_DIR / f"{project_id}__hefei_ggzy_detail_page__detail.html"
        retrieved_at = now_ts()

        if not source_url:
            append_jsonl(
                FETCH_ATTEMPTS_PATH,
                build_attempt_row(
                    run_id=args.run_id,
                    dataset="tender",
                    entity_id=project_id,
                    source_id="hefei_ggzy_detail_page",
                    asset_role="detail_html",
                    source_url="",
                    resolved_url="",
                    fetch_method="requests",
                    status="manual_review_required",
                    content_type="text/html",
                    retrieved_at=retrieved_at,
                    storage_path="",
                    sha256="",
                    extra={"message": "source_url_missing"},
                ),
            )
            failure += 1
            continue

        try:
            _, headers, text = get_text(source_url, headers={"Referer": "https://ggzy.hefei.gov.cn/"}, timeout=60)
            content_type = headers.get("Content-Type", "text/html")
            if not text.strip():
                raise RuntimeError("empty_body")
            if looks_like_block_page(text):
                raise PermissionError("anti_bot_interstitial")
            target_path.write_text(text, encoding="utf-8")
            append_jsonl(
                FETCH_ATTEMPTS_PATH,
                build_attempt_row(
                    run_id=args.run_id,
                    dataset="tender",
                    entity_id=project_id,
                    source_id="hefei_ggzy_detail_page",
                    asset_role="detail_html",
                    source_url=source_url,
                    resolved_url=source_url,
                    fetch_method="requests",
                    status="success",
                    content_type=content_type,
                    retrieved_at=retrieved_at,
                    storage_path=target_path.relative_to(ROOT).as_posix(),
                    sha256=sha256_file(target_path),
                    extra={"title": project_id, "record_role": "raw_original"},
                ),
            )
            success += 1
            continue
        except Exception as exc:  # noqa: BLE001
            request_status = "anti_bot_interstitial" if isinstance(exc, PermissionError) else classify_fetch_error(exc)

        browser_ok, browser_message = dump_dom_with_chrome(source_url, target_path, timeout_seconds=90)
        if browser_ok and target_path.exists():
            browser_text = target_path.read_text(encoding="utf-8", errors="ignore")
            if looks_like_block_page(browser_text):
                target_path.unlink(missing_ok=True)
                browser_ok = False
                browser_message = "anti_bot_interstitial"

        if browser_ok and target_path.exists():
            append_jsonl(
                FETCH_ATTEMPTS_PATH,
                build_attempt_row(
                    run_id=args.run_id,
                    dataset="tender",
                    entity_id=project_id,
                    source_id="hefei_ggzy_detail_page",
                    asset_role="detail_html",
                    source_url=source_url,
                    resolved_url=source_url,
                    fetch_method="browser_rendered",
                    status="success",
                    content_type="text/html",
                    retrieved_at=now_ts(),
                    storage_path=target_path.relative_to(ROOT).as_posix(),
                    sha256=sha256_file(target_path),
                    extra={"title": project_id, "record_role": "raw_original"},
                ),
            )
            success += 1
            continue

        append_jsonl(
            FETCH_ATTEMPTS_PATH,
            build_attempt_row(
                run_id=args.run_id,
                dataset="tender",
                entity_id=project_id,
                source_id="hefei_ggzy_detail_page",
                asset_role="detail_html",
                source_url=source_url,
                resolved_url=source_url,
                fetch_method="browser_rendered" if browser_message else "requests",
                status=browser_message if browser_message == "anti_bot_interstitial" else request_status if request_status != "fetch_error" else "manual_review_required",
                content_type="text/html",
                retrieved_at=now_ts(),
                storage_path="",
                sha256="",
                extra={"message": browser_message or request_status},
            ),
        )
        failure += 1

    print(json.dumps({"run_id": args.run_id, "success": success, "failure": failure}, ensure_ascii=False))


if __name__ == "__main__":
    main()
