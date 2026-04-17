import argparse
import json
from pathlib import Path
from urllib.parse import urlparse

from lib.fetch_utils import classify_fetch_error, get_bytes
from lib.hash_utils import sha1_text, sha256_file
from lib.io_utils import ROOT, append_jsonl, now_ts, read_json, read_jsonl, write_json
from lib.manifest_utils import build_attempt_row


FETCH_QUEUE_PATH = ROOT / "data" / "manifests" / "fetch_queue_v1.jsonl"
FETCH_ATTEMPTS_PATH = ROOT / "data" / "manifests" / "fetch_attempts.jsonl"
TENDER_OTHER_DIR = ROOT / "data" / "raw" / "tender" / "other"
TENDER_PDF_DIR = ROOT / "data" / "raw" / "tender" / "pdf"
TENDER_OTHER_RAW_DIR = ROOT / "data" / "raw" / "tender" / "other"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=f"tender_attach_{now_ts().replace(' ', '_').replace(':', '')}")
    parser.add_argument("--limit", type=int, default=20)
    return parser.parse_args()


def extract_attachment_rows(bundle_path: Path) -> list[dict[str, str]]:
    payload = read_json(bundle_path)
    rows = payload.get("raw_rows", []) if isinstance(payload, dict) else []
    attachments: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        bid_content = str(row.get("bid_content", "")).strip()
        if not bid_content:
            continue
        try:
            parsed = json.loads(bid_content, strict=False)
        except json.JSONDecodeError:
            continue
        for block in parsed.get("data", []):
            if not isinstance(block, dict):
                continue
            for attachment in block.get("attachments", []):
                if not isinstance(attachment, dict):
                    continue
                file_name = str(attachment.get("file_name", "")).strip()
                file_link = str(attachment.get("file_link", "")).strip()
                if file_link:
                    attachments.append({"file_name": file_name, "file_link": file_link})
    deduped: dict[str, dict[str, str]] = {}
    for item in attachments:
        deduped[item["file_link"]] = item
    return list(deduped.values())


def main() -> None:
    args = parse_args()
    tasks = [
        task
        for task in read_jsonl(FETCH_QUEUE_PATH)
        if task.get("dataset") == "tender" and task.get("asset_role") == "attachment_link_index"
    ]
    if args.limit > 0:
        tasks = tasks[: args.limit]

    success = 0
    failure = 0

    for task in tasks:
        project_id = str(task["entity_id"])
        bundle_path = TENDER_OTHER_DIR / f"{project_id}.json"
        retrieved_at = now_ts()
        if not bundle_path.exists():
            append_jsonl(
                FETCH_ATTEMPTS_PATH,
                build_attempt_row(
                    run_id=args.run_id,
                    dataset="tender",
                    entity_id=project_id,
                    source_id="hefei_ggzy_detail_page",
                    asset_role="attachment_link_index",
                    source_url=str(task.get("source_url", "")),
                    resolved_url="",
                    fetch_method="parse_bundle",
                    status="manual_review_required",
                    content_type="application/json",
                    retrieved_at=retrieved_at,
                    storage_path="",
                    sha256="",
                    extra={"message": "bundle_not_found", "record_role": "derived_support"},
                ),
            )
            failure += 1
            continue

        attachments = extract_attachment_rows(bundle_path)
        link_index_path = TENDER_OTHER_RAW_DIR / f"{project_id}__hefei_ggzy_attachment__attachment_link_index.json"
        write_json(
            link_index_path,
            {
                "dataset": "tender",
                "project_id": project_id,
                "source_id": "hefei_ggzy_attachment",
                "record_role": "derived_support",
                "attachments": attachments,
            },
        )
        append_jsonl(
            FETCH_ATTEMPTS_PATH,
            build_attempt_row(
                run_id=args.run_id,
                dataset="tender",
                entity_id=project_id,
                source_id="hefei_ggzy_detail_page",
                asset_role="attachment_link_index",
                source_url=str(task.get("source_url", "")),
                resolved_url=str(task.get("source_url", "")),
                fetch_method="parse_bundle",
                status="success" if attachments else "parse_link_fail",
                content_type="application/json",
                retrieved_at=retrieved_at,
                storage_path=link_index_path.relative_to(ROOT).as_posix(),
                sha256=sha256_file(link_index_path),
                extra={"message": "attachments_found" if attachments else "no_attachment_links", "record_role": "derived_support"},
            ),
        )

        for index, attachment in enumerate(attachments, start=1):
            url = attachment["file_link"]
            parsed = urlparse(url)
            suffix = Path(parsed.path).suffix.lower()
            if suffix not in {".pdf", ".doc", ".docx", ".xls", ".xlsx"}:
                suffix = ".pdf" if "pdf" in attachment["file_name"].lower() else ".bin"
            target_dir = TENDER_PDF_DIR if suffix == ".pdf" else TENDER_OTHER_RAW_DIR
            target_name = f"{project_id}__att{index:02d}__{sha1_text(url)[:8]}{suffix}"
            target_path = target_dir / target_name
            if target_path.exists():
                continue
            try:
                _, headers, payload = get_bytes(url, headers={"Referer": str(task.get("source_url", ""))}, timeout=60)
                if not payload:
                    raise RuntimeError("empty_body")
                target_path.write_bytes(payload)
                append_jsonl(
                    FETCH_ATTEMPTS_PATH,
                    build_attempt_row(
                        run_id=args.run_id,
                        dataset="tender",
                        entity_id=project_id,
                        source_id="hefei_ggzy_attachment",
                        asset_role="attachment_pdf" if suffix == ".pdf" else "attachment_other",
                        source_url=url,
                        resolved_url=url,
                        fetch_method="direct_file",
                        status="success",
                        content_type=headers.get("Content-Type", "application/octet-stream"),
                        retrieved_at=now_ts(),
                        storage_path=target_path.relative_to(ROOT).as_posix(),
                        sha256=sha256_file(target_path),
                        extra={"title": attachment["file_name"], "record_role": "raw_original"},
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                append_jsonl(
                    FETCH_ATTEMPTS_PATH,
                    build_attempt_row(
                        run_id=args.run_id,
                        dataset="tender",
                        entity_id=project_id,
                        source_id="hefei_ggzy_attachment",
                        asset_role="attachment_pdf" if suffix == ".pdf" else "attachment_other",
                        source_url=url,
                        resolved_url=url,
                        fetch_method="direct_file",
                        status=classify_fetch_error(exc) if str(exc) != "empty_body" else "empty_body",
                        content_type="application/octet-stream",
                        retrieved_at=now_ts(),
                        storage_path="",
                        sha256="",
                        extra={"title": attachment["file_name"], "message": str(exc)},
                    ),
                )

        if attachments:
            success += 1
        else:
            failure += 1

    print(json.dumps({"run_id": args.run_id, "success": success, "failure": failure}, ensure_ascii=False))


if __name__ == "__main__":
    main()
