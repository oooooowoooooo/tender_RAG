import json
from pathlib import Path

from lib.io_utils import ROOT, read_json, write_jsonl
from lib.manifest_utils import build_task_id


FETCH_QUEUE_PATH = ROOT / "data" / "manifests" / "fetch_queue_v1.jsonl"
TENDER_OTHER_DIR = ROOT / "data" / "raw" / "tender" / "other"
TENDER_HTML_DIR = ROOT / "data" / "raw" / "tender" / "html"
ENTERPRISE_JSON_DIR = ROOT / "data" / "raw" / "enterprise" / "json"
ENTERPRISE_OTHER_DIR = ROOT / "data" / "raw" / "enterprise" / "other"


def load_tender_payloads() -> list[dict[str, object]]:
    payloads = []
    for path in sorted(TENDER_OTHER_DIR.glob("*.json")):
        if path.name.startswith("official_"):
            continue
        if "__" in path.stem:
            continue
        payload = read_json(path)
        if isinstance(payload, dict) and payload.get("project_id"):
            payloads.append(payload)
    return payloads


def load_enterprise_payloads() -> list[dict[str, object]]:
    payloads = []
    for path in sorted(ENTERPRISE_JSON_DIR.glob("*.json")):
        payload = read_json(path)
        if isinstance(payload, dict) and payload.get("entity_key"):
            payloads.append(payload)
    return payloads


def tender_detail_target(project_id: str, source_id: str) -> Path:
    return TENDER_HTML_DIR / f"{project_id}__{source_id}__detail.html"


def enterprise_raw_target(entity_id: str, source_id: str) -> Path:
    return ENTERPRISE_OTHER_DIR / f"{entity_id}__{source_id}__profile_raw.json"


def main() -> None:
    tasks: list[dict[str, object]] = []

    for payload in load_tender_payloads():
        project_id = str(payload["project_id"])
        normalized = payload.get("normalized_projection", {})
        source_url = ""
        if isinstance(normalized, dict):
            source_url = str(normalized.get("source_url", "")).strip()

        detail_path = tender_detail_target(project_id, "hefei_ggzy_detail_page")
        if not detail_path.exists():
            tasks.append(
                {
                    "task_id": build_task_id("tender", project_id, "hefei_ggzy_detail_page", "detail_html", source_url),
                    "dataset": "tender",
                    "entity_id": project_id,
                    "source_id": "hefei_ggzy_detail_page",
                    "asset_role": "detail_html",
                    "source_url": source_url,
                    "priority": 100,
                    "retry_count": 0,
                    "status": "pending",
                    "target_path": detail_path.relative_to(ROOT).as_posix(),
                }
            )

        attachment_index_path = TENDER_OTHER_DIR / f"{project_id}__hefei_ggzy_attachment__attachment_link_index.json"
        tasks.append(
            {
                "task_id": build_task_id("tender", project_id, "hefei_ggzy_detail_page", "attachment_link_index", source_url),
                "dataset": "tender",
                "entity_id": project_id,
                "source_id": "hefei_ggzy_detail_page",
                "asset_role": "attachment_link_index",
                "source_url": source_url,
                "priority": 90,
                "retry_count": 0,
                "status": "pending",
                "target_path": attachment_index_path.relative_to(ROOT).as_posix(),
            }
        )

    for payload in load_enterprise_payloads():
        entity_id = str(payload["entity_key"])
        target_path = enterprise_raw_target(entity_id, "enterprise_existing_profile_row")
        if target_path.exists():
            continue
        tasks.append(
            {
                "task_id": build_task_id("enterprise", entity_id, "enterprise_existing_profile_row", "source_raw_json", ""),
                "dataset": "enterprise",
                "entity_id": entity_id,
                "source_id": "enterprise_existing_profile_row",
                "asset_role": "source_raw_json",
                "source_url": "",
                "priority": 80,
                "retry_count": 0,
                "status": "pending",
                "target_path": target_path.relative_to(ROOT).as_posix(),
            }
        )

    tasks.sort(key=lambda item: (-int(item["priority"]), str(item["dataset"]), str(item["entity_id"]), str(item["asset_role"])))
    write_jsonl(FETCH_QUEUE_PATH, tasks)
    print(json.dumps({"task_count": len(tasks)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
