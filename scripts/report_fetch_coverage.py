import json
import re
from collections import Counter
from pathlib import Path

from lib.hash_utils import sha256_file
from lib.io_utils import ROOT, read_jsonl, write_json


RAW_MANIFEST_PATH = ROOT / "data" / "manifests" / "raw_manifest.jsonl"
RAW_MANIFEST_BASELINE_PATH = ROOT / "data" / "manifests" / "raw_manifest.baseline_phase1.jsonl"
FETCH_QUEUE_PATH = ROOT / "data" / "manifests" / "fetch_queue_v1.jsonl"
FETCH_ATTEMPTS_PATH = ROOT / "data" / "manifests" / "fetch_attempts.jsonl"
REPORTS_DIR = ROOT / "reports"
ATTACHMENT_RAW_PATTERN = re.compile(r"__att\d{2}__")
DERIVED_RAW_ATTEMPT_ROLES = {"attachment_pdf", "attachment_other", "history_api_page_json"}


def relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def count_jsonl_rows(path: Path) -> int:
    return len(read_jsonl(path))


def bundle_paths(directory: Path, suffix: str) -> list[Path]:
    return sorted([path for path in directory.glob(suffix) if path.is_file()])


def baseline_paths() -> set[str]:
    rows = read_jsonl(RAW_MANIFEST_BASELINE_PATH)
    return {
        str(row.get("local_path", "")).strip() or str(row.get("storage_path", "")).strip()
        for row in rows
        if str(row.get("local_path", "")).strip() or str(row.get("storage_path", "")).strip()
    }


def checksum_payload() -> dict[str, object]:
    paths = baseline_paths()
    return {
        "generated_at": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "deliverables": [
            {
                "path": "data/raw",
                "type": "directory",
                "baseline_manifest_file_count": len(paths),
            },
            {
                "path": "data/manifests/raw_manifest.baseline_phase1.jsonl",
                "type": "file",
                "sha256": sha256_file(RAW_MANIFEST_BASELINE_PATH),
                "line_count": count_jsonl_rows(RAW_MANIFEST_BASELINE_PATH),
            },
            {
                "path": "data/contracts/data_targets.json",
                "type": "file",
                "sha256": sha256_file(ROOT / "data" / "contracts" / "data_targets.json"),
            },
            {
                "path": "docs/retrieval_strategy.md",
                "type": "file",
                "sha256": sha256_file(ROOT / "docs" / "retrieval_strategy.md"),
            },
            {
                "path": "reports/coverage_report.md",
                "type": "file",
                "sha256": sha256_file(ROOT / "reports" / "coverage_report.md"),
            },
        ],
    }


def write_baseline_reports() -> None:
    raw_rows = read_jsonl(RAW_MANIFEST_BASELINE_PATH)
    paths = baseline_paths()
    dataset_counter = Counter(str(row.get("dataset", "")) for row in raw_rows)
    filetype_counter = Counter(str(row.get("file_type", "")) for row in raw_rows)

    def prefix_count(prefix: str) -> int:
        return sum(1 for path in paths if path.startswith(prefix))

    lines = [
        "# Baseline Phase 1 Inventory",
        "",
        f"- Baseline manifest: `{relative(RAW_MANIFEST_BASELINE_PATH)}`",
        f"- Baseline manifest rows: `{len(raw_rows)}`",
        f"- Raw files present at freeze: `{len(paths)}`",
        f"- Dataset counts: tender `{dataset_counter.get('tender', 0)}`, policy `{dataset_counter.get('policy', 0)}`, enterprise `{dataset_counter.get('enterprise', 0)}`",
        f"- File types: json `{filetype_counter.get('json', 0)}`, jsonl `{filetype_counter.get('jsonl', 0)}`, html `{filetype_counter.get('html', 0)}`",
        "",
        "## Raw Directory Snapshot",
        "",
        f"- `data/raw/tender/html`: `{prefix_count('data/raw/tender/html/')}`",
        f"- `data/raw/tender/pdf`: `{prefix_count('data/raw/tender/pdf/')}`",
        f"- `data/raw/tender/other`: `{prefix_count('data/raw/tender/other/')}`",
        f"- `data/raw/policy/html`: `{prefix_count('data/raw/policy/html/')}`",
        f"- `data/raw/policy/pdf`: `{prefix_count('data/raw/policy/pdf/')}`",
        f"- `data/raw/policy/other`: `{prefix_count('data/raw/policy/other/')}`",
        f"- `data/raw/enterprise/html`: `{prefix_count('data/raw/enterprise/html/')}`",
        f"- `data/raw/enterprise/json`: `{prefix_count('data/raw/enterprise/json/')}`",
        f"- `data/raw/enterprise/other`: `{prefix_count('data/raw/enterprise/other/')}`",
    ]
    write_text(REPORTS_DIR / "baseline_phase1_inventory.md", "\n".join(lines) + "\n")
    write_json(REPORTS_DIR / "baseline_phase1_checksums.json", checksum_payload())


def latest_attempts() -> tuple[dict[tuple[str, str, str, str, str], dict[str, object]], list[dict[str, object]]]:
    rows = read_jsonl(FETCH_ATTEMPTS_PATH)
    latest: dict[tuple[str, str, str, str, str], dict[str, object]] = {}
    for row in rows:
        key = (
            str(row.get("dataset", "")),
            str(row.get("entity_id", "")),
            str(row.get("source_id", "")),
            str(row.get("asset_role", "")),
            str(row.get("source_url", "")),
        )
        latest[key] = row
    return latest, rows


def phase15_report_text() -> str:
    tender_html_files = bundle_paths(ROOT / "data" / "raw" / "tender" / "html", "*.html")
    tender_pdf_files = bundle_paths(ROOT / "data" / "raw" / "tender" / "pdf", "*")
    tender_other_files = bundle_paths(ROOT / "data" / "raw" / "tender" / "other", "*")
    tender_bundle_files = [
        path
        for path in bundle_paths(ROOT / "data" / "raw" / "tender" / "other", "*.json")
        if not path.name.startswith("official_")
        and "__hefei_ggzy_attachment__attachment_link_index.json" not in path.name
        and not ATTACHMENT_RAW_PATTERN.search(path.name)
        and not path.name.startswith("history_api__")
        and path.name != "history_api_checkpoint.json"
    ]
    enterprise_bundle_files = bundle_paths(ROOT / "data" / "raw" / "enterprise" / "json", "*.json")
    enterprise_html_files = bundle_paths(ROOT / "data" / "raw" / "enterprise" / "html", "*.html")
    enterprise_other_files = bundle_paths(ROOT / "data" / "raw" / "enterprise" / "other", "*.json")
    link_index_files = [path for path in tender_other_files if path.name.endswith("__hefei_ggzy_attachment__attachment_link_index.json")]
    attachment_raw_ids = {
        path.name.split("__", 1)[0]
        for path in [*tender_pdf_files, *tender_other_files]
        if ATTACHMENT_RAW_PATTERN.search(path.name)
    }
    history_api_page_files = bundle_paths(ROOT / "data" / "raw" / "tender" / "other" / "official_history_api_pages", "*.json")
    queue_rows = read_jsonl(FETCH_QUEUE_PATH)
    latest, attempts = latest_attempts()

    queue_targets: dict[tuple[str, str, str, str, str], str] = {}
    for task in queue_rows:
        key = (
            str(task.get("dataset", "")),
            str(task.get("entity_id", "")),
            str(task.get("source_id", "")),
            str(task.get("asset_role", "")),
            str(task.get("source_url", "")),
        )
        queue_targets[key] = str(task.get("target_path", ""))

    materialized_attempt_keys = {
        key
        for key, row in latest.items()
        if str(row.get("status", "")) == "success" or str(row.get("storage_path", "")).strip()
    }
    derived_attempt_keys = {
        key
        for key, row in latest.items()
        if str(row.get("asset_role", "")).strip() in DERIVED_RAW_ATTEMPT_ROLES
    }
    all_task_keys = set(queue_targets) | materialized_attempt_keys | derived_attempt_keys
    success_by_task = 0
    failed_by_task = 0
    replay_skipped = 0
    manual_review_required = 0
    failed_with_reason = 0
    for key in all_task_keys:
        attempt = latest.get(key)
        target_path_value = queue_targets.get(key, "")
        target_path = ROOT / target_path_value if target_path_value else ROOT
        if attempt is None:
            if target_path_value and target_path.exists():
                replay_skipped += 1
            continue
        status = str(attempt.get("status", ""))
        if status == "success":
            success_by_task += 1
        else:
            failed_by_task += 1
            if status:
                failed_with_reason += 1
            if status == "manual_review_required":
                manual_review_required += 1
    pending_tasks = len(all_task_keys) - success_by_task - failed_by_task - replay_skipped

    history_attempts = [row for row in attempts if str(row.get("source_id")) == "national_history_api"]
    history_success = len(history_api_page_files)
    history_failed = sum(1 for row in history_attempts if str(row.get("status")) != "success")
    unresolved_counter = Counter(
        str(latest[key].get("status", ""))
        for key in all_task_keys
        if key in latest and str(latest[key].get("status", "")) != "success"
    )
    message_counter = Counter(
        str(latest[key].get("message", ""))
        for key in all_task_keys
        if key in latest and str(latest[key].get("status", "")) != "success" and str(latest[key].get("message", "")).strip()
    )
    entities_with_any_raw = {
        path.name.split("__", 1)[0]
        for path in [*enterprise_html_files, *enterprise_other_files]
    }

    lines = [
        "# Fetch Coverage Phase 1.5",
        "",
        "## Tender",
        "",
        f"- `projects_total`: `{len(tender_bundle_files)}`",
        f"- `projects_with_detail_html`: `{len({path.name.split('__', 1)[0] for path in tender_html_files})}`",
        f"- `projects_with_attachment_link_index`: `{len({path.name.split('__', 1)[0] for path in link_index_files})}`",
        f"- `projects_with_at_least_one_attachment_raw`: `{len(attachment_raw_ids)}`",
        f"- `history_api_pages_total`: `{len(history_api_page_files)}`",
        f"- `history_api_success_pages`: `{history_success}`",
        f"- `history_api_failed_pages`: `{history_failed}`",
        "",
        "## Enterprise",
        "",
        f"- `entities_total`: `{len(enterprise_bundle_files)}`",
        f"- `entities_with_profile_html`: `{len({path.name.split('__', 1)[0] for path in enterprise_html_files})}`",
        f"- `entities_with_source_raw_json`: `{len({path.name.split('__', 1)[0] for path in enterprise_other_files})}`",
        f"- `entities_without_any_official_raw`: `{len(enterprise_bundle_files) - len(entities_with_any_raw)}`",
        "",
        "## Process",
        "",
        f"- `total_tasks`: `{len(all_task_keys)}`",
        f"- `success_tasks`: `{success_by_task}`",
        f"- `failed_tasks`: `{failed_by_task}`",
        f"- `failed_tasks_with_reason_code`: `{failed_with_reason}`",
        f"- `replay_skipped_tasks`: `{replay_skipped}`",
        f"- `pending_tasks`: `{pending_tasks}`",
        f"- `manual_review_required_tasks`: `{manual_review_required}`",
        "",
        "## Raw Manifest",
        "",
        f"- `raw_manifest_rows`: `{count_jsonl_rows(RAW_MANIFEST_PATH)}`",
        "",
        "## Unresolved Gaps",
        "",
    ]

    if unresolved_counter:
        for status, count in unresolved_counter.most_common():
            lines.append(f"- `{status}`: `{count}`")
    else:
        lines.append("- No unresolved task statuses recorded.")

    lines.extend(["", "## Failure Messages", ""])
    if message_counter:
        for message, count in message_counter.most_common(10):
            lines.append(f"- `{message}`: `{count}`")
    else:
        lines.append("- No failure messages recorded.")

    return "\n".join(lines) + "\n"


def main() -> None:
    write_baseline_reports()
    write_text(REPORTS_DIR / "fetch_coverage_phase1_5.md", phase15_report_text())
    print(
        json.dumps(
            {
                "baseline_inventory": relative(REPORTS_DIR / "baseline_phase1_inventory.md"),
                "baseline_checksums": relative(REPORTS_DIR / "baseline_phase1_checksums.json"),
                "phase15_report": relative(REPORTS_DIR / "fetch_coverage_phase1_5.md"),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
