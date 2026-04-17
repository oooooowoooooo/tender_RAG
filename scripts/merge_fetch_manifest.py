import json
from pathlib import Path

from lib.io_utils import ROOT, append_jsonl, read_jsonl
from lib.manifest_utils import build_raw_manifest_entry


RAW_MANIFEST_PATH = ROOT / "data" / "manifests" / "raw_manifest.jsonl"
FETCH_ATTEMPTS_PATH = ROOT / "data" / "manifests" / "fetch_attempts.jsonl"


def existing_paths() -> set[str]:
    paths: set[str] = set()
    for row in read_jsonl(RAW_MANIFEST_PATH):
        local_path = str(row.get("local_path", "")).strip()
        storage_path = str(row.get("storage_path", "")).strip()
        if local_path:
            paths.add(local_path)
        if storage_path:
            paths.add(storage_path)
    return paths


def main() -> None:
    seen_paths = existing_paths()
    added = 0
    skipped = 0

    for row in read_jsonl(FETCH_ATTEMPTS_PATH):
        if str(row.get("status")) != "success":
            continue
        if str(row.get("record_role", "")) != "raw_original":
            continue

        storage_path = str(row.get("storage_path", "")).strip()
        if not storage_path or storage_path in seen_paths:
            skipped += 1
            continue

        absolute_path = ROOT / storage_path
        if not absolute_path.exists():
            skipped += 1
            continue

        entry = build_raw_manifest_entry(
            path=absolute_path,
            dataset=str(row.get("dataset", "")).strip(),
            entity_id=str(row.get("entity_id", "")).strip(),
            title=str(row.get("title", "")).strip() or str(row.get("entity_id", "")).strip(),
            source_id=str(row.get("source_id", "")).strip(),
            asset_role=str(row.get("asset_role", "")).strip(),
            source_url=str(row.get("source_url", "")).strip(),
            resolved_url=str(row.get("resolved_url", "")).strip() or str(row.get("source_url", "")).strip(),
            fetch_method=str(row.get("fetch_method", "")).strip(),
            crawl_time=str(row.get("retrieved_at", "")).strip(),
            record_role="raw_original",
        )
        append_jsonl(RAW_MANIFEST_PATH, entry)
        seen_paths.add(storage_path)
        added += 1

    print(json.dumps({"added": added, "skipped": skipped}, ensure_ascii=False))


if __name__ == "__main__":
    main()
