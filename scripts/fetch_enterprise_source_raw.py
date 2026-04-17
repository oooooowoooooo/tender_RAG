import argparse
import json

from lib.hash_utils import sha256_file
from lib.io_utils import ROOT, append_jsonl, now_ts, read_json, read_jsonl, write_json
from lib.manifest_utils import build_attempt_row


FETCH_QUEUE_PATH = ROOT / "data" / "manifests" / "fetch_queue_v1.jsonl"
FETCH_ATTEMPTS_PATH = ROOT / "data" / "manifests" / "fetch_attempts.jsonl"
ENTERPRISE_JSON_DIR = ROOT / "data" / "raw" / "enterprise" / "json"
ENTERPRISE_OTHER_DIR = ROOT / "data" / "raw" / "enterprise" / "other"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=f"enterprise_raw_{now_ts().replace(' ', '_').replace(':', '')}")
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tasks = [
        task
        for task in read_jsonl(FETCH_QUEUE_PATH)
        if task.get("dataset") == "enterprise" and task.get("asset_role") == "source_raw_json"
    ]
    if args.limit > 0:
        tasks = tasks[: args.limit]

    success = 0
    failure = 0

    for task in tasks:
        entity_id = str(task["entity_id"])
        source_path = ENTERPRISE_JSON_DIR / f"{entity_id}.json"
        target_path = ENTERPRISE_OTHER_DIR / f"{entity_id}__enterprise_existing_profile_row__profile_raw.json"
        retrieved_at = now_ts()

        if target_path.exists():
            continue

        if not source_path.exists():
            append_jsonl(
                FETCH_ATTEMPTS_PATH,
                build_attempt_row(
                    run_id=args.run_id,
                    dataset="enterprise",
                    entity_id=entity_id,
                    source_id="enterprise_existing_profile_row",
                    asset_role="source_raw_json",
                    source_url="",
                    resolved_url="",
                    fetch_method="copy_existing_row",
                    status="manual_review_required",
                    content_type="application/json",
                    retrieved_at=retrieved_at,
                    storage_path="",
                    sha256="",
                    extra={"message": "bundle_not_found"},
                ),
            )
            failure += 1
            continue

        payload = read_json(source_path)
        raw_profile = payload.get("raw_profile", {}) if isinstance(payload, dict) else {}
        title = ""
        normalized = payload.get("normalized_projection", {}) if isinstance(payload, dict) else {}
        if isinstance(normalized, dict):
            title = str(normalized.get("enterprise_name", "")).strip()

        if not raw_profile:
            append_jsonl(
                FETCH_ATTEMPTS_PATH,
                build_attempt_row(
                    run_id=args.run_id,
                    dataset="enterprise",
                    entity_id=entity_id,
                    source_id="enterprise_existing_profile_row",
                    asset_role="source_raw_json",
                    source_url="",
                    resolved_url="",
                    fetch_method="copy_existing_row",
                    status="empty_body",
                    content_type="application/json",
                    retrieved_at=retrieved_at,
                    storage_path="",
                    sha256="",
                    extra={"message": "raw_profile_missing", "title": title},
                ),
            )
            failure += 1
            continue

        write_json(
            target_path,
            {
                "dataset": "enterprise",
                "entity_id": entity_id,
                "source_id": "enterprise_existing_profile_row",
                "record_role": "raw_original",
                "title": title,
                "raw_profile": raw_profile,
            },
        )
        sha256 = sha256_file(target_path)
        append_jsonl(
            FETCH_ATTEMPTS_PATH,
            build_attempt_row(
                run_id=args.run_id,
                dataset="enterprise",
                entity_id=entity_id,
                source_id="enterprise_existing_profile_row",
                asset_role="source_raw_json",
                source_url="",
                resolved_url="",
                fetch_method="copy_existing_row",
                status="success",
                content_type="application/json",
                retrieved_at=retrieved_at,
                storage_path=target_path.relative_to(ROOT).as_posix(),
                sha256=sha256,
                extra={"title": title, "record_role": "raw_original"},
            ),
        )
        success += 1

    print(json.dumps({"run_id": args.run_id, "success": success, "failure": failure}, ensure_ascii=False))


if __name__ == "__main__":
    main()
