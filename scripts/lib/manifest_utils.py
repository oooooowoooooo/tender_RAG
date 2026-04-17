from pathlib import Path

from .hash_utils import sha1_text, sha256_file
from .io_utils import relative_to_root


def build_task_id(dataset: str, entity_id: str, source_id: str, asset_role: str, source_url: str) -> str:
    seed = "|".join([dataset, entity_id, source_id, asset_role, source_url])
    return sha1_text(seed)


def build_attempt_row(
    *,
    run_id: str,
    dataset: str,
    entity_id: str,
    source_id: str,
    asset_role: str,
    source_url: str,
    resolved_url: str,
    fetch_method: str,
    status: str,
    content_type: str,
    retrieved_at: str,
    storage_path: str,
    sha256: str,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = {
        "run_id": run_id,
        "dataset": dataset,
        "entity_id": entity_id,
        "source_id": source_id,
        "asset_role": asset_role,
        "source_url": source_url,
        "resolved_url": resolved_url,
        "fetch_method": fetch_method,
        "status": status,
        "content_type": content_type,
        "retrieved_at": retrieved_at,
        "storage_path": storage_path,
        "sha256": sha256,
    }
    if extra:
        payload.update(extra)
    return payload


def build_raw_manifest_entry(
    *,
    path: Path,
    dataset: str,
    entity_id: str,
    title: str,
    source_id: str,
    asset_role: str,
    source_url: str,
    resolved_url: str,
    fetch_method: str,
    crawl_time: str,
    record_role: str = "raw_original",
) -> dict[str, object]:
    return {
        "source_url": source_url,
        "resolved_url": resolved_url,
        "source_type": source_id,
        "crawl_time": crawl_time,
        "local_path": relative_to_root(path),
        "sha256": sha256_file(path),
        "file_type": path.suffix.lstrip(".").lower() or "bin",
        "project_id_or_doc_id_or_credit_code": entity_id,
        "title": title,
        "dataset": dataset,
        "record_role": record_role,
        "asset_role": asset_role,
        "entity_id": entity_id,
        "source_id": source_id,
        "fetch_method": fetch_method,
        "storage_path": relative_to_root(path),
    }
