import argparse
import json
import time
from pathlib import Path

from lib.fetch_utils import post_form_json
from lib.hash_utils import sha1_text, sha256_file
from lib.io_utils import ROOT, append_jsonl, now_ts, read_json, write_json
from lib.manifest_utils import build_attempt_row


FETCH_ATTEMPTS_PATH = ROOT / "data" / "manifests" / "fetch_attempts.jsonl"
PAGE_DIR = ROOT / "data" / "raw" / "tender" / "other" / "official_history_api_pages"
CHECKPOINT_PATH = ROOT / "data" / "raw" / "tender" / "other" / "history_api_checkpoint.json"
API_URL = "https://www.ggzy.gov.cn/his/information/pubTradingInfo/getTradList"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=f"tender_history_api_{now_ts().replace(' ', '_').replace(':', '')}")
    parser.add_argument("--time-begin", default="2023-11-01")
    parser.add_argument("--time-end", default="2024-03-28")
    parser.add_argument("--page-start", type=int, default=1)
    parser.add_argument("--page-end", type=int, default=1)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    return parser.parse_args()


def checkpoint_payload() -> dict[str, object]:
    if CHECKPOINT_PATH.exists():
        return read_json(CHECKPOINT_PATH)
    return {"last_success_page": 0, "last_status": "never_run", "updated_at": ""}


def main() -> None:
    args = parse_args()
    checkpoint = checkpoint_payload()

    for page_no in range(args.page_start, args.page_end + 1):
        form_data = {
            "SOURCE_TYPE": "1",
            "DEAL_TIME": "06",
            "TIMEBEGIN": args.time_begin,
            "TIMEEND": args.time_end,
            "DEAL_PROVINCE": "340000",
            "DEAL_CITY": "340100",
            "PAGENUMBER": str(page_no),
        }
        retrieved_at = now_ts()
        try:
            response = post_form_json(API_URL, form_data, timeout=60)
            code = int(response.get("code", -1))
            if code != 200:
                status = "rate_limited" if code == 800 else "fetch_error"
                append_jsonl(
                    FETCH_ATTEMPTS_PATH,
                    build_attempt_row(
                        run_id=args.run_id,
                        dataset="tender",
                        entity_id=f"history_api_page_{page_no}",
                        source_id="national_history_api",
                        asset_role="history_api_page_json",
                        source_url=API_URL,
                        resolved_url=API_URL,
                        fetch_method="requests",
                        status=status,
                        content_type="application/json",
                        retrieved_at=retrieved_at,
                        storage_path="",
                        sha256="",
                        extra={"page_no": page_no, "request_params": form_data, "message": response.get("message", "")},
                    ),
                )
                checkpoint.update({"last_status": status, "updated_at": retrieved_at})
                write_json(CHECKPOINT_PATH, checkpoint)
                break

            file_name = f"history_api__page{page_no:04d}__{sha1_text(json.dumps(form_data, ensure_ascii=False))[:8]}.json"
            path = PAGE_DIR / file_name
            write_json(path, {"request_params": form_data, "response": response})
            append_jsonl(
                FETCH_ATTEMPTS_PATH,
                build_attempt_row(
                    run_id=args.run_id,
                    dataset="tender",
                    entity_id=f"history_api_page_{page_no}",
                    source_id="national_history_api",
                    asset_role="history_api_page_json",
                    source_url=API_URL,
                    resolved_url=API_URL,
                    fetch_method="requests",
                    status="success",
                    content_type="application/json",
                    retrieved_at=retrieved_at,
                    storage_path=path.relative_to(ROOT).as_posix(),
                    sha256=sha256_file(path),
                    extra={"page_no": page_no, "request_params": form_data, "record_role": "raw_original"},
                ),
            )
            checkpoint.update({"last_success_page": page_no, "last_status": "success", "updated_at": retrieved_at})
            write_json(CHECKPOINT_PATH, checkpoint)
            time.sleep(args.sleep_seconds)
        except Exception as exc:  # noqa: BLE001
            append_jsonl(
                FETCH_ATTEMPTS_PATH,
                build_attempt_row(
                    run_id=args.run_id,
                    dataset="tender",
                    entity_id=f"history_api_page_{page_no}",
                    source_id="national_history_api",
                    asset_role="history_api_page_json",
                    source_url=API_URL,
                    resolved_url=API_URL,
                    fetch_method="requests",
                    status="fetch_error",
                    content_type="application/json",
                    retrieved_at=retrieved_at,
                    storage_path="",
                    sha256="",
                    extra={"page_no": page_no, "request_params": form_data, "message": str(exc)},
                ),
            )
            checkpoint.update({"last_status": "fetch_error", "updated_at": retrieved_at})
            write_json(CHECKPOINT_PATH, checkpoint)
            break


if __name__ == "__main__":
    main()
