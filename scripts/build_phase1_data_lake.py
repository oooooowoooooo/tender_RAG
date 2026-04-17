from __future__ import annotations

import csv
import hashlib
import html
import json
import re
import shutil
import ssl
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
import urllib.parse
import urllib.request


csv.field_size_limit(1024 * 1024 * 512)

ROOT = Path(__file__).resolve().parents[1]

TENDER_SOURCE = ROOT / "data_new" / "02_tender" / "tender_docs_ahzb_curated.csv"
POLICY_DOC_SOURCE = ROOT / "data_new" / "01_policy" / "policy_curated_docs.csv"
POLICY_META_SOURCE = ROOT / "data_new" / "01_policy" / "policy_curated_meta.csv"
POLICY_LEGAL_SOURCE = ROOT / "data_new" / "01_policy" / "policy_src_legal_documents.csv"
ENTERPRISE_LOCAL_SOURCE = ROOT / "data_new" / "03_company" / "company_profiles_local_matched.csv"
ENTERPRISE_NATIONAL_SOURCE = ROOT / "data_new" / "03_company" / "company_profiles_national.csv"

DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
TENDER_RAW_DIR = RAW_DIR / "tender"
POLICY_RAW_DIR = RAW_DIR / "policy"
ENTERPRISE_RAW_DIR = RAW_DIR / "enterprise"

TENDER_HTML_DIR = TENDER_RAW_DIR / "html"
TENDER_PDF_DIR = TENDER_RAW_DIR / "pdf"
TENDER_OTHER_DIR = TENDER_RAW_DIR / "other"
TENDER_API_PAGE_DIR = TENDER_OTHER_DIR / "official_history_api_pages"
TENDER_API_RECORD_INDEX_PATH = TENDER_OTHER_DIR / "official_hefei_history_record_index.jsonl"
TENDER_API_MATCH_INDEX_PATH = TENDER_OTHER_DIR / "official_hefei_project_match_index.json"

POLICY_HTML_DIR = POLICY_RAW_DIR / "html"
POLICY_PDF_DIR = POLICY_RAW_DIR / "pdf"
POLICY_OTHER_DIR = POLICY_RAW_DIR / "other"

ENTERPRISE_HTML_DIR = ENTERPRISE_RAW_DIR / "html"
ENTERPRISE_JSON_DIR = ENTERPRISE_RAW_DIR / "json"
ENTERPRISE_OTHER_DIR = ENTERPRISE_RAW_DIR / "other"

MANIFEST_DIR = DATA_DIR / "manifests"
CONTRACT_DIR = DATA_DIR / "contracts"
DOCS_DIR = ROOT / "docs"
REPORTS_DIR = ROOT / "reports"

RAW_MANIFEST_PATH = MANIFEST_DIR / "raw_manifest.jsonl"
DATA_TARGETS_PATH = CONTRACT_DIR / "data_targets.json"
RETRIEVAL_STRATEGY_PATH = DOCS_DIR / "retrieval_strategy.md"
COVERAGE_REPORT_PATH = REPORTS_DIR / "coverage_report.md"

TARGET_TENDER_PROJECTS = 1000
TARGET_POLICY_DOCUMENTS = 1000
TARGET_ENTERPRISES = 1000

POLICY_OFFICIAL_FEEDS = [
    {
        "feed_name": "gov_cn_gwywj",
        "feed_url": "https://www.gov.cn/zhengce/zhengceku/gwywj/TONGYONGGAILAN.json",
        "policy_level": "national",
        "source_label": "中国政府网-国务院文件",
    },
    {
        "feed_name": "gov_cn_bmwj",
        "feed_url": "https://www.gov.cn/zhengce/zhengceku/bmwj/TONGYONGGAILAN.json",
        "policy_level": "national",
        "source_label": "中国政府网-国务院部门文件",
    },
]

TENDER_HISTORY_API_URL = "https://www.ggzy.gov.cn/his/information/pubTradingInfo/getTradList"
TENDER_HISTORY_API_BASE = "https://www.ggzy.gov.cn"
TENDER_HISTORY_CITY_CODE = "340100"
TENDER_HISTORY_PROVINCE_CODE = "340000"
TENDER_HISTORY_SOURCE_TYPE = "1"
TENDER_HISTORY_FETCH_SLEEP_SECONDS = 1.0

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
}
HTTP_TIMEOUT_SECONDS = 60
HTTP_RETRY_COUNT = 2
POLICY_FETCH_WORKERS = 8
POLICY_FETCH_BATCH_SIZE = 160
RAW_VALUE_UNLABELED = "未标注"
ATTACHMENT_SUFFIXES = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar")
SSL_CONTEXT = ssl._create_unverified_context()

TENDER_REQUIRED_FIELDS = [
    "project_code",
    "project_name",
    "business_type",
    "info_type",
    "publish_time",
    "region",
    "tenderer_or_purchaser",
    "agency",
    "budget_or_bid_amount",
    "opening_time",
    "source_platform",
    "source_url",
    "attachment_paths",
]

POLICY_REQUIRED_FIELDS = [
    "title",
    "issuer",
    "index_no",
    "subject_category",
    "doc_no",
    "publish_date",
    "validity_status",
    "policy_level",
    "source_url",
    "attachment_paths",
]

ENTERPRISE_REQUIRED_FIELDS = [
    "unified_social_credit_code",
    "enterprise_name",
    "legal_representative",
    "entity_type",
    "established_date",
    "registration_authority",
    "registered_capital",
    "business_status",
    "registered_address",
    "business_scope",
    "source_url",
]

INVALID_NAME_TOKENS = {
    "",
    "-",
    "/",
    "无",
    "None",
    "详见项目招标公告",
    "详见招标公告",
}

DOC_NO_PATTERN = re.compile(
    r"([A-Za-z\u4e00-\u9fa5]{1,30}〔\d{4}〕\d{1,5}号|[A-Za-z\u4e00-\u9fa5]{1,30}\[\d{4}\]\d{1,5}号)"
)


def ensure_dirs() -> None:
    for path in (
        TENDER_HTML_DIR,
        TENDER_PDF_DIR,
        TENDER_OTHER_DIR,
        TENDER_API_PAGE_DIR,
        POLICY_HTML_DIR,
        POLICY_PDF_DIR,
        POLICY_OTHER_DIR,
        ENTERPRISE_HTML_DIR,
        ENTERPRISE_JSON_DIR,
        ENTERPRISE_OTHER_DIR,
        MANIFEST_DIR,
        CONTRACT_DIR,
        DOCS_DIR,
        REPORTS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def clean_generated_dir(path: Path, suffixes: tuple[str, ...]) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_file() and child.suffix.lower() in suffixes:
            child.unlink()


def clean_generated_tree(path: Path, suffixes: tuple[str, ...]) -> None:
    if not path.exists():
        return
    for child in path.rglob("*"):
        if child.is_file() and child.suffix.lower() in suffixes:
            child.unlink()


def reset_output_dirs() -> None:
    clean_generated_dir(TENDER_OTHER_DIR, (".json",))
    for path in (TENDER_API_RECORD_INDEX_PATH, TENDER_API_MATCH_INDEX_PATH):
        if path.exists():
            path.unlink()
    clean_generated_dir(POLICY_OTHER_DIR, (".json",))
    clean_generated_dir(POLICY_HTML_DIR, (".html",))
    clean_generated_dir(ENTERPRISE_JSON_DIR, (".json",))
    if RAW_MANIFEST_PATH.exists():
        RAW_MANIFEST_PATH.unlink()


def norm(value: str | None) -> str:
    return (value or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def clean_key(key: str | None) -> str:
    return (key or "").lstrip("\ufeff")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for row in reader:
            rows.append({clean_key(key): value or "" for key, value in row.items()})
    return rows


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def write_bytes(path: Path, payload: bytes) -> None:
    path.write_bytes(payload)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    cleaned = cleaned.strip("._")
    return cleaned or "item"


def parse_datetime(value: str | None) -> datetime:
    text = norm(value)
    if not text:
        return datetime.min
    candidates = (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S.%f",
        "%m-%d-%Y",
        "%m-%d-%Y %H:%M:%S",
    )
    for candidate in candidates:
        try:
            return datetime.strptime(text, candidate)
        except ValueError:
            continue
    return datetime.min


def relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def unique_non_empty(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = norm(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def split_subject_names(value: str) -> list[str]:
    text = norm(value).replace("；", ";").replace("、", ";")
    names = []
    for part in text.split(";"):
        cleaned = norm(part)
        if cleaned and cleaned not in INVALID_NAME_TOKENS:
            names.append(cleaned)
    return names


def first_non_empty(values: list[str]) -> str:
    for value in values:
        cleaned = norm(value)
        if cleaned and cleaned != "None":
            return cleaned
    return ""


def pick_project_title(rows: list[dict[str, str]]) -> str:
    titles = [row.get("procurement_title", "") for row in rows]
    titles.extend(row.get("project_name", "") for row in rows)
    return first_non_empty(titles)


def extract_doc_no(text: str) -> str:
    match = DOC_NO_PATTERN.search(text)
    return match.group(1) if match else ""


def normalize_tender_title(value: str | None) -> str:
    text = norm(value)
    if not text:
        return ""
    text = text.replace("\u3000", "").replace(" ", "")
    text = re.sub(r"[【】\[\]（）()“”\"'《》<>]", "", text)
    text = re.sub(r"（第.+?次）$", "", text)
    suffixes = [
        "中标成交结果公告",
        "中标成交公告",
        "中标结果公告",
        "中标公告",
        "成交结果公告",
        "成交公告",
        "结果公告",
        "采购合同",
        "合同公告",
        "终止公告",
        "更正公告",
        "变更公告",
        "澄清公告",
        "补遗",
        "补充公告",
        "答疑公告",
        "竞争性磋商公告",
        "竞争性谈判公告",
        "询价公告",
        "招标公告",
        "采购公告",
        "招标资审公告",
        "采购资审公告",
        "资格预审公告",
        "资审公告",
        "招标资审文件澄清",
        "采购文件更正",
        "采购资审文件澄清",
        "招标文件澄清",
        "交易结果公示",
        "中标候选人公示",
        "开标记录",
        "异常公告",
    ]
    changed = True
    while changed:
        changed = False
        for suffix in suffixes:
            if text.endswith(suffix):
                text = text[: -len(suffix)]
                changed = True
    text = re.sub(r"[\-—_:：,，。；;、]+$", "", text)
    return text


def extract_titles_from_bid_content(raw_value: str | None) -> list[str]:
    text = norm(raw_value)
    if not text:
        return []
    titles = []
    for matched in re.findall(r'"title"\s*:\s*"([^"]+)"', text):
        cleaned = norm(matched).replace("\\n", " ")
        if cleaned:
            titles.append(cleaned)
    return unique_non_empty(titles)


def build_tender_project_title_candidates(payload: dict[str, object]) -> list[str]:
    candidates: list[str] = []
    candidates.append(str(payload.get("project_title", "")))
    projection = payload.get("normalized_projection", {})
    if isinstance(projection, dict):
        project_name = projection.get("project_name", "")
        if isinstance(project_name, str):
            candidates.append(project_name)
    for row in payload.get("raw_rows", []):
        if not isinstance(row, dict):
            continue
        candidates.append(str(row.get("procurement_title", "")))
        candidates.append(str(row.get("project_name", "")))
        candidates.extend(extract_titles_from_bid_content(str(row.get("bid_content", ""))))
    normalized_candidates = []
    for candidate in candidates:
        normalized = normalize_tender_title(candidate)
        if normalized:
            normalized_candidates.append(normalized)
    return unique_non_empty(normalized_candidates)


def has_coverage_value(value: object) -> bool:
    if isinstance(value, list):
        return bool(value)
    text = norm("" if value is None else str(value))
    return bool(text and text != RAW_VALUE_UNLABELED)


def normalize_policy_date(value: str | None) -> str:
    text = norm(value)
    if not text:
        return ""
    matched = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
    if matched:
        year, month, day = matched.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    matched = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", text)
    if matched:
        year, month, day = matched.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    parsed = parse_datetime(text)
    if parsed != datetime.min:
        return parsed.strftime("%Y-%m-%d")
    return text[:10] if len(text) >= 10 else text


def strip_html_text(fragment: str) -> str:
    text = fragment or ""
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", "", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|li|tr|h[1-6]|table)>", "\n", text)
    text = re.sub(r"(?i)</td>", "\t", text)
    text = re.sub(r"(?is)<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ").replace("\u3000", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_policy_label(label: str) -> str:
    text = strip_html_text(label)
    text = text.replace("：", "").replace(":", "")
    text = text.replace("\xa0", "").replace("\u3000", "").replace(" ", "")
    return text


def extract_meta_value(html_text: str, meta_name: str) -> str:
    patterns = [
        rf'<meta[^>]+name=["\']{re.escape(meta_name)}["\'][^>]+content=["\'](.*?)["\']',
        rf'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']{re.escape(meta_name)}["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text, re.I | re.S)
        if match:
            return strip_html_text(match.group(1))
    return ""


def extract_policy_metadata_pairs(html_text: str) -> dict[str, str]:
    target_labels = {"索引号", "主题分类", "发文机关", "成文日期", "标题", "发文字号", "发布日期", "来源", "公文种类"}
    pairs: dict[str, str] = {}

    for row_html in re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", html_text):
        cells = re.findall(r"(?is)<t[dh][^>]*>(.*?)</t[dh]>", row_html)
        if len(cells) < 2:
            continue
        for index in range(0, len(cells) - 1, 2):
            label = normalize_policy_label(cells[index])
            value = strip_html_text(cells[index + 1])
            if label in target_labels and value and label not in pairs:
                pairs[label] = value

    for label_html, value_html in re.findall(r"(?is)<li[^>]*>\s*<span[^>]*>(.*?)</span>\s*<span[^>]*>(.*?)</span>\s*</li>", html_text):
        label = normalize_policy_label(label_html)
        value = strip_html_text(value_html)
        if label in target_labels and value and label not in pairs:
            pairs[label] = value

    return pairs


def extract_div_inner_html_by_id(html_text: str, element_id: str) -> str:
    marker = f'id="{element_id}"'
    index = html_text.find(marker)
    if index < 0:
        marker = f"id='{element_id}'"
        index = html_text.find(marker)
    if index < 0:
        return ""

    open_start = html_text.rfind("<div", 0, index)
    open_end = html_text.find(">", index)
    if open_start < 0 or open_end < 0:
        return ""

    depth = 1
    cursor = open_end + 1
    while depth > 0:
        next_open = html_text.find("<div", cursor)
        next_close = html_text.find("</div", cursor)
        if next_close < 0:
            return html_text[open_end + 1 :]
        if next_open >= 0 and next_open < next_close:
            next_open_end = html_text.find(">", next_open)
            if next_open_end < 0:
                return html_text[open_end + 1 : next_close]
            depth += 1
            cursor = next_open_end + 1
        else:
            depth -= 1
            if depth == 0:
                return html_text[open_end + 1 : next_close]
            cursor = next_close + len("</div>")
    return ""


def extract_policy_body(html_text: str) -> str:
    body_html = extract_div_inner_html_by_id(html_text, "UCAP-CONTENT")
    if not body_html:
        match = re.search(r'(?is)<div[^>]+class="[^"]*trs_editor_view[^"]*"[^>]*>(.*?)</div>', html_text)
        if match:
            body_html = match.group(1)
    body_text = strip_html_text(body_html)
    if body_text:
        return body_text
    return extract_meta_value(html_text, "description")


def extract_attachment_urls(html_text: str, page_url: str) -> list[str]:
    attachments: list[str] = []
    for href in re.findall(r'(?is)href=["\']([^"\']+)["\']', html_text):
        candidate = html.unescape(href).strip()
        if not candidate:
            continue
        normalized_href = candidate.lower().split("#", 1)[0].split("?", 1)[0]
        if not normalized_href.endswith(ATTACHMENT_SUFFIXES):
            continue
        attachments.append(urllib.parse.urljoin(page_url, candidate))
    return unique_non_empty(attachments)


def policy_identity_tokens(projection: dict[str, object]) -> list[str]:
    title = norm(str(projection.get("title", "")))
    issuer = norm(str(projection.get("issuer", "")))
    doc_no = norm(str(projection.get("doc_no", "")))
    publish_date = norm(str(projection.get("publish_date", "")))
    source_url = norm(str(projection.get("source_url", "")))

    tokens: list[str] = []
    if source_url and source_url != RAW_VALUE_UNLABELED:
        tokens.append(f"url::{source_url}")
    if issuer and doc_no and issuer != RAW_VALUE_UNLABELED and doc_no != RAW_VALUE_UNLABELED:
        tokens.append(f"issuer_doc::{issuer}::{doc_no}")
    if issuer and title and issuer != RAW_VALUE_UNLABELED:
        tokens.append(f"issuer_title::{issuer}::{title}")
    if title and publish_date and publish_date != RAW_VALUE_UNLABELED:
        tokens.append(f"title_date::{title}::{publish_date}")
    elif title:
        tokens.append(f"title::{title}")
    return tokens


def is_duplicate_policy_projection(projection: dict[str, object], seen_tokens: set[str]) -> bool:
    return any(token in seen_tokens for token in policy_identity_tokens(projection))


def register_policy_projection(projection: dict[str, object], seen_tokens: set[str]) -> None:
    for token in policy_identity_tokens(projection):
        seen_tokens.add(token)


def http_get_bytes(url: str) -> bytes:
    last_error: Exception | None = None
    for attempt in range(HTTP_RETRY_COUNT + 1):
        request = urllib.request.Request(url, headers=HTTP_HEADERS)
        try:
            with urllib.request.urlopen(request, context=SSL_CONTEXT, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return response.read()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= HTTP_RETRY_COUNT:
                break
            time.sleep(1 + attempt)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"failed to fetch url: {url}")


def fetch_json_payload(url: str) -> list[dict[str, object]]:
    content = http_get_bytes(url).decode("utf-8-sig", "ignore")
    payload = json.loads(content)
    return payload if isinstance(payload, list) else []


def http_post_form_json(url: str, form_data: dict[str, str]) -> dict[str, object]:
    encoded = urllib.parse.urlencode(form_data).encode("utf-8")
    last_error: Exception | None = None
    for attempt in range(HTTP_RETRY_COUNT + 1):
        request = urllib.request.Request(
            url,
            data=encoded,
            headers={
                **HTTP_HEADERS,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        try:
            with urllib.request.urlopen(request, context=SSL_CONTEXT, timeout=HTTP_TIMEOUT_SECONDS) as response:
                content = response.read().decode("utf-8-sig", "ignore")
                payload = json.loads(content)
                return payload if isinstance(payload, dict) else {"code": -1, "message": "non-dict payload"}
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= HTTP_RETRY_COUNT:
                break
            time.sleep(1 + attempt)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"failed to post form: {url}")


def build_official_policy_doc_id(url: str) -> str:
    stem = Path(urllib.parse.urlparse(url).path).stem
    safe_stem = safe_name(stem or hashlib.sha256(url.encode("utf-8")).hexdigest()[:16])
    return f"policy_official_{safe_stem}"


def collect_official_policy_candidates() -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for feed in POLICY_OFFICIAL_FEEDS:
        for row in fetch_json_payload(feed["feed_url"]):
            title = norm(str(row.get("TITLE", "")))
            source_url = norm(str(row.get("URL", "")))
            if not title or not source_url or source_url in seen_urls:
                continue
            seen_urls.add(source_url)
            candidates.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "publish_date": normalize_policy_date(str(row.get("DOCRELPUBTIME", ""))),
                    "feed_name": feed["feed_name"],
                    "policy_level": feed["policy_level"],
                    "source_label": feed["source_label"],
                }
            )

    candidates.sort(
        key=lambda item: (
            parse_datetime(item["publish_date"]),
            item["title"],
        ),
        reverse=True,
    )
    return candidates


def build_existing_policy_payloads() -> list[dict[str, object]]:
    docs = read_csv_rows(POLICY_DOC_SOURCE)
    meta_rows = read_csv_rows(POLICY_META_SOURCE)
    legal_rows = read_csv_rows(POLICY_LEGAL_SOURCE)

    meta_by_id = {norm(row.get("id")): row for row in meta_rows}
    legal_by_title = {norm(row.get("title")): row for row in legal_rows}

    payloads: list[dict[str, object]] = []
    for row in docs:
        doc_id = norm(row.get("id"))
        meta = meta_by_id.get(doc_id, {})
        legal = legal_by_title.get(norm(row.get("title")), {})
        title = first_non_empty([row.get("title", ""), meta.get("title", ""), RAW_VALUE_UNLABELED])
        content = norm(row.get("content"))
        source_url = first_non_empty([row.get("url", ""), meta.get("source_url", ""), meta.get("document_url", ""), RAW_VALUE_UNLABELED])
        issuer = first_non_empty([row.get("source", ""), meta.get("source_name", ""), RAW_VALUE_UNLABELED])
        publish_date = normalize_policy_date(first_non_empty([meta.get("publish_date", ""), row.get("publish_time", ""), legal.get("publish_date", "")]))
        validity_status = first_non_empty([legal.get("status", ""), RAW_VALUE_UNLABELED])
        policy_level = first_non_empty([meta.get("region_level", ""), legal.get("authority_level", ""), RAW_VALUE_UNLABELED])
        doc_no = first_non_empty([extract_doc_no("\n".join([title, content])), RAW_VALUE_UNLABELED])
        subject_category = first_non_empty([meta.get("category", ""), legal.get("category", ""), RAW_VALUE_UNLABELED])

        normalized_projection = {
            "title": title,
            "issuer": issuer,
            "index_no": RAW_VALUE_UNLABELED,
            "subject_category": subject_category,
            "doc_no": doc_no,
            "publish_date": publish_date or RAW_VALUE_UNLABELED,
            "validity_status": validity_status,
            "policy_level": policy_level,
            "source_url": source_url,
            "attachment_paths": [],
            "keywords": [],
            "effective_date": "",
            "abolish_date": "",
            "summary": "",
        }

        payloads.append(
            {
                "dataset": "policy",
                "count_unit": "document",
                "doc_id": doc_id,
                "normalized_projection": normalized_projection,
                "raw_doc": row,
                "raw_meta": meta,
                "raw_legal_doc": legal,
            }
        )

    return payloads


def fetch_official_policy_result(candidate: dict[str, str]) -> dict[str, object]:
    try:
        source_url = candidate["source_url"]
        html_bytes = http_get_bytes(source_url)
        html_text = html_bytes.decode("utf-8", "ignore")
        metadata_pairs = extract_policy_metadata_pairs(html_text)
        body_text = extract_policy_body(html_text)
        subject_category = first_non_empty([metadata_pairs.get("主题分类", ""), RAW_VALUE_UNLABELED])
        title = first_non_empty([metadata_pairs.get("标题", ""), candidate["title"], RAW_VALUE_UNLABELED])
        issuer = first_non_empty([metadata_pairs.get("发文机关", ""), RAW_VALUE_UNLABELED])
        source_name = first_non_empty([metadata_pairs.get("来源", ""), candidate["source_label"], RAW_VALUE_UNLABELED])
        publish_date = first_non_empty(
            [
                normalize_policy_date(metadata_pairs.get("发布日期", "")),
                normalize_policy_date(extract_meta_value(html_text, "firstpublishedtime")),
                candidate["publish_date"],
                RAW_VALUE_UNLABELED,
            ]
        )
        effective_date = normalize_policy_date(metadata_pairs.get("成文日期", ""))
        doc_no = first_non_empty([metadata_pairs.get("发文字号", ""), extract_doc_no(body_text), RAW_VALUE_UNLABELED])
        index_no = first_non_empty([metadata_pairs.get("索引号", ""), RAW_VALUE_UNLABELED])
        attachment_urls = extract_attachment_urls(html_text, source_url)
        summary = first_non_empty([extract_meta_value(html_text, "description"), body_text[:240]])
        doc_id = build_official_policy_doc_id(source_url)

        normalized_projection = {
            "title": title,
            "issuer": issuer,
            "index_no": index_no,
            "subject_category": subject_category,
            "doc_no": doc_no,
            "publish_date": publish_date,
            "validity_status": RAW_VALUE_UNLABELED,
            "policy_level": first_non_empty([candidate["policy_level"], RAW_VALUE_UNLABELED]),
            "source_url": first_non_empty([source_url, RAW_VALUE_UNLABELED]),
            "attachment_paths": attachment_urls,
            "keywords": unique_non_empty(re.split(r"[、，,;；/\\\s]+", subject_category if subject_category != RAW_VALUE_UNLABELED else "")),
            "effective_date": effective_date,
            "abolish_date": "",
            "summary": summary,
        }

        raw_doc = {
            "id": doc_id,
            "title": title,
            "content": body_text,
            "source": source_name,
            "publish_time": publish_date,
            "url": source_url,
            "category": subject_category,
            "project_type": "",
            "region": candidate["policy_level"],
            "region_level": candidate["policy_level"],
            "source_type": candidate["feed_name"],
            "corpus_tag": "official_policy_html",
            "word_count": str(len(body_text)),
        }

        raw_meta = {
            "id": doc_id,
            "title": title,
            "source_name": source_name,
            "source_type": candidate["feed_name"],
            "region_level": candidate["policy_level"],
            "publish_date": publish_date,
            "source_url": source_url,
            "document_url": source_url,
            "source_feed": candidate["feed_name"],
            "metadata_pairs": metadata_pairs,
            "attachment_urls": attachment_urls,
        }

        payload = {
            "dataset": "policy",
            "count_unit": "document",
            "doc_id": doc_id,
            "normalized_projection": normalized_projection,
            "raw_doc": raw_doc,
            "raw_meta": raw_meta,
            "raw_legal_doc": {},
        }

        return {
            "ok": True,
            "payload": payload,
            "html_bytes": html_bytes,
            "source_url": source_url,
            "title": title,
            "feed_name": candidate["feed_name"],
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "title": candidate.get("title", ""),
            "source_url": candidate.get("source_url", ""),
            "feed_name": candidate.get("feed_name", ""),
            "error": f"{type(exc).__name__}: {exc}",
        }


def collect_official_policy_results(existing_payloads: list[dict[str, object]]) -> tuple[list[dict[str, object]], dict[str, object]]:
    seen_tokens: set[str] = set()
    for payload in existing_payloads:
        register_policy_projection(payload["normalized_projection"], seen_tokens)

    candidates = collect_official_policy_candidates()
    target_needed = max(TARGET_POLICY_DOCUMENTS - len(existing_payloads), 0)
    selected_results: list[dict[str, object]] = []
    selected_source_counter = Counter()
    fetch_failures = 0
    skipped_as_duplicate = 0
    fetched_attempts = 0
    cursor = 0

    while len(selected_results) < target_needed and cursor < len(candidates):
        batch: list[dict[str, str]] = []
        while len(batch) < POLICY_FETCH_BATCH_SIZE and cursor < len(candidates):
            candidate = candidates[cursor]
            cursor += 1
            preview_projection = {
                "title": candidate["title"],
                "issuer": "",
                "doc_no": "",
                "publish_date": candidate["publish_date"],
                "source_url": candidate["source_url"],
            }
            if is_duplicate_policy_projection(preview_projection, seen_tokens):
                skipped_as_duplicate += 1
                continue
            batch.append(candidate)

        if not batch:
            continue

        batch_results: list[dict[str, object]] = []
        with ThreadPoolExecutor(max_workers=POLICY_FETCH_WORKERS) as executor:
            future_map = {executor.submit(fetch_official_policy_result, candidate): candidate for candidate in batch}
            for future in as_completed(future_map):
                fetched_attempts += 1
                batch_results.append(future.result())

        batch_results.sort(
            key=lambda item: (
                parse_datetime(
                    str(
                        item.get("payload", {})
                        .get("normalized_projection", {})
                        .get("publish_date", "")
                    )
                ),
                str(item.get("title", "")),
            ),
            reverse=True,
        )

        for result in batch_results:
            if not result.get("ok"):
                fetch_failures += 1
                continue
            payload = result["payload"]
            projection = payload["normalized_projection"]
            if is_duplicate_policy_projection(projection, seen_tokens):
                skipped_as_duplicate += 1
                continue
            register_policy_projection(projection, seen_tokens)
            selected_results.append(result)
            selected_source_counter[str(result.get("feed_name", "unknown"))] += 1
            if len(selected_results) >= target_needed:
                break

    return selected_results, {
        "official_feed_candidate_total": len(candidates),
        "official_target_needed": target_needed,
        "official_selected_count": len(selected_results),
        "official_fetch_attempts": fetched_attempts,
        "official_fetch_failures": fetch_failures,
        "official_duplicate_skips": skipped_as_duplicate,
        "official_source_counter": dict(selected_source_counter),
    }


def summarize_policy_payloads(payloads: list[dict[str, object]]) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    required_field_coverage = Counter()
    region_level_counter = Counter()
    source_type_counter = Counter()

    for payload in payloads:
        projection = payload["normalized_projection"]
        checks = {
            "title": projection["title"],
            "issuer": projection["issuer"],
            "index_no": projection["index_no"],
            "subject_category": projection["subject_category"],
            "doc_no": projection["doc_no"],
            "publish_date": projection["publish_date"],
            "validity_status": projection["validity_status"],
            "policy_level": projection["policy_level"],
            "source_url": projection["source_url"],
            "attachment_paths": projection["attachment_paths"],
        }
        for key, value in checks.items():
            if has_coverage_value(value):
                required_field_coverage[key] += 1

        region_level_counter[norm(str(projection.get("policy_level", ""))) or "unknown"] += 1

        raw_meta = payload.get("raw_meta", {})
        if isinstance(raw_meta, dict):
            source_type = norm(str(raw_meta.get("source_type", ""))) or "unknown"
        else:
            source_type = "unknown"
        source_type_counter[source_type] += 1

    return dict(required_field_coverage), dict(region_level_counter), dict(source_type_counter)


def build_manifest_entry(
    path: Path,
    source_url: str,
    source_type: str,
    object_key: str,
    title: str,
    file_type: str,
    crawl_time: str,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = {
        "source_url": source_url,
        "source_type": source_type,
        "crawl_time": crawl_time,
        "local_path": relative(path),
        "sha256": sha256_file(path),
        "file_type": file_type,
        "project_id_or_doc_id_or_credit_code": object_key,
        "title": title,
    }
    if extra:
        payload.update(extra)
    return payload


def build_tender_raw(crawl_time: str) -> tuple[list[dict[str, object]], dict[str, object], set[str], Counter[str], Counter[str]]:
    rows = read_csv_rows(TENDER_SOURCE)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    blank_project_rows = 0
    all_name_counter: Counter[str] = Counter()

    for row in rows:
        project_id = norm(row.get("project_id"))
        for key in ("purchaser", "agency", "bid_company"):
            for name in split_subject_names(row.get(key, "")):
                all_name_counter[name] += 1
        if not project_id:
            blank_project_rows += 1
            continue
        grouped[project_id].append(row)

    sorted_projects = sorted(
        grouped.items(),
        key=lambda item: (
            max(parse_datetime(row.get("event_time")) for row in item[1]),
            len({norm(row.get("link")) for row in item[1] if norm(row.get("link"))}),
            len(item[1]),
        ),
        reverse=True,
    )
    selected_projects = sorted_projects[:TARGET_TENDER_PROJECTS]
    selected_project_ids = {project_id for project_id, _ in selected_projects}

    manifest_entries: list[dict[str, object]] = []
    selected_name_counter: Counter[str] = Counter()
    selected_row_count = 0
    projects_with_multiple_rows = 0
    projects_with_multiple_links = 0
    projects_with_procurement = 0
    projects_with_bid = 0
    projects_with_attachments = 0
    stage_counter: Counter[str] = Counter()
    required_field_coverage = Counter()

    for project_id, project_rows in selected_projects:
        selected_row_count += len(project_rows)
        title = pick_project_title(project_rows)
        unique_links = unique_non_empty([row.get("link", "") for row in project_rows])
        stages = unique_non_empty([row.get("stage", "") for row in project_rows])
        purchasers = unique_non_empty([name for row in project_rows for name in split_subject_names(row.get("purchaser", ""))])
        agencies = unique_non_empty([name for row in project_rows for name in split_subject_names(row.get("agency", ""))])
        winners = unique_non_empty([name for row in project_rows for name in split_subject_names(row.get("bid_company", ""))])
        budget_values = unique_non_empty([row.get("budget_amount", "") for row in project_rows])
        bid_values = unique_non_empty([row.get("bid_amount", "") for row in project_rows])
        regions = unique_non_empty(
            [
                " / ".join(part for part in [norm(row.get("province")), norm(row.get("city")), norm(row.get("town"))] if part)
                for row in project_rows
            ]
        )
        primary_url = unique_links[0] if unique_links else ""
        publish_time = max(parse_datetime(row.get("event_time")) for row in project_rows)
        has_procurement = any(norm(row.get("procurement_content")) for row in project_rows)
        has_bid = any(norm(row.get("bid_content")) for row in project_rows)
        has_attachment = any("attach" in norm(row.get("bid_content")).lower() for row in project_rows)

        if len(project_rows) > 1:
            projects_with_multiple_rows += 1
        if len(unique_links) > 1:
            projects_with_multiple_links += 1
        if has_procurement:
            projects_with_procurement += 1
        if has_bid:
            projects_with_bid += 1
        if has_attachment:
            projects_with_attachments += 1
        for stage in stages:
            stage_counter[stage or "unlabeled"] += 1

        for name in purchasers + agencies + winners:
            selected_name_counter[name] += 1

        normalized_record = {
            "project_code": project_id,
            "project_name": title,
            "business_type": unique_non_empty([row.get("type", "") for row in project_rows]),
            "info_type": stages,
            "publish_time": publish_time.strftime("%Y-%m-%d %H:%M:%S") if publish_time != datetime.min else "",
            "region": regions,
            "tenderer_or_purchaser": purchasers,
            "agency": agencies,
            "budget_or_bid_amount": unique_non_empty(budget_values + bid_values),
            "opening_time": "",
            "source_platform": "安徽合肥公共资源交易电子服务系统",
            "source_url": primary_url,
            "attachment_paths": [],
            "candidate_company_names": winners,
            "winner_company_name": winners[:1],
            "contact_phone": "",
            "qualification_requirements": "",
            "procurement_method": unique_non_empty([row.get("type", "") for row in project_rows]),
        }

        checks = {
            "project_code": project_id,
            "project_name": title,
            "business_type": normalized_record["business_type"],
            "info_type": stages,
            "publish_time": normalized_record["publish_time"],
            "region": regions,
            "tenderer_or_purchaser": purchasers,
            "agency": agencies,
            "budget_or_bid_amount": normalized_record["budget_or_bid_amount"],
            "opening_time": "",
            "source_platform": normalized_record["source_platform"],
            "source_url": primary_url,
            "attachment_paths": [],
        }
        for key, value in checks.items():
            if value:
                required_field_coverage[key] += 1

        payload = {
            "dataset": "tender",
            "count_unit": "project",
            "project_id": project_id,
            "project_title": title,
            "lifecycle_summary": {
                "row_count": len(project_rows),
                "unique_link_count": len(unique_links),
                "stages": stages,
                "has_procurement_content": has_procurement,
                "has_bid_content": has_bid,
                "has_attachment_hint": has_attachment,
            },
            "normalized_projection": normalized_record,
            "raw_rows": sorted(
                project_rows,
                key=lambda row: (
                    parse_datetime(row.get("event_time")),
                    norm(row.get("link")),
                    norm(row.get("bid_company")),
                ),
            ),
        }

        path = TENDER_OTHER_DIR / f"{safe_name(project_id)}.json"
        write_json(path, payload)
        manifest_entries.append(
            build_manifest_entry(
                path=path,
                source_url=primary_url,
                source_type="tender_project_bundle",
                object_key=project_id,
                title=title or project_id,
                file_type="json",
                crawl_time=crawl_time,
                extra={"dataset": "tender", "link_count": len(unique_links), "row_count": len(project_rows)},
            )
        )

    stats = {
        "target_count": TARGET_TENDER_PROJECTS,
        "count_unit": "project",
        "raw_row_total": len(rows),
        "blank_project_id_rows": blank_project_rows,
        "unique_project_total": len(grouped),
        "packaged_project_count": len(selected_projects),
        "packaged_row_count": selected_row_count,
        "projects_with_multiple_rows": projects_with_multiple_rows,
        "projects_with_multiple_links": projects_with_multiple_links,
        "projects_with_procurement_content": projects_with_procurement,
        "projects_with_bid_content": projects_with_bid,
        "projects_with_attachment_hints": projects_with_attachments,
        "selected_unique_subject_names": len(selected_name_counter),
        "required_field_coverage": dict(required_field_coverage),
        "stage_counter": dict(stage_counter),
    }
    return manifest_entries, stats, selected_project_ids, selected_name_counter, all_name_counter


def load_selected_tender_project_payloads() -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for path in sorted(TENDER_OTHER_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        payloads.append({"path": path, "payload": payload})
    return payloads


def fetch_tender_history_api_page(form_data: dict[str, str]) -> dict[str, object]:
    last_message = ""
    for attempt in range(8):
        response = http_post_form_json(TENDER_HISTORY_API_URL, form_data)
        code = response.get("code")
        if code == 200:
            return response
        message = norm(str(response.get("message", ""))) or f"api code {code}"
        last_message = message
        if code == 800 or "系统繁忙" in message or "稍后再试" in message:
            time.sleep(min(30, 5 + attempt * 3))
            continue
        raise RuntimeError(message)
    raise RuntimeError(last_message or "tender history api failed")


def build_tender_official_history_raw(crawl_time: str) -> tuple[list[dict[str, object]], dict[str, object]]:
    selected_payloads = load_selected_tender_project_payloads()
    if not selected_payloads:
        return [], {
            "official_history_time_begin": "",
            "official_history_time_end": "",
            "official_history_page_count": 0,
            "official_history_cached_page_count": 0,
            "official_history_failed_page_count": 0,
            "official_history_raw_record_count": 0,
            "official_history_unique_record_count": 0,
            "official_history_match_project_count": 0,
            "official_history_unmatched_project_count": 0,
            "official_history_matched_record_count": 0,
            "official_history_info_type_counter": {},
            "official_history_match_method_counter": {},
            "official_history_is_partial": False,
        }

    publish_times = []
    for item in selected_payloads:
        payload = item["payload"]
        projection = payload.get("normalized_projection", {})
        if isinstance(projection, dict):
            parsed = parse_datetime(str(projection.get("publish_time", "")))
            if parsed != datetime.min:
                publish_times.append(parsed)
    time_begin = min(publish_times).strftime("%Y-%m-%d") if publish_times else "2023-11-01"
    time_end = (max(publish_times) + timedelta(days=90)).strftime("%Y-%m-%d") if publish_times else "2024-03-31"

    manifest_entries: list[dict[str, object]] = []
    page_payloads: list[tuple[int, Path, dict[str, object]]] = []
    request_base = {
        "SOURCE_TYPE": TENDER_HISTORY_SOURCE_TYPE,
        "DEAL_TIME": "06",
        "TIMEBEGIN": time_begin,
        "TIMEEND": time_end,
        "DEAL_PROVINCE": TENDER_HISTORY_PROVINCE_CODE,
        "DEAL_CITY": TENDER_HISTORY_CITY_CODE,
    }
    file_prefix = f"hefei_history_{time_begin.replace('-', '')}_{time_end.replace('-', '')}_page_"

    cached_pages: dict[int, tuple[Path, dict[str, object]]] = {}
    for path in sorted(TENDER_API_PAGE_DIR.glob(f"{file_prefix}*.json")):
        match = re.search(r"page_(\d+)\.json$", path.name)
        if not match:
            continue
        page_number = int(match.group(1))
        try:
            cached_payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        response_payload = cached_payload.get("response", cached_payload)
        if isinstance(response_payload, dict):
            cached_pages[page_number] = (path, response_payload)

    def persist_page(page_number: int, form_data: dict[str, str], response_payload: dict[str, object]) -> tuple[Path, dict[str, object]]:
        filename = f"{file_prefix}{page_number:04d}.json"
        path = TENDER_API_PAGE_DIR / filename
        write_json(path, {"request": form_data, "response": response_payload})
        return path, response_payload

    first_form = {**request_base, "PAGENUMBER": "1"}
    if 1 in cached_pages:
        first_path, first_response = cached_pages[1]
    else:
        first_path, first_response = persist_page(1, first_form, fetch_tender_history_api_page(first_form))
        cached_pages[1] = (first_path, first_response)

    total_pages = int(((first_response.get("data") or {}).get("pages")) or 0)
    if total_pages <= 0:
        total_pages = 1

    failed_pages: list[int] = []
    api_locked = False
    for page_number in range(1, total_pages + 1):
        form_data = {**request_base, "PAGENUMBER": str(page_number)}
        if page_number in cached_pages:
            path, response_payload = cached_pages[page_number]
        else:
            if api_locked:
                failed_pages.append(page_number)
                continue
            time.sleep(TENDER_HISTORY_FETCH_SLEEP_SECONDS)
            try:
                path, response_payload = persist_page(page_number, form_data, fetch_tender_history_api_page(form_data))
                cached_pages[page_number] = (path, response_payload)
            except RuntimeError as exc:
                failed_pages.append(page_number)
                if "系统繁忙" in str(exc) or "稍后再试" in str(exc):
                    api_locked = True
                continue
        page_payloads.append((page_number, path, response_payload))
        manifest_entries.append(
            build_manifest_entry(
                path=path,
                source_url=TENDER_HISTORY_API_URL,
                source_type="tender_official_history_api_page",
                object_key=f"hefei_page_{page_number:04d}",
                title=f"Hefei official tender history api page {page_number}",
                file_type="json",
                crawl_time=crawl_time,
                extra={"dataset": "tender", "page_number": page_number, "api": True},
            )
        )

    raw_record_total = 0
    info_type_counter: Counter[str] = Counter()
    unique_records: dict[str, dict[str, object]] = {}
    title_group: dict[str, list[dict[str, object]]] = defaultdict(list)

    for page_number, path, response_payload in page_payloads:
        records = ((response_payload.get("data") or {}).get("records")) or []
        for item in records:
            if not isinstance(item, dict):
                continue
            raw_record_total += 1
            key = norm(str(item.get("id") or item.get("url") or f"{page_number}_{raw_record_total}"))
            if key in unique_records:
                continue
            record = dict(item)
            detail_url = urllib.parse.urljoin(TENDER_HISTORY_API_BASE, str(record.get("url", "")))
            record["detail_full_url"] = detail_url
            record["raw_page_local_path"] = relative(path)
            record["normalized_title"] = normalize_tender_title(str(record.get("title", "")))
            info_type_counter[norm(str(record.get("informationTypeText", ""))) or "unknown"] += 1
            unique_records[key] = record
            if record["normalized_title"]:
                title_group[record["normalized_title"]].append(record)

    unique_record_rows = sorted(
        unique_records.values(),
        key=lambda item: (
            parse_datetime(str(item.get("publishTime", ""))),
            str(item.get("title", "")),
            str(item.get("id", "")),
        ),
        reverse=True,
    )
    write_jsonl(TENDER_API_RECORD_INDEX_PATH, unique_record_rows)
    manifest_entries.append(
        build_manifest_entry(
            path=TENDER_API_RECORD_INDEX_PATH,
            source_url=TENDER_HISTORY_API_URL,
            source_type="tender_official_history_record_index",
            object_key="hefei_history_record_index",
            title="Hefei official tender history record index",
            file_type="jsonl",
            crawl_time=crawl_time,
            extra={"dataset": "tender", "api": True, "record_count": len(unique_record_rows)},
        )
    )

    title_keys = list(title_group.keys())
    project_matches: list[dict[str, object]] = []
    matched_project_count = 0
    matched_record_ids: set[str] = set()
    match_method_counter: Counter[str] = Counter()

    for item in selected_payloads:
        payload = item["payload"]
        project_id = norm(str(payload.get("project_id", "")))
        candidate_titles = build_tender_project_title_candidates(payload)
        matched: dict[str, dict[str, object]] = {}
        match_method = ""

        for candidate in candidate_titles:
            for record in title_group.get(candidate, []):
                record_id = norm(str(record.get("id", ""))) or norm(str(record.get("detail_full_url", "")))
                matched[record_id] = record
        if matched:
            match_method = "exact_normalized_title"

        if not matched:
            for candidate in candidate_titles:
                if len(candidate) < 8:
                    continue
                contained_keys = [key for key in title_keys if candidate in key or key in candidate]
                if 0 < len(contained_keys) <= 3:
                    for key in contained_keys:
                        for record in title_group[key]:
                            record_id = norm(str(record.get("id", ""))) or norm(str(record.get("detail_full_url", "")))
                            matched[record_id] = record
            if matched:
                match_method = "contained_normalized_title"

        matched_rows = sorted(
            matched.values(),
            key=lambda row: (
                parse_datetime(str(row.get("publishTime", ""))),
                str(row.get("informationTypeText", "")),
                str(row.get("title", "")),
            ),
        )

        if matched_rows:
            matched_project_count += 1
            match_method_counter[match_method or "unknown"] += 1
            for row in matched_rows:
                matched_record_ids.add(norm(str(row.get("id", ""))) or norm(str(row.get("detail_full_url", ""))))

        project_matches.append(
            {
                "project_id": project_id,
                "project_title": payload.get("project_title", ""),
                "candidate_titles": candidate_titles,
                "match_method": match_method,
                "matched_record_count": len(matched_rows),
                "matched_official_records": [
                    {
                        "id": row.get("id", ""),
                        "title": row.get("title", ""),
                        "publishTime": row.get("publishTime", ""),
                        "informationTypeText": row.get("informationTypeText", ""),
                        "transactionSourcesPlatformText": row.get("transactionSourcesPlatformText", ""),
                        "detail_full_url": row.get("detail_full_url", ""),
                        "raw_page_local_path": row.get("raw_page_local_path", ""),
                    }
                    for row in matched_rows
                ],
            }
        )

    match_payload = {
        "dataset": "tender",
        "source": "ggzy.gov.cn historical api",
        "time_window": {
            "begin": time_begin,
            "end": time_end,
        },
        "project_count": len(selected_payloads),
        "matched_project_count": matched_project_count,
        "unmatched_project_count": len(selected_payloads) - matched_project_count,
        "projects": project_matches,
    }
    write_json(TENDER_API_MATCH_INDEX_PATH, match_payload)
    manifest_entries.append(
        build_manifest_entry(
            path=TENDER_API_MATCH_INDEX_PATH,
            source_url=TENDER_HISTORY_API_URL,
            source_type="tender_official_project_match_index",
            object_key="hefei_project_match_index",
            title="Hefei official tender project match index",
            file_type="json",
            crawl_time=crawl_time,
            extra={"dataset": "tender", "api": True, "matched_projects": matched_project_count},
        )
    )

    stats = {
        "official_history_time_begin": time_begin,
        "official_history_time_end": time_end,
        "official_history_page_count": len(page_payloads),
        "official_history_cached_page_count": len(page_payloads),
        "official_history_failed_page_count": len(failed_pages),
        "official_history_raw_record_count": raw_record_total,
        "official_history_unique_record_count": len(unique_record_rows),
        "official_history_match_project_count": matched_project_count,
        "official_history_unmatched_project_count": len(selected_payloads) - matched_project_count,
        "official_history_matched_record_count": len(matched_record_ids),
        "official_history_info_type_counter": dict(info_type_counter),
        "official_history_match_method_counter": dict(match_method_counter),
        "official_history_is_partial": bool(failed_pages),
    }
    return manifest_entries, stats


def build_policy_raw(crawl_time: str) -> tuple[list[dict[str, object]], dict[str, object]]:
    existing_payloads = build_existing_policy_payloads()
    official_results, official_stats = collect_official_policy_results(existing_payloads)

    manifest_entries: list[dict[str, object]] = []
    for result in official_results:
        payload = result["payload"]
        doc_id = payload["doc_id"]
        html_path = POLICY_HTML_DIR / f"{safe_name(doc_id)}.html"
        html_path.write_bytes(result["html_bytes"])
        payload["raw_meta"]["html_local_path"] = relative(html_path)
        manifest_entries.append(
            build_manifest_entry(
                path=html_path,
                source_url=str(payload["normalized_projection"]["source_url"]),
                source_type="policy_html_official",
                object_key=doc_id,
                title=str(payload["normalized_projection"]["title"] or doc_id),
                file_type="html",
                crawl_time=crawl_time,
                extra={"dataset": "policy", "feed_name": result.get("feed_name", "unknown")},
            )
        )

    merged_docs = existing_payloads + [result["payload"] for result in official_results]
    merged_docs.sort(
        key=lambda item: (
            parse_datetime(str(item["normalized_projection"]["publish_date"])),
            str(item["normalized_projection"]["title"]),
        ),
        reverse=True,
    )

    for payload in merged_docs:
        doc_id = str(payload["doc_id"] or safe_name(str(payload["normalized_projection"]["title"])))
        path = POLICY_OTHER_DIR / f"{safe_name(doc_id)}.json"
        write_json(path, payload)
        manifest_entries.append(
            build_manifest_entry(
                path=path,
                source_url=str(payload["normalized_projection"]["source_url"]),
                source_type="policy_document_bundle",
                object_key=doc_id,
                title=str(payload["normalized_projection"]["title"] or doc_id),
                file_type="json",
                crawl_time=crawl_time,
                extra={"dataset": "policy"},
            )
        )

    html_sample_count = copy_policy_html_samples(crawl_time, manifest_entries)
    required_field_coverage, region_level_counter, source_type_counter = summarize_policy_payloads(merged_docs)

    stats = {
        "target_count": TARGET_POLICY_DOCUMENTS,
        "count_unit": "document",
        "available_document_total": len(existing_payloads),
        "official_feed_candidate_total": official_stats["official_feed_candidate_total"],
        "official_target_needed": official_stats["official_target_needed"],
        "official_selected_count": official_stats["official_selected_count"],
        "official_fetch_attempts": official_stats["official_fetch_attempts"],
        "official_fetch_failures": official_stats["official_fetch_failures"],
        "official_duplicate_skips": official_stats["official_duplicate_skips"],
        "official_source_counter": official_stats["official_source_counter"],
        "packaged_document_count": len(merged_docs),
        "gap_to_target": max(TARGET_POLICY_DOCUMENTS - len(merged_docs), 0),
        "region_level_counter": region_level_counter,
        "source_type_counter": source_type_counter,
        "required_field_coverage": required_field_coverage,
        "html_sample_count": html_sample_count,
        "official_html_count": len(official_results),
    }
    return manifest_entries, stats


def copy_policy_html_samples(crawl_time: str, manifest_entries: list[dict[str, object]]) -> int:
    sample_dir = ROOT / "acceptance_assets" / "raw_sources" / "01_policy" / "html_samples"
    sample_manifest_path = ROOT / "acceptance_assets" / "raw_sources" / "original_file_samples_manifest.json"
    source_url_by_name: dict[str, str] = {}

    if sample_manifest_path.exists():
        sample_manifest = json.loads(sample_manifest_path.read_text(encoding="utf-8"))
        for item in sample_manifest.get("policy_html_samples", []):
            if item.get("status") == "success":
                source_url_by_name[Path(item["path"]).name] = item.get("url", "")

    copied = 0
    if not sample_dir.exists():
        return copied

    for source in sample_dir.glob("*.html"):
        target = POLICY_HTML_DIR / source.name
        shutil.copy2(source, target)
        manifest_entries.append(
            build_manifest_entry(
                path=target,
                source_url=source_url_by_name.get(source.name, ""),
                source_type="policy_html_sample",
                object_key=target.stem,
                title=target.stem,
                file_type="html",
                crawl_time=crawl_time,
                extra={"dataset": "policy", "sample": True},
            )
        )
        copied += 1
    return copied


def build_enterprise_indexes() -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    local_index: dict[str, dict[str, str]] = {}
    national_index: dict[str, dict[str, str]] = {}

    for row in read_csv_rows(ENTERPRISE_LOCAL_SOURCE):
        if norm(row.get("match_status")) != "成功":
            continue
        name = norm(row.get("company_name"))
        if name and name not in local_index:
            local_index[name] = row

    for row in read_csv_rows(ENTERPRISE_NATIONAL_SOURCE):
        name = norm(row.get("title"))
        if name and name not in national_index:
            national_index[name] = row
    return local_index, national_index


def build_enterprise_raw(
    crawl_time: str,
    selected_project_ids: set[str],
    selected_name_counter: Counter[str],
    all_name_counter: Counter[str],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    local_index, national_index = build_enterprise_indexes()
    tender_rows = read_csv_rows(TENDER_SOURCE)

    project_ids_by_name: dict[str, set[str]] = defaultdict(set)
    roles_by_name: dict[str, set[str]] = defaultdict(set)
    for row in tender_rows:
        project_id = norm(row.get("project_id"))
        for role_key, role_label in (("purchaser", "purchaser"), ("agency", "agency"), ("bid_company", "winner_or_supplier")):
            for name in split_subject_names(row.get(role_key, "")):
                if project_id:
                    project_ids_by_name[name].add(project_id)
                roles_by_name[name].add(role_label)

    candidate_names = sorted(
        all_name_counter,
        key=lambda name: (selected_name_counter.get(name, 0), all_name_counter.get(name, 0), name),
        reverse=True,
    )

    selected_entities: list[dict[str, object]] = []
    manifest_entries: list[dict[str, object]] = []
    direct_selected_matches = 0
    local_count = 0
    national_count = 0
    required_field_coverage = Counter()

    for name in candidate_names:
        if len(selected_entities) >= TARGET_ENTERPRISES:
            break

        source = ""
        raw_profile: dict[str, str] | None = None
        if name in local_index:
            source = "local_exact_match"
            raw_profile = local_index[name]
            local_count += 1
        elif name in national_index:
            source = "national_exact_match"
            raw_profile = national_index[name]
            national_count += 1
        else:
            continue

        if selected_name_counter.get(name, 0) > 0:
            direct_selected_matches += 1

        company_name = norm(raw_profile.get("company_name")) or norm(raw_profile.get("title")) or name
        uscc = norm(raw_profile.get("unified_social_credit_code")) or norm(raw_profile.get("USCC"))
        source_url = norm(raw_profile.get("source_url"))
        if source_url == "-":
            source_url = ""

        normalized_projection = {
            "unified_social_credit_code": uscc,
            "enterprise_name": company_name,
            "legal_representative": norm(raw_profile.get("legal_representative")) or norm(raw_profile.get("corporation")),
            "entity_type": norm(raw_profile.get("company_type")) or norm(raw_profile.get("type")),
            "established_date": norm(raw_profile.get("establishment_date")) or norm(raw_profile.get("event_time")),
            "registration_authority": "",
            "registered_capital": norm(raw_profile.get("registered_capital")) or norm(raw_profile.get("amount_raw")) or norm(raw_profile.get("capital_value")),
            "business_status": norm(raw_profile.get("business_status")) or norm(raw_profile.get("status")),
            "registered_address": norm(raw_profile.get("registered_address")) or norm(raw_profile.get("location")),
            "business_scope": norm(raw_profile.get("business_scope")) or norm(raw_profile.get("content")),
            "source_url": source_url,
            "administrative_licenses": [],
            "administrative_penalties": [],
            "annual_reports": [],
            "spot_checks": [],
            "abnormal_operations": [],
            "serious_illegal_list": [],
        }

        checks = {
            "unified_social_credit_code": normalized_projection["unified_social_credit_code"],
            "enterprise_name": normalized_projection["enterprise_name"],
            "legal_representative": normalized_projection["legal_representative"],
            "entity_type": normalized_projection["entity_type"],
            "established_date": normalized_projection["established_date"],
            "registration_authority": normalized_projection["registration_authority"],
            "registered_capital": normalized_projection["registered_capital"],
            "business_status": normalized_projection["business_status"],
            "registered_address": normalized_projection["registered_address"],
            "business_scope": normalized_projection["business_scope"],
            "source_url": normalized_projection["source_url"],
        }
        for key, value in checks.items():
            if value:
                required_field_coverage[key] += 1

        linked_project_ids = sorted(project_ids_by_name.get(name, set()))
        selected_project_hits = sorted(pid for pid in linked_project_ids if pid in selected_project_ids)

        entity_key = uscc or company_name
        payload = {
            "dataset": "enterprise",
            "count_unit": "entity",
            "entity_key": entity_key,
            "selection_source": source,
            "normalized_projection": normalized_projection,
            "tender_linkage": {
                "selected_project_frequency": selected_name_counter.get(name, 0),
                "all_tender_frequency": all_name_counter.get(name, 0),
                "selected_project_ids": selected_project_hits,
                "all_linked_project_ids": linked_project_ids[:50],
                "roles": sorted(roles_by_name.get(name, set())),
            },
            "raw_profile": raw_profile,
        }

        path = ENTERPRISE_JSON_DIR / f"{safe_name(entity_key)}.json"
        write_json(path, payload)
        manifest_entries.append(
            build_manifest_entry(
                path=path,
                source_url=source_url,
                source_type=source,
                object_key=entity_key,
                title=company_name or entity_key,
                file_type="json",
                crawl_time=crawl_time,
                extra={"dataset": "enterprise", "selected_project_frequency": selected_name_counter.get(name, 0)},
            )
        )
        selected_entities.append(payload)

    stats = {
        "target_count": TARGET_ENTERPRISES,
        "count_unit": "entity",
        "all_tender_related_names": len(all_name_counter),
        "selected_project_related_names": len(selected_name_counter),
        "matched_entity_candidates": sum(1 for name in all_name_counter if name in local_index or name in national_index),
        "packaged_entity_count": len(selected_entities),
        "direct_matches_from_selected_projects": direct_selected_matches,
        "backfill_from_full_tender_corpus": len(selected_entities) - direct_selected_matches,
        "local_exact_match_count": local_count,
        "national_exact_match_count": national_count,
        "required_field_coverage": dict(required_field_coverage),
    }
    return manifest_entries, stats


def build_data_targets() -> None:
    payload = {
        "version": "v1",
        "tender": {
            "target_count": 1000,
            "count_unit": "project",
            "source_scope": [
                "全国公共资源交易平台-安徽/合肥相关项目",
                "安徽合肥公共资源交易电子服务系统",
            ],
            "raw_formats": ["html", "pdf"],
            "packaged_raw_formats": ["json"],
            "dedupe_keys": ["source_platform", "project_code", "notice_url_hash"],
            "required_fields": [
                "project_code",
                "project_name",
                "business_type",
                "info_type",
                "publish_time",
                "region",
                "tenderer_or_purchaser",
                "agency",
                "budget_or_bid_amount",
                "opening_time",
                "source_platform",
                "source_url",
                "attachment_paths",
            ],
            "optional_fields": [
                "candidate_company_names",
                "winner_company_name",
                "contact_phone",
                "qualification_requirements",
                "procurement_method",
            ],
        },
        "policy": {
            "target_count": 1000,
            "count_unit": "document",
            "source_scope": [
                "国务院/国家部委",
                "安徽省政府信息公开",
                "合肥政务公开",
            ],
            "raw_formats": ["html", "pdf"],
            "packaged_raw_formats": ["json", "html"],
            "dedupe_keys": ["issuer", "doc_no", "title_hash"],
            "required_fields": [
                "title",
                "issuer",
                "index_no",
                "subject_category",
                "doc_no",
                "publish_date",
                "validity_status",
                "policy_level",
                "source_url",
                "attachment_paths",
            ],
            "optional_fields": [
                "keywords",
                "effective_date",
                "abolish_date",
                "summary",
            ],
        },
        "enterprise": {
            "target_count": 1000,
            "count_unit": "entity",
            "source_scope": [
                "国家企业信用信息公示系统-安徽",
                "从招投标主体反查得到的企业",
            ],
            "raw_formats": ["html", "json"],
            "packaged_raw_formats": ["json"],
            "dedupe_keys": ["unified_social_credit_code", "enterprise_name"],
            "required_fields": [
                "unified_social_credit_code",
                "enterprise_name",
                "legal_representative",
                "entity_type",
                "established_date",
                "registration_authority",
                "registered_capital",
                "business_status",
                "registered_address",
                "business_scope",
                "source_url",
            ],
            "optional_fields": [
                "administrative_licenses",
                "administrative_penalties",
                "annual_reports",
                "spot_checks",
                "abnormal_operations",
                "serious_illegal_list",
            ],
        },
    }
    write_json(DATA_TARGETS_PATH, payload)


def build_retrieval_strategy() -> None:
    content = """# Retrieval Strategy

## 招标信息：混合检索

招标数据同时带强结构字段和长文本正文。

- `project_code`、地区、时间、预算、采购人、代理机构，优先走 SQL / metadata filter。
- 标题、正文、资格条件、评分办法、公告术语，优先走 BM25。
- 附件和长篇招标文件，在第二阶段切块后再上向量最合适。

结论：

- 第一阶段主路径：`SQL / metadata filter + BM25`
- 第二阶段补充：`vector retrieval`
- 项目生命周期是主键组织方式，不再按孤立页面做主计数

## 政策信息：元数据过滤 + BM25 为主，向量为辅

政策检索里，标题、文号、发文机关、有效性、层级，优先级高于语义相近。

- 官方公开系统天然支持标题 / 全文检索和核心元数据组合检索。
- 因此结构化元数据必须保留，不能只靠向量。
- BM25 负责标题、文号、关键词、条款术语命中。
- 向量只作为问答解释和语义改写补充。

结论：

- 主路径：`metadata filter + BM25`
- 辅路径：`vector retrieval`
- 计数单位固定为文档，不再拿条文数替代文档数

## 企业信息：SQL / 精确检索为主，不把向量当主检索

企业画像本质是实体检索。

- 统一社会信用代码、企业名称、法定代表人、登记机关、成立日期、状态，都是强结构字段。
- 许可、处罚、年报、抽查检查，也都是围绕实体主键展开的结构化记录。
- 只有在后续引入年报正文、处罚文书长文本时，向量才有辅助位置。

结论：

- 主路径：`SQL / exact match + 关键词过滤`
- 辅路径：`vector retrieval` 仅用于长文本阶段
- 企业库优先由招标主体反向长出，再和政策库做关联

## 三库关系

- 招标库：按 `project_code` 聚合项目生命周期
- 企业库：从招标主体反查并按 `unified_social_credit_code` 聚合
- 政策库：按国家 / 安徽 / 合肥三层组织，并保留有效性、文号、发文机关

目标不是先堆页面数量，而是先搭出一层可追溯、可去重、可关联的原始数据湖底座。
"""
    write_text(RETRIEVAL_STRATEGY_PATH, content)


def format_counter(counter_data: dict[str, object]) -> str:
    if not counter_data:
        return "无"
    items = [f"`{key}`: {value}" for key, value in counter_data.items()]
    return "；".join(items)


def ordered_coverage(counter_data: dict[str, object], keys: list[str]) -> dict[str, object]:
    return {key: counter_data.get(key, 0) for key in keys}


def build_coverage_report(
    tender_stats: dict[str, object],
    policy_stats: dict[str, object],
    enterprise_stats: dict[str, object],
    manifest_entries: list[dict[str, object]],
) -> None:
    manifest_by_dataset = Counter(entry.get("dataset", "unknown") for entry in manifest_entries)
    manifest_by_file_type = Counter(entry.get("file_type", "unknown") for entry in manifest_entries)

    content = f"""# Coverage Report

生成时间：`{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}`

## 交付物状态

1. `data/raw/` 已生成三类原始层目录
2. `data/manifests/raw_manifest.jsonl` 已生成
3. `data/contracts/data_targets.json` 已生成
4. `docs/retrieval_strategy.md` 已生成
5. `reports/coverage_report.md` 已生成

## 招标数据

- 目标：`{tender_stats['target_count']}` 个唯一项目主键，计数单位为 `project`
- 当前仓库可用上游：`{tender_stats['raw_row_total']}` 条页面记录，其中 `project_id` 非空后可聚合成 `{'{:,}'.format(tender_stats['unique_project_total'])}` 个唯一项目
- 当前打包：`{tender_stats['packaged_project_count']}` 个项目 bundle，底层共包含 `{'{:,}'.format(tender_stats['packaged_row_count'])}` 条生命周期页面记录
- 去重收益：`{tender_stats['blank_project_id_rows']}` 条空 `project_id` 页面记录被排除，不计入项目覆盖
- 生命周期完整性：
  - 多页面项目：`{tender_stats['projects_with_multiple_rows']}` / `{tender_stats['packaged_project_count']}`
  - 多链接项目：`{tender_stats['projects_with_multiple_links']}` / `{tender_stats['packaged_project_count']}`
  - 含采购正文：`{tender_stats['projects_with_procurement_content']}` / `{tender_stats['packaged_project_count']}`
  - 含结果正文：`{tender_stats['projects_with_bid_content']}` / `{tender_stats['packaged_project_count']}`
  - 含附件线索：`{tender_stats['projects_with_attachment_hints']}` / `{tender_stats['packaged_project_count']}`
- 官方历史源补采：
  - 时间窗口：`{tender_stats['official_history_time_begin']}` 至 `{tender_stats['official_history_time_end']}`
  - 国家平台 API 原始页：`{tender_stats['official_history_page_count']}` 页
  - 当前可用缓存页：`{tender_stats['official_history_cached_page_count']}` 页；本轮频控未拉全的页数：`{tender_stats['official_history_failed_page_count']}`
  - 官方 raw 记录：`{'{:,}'.format(tender_stats['official_history_raw_record_count'])}` 条，去重后 `{'{:,}'.format(tender_stats['official_history_unique_record_count'])}` 条
  - 已匹配到当前 1000 项目的项目数：`{tender_stats['official_history_match_project_count']}` / `{tender_stats['packaged_project_count']}`
  - 已匹配的官方记录数：`{tender_stats['official_history_matched_record_count']}`
  - 匹配方法分布：{format_counter(tender_stats['official_history_match_method_counter'])}
  - 官方信息类型分布：{format_counter(tender_stats['official_history_info_type_counter'])}
- 必填字段预覆盖：{format_counter(ordered_coverage(tender_stats['required_field_coverage'], TENDER_REQUIRED_FIELDS))}
- 信息类型分布：{format_counter(tender_stats['stage_counter'])}
- 备注：合肥公共资源交易站点的原始 HTML / PDF 仍存在脚本直连 `403` 问题；本轮补齐路径改为“项目级 bundle + 国家平台历史 API 官方原始 JSON 页”。国家平台历史 API 当前存在频控，已先固化可抓到的缓存页，详情页 HTML 仍待后续浏览器态采集。

## 政策数据

- 目标：`{policy_stats['target_count']}` 个文档，计数单位为 `document`
- 当前仓库本地已有：`{policy_stats['available_document_total']}` 个文档级原始记录
- 官方政策库 feed 候选：`{'{:,}'.format(policy_stats['official_feed_candidate_total'])}` 条
- 本轮官方补采：`{policy_stats['official_selected_count']}` 个文档，写入 `{'{:,}'.format(policy_stats['official_html_count'])}` 份原始 HTML
- 当前打包：`{policy_stats['packaged_document_count']}` 个政策文档 bundle，外加 `{'{:,}'.format(policy_stats['html_sample_count'])}` 个历史 HTML 样本
- 官方抓取过程：尝试 `{'{:,}'.format(policy_stats['official_fetch_attempts'])}` 条，失败 `{'{:,}'.format(policy_stats['official_fetch_failures'])}` 条，因去重跳过 `{'{:,}'.format(policy_stats['official_duplicate_skips'])}` 条
- 距离目标缺口：`{policy_stats['gap_to_target']}` 个文档
- 层级分布：{format_counter(policy_stats['region_level_counter'])}
- 来源分布：{format_counter(policy_stats['source_type_counter'])}
- 官方补采来源分布：{format_counter(policy_stats['official_source_counter'])}
- 必填字段预覆盖：{format_counter(ordered_coverage(policy_stats['required_field_coverage'], POLICY_REQUIRED_FIELDS))}
- 备注：政策库已按“document”口径补到目标规模；本地原始底表仍保留 `180` 份，缺口主要由中国政府网官方政策库补齐，后续再继续补安徽 / 合肥官方源占比。

## 企业数据

- 目标：`{enterprise_stats['target_count']}` 个实体，计数单位为 `entity`
- 主体来源：先从已选招标项目抽主体，再对全量招标主体做回填
- 当前可用招标主体名：
  - 已选 1000 项目内：`{enterprise_stats['selected_project_related_names']}` 个唯一主体名
  - 全量招标语料内：`{enterprise_stats['all_tender_related_names']}` 个唯一主体名
- 当前可匹配企业画像：`{enterprise_stats['matched_entity_candidates']}` 个唯一实体候选
- 当前打包：`{enterprise_stats['packaged_entity_count']}` 个企业实体 bundle
- 选择策略：
  - 直接来自已选 1000 项目：`{enterprise_stats['direct_matches_from_selected_projects']}` 个
  - 为补足到 1000 从全量招标主体回填：`{enterprise_stats['backfill_from_full_tender_corpus']}` 个
  - 本地企业库精确命中：`{enterprise_stats['local_exact_match_count']}` 个
  - 全国企业库精确命中：`{enterprise_stats['national_exact_match_count']}` 个
- 必填字段预覆盖：{format_counter(ordered_coverage(enterprise_stats['required_field_coverage'], ENTERPRISE_REQUIRED_FIELDS))}
- 备注：企业库不再随机抽取，而是从招标主体反查生成，优先保证后续 `project -> entity -> policy` 可以关联。

## Raw Manifest

- `raw_manifest.jsonl` 总行数：`{len(manifest_entries)}`
- 按数据集：{format_counter(dict(manifest_by_dataset))}
- 按文件类型：{format_counter(dict(manifest_by_file_type))}

## 当前结论

- 招标口径已改成“唯一项目主键”，后续不会再因为多阶段页面重复而返工。
- 政策口径已改成“文档”，并把 `issuer / doc_no / validity_status` 定义成强制字段，即便当前覆盖不足，也不会再混口径。
- 企业口径已改成“招标主体反查”，不再随机抽企业。
- 当前阶段的重点不是伪造 1000 文档政策覆盖，而是先把主键、目录、manifest、字段契约、覆盖报告全部钉死。
"""
    write_text(COVERAGE_REPORT_PATH, content)


def main() -> None:
    crawl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ensure_dirs()
    reset_output_dirs()

    tender_manifest, tender_stats, selected_project_ids, selected_name_counter, all_name_counter = build_tender_raw(crawl_time)
    tender_official_manifest, tender_official_stats = build_tender_official_history_raw(crawl_time)
    tender_stats.update(tender_official_stats)
    policy_manifest, policy_stats = build_policy_raw(crawl_time)
    enterprise_manifest, enterprise_stats = build_enterprise_raw(
        crawl_time=crawl_time,
        selected_project_ids=selected_project_ids,
        selected_name_counter=selected_name_counter,
        all_name_counter=all_name_counter,
    )

    manifest_entries = tender_manifest + tender_official_manifest + policy_manifest + enterprise_manifest
    write_jsonl(RAW_MANIFEST_PATH, manifest_entries)
    build_data_targets()
    build_retrieval_strategy()
    build_coverage_report(tender_stats, policy_stats, enterprise_stats, manifest_entries)

    print("phase1 data lake generated")
    print(json.dumps(
        {
            "tender_projects": tender_stats["packaged_project_count"],
            "policy_documents": policy_stats["packaged_document_count"],
            "enterprise_entities": enterprise_stats["packaged_entity_count"],
            "raw_manifest_rows": len(manifest_entries),
        },
        ensure_ascii=False,
    ))


if __name__ == "__main__":
    main()
