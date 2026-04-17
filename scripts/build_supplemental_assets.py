from __future__ import annotations

import ast
import csv
import io
import re
import ssl
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import Request, urlopen

import certifi
from bs4 import BeautifulSoup
from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data_new"
POLICY_DIR = DATA_DIR / "01_policy"
TENDER_DIR = DATA_DIR / "02_tender"
COMPANY_DIR = DATA_DIR / "03_company"
ATTACHMENT_DIR = DATA_DIR / "05_attachment"

LOCAL_DOCS_PATH = POLICY_DIR / "policy_local_docs_ah_hf.csv"

CURATED_DOCS_PATH = POLICY_DIR / "policy_curated_docs.csv"
CURATED_META_PATH = POLICY_DIR / "policy_curated_meta.csv"

ATTACHMENT_MANIFEST_PATH = ATTACHMENT_DIR / "attachment_manifest_all.csv"
ATTACHMENT_QUEUE_PATH = ATTACHMENT_DIR / "attachment_download_queue_priority.csv"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

CONTENT_SELECTORS = [
    "div.TRS_Editor",
    "div.article_con",
    "div.article-content",
    "div.content",
    "div.wzy1",
    "div.pages_content",
    "div.wp_articlecontent",
    "div.Custom_UnionStyle",
    "div.view.TRSDivStyle",
    "div.ewb-article-info-content",
    "article",
    "main",
]


@dataclass(frozen=True)
class PolicySource:
    doc_id: str
    page_url: str
    region: str
    region_level: str
    category: str
    project_type: str
    source_type: str
    expected_title: str
    body_anchor: str = ""
    publish_hint: str = ""
    mode: str = "html"


LOCAL_POLICY_SOURCES: tuple[PolicySource, ...] = (
    PolicySource(
        doc_id="ah_hf_001",
        page_url="https://ggj.huainan.gov.cn/zcfg/ahzcfg/551706157.html",
        region="安徽省",
        region_level="province",
        category="公共资源交易",
        project_type="招投标",
        source_type="official",
        expected_title="安徽省公共资源交易监督管理办法(安徽省人民政府令第255号)",
        body_anchor="《安徽省公共资源交易监督管理办法》已经2014年10月27日省人民政府第36次常务会议通过",
    ),
    PolicySource(
        doc_id="ah_hf_002",
        page_url="https://whsggj.wuhu.gov.cn/xwzx/zcfg/ahszcfg/8407371.html",
        region="安徽省",
        region_level="province",
        category="招投标",
        project_type="招投标",
        source_type="official",
        expected_title="安徽省发展改革委等部门关于进一步完善招标投标交易担保制度降低招标投标交易成本的通知",
        body_anchor="各市公共资源交易综合管理部门",
    ),
    PolicySource(
        doc_id="ah_hf_003",
        page_url="https://www.tljq.gov.cn/jqrmzf/zcwj01/pc/content/content_1726798217714376704.html",
        region="安徽省",
        region_level="province",
        category="政府采购",
        project_type="政府采购",
        source_type="official",
        expected_title="安徽省财政厅关于印发《安徽省政府采购“徽采云”监管系统管理办法》的通知",
        body_anchor="皖财购〔2022〕673号",
        publish_hint="2022-06-23",
    ),
    PolicySource(
        doc_id="ah_hf_004",
        page_url="https://www.fadada.com/article/detail-14026",
        region="合肥市",
        region_level="city",
        category="公共资源交易",
        project_type="招投标",
        source_type="mirror",
        expected_title="合肥市公共资源交易管理条例",
        body_anchor="合肥市公共资源交易管理条例",
        publish_hint="2019-10-16",
    ),
    PolicySource(
        doc_id="ah_hf_005",
        page_url="https://gcs66.com/document_detail/147787.html",
        region="合肥市",
        region_level="city",
        category="公共资源交易",
        project_type="招投标",
        source_type="mirror",
        expected_title="合肥市公共资源交易项目交易方式管理规定",
        body_anchor="各县（市）、区人民政府",
        publish_hint="2024-01-25",
    ),
    PolicySource(
        doc_id="ah_hf_006",
        page_url="https://whsggj.wuhu.gov.cn/xwzx/zcfg/ahszcfg/8187900.html",
        region="安徽省",
        region_level="province",
        category="公共资源交易",
        project_type="招投标",
        source_type="official",
        expected_title="安徽省人民政府办公厅关于印发安徽省公共资源交易平台服务管理细则的通知",
        body_anchor="各市、县人民政府，省政府各部门、各直属机构",
        publish_hint="2021-10-12",
    ),
    PolicySource(
        doc_id="ah_hf_007",
        page_url="https://whsggzy.wuhu.gov.cn/whggzyjy/xxgk/019001/019001001/20181212/883717c5-a948-4e14-8816-2afe6f76f70f.html",
        region="安徽省",
        region_level="province",
        category="公共资源交易",
        project_type="招投标",
        source_type="official",
        expected_title="安徽省人民政府办公厅关于印发安徽省省级公共资源交易综合管理办法的通知",
        body_anchor="为规范省级公共资源交易活动",
        publish_hint="2018-09-03",
    ),
    PolicySource(
        doc_id="ah_hf_008",
        page_url="https://www.lingbi.gov.cn/public/6626171/160330201.html",
        region="安徽省",
        region_level="province",
        category="公共资源交易",
        project_type="招投标",
        source_type="official",
        expected_title="安徽省评标评审专家库和评标评审专家管理办法",
        body_anchor="第一章 总 则",
        publish_hint="2024-02-05",
    ),
    PolicySource(
        doc_id="ah_hf_009",
        page_url="https://cz.huainan.gov.cn/zfcg/551739310.html",
        region="安徽省",
        region_level="province",
        category="政府采购",
        project_type="政府采购",
        source_type="official",
        expected_title="安徽省财政厅关于印发《安徽省政府采购“徽采云”电子卖场管理办法（2024年版）》的通知",
        body_anchor="皖财购〔2023〕1324号",
        publish_hint="2023-12-19",
        mode="page_pdf",
    ),
    PolicySource(
        doc_id="ah_hf_010",
        page_url="https://www.ahhy.gov.cn/zfxxgk/public/24561/51263413.html",
        region="安徽省",
        region_level="province",
        category="政府采购",
        project_type="政府采购",
        source_type="official",
        expected_title="安徽省财政厅关于印发《“徽采云”电子卖场供应商及商品管理工作指引（2024年版）》的通知",
        body_anchor="省直各部门、单位，各市、县（区）财政局",
        publish_hint="2024-05-31",
    ),
    PolicySource(
        doc_id="ah_hf_011",
        page_url="https://cz.huainan.gov.cn/zfcg/551792968.html",
        region="安徽省",
        region_level="province",
        category="政府采购",
        project_type="政府采购",
        source_type="official",
        expected_title="安徽省财政厅等5部门关于印发《〈政府采购领域“整顿市场秩序、建设法规体系、促进产业发展”三年行动方案（2024—2026年）〉贯彻落实举措》的通知",
        body_anchor="皖财购〔2024〕1361号",
        publish_hint="2025-04-08",
    ),
)


def make_request(url: str) -> Request:
    return Request(url, headers={"User-Agent": USER_AGENT})


def fetch_bytes(url: str) -> bytes:
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    with urlopen(make_request(url), timeout=30, context=ssl_context) as response:
        return response.read()


def decode_bytes(raw: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "gb18030", "gbk"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def fetch_html(url: str) -> str:
    return decode_bytes(fetch_bytes(url))


def collapse_text(text: str) -> str:
    text = text.replace("\u3000", " ")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def choose_longest_text(candidates: Iterable[str]) -> str:
    cleaned = [collapse_text(item) for item in candidates if item and collapse_text(item)]
    if not cleaned:
        return ""
    return max(cleaned, key=len)


def fuzzy_index(text: str, anchor: str) -> int:
    pattern = r"\s*".join(re.escape(char) for char in anchor if char.strip())
    if not pattern:
        return -1
    match = re.search(pattern, text)
    return match.start() if match else -1


def extract_main_text(soup: BeautifulSoup) -> str:
    candidates: list[str] = []
    for selector in CONTENT_SELECTORS:
        for node in soup.select(selector):
            text = node.get_text("\n", strip=True)
            if len(text) >= 200:
                candidates.append(text)

    if not candidates:
        body = soup.body.get_text("\n", strip=True) if soup.body else soup.get_text("\n", strip=True)
        candidates.append(body)

    return choose_longest_text(candidates)


def trim_content(raw_text: str, expected_title: str, body_anchor: str) -> str:
    text = collapse_text(raw_text)
    if body_anchor:
        index = fuzzy_index(text, body_anchor)
        if index >= 0:
            return text[index:].strip()

    occurrences = [match.start() for match in re.finditer(re.escape(expected_title), text)]
    if occurrences:
        return text[occurrences[-1] if len(occurrences) > 1 else occurrences[0] :].strip()

    return text


def extract_title(soup: BeautifulSoup, expected_title: str, source_type: str) -> str:
    if source_type == "mirror":
        return expected_title

    meta_candidates = [
        soup.find("meta", attrs={"name": "ArticleTitle"}),
        soup.find("meta", attrs={"property": "og:title"}),
    ]
    for meta in meta_candidates:
        if meta and meta.get("content"):
            title = collapse_text(meta["content"])
            if title:
                return title

    for selector in ("h1", "h2", "title"):
        node = soup.select_one(selector)
        if node:
            title = collapse_text(node.get_text(" ", strip=True))
            if title:
                return title

    return expected_title


def extract_publish_date(html: str, soup: BeautifulSoup) -> str:
    meta_names = ["PubDate", "publishdate", "ArticlePublishDate", "article:published_time"]
    for meta_name in meta_names:
        meta = soup.find("meta", attrs={"name": meta_name}) or soup.find("meta", attrs={"property": meta_name})
        if meta and meta.get("content"):
            return meta["content"].strip()

    patterns = [
        r"(\d{4}[/-]\d{1,2}[/-]\d{1,2}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)",
        r"(\d{4}年\d{1,2}月\d{1,2}日(?:\s*\d{1,2}:\d{2}(?::\d{2})?)?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            return match.group(1)
    return ""


def extract_source_name(soup: BeautifulSoup, fallback: str) -> str:
    meta = soup.find("meta", attrs={"name": "ContentSource"})
    if meta and meta.get("content"):
        return collapse_text(meta["content"])

    text = soup.get_text("\n", strip=True)
    match = re.search(r"来源[:：]\s*([^\n]+)", text)
    if match:
        return collapse_text(match.group(1))

    return fallback


def extract_pdf_url(html: str, page_url: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    candidates: list[str] = []
    for node in soup.find_all(href=True):
        href = node.get("href", "").strip()
        if ".pdf" in href.lower():
            candidates.append(urljoin(page_url, href))
    if candidates:
        return candidates[0]

    match = re.search(r"""(["'])([^"']+\.pdf(?:\?[^"']*)?)\1""", html, flags=re.IGNORECASE)
    if match:
        return urljoin(page_url, match.group(2))

    return ""


def extract_pdf_text(raw_pdf: bytes) -> str:
    reader = PdfReader(io.BytesIO(raw_pdf))
    chunks = [page.extract_text() or "" for page in reader.pages]
    return collapse_text("\n\n".join(chunks))


def split_chunks(text: str, chunk_size: int = 1200) -> list[str]:
    paragraphs = [collapse_text(item) for item in re.split(r"\n{2,}", text) if collapse_text(item)]
    chunks: list[str] = []
    current = ""

    for paragraph in paragraphs:
        if len(paragraph) <= chunk_size:
            candidate = f"{current}\n\n{paragraph}".strip() if current else paragraph
            if len(candidate) <= chunk_size:
                current = candidate
            else:
                if current:
                    chunks.append(current)
                current = paragraph
            continue

        for part in re.split(r"(?<=[。！？；])", paragraph):
            part = collapse_text(part)
            if not part:
                continue
            candidate = f"{current}{part}".strip() if current else part
            if len(candidate) <= chunk_size:
                current = candidate
            else:
                if current:
                    chunks.append(current)
                current = part

    if current:
        chunks.append(current)

    return chunks


def normalize_date(value: str) -> str:
    value = collapse_text(value)
    if not value:
        return ""

    value = value.replace(".", "-").replace("/", "-")
    value = re.sub(r"\s+", " ", value)

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y年%m月%d日 %H:%M:%S",
        "%Y年%m月%d日 %H:%M",
        "%Y年%m月%d日",
    ):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d %H:%M:%S" if "H" in fmt else "%Y-%m-%d")
        except ValueError:
            continue

    mdy_match = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4})", value)
    if mdy_match:
        first = int(mdy_match.group(1))
        second = int(mdy_match.group(2))
        year = int(mdy_match.group(3))
        if first > 12:
            return f"{year:04d}-{second:02d}-{first:02d}"
        if second > 12:
            return f"{year:04d}-{first:02d}-{second:02d}"
        return value

    return value


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                return list(csv.DictReader(handle))
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8", b"", 0, 1, f"failed to decode {path}")


def clean_legal_content(title: str, content: str) -> str:
    text = collapse_text(content)
    text = re.sub(r"^#+\s*", "", text)
    title_variants = [title, f"### {title}", f"# {title}"]
    for variant in title_variants:
        if text.startswith(variant):
            text = text[len(variant) :].lstrip()
    if not text.startswith(title):
        text = f"{title}\n\n{text}".strip()
    return text


def is_relevant_legal_doc(row: dict[str, str]) -> bool:
    category = collapse_text(row.get("category", ""))
    text = collapse_text(f"{row.get('title', '')}\n{row.get('content', '')}")
    strong_categories = {"招投标", "政府采购"}
    keywords = [
        "招标",
        "投标",
        "招投标",
        "政府采购",
        "采购",
        "公共资源",
        "评标",
        "评审",
        "竞争性磋商",
        "竞争性谈判",
        "询价",
        "框架协议",
    ]
    return category in strong_categories or any(keyword in text for keyword in keywords)


def infer_project_type(text: str, category: str) -> str:
    full_text = f"{category}\n{text}"
    head_text = text[:200]
    if "政府采购" in head_text or "采购法" in head_text or "徽采云" in head_text:
        return "政府采购"
    if "招标" in head_text or "投标" in head_text or "评标" in head_text:
        return "招投标"
    if "政府采购" in category:
        return "政府采购"
    if "公共资源" in category:
        return "公共资源交易"
    if "招投标" in category or "招标" in category or "投标" in category:
        return "招投标"
    if "政府采购" in full_text or "采购" in full_text:
        return "政府采购"
    if "公共资源" in full_text:
        return "公共资源交易"
    return "招投标"


def normalize_attachment_cell(value: str) -> str:
    value = collapse_text(value or "")
    if value in {"", "/", "[]", "null", "None"}:
        return ""
    return value


def parse_list_like_value(value: str) -> list[str]:
    if not value or not (value.startswith("[") and value.endswith("]")):
        return []
    try:
        parsed = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return []
    if isinstance(parsed, list):
        return [collapse_text(str(item)) for item in parsed if collapse_text(str(item))]
    return []


def split_numbered_items(value: str) -> list[str]:
    value = normalize_attachment_cell(value)
    if not value:
        return []

    parsed = parse_list_like_value(value)
    if parsed:
        return parsed

    matches = [
        collapse_text(match.group(1))
        for match in re.finditer(r"(?:^|\n)\s*\d+\s*[\.:：、]\s*(.+?)(?=(?:\n\s*\d+\s*[\.:：、]\s*)|$)", value, flags=re.S)
    ]
    if matches:
        return matches

    parts = [collapse_text(part) for part in value.split("\n") if collapse_text(part)]
    if len(parts) > 1:
        return parts

    return [value]


def derive_file_ext(name: str, url: str) -> str:
    candidate = name or urlparse(url).path
    match = re.search(r"\.([a-zA-Z0-9]{1,8})(?:$|\?)", candidate)
    return match.group(1).lower() if match else ""


def derive_file_type(file_ext: str) -> str:
    if file_ext == "pdf":
        return "pdf"
    if file_ext in {"doc", "docx", "wps"}:
        return "doc"
    if file_ext in {"xls", "xlsx", "csv"}:
        return "excel"
    if file_ext in {"zip", "rar", "7z"}:
        return "archive"
    if file_ext in {"jpg", "jpeg", "png"}:
        return "image"
    return "other"


def extract_attach_guid(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    values = params.get("attachGuid") or params.get("guid")
    return values[0] if values else ""


def attachment_priority(attachment_name: str, file_type: str, attachment_role: str) -> tuple[int, str]:
    priority = 40
    reason = "generic_attachment"

    if file_type in {"pdf", "doc", "excel"}:
        priority += 20
        reason = "document_attachment"

    for keyword, score, keyword_reason in (
        ("招标文件", 30, "tender_document"),
        ("采购文件", 30, "procurement_document"),
        ("磋商文件", 25, "consultation_document"),
        ("竞争性谈判", 25, "negotiation_document"),
        ("定标", 20, "award_decision"),
        ("评审", 20, "review_attachment"),
        ("中标", 15, "award_attachment"),
        ("成交", 15, "award_attachment"),
    ):
        if keyword in attachment_name:
            priority += score
            reason = keyword_reason
            break

    priority += {"procurement_notice": 15, "bid_result": 12, "notification": 10}.get(attachment_role, 0)
    return priority, reason


def build_local_policy_assets() -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    doc_rows: list[dict[str, str]] = []
    meta_rows: list[dict[str, str]] = []
    chunk_rows: list[dict[str, str]] = []
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for source in LOCAL_POLICY_SOURCES:
        try:
            html = fetch_html(source.page_url)
        except (HTTPError, URLError, TimeoutError) as exc:
            print(f"skip {source.doc_id}: {exc}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        source_name = extract_source_name(soup, source.region)
        publish_date = normalize_date(source.publish_hint or extract_publish_date(html, soup))
        title = extract_title(soup, source.expected_title, source.source_type) or source.expected_title
        document_url = source.page_url

        if source.mode == "page_pdf":
            pdf_url = extract_pdf_url(html, source.page_url)
            if not pdf_url:
                print(f"skip {source.doc_id}: no pdf link found")
                continue
            try:
                content = extract_pdf_text(fetch_bytes(pdf_url))
            except (HTTPError, URLError, TimeoutError, ValueError) as exc:
                print(f"skip {source.doc_id}: {exc}")
                continue
            document_url = pdf_url
        else:
            content = extract_main_text(soup)

        content = trim_content(content, source.expected_title, source.body_anchor)
        if source.expected_title not in content:
            content = f"{source.expected_title}\n\n{content}".strip()

        if len(content) < 400:
            print(f"skip {source.doc_id}: content too short")
            continue

        word_count = str(len(content))
        doc_rows.append(
            {
                "id": source.doc_id,
                "title": title,
                "content": content,
                "source": source_name,
                "publish_time": publish_date,
                "url": source.page_url,
                "category": source.category,
                "project_type": source.project_type,
                "word_count": word_count,
            }
        )
        meta_rows.append(
            {
                "id": source.doc_id,
                "title": title,
                "publish_date": publish_date,
                "source_url": source.page_url,
                "document_url": document_url,
                "region": source.region,
                "region_level": source.region_level,
                "category": source.category,
                "project_type": source.project_type,
                "source_type": source.source_type,
                "source_name": source_name,
                "content_length": word_count,
                "created_at": created_at,
            }
        )

        chunks = split_chunks(content)
        total_chunks = str(len(chunks))
        for index, chunk in enumerate(chunks):
            chunk_rows.append(
                {
                    "doc_id": f"{source.doc_id}:chunk:{index}",
                    "source_table": "policy_documents_ah_hf",
                    "title": f"{title} (第{index + 1}部分)",
                    "rule_title": title,
                    "event_time": publish_date,
                    "release_time": publish_date,
                    "ingest_time": created_at,
                    "is_chunked": "1" if len(chunks) > 1 else "0",
                    "chunk_id": str(index),
                    "total_chunks": total_chunks,
                    "original_doc_id": source.doc_id,
                    "text": chunk,
                    "source_url": source.page_url,
                    "created_at": created_at,
                    "updated_at": created_at,
                }
            )

    doc_rows.sort(key=lambda row: row["id"])
    meta_rows.sort(key=lambda row: row["id"])
    chunk_rows.sort(key=lambda row: row["doc_id"])
    return doc_rows, meta_rows, chunk_rows


def build_curated_policy_assets(
    local_docs: list[dict[str, str]],
    local_meta: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    meta_by_id = {row["id"]: row for row in local_meta}
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    curated_docs: list[dict[str, str]] = []
    curated_meta: list[dict[str, str]] = []
    curated_chunks: list[dict[str, str]] = []

    for row in local_docs:
        metadata = meta_by_id[row["id"]]
        curated_id = f"local:{row['id']}"
        curated_docs.append(
            {
                "id": curated_id,
                "title": row["title"],
                "content": row["content"],
                "source": row["source"],
                "publish_time": row["publish_time"],
                "url": row["url"],
                "category": row["category"],
                "project_type": row["project_type"],
                "region": metadata["region"],
                "region_level": metadata["region_level"],
                "source_type": metadata["source_type"],
                "corpus_tag": "local_policy",
                "word_count": row["word_count"],
            }
        )
        curated_meta.append(
            {
                "id": curated_id,
                "title": row["title"],
                "publish_date": row["publish_time"],
                "source_url": row["url"],
                "document_url": metadata.get("document_url", row["url"]),
                "region": metadata["region"],
                "region_level": metadata["region_level"],
                "category": row["category"],
                "project_type": row["project_type"],
                "source_type": metadata["source_type"],
                "source_name": row["source"],
                "word_count": row["word_count"],
                "corpus_tag": "local_policy",
                "upstream_table": "policy_documents_ah_hf",
                "created_at": created_at,
            }
        )

    for row in read_csv_rows(POLICY_DIR / "policy_src_legal_documents.csv"):
        if not is_relevant_legal_doc(row):
            continue

        title = collapse_text(row.get("title", ""))
        content = clean_legal_content(title, row.get("content", ""))
        publish_time = normalize_date(row.get("publish_date", ""))
        category = collapse_text(row.get("category", ""))
        project_type = infer_project_type(f"{title}\n{content}", category)
        curated_id = f"legal:{row['id']}"
        word_count = str(len(content))

        curated_docs.append(
            {
                "id": curated_id,
                "title": title,
                "content": content,
                "source": "全国法规整理",
                "publish_time": publish_time,
                "url": "",
                "category": category,
                "project_type": project_type,
                "region": "全国",
                "region_level": "national",
                "source_type": "curated_existing",
                "corpus_tag": "national_baseline",
                "word_count": word_count,
            }
        )
        curated_meta.append(
            {
                "id": curated_id,
                "title": title,
                "publish_date": publish_time,
                "source_url": "",
                "document_url": "",
                "region": "全国",
                "region_level": "national",
                "category": category,
                "project_type": project_type,
                "source_type": "curated_existing",
                "source_name": "全国法规整理",
                "word_count": word_count,
                "corpus_tag": "national_baseline",
                "upstream_table": "legal_documents",
                "created_at": created_at,
            }
        )

    for doc_row in curated_docs:
        chunks = split_chunks(doc_row["content"])
        total_chunks = str(len(chunks))
        for index, chunk in enumerate(chunks):
            curated_chunks.append(
                {
                    "doc_id": f"{doc_row['id']}:chunk:{index}",
                    "source_table": "policy_documents_curated",
                    "title": f"{doc_row['title']} (第{index + 1}部分)",
                    "rule_title": doc_row["title"],
                    "event_time": doc_row["publish_time"],
                    "release_time": doc_row["publish_time"],
                    "ingest_time": created_at,
                    "is_chunked": "1" if len(chunks) > 1 else "0",
                    "chunk_id": str(index),
                    "total_chunks": total_chunks,
                    "original_doc_id": doc_row["id"],
                    "text": chunk,
                    "source_url": doc_row["url"],
                    "created_at": created_at,
                    "updated_at": created_at,
                }
            )

    curated_docs.sort(key=lambda row: row["id"])
    curated_meta.sort(key=lambda row: row["id"])
    curated_chunks.sort(key=lambda row: row["doc_id"])
    return curated_docs, curated_meta, curated_chunks


def append_attachment_rows(
    rows: list[dict[str, str]],
    source_table: str,
    record_key: str,
    title: str,
    project_name: str,
    publish_time: str,
    attachment_role: str,
    names: list[str],
    links: list[str],
    created_at: str,
) -> None:
    max_len = max(len(names), len(links))
    if max_len == 0:
        return

    for index in range(max_len):
        attachment_name = names[index] if index < len(names) else ""
        attachment_url = links[index] if index < len(links) else ""
        file_ext = derive_file_ext(attachment_name, attachment_url)
        file_type = derive_file_type(file_ext)
        priority, priority_reason = attachment_priority(attachment_name, file_type, attachment_role)
        rows.append(
            {
                "attachment_id": f"{source_table}:{record_key}:{attachment_role}:{index}",
                "source_table": source_table,
                "record_key": record_key,
                "attachment_role": attachment_role,
                "title": title,
                "project_name": project_name,
                "publish_time": publish_time,
                "attachment_name": attachment_name,
                "attachment_url": attachment_url,
                "file_ext": file_ext,
                "file_type": file_type,
                "domain": urlparse(attachment_url).netloc if attachment_url else "",
                "attach_guid": extract_attach_guid(attachment_url),
                "is_pdf": "1" if file_ext == "pdf" else "0",
                "is_downloadable": "1" if attachment_url.startswith("http") else "0",
                "priority": str(priority),
                "priority_reason": priority_reason,
                "created_at": created_at,
            }
        )


def build_attachment_assets() -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    manifest_rows: list[dict[str, str]] = []

    for index, row in enumerate(read_csv_rows(ATTACHMENT_DIR / "attachment_links_raw.csv"), start=1):
        title = collapse_text(row.get("title", ""))
        append_attachment_rows(
            manifest_rows,
            "attachments",
            str(index),
            title,
            title,
            "",
            "procurement_notice",
            split_numbered_items(row.get("procurement_notice_attachments_file_name", "")),
            split_numbered_items(row.get("procurement_notice_attachments_file_link", "")),
            created_at,
        )
        append_attachment_rows(
            manifest_rows,
            "attachments",
            str(index),
            title,
            title,
            "",
            "bid_result",
            split_numbered_items(row.get("bid_results_attachments_file_name", "")),
            split_numbered_items(row.get("bid_results_attachments_file_link", "")),
            created_at,
        )

    for index, row in enumerate(read_csv_rows(TENDER_DIR / "tender_notices_procurement.csv"), start=1):
        append_attachment_rows(
            manifest_rows,
            "procurement_notices",
            str(index),
            collapse_text(row.get("title", "")),
            collapse_text(row.get("project_name", "")),
            normalize_date(row.get("procurement_date", "")),
            "procurement_notice",
            split_numbered_items(row.get("attachment_file_name", "")),
            split_numbered_items(row.get("attachment_file_link", "")),
            created_at,
        )

    for row in read_csv_rows(TENDER_DIR / "tender_notices_result.csv"):
        record_key = row.get("id", "") or str(len(manifest_rows) + 1)
        append_attachment_rows(
            manifest_rows,
            "notifications",
            record_key,
            collapse_text(row.get("title", "")),
            collapse_text(row.get("project_name", "")),
            normalize_date(row.get("issuance_date", "")),
            "notification",
            split_numbered_items(row.get("attachment_file_name", "")),
            split_numbered_items(row.get("attachment_file_link", "")),
            created_at,
        )

    manifest_rows = [row for row in manifest_rows if row["attachment_name"] or row["attachment_url"]]
    manifest_rows.sort(key=lambda row: (row["source_table"], row["record_key"], row["attachment_role"], row["attachment_name"]))

    deduped_queue: dict[str, dict[str, str]] = {}
    for row in manifest_rows:
        if row["file_type"] not in {"pdf", "doc", "excel", "archive"}:
            continue
        dedupe_key = row["attachment_url"] or f"{row['source_table']}::{row['record_key']}::{row['attachment_name']}"
        current = deduped_queue.get(dedupe_key)
        if not current:
            deduped_queue[dedupe_key] = {
                "queue_id": f"queue:{len(deduped_queue) + 1}",
                "attachment_url": row["attachment_url"],
                "attachment_name": row["attachment_name"],
                "file_ext": row["file_ext"],
                "file_type": row["file_type"],
                "domain": row["domain"],
                "attach_guid": row["attach_guid"],
                "priority": row["priority"],
                "priority_reason": row["priority_reason"],
                "source_hits": "1",
                "sample_source_table": row["source_table"],
                "sample_record_key": row["record_key"],
                "sample_project_name": row["project_name"],
                "sample_title": row["title"],
                "created_at": created_at,
            }
            continue

        current["source_hits"] = str(int(current["source_hits"]) + 1)
        if int(row["priority"]) > int(current["priority"]):
            current["priority"] = row["priority"]
            current["priority_reason"] = row["priority_reason"]
            current["sample_source_table"] = row["source_table"]
            current["sample_record_key"] = row["record_key"]
            current["sample_project_name"] = row["project_name"]
            current["sample_title"] = row["title"]

    queue_rows = list(deduped_queue.values())
    queue_rows.sort(key=lambda row: (-int(row["priority"]), row["file_type"], row["attachment_name"]))
    return manifest_rows, queue_rows


def main() -> None:
    local_docs, local_meta, _local_chunks = build_local_policy_assets()
    curated_docs, curated_meta, _curated_chunks = build_curated_policy_assets(local_docs, local_meta)
    attachment_manifest, attachment_queue = build_attachment_assets()

    write_csv(
        LOCAL_DOCS_PATH,
        ["id", "title", "content", "source", "publish_time", "url", "category", "project_type", "word_count"],
        local_docs,
    )
    write_csv(
        CURATED_DOCS_PATH,
        [
            "id",
            "title",
            "content",
            "source",
            "publish_time",
            "url",
            "category",
            "project_type",
            "region",
            "region_level",
            "source_type",
            "corpus_tag",
            "word_count",
        ],
        curated_docs,
    )
    write_csv(
        CURATED_META_PATH,
        [
            "id",
            "title",
            "publish_date",
            "source_url",
            "document_url",
            "region",
            "region_level",
            "category",
            "project_type",
            "source_type",
            "source_name",
            "word_count",
            "corpus_tag",
            "upstream_table",
            "created_at",
        ],
        curated_meta,
    )
    write_csv(
        ATTACHMENT_MANIFEST_PATH,
        [
            "attachment_id",
            "source_table",
            "record_key",
            "attachment_role",
            "title",
            "project_name",
            "publish_time",
            "attachment_name",
            "attachment_url",
            "file_ext",
            "file_type",
            "domain",
            "attach_guid",
            "is_pdf",
            "is_downloadable",
            "priority",
            "priority_reason",
            "created_at",
        ],
        attachment_manifest,
    )
    write_csv(
        ATTACHMENT_QUEUE_PATH,
        [
            "queue_id",
            "attachment_url",
            "attachment_name",
            "file_ext",
            "file_type",
            "domain",
            "attach_guid",
            "priority",
            "priority_reason",
            "source_hits",
            "sample_source_table",
            "sample_record_key",
            "sample_project_name",
            "sample_title",
            "created_at",
        ],
        attachment_queue,
    )

    print(f"local_policy_docs={len(local_docs)}")
    print(f"curated_policy_docs={len(curated_docs)}")
    print(f"attachment_manifest={len(attachment_manifest)}")
    print(f"attachment_queue={len(attachment_queue)}")


if __name__ == "__main__":
    main()
