from __future__ import annotations

import csv
import json
from collections import defaultdict, deque
from datetime import datetime
from heapq import heappush, heappushpop
from pathlib import Path
from typing import Iterable


csv.field_size_limit(1024 * 1024 * 512)

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT / "data_new"
OUTPUT_DIR = ROOT / "acceptance_assets"
RAW_DIR = OUTPUT_DIR / "raw_sources"
POLICY_DIR = RAW_DIR / "01_policy"
TENDER_DIR = RAW_DIR / "02_tender"
COMPANY_DIR = RAW_DIR / "03_company"

TARGET_COUNT = 1000


def ensure_dirs() -> None:
    for path in (OUTPUT_DIR, RAW_DIR, POLICY_DIR, TENDER_DIR, COMPANY_DIR):
        path.mkdir(parents=True, exist_ok=True)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for row in reader:
            clean_row: dict[str, str] = {}
            for key, value in row.items():
                clean_key = key.lstrip("\ufeff") if key else key
                clean_row[clean_key] = value or ""
            rows.append(clean_row)
    return rows


def iter_csv_rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            clean_row: dict[str, str] = {}
            for key, value in row.items():
                clean_key = key.lstrip("\ufeff") if key else key
                clean_row[clean_key] = value or ""
            yield clean_row


def read_csv_headers(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        headers = next(reader)
    return [header.lstrip("\ufeff") for header in headers]


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def repair_mojibake(value: object) -> object:
    if isinstance(value, str):
        try:
            return value.encode("gbk").decode("utf-8")
        except UnicodeError:
            return value
    if isinstance(value, list):
        return [repair_mojibake(item) for item in value]
    if isinstance(value, dict):
        return {repair_mojibake(key): repair_mojibake(item) for key, item in value.items()}
    return value


def write_json(path: Path, payload: object) -> None:
    clean_payload = repair_mojibake(payload)
    path.write_text(json.dumps(clean_payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    clean_content = repair_mojibake(content)
    path.write_text(clean_content, encoding="utf-8")


def norm(value: str | None) -> str:
    return (value or "").replace("\r\n", "\n").replace("\r", "\n").strip()


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
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
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


def build_policy_raw_assets() -> dict[str, object]:
    article_source = UPSTREAM / "01_policy" / "policy_national_article_units.csv"
    document_source = UPSTREAM / "01_policy" / "policy_src_legal_documents.csv"
    local_source = UPSTREAM / "01_policy" / "policy_local_docs_ah_hf.csv"

    article_rows = read_csv_rows(article_source)
    groups: dict[str, deque[dict[str, str]]] = defaultdict(deque)
    group_sort_seed: dict[str, tuple[datetime, str]] = {}

    for row in article_rows:
        if not norm(row.get("text")):
            continue
        law_id = norm(row.get("law_id"))
        groups[law_id].append(row)
        if law_id not in group_sort_seed:
            group_sort_seed[law_id] = (
                parse_datetime(row.get("publish_date")),
                norm(row.get("law_title")),
            )

    ordered_law_ids = sorted(
        groups,
        key=lambda item: (group_sort_seed[item][0], group_sort_seed[item][1]),
        reverse=True,
    )

    selected_articles: list[dict[str, str]] = []
    active = True
    while active and len(selected_articles) < TARGET_COUNT:
        active = False
        for law_id in ordered_law_ids:
            if groups[law_id]:
                selected_articles.append(groups[law_id].popleft())
                active = True
                if len(selected_articles) >= TARGET_COUNT:
                    break

    selected_titles = {norm(row.get("law_title")) for row in selected_articles}
    document_rows = read_csv_rows(document_source)
    supporting_documents = [
        row
        for row in document_rows
        if norm(row.get("title")) in selected_titles
    ]
    supporting_documents.sort(
        key=lambda row: (parse_datetime(row.get("publish_date")), norm(row.get("title"))),
        reverse=True,
    )

    local_documents = read_csv_rows(local_source)
    local_documents.sort(
        key=lambda row: (parse_datetime(row.get("publish_time")), norm(row.get("title"))),
        reverse=True,
    )

    article_output = POLICY_DIR / "policy_article_raw_1000.csv"
    document_output = POLICY_DIR / "policy_document_fulltext_support.csv"
    local_output = POLICY_DIR / "policy_local_fulltext_ah_hf.csv"

    write_csv(article_output, read_csv_headers(article_source), selected_articles)
    write_csv(document_output, read_csv_headers(document_source), supporting_documents)
    write_csv(local_output, read_csv_headers(local_source), local_documents)

    manifest = {
        "dataset_key": "policy",
        "display_name": "政策信息",
        "record_granularity": "条文级原始单元，不切块",
        "selection_rule": (
            "从 data_new/01_policy/policy_national_article_units.csv 中按 law_id 轮询抽取，"
            "优先覆盖更多法规，再保留政策全文母本与本地政策补充。"
        ),
        "available_counts": {
            "policy_article_units_total": len(article_rows),
            "policy_fulltext_documents_total": len(document_rows),
            "policy_local_documents_total": len(local_documents),
        },
        "selected_counts": {
            "policy_article_units_selected": len(selected_articles),
            "supporting_fulltext_documents_selected": len(supporting_documents),
            "local_fulltext_documents_selected": len(local_documents),
            "covered_laws": len(selected_titles),
        },
        "output_files": [
            relative(article_output),
            relative(document_output),
            relative(local_output),
        ],
        "upstream_files": [
            relative(article_source),
            relative(document_source),
            relative(local_source),
        ],
    }

    write_json(POLICY_DIR / "source_manifest.json", manifest)
    return manifest


def build_tender_raw_assets() -> dict[str, object]:
    source_path = UPSTREAM / "02_tender" / "tender_docs_ahzb_curated.csv"
    fieldnames = read_csv_headers(source_path)

    heap: list[tuple[datetime, int, dict[str, str]]] = []
    sequence = 0
    total_rows = 0

    for row in iter_csv_rows(source_path):
        total_rows += 1
        if not (norm(row.get("procurement_content")) or norm(row.get("bid_content"))):
            continue

        rank = parse_datetime(row.get("event_time"))
        item = (rank, sequence, row)
        sequence += 1

        if len(heap) < TARGET_COUNT:
            heappush(heap, item)
            continue

        if item[:2] > heap[0][:2]:
            heappushpop(heap, item)

    selected_rows = [row for _, _, row in sorted(heap, key=lambda item: (item[0], item[1]), reverse=True)]

    output_path = TENDER_DIR / "tender_notice_raw_1000.csv"
    write_csv(output_path, fieldnames, selected_rows)

    manifest = {
        "dataset_key": "tender",
        "display_name": "招标信息",
        "record_granularity": "公告级原始单元，不切块",
        "selection_rule": (
            "从 data_new/02_tender/tender_docs_ahzb_curated.csv 中保留正文非空记录，"
            "按 event_time 取最近 1000 条。"
        ),
        "available_counts": {
            "tender_notice_total": total_rows,
        },
        "selected_counts": {
            "tender_notice_selected": len(selected_rows),
        },
        "output_files": [
            relative(output_path),
        ],
        "upstream_files": [
            relative(source_path),
        ],
    }

    write_json(TENDER_DIR / "source_manifest.json", manifest)
    return manifest


def build_company_raw_assets() -> dict[str, object]:
    local_source = UPSTREAM / "03_company" / "company_profiles_local_matched.csv"
    national_source = UPSTREAM / "03_company" / "company_profiles_national.csv"

    local_rows = [
        row
        for row in read_csv_rows(local_source)
        if norm(row.get("match_status")) == "成功"
    ]
    local_rows.sort(
        key=lambda row: (
            parse_datetime(row.get("approval_date")),
            norm(row.get("company_name")),
        ),
        reverse=True,
    )
    selected_rows = local_rows[:TARGET_COUNT]

    output_path = COMPANY_DIR / "company_profile_raw_1000.csv"
    write_csv(output_path, read_csv_headers(local_source), selected_rows)

    manifest = {
        "dataset_key": "company",
        "display_name": "企业信息",
        "record_granularity": "企业画像级原始单元，不切块",
        "selection_rule": (
            "优先使用 data_new/03_company/company_profiles_local_matched.csv，"
            "过滤 match_status=成功 后按 approval_date 取最近 1000 条。"
        ),
        "available_counts": {
            "local_company_profiles_total": len(read_csv_rows(local_source)),
            "local_company_profiles_matched": len(local_rows),
            "national_company_profiles_total": len(read_csv_rows(national_source)),
        },
        "selected_counts": {
            "company_profile_selected": len(selected_rows),
        },
        "output_files": [
            relative(output_path),
        ],
        "upstream_files": [
            relative(local_source),
            relative(national_source),
        ],
    }

    write_json(COMPANY_DIR / "source_manifest.json", manifest)
    return manifest


def build_schema_json(
    policy_manifest: dict[str, object],
    tender_manifest: dict[str, object],
    company_manifest: dict[str, object],
) -> None:
    policy_primary_fields = read_csv_headers(POLICY_DIR / "policy_article_raw_1000.csv")
    policy_local_fields = read_csv_headers(POLICY_DIR / "policy_local_fulltext_ah_hf.csv")
    tender_fields = read_csv_headers(TENDER_DIR / "tender_notice_raw_1000.csv")
    company_fields = read_csv_headers(COMPANY_DIR / "company_profile_raw_1000.csv")

    descriptions = {
        "policy": {
            "id": "条文唯一标识",
            "law_id": "法规母本标识",
            "law_title": "法规名称",
            "article_number": "条文编号",
            "article_title": "条文标题",
            "chapter": "所属章节",
            "publish_date": "发布日期",
            "category": "政策主题分类",
            "project_type": "业务主题，如招投标/政府采购",
            "region": "地域范围",
            "region_level": "地域层级",
            "source_type": "来源类型",
            "text": "条文全文",
            "word_count": "条文长度统计",
            "created_at": "入库时间",
            "title": "政策标题",
            "content": "政策全文",
            "source": "来源站点或机构",
            "url": "原始页面链接",
            "publish_time": "页面发布时间",
        },
        "tender": {
            "doc_id": "公告唯一标识",
            "project_id": "项目编号",
            "link": "公告原始链接",
            "type": "业务类型，如政府采购/工程施工",
            "stage": "公告阶段，如采购/中标",
            "event_time": "公告发布时间",
            "province": "省份",
            "city": "城市",
            "town": "区县或交易区域",
            "category": "一级分类",
            "sub_category": "二级分类",
            "budget_amount": "预算金额",
            "purchaser": "采购人/招标人",
            "agency": "代理机构",
            "project_name": "项目名称",
            "procurement_title": "采购公告标题",
            "bid_company": "中标/成交供应商",
            "bid_amount": "中标金额",
            "bid_date": "中标日期",
            "procurement_content": "采购公告正文",
            "bid_content": "中标结果正文或原始 JSON 字符串",
            "procurement_content_length": "采购正文长度",
            "bid_content_length": "结果正文长度",
            "created_at": "入库时间",
        },
        "company": {
            "company_name": "企业名称",
            "business_status": "经营状态",
            "legal_representative": "法定代表人",
            "registered_capital": "注册资本",
            "paid_in_capital": "实缴资本",
            "establishment_date": "成立日期",
            "approval_date": "最近核准日期",
            "business_term": "营业期限",
            "province": "省份",
            "city": "城市",
            "district": "区县",
            "phone": "主联系电话",
            "additional_phones": "补充联系电话",
            "email": "主邮箱",
            "additional_emails": "补充邮箱",
            "unified_social_credit_code": "统一社会信用代码",
            "taxpayer_identification_number": "纳税人识别号",
            "business_registration_number": "工商注册号",
            "organization_code": "组织机构代码",
            "insured_personnel_count": "参保人数",
            "company_type": "企业类型",
            "industry": "所属行业",
            "former_name": "曾用名",
            "registered_address": "注册地址",
            "latest_annual_report_address": "年报地址",
            "website": "官网",
            "business_scope": "经营范围",
            "match_status": "本地匹配状态",
        },
    }

    payload = {
        "project": "XunFei_Rag",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "principle": "当前验收包仅保留原始记录级数据，不做 chunk，不做 embedding，不做索引入库。",
        "datasets": [
            {
                "dataset_key": "policy",
                "display_name": "政策信息",
                "target_count": TARGET_COUNT,
                "selected_count": policy_manifest["selected_counts"]["policy_article_units_selected"],
                "record_granularity": "条文级原始单元",
                "primary_raw_file": relative(POLICY_DIR / "policy_article_raw_1000.csv"),
                "supplementary_raw_files": [
                    relative(POLICY_DIR / "policy_document_fulltext_support.csv"),
                    relative(POLICY_DIR / "policy_local_fulltext_ah_hf.csv"),
                ],
                "raw_source_fields": [
                    {"name": field, "description": descriptions["policy"].get(field, "")}
                    for field in policy_primary_fields
                ],
                "supplementary_source_fields": [
                    {
                        "file": relative(POLICY_DIR / "policy_local_fulltext_ah_hf.csv"),
                        "fields": [
                            {"name": field, "description": descriptions["policy"].get(field, "")}
                            for field in policy_local_fields
                        ],
                    }
                ],
                "target_clean_fields": [
                    {"name": "policy_unit_id", "type": "string", "required": True, "source_fields": ["id"], "cleaning": "trim + 唯一性校验"},
                    {"name": "policy_doc_id", "type": "string", "required": True, "source_fields": ["law_id"], "cleaning": "trim + 与法规母本关联"},
                    {"name": "policy_title", "type": "string", "required": True, "source_fields": ["law_title"], "cleaning": "清理空白字符 + 标题归一"},
                    {"name": "article_number", "type": "string", "required": True, "source_fields": ["article_number"], "cleaning": "保留原始条号格式"},
                    {"name": "article_title", "type": "string", "required": False, "source_fields": ["article_title"], "cleaning": "空值置空字符串"},
                    {"name": "chapter", "type": "string", "required": False, "source_fields": ["chapter"], "cleaning": "章节名称标准化"},
                    {"name": "publish_date", "type": "date", "required": True, "source_fields": ["publish_date"], "cleaning": "统一到 YYYY-MM-DD"},
                    {"name": "category", "type": "string", "required": True, "source_fields": ["category"], "cleaning": "枚举归一，如招投标/政府采购/公共资源交易"},
                    {"name": "region", "type": "string", "required": True, "source_fields": ["region"], "cleaning": "统一国家/省/市命名"},
                    {"name": "region_level", "type": "string", "required": True, "source_fields": ["region_level"], "cleaning": "统一 national/province/city"},
                    {"name": "policy_text", "type": "text", "required": True, "source_fields": ["text"], "cleaning": "保留原文，不做切块，仅去除多余空白"},
                ],
            },
            {
                "dataset_key": "tender",
                "display_name": "招标信息",
                "target_count": TARGET_COUNT,
                "selected_count": tender_manifest["selected_counts"]["tender_notice_selected"],
                "record_granularity": "公告级原始单元",
                "primary_raw_file": relative(TENDER_DIR / "tender_notice_raw_1000.csv"),
                "raw_source_fields": [
                    {"name": field, "description": descriptions["tender"].get(field, "")}
                    for field in tender_fields
                ],
                "target_clean_fields": [
                    {"name": "notice_id", "type": "string", "required": True, "source_fields": ["doc_id"], "cleaning": "trim + 去重"},
                    {"name": "project_id", "type": "string", "required": True, "source_fields": ["project_id"], "cleaning": "项目编号标准化"},
                    {"name": "source_url", "type": "string", "required": True, "source_fields": ["link"], "cleaning": "URL 完整性校验"},
                    {"name": "business_type", "type": "string", "required": True, "source_fields": ["type"], "cleaning": "统一到工程/货物/服务/政府采购等枚举"},
                    {"name": "notice_stage", "type": "string", "required": True, "source_fields": ["stage"], "cleaning": "统一采购/中标/变更等阶段枚举"},
                    {"name": "publish_date", "type": "date", "required": True, "source_fields": ["event_time"], "cleaning": "统一到 YYYY-MM-DD"},
                    {"name": "province", "type": "string", "required": True, "source_fields": ["province"], "cleaning": "行政区标准化"},
                    {"name": "city", "type": "string", "required": True, "source_fields": ["city"], "cleaning": "行政区标准化"},
                    {"name": "district", "type": "string", "required": False, "source_fields": ["town"], "cleaning": "行政区标准化"},
                    {"name": "category", "type": "string", "required": False, "source_fields": ["category", "sub_category"], "cleaning": "一级/二级分类统一"},
                    {"name": "budget_amount", "type": "number", "required": False, "source_fields": ["budget_amount"], "cleaning": "金额转 numeric，无法解析保留原值"},
                    {"name": "purchaser", "type": "string", "required": False, "source_fields": ["purchaser"], "cleaning": "主体名称去空格、括号统一"},
                    {"name": "agency", "type": "string", "required": False, "source_fields": ["agency"], "cleaning": "代理机构名称去空格、括号统一"},
                    {"name": "project_name", "type": "string", "required": False, "source_fields": ["project_name", "procurement_title"], "cleaning": "优先 project_name，缺失回退 procurement_title"},
                    {"name": "winner_company", "type": "string", "required": False, "source_fields": ["bid_company"], "cleaning": "多家中标单位拆分为数组字段"},
                    {"name": "winner_amount", "type": "number", "required": False, "source_fields": ["bid_amount"], "cleaning": "金额转 numeric"},
                    {"name": "bid_date", "type": "date", "required": False, "source_fields": ["bid_date"], "cleaning": "统一日期格式"},
                    {"name": "procurement_text", "type": "text", "required": True, "source_fields": ["procurement_content"], "cleaning": "正文去噪、保留原始段落结构"},
                    {"name": "result_text_raw", "type": "text", "required": False, "source_fields": ["bid_content"], "cleaning": "保留原始结果正文或 JSON 字符串，后续再解析附件与表格"},
                ],
            },
            {
                "dataset_key": "company",
                "display_name": "企业信息",
                "target_count": TARGET_COUNT,
                "selected_count": company_manifest["selected_counts"]["company_profile_selected"],
                "record_granularity": "企业画像级原始单元",
                "primary_raw_file": relative(COMPANY_DIR / "company_profile_raw_1000.csv"),
                "raw_source_fields": [
                    {"name": field, "description": descriptions["company"].get(field, "")}
                    for field in company_fields
                ],
                "target_clean_fields": [
                    {"name": "company_name", "type": "string", "required": True, "source_fields": ["company_name"], "cleaning": "主体名称去空格、括号与全半角统一"},
                    {"name": "uscc", "type": "string", "required": True, "source_fields": ["unified_social_credit_code"], "cleaning": "18 位信用代码格式校验"},
                    {"name": "business_status", "type": "string", "required": True, "source_fields": ["business_status"], "cleaning": "统一存续/开业/注销等枚举"},
                    {"name": "legal_representative", "type": "string", "required": False, "source_fields": ["legal_representative"], "cleaning": "姓名清洗"},
                    {"name": "industry", "type": "string", "required": False, "source_fields": ["industry"], "cleaning": "行业标签归一"},
                    {"name": "registered_capital", "type": "number_or_text", "required": False, "source_fields": ["registered_capital"], "cleaning": "抽取数值与币种，保留原始值备查"},
                    {"name": "paid_in_capital", "type": "number_or_text", "required": False, "source_fields": ["paid_in_capital"], "cleaning": "抽取数值与币种，保留原始值备查"},
                    {"name": "establishment_date", "type": "date", "required": False, "source_fields": ["establishment_date"], "cleaning": "统一到 YYYY-MM-DD"},
                    {"name": "approval_date", "type": "date", "required": False, "source_fields": ["approval_date"], "cleaning": "统一到 YYYY-MM-DD"},
                    {"name": "province", "type": "string", "required": True, "source_fields": ["province"], "cleaning": "行政区标准化"},
                    {"name": "city", "type": "string", "required": True, "source_fields": ["city"], "cleaning": "行政区标准化"},
                    {"name": "district", "type": "string", "required": False, "source_fields": ["district"], "cleaning": "行政区标准化"},
                    {"name": "phone_list", "type": "array[string]", "required": False, "source_fields": ["phone", "additional_phones"], "cleaning": "手机号/固话去重拆分"},
                    {"name": "email_list", "type": "array[string]", "required": False, "source_fields": ["email", "additional_emails"], "cleaning": "邮箱拆分去重"},
                    {"name": "registered_address", "type": "string", "required": False, "source_fields": ["registered_address"], "cleaning": "地址文本标准化"},
                    {"name": "website", "type": "string", "required": False, "source_fields": ["website"], "cleaning": "域名标准化，空值剔除"},
                    {"name": "business_scope", "type": "text", "required": False, "source_fields": ["business_scope"], "cleaning": "保留原文，后续可做能力标签抽取"},
                ],
            },
        ],
    }

    write_json(OUTPUT_DIR / "data_source_cleaning_schema.json", payload)


def build_retrieval_strategy_md(
    policy_manifest: dict[str, object],
    tender_manifest: dict[str, object],
    company_manifest: dict[str, object],
) -> None:
    content = f"""# 检索策略建议

## 前提

- 当前验收包只保留原始记录级数据，不做 chunk。
- 因此当前推荐的检索单元是整条招标记录、整条政策条文、整条企业画像。
- 后续如果进入生成式问答阶段，再按命中的数据源分别决定是否切块。

## 1. 招标信息

- 当前数据量：{tender_manifest["selected_counts"]["tender_notice_selected"]} 条
- 检索单元：公告级记录
- 推荐策略：`SQL/结构化过滤 + BM25 + 向量` 的混合检索

原因：

- 招标查询天然带强筛选条件，如地区、时间、阶段、采购类型、预算区间、采购人、代理机构。
- 同时又存在大量关键词刚需，如资质条件、评分办法、履约期限、联合体、保证金、资格要求。
- 还存在“找相似项目”的语义需求，比如“找和某项目规模相近、行业相近、资格要求相近的历史公告”。

建议流程：

1. 先用 SQL / 结构化索引过滤地区、阶段、时间、类型、预算、主体。
2. 对 `procurement_title + project_name + procurement_content + bid_content` 做 BM25，保障资格条款、评分词、法定术语的精确命中。
3. 对 `procurement_content + bid_content` 做向量检索，用于相似项目、相似招标要求、相似成交结果召回。
4. 最后统一 rerank。

结论：

- 招标信息不适合纯 SQL，也不适合纯向量。
- 最优是混合检索，其中 SQL 负责硬过滤，BM25 负责精确条款命中，向量负责相似项目召回。

## 2. 政策信息

- 当前数据量：{policy_manifest["selected_counts"]["policy_article_units_selected"]} 条条文 + {policy_manifest["selected_counts"]["local_fulltext_documents_selected"]} 条本地政策全文
- 检索单元：条文级原始单元，全文母本用于回溯
- 推荐策略：`元数据过滤 + BM25 为主 + 向量补充` 的混合检索

原因：

- 政策问答里“条文精确命中”比“语义近似”更重要。
- 用户经常问的是资格、程序、时限、处罚、适用范围、是否允许、是否必须等问题，这类问题高度依赖原词原句。
- 但自然语言提问和法规表述之间往往存在改写，所以仍然需要向量召回补位。

建议流程：

1. 先按 `region / region_level / category / project_type / publish_date` 做过滤。
2. 对 `law_title + article_number + article_title + text` 建 BM25，优先保证法规标题、条号、术语、处罚条款的命中。
3. 对 `text` 做向量检索，用于“换一种说法”时的语义召回。
4. 命中条文后，再回查全文母本，保证回答时可补上下文。

结论：

- 政策信息不建议纯向量。
- 最合理是 BM25 主导的混合检索，向量只做补充，全文母本用于证据回溯。

## 3. 企业信息

- 当前数据量：{company_manifest["selected_counts"]["company_profile_selected"]} 条
- 检索单元：企业画像级记录
- 推荐策略：`SQL 为主，BM25 次之，向量按需补充`

原因：

- 企业查询通常是主体直查或筛选，如企业名、统一社会信用代码、法定代表人、地区、行业、经营状态。
- 这些场景天然适合结构化查询和模糊匹配。
- 只有在“找具备某类能力的企业”“根据经营范围做相似企业推荐”时，向量检索才更有价值。

建议流程：

1. 先用 SQL 或倒排索引命中 `company_name / unified_social_credit_code / legal_representative / province / city / industry / business_status`。
2. 对 `business_scope + industry + former_name` 做 BM25，支持能力关键词、历史名称、行业术语查询。
3. 对 `business_scope` 做向量检索，仅用于能力相似企业发现和自然语言模糊召回。

结论：

- 企业信息不建议把向量作为主检索方案。
- 应采用 SQL 主导的混合检索：主体查找靠 SQL，经营范围补充 BM25，能力发现再上向量。

## 最终建议

| 数据源 | 主策略 | 辅策略 | 是否推荐纯向量 | 是否推荐纯 SQL |
| --- | --- | --- | --- | --- |
| 招标信息 | 混合检索 | SQL + BM25 + 向量 | 否 | 否 |
| 政策信息 | BM25 主导混合检索 | 元数据过滤 + 向量补充 | 否 | 否 |
| 企业信息 | SQL 主导混合检索 | BM25 + 向量补充 | 否 | 仅主体直查时可行，但不覆盖能力检索 |

"""

    (OUTPUT_DIR / "retrieval_strategy.md").write_text(content, encoding="utf-8")


def build_readme(
    policy_manifest: dict[str, object],
    tender_manifest: dict[str, object],
    company_manifest: dict[str, object],
) -> None:
    content = f"""# 验收包说明

本目录用于满足当前阶段的三项验收目标：

1. `raw_sources/`：三类原始数据源文件
2. `data_source_cleaning_schema.json`：字段与清洗目标说明
3. `retrieval_strategy.md`：三类数据源检索策略建议

## 当前口径

- 不切块
- 不做 embedding
- 不做索引构建
- 仅保留原始记录级数据，保证先把数据量和字段口径固定下来

## 数据量

- 政策信息：{policy_manifest["selected_counts"]["policy_article_units_selected"]} 条条文原始单元
- 招标信息：{tender_manifest["selected_counts"]["tender_notice_selected"]} 条公告原始单元
- 企业信息：{company_manifest["selected_counts"]["company_profile_selected"]} 条企业原始画像

## 目录

- `raw_sources/01_policy/`
  - `policy_article_raw_1000.csv`
  - `policy_document_fulltext_support.csv`
  - `policy_local_fulltext_ah_hf.csv`
  - `source_manifest.json`
- `raw_sources/02_tender/`
  - `tender_notice_raw_1000.csv`
  - `source_manifest.json`
- `raw_sources/03_company/`
  - `company_profile_raw_1000.csv`
  - `source_manifest.json`
- `raw_sources/original_file_samples_manifest.json`
  - 政策原始 HTML 样本已落地
  - 招标原始 HTML/PDF 样本保留下载结果与失败原因

## 说明

- 当前仓库现成、可复用且未切块的上游主要是 CSV 快照层，所以本次验收包以 CSV 原始记录为主。
- 其中招标和政策数据保留了正文、原始链接、附件链接/结果原文；企业数据保留了主体画像核心字段。
- 已补充下载了 3 份政策 HTML 原件到 `raw_sources/01_policy/html_samples/`。
- 合肥招标站点的原始 HTML/PDF 对脚本下载返回 403，失败信息已记录在 `raw_sources/original_file_samples_manifest.json`，后续如果改用浏览器态 Cookie 或更强的采集策略，可以继续补齐。

"""

    (OUTPUT_DIR / "README.md").write_text(content, encoding="utf-8")


def build_global_manifest(
    policy_manifest: dict[str, object],
    tender_manifest: dict[str, object],
    company_manifest: dict[str, object],
) -> None:
    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "targets": {
            "policy": TARGET_COUNT,
            "tender": TARGET_COUNT,
            "company": TARGET_COUNT,
        },
        "datasets": [
            policy_manifest,
            tender_manifest,
            company_manifest,
        ],
    }
    write_json(RAW_DIR / "raw_source_manifest.json", payload)


def main() -> None:
    ensure_dirs()
    policy_manifest = build_policy_raw_assets()
    tender_manifest = build_tender_raw_assets()
    company_manifest = build_company_raw_assets()
    build_global_manifest(policy_manifest, tender_manifest, company_manifest)
    build_schema_json(policy_manifest, tender_manifest, company_manifest)
    build_retrieval_strategy_md(policy_manifest, tender_manifest, company_manifest)
    build_readme(policy_manifest, tender_manifest, company_manifest)

    print("acceptance_assets generated")
    print(f"policy={policy_manifest['selected_counts']['policy_article_units_selected']}")
    print(f"tender={tender_manifest['selected_counts']['tender_notice_selected']}")
    print(f"company={company_manifest['selected_counts']['company_profile_selected']}")


if __name__ == "__main__":
    main()
