from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from build_supplemental_assets import (
    DATA_DIR,
    collapse_text,
    infer_project_type,
    normalize_date,
    read_csv_rows,
    split_chunks,
    write_csv,
)


POLICY_DIR = DATA_DIR / "01_policy"
TENDER_DIR = DATA_DIR / "02_tender"
COMPANY_DIR = DATA_DIR / "03_company"
RISK_DIR = DATA_DIR / "04_risk"

POLICY_ARTICLE_PATH = POLICY_DIR / "policy_national_article_units.csv"
RISK_ENRICHED_PATH = RISK_DIR / "risk_penalty_records_enriched.csv"
AHZB_CURATED_PATH = TENDER_DIR / "tender_docs_ahzb_curated.csv"
ANOMALY_RECORD_PATH = RISK_DIR / "risk_anomaly_records.csv"

ANOMALY_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("\u7ec8\u6b62", "terminated"),
    ("\u5e9f\u6807", "invalid_bid"),
    ("\u6d41\u6807", "failed_bid"),
    ("\u5f02\u5e38", "abnormal"),
    ("\u6682\u505c", "suspended"),
    ("\u8d28\u7591", "question"),
    ("\u6295\u8bc9", "complaint"),
    ("\u53d8\u66f4", "change"),
    ("\u66f4\u6b63", "correction"),
    ("\u6f84\u6e05", "clarification"),
    ("\u53d6\u6d88", "canceled"),
)


def policy_category_from_meta(meta: dict[str, str]) -> str:
    title = collapse_text(meta.get("name_zh", ""))
    hierarchy = collapse_text(meta.get("hierarchy_path", ""))
    category = hierarchy.split("/")[0] if hierarchy else ""
    if "政府采购" in title or "政府采购" in category:
        return "政府采购"
    if "公共资源" in title or "公共资源" in category:
        return "公共资源交易"
    if "招标" in title or "投标" in title or "招投标" in category:
        return "招投标"
    return category or "综合法规"


def build_policy_article_assets() -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    meta_rows = read_csv_rows(POLICY_DIR / "policy_src_legal_metadata.csv")
    article_rows = read_csv_rows(POLICY_DIR / "policy_src_legal_articles.csv")
    meta_by_law_id = {row["law_id"]: row for row in meta_rows}
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    policy_rows: list[dict[str, str]] = []
    chunk_rows: list[dict[str, str]] = []

    for article in article_rows:
        meta = meta_by_law_id.get(article.get("law_id", ""))
        if not meta:
            continue

        law_title = collapse_text(meta.get("name_zh", ""))
        article_number = collapse_text(article.get("article_number", ""))
        article_title = collapse_text(article.get("article_title", ""))
        chapter = collapse_text(article.get("chapter", ""))
        content_text = collapse_text(article.get("content_text", ""))
        if not content_text:
            continue

        category = policy_category_from_meta(meta)
        project_type = infer_project_type(f"{law_title}\n{article_title}\n{content_text}", category)
        unit_id = f"article:{article['id']}"
        title_parts = [law_title, article_number, article_title]
        title = " ".join(part for part in title_parts if part)
        text_parts = [law_title]
        if chapter:
            text_parts.append(chapter)
        if article_number or article_title:
            text_parts.append(" ".join(part for part in [article_number, article_title] if part))
        text_parts.append(content_text)
        article_text = "\n".join(part for part in text_parts if part)

        policy_rows.append(
            {
                "id": unit_id,
                "law_id": article.get("law_id", ""),
                "law_title": law_title,
                "article_number": article_number,
                "article_title": article_title,
                "chapter": chapter,
                "publish_date": normalize_date(meta.get("publish_date", "")),
                "category": category,
                "project_type": project_type,
                "region": "全国",
                "region_level": "national",
                "source_type": "legal_article",
                "text": article_text,
                "word_count": str(len(article_text)),
                "created_at": created_at,
            }
        )

        chunks = split_chunks(article_text, chunk_size=900)
        total_chunks = str(len(chunks))
        for index, chunk in enumerate(chunks):
            chunk_rows.append(
                {
                    "doc_id": f"{unit_id}:chunk:{index}",
                    "source_table": "policy_article_units",
                    "title": f"{title} (第{index + 1}部分)",
                    "rule_title": law_title,
                    "event_time": normalize_date(meta.get("publish_date", "")),
                    "release_time": normalize_date(meta.get("publish_date", "")),
                    "ingest_time": created_at,
                    "is_chunked": "1" if len(chunks) > 1 else "0",
                    "chunk_id": str(index),
                    "total_chunks": total_chunks,
                    "original_doc_id": unit_id,
                    "text": chunk,
                    "source_url": "",
                    "created_at": created_at,
                    "updated_at": created_at,
                }
            )

    policy_rows.sort(key=lambda row: row["id"])
    chunk_rows.sort(key=lambda row: row["doc_id"])
    return policy_rows, chunk_rows


def build_company_indexes() -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]], dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    local_credit: dict[str, dict[str, str]] = {}
    local_name: dict[str, dict[str, str]] = {}
    for row in read_csv_rows(COMPANY_DIR / "company_profiles_local_matched.csv"):
        if row.get("match_status") != "成功":
            continue
        credit = collapse_text(row.get("unified_social_credit_code", ""))
        name = collapse_text(row.get("company_name", ""))
        if credit and credit not in local_credit:
            local_credit[credit] = row
        if name and name not in local_name:
            local_name[name] = row

    national_credit: dict[str, dict[str, str]] = {}
    national_name: dict[str, dict[str, str]] = {}
    for row in read_csv_rows(COMPANY_DIR / "company_profiles_national.csv"):
        credit = collapse_text(row.get("USCC", ""))
        name = collapse_text(row.get("title", ""))
        if credit and credit not in national_credit:
            national_credit[credit] = row
        if name and name not in national_name:
            national_name[name] = row

    return local_credit, local_name, national_credit, national_name


def build_risk_assets() -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    local_credit, local_name, national_credit, national_name = build_company_indexes()
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    risk_rows: list[dict[str, str]] = []
    chunk_rows: list[dict[str, str]] = []

    for row in read_csv_rows(RISK_DIR / "risk_src_illegal_behavior.csv"):
        credit_code = collapse_text(row.get("credit_code", ""))
        company_name = collapse_text(row.get("company_name", ""))

        matched = None
        matched_source = ""
        if credit_code and credit_code in local_credit:
            matched = local_credit[credit_code]
            matched_source = "company_info"
        elif company_name and company_name in local_name:
            matched = local_name[company_name]
            matched_source = "company_info"
        elif credit_code and credit_code in national_credit:
            matched = national_credit[credit_code]
            matched_source = "ods_company_detail"
        elif company_name and company_name in national_name:
            matched = national_name[company_name]
            matched_source = "ods_company_detail"

        matched_company_name = ""
        matched_credit_code = ""
        matched_status = ""
        matched_province = ""
        matched_city = ""
        matched_industry = ""
        matched_legal_person = ""

        if matched_source == "company_info" and matched:
            matched_company_name = collapse_text(matched.get("company_name", ""))
            matched_credit_code = collapse_text(matched.get("unified_social_credit_code", ""))
            matched_status = collapse_text(matched.get("business_status", ""))
            matched_province = collapse_text(matched.get("province", ""))
            matched_city = collapse_text(matched.get("city", ""))
            matched_industry = collapse_text(matched.get("industry", ""))
            matched_legal_person = collapse_text(matched.get("legal_representative", ""))
        elif matched_source == "ods_company_detail" and matched:
            matched_company_name = collapse_text(matched.get("title", ""))
            matched_credit_code = collapse_text(matched.get("USCC", ""))
            matched_status = collapse_text(matched.get("status", ""))
            matched_province = collapse_text(matched.get("province", ""))
            matched_city = collapse_text(matched.get("city", ""))
            matched_industry = collapse_text(matched.get("industry", ""))
            matched_legal_person = collapse_text(matched.get("corporation", ""))

        risk_text_parts = [
            f"企业名称：{company_name}",
            f"统一社会信用代码：{credit_code}" if credit_code else "",
            f"发布日期：{normalize_date(row.get('publish_date', ''))}",
            f"处罚日期：{normalize_date(row.get('penalty_date', ''))}",
            f"执法单位：{collapse_text(row.get('law_enforcement_unit', ''))}",
            f"违法行为：{collapse_text(row.get('illegal_behavior_details', ''))}",
            f"处罚结果：{collapse_text(row.get('penalty_result', ''))}",
            f"处罚依据：{collapse_text(row.get('penalty_basis', ''))}",
            f"企业状态：{matched_status}" if matched_status else "",
            f"所属地区：{matched_province} {matched_city}".strip() if matched_province or matched_city else "",
            f"行业：{matched_industry}" if matched_industry else "",
        ]
        risk_text = "\n".join(item for item in risk_text_parts if item)

        risk_id = f"risk:{row.get('id', '')}"
        risk_rows.append(
            {
                "id": risk_id,
                "company_name": company_name,
                "credit_code": credit_code,
                "publish_date": normalize_date(row.get("publish_date", "")),
                "penalty_date": normalize_date(row.get("penalty_date", "")),
                "publish_deadline": normalize_date(row.get("publish_deadline", "")),
                "law_enforcement_unit": collapse_text(row.get("law_enforcement_unit", "")),
                "illegal_behavior_details": collapse_text(row.get("illegal_behavior_details", "")),
                "penalty_result": collapse_text(row.get("penalty_result", "")),
                "penalty_basis": collapse_text(row.get("penalty_basis", "")),
                "company_address": collapse_text(row.get("company_address", "")),
                "matched_company_name": matched_company_name,
                "matched_credit_code": matched_credit_code,
                "matched_legal_person": matched_legal_person,
                "matched_status": matched_status,
                "matched_province": matched_province,
                "matched_city": matched_city,
                "matched_industry": matched_industry,
                "matched_source": matched_source,
                "text": risk_text,
                "created_at": created_at,
            }
        )
        chunk_rows.append(
            {
                "doc_id": f"{risk_id}:chunk:0",
                "source_table": "risk_records_enriched",
                "title": f"{company_name} 风险记录",
                "rule_title": company_name,
                "event_time": normalize_date(row.get("penalty_date", "")) or normalize_date(row.get("publish_date", "")),
                "release_time": normalize_date(row.get("publish_date", "")),
                "ingest_time": created_at,
                "is_chunked": "0",
                "chunk_id": "0",
                "total_chunks": "1",
                "original_doc_id": risk_id,
                "text": risk_text,
                "source_url": "",
                "created_at": created_at,
                "updated_at": created_at,
            }
        )

    risk_rows.sort(key=lambda row: row["id"])
    chunk_rows.sort(key=lambda row: row["doc_id"])
    return risk_rows, chunk_rows


def open_writer(path: Path, fieldnames: list[str]) -> tuple[object, csv.DictWriter]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("w", encoding="utf-8-sig", newline="")
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    return handle, writer


def compose_ahzb_text(role: str, row: dict[str, str]) -> str:
    title = collapse_text(row.get("procurement_title", "")) or collapse_text(row.get("project_name", ""))
    parts = [
        f"project_title: {title}" if title else "",
        f"project_id: {collapse_text(row.get('_id', ''))}" if row.get("_id") else "",
        f"business_type: {collapse_text(row.get('type', ''))}" if row.get("type") else "",
        f"stage: {collapse_text(row.get('stage', ''))}" if row.get("stage") else "",
        f"publish_date: {normalize_date(row.get('date', ''))}" if row.get("date") else "",
        f"purchaser: {collapse_text(row.get('purchaser', ''))}" if row.get("purchaser") else "",
        f"agency: {collapse_text(row.get('agency', ''))}" if row.get("agency") else "",
        f"budget_amount: {collapse_text(row.get('budget_amount', ''))}" if row.get("budget_amount") else "",
        f"winning_company: {collapse_text(row.get('bid_company', ''))}" if row.get("bid_company") else "",
        f"winning_amount: {collapse_text(row.get('bid_amount', ''))}" if row.get("bid_amount") else "",
        f"winning_date: {normalize_date(row.get('bid_date', ''))}" if row.get("bid_date") else "",
    ]
    if role == "procurement":
        parts.append(collapse_text(row.get("procurement_content", "")))
    else:
        parts.append(collapse_text(row.get("bid_content", "")))
    return "\n".join(part for part in parts if part)


def anomaly_tags(text: str) -> list[str]:
    tags: list[str] = []
    for keyword, tag in ANOMALY_KEYWORDS:
        if keyword in text:
            tags.append(tag)
    return tags


def main() -> None:
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    policy_rows, _policy_chunks = build_policy_article_assets()
    write_csv(
        POLICY_ARTICLE_PATH,
        [
            "id",
            "law_id",
            "law_title",
            "article_number",
            "article_title",
            "chapter",
            "publish_date",
            "category",
            "project_type",
            "region",
            "region_level",
            "source_type",
            "text",
            "word_count",
            "created_at",
        ],
        policy_rows,
    )

    risk_rows, _risk_chunks = build_risk_assets()
    write_csv(
        RISK_ENRICHED_PATH,
        [
            "id",
            "company_name",
            "credit_code",
            "publish_date",
            "penalty_date",
            "publish_deadline",
            "law_enforcement_unit",
            "illegal_behavior_details",
            "penalty_result",
            "penalty_basis",
            "company_address",
            "matched_company_name",
            "matched_credit_code",
            "matched_legal_person",
            "matched_status",
            "matched_province",
            "matched_city",
            "matched_industry",
            "matched_source",
            "text",
            "created_at",
        ],
        risk_rows,
    )

    ahzb_doc_handle, ahzb_doc_writer = open_writer(
        AHZB_CURATED_PATH,
        [
            "doc_id",
            "project_id",
            "link",
            "type",
            "stage",
            "event_time",
            "province",
            "city",
            "town",
            "category",
            "sub_category",
            "budget_amount",
            "purchaser",
            "agency",
            "project_name",
            "procurement_title",
            "bid_company",
            "bid_amount",
            "bid_date",
            "procurement_content",
            "bid_content",
            "procurement_content_length",
            "bid_content_length",
            "created_at",
        ],
    )
    anomaly_handle, anomaly_writer = open_writer(
        ANOMALY_RECORD_PATH,
        [
            "id",
            "source_doc_id",
            "project_id",
            "title",
            "stage",
            "event_time",
            "type",
            "category",
            "purchaser",
            "agency",
            "bid_company",
            "bid_amount",
            "source_url",
            "anomaly_tags",
            "text",
            "created_at",
        ],
    )

    seen: set[tuple[str, str, str, str, str, str]] = set()
    doc_count = 0
    anomaly_count = 0

    with (TENDER_DIR / "tender_src_ahzb_raw.csv").open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = (
                collapse_text(row.get("_id", "")),
                collapse_text(row.get("stage", "")),
                collapse_text(row.get("procurement_title", "")),
                collapse_text(row.get("bid_company", "")),
                collapse_text(row.get("bid_amount", "")),
                collapse_text(row.get("bid_date", "")),
            )
            if key in seen:
                continue
            seen.add(key)

            procurement_content = collapse_text(row.get("procurement_content", ""))
            bid_content = collapse_text(row.get("bid_content", ""))
            if not procurement_content and not bid_content:
                continue

            doc_count += 1
            doc_id = f"ahzb:{doc_count}"
            ahzb_doc_writer.writerow(
                {
                    "doc_id": doc_id,
                    "project_id": collapse_text(row.get("_id", "")),
                    "link": collapse_text(row.get("link", "")),
                    "type": collapse_text(row.get("type", "")),
                    "stage": collapse_text(row.get("stage", "")),
                    "event_time": normalize_date(row.get("date", "")),
                    "province": collapse_text(row.get("province", "")),
                    "city": collapse_text(row.get("city", "")),
                    "town": collapse_text(row.get("town", "")),
                    "category": collapse_text(row.get("category", "")),
                    "sub_category": collapse_text(row.get("sub_category", "")),
                    "budget_amount": collapse_text(row.get("budget_amount", "")),
                    "purchaser": collapse_text(row.get("purchaser", "")),
                    "agency": collapse_text(row.get("agency", "")),
                    "project_name": collapse_text(row.get("project_name", "")),
                    "procurement_title": collapse_text(row.get("procurement_title", "")),
                    "bid_company": collapse_text(row.get("bid_company", "")),
                    "bid_amount": collapse_text(row.get("bid_amount", "")),
                    "bid_date": normalize_date(row.get("bid_date", "")),
                    "procurement_content": procurement_content,
                    "bid_content": bid_content,
                    "procurement_content_length": str(len(procurement_content)),
                    "bid_content_length": str(len(bid_content)),
                    "created_at": created_at,
                }
            )

            title = collapse_text(row.get("procurement_title", "")) or collapse_text(row.get("project_name", "")) or collapse_text(row.get("_id", ""))
            full_text = "\n".join(
                [
                    title,
                    collapse_text(row.get("stage", "")),
                    procurement_content,
                    bid_content,
                ]
            )
            tags = anomaly_tags(full_text)
            if tags:
                anomaly_count += 1
                anomaly_id = f"anomaly:{anomaly_count}"
                anomaly_text = "\n".join(
                    [
                        f"project_title: {title}",
                        f"project_id: {collapse_text(row.get('_id', ''))}",
                        f"stage: {collapse_text(row.get('stage', ''))}",
                        f"business_type: {collapse_text(row.get('type', ''))}",
                        f"anomaly_tags: {','.join(tags)}",
                        f"purchaser: {collapse_text(row.get('purchaser', ''))}" if row.get("purchaser") else "",
                        f"agency: {collapse_text(row.get('agency', ''))}" if row.get("agency") else "",
                        f"winning_company: {collapse_text(row.get('bid_company', ''))}" if row.get("bid_company") else "",
                        f"winning_amount: {collapse_text(row.get('bid_amount', ''))}" if row.get("bid_amount") else "",
                        procurement_content,
                        bid_content,
                    ]
                )
                anomaly_writer.writerow(
                    {
                        "id": anomaly_id,
                        "source_doc_id": doc_id,
                        "project_id": collapse_text(row.get("_id", "")),
                        "title": title,
                        "stage": collapse_text(row.get("stage", "")),
                        "event_time": normalize_date(row.get("date", "")),
                        "type": collapse_text(row.get("type", "")),
                        "category": collapse_text(row.get("category", "")),
                        "purchaser": collapse_text(row.get("purchaser", "")),
                        "agency": collapse_text(row.get("agency", "")),
                        "bid_company": collapse_text(row.get("bid_company", "")),
                        "bid_amount": collapse_text(row.get("bid_amount", "")),
                        "source_url": collapse_text(row.get("link", "")),
                        "anomaly_tags": ",".join(tags),
                        "text": anomaly_text,
                        "created_at": created_at,
                    }
                )

    ahzb_doc_handle.close()
    anomaly_handle.close()

    print(f"policy_article_units={len(policy_rows)}")
    print(f"risk_records_enriched={len(risk_rows)}")
    print(f"ahzb_curated={doc_count}")
    print(f"anomaly_event_records={anomaly_count}")


if __name__ == "__main__":
    main()
