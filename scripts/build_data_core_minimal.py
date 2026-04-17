from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path


csv.field_size_limit(1024 * 1024 * 512)

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT / "data_new"
CORE = ROOT / "data_core"

GOV_DIR = CORE / "00_governance"
META_DIR = CORE / "01_metadata"
STRUCT_DIR = CORE / "02_structured"
TEXT_DIR = CORE / "03_text"
CHUNK_DIR = CORE / "04_chunks"


def ensure_dirs() -> None:
    for path in [GOV_DIR, META_DIR, STRUCT_DIR, TEXT_DIR, CHUNK_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            clean_row = {}
            for key, value in row.items():
                clean_key = key.lstrip("\ufeff") if key else key
                clean_row[clean_key] = value
            rows.append(clean_row)
        return rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def norm(value: str | None) -> str:
    return (value or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def compact_text(value: str | None) -> str:
    text = norm(value)
    lines = [line.strip() for line in text.split("\n")]
    kept = [line for line in lines if line]
    return "\n".join(kept)


def hash_text(value: str | None) -> str:
    text = compact_text(value)
    if not text:
        return ""
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def join_non_empty(parts: list[str], sep: str = "\n\n") -> str:
    return sep.join(part for part in parts if part)


def list_to_json(values: list[str]) -> str:
    clean = [value for value in values if value]
    return json.dumps(clean, ensure_ascii=False)


def build_policy_tables() -> tuple[int, int, int, int]:
    meta_rows = read_csv_rows(UPSTREAM / "01_policy" / "policy_curated_meta.csv")
    doc_rows = read_csv_rows(UPSTREAM / "01_policy" / "policy_curated_docs.csv")
    clause_rows = read_csv_rows(UPSTREAM / "01_policy" / "policy_national_article_units.csv")

    docs_by_id = {row["id"]: row for row in doc_rows}

    policy_meta = []
    policy_text = []
    for row in meta_rows:
        doc = docs_by_id.get(row["id"], {})
        policy_meta.append(
            {
                "doc_id": norm(row.get("id")),
                "title": norm(row.get("title")),
                "publish_date": norm(row.get("publish_date")),
                "region": norm(row.get("region")),
                "region_level": norm(row.get("region_level")),
                "category": norm(row.get("category")),
                "project_type": norm(row.get("project_type")),
                "source_type": norm(row.get("source_type")),
                "source_name": norm(row.get("source_name")),
                "corpus_tag": norm(row.get("corpus_tag")),
                "source_url": norm(row.get("source_url")),
                "document_url": norm(row.get("document_url")),
                "upstream_table": norm(row.get("upstream_table")),
                "has_full_text": "1" if docs_by_id.get(row["id"]) else "0",
            }
        )

        if doc:
            full_text = compact_text(doc.get("content"))
            policy_text.append(
                {
                    "doc_id": norm(doc.get("id")),
                    "title": norm(doc.get("title")),
                    "full_text": full_text,
                    "word_count": norm(doc.get("word_count")),
                    "text_hash": hash_text(full_text),
                    "source": norm(doc.get("source")),
                    "source_url": norm(doc.get("url")),
                    "publish_time": norm(doc.get("publish_time")),
                }
            )

    clause_structured = []
    clause_text = []
    for row in clause_rows:
        clause_structured.append(
            {
                "clause_id": norm(row.get("id")),
                "source_doc_key": norm(row.get("law_id")),
                "law_title": norm(row.get("law_title")),
                "chapter": norm(row.get("chapter")),
                "clause_number": norm(row.get("article_number")),
                "clause_title": norm(row.get("article_title")),
                "publish_date": norm(row.get("publish_date")),
                "category": norm(row.get("category")),
                "project_type": norm(row.get("project_type")),
                "region": norm(row.get("region")),
                "region_level": norm(row.get("region_level")),
                "source_type": norm(row.get("source_type")),
            }
        )
        clause_text_value = compact_text(row.get("text"))
        clause_text.append(
            {
                "clause_id": norm(row.get("id")),
                "law_title": norm(row.get("law_title")),
                "clause_number": norm(row.get("article_number")),
                "clause_text": clause_text_value,
                "word_count": norm(row.get("word_count")),
                "text_hash": hash_text(clause_text_value),
            }
        )

    write_csv(
        META_DIR / "policy_document_meta.csv",
        [
            "doc_id",
            "title",
            "publish_date",
            "region",
            "region_level",
            "category",
            "project_type",
            "source_type",
            "source_name",
            "corpus_tag",
            "source_url",
            "document_url",
            "upstream_table",
            "has_full_text",
        ],
        policy_meta,
    )
    write_csv(
        TEXT_DIR / "policy_document_text.csv",
        [
            "doc_id",
            "title",
            "full_text",
            "word_count",
            "text_hash",
            "source",
            "source_url",
            "publish_time",
        ],
        policy_text,
    )
    write_csv(
        STRUCT_DIR / "policy_clause_structured.csv",
        [
            "clause_id",
            "source_doc_key",
            "law_title",
            "chapter",
            "clause_number",
            "clause_title",
            "publish_date",
            "category",
            "project_type",
            "region",
            "region_level",
            "source_type",
        ],
        clause_structured,
    )
    write_csv(
        TEXT_DIR / "policy_clause_text.csv",
        [
            "clause_id",
            "law_title",
            "clause_number",
            "clause_text",
            "word_count",
            "text_hash",
        ],
        clause_text,
    )
    return len(policy_meta), len(policy_text), len(clause_structured), len(clause_text)


def build_tender_tables() -> tuple[int, int]:
    rows = read_csv_rows(UPSTREAM / "02_tender" / "tender_docs_ahzb_curated.csv")

    tender_meta = []
    tender_text = []
    for row in rows:
        title = norm(row.get("procurement_title")) or norm(row.get("project_name"))
        procurement_text = compact_text(row.get("procurement_content"))
        result_text = compact_text(row.get("bid_content"))
        full_text = join_non_empty([procurement_text, result_text])

        tender_meta.append(
            {
                "notice_id": norm(row.get("doc_id")),
                "project_id": norm(row.get("project_id")),
                "title": title,
                "project_name": norm(row.get("project_name")),
                "business_type": norm(row.get("type")),
                "notice_stage": norm(row.get("stage")),
                "province": norm(row.get("province")),
                "city": norm(row.get("city")),
                "district": norm(row.get("town")),
                "category": norm(row.get("category")),
                "sub_category": norm(row.get("sub_category")),
                "budget_amount": norm(row.get("budget_amount")),
                "purchaser": norm(row.get("purchaser")),
                "agency": norm(row.get("agency")),
                "bid_company": norm(row.get("bid_company")),
                "bid_amount": norm(row.get("bid_amount")),
                "publish_date": norm(row.get("event_time")),
                "bid_date": norm(row.get("bid_date")),
                "source_url": norm(row.get("link")),
            }
        )
        tender_text.append(
            {
                "notice_id": norm(row.get("doc_id")),
                "project_id": norm(row.get("project_id")),
                "title": title,
                "procurement_text": procurement_text,
                "result_text": result_text,
                "full_text": full_text,
                "procurement_text_length": norm(row.get("procurement_content_length")),
                "result_text_length": norm(row.get("bid_content_length")),
                "text_hash": hash_text(full_text),
            }
        )

    write_csv(
        META_DIR / "tender_notice_meta.csv",
        [
            "notice_id",
            "project_id",
            "title",
            "project_name",
            "business_type",
            "notice_stage",
            "province",
            "city",
            "district",
            "category",
            "sub_category",
            "budget_amount",
            "purchaser",
            "agency",
            "bid_company",
            "bid_amount",
            "publish_date",
            "bid_date",
            "source_url",
        ],
        tender_meta,
    )
    write_csv(
        TEXT_DIR / "tender_notice_text.csv",
        [
            "notice_id",
            "project_id",
            "title",
            "procurement_text",
            "result_text",
            "full_text",
            "procurement_text_length",
            "result_text_length",
            "text_hash",
        ],
        tender_text,
    )
    return len(tender_meta), len(tender_text)


def build_company_table() -> int:
    rows = read_csv_rows(UPSTREAM / "03_company" / "company_profiles_local_matched.csv")
    company_rows = []
    for row in rows:
        if norm(row.get("match_status")) != "成功":
            continue
        uscc = norm(row.get("unified_social_credit_code"))
        company_name = norm(row.get("company_name"))
        company_id = uscc or hashlib.sha1(company_name.encode("utf-8")).hexdigest()
        phone_list = list_to_json(
            [
                norm(row.get("phone")),
                *[norm(item) for item in norm(row.get("additional_phones")).split(",") if norm(item)],
            ]
        )
        email_list = list_to_json(
            [
                norm(row.get("email")),
                *[norm(item) for item in norm(row.get("additional_emails")).split(",") if norm(item)],
            ]
        )

        company_rows.append(
            {
                "company_id": company_id,
                "company_name": company_name,
                "normalized_company_name": company_name,
                "uscc": uscc,
                "tax_id": norm(row.get("taxpayer_identification_number")),
                "legal_person": norm(row.get("legal_representative")),
                "business_status": norm(row.get("business_status")),
                "company_type": norm(row.get("company_type")),
                "industry": norm(row.get("industry")),
                "province": norm(row.get("province")),
                "city": norm(row.get("city")),
                "district": norm(row.get("district")),
                "registered_capital": norm(row.get("registered_capital")),
                "paid_in_capital": norm(row.get("paid_in_capital")),
                "establishment_date": norm(row.get("establishment_date")),
                "approval_date": norm(row.get("approval_date")),
                "business_term": norm(row.get("business_term")),
                "insured_personnel_count": norm(row.get("insured_personnel_count")),
                "phone_list": phone_list,
                "email_list": email_list,
                "website": norm(row.get("website")),
                "registered_address": norm(row.get("registered_address")),
                "business_scope": compact_text(row.get("business_scope")),
                "source_table": "company_profiles_local_matched",
                "data_quality": "high_local_match",
            }
        )

    write_csv(
        STRUCT_DIR / "company_profile_structured.csv",
        [
            "company_id",
            "company_name",
            "normalized_company_name",
            "uscc",
            "tax_id",
            "legal_person",
            "business_status",
            "company_type",
            "industry",
            "province",
            "city",
            "district",
            "registered_capital",
            "paid_in_capital",
            "establishment_date",
            "approval_date",
            "business_term",
            "insured_personnel_count",
            "phone_list",
            "email_list",
            "website",
            "registered_address",
            "business_scope",
            "source_table",
            "data_quality",
        ],
        company_rows,
    )
    return len(company_rows)


def build_risk_tables() -> tuple[int, int]:
    penalty_rows = read_csv_rows(UPSTREAM / "04_risk" / "risk_penalty_records_enriched.csv")
    anomaly_rows = read_csv_rows(UPSTREAM / "04_risk" / "risk_anomaly_records.csv")
    tender_meta_rows = read_csv_rows(META_DIR / "tender_notice_meta.csv")
    tender_meta_by_notice = {row["notice_id"]: row for row in tender_meta_rows}

    risk_structured = []
    risk_text = []

    for row in penalty_rows:
        risk_id = norm(row.get("id"))
        company_name = norm(row.get("matched_company_name")) or norm(row.get("company_name"))
        event_text = join_non_empty(
            [
                compact_text(row.get("illegal_behavior_details")),
                compact_text(row.get("penalty_result")),
                compact_text(row.get("penalty_basis")),
            ]
        )
        risk_structured.append(
            {
                "risk_id": risk_id,
                "risk_type": "penalty",
                "risk_subtype": "penalty",
                "company_name": company_name,
                "credit_code": norm(row.get("matched_credit_code")) or norm(row.get("credit_code")),
                "project_id": "",
                "title": f"{company_name} 行政处罚" if company_name else "行政处罚",
                "event_date": norm(row.get("penalty_date")),
                "publish_date": norm(row.get("publish_date")),
                "province": norm(row.get("matched_province")),
                "city": norm(row.get("matched_city")),
                "authority": norm(row.get("law_enforcement_unit")),
                "basis": norm(row.get("penalty_basis")),
                "action_result": norm(row.get("penalty_result")),
                "tags": "penalty",
                "source_url": "",
                "source_table": "risk_penalty_records_enriched",
            }
        )
        risk_text.append(
            {
                "risk_id": risk_id,
                "risk_type": "penalty",
                "title": f"{company_name} 行政处罚" if company_name else "行政处罚",
                "event_text": event_text,
                "text_hash": hash_text(event_text),
            }
        )

    for row in anomaly_rows:
        risk_id = norm(row.get("id"))
        notice_meta = tender_meta_by_notice.get(norm(row.get("source_doc_id")), {})
        event_text = compact_text(row.get("text"))
        risk_structured.append(
            {
                "risk_id": risk_id,
                "risk_type": "anomaly",
                "risk_subtype": norm(row.get("anomaly_tags")),
                "company_name": norm(row.get("bid_company")),
                "credit_code": "",
                "project_id": norm(row.get("project_id")),
                "title": norm(row.get("title")),
                "event_date": norm(row.get("event_time")),
                "publish_date": norm(row.get("event_time")),
                "province": norm(notice_meta.get("province")),
                "city": norm(notice_meta.get("city")),
                "authority": "",
                "basis": "",
                "action_result": norm(row.get("anomaly_tags")),
                "tags": norm(row.get("anomaly_tags")),
                "source_url": norm(row.get("source_url")),
                "source_table": "risk_anomaly_records",
            }
        )
        risk_text.append(
            {
                "risk_id": risk_id,
                "risk_type": "anomaly",
                "title": norm(row.get("title")),
                "event_text": event_text,
                "text_hash": hash_text(event_text),
            }
        )

    write_csv(
        STRUCT_DIR / "risk_event_structured.csv",
        [
            "risk_id",
            "risk_type",
            "risk_subtype",
            "company_name",
            "credit_code",
            "project_id",
            "title",
            "event_date",
            "publish_date",
            "province",
            "city",
            "authority",
            "basis",
            "action_result",
            "tags",
            "source_url",
            "source_table",
        ],
        risk_structured,
    )
    write_csv(
        TEXT_DIR / "risk_event_text.csv",
        ["risk_id", "risk_type", "title", "event_text", "text_hash"],
        risk_text,
    )
    return len(risk_structured), len(risk_text)


def build_attachment_table() -> int:
    rows = read_csv_rows(UPSTREAM / "05_attachment" / "attachment_manifest_all.csv")
    attachment_rows = []
    for row in rows:
        attachment_rows.append(
            {
                "attachment_id": norm(row.get("attachment_id")),
                "parent_record_key": norm(row.get("record_key")),
                "source_table": norm(row.get("source_table")),
                "attachment_role": norm(row.get("attachment_role")),
                "title": norm(row.get("title")),
                "project_name": norm(row.get("project_name")),
                "publish_time": norm(row.get("publish_time")),
                "attachment_name": norm(row.get("attachment_name")),
                "attachment_url": norm(row.get("attachment_url")),
                "file_ext": norm(row.get("file_ext")),
                "file_type": norm(row.get("file_type")),
                "domain": norm(row.get("domain")),
                "attach_guid": norm(row.get("attach_guid")),
                "is_pdf": norm(row.get("is_pdf")),
                "is_downloadable": norm(row.get("is_downloadable")),
                "priority": norm(row.get("priority")),
                "priority_reason": norm(row.get("priority_reason")),
                "parse_status": "pending",
            }
        )

    write_csv(
        META_DIR / "attachment_asset_meta.csv",
        [
            "attachment_id",
            "parent_record_key",
            "source_table",
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
            "parse_status",
        ],
        attachment_rows,
    )
    return len(attachment_rows)


def write_governance_files(counts: dict[str, int]) -> None:
    inventory_rows = [
        {"layer": "metadata", "table_name": "policy_document_meta.csv", "rows": counts["policy_document_meta"], "object_type": "policy_document"},
        {"layer": "text", "table_name": "policy_document_text.csv", "rows": counts["policy_document_text"], "object_type": "policy_document"},
        {"layer": "structured", "table_name": "policy_clause_structured.csv", "rows": counts["policy_clause_structured"], "object_type": "policy_clause"},
        {"layer": "text", "table_name": "policy_clause_text.csv", "rows": counts["policy_clause_text"], "object_type": "policy_clause"},
        {"layer": "metadata", "table_name": "tender_notice_meta.csv", "rows": counts["tender_notice_meta"], "object_type": "tender_notice"},
        {"layer": "text", "table_name": "tender_notice_text.csv", "rows": counts["tender_notice_text"], "object_type": "tender_notice"},
        {"layer": "structured", "table_name": "company_profile_structured.csv", "rows": counts["company_profile_structured"], "object_type": "company_profile"},
        {"layer": "structured", "table_name": "risk_event_structured.csv", "rows": counts["risk_event_structured"], "object_type": "risk_event"},
        {"layer": "text", "table_name": "risk_event_text.csv", "rows": counts["risk_event_text"], "object_type": "risk_event"},
        {"layer": "metadata", "table_name": "attachment_asset_meta.csv", "rows": counts["attachment_asset_meta"], "object_type": "attachment_asset"},
    ]
    write_csv(
        GOV_DIR / "core_table_inventory.csv",
        ["layer", "table_name", "rows", "object_type"],
        inventory_rows,
    )

    excluded_rows = [
        {"upstream_table": "policy_local_docs_ah_hf.csv", "decision": "excluded_from_core", "reason": "already merged into policy_curated_*"},
        {"upstream_table": "tender_records_structured.csv", "decision": "excluded_from_core", "reason": "encoding issues and weak schema consistency"},
        {"upstream_table": "tender_records_history_hefei.csv", "decision": "excluded_from_core", "reason": "benchmark supplement, not minimal serving table"},
        {"upstream_table": "tender_award_records.csv", "decision": "excluded_from_core", "reason": "covered by tender_notice_meta minimal loop"},
        {"upstream_table": "company_profiles_national.csv", "decision": "excluded_from_core", "reason": "encoding risk, keep as upstream only until cleaned"},
    ]
    write_csv(
        GOV_DIR / "excluded_upstream_tables.csv",
        ["upstream_table", "decision", "reason"],
        excluded_rows,
    )

    availability_rows = [
        {"object_type": "policy_document", "field_name": "doc_id", "status": "available_direct", "source": "policy_curated_meta.id", "decision": "keep"},
        {"object_type": "policy_document", "field_name": "title", "status": "available_direct", "source": "policy_curated_meta.title", "decision": "keep"},
        {"object_type": "policy_document", "field_name": "publish_date", "status": "available_direct", "source": "policy_curated_meta.publish_date", "decision": "keep"},
        {"object_type": "policy_document", "field_name": "region", "status": "available_direct", "source": "policy_curated_meta.region", "decision": "keep"},
        {"object_type": "policy_document", "field_name": "doc_type", "status": "missing_now", "source": "", "decision": "do_not_add_now"},
        {"object_type": "policy_document", "field_name": "effectiveness_status", "status": "missing_now", "source": "", "decision": "do_not_add_now"},
        {"object_type": "policy_document", "field_name": "effective_date", "status": "missing_now", "source": "", "decision": "do_not_add_now"},
        {"object_type": "policy_document", "field_name": "issuer", "status": "missing_now", "source": "", "decision": "do_not_add_now"},
        {"object_type": "policy_clause", "field_name": "clause_number", "status": "available_direct", "source": "policy_national_article_units.article_number", "decision": "keep"},
        {"object_type": "policy_clause", "field_name": "clause_text", "status": "available_direct", "source": "policy_national_article_units.text", "decision": "keep"},
        {"object_type": "tender_notice", "field_name": "project_id", "status": "available_direct", "source": "tender_docs_ahzb_curated.project_id", "decision": "keep"},
        {"object_type": "tender_notice", "field_name": "notice_stage", "status": "available_direct", "source": "tender_docs_ahzb_curated.stage", "decision": "keep"},
        {"object_type": "tender_notice", "field_name": "section_num", "status": "excluded_now", "source": "only in raw source", "decision": "omit_for_minimality"},
        {"object_type": "tender_notice", "field_name": "full_text", "status": "derived_reliable", "source": "procurement_content + bid_content", "decision": "keep"},
        {"object_type": "company_profile", "field_name": "uscc", "status": "available_direct", "source": "company_profiles_local_matched.unified_social_credit_code", "decision": "keep"},
        {"object_type": "company_profile", "field_name": "source_url", "status": "missing_now", "source": "", "decision": "do_not_add_now"},
        {"object_type": "company_profile", "field_name": "national_fallback", "status": "excluded_now", "source": "company_profiles_national", "decision": "exclude_until_cleaned"},
        {"object_type": "risk_event", "field_name": "source_url", "status": "partial", "source": "available for anomaly only", "decision": "keep_with_blank_for_penalty"},
        {"object_type": "attachment_asset", "field_name": "parent_record_key", "status": "available_direct", "source": "attachment_manifest_all.record_key", "decision": "keep"},
        {"object_type": "attachment_asset", "field_name": "extracted_text", "status": "missing_now", "source": "", "decision": "do_not_add_now"},
    ]
    write_csv(
        GOV_DIR / "field_availability_matrix.csv",
        ["object_type", "field_name", "status", "source", "decision"],
        availability_rows,
    )


def main() -> None:
    ensure_dirs()
    policy_meta_count, policy_text_count, clause_structured_count, clause_text_count = build_policy_tables()
    tender_meta_count, tender_text_count = build_tender_tables()
    company_count = build_company_table()
    risk_structured_count, risk_text_count = build_risk_tables()
    attachment_count = build_attachment_table()

    counts = {
        "policy_document_meta": policy_meta_count,
        "policy_document_text": policy_text_count,
        "policy_clause_structured": clause_structured_count,
        "policy_clause_text": clause_text_count,
        "tender_notice_meta": tender_meta_count,
        "tender_notice_text": tender_text_count,
        "company_profile_structured": company_count,
        "risk_event_structured": risk_structured_count,
        "risk_event_text": risk_text_count,
        "attachment_asset_meta": attachment_count,
    }
    write_governance_files(counts)

    for key, value in counts.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
