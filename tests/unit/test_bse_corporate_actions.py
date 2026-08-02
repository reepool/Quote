from datetime import date

from data_sources.bse_corporate_actions import (
    BSE_DIVIDEND_PROFILE,
    parse_bse_dividend_implementation,
)
from data_sources.cninfo_corporate_action_documents import CorporateActionPageText
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    build_announcement_key,
)


def _record(
    title="乐创技术2025年年度权益分派实施公告",
    published_at_raw="2026-07-16",
):
    return AnnouncementRecord(
        source="bse",
        source_announcement_id="bse-1",
        announcement_key=build_announcement_key("bse", "bse-1"),
        title=title,
        published_at="2026-07-16T00:00:00+00:00",
        published_at_raw=published_at_raw,
        exchange="BSE",
        market="BSE",
        symbols=("920425",),
        attachments=(AnnouncementAttachment(
            source_url="/disclosure/example.pdf",
            resolved_url="https://www.bse.cn/disclosure/example.pdf",
            media_type="application/pdf",
        ),),
    )


def _page(text):
    return CorporateActionPageText(
        page_number=1,
        text=text,
        text_hash="a" * 64,
    )


def test_bse_implementation_parser_extracts_explicit_terms_and_dates():
    result = parse_bse_dividend_implementation(
        record=_record(),
        instrument_id="920425.BJ",
        pages=[_page(
            "公司以资本公积金向全体股东每10股转增4股，"
            "每10股派发现金红利2.50元（含税）。"
            "股权登记日：2026年7月22日，除权除息日：2026年7月23日，"
            "新增股份上市日：2026年7月23日，现金红利发放日：2026年7月23日。"
        )],
        document={"content_hash": "b" * 64},
        as_of_date=date(2026, 7, 31),
    )

    assert result.status == "success"
    observation = result.observation
    assert observation["source"] == "bse"
    assert observation["source_profile"] == BSE_DIVIDEND_PROFILE
    assert observation["cash_dividend_per_share"] == 0.25
    assert observation["capitalization_shares_per_share"] == 0.4
    assert observation["record_date"] == date(2026, 7, 22)
    assert observation["ex_date"] == date(2026, 7, 23)
    assert observation["pay_date"] == date(2026, 7, 23)
    assert observation["share_arrival_date"] == date(2026, 7, 23)
    assert observation["quality_status"] == "official_document_complete"
    assert observation["raw_payload"]["document"]["content_hash"] == "b" * 64


def test_bse_implementation_parser_fails_closed_when_ex_date_is_missing():
    result = parse_bse_dividend_implementation(
        record=_record(),
        instrument_id="920425.BJ",
        pages=[_page(
            "每10股派发现金红利2.50元，股权登记日为2026年7月22日。"
        )],
    )

    assert result.status == "partial"
    assert result.observation is None
    assert "ex_date_missing" in result.diagnostics


def test_bse_event_identity_survives_an_official_ex_date_correction():
    record = _record(published_at_raw="2026-07-16 18:30:00")
    original = parse_bse_dividend_implementation(
        record=record,
        instrument_id="920425.BJ",
        pages=[_page(
            "每10股派发现金红利2.50元，股权登记日为2026年7月22日，"
            "除权除息日为2026年7月23日。"
        )],
    ).observation
    corrected = parse_bse_dividend_implementation(
        record=record,
        instrument_id="920425.BJ",
        pages=[_page(
            "每10股派发现金红利2.50元，股权登记日为2026年7月23日，"
            "除权除息日为2026年7月24日。"
        )],
    ).observation

    assert original["source_event_key"] == corrected["source_event_key"]
    assert original["ex_date"] == date(2026, 7, 23)
    assert corrected["ex_date"] == date(2026, 7, 24)
    assert corrected["announcement_date"] == date(2026, 7, 16)


def test_bse_parser_rejects_non_implementation_title():
    result = parse_bse_dividend_implementation(
        record=_record("乐创技术董事会决议公告"),
        instrument_id="920425.BJ",
        pages=[_page("每10股派2.50元，除权除息日2026年7月23日。")],
    )

    assert result.status == "not_applicable"
    assert result.observation is None
