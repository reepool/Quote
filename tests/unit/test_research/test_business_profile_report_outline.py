import hashlib

from research.business_profile_report_outline import (
    assess_business_profile_recovery,
    locate_business_profile_outline,
)


def _artifact(*texts):
    return {
        "source_content_hash": hashlib.sha256(b"outline").hexdigest(),
        "pages": [
            {
                "page_number": index,
                "text": text,
                "text_hash": hashlib.sha256(text.encode()).hexdigest(),
            }
            for index, text in enumerate(texts, start=1)
        ],
    }


def test_toc_printed_pages_are_mapped_to_actual_major_headings():
    outline = locate_business_profile_outline(
        _artifact(
            "目录\n第三节 管理层讨论与分析......1\n第四节 公司治理......4",
            "封面后的说明",
            "第三节 管理层讨论与分析\n一、公司从事的主要业务",
            "主营业务分析",
            "收入与成本",
            "第四节 公司治理",
        )
    )

    assert (outline.start_page, outline.end_page) == (3, 5)
    assert outline.source == "table_of_contents"
    assert outline.confidence == "high"


def test_major_heading_fallback_bounds_management_discussion():
    outline = locate_business_profile_outline(
        _artifact(
            "年度报告全文",
            "第三节 经营情况讨论与分析",
            "主要产品及应用",
            "主营业务分析",
            "第四节 公司治理",
        )
    )

    assert (outline.start_page, outline.end_page) == (2, 4)
    assert outline.source == "major_heading_fallback"
    assert outline.confidence == "medium"


def test_missing_outline_uses_low_confidence_full_document_scope():
    outline = locate_business_profile_outline(_artifact("封面", "正文", "附注"))

    assert (outline.start_page, outline.end_page) == (1, 3)
    assert outline.source == "bounded_full_document_fallback"
    assert outline.confidence == "low"


def test_recovery_assessment_marks_native_business_pages_ready():
    decision = assess_business_profile_recovery(
        _artifact(
            "目录\n第三节 管理层讨论与分析......1\n第四节 公司治理......4",
            "封面后的说明",
            "第三节 管理层讨论与分析\n一、公司从事的主要业务",
            "主营业务分析",
            "收入与成本",
            "第四节 公司治理",
        )
    )

    assert decision.state == "native_ready"
    assert decision.outline.page_numbers == (3, 4, 5)


def test_recovery_assessment_requires_section_recovery_for_bad_pages():
    artifact = _artifact(
        "目录\n第三节 管理层讨论与分析......1\n第四节 公司治理......4",
        "封面后的说明",
        "第三节 管理层讨论与分析\n一、公司从事的主要业务",
        "主营业务分析",
        "收入与成本",
        "第四节 公司治理",
    )
    artifact["pages"][3]["text"] = ""
    artifact["pages"][3]["native_text_status"] = "empty"
    artifact["pages"][3]["ocr_required"] = True

    decision = assess_business_profile_recovery(artifact, section_max_pages=2)

    assert decision.state == "section_ocr_required"
    assert decision.section_pages == (4,)
