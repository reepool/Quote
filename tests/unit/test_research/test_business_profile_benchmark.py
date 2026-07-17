from research.business_profile_benchmark import select_parser_benchmark


def _issuer(instrument_id, exchange, l2, l3, listed_date):
    return {
        "instrument_id": instrument_id,
        "symbol": instrument_id.split(".")[0],
        "company_name": instrument_id,
        "exchange": exchange,
        "industry_group": "coal",
        "sw_l2_code": l2,
        "sw_l3_code": l3,
        "listed_date": listed_date,
    }


def _manifest(source_file_id, instrument_id):
    return {
        "source_file_id": source_file_id,
        "instrument_id": instrument_id,
        "source": "cninfo",
        "source_tier": "official_primary",
        "schema_version": "business_profile_source_file_manifest.v1",
        "status": "archived",
        "archive_path": f"/archive/{source_file_id}.pdf",
        "content_hash": "a" * 64,
        "filing_id": f"filing-{source_file_id}",
    }


def test_selector_hard_covers_available_exchanges_and_verified_edges():
    universe = [
        _issuer("600001.SH", "SSE", "l2-a", "l3-a", "1999-01-01"),
        _issuer("600002.SH", "SSE", "l2-b", "l3-b", "2005-01-01"),
        _issuer("000001.SZ", "SZSE", "l2-c", "l3-c", "2012-01-01"),
        _issuer("000002.SZ", "SZSE", "l2-d", "l3-d", "2021-01-01"),
        _issuer("920001.BJ", "BSE", "l2-e", "l3-e", "2023-01-01"),
        _issuer("920002.BJ", "BSE", "l2-f", "l3-f", "2024-01-01"),
    ]
    evidence = [
        {
            "instrument_id": "600002.SH",
            "verified": True,
            "diversified_business": True,
            "source_document_ids": ["annual-1"],
        },
        {
            "instrument_id": "000002.SZ",
            "verified": True,
            "correction_report": True,
            "complex_table": True,
            "source_document_ids": ["annual-2"],
        },
    ]

    result = select_parser_benchmark(
        universe,
        evidence_profiles=evidence,
        source_manifests=[
            _manifest("annual-1", "600002.SH"),
            _manifest("annual-2", "000002.SZ"),
        ],
        issuers_per_industry=5,
        expected_industry_groups=["coal"],
    )
    coal = result["industries"]["coal"]

    assert coal["status"] == "ready"
    assert coal["selected_exchanges"] == ["BSE", "SSE", "SZSE"]
    assert coal["selected_sw_l3_count"] == 5
    assert coal["evidence_coverage"]["diversified_business"] is True
    assert coal["evidence_coverage"]["correction_report"] is True
    assert coal["evidence_coverage"]["pdf_format_edge"] is True


def test_unverified_evidence_does_not_satisfy_readiness():
    universe = [
        _issuer(f"60000{index}.SH", "SSE", "l2", f"l3-{index}", "2001-01-01")
        for index in range(1, 6)
    ]
    result = select_parser_benchmark(
        universe,
        evidence_profiles=[
            {
                "instrument_id": "600001.SH",
                "verified": False,
                "diversified_business": True,
                "correction_report": True,
                "ocr_required": True,
            }
        ],
        expected_industry_groups=["coal"],
    )

    coal = result["industries"]["coal"]
    assert result["status"] == "evidence_incomplete"
    assert coal["missing_required_strata"] == [
        "evidence:diversified_business",
        "evidence:correction_report",
        "evidence:pdf_format_edge",
    ]
    assert all(item["evidence"] == {} for item in coal["selected_issuers"])


def test_verified_evidence_merges_multiple_official_documents_per_issuer():
    universe = [
        _issuer(f"60000{index}.SH", "SSE", "l2", f"l3-{index}", "2001-01-01")
        for index in range(1, 6)
    ]
    result = select_parser_benchmark(
        universe,
        evidence_profiles=[
            {
                "instrument_id": "600001.SH",
                "verified": True,
                "diversified_business": True,
                "source_document_ids": ["annual-original"],
                "notes": "segment evidence",
            },
            {
                "instrument_id": "600001.SH",
                "verified": True,
                "correction_report": True,
                "cross_page_table": True,
                "source_document_ids": ["annual-correction"],
                "notes": "format evidence",
            },
        ],
        source_manifests=[
            _manifest("annual-original", "600001.SH"),
            _manifest("annual-correction", "600001.SH"),
        ],
        expected_industry_groups=["coal"],
    )

    selected = result["industries"]["coal"]["selected_issuers"]
    issuer = next(item for item in selected if item["instrument_id"] == "600001.SH")
    assert issuer["evidence"]["diversified_business"] is True
    assert issuer["evidence"]["correction_report"] is True
    assert issuer["evidence"]["cross_page_table"] is True
    assert issuer["evidence"]["source_document_ids"] == [
        "annual-correction",
        "annual-original",
    ]
    assert result["status"] == "ready"


def test_selector_is_stable_for_input_order():
    universe = [
        _issuer(f"60000{index}.SH", "SSE", "l2", f"l3-{index}", "2001-01-01")
        for index in range(1, 7)
    ]

    first = select_parser_benchmark(
        universe,
        expected_industry_groups=["coal"],
    )
    second = select_parser_benchmark(
        list(reversed(universe)),
        expected_industry_groups=["coal"],
    )

    first_ids = [
        item["instrument_id"]
        for item in first["industries"]["coal"]["selected_issuers"]
    ]
    second_ids = [
        item["instrument_id"]
        for item in second["industries"]["coal"]["selected_issuers"]
    ]
    assert first_ids == second_ids


def test_global_readiness_fails_closed_for_empty_or_missing_first_wave_groups():
    empty = select_parser_benchmark([])
    coal_only = select_parser_benchmark(
        [
            _issuer(
                f"60000{index}.SH",
                "SSE",
                "l2",
                f"l3-{index}",
                "2001-01-01",
            )
            for index in range(1, 6)
        ]
    )

    assert empty["status"] == "evidence_incomplete"
    assert len(empty["incomplete_industry_groups"]) == 6
    assert coal_only["status"] == "evidence_incomplete"
    assert "steel" in coal_only["incomplete_industry_groups"]


def test_fake_or_cross_instrument_manifest_ids_do_not_satisfy_readiness():
    universe = [
        _issuer(f"60000{index}.SH", "SSE", "l2", f"l3-{index}", "2001-01-01")
        for index in range(1, 6)
    ]
    evidence = [
        {
            "instrument_id": "600001.SH",
            "verified": True,
            "diversified_business": True,
            "correction_report": True,
            "complex_table": True,
            "source_document_ids": ["not-a-real-manifest"],
        },
        {
            "instrument_id": "600002.SH",
            "verified": True,
            "diversified_business": True,
            "correction_report": True,
            "complex_table": True,
            "source_document_ids": ["other-company-report"],
        },
    ]

    result = select_parser_benchmark(
        universe,
        evidence_profiles=evidence,
        source_manifests=[
            _manifest("other-company-report", "600003.SH"),
        ],
        expected_industry_groups=["coal"],
    )

    assert result["status"] == "evidence_incomplete"
    assert all(
        item["evidence"] == {}
        for item in result["industries"]["coal"]["selected_issuers"]
    )
