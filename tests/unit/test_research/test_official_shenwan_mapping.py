from research.official_shenwan_mapping import OfficialShenwanCodeMapper
from research.providers.base import OfficialIndustryHistorySnapshot


def test_official_shenwan_code_mapper_picks_best_taxonomy_match():
    mapper = OfficialShenwanCodeMapper(
        min_overlap_count=2,
        min_precision=0.6,
        min_recall=0.6,
    )

    mappings = mapper.infer_mappings(
        official_snapshots=[
            OfficialIndustryHistorySnapshot(
                instrument_id="600519.SH",
                symbol="600519",
                exchange="SSE",
                official_industry_code="340501",
            ),
            OfficialIndustryHistorySnapshot(
                instrument_id="000858.SZ",
                symbol="000858",
                exchange="SZSE",
                official_industry_code="340501",
            ),
            OfficialIndustryHistorySnapshot(
                instrument_id="603589.SH",
                symbol="603589",
                exchange="SSE",
                official_industry_code="340501",
            ),
            OfficialIndustryHistorySnapshot(
                instrument_id="000001.SZ",
                symbol="000001",
                exchange="SZSE",
                official_industry_code="480301",
            ),
            OfficialIndustryHistorySnapshot(
                instrument_id="002142.SZ",
                symbol="002142",
                exchange="SZSE",
                official_industry_code="480301",
            ),
        ],
        taxonomy_components={
            "850555.SI": {"600519", "000858", "603589"},
            "850533.SI": {"002304", "603288"},
            "850310.SI": {"000001", "002142", "600000"},
        },
    )

    mapping_by_official_code = {
        item.official_industry_code: item
        for item in mappings
    }

    assert mapping_by_official_code["340501"].taxonomy_industry_code == "850555.SI"
    assert mapping_by_official_code["340501"].best_taxonomy_industry_code == "850555.SI"
    assert mapping_by_official_code["340501"].confidence == "high"
    assert mapping_by_official_code["340501"].mapping_source == "inferred"
    assert (
        mapping_by_official_code["340501"].candidate_rankings[0].taxonomy_industry_code
        == "850555.SI"
    )
    assert mapping_by_official_code["480301"].taxonomy_industry_code == "850310.SI"
    assert mapping_by_official_code["480301"].best_taxonomy_industry_code == "850310.SI"
    assert mapping_by_official_code["480301"].confidence == "medium"
    assert mapping_by_official_code["480301"].mapping_source == "inferred"
    assert (
        mapping_by_official_code["480301"].candidate_rankings[0].taxonomy_industry_code
        == "850310.SI"
    )


def test_official_shenwan_code_mapper_marks_low_overlap_as_unmapped():
    mapper = OfficialShenwanCodeMapper(
        min_overlap_count=2,
        min_precision=0.6,
        min_recall=0.6,
    )

    mappings = mapper.infer_mappings(
        official_snapshots=[
            OfficialIndustryHistorySnapshot(
                instrument_id="600519.SH",
                symbol="600519",
                exchange="SSE",
                official_industry_code="340501",
            ),
        ],
        taxonomy_components={
            "850555.SI": {"600519", "000858", "603589"},
        },
    )

    assert mappings[0].official_industry_code == "340501"
    assert mappings[0].best_taxonomy_industry_code == "850555.SI"
    assert mappings[0].taxonomy_industry_code is None
    assert mappings[0].confidence == "unmapped"
    assert mappings[0].candidate_rankings[0].taxonomy_industry_code == "850555.SI"
