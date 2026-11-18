"""Read-only 6.DK evidence consolidation; never imports an LLM or task runner.

Run from the repository with quote_dev_env.sh. --write regenerates only this
directory's derived JSON; the API runtime, sources and previous audits are inputs.
This is an acceptance-evidence operator, not a second business implementation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import statistics
from collections import Counter
from pathlib import Path

from research.company_profile.reads import CompanyProfileReadService

ROOT = Path(__file__).resolve().parents[4]
CHANGE = Path(__file__).resolve().parent.parent
DEST = Path(__file__).resolve().parent
RUNTIME = ROOT / "data/research/company_profile_api_preview_20261117"

# Explicit human selection after reading the current business statements and
# existing independent audits; neither field presence nor regex decides meaning.
BASIS = {
    "600094.SH": (["F01", "F02", "F04"], ["F01", "F06"], ["F06", "F08", "F10"]),
    "600103.SH": (["F01", "F02", "F03"], ["F04", "F05", "F06", "F07", "F08"], ["F10", "F11"]),
    "600100.SH": (["F01"], ["F05", "F06", "F07", "F08"], ["F09", "F10", "F11", "F12"]),
    "600107.SH": (["F01", "F02"], ["F03", "F04"], ["F06", "F07"]),
    "600098.SH": (["F01", "F03", "F04", "F05"], ["F06", "F07", "F08"], ["F09", "F10"]),
    "600104.SH": (["F01", "F03"], ["F04", "F06", "F07", "F08"], ["F09"]),
    "600088.SH": (["F01", "F02", "F03"], ["F04", "F05", "F06", "F07", "F08"], ["F08", "F10", "F11", "F12", "F13"]),
    "600099.SH": (["F01", "F02"], ["F03", "F04", "F05", "F06"], ["F07"]),
    "600082.SH": (["F01", "F02"], ["F03", "F04", "F05"], ["F06", "F07"]),
    "600097.SH": (["bs-01", "bs-02"], ["ps-01", "ps-02", "ps-03"], ["rm-01", "rm-07"]),
    "600054.SH": (["F01", "F02", "F03", "F04", "F05", "F06"], ["F08"], ["F13", "F14", "F15", "F16", "F17"]),
    "600000.SH": (["bs-1"], ["ps-1", "ps-2", "ps-3"], ["rm-1", "rm-2"]),
}
QUESTIONS = ("business_structure", "products_services", "revenue_model")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(relative: str):
    return json.loads((CHANGE / relative).read_text())


def dump(path: Path, value) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def build() -> tuple[dict, dict, dict]:
    input_paths = [
        "workpaper-api-delivery-result-20261117/index.json",
        "workpaper-research-export-result-20261117/acceptance.json",
        "workpaper-core-readiness-20261114/source-first-contract.json",
        "workpaper-quality-and-regression-readiness-20261113/source-readiness.json",
        "workpaper-final-source-review-result-20261117/independent-audit.json",
        "workpaper-three-final-source-review-result-20261117/independent-audit.json",
        "workpaper-saic-enrichment-result-20261117/independent-audit.json",
    ]
    inputs = {path: sha(CHANGE / path) for path in input_paths}
    runtime_hashes = {str(p.relative_to(ROOT)): sha(p)
        for p in sorted((RUNTIME / "company_profile_common_core.v1").glob("*.json"))}
    index = read(input_paths[0])["profiles"]
    historical = read(input_paths[1])
    references = {c["instrument_id"]: c for c in read(input_paths[2])["companies"]}
    assets = {c["instrument_id"]: c for c in read(input_paths[3])["companies"]}
    assert set(BASIS) == {c["instrument_id"] for c in index} == set(references)
    reader = CompanyProfileReadService(RUNTIME)
    companies, contract = [], []
    for item in index:
        instrument_id, work_id = item["instrument_id"], item["work_id"]
        assert sha(Path(item["runtime_record"])) == item["runtime_sha256"]
        assert sha(ROOT / item["source_query"]) == item["source_sha256"]
        projection = reader.research_profile(instrument_id)
        assert projection["research_only"] and projection["production_authorization"] == "not_authorized"
        assert projection == CompanyProfileReadService(RUNTIME).research_profile(instrument_id)
        assert projection["full_report_completeness"] == "not_assessed"
        visible = [f for f in projection["facts"] if f["fact"] is not None]
        by_id = {f["fact_id"]: f for f in visible if f["reading_work_id"] == work_id}
        excluded = [f for f in projection["facts"] if f["fact"] is None]
        readings = {r["work_id"]: r for r in projection["readings"]}
        evidence = {e["evidence_id"]: e for e in readings[work_id]["evidence"]}
        basis = {}
        for question, fact_ids in zip(QUESTIONS, BASIS[instrument_id]):
            facts = [by_id[fact_id] for fact_id in fact_ids]
            assert all(f["state"] in ("reviewed", "limited") for f in facts)
            assert all(f["evidence_ids"] and all(e in evidence for e in f["evidence_ids"]) for f in facts)
            basis[question] = {
                "reading_work_id": work_id,
                "fact_ids": fact_ids,
                "evidence_ids": sorted({e for f in facts for e in f["evidence_ids"]}),
                "conclusion": "substantive_research_answer_present",
            }
        asset = assets[instrument_id]["asset"]
        assert sha(Path(asset["archive_path"])) == asset["content_hash"] == projection["report"]["document_version"]
        original = next(c for c in historical["current_selected_scope_core"] if c["instrument_id"] == instrument_id)
        resource = next(c for c in historical["original_CT_resource_reference"]["companies"] if c["instrument_id"] == instrument_id)
        companies.append({
            "instrument_id": instrument_id,
            "name": assets[instrument_id]["name"],
            "report": projection["report"],
            "research_readability": "basic_business_description_present_in_selected_scope",
            "assessment_basis": "human_reading_of_current_statements_and_existing_independent_audits_not_new_PDF_audit",
            "basic_answers": basis,
            "readings": len(readings),
            "visible_records": len(visible),
            "visible_metrics": sum(len(f["fact"]["metrics"]) for f in visible),
            "delivery_counts": dict(Counter(f["state"] for f in visible)),
            "excluded_records_without_fact_payload": len(excluded),
            "information_gaps": projection["information_gaps"],
            "suggested_acquisitions": projection["suggested_acquisitions"],
            "historical_133_reference_unchanged": original,
            "historical_complete_entry_resource_not_current_or_cumulative": resource,
            "formal_accuracy": None,
            "formal_company_usable": "not_assessed",
            "full_report_completeness": "not_assessed",
        })
        contexts = references[instrument_id]["source_contexts"]
        contract.append({
            "instrument_id": instrument_id,
            "report": projection["report"],
            "questions": [{
                "question": question,
                "meaning": meaning,
                "source_context_reference": context["condition_id"],
                "source_expected": context["expected"],
                "source_sections": context["section_paths"],
                "source_pages_for_independent_review_only": context["physical_pages"],
                "assessment": "not_run",
            } for question, meaning, context in zip(QUESTIONS, (
                "公司当前主要做什么；不漏整个主要板块，投资/计划不能当已经营。",
                "主要板块提供何种产品或服务；不强求所有型号、细用途、子公司清单。",
                "主要业务怎样产生收入；分清制造、贸易、运营或服务等实际角色。",
            ), contexts[:3])],
        })
    assert len(runtime_hashes) == 13
    assert all(sha(ROOT / path) == digest for path, digest in runtime_hashes.items())
    assert all(sha(CHANGE / path) == digest for path, digest in inputs.items())
    visible_count = sum(c["visible_records"] for c in companies)
    metric_count = sum(c["visible_metrics"] for c in companies)
    assert (visible_count, metric_count) == (371, 524)
    old_times = [c["business_seconds"] for c in historical["original_CT_resource_reference"]["companies"]]
    acceptance = {
        "task": "6.DK", "new_model_calls": 0,
        "research_delivery": {"companies": 12, "readings": 13, "visible_records": visible_count,
            "visible_metrics": metric_count, "unique_economic_discoveries": "not_assessed",
            "all_have_basic_research_description": True, "not_a_formal_company_score": True},
        "formal_S1": {"passed": False, "historical_count_unchanged": "0/12",
            "verdict": "not_passed_not_a_claim_of_zero_research_outputs",
            "remaining": [
                "当前完整自动首读链尚无十二家同版完整正反/独立质量与资源的联合验收；后继稿与补读不拼首发。",
                "已审对象准确率须以独立核源的唯一对象及实际判定统计，不能把已核分级或模型接受数直接当准确率。",
                "六类适用风险须逐家公司绑定实际正反输入/输出，未触发或未核范围不算通过。",
                "旧常规900秒/120000/16与双模型开发1800秒/500000/10不同；unknown费用不证明10元达标。",
            ]},
        "S2_released": False, "production_authorization": "not_authorized",
        "formal_accuracy": None,
        "timing_reference": {"source": input_paths[1], "historical_CT_median_all_entries_seconds": statistics.median(old_times),
            "not_current_end_to_end_latency": True, "includes_failed_entries": True},
        "companies": companies, "input_sha256": inputs, "runtime_sha256_unchanged": runtime_hashes,
    }
    prospective = {
        "contract_id": "company_profile_basic_questions.2026-11-18.v1",
        "status": "frozen_for_prospective_evaluation_not_executed",
        "source_reference": input_paths[2], "source_reference_sha256": inputs[input_paths[2]],
        "reference_sent_to_models": False, "historical_133_and_scores_unchanged": True,
        "purpose": "区分基础业务可读性与建议补充，不把36个粗问题替代正式核心召回分母。",
        "formal_scoring": "按独立源范围在运行前拆成可判定的实质业务条件并冻结；重要补充不作强制条件；不从本轮模型答案反推分母。",
        "supplementary_not_company_veto": ["产品型号和用途细目", "子公司完整清单", "并列营业收入科目", "金融服务细项", "精细供应链和数量", "其他非主营遗漏"],
        "scale_priority": "优先获取有据公司规模和主要构成；没有精确金额可明示未获取，不造数，不因此否定成立的三项基本描述。",
        "companies": contract,
    }
    scope_files = sorted((ROOT / "research/company_profile").glob("*.py"))
    scope_files += sorted((ROOT / "utils/llm").glob("*.py"))
    scope_files += [ROOT / p for p in (
        "research/storage.py", "research/business_profile_pdf_artifacts.py",
        "research/business_profile_production_operations.py", "api/company_profile_routes.py")]
    next_plan = {
        "task": "6.DL", "status": "proposed_not_authorized", "attempts_started": 0,
        "purpose": "一次当前完整链的同版S1质量与资源补证，不再补单家公司尾项。",
        "why_not_replay_only": "已有真稿可证明研究内容和后段纠错；离线重放不能证明当前选章/抽取/复核整链的真实首发效果或资源。只有用户另批本卡才新抽十二家。",
        "authority_required": ["本次十二家各一次新的外传与模型预算", "前瞻调整仅本批S1为双模型资源档；保旧常规结果，不降低内容质量门槛"],
        "entry_owner": "CompanyProfileTaskService.execute(run)",
        "writer": "company_profile_research_writer.v1",
        "source_manifest": input_paths[3], "source_manifest_sha256": inputs[input_paths[3]],
        "order": ["600099.SH", "600054.SH", "600000.SH", "600100.SH"] + [
            instrument for instrument in BASIS if instrument not in ("600099.SH", "600054.SH", "600000.SH", "600100.SH")],
        "initial_four_type_dispatch": ["600099.SH", "600054.SH", "600000.SH", "600100.SH"],
        "dispatch_after_first_four": "四家基础业务链和底线均无阻塞才同合同继续余八；失败及未启动留原分母，不另请逐公司许可或换样本。",
        "model_profiles": {"select_extract_repair": "semantic_extraction__deepseek",
            "review_verify": "semantic_extraction__scorpio_grok",
            "extraction_model": "deepseek-flash", "extraction_thinking": "disabled",
            "review_host": "scorpio.reepool.com", "review_model": "grok-4.7", "review_reasoning": "high"},
        "each_limits": {"business_seconds": 1800, "conservative_ledger": 500000,
            "physical_requests": 10, "recoveries": 3, "recoveries_per_logical": 1,
            "merged_business_revision": 1, "known_priced_cost_cny": 10},
        "group_limits": {"concurrency": 4, "business_wall_seconds": 7200,
            "conservative_ledger": 6000000, "physical_requests": 120},
        "resource_policy_proposal": {"status": "not_authorized",
            "after_explicit_approval": "仅本批S1使用上述双模型硬限作为资源验收合同；费用unknown如实公开，不作10元总费用达标声明。质量及12家正反门槛不变，符合新合同才可裁决本批S1通过；旧组失败和旧常规资源不回算。",
            "without_approval": "旧900秒/120000/16及费用要求仍有效，不得用500000开发限冒称正式通过。",
            "ordinary_resource_reference": {"business_seconds": 900, "ledger": 120000, "physical_requests": 16},
            "production_or_market_scale_default_changed": False},
        "basic_source_contract": "basic-source-contract.json",
        "reference_not_model_input": True,
        "before_first_send": [
            "核已批准文本、同一获准联网环境DNS/TLS/实际profile与密钥接线；不改共享配置。",
            "以本卡源上下文独立冻结可判定的基础条件及补充清单；36粗问题不冒充正式召回分母，不用模型输出反推条件。",
            "每类至少两家实源风险和正反断言冻结；按正式核心/高风险全核、补充20或不足全核，唯一对象去重计分。",
            "复用正式PDF/有效解析，不载任何旧导航、答案、review或人工参考；按本报告目录选章及完整跨页表格。",
            "先清本人可重建无依赖产物；准备库/冻结全源码与实际请求参数后开业务时钟，初始化另报。",
        ],
        "quality_criteria_unchanged": {"company_core_recall_min": 0.90,
            "company_audited_object_accuracy_min": 0.98, "S1_all_12_applicable_core_positive_negative": True,
            "known_bottom_line_error_released": 0, "S2_thresholds_unchanged": True},
        "stop_conditions": ["同一核心根因连续两家失败则停未启动；正常在途按原限收尾。",
            "核心污染无法局部隔离则封后续发送。", "任一公司或整组硬限达到即停止，不追加窗口或第十三家。",
            "整组冻结不热修；组末最多两次必要共性离线修复，旧首发不重写；任何付费后继另需批准。"],
        "outputs": ["十二家实际画像、基础/补充缺项与独立源核结论", "首发/补救分列的核心召回、唯一对象准确率和适用负例",
            "逐家公司阶段/全流程精确usage、物理次数、恢复、准备/工作/监督耗时与费用", "原owner三读一致及可用内容的独立研究根"],
        "cleanup": "必要证据一套持久核hash后finally清本人库/副本/过程日志/脚本；保正式PDF、有效解析、attempt、现有API根与既有并行。",
        "production_authorization": "not_authorized", "S2_release": False,
        "code_sha256_candidate_must_match_before_freeze": {str(p.relative_to(ROOT)): sha(p) for p in scope_files},
        "shared_config_sha256_not_contents": sha(ROOT / "config/13_llm.json"),
    }
    return acceptance, prospective, next_plan


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    acceptance, prospective, next_plan = build()
    if args.write:
        dump(DEST / "acceptance.json", acceptance)
        dump(DEST / "basic-source-contract.json", prospective)
        dump(DEST / "next-run-plan.json", next_plan)
    else:
        assert acceptance == json.loads((DEST / "acceptance.json").read_text())
        assert prospective == json.loads((DEST / "basic-source-contract.json").read_text())
        assert next_plan == json.loads((DEST / "next-run-plan.json").read_text())
    print(json.dumps({"checked_companies": len(acceptance["companies"]),
        "visible_records": acceptance["research_delivery"]["visible_records"],
        "historical_and_runtime_inputs_unchanged": True, "new_model_calls": 0,
        "formal_S1_pass": False}, ensure_ascii=False))
