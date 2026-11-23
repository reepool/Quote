"""Bound unread-state consumption never rewrites author facts or supplies business values."""
import copy
import hashlib
import json

import pytest

from research.company_profile.workpaper import WorkpaperDraftResponse
from research.company_profile.workpaper_llm import normalize_retained_enrichment_unread, run_workpaper
from research.company_profile.workpaper_review import prepare_review_recovery
from research.company_profile.runtime import CompanyProfileRuntimeRecordV3
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _report
from tests.unit.test_research.test_company_profile_workpaper_format_consumption import local_input, NoModelCalls


def source():
    resume, identity, draft = local_input()
    draft['elements']['products_services'].update(status='not_read_for_this_task', facts=[], limitations=['本轮未读此项。'])
    identity['workpaper_enrichment'] = {'targets': [{'element': 'business_structure', 'question': '业务是什么？'}]}
    resume.update(mode='local_enrichment_schema_only', original_processing_identity=identity.copy())
    raw = json.dumps(draft, ensure_ascii=False)
    resume['failed_response'].update(content=raw, content_sha256=hashlib.sha256(raw.encode()).hexdigest())
    resume['failed_call']['response_hash'] = resume['failed_response']['content_sha256']
    identity['workpaper_extraction_resume_hash'] = hashlib.sha256(json.dumps(resume,ensure_ascii=False,sort_keys=True).encode()).hexdigest()
    return raw, draft, resume, identity


@pytest.mark.parametrize('damage', [None, 'target', 'facts', 'no_limitation', 'blank_limitation', 'other_status', 'no_targets'])
def test_only_explicit_empty_non_target_unread_state_can_be_normalized(damage):
    _, draft, _, _ = source()
    targets=['business_structure']
    element=draft['elements']['products_services']
    if damage=='target': targets=['products_services']
    elif damage=='facts': element['facts']=copy.deepcopy(draft['elements']['business_structure']['facts'])
    elif damage=='no_limitation': element['limitations']=[]
    elif damage=='blank_limitation': element['limitations']=[' ']
    elif damage=='other_status': element['status']='unknown-state'
    elif damage=='no_targets': targets=[]
    raw=json.dumps(draft,ensure_ascii=False)
    before=copy.deepcopy(draft)
    if damage:
        with pytest.raises(ValueError):
            normalized,_=normalize_retained_enrichment_unread(raw,targets)
            WorkpaperDraftResponse.model_validate(normalized)
    else:
        normalized,record=normalize_retained_enrichment_unread(raw,targets)
        assert record.normalized_elements==('products_services',)
        assert record.original_content_sha256==hashlib.sha256(raw.encode()).hexdigest()
        expected=copy.deepcopy(before);expected['elements']['products_services']['status']='unclear'
        assert normalized==expected
    assert draft==before


def test_local_unread_consumption_keeps_rejected_author_and_review_plan_binding():
    raw, original, resume, identity=source()
    result=run_workpaper(report=_report(),pages=PAGES,processing_identity=identity,
        provider=NoModelCalls(),work_id='unread-local',extraction_resume=resume)
    paper=result.workpaper
    assert paper.generation.extraction.contract_status=='rejected'
    assert paper.generation.extraction.model=='old-extractor'
    assert paper.generation.extraction.usage.total_tokens==123
    assert paper.generation.local_format_normalization is None
    assert paper.generation.local_schema_normalization.normalized_elements==('products_services',)
    assert result.receipts[-1]['raw_content']==raw
    assert paper.elements.products_services.limitations==tuple(original['elements']['products_services']['limitations'])
    assert paper.elements.business_structure.facts[0].text==original['elements']['business_structure']['facts'][0]['text']
    # The same saved-receipt owner must bind the rejected draft before future review.
    record=type('Saved',(),{'workpaper':paper,'workpaper_receipts':result.receipts})()
    seed, recovery, receipts=prepare_review_recovery(record)
    assert seed==paper and recovery is None and len(receipts)==2
    assert seed.quality_review is None
