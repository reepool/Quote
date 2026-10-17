"""Read-only projection of the stopped second scheme; zero transport/model sends.

This does not resume, override its budget, or constitute a third resource scheme.
It builds the unsent synthesis/verify payloads using the same gateway serializer.
"""
import json
import gzip
from pathlib import Path
from research.company_profile.contracts import ReportSemanticRequestV3, ReportSemanticResponseV3
from research.company_profile.report_wire import compact_request, response_schema, instructions, expand_response
from research.company_profile.resource_probe import protocol_reply
from utils.llm import LlmConfig, LlmClient

root=Path(__file__).resolve().parent
r=root/'iteration2'
checkpoint=json.loads(gzip.decompress((r/'checkpoint.json.gz').read_bytes()))
interpretation=checkpoint['report_interpretation']
cited={n for e in [*(e for c in interpretation['source_conditions'] for e in c['evidence']),*(e for c in interpretation['records'] for e in c['evidence'])] for n in (e['page'],*e['continuation_pages'],*e['subject_evidence_pages'])}
pages=[{'page':p['page'],'text':p['text'],'readable':p.get('readable',True)} for p in checkpoint['pages'] if p.get('readable',True) and any(abs(p['page']-n)<=1 for n in cited)]
base={'schema_version':'company_profile_report_request.v3','report':interpretation['report'],'processing_identity':interpretation['processing_identity'],'source_scope':'full_context','source_pages':pages,'source_conditions':interpretation['source_conditions'],'records':interpretation['records'],'max_output_tokens':2500,'timeout_seconds':800.0}
synthesis=ReportSemanticRequestV3.model_validate_json(json.dumps({**base,'purpose':'synthesize_views','request_id':interpretation['call_receipts'][-1]['request_id']}))
wire_reply=protocol_reply(compact_request(synthesis))
expanded=ReportSemanticResponseV3.model_validate_json(json.dumps(expand_response(wire_reply,synthesis)))
verify=ReportSemanticRequestV3.model_validate_json(json.dumps({**base,'purpose':'verify_views','request_id':interpretation['call_receipts'][-1]['request_id'].rsplit(':',1)[0]+':4','max_output_tokens':2000,'claims':[c.model_dump(mode='json') for c in expanded.claims]}))
verify_reply=protocol_reply(compact_request(verify))
expand_response(verify_reply,verify)
config=LlmConfig.from_mapping(json.loads(Path('config/13_llm.json').read_text())['llm'])
projected=[]
for request in (synthesis,verify):
 wire=compact_request(request); schema=response_schema(request)
 result={'classification':'UNSENT_projection_same_iteration2_no_admission_or_transport','purpose':request.purpose,'full_context_pages':[p['page'] for p in request.source_pages],'context_text_utf8_bytes':sum(len(p['text'].encode()) for p in request.source_pages),'wire_schema_utf8_bytes':len(json.dumps(schema,ensure_ascii=False,separators=(',',':')).encode()),'synthetic_response_utf8_bytes':len(json.dumps(protocol_reply(wire),ensure_ascii=False).encode()),'max_output_tokens':request.max_output_tokens,'physical_source_bounds':[]}
 for p in config.concrete_profiles_for('semantic_extraction'):
  payload=LlmClient._build_payload(model=p.model,messages=[{'role':'system','content':instructions(request)},{'role':'user','content':json.dumps(wire,ensure_ascii=False,separators=(',',':'))}],schema=schema,schema_name='company_profile_report_response',mode='json_object',temperature=0,max_output_tokens=request.max_output_tokens,max_output_tokens_field=p.max_output_tokens_field,stream=p.stream,stream_include_usage=p.stream_include_usage)
  input_bound=len(json.dumps(payload,ensure_ascii=False,separators=(',',':')).encode())+256
  result['physical_source_bounds'].append({'profile':p.name,'provider_resource':p.provider_resource,'model':p.model,'reserved_input_units':input_bound,'max_output_tokens':request.max_output_tokens,'total_reserved_units':input_bound+request.max_output_tokens})
 projected.append(result)
ledger=json.loads((r/'budget-final.json').read_text())
assert any(x['reserved_input_units']==ledger['denied_request']['reserved_input_tokens'] for x in projected[0]['physical_source_bounds'])
normal=ledger['charged_tokens']+sum(max(x['total_reserved_units'] for x in p['physical_source_bounds']) for p in projected)
largest=max(max(x['total_reserved_units'] for x in p['physical_source_bounds']) for p in projected)
result={'schema_version':'b1_resource_projection.v1','classification':'unsent_plan_not_complete_rehearsal','real_calls':0,'simulated_additional_sends':0,'scheme':'iteration2_unchanged','actual_charged_units':ledger['charged_tokens'],'unsent_requests':projected,'normal_four_request_upper_units':normal,'normal_gap_units':normal-120000,'one_retry_additional_upper_units':max(largest,ledger['attempts'][0]['charged_tokens']),'normal_plus_one_retry_upper_units':normal+max(largest,ledger['attempts'][0]['charged_tokens']),'normal_requests':4,'with_one_retry_requests':5,'wall_deadline_seconds':900,'context_and_currency_cost':'unknown_until_verified_source资料','supplement':'At most one additional source discovery round, bounded by remaining shared ledger; no capacity is proved for it','semantics':'All 14 conditions and 7 claims are synthetic unclear; 27 checks are protocol shapes, not business judgments'}
assert result == json.loads((root/'remaining-projection.json').read_text()), 'projection drift'
print(json.dumps({k:result[k] for k in ('normal_four_request_upper_units','normal_gap_units','one_retry_additional_upper_units','normal_plus_one_retry_upper_units')},ensure_ascii=False))
