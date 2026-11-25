"""Count explicit independent judgments only; no semantic auto-grading."""
from collections import Counter
import hashlib
import json
from pathlib import Path

ROOT=Path('/home/python/Quote')
E=ROOT/'openspec/changes/restore-company-profile-llm-led-generalization/workpaper-current-version-acceptance-20261125'
CHECK=ROOT/'data/checkpoints/company_profile_v1_acceptance_20261125'
read=lambda p:json.loads(p.read_text())
digest=lambda v:hashlib.sha256(json.dumps(v,ensure_ascii=False,sort_keys=True).encode()).hexdigest()

def status(row):
    return row.get('status',row.get('judgment',row.get('verdict')))

def phase_of(company,name):
    value=company.get('phases',company.get('stages',{})).get(name)
    if value is None:
        value=company.get('first_read' if name=='first' else 'automatic_final')
    return value if isinstance(value,dict) else {}

def core_result(value):
    if isinstance(value,bool): return value
    if not isinstance(value,dict): return None
    if isinstance(value.get('result'),bool): return value['result']
    if isinstance(value.get('supported'),bool): return value['supported']
    if isinstance(status(value),bool): return status(value)
    if status(value) in ('supported','covered'): return True
    if status(value) in ('error','missing','not_supported','not_observed'): return False
    return None

def summarize():
    manifest=read(E/'b-sample-manifest.json')['recommended']
    ordered=sorted(manifest,key=lambda r:(r['group_position'],['manufacturing','services','financial','multiple_businesses'].index(r['group'])))
    all_companies={}
    for file in ['a-audit-main.json','a-audit-group-1.json','a-audit-group-2.json','a-audit-group-3.json']:
        for company in read(E/file).get('companies',[]):
            assert company['instrument_id'] not in all_companies
            all_companies[company['instrument_id']]=company
    result={'task':'6.FN','fixed_companies':20,'fixed_core_questions':60,'seed':'6.FN:20261125','phases':{},'verification_errors':[]}
    for name in ['first','automatic-final']:
        counts=Counter(); objects=[]; companies=[]; supplemental={}
        for meta in ordered:
            stock=meta['instrument_id']; company=all_companies.get(stock,{}); audit=phase_of(company,name)
            api_path=CHECK/stock/'execution'/(name+'-api.json')
            if not api_path.exists():
                companies.append({'instrument_id':stock,'state':'no_api_snapshot','core_supported':0,'basic_usable':False})
                continue
            api=read(api_path); actual={(r['reading_work_id'],r['fact_id']):r for r in api['facts'] if r.get('fact')}
            judgments={(r['reading_work_id'],r['fact_id']):r for r in (audit.get('objects') or [])}
            if set(judgments)!=set(actual):
                companies.append({'instrument_id':stock,'state':'awaiting_full_independent_audit','actual_objects':len(actual),'audited_objects':len(judgments)})
                continue
            cores=audit.get('core',audit.get('core_questions',audit.get('core_three_questions',audit.get('core_judgments',{}))))
            core_values=[core_result(v) for v in cores.values()] if isinstance(cores,dict) else [core_result(v) for v in cores]
            if len(core_values)!=3: result['verification_errors'].append([stock,name,'core_question_shape',list(cores) if isinstance(cores,dict) else None])
            minimum=set(audit.get('minimal_core_fact_ids',[]))
            minimum.update(r.get('fact_id') if isinstance(r,dict) else r for r in audit.get('minimal_core_ids',[]))
            minimum.update(r['fact_id'] for r in judgments.values() if r.get('minimal_core_object') or r.get('minimal_core'))
            duplicate_ids=set()
            for duplicate in company.get('duplicates',company.get('semantic_duplicates',[]))+audit.get('semantic_duplicate_exclusions',[])+audit.get('semantic_duplicates',[]):
                if isinstance(duplicate,dict): duplicate_ids.add(duplicate.get('duplicate_fact_id',duplicate.get('fact_id')))
            this=[]
            for identity,row in actual.items():
                judged=judgments[identity]; fact=row['fact']; expected=digest(fact)
                saved=judged.get('consumer_fact_sha256',judged.get('api_fact_sha256',judged.get('exposed_fact_sha256')))
                if saved and saved!=expected: result['verification_errors'].append([stock,name,'fact_hash_mismatch',identity])
                verdict=status(judged)
                duplicate=(row['fact_id'] in duplicate_ids or bool(judged.get('semantic_duplicate_of')) or verdict=='duplicate')
                metric_checks=judged.get('metric_checks',judged.get('metrics',[]))
                if len(metric_checks)!=len(fact.get('metrics',[])): result['verification_errors'].append([stock,name,'metric_count_mismatch',identity,len(metric_checks),len(fact.get('metrics',[]))])
                metric_statuses=[status(m) for m in metric_checks]
                narrative=judged.get('narrative_status') or status(judged.get('narrative',{}))
                sampling=hashlib.sha256(f'6.FN:20261125:{stock}:{identity[0]}:{identity[1]}'.encode()).hexdigest()
                obj={'instrument_id':stock,'group':meta['group'],'reading_work_id':identity[0],'fact_id':identity[1],
                     'fact_sha256':expected,'status':verdict,'narrative_status':narrative,'metric_statuses':metric_statuses,
                     'standardized_metric_count':sum(m.get('value') is not None for m in fact.get('metrics',[])),
                     'minimal_core':row['fact_id'] in minimum,'duplicate':duplicate,'sampling_sha256':sampling}
                counts['gross_readable_objects']+=1
                counts['gross_exposed_metrics']+=len(metric_statuses)
                if duplicate: counts['semantic_duplicates']+=1; continue
                this.append(obj); objects.append(obj); counts['unique_objects']+=1; counts['object_'+str(verdict)]+=1
                counts['narrative_'+str(narrative)]+=1;counts['exposed_metrics']+=len(metric_statuses)
                counts['standardized_metrics']+=obj['standardized_metric_count']
                for ms in metric_statuses:counts['metric_'+str(ms)]+=1
            supported=sum(x is True for x in core_values); counts['core_supported']+=supported
            execution=api['readings'][-1].get('review_status')
            basic=supported==3 and (name=='first' or execution in ('completed','completed_with_open_findings'))
            counts['basic_usable']+=int(basic);counts['companies_with_independent_review_record']+=1
            counts['companies_with_all_objects_decisively_judged']+=int(all(x['status'] in ('supported','error') for x in this))
            candidates=sorted([x for x in this if not x['minimal_core']],key=lambda x:x['sampling_sha256'])
            supplemental[stock]=candidates
            companies.append({'instrument_id':stock,'state':'independently_audited','core_supported':supported,'basic_usable':basic,
                              'review_status':execution,'unique_objects':len(this),'supplement_universe':len(candidates),
                              'object_statuses':dict(Counter(x['status'] for x in this))})
        selected=[x for stock in supplemental for x in supplemental[stock][:20]]
        rest={g:sorted([x for stock in supplemental for x in supplemental[stock][20:] if x['group']==g],key=lambda x:x['sampling_sha256'])
              for g in ['manufacturing','services','financial','multiple_businesses']}
        while len(selected)<400 and any(rest.values()):
            for g in rest:
                if rest[g] and len(selected)<400: selected.append(rest[g].pop(0))
        judged=counts['object_supported']+counts['object_error']
        result['phases'][name]={'counts':dict(counts),'core_recall_fixed60_percent':counts['core_supported']/60*100,
            'independently_judged_accuracy_percent':100*counts['object_supported']/judged if judged else None,
            'accuracy_scope':'all explicitly independently reviewed unique outward objects in this current cohort; unchecked/unclear not manufactured as correct',
            'selected_supplement_count':len(selected),'supplement_universe':sum(map(len,supplemental.values())),
            'selected_supplement_statuses':dict(Counter(x['status'] for x in selected)),
            'companies':companies,'selected_supplement_objects':selected,'all_audited_unique_objects':objects}
    return result

if __name__=='__main__':
    value=summarize()
    import sys
    if sys.argv[1:]==['freeze-result']:
        assert not value['verification_errors'],value['verification_errors']
        value['audit_sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in E.glob('a-audit-*.json')}
        value['formal_verdict']='not_passed'
        value['no_paid_successor_after_irreversible_failure']=True
        value['all_market_accuracy_claim_allowed']=False
        target=E/'a-quality-result.json'
        assert not target.exists(),'quality result freezes only once'
        target.write_text(json.dumps(value,ensure_ascii=False,indent=2))
    print(json.dumps({'phases':{n:{k:v for k,v in p.items() if k not in ('companies','selected_supplement_objects','all_audited_unique_objects')} for n,p in value['phases'].items()},'verification_errors':value['verification_errors']},ensure_ascii=False,indent=2))
