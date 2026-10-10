"""Task 2.1: the first genuine unseen execute through second export."""
import asyncio
import json
import time
from pathlib import Path
from datetime import datetime,timezone
from utils import config_manager
from research.storage import ResearchStorageManager
from research.announcement_assets import AnnouncementAssetConfig, AnnouncementAssetRepository, AnnouncementAssetService
from research.announcement_assets.access import AnnouncementAssetAccess
from research.company_profile.execution import default_processing_identity
from research.company_profile.m4_next_batch import load_m4_next_batch_plan, load_m4_next_batch_observation, remaining_token_budget
from research.company_profile.operations import execute_published_task

ROOT=Path.cwd()
CHECKPOINT=ROOT/'data/checkpoints/company_profile_common_core'
PLAN=CHECKPOINT/'reports/m4_v41_same_source_repair'
OUTPUT=ROOT/'data/research/company_profile_common_core/m4_v41_same_source_repair'
EXPORT=ROOT/'data/exports/m4_v41_same_source_repair'
CHANGE=ROOT/'openspec/changes/deliver-company-profile-m4-v40-unseen-small-batch'
IDENTITY={**default_processing_identity(),'revenue_sentence_repair':'v41'}

async def main():
 plan=load_m4_next_batch_plan(CHECKPOINT,plan_directory=PLAN)
 assert plan is not None and (CHANGE/'v41-source-freeze-receipt.json').exists()
 assert (CHANGE/'v41-source-contract.json').exists()
 assert (CHANGE/'freeze-receipt.json').exists()
 receipt=json.loads((CHANGE/'v41-source-freeze-receipt.json').read_text())
 assert all(__import__('hashlib').sha256((ROOT/p).read_bytes()).hexdigest()==h for p,h in receipt['executed_code_sha256'].items())
 assert receipt['source_scope_sha256']==__import__('hashlib').sha256((CHANGE/'v41-source-contract.json').read_bytes()).hexdigest()
 assert not (PLAN/'formal_execution.json').exists() and not EXPORT.exists()
 assert not list(OUTPUT.rglob('bp-work-*.json'))
 rc=config_manager.get_research_config(); storage=ResearchStorageManager(rc)
 cfg=AnnouncementAssetConfig.from_research_config(rc,project_root=ROOT)
 repository=AnnouncementAssetRepository(Path(str(rc.storage.db_path)).resolve())
 asset_service=AnnouncementAssetService(repository=repository,config=cfg,acquisition_service=None,attachment_retriever=None)
 access=AnnouncementAssetAccess(repository=repository,config=cfg,service=asset_service)
 common=dict(storage=storage,shared_asset_access=access,checkpoint_root=CHECKPOINT,
  output_root=OUTPUT,processing_identity=IDENTITY,plan_directory=PLAN,knowledge_cutoff=plan.knowledge_cutoff)
 results={};started_at=datetime.now(timezone.utc).isoformat();start=time.monotonic();print('FIRST_FORMAL_EXECUTE_START',flush=True)
 for report in plan.reports:
  company_start=time.monotonic(); ins=report.instrument_id
  budget=remaining_token_budget(load_m4_next_batch_observation(CHECKPOINT,plan,plan_directory=PLAN))
  run=await execute_published_task(action='run',instrument_ids=(ins,),token_budget=budget,max_elapsed_seconds=300.0,**common)
  query=await execute_published_task(action='query',instrument_ids=(ins,),**common)
  export=await execute_published_task(action='export',instrument_ids=(ins,),output_directory=EXPORT/ins,**common)
  elapsed=time.monotonic()-company_start
  results[ins]=dict(run=run,query=query,export=export,requested_token_budget=budget,execute_through_export_seconds=elapsed)
  print('DELIVERED',ins,run['state'],query['state'],export['state'],round(elapsed,3),flush=True)
 elapsed=time.monotonic()-start
 payload=dict(executed_code_sha256=json.loads((CHANGE/'v41-source-freeze-receipt.json').read_text())['executed_code_sha256'],processing_identity=IDENTITY,plan_id=plan.plan_id,whole_round_seconds=elapsed,
  started_at=started_at,finished_at=datetime.now(timezone.utc).isoformat(),measurement='first_execute_to_second_export_return',formal_execution_index=1,results=results,source_scope_sha256=__import__('hashlib').sha256((CHANGE/'v41-source-contract.json').read_bytes()).hexdigest(),
  time_gate_passed=elapsed<=300.0)
 (PLAN/'formal_execution.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n')
 print('WHOLE_ROUND_SECONDS',elapsed,flush=True)
 print('OBSERVATION',(PLAN/'observation.json').read_text(),flush=True)

if __name__=='__main__':asyncio.run(main())
