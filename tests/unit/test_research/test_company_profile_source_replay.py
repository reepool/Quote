"""Use the existing queue replay owner after a binding-only acquire failure."""
import json

from research.business_profile_async_production import BusinessProfileWorkRepository
from tests.unit.test_research.test_business_profile_async_production import _frontier
from tests.unit.test_research.test_business_profile_exposure_components import _storage


def test_explicit_force_replay_preserves_unbound_acquire_history_and_identity(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    repository = BusinessProfileWorkRepository(storage, checkpoint_root=tmp_path / "checkpoints")
    args = dict(knowledge_cutoff="2026-08-30", instrument_ids=("600000.SH",),
        processing_identity={"rules": "source-replay-controlled-test"})
    initial = repository.enqueue_latest_annual(**args)
    work_id = initial["work_ids"][0]
    with storage.get_connection() as connection:
        metadata = repository.get(work_id)["metadata"]
        metadata["stage_results"] = {"acquire": {"status": "blocked", "reason": "pages_not_bound"}}
        connection.execute("UPDATE business_profile_work_items SET stage='acquire', status='machine_rework', "
            "last_error='machine_rework:pages_not_bound', metadata_json=? WHERE work_id=?",
            (json.dumps(metadata), work_id))
        connection.commit()
    prior = repository.get(work_id)
    replay = repository.enqueue_latest_annual(**args, force=True)
    current = repository.get(work_id)
    assert replay["work_ids"] == initial["work_ids"]
    assert current["stage"] == "acquire" and current["status"] == "pending"
    assert prior["last_error"] == "machine_rework:pages_not_bound"
    history = current["metadata"]["recovery_history"]
    assert history[-1]["reason"] == "force_replay_checkpoint_rotated"
    assert history[-1]["invalidated_stage_result_names"] == ["acquire"]
