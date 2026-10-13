import json
import pytest
from codeants_2pf_hcr import fresh_batch


@pytest.mark.parametrize("control_passes", [False, True])
def test_batch_is_conditional_without_polling_or_repair(tmp_path, monkeypatch, control_passes):
    specs = []
    for i in range(3):
        path = tmp_path / f"fish{i}.json"
        path.write_text("{}")
        specs.append({"path": str(path), "sha256": fresh_batch._file_hash(path)})
    batch = tmp_path / "batch.json"
    batch.write_text(json.dumps({"specs": specs}))
    calls = []
    def finish(*, spec_path, validate_noop):
        calls.append(spec_path)
        assert validate_noop
        return {"status": "pass" if control_passes else "fail", "report": "saved", "error": None}
    monkeypatch.setattr(fresh_batch, "run_finishing_job", finish)
    result = fresh_batch.run_fresh_batch(batch)
    assert len(calls) == (3 if control_passes else 1)
    assert result["status"] == ("pass" if control_passes else "fail")
    assert batch.with_suffix(".report.json").exists()


def test_batch_rejects_modified_spec_before_execution(tmp_path, monkeypatch):
    spec = tmp_path / "fish.json"
    spec.write_text("modified")
    batch = tmp_path / "batch.json"
    batch.write_text(json.dumps({"specs": [{"path": str(spec), "sha256": "wrong"}]}))
    monkeypatch.setattr(fresh_batch, "run_finishing_job", lambda **kw: pytest.fail("must not execute"))
    assert fresh_batch.run_fresh_batch(batch)["status"] == "fail"
