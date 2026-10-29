import os
import pytest
from ceci import Pipeline
from ceci.pipeline.register import RegistrationError, MAX_REGISTER_BYTES, plan_registration, check_size
from ceci.main import parser


class FakeQuery:
    def __init__(self, existing=(), executions=()):
        self.existing = existing
        self.executions = {n: 7 for n in executions}

    def gen_filter(self, prop, op, value):
        return (prop, value)

    def get_table_values(self, table, props, filters):
        n = dict(filters)["execution.name"]
        return {"execution.execution_id": [self.executions.get(n, 42)] if n in self.executions else []}

    def find_datasets(self, props, filters):
        name = dict(filters)["dataset.name"]
        return {"dataset.dataset_id": [1] if name in self.existing else []}


class FakeExecution:
    def __init__(self):
        self.calls = []

    def register(self, name, **kwargs):
        if kwargs["configuration"]:
            with open(kwargs["configuration"]) as f:
                kwargs["configuration_text"] = f.read()
        self.calls.append((name, kwargs))
        return 42


class FakeDataset:
    def __init__(self):
        self.calls = []

    def register(self, name, version, **kwargs):
        self.calls.append((name, version, kwargs))
        return len(self.calls), kwargs["execution_id"]


class FakeRegistry:
    def __init__(self, existing=(), executions=()):
        self.Query = FakeQuery(existing, executions)

        class Registrar:
            execution = FakeExecution()
            dataset = FakeDataset()

        self.Registrar = Registrar


def make_pipeline(tmp_path, complete=True):
    config = Pipeline.build_config(
        "tests/test.yml", [f"output_dir={tmp_path}"], True, "", None
    )
    p = Pipeline.create(config)
    outputs = Pipeline.find_all_outputs(p)
    for i, path in enumerate(outputs.values()):
        if complete or i > 0:
            with open(path, "w") as f:
                f.write("data")
    return p, outputs


def test_register(tmp_path):
    p, outputs = make_pipeline(tmp_path)
    registry = FakeRegistry()
    p.data_registry = {"registry": registry}
    ids = p.register_outputs("exec1", configuration={"pipe.yml": "a: 1"})

    assert set(ids) == {f"exec1_{tag}" for tag in outputs}
    assert len(registry.Registrar.execution.calls) == 1
    name, kw = registry.Registrar.execution.calls[0]
    assert name == "exec1"
    assert "a: 1" in kw["configuration_text"]
    for name, version, kw in registry.Registrar.dataset.calls:
        assert version == "1.0.0"
        assert kw["execution_id"] == 42
        assert os.path.exists(kw["old_location"])


def test_register_version(tmp_path):
    p, _ = make_pipeline(tmp_path)
    registry = FakeRegistry()
    p.data_registry = {"registry": registry}
    p.register_outputs("exec1", version="2.1.0")
    assert all(c[1] == "2.1.0" for c in registry.Registrar.dataset.calls)
    with pytest.raises(RegistrationError):
        p.register_outputs("exec1", version="two")


def test_dry_run(tmp_path, capsys):
    p, outputs = make_pipeline(tmp_path)
    registry = FakeRegistry()
    p.data_registry = {"registry": registry}
    assert p.register_outputs("exec1", dry_run=True) == {}
    assert not registry.Registrar.execution.calls
    out = capsys.readouterr().out
    assert all(f"exec1_{tag}" in out for tag in outputs)


def test_incomplete(tmp_path):
    p, outputs = make_pipeline(tmp_path, complete=False)
    registry = FakeRegistry()
    p.data_registry = {"registry": registry}
    with pytest.raises(RegistrationError, match="dregs-incomplete"):
        p.register_outputs("exec1")
    assert not registry.Registrar.execution.calls
    ids = p.register_outputs("exec1", allow_incomplete=True)
    assert len(ids) == len(outputs) - 1


def test_existing_name(tmp_path):
    p, outputs = make_pipeline(tmp_path)
    first = f"exec1_{next(iter(outputs))}"
    registry = FakeRegistry(existing=[first])
    p.data_registry = {"registry": registry}
    with pytest.raises(RegistrationError, match="dregs-more"):
        p.register_outputs("exec1")
    assert not registry.Registrar.execution.calls


def test_more(tmp_path):
    p, outputs = make_pipeline(tmp_path)
    tags = list(outputs)
    already = [f"exec1_{t}" for t in tags[:3]]
    registry = FakeRegistry(existing=already, executions=["exec1"])
    p.data_registry = {"registry": registry}
    ids = p.register_outputs("exec1", more=True)

    assert set(ids) == {f"exec1_{t}" for t in tags[3:]}
    # No new execution, and the new datasets join the existing one
    assert not registry.Registrar.execution.calls
    assert all(c[2]["execution_id"] == 7 for c in registry.Registrar.dataset.calls)


def test_more_nothing_new(tmp_path):
    p, outputs = make_pipeline(tmp_path)
    registry = FakeRegistry(existing=[f"exec1_{t}" for t in outputs], executions=["exec1"])
    p.data_registry = {"registry": registry}
    assert p.register_outputs("exec1", more=True) == {}
    assert not registry.Registrar.dataset.calls


def test_more_no_execution(tmp_path):
    p, _ = make_pipeline(tmp_path)
    p.data_registry = {"registry": FakeRegistry()}
    with pytest.raises(RegistrationError, match="exactly one"):
        p.register_outputs("exec1", more=True)


def test_illegal_name(tmp_path):
    p, _ = make_pipeline(tmp_path)
    p.data_registry = {"registry": FakeRegistry()}
    with pytest.raises(RegistrationError, match="illegal"):
        p.register_outputs("bad name")


def test_symlink_refused(tmp_path):
    target = tmp_path / "target.txt"
    target.write_text("x")
    link = tmp_path / "link.txt"
    link.symlink_to(target)
    with pytest.raises(RegistrationError, match="symlink"):
        plan_registration({"a": str(link)}, "e", "1.0.0", False)


def test_large(tmp_path, monkeypatch):
    f = tmp_path / "f.txt"
    f.write_text("x" * 100)
    monkeypatch.setattr("ceci.pipeline.register.MAX_REGISTER_BYTES", 10)
    plan, _ = plan_registration({"a": str(f)}, "e", "1.0.0", False)
    assert plan[0][0] == "e_a"
    with pytest.raises(RegistrationError, match="dregs-allow-large"):
        check_size(plan, False)
    check_size(plan, True)


def test_cli_args():
    args = parser.parse_args(
        ["--register", "x", "--dregs-version", "2.0.0", "--dregs-allow-large",
         "--dregs-incomplete", "--dregs-more", "--dregs-dry-run", "p.yml"]
    )
    assert args.register == "x"
    assert args.dregs_version == "2.0.0"
    assert args.dregs_allow_large and args.dregs_incomplete and args.dregs_dry_run and args.dregs_more
    assert parser.parse_args(["--register", "x", "p.yml"]).dregs_version == "1.0.0"
