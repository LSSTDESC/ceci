from ceci import PipelineStage, MiniPipeline, Pipeline, FlowChartPipeline
from ceci.pipeline import StageExecutionConfig
from ceci.pipeline.graph import StageStatus
from ceci_example.types import TextFile
import pytest


class GraphStageX(PipelineStage):
    name = "GraphStageX"
    inputs = [("gx_in", TextFile)]
    outputs = [("gx_out", TextFile)]
    config_options = {"a": 1}

    def run(self):  # pragma: no cover
        pass


class GraphStageY(PipelineStage):
    name = "GraphStageY"
    inputs = [("gx_out", TextFile)]
    outputs = [("gy_out", TextFile), ("gy_extra", TextFile)]
    config_options = {"b": 2}

    def run(self):  # pragma: no cover
        pass


class GraphStageZ(PipelineStage):
    name = "GraphStageZ"
    inputs = [("gy_out", TextFile), ("gx_in", TextFile)]
    outputs = [("gz_out", TextFile)]
    config_options = {}

    def run(self):  # pragma: no cover
        pass


STAGES = [{"name": "GraphStageZ"}, {"name": "GraphStageY"}, {"name": "GraphStageX"}]
INPUTS = {"gx_in": "./tests/inputs/gx_in.txt"}
CONFIG = {"GraphStageX": {"a": 10}, "global": {"c": 3}}


def make_pipeline(tmp_path, **run_config):
    rc = {
        "output_dir": str(tmp_path),
        "log_dir": str(tmp_path),
        "resume": "restart",
    }
    rc.update(run_config)
    pipeline = FlowChartPipeline(STAGES, {"name": "mini"})
    pipeline.initialize(INPUTS, rc, CONFIG)
    return pipeline


def test_stage_nodes(tmp_path):
    pipeline = make_pipeline(tmp_path)
    graph = pipeline.graph

    assert pipeline.stage_names == ["GraphStageX", "GraphStageY", "GraphStageZ"]
    assert [s.instance_name for s in pipeline.stages] == pipeline.stage_names

    for name in pipeline.stage_names:
        node = graph.nodes[name]
        assert node["type"] == "stage"
        assert isinstance(node["sec"], StageExecutionConfig)
        assert node["sec"].stage_obj is pipeline[name]
        assert node["status"] == StageStatus.QUEUED
        assert pipeline.get_stage_status(name) == StageStatus.QUEUED

    # stage configuration, including global options, is on the node
    assert graph.nodes["GraphStageX"]["config"] == {"a": 10, "c": 3}
    assert graph.nodes["GraphStageY"]["config"] == {}
    assert pipeline.GraphStageX.config["a"] == 10


def test_file_nodes(tmp_path):
    pipeline = make_pipeline(tmp_path)
    graph = pipeline.graph

    assert graph.nodes["gx_in"]["type"] == "input"
    assert graph.nodes["gx_in"]["path"] == INPUTS["gx_in"]

    node = graph.nodes["gy_out"]
    assert node["type"] == "output"
    assert node["creator"] == "GraphStageY"
    assert node["ftype"] is TextFile
    assert node["path"] == f"{tmp_path}/gy_out.txt"

    # input paths for a stage come from the graph, in input tag order
    assert pipeline.stage_input_paths(pipeline.GraphStageZ) == {
        "gy_out": node["path"],
        "gx_in": INPUTS["gx_in"],
    }


def test_trimmed_stages_removed(tmp_path):
    pipeline = make_pipeline(tmp_path, **{"from": "GraphStageY"})
    assert pipeline.stage_names == ["GraphStageY", "GraphStageZ"]
    assert "GraphStageX" not in pipeline.graph
    with pytest.raises(AttributeError):
        pipeline.GraphStageX

    # the output of the trimmed stage is now an input from the previous run
    assert pipeline.overall_inputs["gx_out"] == f"{tmp_path}/gx_out.txt"
    assert pipeline.graph.nodes["gx_out"]["path"] == f"{tmp_path}/gx_out.txt"

    # Only the remaining stages are saved
    pipeline.save(str(tmp_path / "trimmed.yml"))
    saved = Pipeline.build_config(str(tmp_path / "trimmed.yml"))
    assert [s["name"] for s in saved["stages"]] == ["GraphStageY", "GraphStageZ"]


def test_remove_stage():
    pipeline = Pipeline(STAGES, {"name": "mini"})
    assert "gy_extra" in pipeline.graph
    pipeline.remove_stage("GraphStageY")
    assert "GraphStageY" not in pipeline.graph
    assert "gy_out" not in pipeline.graph
    assert "gy_extra" not in pipeline.graph
    with pytest.raises(KeyError):
        pipeline.remove_stage("GraphStageY")


def test_flow_chart_structure_only(tmp_path):
    pipeline = make_pipeline(tmp_path)
    dot_file = str(tmp_path / "chart.dot")
    pipeline.make_flow_chart(dot_file)
    text = open(dot_file).read()
    assert "GraphStageX" in text
    assert "StageExecutionConfig" not in text
    assert "status" not in text


def test_interactive_inputs():
    pipeline = Pipeline.interactive()
    pipeline.add_inputs({"gx_in": "gx_in.txt"})
    outputs = pipeline.build_stage(GraphStageX, a=5)
    assert outputs == {"gx_out": "./gx_out.txt"}
    assert pipeline.GraphStageX.get_input("gx_in") == "gx_in.txt"
    assert pipeline.graph.nodes["gx_out"]["path"] == "./gx_out.txt"

    # outputs of earlier stages are used as inputs to later ones,
    # unless given explicitly
    pipeline.build_stage(GraphStageY)
    assert pipeline.GraphStageY.get_input("gx_out") == "./gx_out.txt"
    pipeline.build_stage(GraphStageZ, gy_out="elsewhere.txt")
    assert pipeline.GraphStageZ.get_input("gy_out") == "elsewhere.txt"

    # Inputs cannot be set for files that a stage generates
    with pytest.raises(ValueError):
        pipeline.add_inputs({"gx_out": "x.txt"})


def test_mini_status_callback():
    events = []
    pipeline = MiniPipeline(
        STAGES, {"name": "mini"}, callback=lambda *args: events.append(args)
    )

    class FakeJob:
        name = "GraphStageY"

    pipeline._status_callback("launch", {"job": FakeJob})
    assert pipeline.get_stage_status("GraphStageY") == StageStatus.RUNNING
    pipeline._status_callback("completed", {"job": FakeJob})
    assert pipeline.get_stage_status("GraphStageY") == StageStatus.COMPLETE
    pipeline._status_callback("fail", {"job": FakeJob})
    assert pipeline.get_stage_status("GraphStageY") == StageStatus.FAILED
    # user callback still gets everything
    assert [e[0] for e in events] == ["launch", "completed", "fail"]


def test_mini_dag(tmp_path):
    rc = {"output_dir": str(tmp_path), "log_dir": str(tmp_path), "resume": "restart"}
    pipeline = MiniPipeline(STAGES, {"name": "mini"})
    pipeline.initialize(INPUTS, rc, CONFIG)
    jobs, _ = pipeline.run_info
    dag = pipeline.build_dag(jobs)
    deps = {job.name: sorted(j.name for j in parents) for job, parents in dag.items()}
    assert deps == {
        "GraphStageX": [],
        "GraphStageY": ["GraphStageX"],
        "GraphStageZ": ["GraphStageY"],
    }
