import ceci_example  # noqa: F401  (registers the example stages)
import yaml
from ceci import DryRunPipeline
from ceci.pipeline.templates import read_and_apply_template


def test_stage_name_differs_from_classname():
    """A stage whose pipeline name differs from its class name should
    be set up with the pipeline name as its instance name."""
    config = yaml.safe_load(read_and_apply_template("tests/test.yml"))
    stages = []
    for stage in config["stages"]:
        if stage["name"] == "WLGCSelector":
            stage = {**stage, "name": "MySelector", "classname": "WLGCSelector"}
        stages.append(stage)

    pipeline = DryRunPipeline(stages, {"interval": 0.5, "name": "mini"})
    run_config = {
        "log_dir": config["log_dir"],
        "output_dir": config["output_dir"],
        "resume": False,
    }
    pipeline.initialize(config["inputs"], run_config, config["config"])

    assert "MySelector" in pipeline.stage_names
    assert "WLGCSelector" not in pipeline.stage_names
    instance_names = [s.instance_name for s in pipeline.stages]
    assert instance_names == pipeline.stage_names
    assert pipeline.run() == 0
