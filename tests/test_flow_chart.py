import yaml
from ceci import FlowChartPipeline
from ceci.pipeline.templates import read_and_apply_template
import os

def test_flow_chart():
    config = yaml.safe_load(read_and_apply_template("tests/test.yml"))
    launcher_config = {"interval": 0.5, "name": "mini"}

    pipeline = FlowChartPipeline(config["stages"], launcher_config)

    run_config = {
        "log_dir": config["log_dir"],
        "output_dir": config["output_dir"],
        "resume": False,
        "flow_chart": "ceci_test_flow_chart.png",
    }

    pipeline.initialize(config["inputs"], run_config, config["config"])
    status = pipeline.run()

    assert os.path.exists("ceci_test_flow_chart.png")
    os.remove('ceci_test_flow_chart.png')
