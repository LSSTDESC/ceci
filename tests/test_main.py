from ceci.main import run_pipeline
from parsl import clear
import tempfile
import os
import pytest
import subprocess

from ceci.pipeline import Pipeline


def run1(*config_changes, config_yaml="tests/test.yml", dry_run=False, expect_fail=False, expect_outputs=True, flow_chart=None):
    try:
        with tempfile.TemporaryDirectory() as dirname:
            out_dir = os.path.join(dirname, "output")
            log_dir = os.path.join(dirname, "logs")
            config = [f"output_dir={out_dir}", f"log_dir={log_dir}"]
            config += config_changes
            pipe_config = Pipeline.build_config(config_yaml, config, dry_run, flow_chart)
            status = run_pipeline(pipe_config)
            if expect_fail:
                assert status != 0
            else:
                assert status == 0
            if expect_outputs:
                assert os.path.exists(os.path.join(out_dir, "wlgc_summary_data.txt"))
                assert os.path.exists(os.path.join(log_dir, "WLGCSummaryStatistic.out"))

    finally:
        clear()


def test_run_mini():
    run1()


def test_run_dry_run():
    run1(dry_run=True, expect_fail=False, expect_outputs=False)

def test_flow_chart():
    run1(flow_chart="test.png", expect_outputs=False)

def test_flow_chart_dot():
    run1(flow_chart="test.dot", expect_outputs=False)


def test_run_parsl():
    run1("launcher.name=parsl", "launcher.max_threads=3")


def test_run_cwl():
    run1("launcher.name=cwl", "launcher.dir=tests/cwl") == 0


def test_run_namespace():
    run1(config_yaml="tests/test_namespace.yml", expect_outputs=False) == 0
  



if __name__ == "__main__":
    test_run_dry_run()
    test_run_parsl()
    test_run_mini()
    test_run_cwl()
    test_pre_script()
