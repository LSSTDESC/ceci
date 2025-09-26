from ceci.main import run_pipeline
from ceci.tools.ancestors import print_ancestors
from parsl import clear
import tempfile
import os
import pytest
import subprocess
import networkx
import math

from ceci.pipeline import Pipeline


def test_save_load():
    config_yaml="tests/test.yml"
    with tempfile.TemporaryDirectory() as dirname:
        out_dir = os.path.join(dirname, "output")
        log_dir = os.path.join(dirname, "logs")
        yml_path = os.path.join(dirname, "saved_pipeline.yml")
        config = [f"output_dir={out_dir}", f"log_dir={log_dir}", "resume=False"]
        pipe_config = Pipeline.build_config(config_yaml, config)

        # Run the first time
        p = Pipeline.create(pipe_config)
        p.run()

        p.save(yml_path)

        with open(yml_path) as f:
            print(f.read())

        # load from the saved path and run again
        q = Pipeline.read(yml_path, config)
        q.run()



def run1(*config_changes, config_yaml="tests/test.yml", dry_run=False, expect_fail=False, expect_outputs=True, flow_chart=None, unexpected_outputs=None):
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
            if expect_outputs is True:
                assert os.path.exists(os.path.join(out_dir, "wlgc_summary_data.txt"))
                assert os.path.exists(os.path.join(log_dir, "WLGCSummaryStatistic.out"))
            elif expect_outputs is False:
                pass
            else:
                for output in expect_outputs:
                    assert os.path.exists(os.path.join(out_dir, output))

            if unexpected_outputs is not None:
                for output in unexpected_outputs:
                    assert not os.path.exists(os.path.join(out_dir, output))

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

def test_trim_to_stage():
    expected = [
        "twopoint_data.txt",
        "random_catalog.txt",
        "diagnostic_maps.txt",
        "shear_catalog.txt",
        "photoz_pdfs.txt",
        "tomography_catalog.txt",
    ]
    unexpected = [
        "covariance_shared.txt",
        "wlgc_summary_data.txt",
        "source_summary_data.txt",
    ]
    run1("to=WLGCTwoPoint", expect_outputs=expected, unexpected_outputs=unexpected)

def test_trim_to_output():
    expected = [
        "shear_catalog.txt",
        "photoz_pdfs.txt",
        "tomography_catalog.txt",
    ]
    unexpected = [
        "random_catalog.txt",
        "twopoint_data.txt",
        "diagnostic_maps.txt",
        "covariance_shared.txt",
        "wlgc_summary_data.txt",
        "source_summary_data.txt",
    ]
    run1("to=tomography_catalog", expect_outputs=expected, unexpected_outputs=unexpected)


def test_trim_from_to():
    config_yaml="tests/test.yml"
    with tempfile.TemporaryDirectory() as dirname:
        out_dir = os.path.join(dirname, "output")
        log_dir = os.path.join(dirname, "logs")
        yml_path = os.path.join(dirname, "saved_pipeline.yml")


        config = [f"output_dir={out_dir}", f"log_dir={log_dir}", "resume=False"]
        pipe_config = Pipeline.build_config(config_yaml, config)

        # Run the first time
        p = Pipeline.create(pipe_config)
        p.run()

        # get the last update times for these files
        update_times = {}
        for filename in os.listdir(out_dir):
            path = os.path.join(out_dir, filename)
            update_times[filename] = os.path.getmtime(path)


        # Run again but with only a subset of the pipeline
        config = [f"output_dir={out_dir}", f"log_dir={log_dir}", "resume=False", "from=WLGCSelector", "to=covariance_shared"]
        pipe_config = Pipeline.build_config(config_yaml, config)

        p = Pipeline.create(pipe_config)
        p.run()

        should_be_new = [
            "tomography_catalog.txt",
            "source_summary_data.txt",
            "covariance_shared.txt",
        ]

        # check that the correct files have been re-generated,
        # and only those files
        for filename in os.listdir(out_dir):
            path = os.path.join(out_dir, filename)
            update_time = os.path.getmtime(path)
            if filename in should_be_new:
                assert update_time > update_times[filename]
            else:
                assert math.isclose(update_time, update_times[filename])




def test_run_namespace():
    run1(config_yaml="tests/test_namespace.yml", expect_outputs=False) == 0
  
def test_ancestors_stage(capsys):
    print_ancestors("tests/test.yml", "WLGCRandoms")
    captured = capsys.readouterr()
    assert captured.out.strip() == "SysMapMaker"

def test_ancestors_output(capsys):
    print_ancestors("tests/test.yml", "tomography_catalog")
    captured = capsys.readouterr().out
    assert "WLGCSelector" in captured
    assert "shearMeasurementPipe" in captured
    assert "PZEstimationPipe" in captured

def test_ancestors_broken(capsys):
    with pytest.raises(networkx.exception.NetworkXError):
        print_ancestors("tests/test.yml", "not-a-real-stage-or-output")



if __name__ == "__main__":
    test_run_dry_run()
    test_run_parsl()
    test_run_mini()
