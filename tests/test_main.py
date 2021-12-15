from ceci.main import run
from parsl import clear
import tempfile
import os
import pytest
import subprocess

from ceci.pipeline import Pipeline


def run1(*config_changes, dry_run=False, expect_fail=False, expect_outputs=True):
    try:
        with tempfile.TemporaryDirectory() as dirname:
            out_dir = os.path.join(dirname, "output")
            log_dir = os.path.join(dirname, "logs")
            config = [f"output_dir={out_dir}", f"log_dir={log_dir}"]
            config += config_changes
            pipe_config = Pipeline.build_config("tests/test.yml", config, dry_run)
            status = run(pipe_config, "tests/test.yml", config, dry_run)
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


def test_run_parsl():
    run1("launcher.name=parsl", "launcher.max_threads=3")


def test_run_cwl():
    run1("launcher.name=cwl", "launcher.dir=tests/cwl") == 0


def test_pre_script():
    # use the bash "true" command to simulate a
    # pre-script suceeding
    run1("pre_script='true'")
    # and false to simulate a failure
    with pytest.raises(subprocess.CalledProcessError):
        # error should happen before we get to the asserts, so no expect_fail etc
        run1("pre_script='false'")


def test_post_script():
    # use the bash "true" command to simulate a
    # pre-script suceeding
    run1("post_script='true'")
    # and false to simulate a failure - should not raise an error
    # but should fail.  Outputs should exist.
    run1("post_script='false'", expect_fail=True, expect_outputs=True)


if __name__ == "__main__":
    test_run_dry_run()
    test_run_parsl()
    test_run_mini()
    test_run_cwl()
    test_pre_script()
