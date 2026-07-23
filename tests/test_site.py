from ceci.sites import load, get_default_site
from ceci.pipeline import StageExecutionConfig
import pytest
import os

def test_nersc_error():
    # check that errors when trying to run multi-process
    # jobs on nersc login nodes are handled correctly.
    # should fail unless dry-run is set.

    launcher_config = {
        "name": "mini",
        "interval": 1.0,
    }
    site_config = {
        "name": "nersc-interactive",
    }

    stage_config = {
        "name": "Test",
        "nprocess": 2,
    }

    load(launcher_config, [site_config])
    site = get_default_site()
    sec = StageExecutionConfig(stage_config)

    # should fail if we don't set dry-run
    with pytest.raises(ValueError):
        site.command("xxx", sec)

    # should work if we do set dry-run
    launcher_config["dry_run"] = True
    load(launcher_config, [site_config])
    site = get_default_site()
    site.command("xxx", sec)


def test_in2p3():
    os.environ["NSLOTS"] = "8"

    launcher_config = {
        "name": "mini",
        "interval": 1.0,
    }
    site_config = {
        "name": "cc-parallel",
    }

    stage_config = {
        "name": "Test",
        "nprocess": 2,
    }

    load(launcher_config, [site_config])
    site = get_default_site()
    sec = StageExecutionConfig(stage_config)

    # should work if we do set dry-run
    launcher_config["dry_run"] = True
    load(launcher_config, [site_config])
    site = get_default_site()
    cmd = site.command("xxx", sec)
    assert "singularity" not in cmd

    stage_config["image"] = "imaginary_file.sif"
    sec = StageExecutionConfig(stage_config)


    load(launcher_config, [site_config])
    site = get_default_site()
    cmd = site.command("xxx", sec)
    assert "singularity run" in cmd
