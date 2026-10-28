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


def test_nersc_parse_int_set():
    from ceci.sites.nersc import parse_int_set

    assert parse_int_set("1,3-5,9") == {1, 3, 4, 5, 9}
    with pytest.raises(ValueError):
        parse_int_set("1,abc")


def test_nersc_mini_login_node(monkeypatch):
    from ceci.sites.nersc import NerscSite

    monkeypatch.delenv("SLURM_JOB_ID", raising=False)
    site = NerscSite({})
    site.configure_for_mini()
    (node,) = site.info["nodes"]
    assert node.cores == 4


def test_nersc_mini_slurm_job(monkeypatch):
    # Simulate being inside a SLURM job, so this can run anywhere (e.g. CI)
    from ceci.sites.nersc import NerscSite

    monkeypatch.setenv("SLURM_JOB_ID", "12345")
    monkeypatch.setenv("SLURM_JOB_NODELIST", "nid[1234-1235,1240]")
    monkeypatch.setenv("SLURM_CPUS_ON_NODE", "64")
    site = NerscSite({})
    site.configure_for_mini()
    nodes = site.info["nodes"]
    assert len(nodes) == 3
    assert all(node.cores == 64 for node in nodes)

    # single node, no brackets, and the default core count
    monkeypatch.setenv("SLURM_JOB_NODELIST", "nid001234")
    monkeypatch.delenv("SLURM_CPUS_ON_NODE")
    site = NerscSite({})
    site.configure_for_mini()
    (node,) = site.info["nodes"]
    assert node.id == "nid001234"
    assert node.cores == 32


def test_nersc_command(monkeypatch):
    from ceci.sites.nersc import NerscSite

    monkeypatch.setenv("SLURM_JOB_ID", "12345")
    site = NerscSite({"python_paths": ["/a", "/b"]})

    sec = StageExecutionConfig({"name": "Test", "nprocess": 4, "threads_per_process": 2})
    cmd = site.command("xxx", sec)
    assert "srun -u -n 4 --cpus-per-task=2" in cmd
    assert "OMP_NUM_THREADS=2" in cmd
    assert "PYTHONPATH=/a:/b:$PYTHONPATH" in cmd
    assert "shifter" not in cmd
    assert "--mpi" in cmd

    sec = StageExecutionConfig({"name": "Test", "nprocess": 1, "image": "img:latest"})
    cmd = site.command("xxx", sec)
    assert "shifter" in cmd
    assert "--image img:latest" in cmd
    assert "--mpi" not in cmd
