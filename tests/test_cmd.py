from ceci.pipeline import StageExecutionConfig
from ceci.sites.local import LocalSite
from ceci.sites.cori import CoriBatchSite
import os
import pytest


class MockSite:
    def __init__(self):
        self.config = {"image": "abc", "volume": "def"}


def test_defaults():
    sec = StageExecutionConfig({"name": "a", "site": MockSite()})
    assert sec.nodes == 1
    assert sec.nprocess == 1
    assert sec.threads_per_process == 1
    assert sec.mem_per_process == 2
    assert sec.image == "abc"
    assert sec.volume == "def"


def test_local():
    site = LocalSite({})
    sec = StageExecutionConfig({"name": "a", "site": site})
    cmd1 = "echo 1"

    # should not start with docker/shifter, since no image specified
    cmd = site.command(cmd1, sec)

    # don't want to test too specifically here, since it may change
    assert "docker" not in cmd
    assert "shifter" not in cmd
    assert "OMP_NUM_THREADS=1" in cmd
    assert cmd1 in cmd


def test_docker():
    site = LocalSite({})
    sec = StageExecutionConfig(
        {
            "name": "a",
            "site": site,
            "image": "username/potato",
            "volume": "a:b",
            "threads_per_process": 4,
            "nprocess": 2,
        }
    )
    cmd1 = "echo 1"

    # should not start with docker/shifter, since no image specified
    cmd = site.command(cmd1, sec)

    # don't want to test too specifically here, since it may change
    assert "docker run" in cmd
    assert "username/potato" in cmd
    assert "-v a:b" in cmd
    assert "mpirun -n 2" in cmd
    assert "shifter" not in cmd
    assert "OMP_NUM_THREADS=4" in cmd
    assert cmd1 in cmd


def _test_cori(job_id):
    site = CoriBatchSite({})
    # fake that we're runnng a job to avoid complaints
    initial = os.environ.get("SLURM_JOB_ID")
    if job_id:
        os.environ["SLURM_JOB_ID"] = "fake_job_id"
    elif initial is not None:
        del os.environ["SLURM_JOB_ID"]

    try:

        sec = StageExecutionConfig(
            {
                "name": "a",
                "site": site,
                "image": "username/potato",
                "volume": "a:b",
                "threads_per_process": 4,
                "nprocess": 2,
                "nodes": 3,
            }
        )
        cmd1 = "echo 1"

        # should not start with docker/shifter, since no image specified
        cmd = site.command(cmd1, sec)

        # don't want to test too specifically here, since it may change
        assert "shifter" in cmd
        assert "--image username/potato" in cmd
        assert "-V a:b" in cmd
        assert "srun -u -n 2" in cmd
        assert "--env OMP_NUM_THREADS=4" in cmd
        assert "--nodes 3" in cmd
        assert "--mpi" in cmd
        assert cmd1 in cmd
    finally:
        if job_id:
            if initial is None:
                del os.environ["SLURM_JOB_ID"]
            else:
                os.environ["SLURM_JOB_ID"] = initial
        elif initial is not None:
            os.environ["SLURM_JOB_ID"] = initial


def test_works():
    _test_cori(True)


def test_warning():
    with pytest.raises(ValueError):
        _test_cori(False)
