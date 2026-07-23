import pytest
import subprocess
import os
import shutil

def test_executable():
    subprocess.check_call(["ceci", "tests/test.yml"])

def test_dry_run():
    subprocess.check_call(["ceci", "--dry-run", "tests/test.yml",])

def test_flow_chart_run():
    subprocess.check_call(["ceci", "--flow-chart", "test-flow.png", "tests/test.yml",])
    assert os.path.exists("test-flow.png")

def test_profiling_and_memmon_flags():
    cmd = "python3 -m ceci_example PZEstimationPipe   --DM=./tests/inputs/dm.txt   --fiducial_cosmology=./tests/inputs/fiducial_cosmology.txt   --config=./tests/config.yml   --photoz_pdfs=./tests/outputs/photoz_pdfs.txt --memmon=1 --cprofile=profile.stats"
    subprocess.check_call(cmd.split())

def test_misuse_mpi():
    cmd = "python3 -m ceci_example PZEstimationPipe --mpi"
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(cmd.split())

def test_run_template_parameters():
    if os.path.exists("tests/template_test_logfile.txt"):
        os.remove("tests/template_test_logfile.txt")
    shutil.rmtree("tests/outputs_north", ignore_errors=True)

    cmd = 'ceci tests/template.yml --template-parameters logfile=template_test_logfile.txt some_directory=inputs field=north'
    subprocess.check_call(cmd.split())
    assert os.path.exists("tests/template_test_logfile.txt")
    assert os.path.isdir("tests/outputs_north")