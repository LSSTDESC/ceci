from .main import run
import subprocess
import pytest


def test_main():
    config_filename = 'test/test.yml'
    assert run(config_filename, dry_run=True) == 0
    assert run(config_filename, dry_run=False) == 0

    # Test sthe pre_script feature using the command "true", which always
    # just returns zero
    assert run(config_filename, dry_run=False, extra_config=["pre_script=true"]) == 0

    # Check that a failing pre_script raises an error
    with pytest.raises(subprocess.CalledProcessError):
        run(config_filename, dry_run=False, extra_config=["pre_script=false"])

    # A failing post-script should just return the error status of the post-script
    assert run(config_filename, dry_run=False, extra_config=["post_script=false"]) == 1
