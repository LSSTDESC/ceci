from ceci.main import run
from parsl import clear

def test_run_mini():
    assert run('tests/test.yml') == 0

def test_run_dry_run():
    assert run('tests/test.yml', dry_run=True) == 0

def test_run_parsl():
    assert run('tests/test.yml', ['launcher.name=parsl', 'launcher.max_threads=3']) == 0
    clear()

def test_run_cwl():
    assert run('tests/test.yml', 
        ['launcher.name=cwl', 'launcher.dir=tests/cwl', 'launcher.launch=cwltool']) == 0


if __name__ == '__main__':
    test_run_parsl()
    test_run_mini()
