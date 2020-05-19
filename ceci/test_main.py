from .main import run


def test_run_mini():
    run('test/test.yml',)

def test_run_dry_run():
    run('test/test.yml', dry_run=True)

def test_run_parsl():
    run('test/test.yml', ['launcher.name=parsl', 'launcher.max_threads=3'])

def test_run_cwl():
    run('test/test.yml', ['launcher.name=cwl', 'launcher.dir=test/cwl', 'launcher.launch=cwltool'])
