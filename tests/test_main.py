from ceci.main import run


def test_run_mini():
    run('tests/test.yml',)

def test_run_dry_run():
    run('tests/test.yml', dry_run=True)

def test_run_parsl():
    run('tests/test.yml', ['launcher.name=parsl', 'launcher.max_threads=3'])

def test_run_cwl():
    run('tests/test.yml', ['launcher.name=cwl', 'launcher.dir=tests/cwl', 'launcher.launch=cwltool'])
