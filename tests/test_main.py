from ceci.main import run
from parsl import clear
import tempfile
import os

def run1(*config_changes, dry_run=False):
    try:
        with tempfile.TemporaryDirectory() as dirname:
            out_dir = os.path.join(dirname, 'output')
            log_dir = os.path.join(dirname, 'logs')
            config = [f'output_dir={out_dir}', f'log_dir={log_dir}']
            config += config_changes
            assert run('tests/test.yml', config, dry_run) == 0
            if not dry_run:
                assert os.path.exists(os.path.join(out_dir, 'wlgc_summary_data.txt'))
                assert os.path.exists(os.path.join(log_dir, 'WLGCSummaryStatistic.out'))

    finally:
        clear()

def test_run_mini():
    run1()

def test_run_dry_run():
    run1(dry_run=True)

def test_run_parsl():
    run1('launcher.name=parsl', 'launcher.max_threads=3')

def test_run_cwl():
    run1('launcher.name=cwl', 'launcher.dir=tests/cwl') == 0


if __name__ == '__main__':
    test_run_dry_run()
    test_run_parsl()
    test_run_mini()
    test_run_cwl()
