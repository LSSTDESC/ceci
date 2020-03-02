from .pipeline import StageExecutionConfig
from .sites.local import LocalSite
from .sites.cori import CoriBatchSite

class MockSite:
    def __init__(self):
        self.config = {'image':'abc', "volume": 'def'}

def test_defaults():
    sec = StageExecutionConfig({'name':'a', 'site': MockSite()})
    assert sec.nodes == 1
    assert sec.nprocess == 1
    assert sec.threads_per_process == 1
    assert sec.mem_per_process == 2
    assert sec.image == 'abc'
    assert sec.volume == 'def'



def test_local():
    site = LocalSite({})
    sec = StageExecutionConfig({'name':'a', 'site': site})
    cmd1 = "echo 1"

    # should not start with docker/shifter, since no image specified
    cmd = site.command(cmd1, sec)

    # don't want to test too specifically here, since it may change
    assert 'docker' not in cmd
    assert 'shifter' not in cmd
    assert 'OMP_NUM_THREADS=1' in cmd
    assert cmd1 in cmd




def test_docker():
    site = LocalSite({})
    sec = StageExecutionConfig({
        'name':'a',
        'site': site,
        'image':'username/potato', 
        'volume': 'a:b',
        'threads_per_process': 4,
        'nprocess': 2
    })
    cmd1 = "echo 1"

    # should not start with docker/shifter, since no image specified
    cmd = site.command(cmd1, sec)

    # don't want to test too specifically here, since it may change
    assert 'docker run'in cmd
    assert 'username/potato' in cmd
    assert '-v a:b' in cmd
    assert 'mpirun -n 2' in cmd
    assert 'shifter' not in cmd
    assert 'OMP_NUM_THREADS=4' in cmd
    assert cmd1 in cmd




def test_cori():
    site = CoriBatchSite({})
    sec = StageExecutionConfig({
        'name':'a',
        'site': site,
        'image':'username/potato', 
        'volume': 'a:b',
        'threads_per_process': 4,
        'nprocess': 2,
        'nodes': 3
    })
    cmd1 = "echo 1"

    # should not start with docker/shifter, since no image specified
    cmd = site.command(cmd1, sec)

    # don't want to test too specifically here, since it may change
    assert 'shifter' in cmd
    assert '--image username/potato' in cmd
    assert '-v a:b' in cmd
    assert 'srun -u -n 2' in cmd
    assert '--env OMP_NUM_THREADS=4' in cmd
    assert '--nodes 3' in cmd
    assert '--mpi' in cmd
    assert cmd1 in cmd

