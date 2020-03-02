from . stage import PipelineStage
import pytest

def test_construct():
    with pytest.raises(ValueError):
        class TestStage(PipelineStage):
            pass

    # This one should work
    class TestStage(PipelineStage):
        name = "test"
        inputs = []
        outputs = []
        config = {}

    assert PipelineStage.get_stage('test') == TestStage
    assert TestStage.get_module() == 'ceci.test_stage'

    s = TestStage({'config': 'test/config.yml'})

    assert s.rank == 0
    assert s.size == 1
    assert s.is_parallel() == False
    assert s.is_mpi() == False
    assert s.split_tasks_by_rank(['1', '2', '3']) == ['1', '2', '3']


    # I'd rather not attempt to unit test MPI stuff - that sounds very unreliable

# could add more tests here for constructor, but the regression tests here and in TXPipe are
# pretty thorough.