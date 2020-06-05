from ceci.stage import PipelineStage
import pytest

# TODO: test MPI facilities properly with:
# https://github.com/rmjarvis/TreeCorr/blob/releases/4.1/tests/mock_mpi.py


class MockCommunicator:
    def __init__(self, size, rank):
        self.size = size
        self.rank = rank

    def Get_size(self):
        return self.size

    def Get_rank(self):
        return self.rank


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

    assert PipelineStage.get_stage("test") == TestStage
    assert TestStage.get_module() == "test_stage"

    s = TestStage({"config": "tests/config.yml"})

    assert s.rank == 0
    assert s.size == 1
    assert s.is_parallel() == False
    assert s.is_mpi() == False
    assert list(s.split_tasks_by_rank(["1", "2", "3"])) == ["1", "2", "3"]

    r = list(s.data_ranges_by_rank(1000, 100))
    assert r[0] == (0, 100)
    assert r[2] == (200, 300)

    # Fake that we are processor 4/10
    comm = MockCommunicator(10, 4)
    s = TestStage({"config": "tests/config.yml"}, comm=comm)

    assert s.rank == 4
    assert s.size == 10
    assert s.is_parallel() == True
    assert s.is_mpi() == True
    assert list(s.split_tasks_by_rank("abcdefghijklmnopqrst")) == ["e", "o"]

    r = list(s.data_ranges_by_rank(10000, 100))
    assert r[0] == (400, 500)
    assert r[3] == (3400, 3500)

    # I'd rather not attempt to unit test MPI stuff - that sounds very unreliable


# could add more tests here for constructor, but the regression tests here and in TXPipe are
# pretty thorough.
