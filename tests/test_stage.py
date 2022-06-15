from ceci.stage import PipelineStage
from ceci.config import StageParameter
from ceci_example.types import HDFFile
import numpy as np
from ceci.errors import *
import pytest
import h5py
import os

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
    # This one should work
    class TestStage(PipelineStage):
        name = "test"
        inputs = [("inp1", HDFFile)]
        outputs = []
        config_options = {}

        def run(self):
            pass

    assert PipelineStage.get_stage("test") == TestStage

    # this fails if you execute this file directly as the qualified
    # name is different
    if __name__ != "__main__":
        assert TestStage.get_module().endswith("test_stage")

    s = TestStage({"config": "tests/config.yml", "inp1": "tests/test.hdf5"})

    assert s.rank == 0
    assert s.size == 1
    assert s.is_parallel() == False
    assert s.is_mpi() == False
    assert list(s.split_tasks_by_rank(["1", "2", "3"])) == ["1", "2", "3"]

    r = list(s.data_ranges_by_rank(1000, 100))
    assert r[0] == (0, 100)
    assert r[2] == (200, 300)

    r = list(s.iterate_hdf("inp1", "group1", ["x", "y", "z"], 10))
    for ri in r:
        s, e, ri = ri
        assert len(ri["x"] == 10)
    assert np.all(r[4][2]["z"] == [-80, -82, -84, -86, -88, -90, -92, -94, -96, -98])

    # Fake that we are processor 4/10
    comm = MockCommunicator(10, 4)
    s = TestStage({"config": "tests/config.yml", "inp1": "tests/test.hdf5"}, comm=comm)

    assert s.rank == 4
    assert s.size == 10
    assert s.is_parallel() == True
    assert s.is_mpi() == True
    assert list(s.split_tasks_by_rank("abcdefghijklmnopqrst")) == ["e", "o"]

    r = list(s.data_ranges_by_rank(10000, 100))
    assert r[0] == (400, 500)
    assert r[3] == (3400, 3500)

    r = list(s.iterate_hdf("inp1", "group1", ["x", "y", "z"], 10))
    assert len(r) == 1
    st, e, r = r[0]
    assert st == 40
    assert e == 50
    assert np.all(r["x"] == range(40, 50))

    r = list(s.iterate_hdf("inp1", "group1", ["x", "y", "z"], 10, parallel=False))
    for ri in r:
        st, e, ri = ri
        assert len(ri["x"]) == 10
    assert np.all(r[4][2]["z"] == [-80, -82, -84, -86, -88, -90, -92, -94, -96, -98])


    s = TestStage({"config": "tests/config.yml", "inp1": "tests/test.hdf5"})
    r = list(s.iterate_hdf("inp1", "group1", ["w", "x", "y", "z"], 10, longest=True))
    for i, ri in enumerate(r):
        st, e, ri = ri
        if i < 10:
            assert len(ri["x"]) == 10
            assert np.allclose(ri["x"], np.arange(10*i, 10*(i+1)))
            assert np.allclose(ri["w"], np.arange(10*i, 10*(i+1)))
        else:
            assert len(ri["x"]) == 0
            assert len(ri["w"]) == 10
            assert np.allclose(ri["w"], np.arange(10*i, 10*(i+1)))

    # check error message is raise appropriately if longest is not specified
    s = TestStage({"config": "tests/config.yml", "inp1": "tests/test.hdf5"})
    with pytest.raises(ValueError):
        r = list(s.iterate_hdf("inp1", "group1", ["w", "x", "y", "z"], 10, longest=False))


def test_make_stage():
    # This one should work
    class TestStage(PipelineStage):
        name = "test_copy"
        inputs = [("inp1", HDFFile)]
        outputs = [("out", HDFFile)]
        config_options = {"a":"b"}

        def run(self):
            pass

    stage = TestStage.make_stage(name="copy_of_test_stage", a='c', inp1='dummy', out="my_file.hdf5")
    assert stage.find_outputs('.')['out_copy_of_test_stage'] =='./my_file.hdf5'
    assert stage.config.a == 'c'


def test_parameter():
    # This one should work
    class TestStage(PipelineStage):
        name = "test_stage_param"
        inputs = [("inp1", HDFFile)]
        outputs = []
        config_options = dict(a=StageParameter(float, 5., msg="a float"))

        def run(self):
            pass

    stage_1 = TestStage.make_stage(a=6., inp1='dummy')
    assert stage_1.config.a == 6.

    # This one should not work
    class TestStage_2(PipelineStage):
        name = "test_stage_param_2"
        inputs = [("inp1", HDFFile)]
        outputs = []
        config_options = dict(a=StageParameter(float, None, msg="a float"))

        def run(self):
            pass

    with pytest.raises(ValueError):
        stage_2 = TestStage_2.make_stage(inp1='dummy')

    # This one should work
    class TestStage_3(PipelineStage):
        name = "test_stage_param_3"
        inputs = [("inp1", HDFFile)]
        outputs = []
        config_options = dict(a=StageParameter(float, None, required=False, msg="a float"))

        def run(self):
            pass

    stage_3 = TestStage_3.make_stage(inp1='dummy')
    assert stage_3.config.a is None

    par = StageParameter(int, 0, msg="a float")
    assert par.value == 0
    par.set_default(1)
    assert par.value == 1
    assert par.default == 1

    # This one should work
    class TestStage_4(PipelineStage):
        name = "test_stage_param_4"
        inputs = [("inp1", HDFFile)]
        outputs = []
        config_options = dict(a=stage_1.config)

        def run(self):
            pass

    stage_4 = TestStage_4.make_stage(inp1='dummy')
    assert stage_4.config.a == 5.


    stage_1.config.get('a').set_default(7.)
    # This one should work
    class TestStage_5(PipelineStage):
        name = "test_stage_param_5"
        inputs = [("inp1", HDFFile)]
        outputs = []
        config_options = dict(a=stage_1.config)

        def run(self):
            pass

    stage_5 = TestStage_4.make_stage(inp1='dummy')
    assert stage_5.config.a == 7.



    

def test_incomplete():
    class Alpha(PipelineStage):
        pass

    assert "Alpha" in PipelineStage.incomplete_pipeline_stages

    with pytest.raises(IncompleteStage):
        PipelineStage.get_stage("Alpha")


def test_auto_name():
    class Bravo(PipelineStage):
        inputs = []
        outputs = []
        config_options = {}

        def run(self):
            pass

    assert Bravo.name == "Bravo"
    assert PipelineStage.get_stage("Bravo") is Bravo


def test_duplicate():
    class Charlie(PipelineStage):
        inputs = []
        outputs = []

        def run(self):
            pass

    assert PipelineStage.get_stage("Charlie") is Charlie

    with pytest.raises(DuplicateStageName):

        class Charlie(PipelineStage):
            inputs = []
            outputs = []

            def run(self):
                pass

    with pytest.raises(DuplicateStageName):
        # Name it specified and duplicated
        class AlsoCharlie(PipelineStage):
            name = "Charlie"
            inputs = []
            outputs = []

            def run(self):
                pass

    # Should work okay as name is overwritten
    class Charlie(PipelineStage):
        name = "NotCharlie"
        inputs = []
        outputs = []

        def run(self):
            pass

    assert Charlie.name == "NotCharlie"
    assert PipelineStage.get_stage("NotCharlie") is Charlie


def test_explicit_config():
    with pytest.raises(ReservedNameError):

        class Delta(PipelineStage):
            inputs = [("config", None)]
            outputs = []
            config_options = {}

            def run(self):
                pass


def test_okay_abc_dupe_name():
    class Echo(PipelineStage):
        pass

    # okay, as the parent is intended as abstract
    class Echo(Echo):
        inputs = []
        outputs = []

        def run(self):
            pass

    assert Echo.name == "Echo"
    assert PipelineStage.get_stage("Echo") is Echo


def test_okay_abc_dupe_name2():
    class FoxtrotBase(PipelineStage):
        name = "Foxtrot"
        pass

    # okay, as the parent is intended as abstract
    class Foxtrot(FoxtrotBase):
        inputs = []
        outputs = []

        def run(self):
            pass

    assert Foxtrot.name == "Foxtrot"
    assert PipelineStage.get_stage("Foxtrot") is Foxtrot


def test_config_specified():
    # check for incomplete classes
    with pytest.raises(ReservedNameError):

        class Golf(PipelineStage):
            config = "golf"

    # check for complete classes
    with pytest.raises(ReservedNameError):

        class Golf(PipelineStage):
            config = "golf"
            inputs = []
            outputs = []

            def run(self):
                pass

def test_bool_flags():
    class Hotel(PipelineStage):
        inputs = []
        outputs = []
        config_options = {'xyz': bool}

    cmd = ["Hotel", "--config", "tests/config.yml"]

    # Basic case with a single flag
    h = Hotel(Hotel.parse_command_line(cmd + ["--xyz"]))
    assert h.config['xyz'] is True

    h = Hotel(Hotel.parse_command_line(cmd + ["--no-xyz"]))
    assert h.config['xyz'] is False

    # check latter takes precedence if both specified
    h = Hotel(Hotel.parse_command_line(cmd + ["--xyz", "--no-xyz"]))
    assert h.config['xyz'] is False

    h = Hotel(Hotel.parse_command_line(cmd + ["--no-xyz", "--xyz"]))
    assert h.config['xyz'] is True

    # flag is not optional here so must be set
    with pytest.raises(ValueError):
        h = Hotel(Hotel.parse_command_line(cmd))

    class Hotel2(PipelineStage):
        inputs = []
        outputs = []
        config_options = {'xyz': False}

    h = Hotel2(Hotel2.parse_command_line(cmd + ["--xyz"]))
    assert h.config['xyz'] is True

    h = Hotel2(Hotel2.parse_command_line(cmd + ["--no-xyz"]))
    assert h.config['xyz'] is False

    h = Hotel2(Hotel2.parse_command_line(cmd + ["--xyz", "--no-xyz"]))
    assert h.config['xyz'] is False

    h = Hotel2(Hotel2.parse_command_line(cmd + ["--no-xyz", "--xyz"]))
    assert h.config['xyz'] is True

    h = Hotel2(Hotel2.parse_command_line(cmd))
    assert h.config['xyz'] is False

    class Hotel3(PipelineStage):
        inputs = []
        outputs = []
        config_options = {'xyz': True}

    h = Hotel3(Hotel3.parse_command_line(cmd + ["--xyz"]))
    assert h.config['xyz'] is True

    h = Hotel3(Hotel3.parse_command_line(cmd + ["--no-xyz"]))
    assert h.config['xyz'] is False

    h = Hotel3(Hotel3.parse_command_line(cmd + ["--xyz", "--no-xyz"]))
    assert h.config['xyz'] is False

    h = Hotel3(Hotel3.parse_command_line(cmd + ["--no-xyz", "--xyz"]))
    assert h.config['xyz'] is True

    h = Hotel3(Hotel3.parse_command_line(cmd))
    assert h.config['xyz'] is True


def test_open_input():
    class India(PipelineStage):
        inputs = [("my_input", HDFFile)]
        outputs = []
        config_options = {}
    cmd = "India", "--config", "tests/config.yml", "--my_input", "tests/test.hdf5"

    # Testing without an alias
    ii = India(India.parse_command_line(cmd))
    assert os.path.exists(ii.get_input("my_input"))
    f = ii.open_input("my_input")
    print(f.keys())
    f.close()

    # Testing with an alias - config.yml defines an alias for my_input, my_alias
    ii = India.make_stage(name="IndiaCopy", my_input="tests/test.hdf5", config="tests/config.yml")

    print(ii.get_aliases())

    # This currently works
    assert os.path.exists(ii.get_input("my_alias"))

    # This works now
    f = ii.open_input("my_input")
    print(f.keys())
    f.close()


def test_open_output():
    class Juliett(PipelineStage):
        inputs = []
        outputs = [("my_output", HDFFile)]
        config_options = {}
    cmd = "Juliett", "--config", "tests/config.yml", "--my_output", "tests/test_out.hdf5"

    # Testing without an alias
    jj = Juliett(Juliett.parse_command_line(cmd))
    #assert os.path.exists(jj.get_output("my_output"))
    f = jj.open_output("my_output")
    print(f.keys())
    f.close()

    # Testing with an alias - config.yml defines an alias for my_input, my_alias
    jj = Juliett.make_stage(name="JuliettCopy", aliases=dict(my_output='my_alias'))

    print(jj.get_aliases())

    # This works now
    f = jj.open_output("my_output")
    print(f.keys())
    f.close()





def test_unknown_stage():
    with pytest.raises(StageNotFound):
        PipelineStage.get_stage("ThisStageIsDeliberatelyLeftBlank")


# could add more tests here for constructor, but the regression tests here and in TXPipe are
# pretty thorough.

if __name__ == "__main__":
    test_construct()
