import ceci.file_types as file_types
import tempfile
import os
import mockmpi


def _test_directory_parallel(comm, tmpdir):
        print(comm.rank, tmpdir)
        d = file_types.Directory(tmpdir + "/test_dir", "w", parallel=True, comm=comm)
        comm.Barrier()
        assert d.path == tmpdir + "/test_dir"
        assert os.path.exists(tmpdir + "/test_dir/provenance.yml")


def test_directory():
    with tempfile.TemporaryDirectory() as tmpdir:
        d = file_types.Directory(tmpdir + "/test_dir", "w", parallel=False)
        assert d.path == tmpdir + "/test_dir"
        assert os.path.exists(tmpdir + "/test_dir/provenance.yml")


def test_directory_parallel():
    with tempfile.TemporaryDirectory() as tmpdir:
        mockmpi.mock_mpiexec(2, _test_directory_parallel, tmpdir)
