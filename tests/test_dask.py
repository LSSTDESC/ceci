from ceci.stage import PipelineStage
import mockmpi

def core_dask(comm):
    class DaskTestStage(PipelineStage):
        name = "dasktest"
        dask_parallel = True
        inputs = []
        outputs = []
        config_options = {}

        def run(self):
            import dask.array as da
            arr = da.arange(100)
            x = arr.sum()
            x = x.compute()
            assert x == 4950


    args = DaskTestStage.parse_command_line(["dasktest", "--config", "tests/config.yml"])
    DaskTestStage.execute(args, comm=comm)

    # check that all procs get here
    if comm is not None:
        comm.Barrier()


def test_dask():
    core_dask(None)
    mockmpi.mock_mpiexec(3, core_dask)
    mockmpi.mock_mpiexec(5, core_dask)


if __name__ == '__main__':
    test_dask()
