import parsl
from parsl.config import Config
from parsl.executors import IPyParallelExecutor

def activate(site_config):
    max_threads = site_config.get('max_threads', 1)

    executor = IPyParallelExecutor(label='cori-interactive', max_threads=max_threads)
    executors = [executor]
    config = Config(executors=executors)
    parsl.load(config)

    labels = [exe.label for exe in executors]
    mpi_command = 'srun -n'

    return labels, mpi_command


