import parsl
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor

def activate():
    executor = ThreadPoolExecutor(label='local', max_threads=4)
    executors = [executor]
    config = Config(executors=executors)
    parsl.load(config)

    labels = [exe.label for exe in executors]
    mpi_command = 'mpirun -n'

    return labels, mpi_command

