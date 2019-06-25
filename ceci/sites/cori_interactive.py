import parsl
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor

def activate():
    executor = ThreadPoolExecutor(label='cori-interactive', max_threads=1)
    executors = [executor]
    config = Config(executors=exectutors)
    parsl.load(config)

    labels = [exe.label for exe in executors]
    mpi_command = 'srun -n'

    return labels, mpi_command


