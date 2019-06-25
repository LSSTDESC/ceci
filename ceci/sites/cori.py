import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import SlurmProvider

def activate(queue, max_slurm_jobs, setup_script, cpu_type):
    provider=SlurmProvider(
         partition=queue,  # Replace with partition name
         min_blocks=0, # one slurm job to start with
         max_blocks=max_blocks, # one slurm job to start with
         scheduler_options=f"#SBATCH --constraint={cpu_type}",
         worker_init=f'source {setup_script}',
    )
    #node_per_block = total_nodes_used
    #max_blocks = 1
    #init_blocks = 0
    #min_blocks = 0
    #launcher = SingleNodeLauncher()
    #max_workers = total_mpi_applications

    executor = HighThroughputExecutor(
             label="cori",
             worker_debug=False,
             provider=provider,
    )


    config = Config(executors=[executor])
    parsl.load(config)


    labels = [exe.label for exe in executors]
    mpi_command = 'srun -n'

    return labels, mpi_command
