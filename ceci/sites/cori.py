import parsl
from parsl.config import Config
from parsl.executors import IPyParallelExecutor
from parsl.providers import SlurmProvider

def activate(site_config):

    # Get the site details that we need    
    cpu_type = site_config.get('cpu_type', 'haswell')
    queue = site_config.get('queue', 'debug')
    max_slurm_jobs = site_config.get('max_jobs', 2)
    account = site_config.get('account', 'm1727')
    walltime = site_config.get('walltime', '00:30:00')
    setup_script = site_config.get('setup', '/global/projecta/projectdirs/lsst/groups/WL/users/zuntz/setup-cori')


    provider=SlurmProvider(
         partition=queue,  # Replace with partition name
         min_blocks=0, # one slurm job to start with
         max_blocks=max_slurm_jobs, # one slurm job to start with
         scheduler_options=f"#SBATCH --constraint={cpu_type}\n" \
            f"#SBATCH --account={account}\n" \
            f"#SBATCH --walltime={walltime}\n",
         nodes_per_block=1,
         init_blocks=1,
         worker_init=f'source {setup_script}',
    )

    executor = IPyParallelExecutor(
        label='cori',
        provider=provider,
    )

    executors = [executor]
    config = Config(executors=executors)
    parsl.load(config)


    labels = [exe.label for exe in executors]
    mpi_command = 'srun -n'

    return labels, mpi_command
