import os
from ..minirunner import Node

from .site import Site

class CoriSite(Site):
    default_mpi_command = 'srun -u -n'

    def command(self, cmd, sec):
        """Generate a complete command line to be run with the specified execution variables.

        This builds up the command from the core and adds any shiftr commands, env var settings,
        or mpirun calls.

        Parameters
        ----------
        cmd: str
            The core command to execute.

        sec: StageExecutionConfig
            sec objects contain info on numbers of processes, threads, etc, and container choices

        Returns
        -------
        full_cmd: str
            The complete decorated command to be executed.
        """

        # on cori we always use srun, even if the command is a single process
        mpi1 = f"{self.mpi_command} {sec.nprocess} --cpus-per-task={sec.threads_per_process}"
        mpi2 = f"--mpi" if sec.nprocess > 1 else ""
        volume_flag = f'-v {sec.volume} ' if sec.volume else ''

        if sec.nodes:
            mpi1 += f" --nodes {sec.nodes}"

        if (sec.nprocess > 1) and (os.environ.get('SLURM_JOB_ID') is None):
            raise ValueError("You cannot use MPI (by setting nprocess > 1) "
                             "on Cori login nodes, only inside jobs.")

        if sec.image:
            return f'{mpi1} ' \
                   f'shifter '\
                   f'--env OMP_NUM_THREADS={sec.threads_per_process} '\
                   f'{volume_flag} '\
                   f'--image {sec.image} '\
                   f'{cmd} {mpi2} '
        else:
            return f'OMP_NUM_THREADS={sec.threads_per_process} '\
                   f'{mpi1} ' \
                   f'{cmd} {mpi2}'



    def configure_for_mini(self):
        # if on local machine, query available cores and mem, make one node
        slurm = os.environ.get('SLURM_JOB_ID')

        if slurm:
            # running a job, either interactive or batch
            # check the environment to find out what nodes we are using
            node_list = os.environ['SLURM_JOB_NODELIST']
            # parse node list
            if '[' in node_list:
                body, vals = node_list.split('[', 1)
                ints = parse_int_set(vals.strip(']'))
                node_names = [f'{body}{i}' for i in ints]
            else:
                node_names = [node_list]

            # cori default
            cpus_per_node = 32
            
            # collect list.
            nodes = [Node(name, cpus_per_node) for name in node_names]
        else:
            # running on login node
            # use at most 4 procs to avoid annoying people
            nodes = [Node('cori', 4)]

        self.info['nodes'] = nodes

    def configure_for_cwl(self):
        pass



class CoriBatchSite(CoriSite):
    def configure_for_parsl(self):
        from parsl.executors import IPyParallelExecutor
        from parsl.providers import SlurmProvider

        # Get the site details that we need    
        cpu_type = self.config.get('cpu_type', 'haswell')
        queue = self.config.get('queue', 'debug')
        max_slurm_jobs = self.config.get('max_jobs', 2)
        account = self.config.get('account')
        if account is None:
            print("Using LSST DESC account. Specify 'account' in the site config to override")
            account = 'm1727'
        walltime = self.config.get('walltime', '00:30:00')
        setup_script = self.config.get('setup', '/global/projecta/projectdirs/lsst/groups/WL/users/zuntz/setup-cori')


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
            label='cori-batch',
            provider=provider,
        )

        self.info['executor'] = executor


class CoriInteractiveSite(CoriSite):
    def configure_for_parsl(self):
        from parsl.executors import ThreadPoolExecutor
        max_threads = int(os.environ.get('SLURM_JOB_NUM_NODES', 1))
        executor = ThreadPoolExecutor(label='local', max_threads=max_threads)
        self.info['executor'] = executor


def parse_int_set(nputstr):
    # https://stackoverflow.com/questions/712460/interpreting-number-ranges-in-python/712483
    selection = set()
    invalid = set()
    # tokens are comma seperated values
    tokens = [x.strip() for x in nputstr.split(',')]
    for i in tokens:
        try:
            # typically tokens are plain old integers
            selection.add(int(i))
        except:
            # if not, then it might be a range
            try:
                token = [int(k.strip()) for k in i.split('-')]
                if len(token) > 1:
                    token.sort()
                    # we have items seperated by a dash
                    # try to build a valid range
                    first = token[0]
                    last = token[len(token)-1]
                    for x in range(first, last+1):
                        selection.add(x)
            except:
               # not an int and not a range...
               invalid.add(i)
    # Report invalid tokens before returning valid selection
    if invalid:
        raise ValueError(f"Invalid node list: {nputstr}")
    return selection
