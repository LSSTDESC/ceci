from .site import Site
import os
import socket
from ..minirunner import Node


class LocalSite(Site):
    """Object representing execution in the local environment, e.g. a laptop.
    """

    def command(self, cmd, sec):
        """Generate a complete command line to be run with the specified execution variables.

        This builds up the command from the core and adds any docker commands, env var settings,
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

        mpi1 = f"{self.mpi_command} {sec.nprocess}" if sec.nprocess > 1 else ""
        mpi2 = f"--mpi" if sec.nprocess > 1 else ""
        volume_flag = f'-v {sec.volume} ' if sec.volume else ''

        # TODO: allow other container types here, like singularity
        if sec.image:
            return f'docker run '\
                   f'--env OMP_NUM_THREADS={sec.threads_per_process} '\
                   f'{volume_flag} '\
                   f'--rm -it {sec.image} '\
                   f'{mpi1} ' \
                   f'{cmd} {mpi2} '
        else:
            return f'OMP_NUM_THREADS={sec.threads_per_process} '\
                   f'{mpi1} ' \
                   f'{cmd} {mpi2}'

    def configure_for_parsl(self):
        from parsl.executors import ThreadPoolExecutor
        max_threads = self.config.get('max_threads', 4)
        executor = ThreadPoolExecutor(label='local', max_threads=max_threads)
        executors = [executor]

        self.info['executor'] = executor


    def configure_for_mini(self):
        import psutil
        cores = psutil.cpu_count(logical=False)
        cores = min(cores, self.config.get('max_threads', 100))
        name = socket.gethostname()
        nodes = [Node(name, cores)]

        self.info['nodes'] = nodes

    def configure_for_cwl(self):
        pass
