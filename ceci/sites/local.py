"""Utility class to interface to workflow managers when using local resources, e.g., a laptop"""

from .site import Site
import socket
from ..minirunner import Node


class LocalSite(Site):
    """Object representing execution in the local environment, e.g. a laptop."""

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
        mpi2 = "--mpi" if sec.nprocess > 1 else ""
        volume_flag = f"-v {sec.volume} " if sec.volume else ""
        paths = self.config.get("python_paths", [])

        # TODO: allow other container types here, like singularity
        if sec.image:
            # If we are setting python paths then we have to modify the executable
            # here.  This is because we need the path to be available right from the
            # start, in case the stage is defined in a module added by these paths.
            # The --env flags in docker/shifter overwrites an env var, and there
            # doesn't seem to be a way to just append to one, so we have to be a bit
            # roundabout to make this work, and invoke bash -c instead.
            paths_start = (
                "bash -c 'PYTHONPATH=$PYTHONPATH:" + (":".join(paths)) if paths else ""
            )
            paths_end = "'" if paths else ""
            return (
                f"docker run "
                f"--env OMP_NUM_THREADS={sec.threads_per_process} "
                f"{volume_flag} "
                f"--rm -it {sec.image} "
                f"{paths_start} "
                f"{mpi1} "
                f"{cmd} {mpi2} "
                f"{paths_end}"
            )
        else:
            # In the non-container case this is much easier
            paths_env = (
                "PYTHONPATH=" + (":".join(paths)) + ":$PYTHONPATH" if paths else ""
            )
            return (
                f"OMP_NUM_THREADS={sec.threads_per_process} "
                f"{paths_env} "
                f"{mpi1} "
                f"{cmd} {mpi2}"
            )

    def configure_for_parsl(self):
        """Utility function to set parsl configuration parameters"""
        from parsl.executors import ThreadPoolExecutor

        max_threads = self.config.get("max_threads", 4)
        executor = ThreadPoolExecutor(label="local", max_threads=max_threads)
        # executors = [executor]

        self.info["executor"] = executor

    def configure_for_mini(self):
        """Utility function to setup self for local execution"""
        import psutil

        # The default is to allow a single process
        # with as many cores as possible, but allow the
        # user to specify both max_processes and max_threads
        # to customize
        procs = self.config.get("max_processes", 1)
        cores_available = psutil.cpu_count(logical=False)
        threads_default = max(cores_available // procs, 1)
        cores = self.config.get("max_threads", threads_default)
        name = socket.gethostname()

        # Create a node, which actually just represents one process
        # here due to SLURM limitations
        nodes = [Node(f"{name}_{i}", cores) for i in range(procs)]
        self.info["nodes"] = nodes

    def configure_for_cwl(self):
        """Utility function to set CWL configuration parameters"""
