"""Utility class to interface to workflow managers when using CC IN2P3"""

from .site import Site
import os
from ..minirunner import Node


class CCParallel(Site):
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

        mpi1 = f"{self.mpi_command} {sec.nprocess} "
        mpi2 = "--mpi" if sec.nprocess > 1 else ""
        volume_flag = f"--bind {sec.volume} " if sec.volume else ""
        paths = self.config.get("python_paths", [])

        # TODO: allow other container types here, like singularity
        if sec.image:
            # If we are setting python paths then we have to modify the executable
            # here.  This is because we need the path to be available right from the
            # start, in case the stage is defined in a module added by these paths.
            # The --env flags in docker/shifter overwrites an env var, and there
            # doesn't seem to be a way to just append to one, so we have to be a bit
            # roundabout to make this work, and invoke bash -c instead.
            bash_start = "bash -c ' cd /opt/TXPipe && "
            bash_end = "'"

            paths_start = "PYTHONPATH=$PYTHONPATH:" + (":".join(paths)) if paths else ""
            return (
                f"{mpi1} "
                f"singularity run "
                f"--env OMP_NUM_THREADS={sec.threads_per_process} "
                f"{volume_flag} "
                f"{sec.image} "
                f"{bash_start} "
                f"{paths_start} "
                f"{cmd} {mpi2} "
                f"{bash_end}"
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

    def configure_for_parsl(self):  # pylint: disable=no-self-use
        """Utility function to set parsl configuration parameters"""
        raise ValueError("Parsl not configured for CC IN2P3 in ceci yet")

    def configure_for_mini(self):
        """Utility function to setup self for local execution"""
        total_cores = int(os.environ["NSLOTS"])
        cores_per_node = 16  # seems to be the case
        nodes = total_cores // cores_per_node
        last_node_codes = total_cores % cores_per_node

        nodes = [Node(f"Node_{i}", cores_per_node) for i in range(nodes)]

        if last_node_codes:
            i = len(nodes)
            nodes.append(Node(f"Node_{i}", last_node_codes))

        self.info["nodes"] = nodes

    def configure_for_cwl(self):
        """Utility function to set CWL configuration parameters"""
