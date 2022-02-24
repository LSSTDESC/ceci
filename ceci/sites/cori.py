"""Utility class to interface to workflow managers when using CORI at NERSC"""

import os
from ..minirunner import Node

from .site import Site


class CoriSite(Site):
    """Object representing execution on the CORI"""

    default_mpi_command = "srun -u -n"

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
        mpi2 = "--mpi" if sec.nprocess > 1 else ""
        volume_flag = f"-V {sec.volume} " if sec.volume else ""
        paths = self.config.get("python_paths", [])

        if sec.nodes:
            mpi1 += f" --nodes {sec.nodes}"

        if (
            (sec.nprocess > 1)
            and (os.environ.get("SLURM_JOB_ID") is None)
            and (not self.config.get("dry_run"))
        ):
            raise ValueError(
                "You cannot use MPI (by setting nprocess > 1) "
                "on Cori login nodes, only inside jobs."
            )

        if sec.image:
            # If we are setting python paths then we have to modify the executable
            # here.  This is because we need the path to be available right from the
            # start, in case the stage is defined in a module added by these paths.
            # The --env flags in docker/shifter overwrites an env var, and there
            # doesn't seem to be a way to just append to one, so we have to be a bit
            # roundabout to make this work, and invoke bash -c instead.
            paths_start = (
                ("bash -c 'PYTHONPATH=$PYTHONPATH:" + (":".join(paths)))
                if paths
                else ""
            )
            paths_end = "'" if paths else ""
            return (
                f"{mpi1} "
                "shifter "
                f"--env OMP_NUM_THREADS={sec.threads_per_process} "
                f"{volume_flag} "
                f"--image {sec.image} "
                f"{paths_start} "
                f"{cmd} {mpi2} "
                f"{paths_end} "
            )
        else:
            paths_env = (
                ("PYTHONPATH=" + (":".join(paths)) + ":$PYTHONPATH") if paths else ""
            )
            return (
                # In the non-container case this is much easier
                f"OMP_NUM_THREADS={sec.threads_per_process} "
                f"{paths_env} "
                f"{mpi1} "
                f"{cmd} {mpi2}"
            )

    def configure_for_mini(self):
        """Utility function to setup self for local execution"""
        # if on local machine, query available cores and mem, make one node
        slurm = os.environ.get("SLURM_JOB_ID")

        if slurm:
            # running a job, either interactive or batch
            # check the environment to find out what nodes we are using
            node_list = os.environ["SLURM_JOB_NODELIST"]
            # parse node list
            if "[" in node_list:
                body, vals = node_list.split("[", 1)
                ints = parse_int_set(vals.strip("]"))
                node_names = [f"{body}{i}" for i in ints]
            else:
                node_names = [node_list]

            # We use "CPU"s here to indicate the number of processes
            # we can run, not the NERSC docs meaning which is the number of threads.
            # On Haswell there are 32 cores with 2 hyper threads each, so this
            # env var reports 64, but we should run 32 processes.
            # On KNL it's 68 cores with 4 threads each, so we should run 68.
            slurm_cpus_on_node = os.environ.get("SLURM_CPUS_ON_NODE")
            if slurm_cpus_on_node == "64":
                cpus_per_node = 32
            elif slurm_cpus_on_node == "272":
                cpus_per_node = 68
            else:
                print("Cannot detect NERSC system - assuming 32 processes per node")
                cpus_per_node = 32

            # collect list.
            nodes = [Node(name, cpus_per_node) for name in node_names]
        else:
            # running on login node
            # use at most 4 procs to avoid annoying people
            nodes = [Node("cori", 4)]

        self.info["nodes"] = nodes

    def configure_for_cwl(self):
        """Utility function to set CWL configuration parameters"""


class CoriBatchSite(CoriSite):
    """Object representing execution on the CORI batch system"""

    def configure_for_parsl(self):
        """Utility function to set parsl configuration parameters"""
        from parsl.executors import IPyParallelExecutor
        from parsl.providers import SlurmProvider

        # Get the site details that we need
        cpu_type = self.config.get("cpu_type", "haswell")
        queue = self.config.get("queue", "debug")
        max_slurm_jobs = self.config.get("max_jobs", 2)
        account = self.config.get("account")
        if account is None:
            print(
                "Using LSST DESC account. Specify 'account' in the site config to override"
            )
            account = "m1727"
        walltime = self.config.get("walltime", "00:30:00")
        setup_script = self.config.get(
            "setup",
            "/global/projecta/projectdirs/lsst/groups/WL/users/zuntz/setup-cori",
        )

        provider = SlurmProvider(
            partition=queue,  # Replace with partition name
            min_blocks=0,  # one slurm job to start with
            max_blocks=max_slurm_jobs,  # one slurm job to start with
            scheduler_options=f"#SBATCH --constraint={cpu_type}\n"
            f"#SBATCH --account={account}\n"
            f"#SBATCH --walltime={walltime}\n",
            nodes_per_block=1,
            init_blocks=1,
            worker_init=f"source {setup_script}",
        )

        executor = IPyParallelExecutor(  # pylint: disable=abstract-class-instantiated
            label="cori-batch",
            provider=provider,
        )

        self.info["executor"] = executor


class CoriInteractiveSite(CoriSite):
    """Object representing execution on the CORI interactive system"""

    def configure_for_parsl(self):
        """Utility function to set parsl configuration parameters"""
        from parsl.executors import ThreadPoolExecutor

        max_threads = int(os.environ.get("SLURM_JOB_NUM_NODES", 1))
        executor = ThreadPoolExecutor(label="local", max_threads=max_threads)
        self.info["executor"] = executor


def parse_int_set(nputstr):
    """Utilty funciton to parse integer sets and ranges

    https://stackoverflow.com/questions/712460/interpreting-number-ranges-in-python/712483
    """
    selection = set()
    invalid = set()
    # tokens are comma seperated values
    tokens = [x.strip() for x in nputstr.split(",")]
    for i in tokens:
        try:
            # typically tokens are plain old integers
            selection.add(int(i))
        except:  # pylint: disable=bare-except
            # if not, then it might be a range
            try:
                token = [int(k.strip()) for k in i.split("-")]
                if len(token) > 1:
                    token.sort()
                    # we have items seperated by a dash
                    # try to build a valid range
                    first = token[0]
                    last = token[len(token) - 1]
                    for x in range(first, last + 1):
                        selection.add(x)
            except:  # pylint: disable=bare-except
                # not an int and not a range...
                invalid.add(i)
    # Report invalid tokens before returning valid selection
    if invalid:
        raise ValueError(f"Invalid node list: {nputstr}")
    return selection
