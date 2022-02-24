"""
Minirunner is a very minimal execution level of a workflow manager.

It understands jobs requiring multiple nodes or cores.
It does minimal checking.

It launches only local jobs, so is designed for debugging or for use on NERSC interactive mode.
"""
import subprocess
import time
from timeit import default_timer

# Constant indicators
COMPLETE = 0
WAITING = 1

EVENT_LAUNCH = "launch"
EVENT_ABORT = "abort"
EVENT_FAIL = "fail"
EVENT_COMPLETED = "completed"


class RunnerError(Exception):
    """Base error class."""


class CannotRun(RunnerError):
    """Error for when no jobs can be run and the pipeline is blocked."""


class TimeOut(RunnerError):
    """Error for when no jobs can be run and the pipeline is blocked."""


class FailedJob(RunnerError):
    """Error for when a job has failed."""

    def __init__(self, msg, job_name):
        super().__init__(msg)
        self.job_name = job_name


class Node:
    """Class for nodes available for a job.

    Nodes have an ID and a number of cores.  Can add more capabilities later
    if needed.
    """

    def __init__(self, node_id, cores):
        """Create a node.

        Parameters
        ----------
        node_id: str
            Name for the node

        cores: int
            Number of cores on the node

        Attributes
        ----------

        id: str
            Name for the node

        cores: int
            Number of cores on the node

        is_assigned: bool
            Whether the node is assigned or not to a job.
        """
        self.id = node_id
        self.cores = cores
        self.is_assigned = False

    def __str__(self):
        return f"Node('{self.id}', {self.cores})"

    __repr__ = __str__

    def __hash__(self):  # pragma: no cover
        return hash(self.id)

    def assign(self):
        """Set this node as assigned to a job"""
        self.is_assigned = True

    def free(self):
        """Set this node as no longer assigned to a job"""
        self.is_assigned = False


class Job:
    """Small wrapper for a job to be run by minirunner, incorporating
    the command line and the resources needed to run it.
    """

    def __init__(self, name, cmd, nodes, cores):
        """Create a node.

        Parameters
        ----------
        name: str
            Name for the job

        cmd: str
            Command line to execute for the job

        cores: int
            Number of cores needed for the job

        nodes: int
            Number of nodes needed for the job

        Attributes
        ----------

        name: str
            Name for the node

        cmd: str
            Command line to execute for the job

        cores: int
            Number of cores needed for the job

        nodes: int
            Number of nodes needed for the job
        """
        self.name = name
        self.cmd = cmd
        self.nodes = nodes
        self.cores = cores

    def __str__(self):
        return f"<Job {self.name}>"

    __repr__ = __str__


def null_callback(
    event_name, event_data
):  # pylint: disable=unused-argument,missing-function-docstring
    pass


class Runner:
    """The main pipeline runner class.

    User is responsible for supplying the graph of jobs to run on it, etc.

    Attributes
    ----------

    nodes: list[Node]
        Nodes available to run on

    job_graph: dict{Job:Job}
        Indicates what jobs are to be run and which other jobs they depend on

    completed_jobs: list[Job]
        Jobs that have finished

    running: list[Job]
        Jobs currently running

    queued_jobs: list[Job]
        Jobs waiting to be run

    log_dir: str
        Dir where the logs are put
    """

    def __init__(self, nodes, job_graph, log_dir, callback=None, sleep=None):
        """Create a Runner

        Parameters
        ----------
        nodes: list[Node]
            Nodes available to run on

        job_graph: dict{Job:Job}
            Indicates what jobs are to be run and which other jobs they depend on

        log_dir: str
            Dir where the logs are put

        callback: function(event_type: str, event_info: dict)
            A function called when jobs launch, complete, or fail,
            and when the pipeline aborts.  Can be used for tracing
            execution. Default=None.

        sleep: function(t: float)
            A function to replace time.sleep called in the pipeline
            to wait until the next time to check process completion
            Most normal usage will not need this. Default=None.
        """
        self.nodes = nodes
        self.job_graph = job_graph
        self.completed_jobs = []
        self.running = []
        self.log_dir = log_dir
        self.queued_jobs = list(job_graph.keys())

        # By default we use an empty callback
        # and the default sleep
        if callback is None:
            callback = null_callback
        if sleep is None:
            sleep = time.sleep
        self.callback = callback
        self.sleep = sleep

    def run(self, interval, timeout=1e300):
        """Launch the pipeline.

        Parameters
        ----------
        interval: int
            How long to wait between each poll of the jobs.

        """
        status = WAITING
        t0 = default_timer()
        try:
            while status == WAITING:
                status = self._update()
                self.sleep(interval)
                t = default_timer() - t0
                if t > timeout:
                    raise TimeOut(
                        f"Pipeline did not finish within {timeout} seconds. "
                        f"These jobs were still running: {self.running}"
                    )
        except Exception:
            # The pipeline should be cleaned up
            # in the event of any error.
            # There should be nothing to clean up
            # if the pipeline ends cleanly
            # TODO: add a test for this
            self.abort()
            raise

    def abort(self):
        """End the pipeline and kill all running jobs."""
        for process, _, _ in self.running:
            process.kill()

        for node in self.nodes:
            node.free()

        # run the callback, listing all jobs that were running
        # when the system failed.  The failed job will already
        # have triggered EVENT_FAIL.
        self.callback(EVENT_ABORT, {"running": self.running[:]})
        self.running = []

    def _launch(self, job, alloc):
        # launch the specified job on the specified nodes
        # dict alloc maps nodes to numbers of cores to be used
        print(f"\nExecuting {job.name}")
        for node in alloc:
            node.assign()

        cmd = job.cmd

        print(f"Command is:\n{cmd}")
        stdout_file = f"{self.log_dir}/{job.name}.out"
        print(f"Output writing to {stdout_file}\n")

        with open(stdout_file, "w") as stdout:
            # launch cmd in a subprocess, and keep track in running jobs
            p = subprocess.Popen(
                cmd, shell=True, stdout=stdout, stderr=subprocess.STDOUT
            )  # pylint: disable=consider-using-with
            self.running.append((p, job, alloc))
            self.callback(
                EVENT_LAUNCH,
                {"job": job, "stdout": stdout_file, "process": p, "nodes": alloc},
            )

    def _ready_jobs(self):
        # Find jobs ready to be run now
        return [
            job
            for job in self.queued_jobs
            if all(p in self.completed_jobs for p in self.job_graph[job])
        ]

    def _check_impossible(self):
        n_node = len(self.nodes)
        n_core = sum(node.cores for node in self.nodes)

        for job in self.queued_jobs:
            if job.nodes > n_node:
                raise CannotRun(
                    f"Job {job} cannot be run - it needs {job.nodes}"
                    f" nodes but only {n_node} is/are available"
                )
            if job.cores > n_core:  # pragma: no cover
                raise CannotRun(
                    f"Job {job} cannot be run - it needs {job.cores}"
                    f" cores but only {n_core} is/are available"
                )

    def _update(self):
        # Iterate, checking the status of all jobs and launching any new ones
        self._check_completed()
        self._check_impossible()

        # If all jobs are done, exit

        if len(self.completed_jobs) == len(self.job_graph):
            return COMPLETE

        # Otherwise check what jobs can now run
        ready = self._ready_jobs()

        # and launch them all
        for job in ready:
            # find nodes to run them on
            alloc = self._check_availability(job)
            if alloc is None:
                continue
            # and launch the job
            self.queued_jobs.remove(job)
            self._launch(job, alloc)

        # indicate that we are still running.
        return WAITING

    def _check_completed(self):
        # check if any running jobs have completed
        completed_jobs = []
        continuing_jobs = []
        # loop through all known running ones
        for process, job, alloc in self.running:
            # check status
            status = process.poll()
            # None indicates job is still running
            if status is None:  # pragma: no cover
                continuing_jobs.append((process, job, alloc))
            # status !=0 indicates error in job.
            # kill everything
            elif status:
                print(f"Job {job.name} has failed with status {status}")
                # Call back with info about the failed job and how it ran,
                # then abort the pipeline to stop other things going on.
                # TODO: offer a mode where other non-dependent jobs keep
                # running
                self.callback(
                    EVENT_FAIL,
                    {"job": job, "status": status, "process": process, "nodes": alloc},
                )
                self.abort()
                raise FailedJob(job.cmd, job.name)
            # status==0 indicates sucess in job, so free resources
            else:
                print(f"Job {job.name} has completed successfully!")
                # Call back with info about the successful job and how it ran
                self.callback(
                    EVENT_COMPLETED,
                    {"job": job, "status": 0, "process": process, "nodes": alloc},
                )
                completed_jobs.append(job)
                for node in alloc:
                    node.free()

        self.running = continuing_jobs

        for job in completed_jobs:
            self.completed_jobs.append(job)

    def _check_availability(self, job):
        # check if there are nodes available to run this job
        # Return them if so, or None if not.

        # cores_on_node = []
        # remaining = job.cores
        alloc = {}

        free_nodes = [node for node in self.nodes if not node.is_assigned]
        if len(free_nodes) >= job.nodes:
            alloc = free_nodes[: job.nodes]
        else:
            alloc = None

        return alloc
