"""
Minirunner is a very minimal execution level of a workflow manager.

It understands jobs requiring multiple nodes or cores.
It does minimal checking.

It launches only local jobs, so is designed for debugging or for use on NERSC interactive mode.
"""
import subprocess
import os
import time
import socket

# Constant indicators
COMPLETE = 0
WAITING = 1

class RunnerError(Exception):
    """Base error class."""
    pass


class NoJobsReady(RunnerError):
    """Error for when no jobs can be run and the pipeline is blocked."""
    pass


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

    def __hash__(self):
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
    def __init__(self, nodes, job_graph, log_dir):
        """Create a Runner

        Parameters
        ----------
        nodes: list[Node]
            Nodes available to run on

        job_graph: dict{Job:Job}
            Indicates what jobs are to be run and which other jobs they depend on

        log_dir: str
            Dir where the logs are put        

        """
        self.nodes = nodes
        self.job_graph = job_graph
        self.completed_jobs = []
        self.running = []
        self.log_dir = log_dir
        self.queued_jobs = list(job_graph.keys())


    def run(self, interval):
        """Launch the pipeline.

        Parameters
        ----------
        interval: int
            How long to wait between each poll of the jobs.

        """
        status = WAITING
        try:
            while status == WAITING:
                status = self._update()
                time.sleep(interval)
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
        for process, job, alloc in self.running:
            process.kill()
        for node in self.nodes:
            node.free()


    def _launch(self, job, alloc):
        # launch the specified job on the specified nodes
        # dict alloc maps nodes to numbers of cores to be used
        print(f"\nExecuting {job.name}")
        for node in alloc:
            node.assign()

        cmd = job.cmd

        print(f"Command is:\n{cmd}")
        stdout_file = f'{self.log_dir}/{job.name}.out'
        print(f"Output writing to {stdout_file}\n")

        stdout = open(stdout_file, 'w')
        # launch cmd in a subprocess, and keep track in running jobs
        p = subprocess.Popen(cmd, shell=True, stdout=stdout, stderr=subprocess.STDOUT)
        self.running.append((p, job, alloc))


    def _ready_jobs(self):
        # Find jobs ready to be run now
        return [job for job in self.queued_jobs if 
            all(p in self.completed_jobs for p in self.job_graph[job])]


    def _update(self):
        # Iterate, checking the status of all jobs and launching any new ones
        self._check_completed()

        # If all jobs are done, exit
        if len(self.completed_jobs) == len(self.job_graph):
            return COMPLETE

        # Otherwise check what jobs can now run
        ready = self._ready_jobs()
        if (not self.running) and (not ready):
            raise NoJobReady("Some jobs cannot be run - not enough cores.")
        
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
            if status is None:
                continuing_jobs.append((process, job, alloc))
            # status !=0 indicates error in job.
            # kill everything
            elif status:
                print(f"Job {job.name} has failed with status {status}")
                self.abort()
                raise FailedJob(job.cmd, job.name)
            # status==0 indicates sucess in job, so free resources
            else:
                print(f"Job {job.name} has completed successfully!")
                completed_jobs.append(job)
                for node in alloc:
                    node.free()

        self.running = continuing_jobs

        for job in completed_jobs:
            self.completed_jobs.append(job)


    def _check_availability(self, job):
        # check if there are nodes available to run this job
        # Return them if so, or None if not.
        cores_on_node = []
        remaining = job.cores
        alloc = {}

        free_nodes = [node for node in self.nodes if not node.is_assigned]
        if len(free_nodes) >= job.nodes:
            alloc = free_nodes[:job.nodes]
        else:
            alloc = None

        return alloc
