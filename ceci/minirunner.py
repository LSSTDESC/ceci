import subprocess
import os
import time
import socket

COMPLETE = 0
WAITING = 1

class RunnerError(Exception):
    pass

class NoJobsReady(RunnerError):
    pass

class FailedJob(RunnerError):
    pass


class Node:
    def __init__(self, node_id, cores):
        self.id = node_id
        self.cores = cores
        self.is_assigned = False

    def __str__(self):
        return f"Node('{self.id}', {self.cores})"

    def __hash__(self):
        return hash(self.id)

    def assign(self):
        self.is_assigned = True

    def free(self):
        self.is_assigned = False


class Job:
    def __init__(self, name, cmd, nodes, cores):
        self.name = name
        self.cmd = cmd
        self.nodes = nodes
        self.cores = cores

    def __str__(self):
        return f"<Job {self.name}>"

class Runner:
    def __init__(self, nodes, job_graph, log_dir):
        self.nodes = nodes
        self.job_graph = job_graph
        self.completed_jobs = []
        self.running = []
        self.log_dir = log_dir
        self.queued_jobs = list(job_graph.keys())

    def ready_jobs(self):
        return [job for job in self.queued_jobs if 
            all(p in self.completed_jobs for p in self.job_graph[job])]

    def abort(self):
        for process, job, alloc in self.running:
            process.kill()
        for node in self.nodes:
            node.free()

    def update(self):

        self.check_completed()

        if len(self.completed_jobs) == len(self.job_graph):
            return COMPLETE

        ready = self.ready_jobs()
        if (not self.running) and (not ready):
            raise NoJobReady("Some jobs cannot be run - not enough cores.")
        
        for job in ready:
            alloc = self.check_availability(job)
            if alloc is None:
                continue
            self.queued_jobs.remove(job)
            self.launch(job, alloc)

        return WAITING

    def check_completed(self):
        completed_jobs = []
        continuing_jobs = []
        for process, job, alloc in self.running:
            status = process.poll()
            if status is None:
                continuing_jobs.append((process, job, alloc))
            elif status:
                print(f"Job {job.name} has failed with status {status}")
                self.abort()
                raise FailedJob(job.cmd)
            else:
                print(f"Job {job.name} has completed successfully!")
                completed_jobs.append(job)
                for node in alloc:
                    node.free()

        self.running = continuing_jobs

        for job in completed_jobs:
            self.completed_jobs.append(job)




    def check_availability(self, job):
        cores_on_node = []
        remaining = job.cores
        alloc = {}

        free_nodes = [node for node in self.nodes if not node.is_assigned]
        if len(free_nodes) >= job.nodes:
            alloc = free_nodes[:job.nodes]
        else:
            alloc = None

        return alloc

    def launch(self, job, alloc):
        # launch the specified job on the specified nodes
        # dict alloc maps nodes to numbers of cores to be used
        print(f"\nExecuting {job.name}")
        for node in alloc:
            node.assign()

        cmd = job.cmd

        print(f"Command is:\n{cmd}")
        stdout_file = f'{self.log_dir}/{job.name}.log'
        print(f"Output writing to {stdout_file}\n")
        stdout = open(stdout_file, 'w')
        p = subprocess.Popen(cmd, shell=True, stdout=stdout, stderr=subprocess.STDOUT)

        self.running.append((p, job, alloc))
        # launch cmd in a subprocess, and keep track in running jobs

def test():
    job1 = Job("Job1", "echo start 1; sleep 3; echo end 1", cores=2, nodes=1)
    job2 = Job("Job2", "echo start 2; sleep 3; echo end 2", cores=2, nodes=1)
    job3 = Job("Job3", "echo start 3; sleep 3; echo end 3", cores=2, nodes=1)
    job4 = Job("Job4", "echo start 4; sleep 3; echo end 4", cores=2, nodes=1)
    job5 = Job("Job5", "echo start 5; sleep 3; echo end 5", cores=2, nodes=1)
    job_dependencies = {
        job1: [job2, job3],
        job2: [job3],
        job3: [],
        job4: [],
        job5: [],
    }

    node = Node('node0001', 4)
    nodes = [node]
    r = Runner(nodes, job_dependencies, '.')
    s = WAITING
    while s == WAITING:
        time.sleep(1)
        s = r.update()

if __name__ == '__main__':
    test()
