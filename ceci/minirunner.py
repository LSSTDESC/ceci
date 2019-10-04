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
    def __init__(self, node_id, cores, mem):
        self.id = node_id
        self.cores = cores
        self.mem = mem
        self.is_assigned = False

    def __str__(self):
        return f"Node('{self.id}', {self.cores}, {self.mem})"

    def __hash__(self):
        return hash(self.id)

    def assign(self):
        self.is_assigned = True

    def free(self):
        self.is_assigned = False


class Job:
    def __init__(self, name, cmd, cores, mem_per_core=4):
        self.name = name
        self.cmd = cmd
        self.cores = cores
        self.mem_per_core = mem_per_core

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
            raise NoJobReady("Some jobs cannot be run - not enough cores or mem")
        
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
                for node, cores in alloc.items():
                    node.free()

        self.running = continuing_jobs

        for job in completed_jobs:
            self.completed_jobs.append(job)




    def check_availability(self, job):
        cores_on_node = []
        remaining = job.cores
        alloc = {}

        for node in self.nodes:
            if node.is_assigned:
                continue
            avail = min(node.cores, node.mem // job.mem_per_core)
            assign = min(avail, remaining)
            alloc[node] = assign
            remaining -= assign
            if remaining == 0:
                break
            assert remaining>0

        if remaining:
            return None
        else:
            return alloc

    def launch(self, job, alloc):
        # launch the specified job on the specified nodes
        # dict alloc maps nodes to numbers of cores to be used
        w = ','.join([f'{node.id}*{cores}' for node,cores in alloc.items()])
        print(f"\nExecuting {job.name} on these nodes: ")
        for node, cores in alloc.items():
            node.assign()

        cmd = job.cmd

        print(f"Command is:\n{cmd}")
        stdout_file = f'{self.log_dir}/{job.name}.log'
        print(f"Output writing to {stdout_file}\n")
        stdout = open(stdout_file, 'w')
        p = subprocess.Popen(cmd, shell=True, stdout=stdout, stderr=subprocess.STDOUT)

        self.running.append((p, job, alloc))
        # launch cmd in a subprocess, and keep track in running jobs

def get_node_list():
    node_list = os.environ['SLURM_JOB_NODELIST']
    if '[' in node_list:
        body, vals = node_list.split('[', 1)
        ints = parse_int_set(vals.strip(']'))
        node_names = [f'{body}{i}' for i in ints]
    else:
        # one noed
        node_names = node_list
    return node_names

def parse_int_set(nputstr):
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
# end parseIntSet

def build_node_list():
    # if on local machine, query available cores and mem, make one node
    nersc_host = os.environ.get('NERSC_HOST')
    slurm = os.environ.get('SLURM_JOB_ID')
    if nersc_host and slurm:
        # we are running a job
        node_list = get_node_list()

        cpus_per_node = int(os.environ['SLURM_CPUS_ON_NODE'])
        
        mem_per_node_mb = float(os.environ['SLURM_MEM_PER_NODE'])
        mem_per_node = mem_per_node_mb/1000.

        # parse node list
        nodes = [Node(name, cpus_per_node, mem_per_node) for name in node_list]
        # if running 
    elif nersc_host:
        # running on head.  use at most 4 procs to avoid annoying people
        nodes = [Node('cori', 2, 8)]
    else:
        import psutil
        mem = psutil.virtual_memory().total
        cores = psutil.cpu_count(logical=False)
        name = socket.gethostname()
        nodes = [Node(name, cores, mem)]

    print("Generated node list:")
    for n in nodes:
        print(f"    {n}")

    return nodes


def test():
    job1 = Job("Job1", "echo start 1; sleep 3; echo end 1", cores=2)
    job2 = Job("Job2", "echo start 2; sleep 3; echo end 2", cores=2)
    job3 = Job("Job3", "echo start 3; sleep 3; echo end 3", cores=2)
    job4 = Job("Job4", "echo start 4; sleep 3; echo end 4", cores=2)
    job5 = Job("Job5", "echo start 5; sleep 3; echo end 5", cores=2)
    job_dependencies = {
        job1: [job2, job3],
        job2: [job3],
        job3: [],
        job4: [],
        job5: [],
    }

    node = Node('node0001', 4, 20)
    nodes = [node]
    r = Runner(nodes, job_dependencies)
    s = WAITING
    while s == WAITING:
        time.sleep(1)
        s = r.update()

if __name__ == '__main__':
    test()
