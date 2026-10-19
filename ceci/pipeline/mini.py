import sys
from .. import minirunner
from .pipeline import Pipeline, get_default_site
from .graph import StageStatus

class MiniPipeline(Pipeline):
    """A pipeline subclass that uses Minirunner, a sub-module
    of ceci, to run.

    Minirunner is a small tool I wrote suitable for interactive
    jobs on NERSC.  It launches jobs locally (not through a batch system),
    which parsl can also do, but it has a simple and clearer (to me at least)
    understanding of available nodes and cores.

    """

    def __init__(self, *args, **kwargs):
        """Create a MiniRunner Pipeline

        In addition to parent initialization parameters (see the
        Pipeline base class), this subclass can take these optional
        keywords.

        Parameters
        ----------
        callback: function(event_type: str, event_info: dict)
            A function called when jobs launch, complete, or fail,
            and when the pipeline aborts.  Can be used for tracing
            execution.  Default=None.

        sleep: function(t: float)
            A function to replace time.sleep called in the pipeline
            to wait until the next time to check process completion
            Most normal usage will not need this.  Default=None.
        """
        self.callback = kwargs.pop("callback", None)
        self.sleep = kwargs.pop("sleep", None)
        super().__init__(*args, **kwargs)

    def build_dag(self, jobs):
        """Build a directed acyclic graph of a set of stages.

        The DAG is represented by a list of jobs that each other job
        depends on.  If all a job's dependencies are complete
        then it can be run.

        Fun fact: the word "dag" is also Australian slang for a
        "dung-caked lock of wool around the hindquarters of a sheep".
        and is used as a semi-affectionate insult.

        Parameters
        ----------

        jobs: dict[str, minirunner.Job]
            The jobs to generate the DAG for, keyed by stage name

        """
        depend = {}

        # for each stage in our pipeline ...
        for stage_name in self.stage_names:
            if stage_name not in jobs:
                continue
            job = jobs[stage_name]
            depend[job] = []
            # the stages it depends on are the ones that make
            # its input files.  Stages that are not being run
            # are not included
            for tag in self.graph.predecessors(stage_name):
                for parent in self.graph.predecessors(tag):
                    if parent in jobs:
                        depend[job].append(jobs[parent])
        return depend

    def _status_callback(self, event_type, event_info):
        """Record job events as stage status in the graph, and
        then pass them on to any user callback."""
        status = {
            minirunner.EVENT_LAUNCH: StageStatus.RUNNING,
            minirunner.EVENT_COMPLETED: StageStatus.COMPLETE,
            minirunner.EVENT_FAIL: StageStatus.FAILED,
        }.get(event_type)
        if status is not None:
            self.set_stage_status(event_info["job"].name, status)
        if self.callback is not None:
            self.callback(event_type, event_info)

    def initiate_run(self, overall_inputs):
        jobs = {}
        stages = []
        return jobs, stages

    def enqueue_job(self, stage, input_paths):
        sec = self.get_stage_execution_config(stage.instance_name)
        outputs = stage.find_outputs(self.run_config["output_dir"])
        cmd = sec.generate_full_command(input_paths, outputs, self.stages_config)
        job = minirunner.Job(
            stage.instance_name,
            cmd,
            cores=sec.threads_per_process * sec.nprocess,
            nodes=sec.nodes,
        )
        self.run_info[0][stage.instance_name] = job
        self.run_info[1].append(stage)
        return outputs

    def run_jobs(self):
        jobs, _ = self.run_info
        # Now the jobs have all been queued, build them into a graph
        graph = self.build_dag(jobs)
        nodes = get_default_site().info["nodes"]
        log_dir = self.run_config["log_dir"]

        # This pipeline, being mainly for testing, can only
        # run at a single site, so we can assume here that the
        # sites are all the same
        sec = self.get_stage_execution_config(self.stage_names[0])
        nodes = sec.site.info["nodes"]

        # Run under minirununer
        runner = minirunner.Runner(
            nodes, graph, log_dir, callback=self._status_callback, sleep=self.sleep
        )
        interval = self.launcher_config.get("interval", 3)
        try:
            runner.run(interval)
        except minirunner.FailedJob as error:
            sys.stderr.write(
                f"""
*************************************************
Error running pipeline stage {error.job_name}.
Failed after {error.run_time}.

Standard output and error streams in {log_dir}/{error.job_name}.out
*************************************************
"""
            )
            return 1

        return 0

