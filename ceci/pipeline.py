import os
import sys
import time
import collections
import yaml
import shutil
from .stage import PipelineStage
from . import minirunner
from .sites import get_default_site

class StageExecutionConfig:
    """
    This class stores information describing how an individual job is to be executed,
    for example how many cores it is run on, and where.  It does not store the
    job input or output information or anything like that.

    TODO: Consider attaching this object to the stage itself.

    Attributes
    ----------

    name: str
        The name of the stage
    site: Site object
        (default the global default) The site this stage is run on
    nprocess: int
        (default 1) The number of (usually MPI) processes to use for this task
    nodes: int
        (default 1) The number of nodes the processes should be spread over
    threads_per_process: int
        (default 1) The number of (usually OpenMP) threads to use per process.
    mem_per_process: float
        (defaut 2GB) The amount of memory in GB required for the job
    image: str
        (default is the site default) A docker image name to use for this task
    volume: str
        (default is the site default) Any volume mappings in the form
        /path/on/host:/path/on/container that the job needs
    """
    def __init__(self, info):

        # Core attributes - mandatory
        self.name = info['name']
        self.site = info.get('site', get_default_site())

        # Parallelism attributes - optional
        self.nprocess = info.get('nprocess', 1)
        self.nodes = info.get('nodes', 1)
        self.threads_per_process = info.get('threads_per_process', 1) #
        self.mem_per_process = info.get('mem_per_process', 2)

        # Container attributes - optional.
        # There may be a default container for the entire site the
        # stage is run on, in which case use that if it is not overridden.
        self.image = info.get('image', self.site.config.get('image'))
        self.volume = info.get('volume', self.site.config.get('volume'))


class Pipeline:
    """
    The Pipeline base class models the shared information and behaviour
    that pipelines need, no matter which workflow manager runs them.

    This includes constructing the pipeline in the first place from
    stages, checking that it is a valid pipeline, finding overall
    inputs and outputs for the pipeline, and constructing the command
    line for a given stage.

    Sub-classes run the pipeline using a given workflow manager.
    """
    def __init__(self, stages, launcher_config):
        """Construct a pipeline using configuraion information.

        Parameters
        ----------
        stages: list[dict]
            Information used to construct each stage and how it is run
        launcher_config: dict
            Any additional configuration that will be needed by the workflow
            management.  The base class does not use this.
        """
        self.launcher_config = launcher_config

        # These are populated as we add stages below
        self.stage_execution_config = {}
        self.stage_names = []

        # Store the individual stage informaton
        for info in stages:
            self.add_stage(info)

    def add_stage(self, stage_info):
        """Add a stage to the pipeline.

        To begin with this stage is not connected to any others - 
        that is determined later.

        The configuration info for this stage must contain at least
        the name of the stage and the name of the site where it is
        to be run.  It can also contain information for the 
        StageExecutionConfig above describing parallelism 
        and container usage.

        The stage_info can contain the following parameters:
            site: Site object
                The site this stage is run on
            nprocess: int
                (default 1) The number of (usually MPI) processes to use for this task
            nodes: int
                (default 1) The number of nodes the processes should be spread over
            threads_per_process: int
                (default 1) The number of (usually OpenMP) threads to use per process.
            mem_per_process: float
                (defaut 2GB) The amount of memory in GB required for the job
            image: str
                (default is the site default) A docker image name to use for this task
            volume: str
                (default is the site default) Any volume mappings in the form
                /path/on/host:/path/on/container that the job needs

        Parameters
        ----------
        stage_info: dict
            Configuration information for this stage. See docstring for info.


        """
        sec = StageExecutionConfig(stage_info)
        self.stage_execution_config[sec.name] = sec
        self.stage_names.append(sec.name)

    def remove_stage(self, name):
        """Delete a stage from the pipeline

        Parameters
        ----------
        name: str
            The name of the stage to remove.

        """
        self.stage_names.remove(name)
        del self.stage_execution_config[name]

    def find_inputs(self, stage, pipeline_files, run_config):
        return {tag: pipeline_files[tag] for tag, _ in stage.inputs}

    def find_outputs(self, stage, run_config):
        """Get the names of all the outputs a stage will generate.

        The name is determined from the output tag, the type of the file,
        and the directory it will be put into.

        Sub-class pipelines might organise results differently, hence this
        being a method of Pipeline rather than PipelineStage.

        Parameters
        ----------
        stage: PipelineStage class
            This usually takes a class, not an instance, since the inputs/outputs are all
            class level variables

        returns: list[str]
            Complete paths to all outputs of the stage.
        """
        outdir = run_config['output_dir']
        return {tag: f'{outdir}/{ftype.make_name(tag)}' for tag, ftype in stage.outputs}



    def ordered_stages(self, overall_inputs):
        """Produce a linear ordering for the stages.

        Some stages within the pipeline might be ruunnable in parallel; this
        method does not analyze this, since different workflow managers will
        treat this differently.

        The stages in the pipeline are also checked for consistency, to avoid
        circular pipelines (A->B->C->A) and to ensure that all overall inputs
        needed in the pipeline are supplied from the overall inputs.

        The naive ordering algorithm used is faster when the stages are in
        the correct order to start with.  This won't matter unless you have
        a large number of stages.

        Parameters
        ----------
        overall_inputs: dict{str: str}
            Any inputs that do not need to be generated by the pipeline but are
            instead already supplied at the start.  Mapping is from tag -> path.

        Returns
        -------
        ordered_stages: list[PipelineStage]
            The pipeline stages in an order that can be run.

        """
        stage_names = self.stage_names[:]
        stages = [PipelineStage.get_stage(stage_name) for stage_name in stage_names]

        n = len(stage_names)

        # Check for a pipeline output that is already given as an input
        for stage in stages:
            for tag in stage.output_tags():
                if tag in overall_inputs:
                    raise ValueError("Pipeline stage {stage.name} "
                                     "generates output {tag}, but "
                                     "it is already an overall input")
        stage_set = {stage for stage in stage_names}
        if len(stage_set) < len(stages):
            raise ValueError("Some stages are included twice in your pipeline")


        # make a dict mapping each tag to the stages that need it
        # as an input. This is the equivalent of the adjacency matrix
        # in graph-speak
        dependencies = collections.defaultdict(list)
        for stage in stages:
            for tag in stage.input_tags():
                dependencies[tag].append(stage)

        # count the number of inputs required by each stage
        missing_input_counts = {stage:len(stage.inputs) for stage in stages}
        found_inputs = set()
        # record the stages which are receiving overall inputs
        for tag in overall_inputs:
            found_inputs.add(tag)
            for stage in dependencies[tag]:
                missing_input_counts[stage] -= 1


        # find all the stages that are ready because they have no missing inputs
        queue = [stage for stage in stages if missing_input_counts[stage]==0]
        ordered_stages = []


        # make the ordering
        while queue:
            # get the next stage that has no inputs missing
            stage = queue.pop()
            # for file that stage produces,
            for tag in stage.output_tags():
                # find all the next_stages that depend on that file
                found_inputs.add(tag)
                for next_stage in dependencies[tag]:
                    # record that the next stage now has one less
                    # missing dependency
                    missing_input_counts[next_stage] -= 1
                    # if that stage now has no missing stages
                    # then enqueue it
                    if missing_input_counts[next_stage] == 0:
                        queue.append(next_stage)
            ordered_stages.append(stage)

        
        # If any stages are still not in the list then there is a problem.
        # Try to diagnose it here.
        if len(ordered_stages) != n:
            stages_missing_inputs = [stage for (stage, count)
                                     in missing_input_counts.items()
                                     if count>0]
            msg1 = []
            for stage in stages_missing_inputs:
                missing_inputs = [tag for tag in stage.input_tags() 
                                  if tag not in found_inputs]
                missing_inputs = ', '.join(missing_inputs)
                msg1.append(f"Stage {stage.name} is missing input(s): {missing_inputs}")

            msg1 = "\n".join(msg1)
            raise ValueError(f"""
Some required inputs to the pipeline could not be found,
(or possibly your pipeline is cyclic):

{msg1}
""")

        return ordered_stages



    def generate_full_command(self, stage, inputs, outputs, config, run_config, missing_inputs_in_outdir=False):
        """Generate the complete command to run a stage.

        This includes any mpirun commands, settings of env vars, and
        if docker/shifter are used it includes that too.  This depends
        on the execution settings (nprocess, nthread) as well as the
        stage itself.

        Parameters
        ----------
        stage: PipelineStage
            The stage to generate the command for

        inputs: dict{str: str}
            Mapping of inputs to the stage to paths

        config: str
            Path to stage configuration file for pipeline stages

        outdir: dict{str: str}
            Mapping of outputs of the stage to paths

        Returns
        -------
        cmd: str
            Complete command to be executed

        """
        core = stage.generate_command(inputs, config, outputs)
        sec = self.stage_execution_config[stage.name]
        cmd = sec.site.command(core, sec)
        return cmd


    def run(self, overall_inputs, run_config, stages_config):
        # Make a copy, since we'll be modifying this.
        pipeline_files = overall_inputs.copy()

        # Get the stages in the order we need.
        stages = self.ordered_stages(overall_inputs)

        # Initiate the run.
        # This is an implementation detail for the different subclasses to store 
        # necessary information about the run if necessary.
        # Usually, the arguments are ignored, but they are provided in case a class needs to
        # do something special with any of them.
        run_info = self.initiate_run(stages, pipeline_files, run_config, stages_config)



        # make sure output directories exist
        os.makedirs(run_config['output_dir'], exist_ok=True)
        os.makedirs(run_config['log_dir'], exist_ok=True)

        for stage in stages:
            # If we are in "resume" mode and the pipeline has already been run
            # then we re-use any existing outputs.  User is responsible for making
            # sure they are complete!
            if self.should_skip_stage(stage, run_config):
                self.already_finished_job(stage, run_info)
                output_paths = self.find_outputs(stage, run_config)
                pipeline_files.update(output_paths)

            # Otherwise, run the pipeline and register any outputs from the
            # pipe element.
            else:
                stage_outputs = self.enqueue_job(stage, pipeline_files, stages_config, run_info, run_config)
                pipeline_files.update(stage_outputs)

        status = self.run_jobs(run_info, run_config)

        # When the
        self.run_info = run_info
        self.pipeline_outputs = self.find_all_outputs(stages, run_config)

        return status

    def already_finished_job(self, stage, run_info):
        print(f"Skipping stage {stage.name} because its outputs exist already")


    def should_skip_stage(self, stage, run_config):
        outputs = self.find_outputs(stage, run_config).values()
        already_run_stage = all(os.path.exists(output) for output in outputs)
        return already_run_stage and run_config['resume']

    def find_all_outputs(self, stages, run_config):
        outputs = {}
        for stage in stages:
            stage_outputs = self.find_outputs(stage, run_config)
            outputs.update(stage_outputs)
        return outputs



class DryRunPipeline(Pipeline):
    """A pipeline subclass which just does a dry-run, showing which commands
    would be executed.

    See the base class for almost all behaviour.

    No additional attributes.
    """
    def initiate_run(self, stages, overall_inputs, run_config, stages_config):
        return []

    def should_skip_stage(self, stage, run_config):
        return False

    def enqueue_job(self, stage, pipeline_files, stages_config, run_info, run_config):
        outputs = self.find_outputs(stage, run_config)
        cmd = self.generate_full_command(stage, pipeline_files, outputs, stages_config, run_config)
        run_info.append(cmd)
        return outputs

    def run_jobs(self, run_info, run_config):
        for cmd in run_info:
            print(cmd)
            print("\n")
        return 0

    def find_all_outputs(self, stages, run_config):
        return {}



class ParslPipeline(Pipeline):
    """A subclass of Pipeline that uses Parsl to manage workflow.
    """

    def initiate_run(self, stages, overall_inputs, run_config, stages_config):
        return [] # list of futures



    def enqueue_job(self, stage, pipeline_files, stages_config, run_info, run_config):
        from parsl.data_provider.files import File

        log_dir = run_config['log_dir']
        # convert the command into an app
        app = self.generate_app(stage, run_config)

        # Convert the dicts of inputs/outputs to the list that
        # parsl wants. 
        # The inputs that exist already need to be converted into Parsl File objects.
        # The ones that don't stay as data futures
        inputs1 = self.find_inputs(stage, pipeline_files, run_config)
        inputs = [File(val) if isinstance(val, str) else val for val in inputs1.values()]
        inputs.append(File(stages_config))
        # The outputs are just strings.  python dicts are now ordered,
        # so this works okay.
        outputs = list(self.find_outputs(stage, run_config).values())

        # have parsl queue the app
        future = app(inputs=inputs, outputs=outputs)
        run_info.append((stage.name, future))
        return {
            tag: future.outputs[i]
            for i,tag in enumerate(stage.output_tags())
        }

    def run_jobs(self, run_info, run_config):
        from parsl.app.errors import AppFailure
        log_dir = run_config['log_dir']
        # Wait for the final results, from all files
        for stage_name, future in run_info:
            try:
                # This waits for b/g pipeline completion.
                future.result()
            # Parsl emits this on any non-zero status code.
            except AppFailure:
                stdout_file = f'{log_dir}/{stage_name}.err'
                stderr_file = f'{log_dir}/{stage_name}.out'
                sys.stderr.write(f"""
*************************************************
Error running pipeline stage {stage_name}.

Standard output and error streams below.

*************************************************

Standard output:
----------------

""")
                if os.path.exists(stdout_file):
                    sys.stderr.write(open(stdout_file).read())
                else:
                    sys.stderr.write("STDOUT MISSING!\n\n")

                sys.stderr.write(f"""
*************************************************

Standard error:
----------------

""")

                if os.path.exists(stderr_file):
                    sys.stderr.write(open(stderr_file).read())
                else:
                    sys.stderr.write("STDERR MISSING!\n\n")
                return 1
        return 0



    def generate_app(self, stage, run_config):
        """Build a parsl app that wraps this pipeline stage.

        This object is passed onto parsl for execution.

        Parameters
        ----------
        stage: PipelineStage
            The stage to wrap

        log_dir: str
            The directory to put the logs in.

        sec: StageExecutionConfig
            The execution info for this stage.

        Returns
        -------
        app: App
            A parsl app object

        """
        import parsl
        module = stage.get_module()
        module = module.split('.')[0]

        inputs = {}
        outputs = {}


        # Parsl wants a function with the inputs and outputs
        # extracted from lists.  That function should iself return
        # a string representing the bash cmd line.
        # We build up all these components here.

        # Parsl wants our functions to take their input/output paths
        # from inputs[0], inputs[1], etc.
        for i,inp in enumerate(stage.input_tags()):
            inputs[inp] = f'{{inputs[{i}]}}'
        for i,out in enumerate(stage.output_tags()):
            outputs[out] = f'{{outputs[{i}]}}'

        # The last input file is always the config file
        config_index = len(stage.input_tags())
        config = f'{{inputs[{config_index}]}}'

        # This includes all the "mpirun" stuff.
        sec = self.stage_execution_config[stage.name]
        executor = sec.site.info['executor']


        # Construct the command line call
        core = self.generate_full_command(stage, inputs, outputs, config, run_config)
        cmd1 = sec.site.command(core, sec)
        log_dir = run_config['log_dir']


        # We will be exec'ing this here.  We can't just define it inline
        # because it just gets too painful with all the i/o names, so instead
        # we build and exec a string.
        template = f"""
@parsl.app.app.bash_app(executors=[executor])
def {stage.name}(inputs, outputs, stdout='{log_dir}/{stage.name}.out', stderr='{log_dir}/{stage.name}.err'):
    cmd = '{cmd1}'.format(inputs=inputs, outputs=outputs)
    print("Launching command:")
    print(cmd, " 2> {log_dir}/{stage.name}.err 1> {log_dir}/{stage.name}.out")
    return cmd
"""
        print(template)

        # local variables for creating this function.
        d = {'executor':executor.label, 'cmd1':cmd1}
        exec(template, {'parsl':parsl}, d)

        # Return the function itself.
        return d[stage.name]



class MiniPipeline(Pipeline):
    """A pipeline subclass that uses Minirunner, a sub-module
    of ceci, to run.

    Minirununer is a small tool I wrote suitable for interactive
    jobs on cori.  It launches jobs locally (not through a batch system),
    which parsl can also do, but it has a simple and clearer (to me at least)
    understanding of available nodes and cores.

    """

    def build_dag(self, stages, jobs):
        """Build a directed acyclic graph of a set of stages.

        The DAG is represented by a list of jobs that each other job
        depends on.  If all a job's dependencies are complete
        then it can be run.

        Fun fact: the word "dag" is also Australian slang for a
        "dung-caked lock of wool around the hindquarters of a sheep".
        and is used as a semi-affectionate insult.

        Parameters
        ----------

        stages: list[PipelineStage]
            A list of stages to generate the DAG for

        """
        depend = {}
        # for each stage in our pipeline ...
        for stage in stages[:]:
            depend[jobs[stage.name]] = []
            job = jobs[stage.name]
            # check for each of the inputs for that stage ...
            for tag in stage.input_tags():
                for potential_parent in stages[:]:
                    # if that stage is supplied by another pipeline stage
                    if tag in potential_parent.output_tags():
                        depend[job].append(jobs[potential_parent.name])
        return depend


    def initiate_run(self, stages, overall_inputs, run_config, stages_config):
        jobs = {}
        stages = []
        return jobs, stages

    def enqueue_job(self, stage, pipeline_files, stages_config, run_info, run_config):
        jobs, stages = run_info
        sec = self.stage_execution_config[stage.name]
        outputs = self.find_outputs(stage, run_config)
        cmd = self.generate_full_command(stage, pipeline_files, outputs, 
            stages_config, run_config)
        job = minirunner.Job(stage.name, cmd, 
            cores=sec.threads_per_process*sec.nprocess, nodes=sec.nodes)
        jobs[stage.name] = job
        stages.append(stage)
        return outputs

    def run_jobs(self, run_info, run_config):
        jobs, stages = run_info
        # Now the jobs have all been queued, build them into a graph
        graph = self.build_dag(stages, jobs)
        nodes = get_default_site().info['nodes']
        log_dir = run_config['log_dir']

        # This pipeline, being mainly for testing, can only
        # run at a single site, so we can assume here that the
        # sites are all the same
        sec = self.stage_execution_config[self.stage_names[0]]
        nodes = sec.site.info['nodes']

        # Run under minirununer
        runner = minirunner.Runner(nodes, graph, log_dir)
        interval = self.launcher_config.get('interval', 3)
        try:
            runner.run(interval)
        except minirunner.FailedJob as error:
            sys.stderr.write(f"""
*************************************************
Error running pipeline stage {error.job_name}.

Standard output and error streams in {log_dir}/{error.job_name}.out
*************************************************
""")
            return 1

        return 0


class CWLPipeline(Pipeline):
    """Export the pipeline as Common Workflow Language files and optionally run it
    with cwltool or another CWL-aware runner.
    
    """

    def make_inputs_file(self, stages, overall_inputs, stages_config, inputs_file):

        # find out the class of the file objects.  This is a bit ugly,
        # but the only way we record this now it in the stages.
        input_types = {}
        for stage in stages:
            for tag, ftype in stage.inputs:
                if (tag in overall_inputs) and (tag not in input_types):
                    input_types[tag] = ftype

        inputs = {}
        # First we build up the file inputs, each with this dictionary
        # information.
        for tag, filepath in overall_inputs.items():
            ftype = input_types[tag]
            filepath = os.path.abspath(filepath)
            d = {'class': 'File', 'path':filepath, 'format': ftype.format}
            inputs[tag] = d


        # CWL also wants the config information passed through in an inputs
        # file, so it is all collected together.
        stage_config_data = yaml.safe_load(open(stages_config))
        global_config = stage_config_data.get('global',  {})

        # For each stage, we check if any of its config information
        # is set in the config file
        for stage in stages:
            # There might be nothing if no options are needed.
            this_stage_config = stage_config_data.get(stage.name, {})
            # Record only keys that have been set.  If any are missing
            # it is an error that will be noticed later.
            for key in stage.config_options:
                val = this_stage_config.get(key, global_config.get(key))
                if val is not None:
                    inputs[f'{key}@{stage.name}'] = val

        inputs['config'] = {
            'class': 'File',
            'path': os.path.abspath(stages_config),
            # YAML file indicator:
            'format': "http://edamontology.org/format_3750"
        }

        # Save to the inputs file
        with open(inputs_file, 'w') as f:
            yaml.dump(inputs, f)


    def initiate_run(self, stages, overall_inputs, run_config, stages_config):
        from cwlgen.workflow import Workflow
        wf = Workflow()

        cwl_dir = self.launcher_config['dir']
        os.makedirs(cwl_dir, exist_ok=True)

        # Write the inputs file
        inputs_file = f'{cwl_dir}/cwl_inputs.yml'
        self.make_inputs_file(stages, overall_inputs, stages_config, inputs_file)

        # CWL treats overall inputs differently, storing
        # them in the inputs file.  We keep the 
        overall_inputs.clear()

        return {
            'workflow': wf,
            'cwl_dir': cwl_dir,
            'inputs_file': inputs_file,
            # keeps track of overall pipeline inputs we have already found
            'workflow_inputs': set(),
            'workflow_outputs': {},
        }



    def enqueue_job(self, stage, pipeline_files, stages_config, run_info, run_config):
        from cwlgen.workflowdeps import WorkflowStep, WorkflowStepInput
        from cwlgen.workflowdeps import WorkflowOutputParameter, InputParameter

        cwl_dir = run_info['cwl_dir']
        workflow = run_info['workflow']
        log_dir = run_config['log_dir']

        # Create a CWL representation of this step
        cwl_tool = stage.generate_cwl(log_dir)
        cwl_tool.export(f'{cwl_dir}/{stage.name}.cwl')

        # Load that representation again and add it to the pipeline
        step = WorkflowStep(stage.name, run=f'{cwl_tool.id}.cwl')

        # For CWL these inputs are a mix of file and config inputs,
        # so not he same as the pipeline_files we usually see
        for inp in cwl_tool.inputs:

            # If this input is an putput from an earlier stage
            # then it takes its name based on that
            if inp.id in pipeline_files:
                name = pipeline_files[inp.id]+'/'+inp.id
            # otherwise if it's a config option we mangle
            # it to avod clashes
            elif inp.id in stage.config_options:
                name = f'{inp.id}@{cwl_tool.id}'
            # otherwise just leave it as-is
            else:
                name = inp.id

            # If it's an overall input to the entire pipeline we
            # record that.  We only want things that aren't outputs
            # (first clause) and that we haven't already recorded (second)
            if (inp.id not in pipeline_files) and (name not in run_info['workflow_inputs']):
                run_info['workflow_inputs'].add(name)
                # These are the overall inputs to the enture pipeline.
                # Convert them to CWL input parameters
                cwl_inp = InputParameter(name,
                                         label=inp.label,
                                         param_type=inp.type,
                                         param_format=inp.format)
                cwl_inp.default = inp.default

                # Bypassing cwlgen type check in case of arrays
                if type(inp.type) == dict:
                    cwl_inp.type = inp.type

                # record that these are overall pipeline inputs
                workflow.inputs.append(cwl_inp)

            # Record that thisis an input to the step.
            step.inputs.append(
                WorkflowStepInput(input_id=inp.id, source=name)
            )

        # Also record that we want all the pipeline outputs
        for tag, ftype in stage.outputs:
            # Record the expected output for this tag
            step.out.append(tag)

            # Also record that each file is an output to the entire pipeline
            cwl_out = WorkflowOutputParameter(tag, f'{step.id}/{tag}',
                                              label=tag,
                                              param_type='File',
                                              param_format=ftype.__name__)
            workflow.outputs.append(cwl_out)

        # Also capture stdout and stderr as outputs
        cwl_out = WorkflowOutputParameter(f'{step.id}@stdout',
                                          output_source=f'{step.id}/{step.id}@stdout',
                                          label='stdout',
                                          param_type='File')
        step.out.append(f'{step.id}@stdout')
        workflow.outputs.append(cwl_out)

        cwl_out = WorkflowOutputParameter(f'{step.id}@stderr',
                                         f'{step.id}/{step.id}@stderr',
                                          label='stderr',
                                          param_type='File')
        step.out.append(f'{step.id}@stderr')
        workflow.outputs.append(cwl_out)

        # This step is now ready - add it to the list
        workflow.steps.append(step)

        # In CWL our data elemnts dict just records which step each
        # output is made in
        return {tag: step.id for tag in stage.output_tags()}


    def run_jobs(self, run_info, run_config):
        workflow = run_info['workflow']
        cwl_dir = run_info['cwl_dir']
        output_dir = run_config['output_dir']
        log_dir = run_config['log_dir']
        inputs_file = run_info['inputs_file']
        workflow.export(f'{cwl_dir}/pipeline.cwl')

        # If 'launcher' is defined, use that
        launcher = self.launcher_config.get('launch',
                                            f'cwltool --outdir {output_dir} '
                                             '--preserve-environment PYTHONPATH')
        if launcher == 'cwltool':
            launcher = f'cwltool --outdir {output_dir} ' \
                        '--preserve-environment PYTHONPATH'

        if launcher:
            # need to include the CWD on the path for CWL as it
            # runs in an isolated directory
            pypath = os.environ.get("PYTHONPATH", "")
            os.environ["PYTHONPATH"] = pypath + ":" + os.getcwd()
            cmd = f'{launcher} {cwl_dir}/pipeline.cwl {inputs_file}'
            print(cmd)
            status = os.system(cmd)
            if pypath:
                os.environ['PYTHONPATH'] = pypath
            else:
                del os.environ['PYTHONPATH']

        # Parsl insists on putting everything in the same output directory,
        # both logs and file outputs.
        # we need to move those

        if status == 0:
            for step in run_info['workflow'].steps:
                shutil.move(f'{output_dir}/{step.id}.out', f'{log_dir}/{step.id}.out')
                shutil.move(f'{output_dir}/{step.id}.err', f'{log_dir}/{step.id}.err')


        return status