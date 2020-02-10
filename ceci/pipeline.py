import parsl
from parsl.data_provider.files import File
from .stage import PipelineStage
from . import minirunner
import os
import sys
import time
import yaml

class StageExecutionConfig:
    """
    The SEC stores information describing how an individual job is to be executed,
    for example how many cores it is run on, and where.  It does not store the
    job input or output information or anything like that.

    Possibly we should be attaching this object to the stage itself.

    Attributes
    ----------

    name: str
        The name of the stage
    site: Site object
        The site this stage is run on
    nprocess: int
        The number of (usually MPI) processes to use for this task
    nodes: int
        The number of nodes the processes should be spread over
    threads_per_process: int
        The number of (usually OpenMP) threads to use per process.
    mem_per_process: float
        The amount of memory in GB required for the job
    image: str
        A docker image name to use for this task
    volume: str
        Any volume mappings in the form /path/on/host:/path/on/container
        that the job needs
    """
    def __init__(self, info):

        # Core attributes - mandatory
        self.name = info['name']
        self.site = info['site']

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

        Parameters
        ----------
        stage_info: dict
            Configuration information for this stage.

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

    def find_outputs(self, stage, outdir):
        """Get the names of all the outputs a stage will generate.

        The name is determined from the output tag, the type of the file,
        and the directory it will be put into.

        Since different pipelines treat input files differently
        the corresponding find_inputs methods are defined in subclasses,
        where needed.

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
        return [f'{outdir}/{ftype.make_name(tag)}' for tag,ftype in stage.outputs]



    def ordered_stages(self, overall_inputs):
        """Produce a linear ordering for the stages.

        Some stages within the pipeline might be ruunnable in parallel; this
        method does not analyze this, since it different workflow managers will
        treat this differently.

        The stages in the pipeline are also checked for consistency, to avoid
        circular pipelines (A->B->C->A) and to ensure that all overall inputs
        needed in the pipeline are supplied from the overall inputs.

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
        all_stages = stages[:]

        # List of tags of inputs that we have already generated.
        # this will be appended to as we deternine new stages can be added
        known_inputs = list(overall_inputs.keys())
        ordered_stages = []
        n = len(stage_names)

        # Loop through the remaining un-listed stages and check if each
        # has all its inputs and so can be added to the list.  We do this
        # at most n_stage times, which is the worst-case scenario for this
        # ordering.  There's probably a faster algorithm, but this is not
        # a slow operation.
        for i in range(n):
            # we make a copy of stages here so we will not be modifying it
            # while it is being iterated through.
            for stage in stages[:]:
                if all(inp in known_inputs for inp in stage.input_tags()):
                    # add this stage to the list and also note that we now
                    # have its outputs for future stages.
                    ordered_stages.append(stage)
                    known_inputs += stage.output_tags()
                    stages.remove(stage)

        # If any stages are still not in the list then there is a problem.
        # Try to diagnose it here.
        if stages:
            all_outputs = sum((s.output_tags() for s in all_stages), [])
            missing_inputs = [t for s in all_stages for t in s.input_tags() if t not in all_outputs and t not in overall_inputs]

            if not missing_inputs:
                msg = """
                The pipeline you have written is circular!

                (Some outputs from the overall pipeline are also inputs to it.)
                """
            else:
                stages_requiring_missing_inputs = {
                    m: ", ".join([s.name for s in all_stages if m in s.input_tags()])
                    for m in missing_inputs}


                msg = f"""
                Some required inputs to the pipeline could not be found,
                (or possibly your pipeline is cyclic).

                These inputs are never generated or specified:
                {missing_inputs}

                They are needed by these stages:
                {stages_requiring_missing_inputs}
                """
            raise ValueError(msg)

        return ordered_stages



    def generate_full_command(self, stage, inputs, config, outdir):
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

        outdir: str:
            Path to directory in which to put outputs

        Returns
        -------
        cmd: str
            Complete command to be executed

        """
        core = stage.generate_command(inputs, config, outdir, missing_inputs_in_outdir=True)
        sec = self.stage_execution_config[stage.name]
        cmd = sec.site.command(core, sec)
        return cmd




class DryRunPipeline(Pipeline):
    """A pipeline subclass which just does a dry-run, showing which commands
    would be executed.

    See the base class for almost all behaviour.

    No additional attributes.
    """
    def run(self, overall_inputs, output_dir, log_dir, resume, stages_config):
        """Dry-run the pipeline and print out all stage command lines

        Some of the arguments here are unused by this sub-class, but are retained
        for consistency with other pipeline subclasses.

        In case useful for scripting, this command also returns a list of all the
        commands it prints.

        Parameters
        ----------
        overall_inputs: dict{str: str}
            Any inputs that do not need to be generated by the pipeline but are
            instead already supplied at the start.  Mapping is from tag -> path.

        output_dir: str
            Path to directory where command would put outputs.
        
        log_dir: str
            Directory in which to write stage log files. Unused.

        resume: bool
            Whether to resume the pipeline and avoid executing stages whose
            outputs already exist.  Unused: all stage commands are printed.
            TODO: Change this.

        stages_config: str
            Path to configuration file for the pipeline stages.

        Returns
        -------
        cmds: list[str]
            A list of all the command line invocations.

        """
        cmds = []
        for stage in self.ordered_stages(overall_inputs):
            sec = self.stage_execution_config[stage.name]
            cmd = self.generate_full_command(stage, overall_inputs, stages_config, output_dir)
            print(cmd)
            print()
            cmd.append(cmd)
        return cmds

class ParslPipeline(Pipeline):
    """A subclass of Pipeline that uses Parsl to manage workflow.
    """

    def find_inputs(self, stage, data_elements):
        """
        """
        inputs = []
        for inp in stage.input_tags():
            item = data_elements[inp]
            if isinstance(item,str):
                item = File(item)
            inputs.append(item)
        return inputs



    def generate_app(self, stage, log_dir, sec):
        """Build a parsl app that wraps this pipeline stage.

        This object is passed onto parsl for execution.
        We do this 

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
        cmd1 = self.generate_full_command(stage, inputs, config, outputs)
        executor = sec.site.info['executor']

        # We will be exec'ing this here.  We can't just define it inline
        # because it just gets too painful with all the i/o names, so instead
        # we build and exec a string.
        template = f"""
@parsl.app.app.bash_app(executors=[executor])
def {stage.name}(inputs, outputs, stdout='{log_dir}/{stage.name}.out', stderr='{log_dir}/{stage.name}.err'):
    cmd = '{cmd1}'.format(inputs=inputs,outputs=outputs)
    print("Launching command:")
    print(cmd, " > {log_dir}/{stage.name}.[out|err]")
    return cmd
"""
        # local variables for creating this function.
        d = {'executor':executor.label, 'cmd1':cmd1}
        exec(template, {'parsl':parsl}, d)

        # Return the function itself.
        return d[stage.name]



    def run(self, overall_inputs, output_dir, log_dir, resume, stages_config):
        """Run the pipeline under Parsl.

        Parameters
        ----------
        overall_inputs: dict{str: str}
            Any inputs that do not need to be generated by the pipeline but are
            instead already supplied at the start.  Mapping is from tag -> path.

        output_dir: str
            Path to directory where pipeline will put outputs.
        
        log_dir: str
            Directory in which to write stage log files.

        resume: bool
            Whether to resume the pipeline and avoid executing stages whose
            outputs already exist.

        stages_config: str
            Path to configuration file for the pipeline stages.

        Returns
        -------
        data_elements: dict{str:str}
            Paths to all output files.

        """
        stages = self.ordered_stages(overall_inputs)
        data_elements = overall_inputs.copy()
        futures = []

        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)

        if resume:
            print("Since parameter 'resume' is True we will skip steps whose outputs exist already")

        for stage in stages:
            sec = self.stage_execution_config[stage.name]
            app = self.generate_app(stage, log_dir, sec)
            inputs = self.find_inputs(stage, data_elements)
            outputs = self.find_outputs(stage, output_dir)
            # All pipeline stages implicitly get the overall configuration file
            inputs.append(File(stages_config))
            already_run_stage = all(os.path.exists(output) for output in outputs)
            # If we are in "resume" mode and the pipeline has already been run
            # then we re-use any existing outputs.  User is responsible for making
            # sure they are complete!
            if resume and already_run_stage:
                print(f"Skipping stage {stage.name} because its outputs exist already")
                for (tag,_),filename in zip(stage.outputs, outputs):
                    data_elements[tag] = filename
            # Otherwise, run the pipeline and register any outputs from the
            # pipe element as a "future" - a file that the pipeline will
            # create later
            else:
                print(f"Pipeline queuing stage {stage.name} with {sec.nprocess} processes")
                future = app(inputs=inputs, outputs=outputs)
                future._ceci_name = stage.name
                futures.append(future)
                for i, output in enumerate(stage.output_tags()):
                    data_elements[output] = future.outputs[i]

        # Wait for the final results, from all files
        for future in futures:
            try:
                # This waits for b/g pipeline completion.
                future.result()
            # Parsl emits this on any non-zero status code.
            except parsl.app.errors.AppFailure:
                stdout_file = f'{log_dir}/{future._ceci_name}.err'
                stderr_file = f'{log_dir}/{future._ceci_name}.out'
                sys.stderr.write(f"""
*************************************************
Error running pipeline stage {future._ceci_name}.

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
                print("Pipeline failed.  No joy sparked.")
                return None

        # Return a dictionary of the resulting file outputs
        print("Pipeline suceeded.  Joy is sparked.")
        return data_elements



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

    def build_jobs(self, stages, overall_inputs, stages_config, output_dir):
        """Build the Minirunner Job objects wrapping the stages

        Parameters
        ----------
        stages: list[PipelineStage]
            The stages to build jobs for.

        overall_inputs: dict{str: str}
            Any inputs that do not need to be generated by the pipeline but are
            instead already supplied at the start.  Mapping is from tag -> path.

        stages_config: str
            Path to configuration file for the pipeline stages.

        output_dir: str
            Path to directory where pipeline will put outputs.

        Returns
        -------
        dict: {str: Job}
            The wrapped jobs
        """
        jobs = {}
        for stage in stages:
            sec = self.stage_execution_config[stage.name]
            cmd = self.generate_full_command(stage, overall_inputs, stages_config, output_dir)
            jobs[stage.name] = minirunner.Job(stage.name, cmd, 
                cores=sec.threads_per_process*sec.nprocess, nodes=sec.nodes)
        return jobs



    def run(self, overall_inputs, output_dir, log_dir, resume, stages_config):
        """Run the pipeline under Minirunner.

        Parameters
        ----------
        overall_inputs: dict{str: str}
            Any inputs that do not need to be generated by the pipeline but are
            instead already supplied at the start.  Mapping is from tag -> path.

        output_dir: str
            Path to directory where pipeline will put outputs.
        
        log_dir: str
            Directory in which to write stage log files.

        resume: bool
            Whether to resume the pipeline and avoid executing stages whose
            outputs already exist.

        stages_config: str
            Path to configuration file for the pipeline stages.
        """

        # copy this as we mutate it later
        overall_inputs = overall_inputs.copy()
        # run using minirunner instead of parsl
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)

        # We just run this to check that the pipeline is valid
        self.ordered_stages(overall_inputs)

        stages = [PipelineStage.get_stage(stage_name) for stage_name in self.stage_names]

        # If we are skipping complete pipeline stages then we do this by
        # adding any completed stage output files to the overall inptus dict
        # and making a new list of incomplete stages.
        if resume:
            stages2 = []
            for stage in stages:
                output_paths = self.find_outputs(stage, output_dir)
                already_run_stage = all(os.path.exists(output) for output in output_paths)
                if already_run_stage:
                    print(f"Stage {stage.name} has already been completed - skipping.")
                    for output_info, output_path in zip(stage.outputs, output_paths):
                        tag = output_info[0]
                        overall_inputs[tag] = output_path
                else:
                    stages2.append(stage)
            stages = stages2

        # Build and order the jobs
        jobs = self.build_jobs(stages, overall_inputs, stages_config, output_dir)
        graph = self.build_dag(stages, jobs)

        # This pipeline, being mainly for testing, can only
        # run at a single site, so we can assume here that the
        # sites are all the same
        sec = self.stage_execution_config[self.stage_names[0]]
        nodes = sec.site.info['nodes']

        # Run under minirununer
        runner = minirunner.Runner(nodes, graph, log_dir)
        interval = self.launcher_config.get('interval', 3)
        runner.run(interval)


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



    def run(self, overall_inputs, output_dir, log_dir, resume, stages_config):
        """Exports the pipeline as a CWL object, and then if a specific CWL tool is chosen, launch it with that.

        Parameters
        ----------
        overall_inputs: dict{str: str}
            Any inputs that do not need to be generated by the pipeline but are
            instead already supplied at the start.  Mapping is from tag -> path.

        output_dir: str
            Path to directory where pipeline will put outputs.
        
        log_dir: str
            Directory in which to write stage log files.

        resume: bool
            Whether to resume the pipeline and avoid executing stages whose
            outputs already exist.

        stages_config: str
            Path to configuration file for the pipeline stages.

        Returns
        -------
        wf: Workflow
            The workflow object

        """
        import cwlgen
        import cwlgen.workflow
        wf = cwlgen.workflow.Workflow()

        # List all the workflow steps and order them
        stages = self.ordered_stages(overall_inputs)

        cwl_dir = self.launcher_config['dir']
        os.makedirs(cwl_dir, exist_ok=True)

        # Write the inputs file
        inputs_file = f'{cwl_dir}/cwl_inputs.yml'
        self.make_inputs_file(stages, overall_inputs, stages_config, inputs_file)

        known_outputs ={}
        workflow_inputs= {}


        cwl_steps = []
        for stage in stages:
            # Get the CWL tool for that stage
            cwl_tool = stage.generate_cwl()
            cwl_tool.export(f'{cwl_dir}/{stage.name}.cwl')

            step = cwlgen.workflowdeps.WorkflowStep(stage.name,
                                   run='%s.cwl'%cwl_tool.id)

            # Loop over the inputs of the tool
            for inp in cwl_tool.inputs:

                # Check if we have encountered this parameter before
                if inp.id in known_outputs:
                    src = known_outputs[inp.id]+'/'+inp.id
                else:
                    # If we haven't seen that parameter before, first check if
                    # it's an option, in which case gave it a special name so
                    # that it's not confused with another pipeline stage
                    if inp.id in stage.config_options:
                        src = f'{inp.id}@{cwl_tool.id}'
                        workflow_inputs[src] = inp
                    else:
                        # Otherwise, treat it as a shared pipeline input
                        src = inp.id
                        if src not in workflow_inputs:
                            workflow_inputs[src] = inp

                step.inputs.append(cwlgen.workflowdeps.WorkflowStepInput(input_id=inp.id, source=src))

            for o in stage.outputs:
                step.out.append(o[0])

            # Keeping track of known output providers
            for o in stage.outputs:
                known_outputs[o[0]] = step.id

            cwl_steps.append(step)

        wf.steps = cwl_steps

        # Export the inputs of the workflow
        for inp in workflow_inputs:
            cwl_inp = cwlgen.workflowdeps.InputParameter(inp,
                                                     label=workflow_inputs[inp].label,
                                                     param_type=workflow_inputs[inp].type,
                                                     param_format=workflow_inputs[inp].format)
            cwl_inp.default = workflow_inputs[inp].default
            # Bypassing cwlgen type check in case of arrays
            if type(workflow_inputs[inp].type) == dict:
                cwl_inp.type = workflow_inputs[inp].type
            wf.inputs.append(cwl_inp)


        # By default only keep the output of the last stage as output
        last_stage = stages[-1]
        for o in last_stage.outputs:
            cwl_out = cwlgen.workflowdeps.WorkflowOutputParameter(o[0], known_outputs[o[0]]+'/'+o[0],
                                                              label=o[0],
                                                              param_type='File',
                                                              param_format=o[1].__name__)
            wf.outputs.append(cwl_out)


        wf.export(f'{cwl_dir}/pipeline.cwl')

        # If 'launcher' is defined, use that
        launcher = self.launcher_config.get('launch', '')

        if launcher:
            cmd = f'{launcher} {cwl_dir}/pipeline.cwl {inputs_file}'
            print(cmd)
            os.system(cmd)

        return wf