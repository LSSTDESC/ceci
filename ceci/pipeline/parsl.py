import sys
import os

from .pipeline import Pipeline


class ParslPipeline(Pipeline):
    """A subclass of Pipeline that uses Parsl to manage workflow."""

    def initiate_run(self, overall_inputs):
        return []  # list of futures

    def enqueue_job(self, stage, pipeline_files):

        from parsl.data_provider.files import File

        # log_dir = self.run_config["log_dir"]
        # convert the command into an app
        app = self.generate_app(stage, self.run_config)

        # Convert the dicts of inputs/outputs to the list that
        # parsl wants.
        # The inputs that exist already need to be converted into Parsl File objects.
        # The ones that don't stay as data futures
        inputs1 = stage.find_inputs(pipeline_files)
        inputs = [
            File(val) if isinstance(val, str) else val for val in inputs1.values()
        ]
        inputs.append(File(self.stages_config))
        # The outputs are just strings.  python dicts are now ordered,
        # so this works okay.
        outputs = [
            File(f) for f in stage.find_outputs(self.run_config["output_dir"]).values()
        ]

        # have parsl queue the app
        future = app(inputs=inputs, outputs=outputs)
        self.run_info.append((stage.instance_name, future))
        return {stage.get_aliased_tag(tag): future.outputs[i] for i, tag in enumerate(stage.output_tags())}

    def run_jobs(self):
        from parsl.app.errors import BashExitFailure

        log_dir = self.run_config["log_dir"]
        # Wait for the final results, from all files
        for stage_name, future in self.run_info:
            try:
                # This waits for b/g pipeline completion.
                future.result()
            # Parsl emits this on any non-zero status code.
            except BashExitFailure:
                stdout_file = f"{log_dir}/{stage_name}.err"
                stderr_file = f"{log_dir}/{stage_name}.out"
                sys.stderr.write(
                    f"""
*************************************************
Error running pipeline stage {stage_name}.

Standard output and error streams below.

*************************************************

Standard output:
----------------

"""
                )
                if os.path.exists(stdout_file):
                    with open(stdout_file) as _stdout:
                        sys.stderr.write(_stdout.read())
                else:  # pragma: no cover
                    sys.stderr.write("STDOUT MISSING!\n\n")

                sys.stderr.write(
                    """
*************************************************

Standard error:
----------------

"""
                )

                if os.path.exists(stderr_file):
                    with open(stderr_file) as _stderr:
                        sys.stderr.write(_stderr.read())
                else:  # pragma: no cover
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
        module = module.split(".")[0]

        inputs = {}
        outputs = {}

        # Parsl wants a function with the inputs and outputs
        # extracted from lists.  That function should iself return
        # a string representing the bash cmd line.
        # We build up all these components here.

        # Parsl wants our functions to take their input/output paths
        # from inputs[0], inputs[1], etc.
        for i, inp in enumerate(stage.input_tags()):
            inp = stage.get_aliased_tag(inp)
            inputs[inp] = f"{{inputs[{i}]}}"
        for i, out in enumerate(stage.output_tags()):
            out = stage.get_aliased_tag(out)
            outputs[out] = f"{{outputs[{i}]}}"

        # The last input file is always the config file
        config_index = len(stage.input_tags())
        config = f"{{inputs[{config_index}]}}"

        # This includes all the "mpirun" stuff.
        sec = self.stage_execution_config[stage.instance_name]
        executor = sec.site.info["executor"]

        # Construct the command line call
        core = sec.generate_full_command(inputs, outputs, config)
        cmd1 = sec.site.command(core, sec)
        log_dir = run_config["log_dir"]

        # We will be exec'ing this here.  We can't just define it inline
        # because it just gets too painful with all the i/o names, so instead
        # we build and exec a string.
        template = f"""
@parsl.app.app.bash_app(executors=[executor])
def {stage.instance_name}(inputs, outputs, stdout='{log_dir}/{stage.instance_name}.out', stderr='{log_dir}/{stage.instance_name}.err'):
    cmd = '{cmd1}'.format(inputs=inputs, outputs=outputs)
    print("Launching command:")
    print(cmd, " 2> {log_dir}/{stage.instance_name}.err 1> {log_dir}/{stage.instance_name}.out")
    return cmd
"""
        print(template)

        # local variables for creating this function.
        d = {"executor": executor.label, "cmd1": cmd1}
        exec(template, {"parsl": parsl}, d)  # pylint: disable=exec-used

        # Return the function itself.
        return d[stage.instance_name]


