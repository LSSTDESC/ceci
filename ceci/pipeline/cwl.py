import yaml
import os
import shutil
from .pipeline import Pipeline

class CWLPipeline(Pipeline):
    """Export the pipeline as Common Workflow Language files and optionally run it
    with cwltool or another CWL-aware runner.

    """

    @staticmethod
    def make_inputs_file(
        stages, overall_inputs, stages_config, inputs_file
    ):  # pylint: disable=missing-function-docstring

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
            d = {"class": "File", "path": filepath, "format": ftype.format}
            inputs[tag] = d

        # CWL also wants the config information passed through in an inputs
        # file, so it is all collected together.
        with open(stages_config) as _stages_config_file:
            stage_config_data = yaml.safe_load(_stages_config_file)
        global_config = stage_config_data.pop("global", {})

        # For each stage, we check if any of its config information
        # is set in the config file
        for stage in stages:
            # There might be nothing if no options are needed.
            this_stage_config = stage_config_data.get(stage.instance_name, {})
            # Record only keys that have been set.  If any are missing
            # it is an error that will be noticed later.
            for key in stage.config_options:
                val = this_stage_config.get(key, global_config.get(key))
                if val is not None:
                    inputs[f"{key}@{stage.instance_name}"] = val

        inputs["config"] = {
            "class": "File",
            "path": os.path.abspath(stages_config),
            # YAML file indicator:
            "format": "http://edamontology.org/format_3750",
        }

        # Save to the inputs file
        with open(inputs_file, "w") as f:
            yaml.dump(inputs, f)

    def initiate_run(self, overall_inputs):
        from cwl_utils.parser.cwl_v1_0 import Workflow

        wf = Workflow([], [], [], cwlVersion="v1.0")

        cwl_dir = self.launcher_config["dir"]
        os.makedirs(cwl_dir, exist_ok=True)

        # Write the inputs files
        inputs_file = f"{cwl_dir}/cwl_inputs.yml"
        self.make_inputs_file(
            self.stages, overall_inputs, self.stages_config, inputs_file
        )

        # CWL treats overall inputs differently, and requires
        # that we include the config file in there too
        self.overall_inputs["config"] = self.stages_config

        return {
            "workflow": wf,
            "cwl_dir": cwl_dir,
            "inputs_file": inputs_file,
            # keeps track of overall pipeline inputs we have already found
            "workflow_inputs": set(),
            "workflow_outputs": {},
        }

    def enqueue_job(self, stage, pipeline_files):
        from cwl_utils.parser.cwl_v1_0 import WorkflowStep, WorkflowStepInput
        from cwl_utils.parser.cwl_v1_0 import WorkflowOutputParameter, InputParameter

        cwl_dir = self.run_info["cwl_dir"]
        workflow = self.run_info["workflow"]
        log_dir = self.run_config["log_dir"]

        # Create a CWL representation of this step
        cwl_tool = stage.generate_cwl(log_dir)
        with open(f"{cwl_dir}/{stage.instance_name}.cwl", "w") as f:
            yaml.dump(cwl_tool.save(), f)

        # Load that representation again and add it to the pipeline
        step = WorkflowStep(stage.instance_name, [], [], run=f"{cwl_tool.id}.cwl")

        # For CWL these inputs are a mix of file and config inputs,
        # so not he same as the pipeline_files we usually see
        for inp in cwl_tool.inputs:

            if inp.id in self.overall_inputs:
                name = inp.id
            # If this input is an putput from an earlier stage
            # then it takes its name based on that
            elif inp.id in pipeline_files:
                name = pipeline_files[inp.id] + "/" + inp.id
            # otherwise if it's a config option we mangle
            # it to avod clashes
            elif inp.id in stage.config_options:
                name = f"{inp.id}@{cwl_tool.id}"
            # otherwise just leave it as-is
            else:  # pragma: no cover
                name = inp.id

            # If it's an overall input to the entire pipeline we
            # record that.  Or a configuration option.
            # And we don't want things that we've already recorded.
            if (
                (inp.id in self.overall_inputs) or (inp.id in stage.config_options)
            ) and (name not in self.run_info["workflow_inputs"]):
                self.run_info["workflow_inputs"].add(name)
                # These are the overall inputs to the enture pipeline.
                # Convert them to CWL input parameters
                cwl_inp = InputParameter(
                    name, label=inp.label, type=inp.type, format=inp.format
                )
                cwl_inp.default = inp.default

                # Bypassing cwlgen type check in case of arrays
                if isinstance(inp.type, dict):
                    cwl_inp.type = inp.type

                # record that these are overall pipeline inputs
                workflow.inputs.append(cwl_inp)

            # Record that thisis an input to the step.
            step.in_.append(WorkflowStepInput(id=inp.id, source=name))

        # Also record that we want all the pipeline outputs
        for tag, ftype in stage.outputs:
            # Record the expected output for this tag
            step.out.append(tag)

            # Also record that each file is an output to the entire pipeline
            cwl_out = WorkflowOutputParameter(
                tag,
                outputSource=f"{step.id}/{tag}",
                type="File",
                format=ftype.__name__,
            )
            workflow.outputs.append(cwl_out)

        # Also capture stdout and stderr as outputs
        cwl_out = WorkflowOutputParameter(
            f"{step.id}@stdout",
            outputSource=f"{step.id}/{step.id}@stdout",
            label="stdout",
            type="File",
        )
        step.out.append(f"{step.id}@stdout")
        workflow.outputs.append(cwl_out)

        cwl_out = WorkflowOutputParameter(
            f"{step.id}@stderr",
            outputSource=f"{step.id}/{step.id}@stderr",
            type="File",
        )
        step.out.append(f"{step.id}@stderr")
        workflow.outputs.append(cwl_out)

        # This step is now ready - add it to the list
        workflow.steps.append(step)

        # In CWL our data elemnts dict just records which step each
        # output is made in
        return {tag: step.id for tag in stage.output_tags()}

    def run_jobs(self):
        workflow = self.run_info["workflow"]
        cwl_dir = self.run_info["cwl_dir"]
        output_dir = self.run_config["output_dir"]
        log_dir = self.run_config["log_dir"]
        inputs_file = self.run_info["inputs_file"]

        with open(f"{cwl_dir}/pipeline.cwl", "w") as f:
            yaml.dump(workflow.save(), f)

        # If 'launcher' is defined, use that
        launcher = self.launcher_config.get(
            "launch",
            f"cwltool --outdir {output_dir} " "--preserve-environment PYTHONPATH",
        )
        if launcher == "cwltool":  # pragma: no cover
            launcher = (
                f"cwltool --outdir {output_dir} " "--preserve-environment PYTHONPATH"
            )

        if launcher:
            # need to include the CWD on the path for CWL as it
            # runs in an isolated directory
            pypath = os.environ.get("PYTHONPATH", "")
            os.environ["PYTHONPATH"] = pypath + ":" + os.getcwd()
            cmd = f"{launcher} {cwl_dir}/pipeline.cwl {inputs_file}"
            print(cmd)
            status = os.system(cmd)
            if pypath:  # pragma: no cover
                os.environ["PYTHONPATH"] = pypath
            else:
                del os.environ["PYTHONPATH"]

        # Parsl insists on putting everything in the same output directory,
        # both logs and file outputs.
        # we need to move those

        if status == 0:
            for step in self.run_info["workflow"].steps:
                shutil.move(f"{output_dir}/{step.id}.out", f"{log_dir}/{step.id}.out")
                shutil.move(f"{output_dir}/{step.id}.err", f"{log_dir}/{step.id}.err")

        return status

