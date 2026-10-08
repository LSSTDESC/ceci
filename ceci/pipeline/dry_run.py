import sys
from .pipeline import Pipeline
from ..utils import embolden


class DryRunPipeline(Pipeline):
    """A pipeline subclass which just does a dry-run, showing which commands
    would be executed.

    See the base class for almost all behaviour.

    No additional attributes.
    """

    def initiate_run(self, overall_inputs):
        return {
            "commands": [],
            "completed_stages": [],
        }

    def should_skip_stage(self, stage):
        return False

    def enqueue_job(self, stage, pipeline_files):
        outputs = stage.find_outputs(self.run_config["output_dir"])
        sec = self.stage_execution_config[stage.instance_name]

        cmd = sec.generate_full_command(pipeline_files, outputs, self.stages_config)

        # Replace the first instance of the stage name with bold
        # text, but only if we are printing to screen. This helps the
        # eye pick out the stage you want to run.
        if sys.stdout.isatty():
            cmd = cmd.replace(stage.instance_name, embolden(stage.instance_name), 1)

        # make a list of complete stages and print at the end
        if self.run_config["resume"] in (True, "resume") and super().should_skip_stage(stage):
            self.run_info["completed_stages"].append(stage.instance_name)

        self.run_info["commands"].append(cmd)
        return outputs

    def run_jobs(self):
        for cmd in self.run_info["commands"]:
            print(cmd)
            print("\n")
        print("\n")
        for name in self.run_info["completed_stages"]:
            print(f"# {name} is complete and could be skipped")
        return 0

    def find_all_outputs(self):
        return {}
