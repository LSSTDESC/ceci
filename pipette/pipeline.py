import parsl
from parsl.data_provider.files import File
from .stage import PipelineStage
import os

class StageExecutionConfig:
    def __init__(self, config):
        self.name = config['name']
        self.nprocess = config.get('nprocess', 1)

class Pipeline:
    def __init__(self, launcher_config, stages):
        self.stage_execution_config = {}
        self.stage_names = []
        self.mpi_command = launcher_config['sites'][0].get('mpi_command', 'mpirun -n')
        self.dfk = parsl.DataFlowKernel(launcher_config)
        for info in stages:
            self.add_stage(info)

    def add_stage(self, stage_info):
        sec = StageExecutionConfig(stage_info)
        self.stage_execution_config[sec.name] = sec
        self.stage_names.append(sec.name)

    def remove_stage(self, name):
        self.stage_names.remove(name)
        del self.stage_execution_config[name]

    def find_outputs(self, stage, outdir):
        return [f'{outdir}/{tag}.{ftype.suffix}' for tag,ftype in stage.outputs]

    def find_inputs(self, stage, data_elements):
        inputs = []
        for inp in stage.input_tags():
            item = data_elements[inp]
            if isinstance(item,str):
                item = File(item)
            inputs.append(item)
        return inputs


    def ordered_stages(self, overall_inputs):
        stage_names = self.stage_names[:]
        stages = [PipelineStage.get_stage(stage_name) for stage_name in stage_names]
        known_inputs = list(overall_inputs.keys())
        ordered_stages = []
        n = len(stage_names)

        for i in range(n):
            for stage in stages[:]:
                if all(inp in known_inputs for inp in stage.input_tags()):
                    ordered_stages.append(stage)
                    known_inputs += stage.output_tags()
                    stages.remove(stage)

        if stages:
            missing_inputs = []
            for stage in stages:
                missing_inputs += [s for s in stage.input_tags() if s not in known_inputs]
            missing_stages = [s.name for s in stages]
            msg = f"""
            Some required inputs to the pipeline could not be found,
            (or possibly your pipeline is cyclic).

            Stages with missing inputs:
            {missing_stages}

            Missing stages:
            {missing_inputs}
            """
            raise ValueError(msg)
        return ordered_stages

    def run(self, overall_inputs, output_dir, log_dir, resume):
        stages = self.ordered_stages(overall_inputs)
        data_elements = overall_inputs.copy()
        futures = []

        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)

        if resume:
            print("Since parameter 'resume' is True we will skip steps whose outputs exist already")

        for stage in stages:
            sec = self.stage_execution_config[stage.name]
            app = stage.generate(self.dfk, sec.nprocess, log_dir, mpi_command=self.mpi_command)
            inputs = self.find_inputs(stage, data_elements)
            outputs = self.find_outputs(stage, output_dir)
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
                futures.append(future)
                for i, output in enumerate(stage.output_tags()):
                    data_elements[output] = future.outputs[i]

        # Wait for the final results, from all files
        for future in futures:
            future.result()

        # Return a dictionary of the resulting file outputs
        return data_elements

    def generate_cwl(self, overall_inputs):
        """
        Exports the pipeline as a CWL object
        """
        import cwlgen
        wf = cwlgen.workflow.Workflow()

        # List all the workflow steps
        stages = self.ordered_stages(overall_inputs)

        known_outputs ={}
        cwl_steps = []
        for stage in stages:
            # Get the CWL tool for that stage
            cwl_tool = stage.generate_cwl()
            cwl_inputs = []

            for i in stage.input_tags():
                if i in known_outputs:
                    src = known_outputs[i]+'/'+i
                else:
                    src=None

                inp = cwlgen.workflow.WorkflowStepInput(id=i, src=src)
                cwl_inputs.append(inp)

            cwl_outputs = []
            for o in stage.outputs:
                cwl_outputs.append(o[0])

            step = cwlgen.workflow.WorkflowStep(stage.name,
                                       inputs=cwl_inputs,
                                       outputs=cwl_outputs,
                                       run='%s.cwl'%cwl_tool.id)

            # Keeping track of known output providers
            for o in stage.outputs:
                known_outputs[o[0]] = step.id

            cwl_steps.append(step)

        wf.steps = cwl_steps

        # TODO: add inputs and outputs for the workflow
        return wf
