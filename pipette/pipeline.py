import parsl
from parsl.data_provider.files import File
from .stage import PipelineStage

class StageExecutionConfig:
    def __init__(self, config):
        self.name = config['name']
        self.nprocess = config.get('nprocess', 1)

class Pipeline:
    def __init__(self, launcher_config, stages):
        self.stage_execution_config = {}
        self.stage_names = []
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
            


    def run(self, overall_inputs, output_dir):
        stages = self.ordered_stages(overall_inputs)
        data_elements = overall_inputs.copy()
        futures = []
        for stage in stages:
            sec = self.stage_execution_config[stage.name]
            print(f"Pipeline queuing stage {stage.name} with {sec.nprocess} processes")
            app = stage.generate(self.dfk, sec.nprocess)
            inputs = self.find_inputs(stage, data_elements)
            outputs = self.find_outputs(stage, output_dir)
            print(inputs, outputs)
            print(app)
            future = app(inputs=inputs, outputs=outputs)
            futures.append(future)
            for i, output in enumerate(stage.output_tags()):
                data_elements[output] = future.outputs[i]
        print(data_elements)
        # Wait for the final result
        for future in futures:
            future.result()
        return data_elements
