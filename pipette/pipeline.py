import parsl
from .base import PipelineStage



class Pipeline:
    def __init__(self, stage_names, launcher_config):
        self.stage_names = stage_names
        self.dfk = parsl.DataFlowKernel(launcher_config)


    def find_outputs(self, stage, outdir):
        return [outdir+out+".txt" for out in stage.outputs]


    def ordered_stages(self, overall_inputs):
        stage_names = self.stage_names[:]
        stages = [PipelineStage.get_stage(stage_name) for stage_name in stage_names]
        known_inputs = list(overall_inputs.keys())
        ordered_stages = []
        n = len(stage_names)
        
        for i in range(n):
            for stage in stages[:]:
                if all(inp in known_inputs for inp in stage.inputs):
                    ordered_stages.append(stage)
                    known_inputs += stage.outputs
                    stages.remove(stage)

        if stages:
            missing = sum([s.inputs for s in stages], [])
            msg = f"""
            Some required inputs to the pipeline could not be found,
            (or possibly your pipeline is cyclic).
            """
            raise ValueError(msg)
        return ordered_stages
            


    def run(self, overall_inputs, output_dir):
        stages = self.ordered_stages(overall_inputs)
        data_elements = overall_inputs.copy()
        for stage in stages:
            app = stage.generate(self.dfk)
            inputs = stage.find_inputs(data_elements)
            outputs = self.find_outputs(stage, output_dir)
            future = app(inputs=inputs, outputs=outputs)
            
            for i,output in enumerate(stage.outputs):
                data_elements[output] = future.outputs[i]

        # Wait for the final result
        future.result()
        return data_elements
