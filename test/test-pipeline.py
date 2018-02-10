import parsl
import pipette
import pipette_lib.wl_apps
from parsl.data_provider.files import File
import pathlib
import os


def find_inputs(stage, data_elements):
    inputs = []
    for inp in stage.inputs:
        item = data_elements[inp]
        if isinstance(item,str):
            item = File(item)
        inputs.append(item)
    return inputs

def find_outputs(outdir, stage):
    return [outdir+out+".txt" for out in stage.outputs]



class Pipeline:
    def __init__(self, stage_names):
        self.stage_names = stage_names
        workers = parsl.ThreadPoolExecutor(max_workers=4)
        self.dfk = parsl.DataFlowKernel(executors=[workers])

    def ordered_stages(self, overall_inputs):
        stage_names = self.stage_names[:]
        stages = [pipette.PipelineStage.get_stage(stage_name) for stage_name in stage_names]
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
            These overall inputs to the pipeline could not be found,
            (or possibly your pipeline is cyclic):

            {missing}
            """
            raise ValueError(msg)
        return ordered_stages
            


    def run(self, overall_inputs, output_dir):
        stages = self.ordered_stages(overall_inputs)
        data_elements = overall_inputs.copy()
        for stage in stages:
            app = stage.generate_bash(self.dfk)
            inputs = find_inputs(stage, data_elements)
            outputs = find_outputs(output_dir, stage)
            future = app(inputs=inputs, outputs=outputs)
            for i,output in enumerate(stage.outputs):
                data_elements[output] = future.outputs[i]

        # Wait for the final result
        future.result()


def main():
    stage_names = [
         'WLGCSummaryStatistic','SysMapMaker',
         'shearMeasurementPipe', 'PZEstimationPipe',
        'WLGCRandoms', 'WLGCSelector', 'SourceSummarizer', 
        'WLGCTwoPoint', 'WLGCCov',
    ]

    pipeline = Pipeline(stage_names)
    output_dir = './test/outputs/'
    overall_inputs = {
        "DM": "./test/inputs/dm.txt",
        "fiducial_cosmology": "./test/inputs/fiducial_cosmology.txt",
    }
    pipeline.run(overall_inputs, output_dir)


if __name__ == '__main__':
    main()