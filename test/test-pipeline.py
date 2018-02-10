import parsl
import pipette
import pipette_lib.wl_apps
from parsl.data_provider.files import File
import pathlib
import os


def find_inputs(stage, overall_inputs, pending_inputs):
    inputs = []
    for inp in stage.inputs:
        if inp in overall_inputs:
            inp_source = File(overall_inputs[inp])
        else:
            inp_source = pending_inputs[inp]
        inputs.append(inp_source)
    return inputs

def find_outputs(outdir, stage):
    return [outdir+out+".txt" for out in stage.outputs]


def main():
    parsl.set_stream_logger()
    workers = parsl.ThreadPoolExecutor(max_workers=4)
    dfk = parsl.DataFlowKernel(executors=[workers])

    outdir = './test/outputs/'
    os.makedirs(outdir, exist_ok=True)

    pending_inputs = {}
    all_outputs = {}

    overall_inputs = {
        "DM": "./test/inputs/dm.txt",
        "fiducial_cosmology": "./test/inputs/fiducial_cosmology.txt",
    }


    stage_names = [
        'shearMeasurementPipe', 'PZEstimationPipe','SysMapMaker',
        'WLGCRandoms', 'WLGCSelector', 'SourceSummarizer', 
        'WLGCTwoPoint', 'WLGCCov', 'WLGCSummaryStatistic'
    ]

    for stage_name in stage_names:
        stage = pipette.PipelineStage.get_stage(stage_name)
        app = stage.generate_bash(dfk)
        inputs = find_inputs(stage, overall_inputs, pending_inputs)
        outputs = find_outputs(outdir, stage)
        future = app(inputs=inputs, outputs=outputs)
        for i,output in enumerate(stage.outputs):
            pending_inputs[output] = future.outputs[i]
    future.result()
    print("")
    print("")
    print("Generated these overall outputs:")
    for output in pending_inputs:
        print("   ", output)
    print("")
# class Pipeline:
#     def __init__(self, stage_names):
#         self.stage_names = stage_names

#     def ordered_stages(self, overall_inputs):
#         stage_names = self.stage_names[:]
#         stage = [pipette.PipelineStage.get_stage(stage_name) for stage_name in stage_names]
#         known_inputs = list(overall_inputs.keys())
#         ordered_stages = []
        
#         while True:
#             for stage in stages:
#                 if all(inp in known_inputs for inp in )
#         for stage_name in stage_names:
            


#     def run(self, overall_inputs, output_dir):
#         stages = self.ordered_stages()

#         for stage in stages:
            
#             app = stage.generate_bash(dfk)
#             inputs = find_inputs(stage, overall_inputs, pending_inputs)
#             outputs = find_outputs(stage)
#             future = app(inputs=inputs, outputs=outputs)
#             for i,output in enumerate(stage.outputs):
#                 pending_inputs[output] = future.outputs[i]

#         # Wait for the final result
#         future.result()
if __name__ == '__main__':
    main()