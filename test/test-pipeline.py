from pipette import Pipeline
import pipette_lib.wl_apps
import pathlib
import os



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