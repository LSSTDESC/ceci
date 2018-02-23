from pipette import Pipeline
import pipette_lib.wl_apps
import pathlib
import os
import yaml


def test_pipeline():
    config = yaml.load(open("./test/config.yml"))
    # Required configuration information

    # List of stage names, must be imported somewhere
    stage_names = config['stage_names']

    # parsl execution/launcher configuration information
    launcher_config = config['launcher']

    # Inputs and outputs
    output_dir = config['output_dir']
    inputs = config['inputs']

    # Create and run pipeline
    pipeline = Pipeline(stage_names, launcher_config)
    pipeline.run(inputs, output_dir)


if __name__ == '__main__':
    test_pipeline()
