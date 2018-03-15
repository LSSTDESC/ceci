from . import Pipeline, PipelineStage
import os
import yaml
import sys
import parsl
import argparse

parser = argparse.ArgumentParser(description='Run a pipette pipeline from a configuration file')
parser.add_argument('config_filename', help='Configuration file in YAML format.')
parser.add_argument('--export_cwl', type=str, help='Exports pipeline in CWL format to provided path and exits')

def run(config_filename):
    """
    Runs the pipeline
    """
    # YAML input file.
    config = yaml.load(open(config_filename))

    # Optional logging of pipeline infrastructure to
    # file.
    log_file = config.get('pipeline_log')
    if log_file:
        parsl.set_file_logger(log_file)

    # Required configuration information
    # List of stage names, must be imported somewhere
    stages = config['stages']

    # Python modules in which to search for pipeline stages
    modules = config['modules'].split()

    # parsl execution/launcher configuration information
    # launcher_config = localIPP
    launcher_config = config['launcher']

    # Inputs and outputs
    output_dir = config['output_dir']
    inputs = config['inputs']
    log_dir = config['log_dir']
    resume = config['resume']

    for module in modules:
        __import__(module)

    # Create and run pipeline
    pipeline = Pipeline(launcher_config, stages)
    pipeline.run(inputs, output_dir, log_dir, resume)

def export_cwl(args):
    """
    Function exports pipeline or pipeline stages into CWL format.
    """
    path = args.export_cwl
    # YAML input file.
    config = yaml.load(open(args.config_filename))

    # Python modules in which to search for pipeline stages
    modules = config['modules'].split()
    for module in modules:
        __import__(module)

    # Export each pipeline stage as a CWL app
    for k in PipelineStage.pipeline_stages:
        tool = PipelineStage.pipeline_stages[k][0].generate_cwl()
        tool.export(f'{path}/{k}.cwl')

    # Exports the pipeline itself
    launcher_config = config['launcher']
    stages = config['stages']
    inputs = config['inputs']
    pipeline = Pipeline(launcher_config, stages)
    cwl_wf = pipeline.generate_cwl(inputs)
    cwl_wf.export(f'{path}/pipeline.cwl')

def main():
    args = parser.parse_args()
    if args.export_cwl is not None:
        export_cwl(args)
    else:
        run(args.config_filename)

if __name__ == '__main__':
    main()
