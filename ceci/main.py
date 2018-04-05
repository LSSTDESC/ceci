from . import Pipeline, PipelineStage
import os
import yaml
import sys
import parsl
import argparse

# Add the current dir to the path - often very useful
sys.path.append(os.getcwd())

parser = argparse.ArgumentParser(description='Run a Ceci pipeline from a configuration file')
parser.add_argument('pipeline_config', help='Pipeline configuration file in YAML format.')
parser.add_argument('--export-cwl', type=str, help='Exports pipeline in CWL format to provided path and exits')

def run(pipeline_config_filename):
    """
    Runs the pipeline
    """
    # YAML input file.
    pipe_config = yaml.load(open(pipeline_config_filename))

    # Optional logging of pipeline infrastructure to
    # file.
    log_file = pipe_config.get('pipeline_log')
    if log_file:
        parsl.set_file_logger(log_file)

    # Required configuration information
    # List of stage names, must be imported somewhere
    stages = pipe_config['stages']

    # Python modules in which to search for pipeline stages
    modules = pipe_config['modules'].split()

    # parsl execution/launcher configuration information
    # launcher_config = localIPP
    launcher_config = pipe_config['launcher']

    # Inputs and outputs
    output_dir = pipe_config['output_dir']
    inputs = pipe_config['inputs']
    log_dir = pipe_config['log_dir']
    resume = pipe_config['resume']

    for module in modules:
        __import__(module)

    # Loads an optional configuration file for the pipeline
    if 'config' in inputs:
        stages_config = yaml.load(open(inputs['config']))
    else:
        stages_config = None

    # Create and run pipeline
    pipeline = Pipeline(launcher_config, stages, stages_config)
    pipeline.run(inputs, output_dir, log_dir, resume)

def export_cwl(args):
    """
    Function exports pipeline or pipeline stages into CWL format.
    """
    path = args.export_cwl
    # YAML input file.
    config = yaml.load(open(args.pipeline_config))

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

    pipeline = Pipeline(launcher_config, stages, None)
    cwl_wf = pipeline.generate_cwl(inputs)
    cwl_wf.export(f'{path}/pipeline.cwl')

def main():
    args = parser.parse_args()
    if args.export_cwl is not None:
        export_cwl(args)
    else:
        run(args.pipeline_config)

if __name__ == '__main__':
    main()
