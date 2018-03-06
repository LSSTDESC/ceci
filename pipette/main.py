from . import Pipeline
import os
import yaml
import sys
import parsl
import argparse

parser = argparse.ArgumentParser(description='Run a pipette pipeline from a configuration file')
parser.add_argument('config_filename', help='Configuration file in YAML format.')

def run(config_filename):
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



def main(args):
    run(args.config_filename)

if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
