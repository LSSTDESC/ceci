import os
import yaml
import sys
import parsl
import argparse
from . import Pipeline, PipelineStage
from . import sites

# Add the current dir to the path - often very useful
sys.path.append(os.getcwd())

parser = argparse.ArgumentParser(description='Run a Ceci pipeline from a configuration file')
parser.add_argument('pipeline_config', help='Pipeline configuration file in YAML format.')
parser.add_argument('--export-cwl', type=str, help='Exports pipeline in CWL format to provided path and exits')
parser.add_argument('--dry-run', action='store_true', help='Just print out the commands the pipeline would run without executing them')

def run(pipeline_config_filename, dry_run=False):
    """
    Runs the pipeline
    """
    # YAML input file.
    # Load the text and then expand any environment variables
    raw_config_text = open(pipeline_config_filename).read()
    config_text = os.path.expandvars(raw_config_text)
    # Then parse with YAML
    pipe_config = yaml.load(config_text)

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
    launcher = pipe_config.get("launcher", "local")
    if launcher == "local":
        launcher_config = sites.local.make_launcher(stages)
    elif launcher == "cori":
        launcher_config = sites.cori.make_launcher(stages)
    elif launcher == "cori-interactive":
        launcher_config = sites.cori_interactive.make_launcher(stages)
    else:
        raise ValueError(f"Unknown launcher {launcher}")
    # 
    # launcher_config = pipe_config['launcher']

    # Inputs and outputs
    output_dir = pipe_config['output_dir']
    inputs = pipe_config['inputs']
    log_dir = pipe_config['log_dir']
    resume = pipe_config['resume']

    stages_config = pipe_config['config']

    for module in modules:
        __import__(module)

    # Create and run pipeline
    pipeline = Pipeline(launcher_config, stages)

    if dry_run:
        pipeline.dry_run(inputs, output_dir, stages_config)
    else:
        pipeline.run(inputs, output_dir, log_dir, resume, stages_config)

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

    stages = config['stages']

    # Exports the pipeline itself
    launcher = config.get("launcher", "local")
    if launcher == "local":
        launcher_config = sites.local.make_launcher(stages)
    elif launcher == "cori":
        launcher_config = sites.cori.make_launcher(stages)
    else:
        raise ValueError(f"Unknown launcher {launcher}")

    inputs = config['inputs']

    pipeline = Pipeline(launcher_config, stages)
    cwl_wf = pipeline.generate_cwl(inputs)
    cwl_wf.export(f'{path}/pipeline.cwl')

def main():
    args = parser.parse_args()
    if args.export_cwl is not None:
        export_cwl(args)
    else:
        run(args.pipeline_config, dry_run=args.dry_run)

if __name__ == '__main__':
    main()
