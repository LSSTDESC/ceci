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


    # Python modules in which to search for pipeline stages
    modules = pipe_config['modules'].split()

    # parsl execution/launcher configuration information
    site = pipe_config.get("launcher", "local")

    # Required configuration information
    # List of stage names, must be imported somewhere
    stages = pipe_config['stages']

    # Each stage know which site it runs on.  This is to support
    # future work where this varies between stages.
    for stage in stages:
        stage['site'] = site
        

    # For now we require that only one site is used.
    # TODO: add support for multiple sites via parsl
    if site == "local":
        # Threads on a local machine, for testing
        executor_labels, mpi_command = sites.local.activate()
    elif site == "cori-interactive":
        # Still threads, but in a cori interactive job
        executor_labels, mpi_command = sites.cori_interactive.activate()
    elif site == "cori":
        # On cori, submits pilot jobs which receive tasks
        site_config = pipe_config.get('site', {})
        cpu_type = site_config.get('cpu_type', 'haswell')
        queue = site_config.get('queue', 'debug')
        max_slurm_jobs = site_config.get('max_jobs', 2)
        setup_script = site_config.get('setup', '/global/projecta/projectdirs/lsst/groups/WL/users/zuntz/setup-cori')
        executor_labels, mpi_command = sites.cori.activate(queue, max_slurm_jobs, setup_script, cpu_type)
    else:
        raise ValueError(f"Unknown site {site}")

    # Inputs and outputs
    output_dir = pipe_config['output_dir']
    inputs = pipe_config['inputs']
    log_dir = pipe_config['log_dir']
    resume = pipe_config['resume']

    stages_config = pipe_config['config']

    for module in modules:
        __import__(module)

    # Create and run pipeline
    pipeline = Pipeline(stages, mpi_command)

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
    inputs = config['inputs']

    pipeline = Pipeline(stages, mpi_command)
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
