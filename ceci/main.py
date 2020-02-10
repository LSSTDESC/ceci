import os
import yaml
import sys
import parsl
import argparse
from . import pipeline
from . import sites as sites_module

# Add the current dir to the path - often very useful
sys.path.append(os.getcwd())

parser = argparse.ArgumentParser(description='Run a Ceci pipeline from a configuration file')
parser.add_argument('pipeline_config', help='Pipeline configuration file in YAML format.')
parser.add_argument('--dry-run', action='store_true', help='Just print out the commands the pipeline would run without executing them')
parser.add_argument('extra_config', nargs='*', help='Over-ride the main pipeline yaml file e.g. launcher.name=cwl')

def run(pipeline_config_filename, extra_config=None, dry_run=False):
    """
    Runs the pipeline
    """
    # YAML input file.
    # Load the text and then expand any environment variables
    raw_config_text = open(pipeline_config_filename).read()
    config_text = os.path.expandvars(raw_config_text)
    # Then parse with YAML
    pipe_config = yaml.safe_load(config_text)

    if extra_config:
        override_config(pipe_config, extra_config)

    # parsl execution/launcher configuration information
    launcher_config = pipe_config.get("launcher", {'name':"local"})
    launcher_name = launcher_config['name']


    # Python modules in which to search for pipeline stages
    modules = pipe_config['modules'].split()

    # Required configuration information
    # List of stage names, must be imported somewhere
    stages = pipe_config['stages']

    # 
    site_config = pipe_config.get('site', {'name':'local'})
    sites = sites_module.load(launcher_config, [site_config])

    # Each stage know which site it runs on.  This is to support
    # future work where this varies between stages.
    for stage in stages:
        stage['site'] = sites[0]

    site_info = sites[0].info


    # Inputs and outputs
    output_dir = pipe_config['output_dir']
    inputs = pipe_config['inputs']
    log_dir = pipe_config['log_dir']
    resume = pipe_config['resume']
    stages_config = pipe_config['config']

    for module in modules:
        __import__(module)


    if dry_run:
        pipeline_class = pipeline.DryRunPipeline
    elif launcher_name == 'cwl':
        pipeline_class = pipeline.CWLPipeline
    elif launcher_name == 'parsl':
        pipeline_class = pipeline.ParslPipeline
    elif launcher_name == 'mini':
        pipeline_class = pipeline.MiniPipeline
    else:
        raise ValueError('Unknown pipeline launcher {launcher_name}')

    p = pipeline_class(stages, launcher_config)
    p.run(inputs, output_dir, log_dir, resume, stages_config)

def override_config(config, extra):
    print("Over-riding config parameters from command line:")

    for arg in extra:
        key, value = arg.split('=', 1)
        item = key.split('.')
        p = config
        print(f"    {key}: {value}")

        for x in item[:-1]:
            if x in p:
                p = p[x]
            else:
                p[x] = {}
                p = p[x]
        p[item[-1]] = value



def main():
    args = parser.parse_args()
    run(args.pipeline_config, args.extra_config, dry_run=args.dry_run)

if __name__ == '__main__':
    main()
