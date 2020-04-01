import os
import yaml
import sys
import parsl
import argparse
import subprocess
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
    Runs the pipeline.

    The pipeline takes a main configuration file, which specifies
    which stages are to be run, and how, where to look for them, what overall
    inputs there are to the pipeline, and where to find a file configuring
    the individual stages. See test.yml for an example.

    The extra_config argument lets you override parameters in the config file,
    as long as they are stored in dictionaries.
    For example, if the config file contains:
    launcher:
        name: x

    Then you could include "launcher.name=y" to change this to "y".

    Parameters
    ----------
    pipeline_config_filename: str
        The path to the configuration file
    extra_config: list[str]
        Config parameters to override
    dry_run: bool
        Whether to do a dry-run of the pipeline, not running anything.
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

    # Pre- and post-scripts are run locally
    # before and after the pipeline is complete
    # They are called with the same arguments as
    # this script.  If the pre_script returns non-zero
    # then the pipeline is not run.
    # These scripts ar not run in the dry-run case.
    # In both cases we pass the script the pipeline_config
    # filename and any extra args.
    pre_script = pipe_config.get('pre_script')
    post_script = pipe_config.get('post_script')
    script_args = [pipeline_config_filename]
    if extra_config:
        script_args += extra_config

    for module in modules:
        __import__(module)


    # Choice of actual pipeline type to run
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

    # Run the pre-script.  Since it's an error for this to fail (because
    # it is useful as a validation check) then we raise an error if it
    # fails using check_call.
    if pre_script and not args.dry_run:
        subprocess.check_call(pre_script.split() + script_args, shell=True)

    # Create and run the pipeline
    p = pipeline_class(stages, launcher_config)
    p.run(inputs, output_dir, log_dir, resume, stages_config)

    # Run the post-script.  There seems less point raising an actual error
    # here, as the pipeline is complete, so we just issue a warning and
    # return a status code to the caller (e.g. to the command line).
    # Thoughts on this welcome.
    if post_script and not args.dry_run:
        return_code = subprocess.call(post_script.split() + script_args, shell=True)
        if return_code:
            sys.stderr.write(f"\nWARNING: The post-script command {post_script} "
                              "returned error status {return_code}\n\n")
        return return_code
    # Otherwise everything must have gone fine.
    else:
        return 0


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
    status = run(args.pipeline_config, args.extra_config, dry_run=args.dry_run)
    return status

if __name__ == '__main__':
    sys.exit(main())
