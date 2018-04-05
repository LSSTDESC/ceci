from . import Pipeline, PipelineStage
import os
import yaml
import sys
import parsl
import argparse

CECI_CENTRAL_USER='EiffL'
CECI_CENTRAL_REPO=f'git@github.com:{CECI_CENTRAL_USER}/ceci-central.git'
CECI_CENTRAL_DOCKER=f'{CECI_CENTRAL_USER}/ceci-base'

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

def push_module(args):
    """
    This function exports the definition of a module as CWL files along with a
    Dockerfile image
    """
    import git
    import tempfile
    import datetime

    module="ceci_example"

    # First introspect the current git repository to extract some info
    r = git.Repo(search_parent_directories=True)
    config = r.config_reader()
    user = config.get_value("user","name")
    email = config.get_value("user","email")
    remote = next(r.remote().urls)
    hexsha = r.head.object.hexsha

    # Now create a temporary folder to do the rest of the work
    with tempfile.TemporaryDirectory() as tmpdir:

        print("Retrieving local copy of ceci-central")
        repo = git.Repo.clone_from(CECI_CENTRAL_REPO, tmpdir)
        # Checkout branch associated with module
        repo.git.checkout(f'{module}')
        repo.git.pull()

        print("Exporting Dockerfile for current pipeline module")
        # First step, creating the Dockerfile for that particular revision of the
        # git repository
        dockerfile=f"""# Ceci n'est pas un Dockerfile
# Created on {datetime.datetime.now()}
FROM {CECI_CENTRAL_DOCKER}
MAINTAINER {user} <{email}>

RUN pip install git+{remote}@{hexsha}
        """
        with open(f'{tmpdir}/tools/{module}.docker', 'w') as f:
            f.write(dockerfile)
        repo.git.add(f'{tmpdir}/tools/{module}.docker')

        # Second step, export each pipeline element as a CWL tool
        __import__(module)
        print("Exporting CWL description of each pipeline stage")
        for k in PipelineStage.pipeline_stages:
            tool = PipelineStage.pipeline_stages[k][0].generate_cwl()
            tool.export(f'{tmpdir}/tools/{k}.cwl')
            repo.git.add(f'{tmpdir}/tools/{k}.cwl')

        # Now commit these changes and push them to ceci-central on a new branch
        # TODO: Check for need to commit
        repo.git.commit('-m "Modifies pipeline elements"')
        repo.git.push("origin", f"{module}")

        # Create pull request
        repo.git.request_pull("master", CECI_CENTRAL_REPO, f'{module}')

def main():
    args = parser.parse_args()
    push_module(args)
    # if args.export_cwl is not None:
    #     export_cwl(args)
    # else:
    #     run(args.pipeline_config)

if __name__ == '__main__':
    main()
