"""Functions to run ceci from the command line"""

import os
import sys
import argparse
import yaml
from .pipeline import Pipeline
from .pipeline.register import RegistrationError, DEFAULT_VERSION
from ._version import __version__

# Add the current dir to the path - often very useful
sys.path.append(os.getcwd())

parser = argparse.ArgumentParser(
    description="Run a Ceci pipeline from a configuration file"
)
parser.add_argument(
    "pipeline_config", help="Pipeline configuration file in YAML format."
)
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Just print out the commands the pipeline would run without executing them",
)
parser.add_argument(
    "--flow-chart",
    type=str,
    default="",
    help="Make a flow chart image instead of running anything",
)
parser.add_argument(
    "--register",
    type=str,
    default="",
    metavar="EXECUTION_NAME",
    help="Instead of running the pipeline, register the outputs of an already-complete "
    "pipeline in the data registry as a single execution. "
    "Datasets are named <EXECUTION_NAME>_<tag>",
)
parser.add_argument(
    "--dregs-version",
    type=str,
    default=DEFAULT_VERSION,
    help="Data registry version for --register (default %(default)s)",
)
parser.add_argument(
    "--dregs-allow-large",
    action="store_true",
    help="Allow --register to register more than 50GB",
)
parser.add_argument(
    "--dregs-incomplete",
    action="store_true",
    help="Allow --register to go ahead, registering only the outputs that exist, "
    "if some pipeline outputs are missing",
)
parser.add_argument(
    "--dregs-more",
    action="store_true",
    help="Allow --register to add more outputs to an execution that was already "
    "registered: outputs already registered are skipped, new ones are added to "
    "the existing execution",
)
parser.add_argument(
    "--dregs-dry-run",
    action="store_true",
    help="Show what --register would register without registering it",
)
parser.add_argument(
    "extra_config",
    nargs="*",
    help="Over-ride the main pipeline yaml file e.g. launcher.name=parsl",
)

parser.add_argument(
    "-t",
    "--template-parameters",
    nargs="*",
    help="Template parameters to use in the pipeline configuration file, e.g. -t some_directory=xyz field=north",
    default="",
)

parser.add_argument(
    "-v",
    "--version",
    action="version",
    version=__version__,
    help="Print the ceci version instead of running anything",
)



def run_pipeline(pipe_config):
    """Run a pipeline as defined by a particular configuration

    Parameters
    ----------
    pipe_config : `dict`
        The configuration dictionary

    Returns
    -------
    status : `int`
        Usual unix convention of 0 -> success, non-zero is an error code
    """
    p = Pipeline.create(pipe_config)
    status = p.run()
    return status



def register_pipeline(pipe_config, args):
    """Register the outputs of an existing pipeline in the data registry

    Returns
    -------
    status : `int`
        0 on success, 1 if registration was refused or failed
    """
    p = Pipeline.create(pipe_config)
    configuration = {args.pipeline_config: yaml.dump(pipe_config, sort_keys=False)}
    stage_config = pipe_config.get("config")
    if isinstance(stage_config, str):
        with open(stage_config) as f:
            configuration[stage_config] = f.read()
    try:
        p.register_outputs(
            args.register,
            version=args.dregs_version,
            allow_large=args.dregs_allow_large,
            allow_incomplete=args.dregs_incomplete,
            more=args.dregs_more,
            dry_run=args.dregs_dry_run,
            configuration=configuration,
        )
    except RegistrationError as error:
        print(f"Not registering: {error}")
        return 1
    return 0


def main():  # pragma: no cover
    """Main function called when ceci is invoked on the command line"""
    args = parser.parse_args()
    if args.register:
        # Registering never runs anything, so build a dry-run pipeline
        pipe_config = Pipeline.build_config(
            args.pipeline_config, args.extra_config, True, "",
            args.template_parameters
        )
        return register_pipeline(pipe_config, args)
    if args.dregs_dry_run or args.dregs_incomplete or args.dregs_allow_large or args.dregs_more:
        parser.error("--dregs-* flags can only be used with --register")
    # If we are making a flow chart then we also set dry_run to stop
    # pre-scripts and post-scripts running
    if args.flow_chart:
        args.dry_run = True
    pipe_config = Pipeline.build_config(
        args.pipeline_config, args.extra_config, args.dry_run, args.flow_chart,
        args.template_parameters
    )
    status = run_pipeline(pipe_config)

    if status == 0:
        print("Pipeline successful.  Joy is sparked.")
    else:
        print("Pipeline failed.  No joy sparked.")
    return status


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
