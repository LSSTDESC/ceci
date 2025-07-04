"""Functions to run ceci from the command line"""

import os
import sys
import argparse
from .pipeline import Pipeline
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
    "extra_config",
    nargs="*",
    help="Over-ride the main pipeline yaml file e.g. launcher.name=parsl",
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



def main():  # pragma: no cover
    """Main function called when ceci is invoked on the command line"""
    args = parser.parse_args()
    # If we are making a flow chart then we also set dry_run to stop
    # pre-scripts and post-scripts running
    if args.flow_chart:
        args.dry_run = True
    pipe_config = Pipeline.build_config(
        args.pipeline_config, args.extra_config, args.dry_run, args.flow_chart
    )
    status = run_pipeline(pipe_config)

    if status == 0:
        print("Pipeline successful.  Joy is sparked.")
    else:
        print("Pipeline failed.  No joy sparked.")
    return status


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
