"""Functions to run ceci from the command line"""

import os
import sys
import argparse
import subprocess
from .pipeline import Pipeline
from .sites import load, set_default_site, get_default_site
from .utils import extra_paths
import contextlib

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
    help="Over-ride the main pipeline yaml file e.g. launcher.name=cwl",
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
    with prepare_for_pipeline(pipe_config):
        p = Pipeline.create(pipe_config)
        status = p.run()
    return status


@contextlib.contextmanager
def prepare_for_pipeline(pipe_config):
    """
    Prepare the paths and launcher needed to read and run a pipeline.
    """

    # Later we will add these paths to sys.path for running here,
    # but we will also need to pass them to the sites below so that
    # they can be added within any containers or other launchers
    # that we use
    paths = pipe_config.get("python_paths", [])
    if isinstance(paths, str):  # pragma: no cover
        paths = paths.split()

    # Get information (maybe the default) on the launcher we may be using
    launcher_config = pipe_config.setdefault("launcher", {"name": "mini"})
    site_config = pipe_config.get("site", {"name": "local"})

    # Pass the paths along to the site
    site_config["python_paths"] = paths
    load(launcher_config, [site_config])

    # Python modules in which to search for pipeline stages
    modules = pipe_config.get("modules", "").split()

    # This helps with testing
    default_site = get_default_site()

    # temporarily add the paths to sys.path,
    # but remove them at the end
    with extra_paths(paths):

        # Import modules. We have to do this because the definitions
        # of the stages can be inside.
        for module in modules:
            __import__(module)

        try:
            yield
        finally:
            set_default_site(default_site)


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
