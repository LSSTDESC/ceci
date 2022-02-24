"""Functions to run ceci from the command line"""

import os
import sys
import argparse
import subprocess
from .pipeline import Pipeline
from .sites import load, set_default_site, get_default_site
from .utils import extra_paths

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
    "extra_config",
    nargs="*",
    help="Over-ride the main pipeline yaml file e.g. launcher.name=cwl",
)


def run_prescript(pre_script=None, dry_run=False, script_args=None):
    """Run a script before running the pipeline

    Parameters
    ----------
    pre_script : `str`
        The script to run
    dry_run : `bool`
        If true do not run the script
    script_args : `list`, (`str`)
        Arguments to the script
    """
    if script_args is None:  # pragma: no cover
        script_args = []
    if pre_script and not dry_run:
        subprocess.check_call(pre_script.split() + script_args, shell=True)


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
    default_site = get_default_site()
    try:
        p = Pipeline.create(pipe_config)
        status = p.run()
    finally:
        # The create command above changes the default site.
        # So that this function doesn't confuse later things,
        # reset that site now.
        set_default_site(default_site)

    return status


def run_postscript(post_script=None, dry_run=False, script_args=None):
    """Run a script after the pipeline finishes

    Parameters
    ----------
    post_script : `str`
        The script to run
    dry_run : `bool`
        If true do not run the script
    script_args : `list`, (`str`)
        Arguments to the script

    Returns
    -------
    return_code : `int`
        Usual unix convention of 0 -> success, non-zero is an error code
    """
    if script_args is None:  # pragma: no cover
        script_args = []
    if post_script and not dry_run:
        return_code = subprocess.call(post_script.split() + script_args, shell=True)
        if return_code:  # pragma: no cover
            sys.stderr.write(
                f"\nWARNING: The post-script command {post_script} "
                "returned error status {return_code}\n\n"
            )
        return return_code
        # Otherwise everything must have gone fine.
    return 0


def run(pipe_config, pipeline_config_filename, extra_config=None, dry_run=False):
    """Run a pipeline and associated scripts

    Parameters
    ----------
    pipe_config : `dict`
        The configuration dictionary
    pipe_config_filename : `str`
        The yaml file with the pipeline configuration
    extra_config : `dict`
        Extra parameters to override configuration
    dry_run : `bool`
        Flag to not actually run jobs

    Returns
    -------
    status : `int`
        Usual unix convention of 0 -> success, non-zero is an error code
    """

    # Later we will add these paths to sys.path for running here,
    # but we will also need to pass them to the sites below so that
    # they can be added within any containers or other launchers
    # that we use
    paths = pipe_config.get("python_paths", [])
    if isinstance(paths, str):  # pragma: no cover
        paths = paths.split()

    launcher_config = pipe_config.setdefault("launcher", {"name": "mini"})
    site_config = pipe_config.get("site", {"name": "local"})
    # Pass the paths along to the site
    site_config["python_paths"] = paths
    load(launcher_config, [site_config])

    # Python modules in which to search for pipeline stages
    modules = pipe_config.get("modules", "").split()

    pre_script = pipe_config.get("pre_script")
    post_script = pipe_config.get("post_script")
    script_args = [pipeline_config_filename]
    if extra_config:
        script_args += extra_config

    # temporarily add the paths to sys.path,
    # but remove them at the end
    with extra_paths(paths):

        for module in modules:
            __import__(module)

        run_prescript(pre_script=pre_script, dry_run=dry_run, script_args=script_args)

        status = run_pipeline(pipe_config)
        if status:  # pragma: no cover
            return status

        status = run_postscript(
            post_script=post_script, dry_run=dry_run, script_args=script_args
        )
        return status


def main():  # pragma: no cover
    """Main function called when ceci is invoked on the command line"""
    args = parser.parse_args()
    pipe_config = Pipeline.build_config(
        args.pipeline_config, args.extra_config, args.dry_run
    )
    status = run(pipe_config, args.pipeline_config, args.extra_config, args.dry_run)

    if status == 0:
        print("Pipeline successful.  Joy is sparked.")
    else:
        print("Pipeline failed.  No joy sparked.")
    return status


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
