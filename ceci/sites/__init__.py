"""Classes and functions to manage site-specific configuration"""

from .cori import CoriBatchSite, CoriInteractiveSite
from .local import LocalSite, Site
from .ccin2p3 import CCParallel
import os


# Maps names from config files to classes.
site_classes = {
    "local": LocalSite,
    "cori-interactive": CoriInteractiveSite,
    "cori-batch": CoriBatchSite,
    "cc-parallel": CCParallel,
}


# by default use a local site configured.
# Overwritten if you call load below.


def set_default_site(site):
    """Set the default site"""
    global _default_site
    _default_site = site
    return _default_site


def reset_default_site():
    """Set the default site to the `LocalSite`"""
    site = LocalSite({"max_threads": 2})
    site.configure_for_mini()
    set_default_site(site)


def get_default_site():
    """Return the current default site"""
    return _default_site


reset_default_site()


def load(launcher_config, site_configs):
    """Configure a launcher and the sites it will execute on.

    This is the only site function needed in the main entry points to the code.

    The launcher_config argument should contain at least the "name" argument.
    Which additional arguments are accepted then depends on the choice of that.
    See test.yml for examples.

    The site_configs list has a similar structure.

    Parameters
    ----------
    launcher_config: dict
        Configuration information on launchers (parsl, minirunner, CWL).

    site_configs: list[dict]
        list of configs for different sites (local, cori-batch, cori-interactive).

    """
    sites = []

    launcher_name = launcher_config["name"]
    dry_run = launcher_config.get("dry_run", False)

    # Create an object for each site.
    for site_config in site_configs:
        site_name = site_config["name"]
        # Also tell the sites whether this is a dry-run.
        # for example, the cori site checks you're not
        # trying to run srun on a login node, but we skip
        # that test if we are not actually running the command,
        # just printing it.
        site_config["dry_run"] = dry_run

        try:
            cls = site_classes[site_name]
        except KeyError as msg:  # pragma: no cover
            raise ValueError(f"Unknown site {site_name}") from msg

        site = cls(site_config)
        site.configure_for_launcher(launcher_name)
        sites.append(site)

    setup_launcher(launcher_config, sites)

    # replace the default site with the first
    # one found here
    set_default_site(sites[0])

    return sites


def setup_launcher(launcher_config, sites):
    """
    Some launchers need an initial setup function to be run.
    Do that here.
    """
    name = launcher_config["name"]

    if name == "parsl":
        setup_parsl(launcher_config, sites)
    # no setup to do for other managers
    else:
        pass


def setup_parsl(launcher_config, sites):
    """
    Set up parsl for use with the specified sites.
    """
    from parsl import load as parsl_load
    from parsl.config import Config
    from parsl import set_file_logger

    executors = [site.info["executor"] for site in sites]
    config = Config(executors=executors)
    parsl_load(config)

    # Optional logging of pipeline infrastructure to file.
    log_file = launcher_config.get("log")
    if log_file:  # pragma: no cover
        log_file_dir = os.path.split(os.path.abspath(log_file))[0]
        os.makedirs(log_file_dir, exist_ok=True)
        set_file_logger(log_file)
