from . import cori
from . import local
from . import cori_interactive


def activate_site(site, site_config):

    # Known sites an the functions that generate their
    # configurations
    activators = {
        'local': local.acivate,
        'cori-interactive': cori_interactive.acivate,
        'cori': cori.acivate,
    }

    # Find the right one for this site or complain if unknown
    try:
        activator = activators
    except KeyError:
        raise ValueError(f"Unknown site {site}")

    # Generate the site config.  This tells the parsl library
    # to associate a particular label with a particular configuration.
    executor_labels, mpi_command = activator(site_config)

    # These are needed by the pipeline to know how to launch jobs
    return executor_labels, mpi_command