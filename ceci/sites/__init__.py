from . import cori
from . import local
from . import cori_interactive

def mini(config):
    return [], 'mpirun -n '

def cori_mini(config):
    return [], 'srun -u -n '



def activate_site(site, site_config):

    # Known sites an the functions that generate their
    # configurations
    activators = {
        'local': local.activate,
        'cori-interactive': cori_interactive.activate,
        'cori': cori.activate,
        'local-mini': mini,
        'cori-mini': cori_mini,
    }

    # Find the right one for this site or complain if unknown
    try:
        activator = activators[site]
    except KeyError:
        valid_sites = list(activators.keys())
        raise ValueError(f"Unknown site {site} - choose one of {valid_sites}")

    # Generate the site config.  This tells the parsl library
    # to associate a particular label with a particular configuration.
    executor_labels, mpi_command = activator(site_config)

    # These are needed by the pipeline to know how to launch jobs
    return executor_labels, mpi_command
