from .cori import CoriBatchSite, CoriInteractiveSite
from .local import LocalSite, Site
from parsl.config import Config
from parsl import load as parsl_load
from parsl import set_file_logger as set_parsl_logger
import os


# Maps names from config files to classes.
site_classes = {
    'local': LocalSite,
    'cori-interactive': CoriInteractiveSite,
    'cori-batch': CoriBatchSite,
}




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
    
    launcher_name = launcher_config['name']

    # Create an object for each site.
    for site_config in site_configs:
        site_name = site_config['name']

        try:
            cls = site_classes[site_name]
        except KeyError:
            raise ValueError(f'Unknown site {name}')

        site = cls(site_config)
        site.configure_for_launcher(launcher_name)
        sites.append(site)

    setup_launcher(launcher_config, sites)

    return sites


def setup_launcher(launcher_config, sites):
    """
    Some launchers need an initial setup function to be run.
    Do that here.
    """
    name = launcher_config['name']

    if name == 'parsl':
        setup_parsl(launcher_config, sites)
    # no setup to do for other managers
    else:
        pass

def setup_parsl(launcher_config, sites):
    """
    Set up parsl for use with the specified sites.
    """
    executors = [site.info['executor'] for site in sites]
    config = Config(executors=executors)
    parsl_load(config)

    # Optional logging of pipeline infrastructure to file.
    log_file = launcher_config.get('log')
    if log_file:
        log_file_dir = os.path.split(os.path.abspath(log_file))[0]
        os.makedirs(log_file_dir, exist_ok=True)
        set_parsl_logger(log_file)

