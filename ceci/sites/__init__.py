from .cori import CoriBatchSite, CoriInteractiveSite
from .local import LocalSite, Site
from parsl.config import Config
from parsl import load as parsl_load


site_classes = {
    'local': LocalSite,
    'cori-interactive': CoriInteractiveSite,
    'cori-batch': CoriBatchSite,
}

def load(launcher_config, site_configs):
    sites = []
    
    launcher_name = launcher_config['name']

    for site_config in site_configs:
        site_name = site_config['name']

        try:
            cls = site_classes[site_name]
        except KeyError:
            raise ValueError(f'Unknown site {name}')

        site = cls(site_config)
        site.configure_for_launcher(launcher_name)
        sites.append(site)

    setup_launcher(launcher_name, sites)

    return sites


def setup_launcher(launcher, sites):
    if launcher == 'parsl':
        setup_parsl(sites)
    # no setup to do for the MiniRuunner
    else:
        pass

def setup_parsl(sites):
    executors = [site.info['executor'] for site in sites]
    config = Config(executors=executors)
    parsl_load(config)
