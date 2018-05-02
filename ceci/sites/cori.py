import copy
import getpass
import os
USERNAME = getpass.getuser()


# This is the template config for Cori that is modified on a per app basis.
coriBase =  {  "site": "Cori_Template",
               "auth": {
                   "channel": "local",
                   "hostname": "cori.nersc.gov",
                   "username": USERNAME,
                   "scriptDir": "SCRIPT_DIR",
               },
               "execution": {
                   "executor": "ipp",
                   "provider": "slurm", 
                   "block": {  # Definition of a block                                                         
                       "nodes": 1,            # of nodes in that block                                         
                       "taskBlocks": 1,       # total tasks in a block                                         
                       "walltime": "XXXXXXXX",
                       "initBlocks": 0,
                       "maxBlocks": 1,
                       "options": {
                           "partition": "debug",
                           # Remember to update this if you are using a different Python version 
                           # on client side. Client and workers should have the same python env.
                           "overrides": """#SBATCH --constraint=haswell                                        
                           module load python/3.5-anaconda ; source activate parsl_env_3.5"""
                       }
                   }
               }
           }



base_launcher = {
    'mpi_command' : 'srun -n',
    "sites" : [],
    "globals": {"lazyErrors": True}
}



def add_site_config(config, nodes, walltime_minutes, partition):
    script_dir = os.environ.get("CECI_SCRIPT_DIR", 
            os.path.join(os.environ["SCRATCH"], "parsl-scripts"))  # default value
    try:
        setup_script = os.environ["CECI_SETUP"]
    except KeyError:
        raise KeyError("Please set the environment variable CECI_SETUP to a script to be sourced when running ceci")
    setup_command = f"""#SBATCH --constraint=haswell                                        
                        source {setup_script}"""


    # Do not recreate site if we've already made a comparable definition
    sitename = "Cori_{}N_{}M".format(nodes, walltime_minutes)
    known_sites = [sitedef["site"] for sitedef in config["sites"]]

    if sitename not in known_sites:
        site = copy.deepcopy(coriBase)
        site["auth"]["username"] = USERNAME
        site["auth"]["scriptDir"] = script_dir
        site["site"] = sitename
        site["execution"]["block"]["nodes"] = nodes
        site["execution"]["block"]["walltime"] = f"00:{walltime_minutes}:00"
        site["execution"]["block"]["options"]["overrides"] = setup_command
        site["execution"]["block"]["options"]["partition"] = partition

        config['sites'].append(site)

    return sitename


def make_launcher(stages):
    launcher = copy.deepcopy(base_launcher)
    for stage in stages:
        nodes = stage.get("nodes", 1)
        walltime_minutes = stage.get("walltime", 10)
        partition = stage.get("partition", "debug")
        site_name = add_site_config(launcher, nodes, walltime_minutes, partition)
        stage['site'] = site_name
    return launcher
