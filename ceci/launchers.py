default_cori_setup = """
#SBATCH --constraint=haswell
module load python/3.5-anaconda ;
source /global/projecta/projectdirs/lsst/groups/WL/users/zuntz/env/bin/activate ;
export PYTHONPATH=$PYTHONPATH:/global/cscratch1/sd/zuntz/pipe/ceci'
"""


def make_cori_site(n_node, max_blocks, minutes=10, partition='debug', setup=default_cori_setup):
    auth = { "channel" : None }
    site = {"site" : f"cori.{n_node}",
            "mpi_command": 'srun -n',
            "auth" : auth,
            "execution" : {
                "executor" : "ipp",
                "provider" : "slurm",
                "block" : {
                    "nodes" : n_node,
                    "taskBlocks" : 1,
                    "walltime" : f"00:{minutes}:00",
                    "initBlocks": 0,
                    "minBlocks" : 0,
                    "maxBlocks" : max_blocks,
                    "scriptDir" : ".",
                    "options" : {
                        "partition" : partition,
                        "overrides" : setup
                    }
                }
            }
        }
    return site

def make_cori_interactive_site(n_node, minutes=10, partition='debug', setup=default_cori_setup):
    auth = { "channel" : None }
    site = {"site" : f"cori.interactive.{n_node}",
            "mpi_command": 'srun -n',
            "auth" : auth,
            "execution" : {
                "executor" : "ipp",
                "provider" : "local",
                "block" : {
                    "nodes" : n_node,
                    "taskBlocks" : n_node,
                    "initBlocks": 1,
                    "minBlocks" : 1,
                    "maxBlocks" : 1,
                    "scriptDir" : ".",
                    "options" : {
                        "overrides" : setup
                    }
                }
            }
        }
    return site

def make_cori_single_node_debug_config():
    sites = [make_cori_site(n_node,1)]
    config = {
        "sites" : sites,
        "globals" : {   "lazyErrors" : True },
    }
    return config

def make_cori_interactive_config(n_node):
    sites = [make_cori_interactive_site(n_node)]
    config = {
        "sites" : sites,
        "globals" : {"lazyErrors" : False },
    }
    return config
