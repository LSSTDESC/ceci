default_cori_setup = """
#SBATCH --constraint=haswell
module load python/3.5-anaconda ;
source /global/projecta/projectdirs/lsst/groups/WL/users/zuntz/env/bin/activate ;
export PYTHONPATH=$PYTHONPATH:/global/cscratch1/sd/zuntz/pipe/pipette'
"""



def make_cori_configs(n_nodes=[1,10], local=True, username=None, setup=default_cori_setup, partition='debug'):
    if local:
        auth = {'channel':'local'}
    else:
        if username is None:
            raise ValueError("Need username for remote cori runs")
        initial = username[0]
        auth = {
                "channel" : "ssh",
                "hostname" : "cori.nersc.gov",
                "username" : username,
                "scriptDir" : f"/global/homes/{initial}/{username}/parsl_scripts"
                }
    sites = []
    for n_node in n_nodes:
        # If needed, run four single-node jobs at once.
        # But if more are needed run 
        if n_node==1:
            max_blocks = 4
        else:
            max_blocks = 1
        site = {"site" : f"cori.{n_node}",
                "mpi_command": 'srun -n'
                "auth" : auth,
                "execution" : {
                    "executor" : "ipp",
                    "provider" : "slurm",
                    "block" : {
                        "nodes" : n_node,
                        "taskBlocks" : 1,
                        "walltime" : "00:30:00",
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
        sites.append(site)


    config = {
        "sites" : sites,
        "globals" : {   "lazyErrors" : True },
    }
    return config
