import copy

base_launcher = {
    'mpi_command' : 'mpirun -n',
    'globals': {'lazyErrors': True},
    'sites': [
        {
        'auth': {'channel': None},
        'execution': 
            {
            'executor': 'threads',
            'maxThreads': 4,
            'provider': None
            },
        'site': 'Local_Threads'
        }
    ]
}


def make_launcher(stages):
    launcher = copy.deepcopy(base_launcher)
    for stage in stages:
        stage['site'] = 'Local_Threads'
    return launcher
