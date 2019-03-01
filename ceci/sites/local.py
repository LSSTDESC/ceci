from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
import parsl

def activate():
    executor2 = ThreadPoolExecutor(label='exe2', max_threads=2)
    executor4 = ThreadPoolExecutor(label='exe4', max_threads=4)
    config = Config(executors=[executor2, executor4])
    parsl.load(config)


# import copy

# base_launcher = {
#     'mpi_command' : 'mpirun -n',
#     'globals': {'lazyErrors': True},
#     'sites': [
#         {
#         'auth': {'channel': None},
#         'execution': 
#             {
#             'executor': 'threads',
#             'maxThreads': 4,
#             'provider': None
#             },
#         'site': 'Local_Threads'
#         }
#     ]
# }



# def make_launcher(stages):
#     launcher = copy.deepcopy(base_launcher)
#     for stage in stages:
#         stage['site'] = 'Local_Threads'
#     return launcher
