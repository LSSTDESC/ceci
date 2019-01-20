from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
#
# cori_debug_config = Config(
#     executors=[
#         IPyParallelExecutor(
#             label='ipp_slurm',
#             provider=SlurmProvider(
#                 'debug',
#                 nodes_per_block=1,
#                 tasks_per_node=64,
#                 init_blocks=1,
#                 max_blocks=1,
#                 walltime="00:25:00",
#                 overrides="#SBATCH --constraint=haswell"
#             )
#         )
#     ]
# )
#
# cori_regular_config = Config(
#     executors=[
#         IPyParallelExecutor(
#             label='ipp_slurm',
#             provider=SlurmProvider(
#                 'regular',
#                 nodes_per_block=1,
#                 tasks_per_node=64,
#                 init_blocks=1,
#                 max_blocks=4,
#                 parallelism=0,
#                 walltime="6:00:00",
#                 overrides="#SBATCH --constraint=haswell"
#             )
#         )
#     ]
# )
#

threads_config =  Config(
    executors=[ThreadPoolExecutor(
            max_threads=8,
            label='local_threads'
            )],
    lazy_errors=True
)
