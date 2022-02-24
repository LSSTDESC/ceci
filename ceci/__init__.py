"""Ceci n'est pas une pipeline"""

from .stage import PipelineStage
from .pipeline import Pipeline, MiniPipeline, ParslPipeline, DryRunPipeline
from pkg_resources import DistributionNotFound
from pkg_resources import get_distribution

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # pragma: no cover
    # package is not installed
    pass
