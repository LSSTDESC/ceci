"""Ceci n'est pas une pipeline"""

from .stage import PipelineStage
from .pipeline import (
    Pipeline,
    MiniPipeline,
    ParslPipeline,
    DryRunPipeline,
    FlowChartPipeline,
)
from .main import run_pipeline
import importlib.metadata

try:
    __version__ = importlib.metadata.metadata(__name__)["Version"]
except:  # pragma: no cover
    # package is not installed
    pass
