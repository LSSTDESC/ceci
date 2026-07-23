"""Ceci n'est pas une pipeline"""

from .provenance import Provenance
from .stage import PipelineStage
from .handle import  BaseIOHandle
from .pipeline import (
    Pipeline,
    MiniPipeline,
    ParslPipeline,
    DryRunPipeline,
    FlowChartPipeline,
)
from .main import run_pipeline
from . import file_types
import importlib.metadata

try:
    __version__ = importlib.metadata.metadata(__name__)["Version"]
except:  # pragma: no cover
    # package is not installed
    __version__ = "unknown"
