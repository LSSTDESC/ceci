from .stage import PipelineStage
from .pipeline import Pipeline, MiniPipeline, ParslPipeline, DryRunPipeline
￼
￼try:
    ￼from pkg_resources import get_distribution
￼    __version__ = get_distribution(__name__).version
￼except:
￼    # package is not installed, too bad, no version
￼    pass
