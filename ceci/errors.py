"""ceci specific error types"""


class DuplicateStageName(TypeError):
    """Raised when a stage name is reused in a pipeline"""


class IncompleteStage(TypeError):
    """Raised when a stage is missing required methods or attributes"""


class StageNotFound(ValueError):
    """Raise when a stage is not found in a pipeline"""


class ReservedNameError(TypeError):
    """Raised when a parameter is given a reserved name"""
