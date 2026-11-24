import copy

class BaseIOHandle:
    """
    Base class for input and output objects. For now this is pretty empty.
    """
    suffix = ""

    def __init__(self, provenance):
        # If provenance is provided then pull that out
        self.provenance =  provenance

    @property
    def provenance(self):
        return self._provenance

    @provenance.setter
    def provenance(self, provenance):
        # always copy the provenance
        self._provenance = copy.deepcopy(provenance)
