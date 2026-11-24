class ProvenanceError(Exception):
    pass


class ProvenanceFileTypeUnknown(ProvenanceError):
    pass


class ProvenanceFileSchemeUnsupported(ProvenanceError):
    pass


class ProvenanceMissingFile(ProvenanceError):
    pass


class ProvenanceMissingSection(ProvenanceError):
    pass


class ProvenanceMissingItem(ProvenanceError):
    pass
