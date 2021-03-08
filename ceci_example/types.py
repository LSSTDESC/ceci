class DataFile:
    """
    A class representing a DataFile to be made by pipeline stages
    and passed on to subsequent ones.

    DataFile itself should not be instantiated - instead subclasses
    should be defined for different file types.

    These subclasses are used in the definition of pipeline stages
    to indicate what kind of file is expected.  The "suffix" attribute,
    which must be defined on subclasses, indicates the file suffix.

    The open method, which can optionally be overridden, is used by the
    machinery of the PipelineStage class to open an input our output
    named by a tag.

    """

    def __init__(self, path, mode, extra_provenance=None, validate=True, **kwargs):
        self.path = path
        self.mode = mode

        if mode not in ["r", "w"]:
            raise ValueError(f"File 'mode' argument must be 'r' or 'w' not '{mode}'")

        self.file = self.open(path, mode, **kwargs)

    @classmethod
    def open(cls, path, mode):
        """
        Open a data file.  The base implementation of this function just
        opens and returns a standard python file object.

        Subclasses can override to either open files using different openers
        (like fitsio.FITS), or, for more specific data types, return an
        instance of the class itself to use as an intermediary for the file.

        """
        return open(path, mode)

    @classmethod
    def make_name(cls, tag):
        if cls.suffix:
            return f"{tag}.{cls.suffix}"
        else:
            return tag


class HDFFile(DataFile):
    """
    A data file in the HDF5 format.
    Using these files requires the h5py package, which in turn
    requires an HDF5 library installation.

    """

    suffix = "hdf"
    format = "http://edamontology.org/format_3590"

    @classmethod
    def open(cls, path, mode, **kwargs):
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            import h5py
        return h5py.File(path, mode, **kwargs)


class FitsFile(DataFile):
    """
    A data file in the FITS format.
    Using these files requires the fitsio package.
    """

    suffix = "fits"
    format = "http://edamontology.org/format_2333"

    @classmethod
    def open(cls, path, mode, **kwargs):
        import fitsio

        # Fitsio doesn't have pure 'w' modes, just 'rw'.
        # Maybe we should check if the file already exists here?
        if mode == "w":
            mode = "rw"
        return fitsio.FITS(path, mode=mode, **kwargs)


class TextFile(DataFile):
    """
    A data file in plain text format.
    """

    suffix = "txt"
    format = "http://edamontology.org/format_2330"


class YamlFile(DataFile):
    """
    A data file in yaml format.
    """

    suffix = "yml"
    format = "http://edamontology.org/format_3750"
