import os


class DataHandle:
    """
    A class representing some data to be made by pipeline stages
    and passed on to subsequent ones.

    DataHandle itself should not be instantiated - instead subclasses
    should be defined for different file types.

    These subclasses are used in the definition of pipeline stages
    to indicate what kind of file is expected.  The "suffix" attribute,
    which must be defined on subclasses, indicates the file suffix.

    The open method, which can optionally be overridden, is used by the
    machinery of the PipelineStage class to open an input our output
    named by a tag.

    """
    def __init__(self, path, mode, data=None, **kwargs):
        self.path = path
        self.mode = mode
        self.kw = kwargs.copy()
        if mode not in ["r", "w"]:
            raise ValueError(f"File 'mode' argument must be 'r' or 'w' not '{mode}'")
        self.data = data

    def __call__(self, force=False):
        if self.data is not None and not force:
            return self.data
        self.data = self.read(self.path, self.mode, **self.kw)
        return self.data

    def write(self, overwrite=False):
        if not self.in_memory:
            raise RuntimeError(f"Tried to write data that doesn't exist yet, target was {self.path}")
        if self.exists and not overwrite:
            raise RuntimeError(f"File {self.path} already exists")
        self.write_data()

    def write_data(self):
        raise NotImplementedError("write_data")

    @property
    def in_memory(self):
        return self.data is not None

    @property
    def exists(self):
        return os.path.exists(self.path)

    @classmethod
    def open(cls, path, mode, **kwargs):
        """
        Open a data file.  The base implementation of this function just
        opens and returns a standard python file object.

        Subclasses can override to either open files using different openers
        (like fitsio.FITS), or, for more specific data types, return an
        instance of the class itself to use as an intermediary for the file.

        """
        return open(path, mode, **kwargs)

    @classmethod
    def read(cls, path, mode):
        """
        Open a data file.  The base implementation of this function just
        opens and returns a standard python file object.

        Subclasses can override to either open files using different openers
        (like fitsio.FITS), or, for more specific data types, return an
        instance of the class itself to use as an intermediary for the file.

        """
        return cls.open(path, mode)
