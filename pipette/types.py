class DataFile:
    @staticmethod
    def open(path, mode):
        return open(path, mode)

class HDFFile(DataFile):
    suffix = 'hdf'
    @staticmethod
    def open(path, mode, **kwargs):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            import h5py
        return h5py.File(path, mode, **kwargs)

class FitsFile(DataFile):
    suffix = 'fits'

    @staticmethod
    def open(path, mode, **kwargs):
        import fitsio
        if mode == 'w':
            mode = 'rw'
        return fitsio.FITS(path, mode=mode, **kwargs)

class TextFile(DataFile):
    suffix = 'txt'

class YamlFile(DataFile):
    suffix = 'yml'

