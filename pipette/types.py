class DataFile:
    pass

class HDFFile(DataFile):
    suffix = 'hdf'

class FitsFile(DataFile):
    suffix = 'fits'

class TextFile(DataFile):
    suffix = 'txt'

