import distutils.version
import sys
import os
import inspect
import pathlib
import contextlib
import shutil


def is_path(p):
    """
    Decide whether the input is likely to be a path instead of a file object.

    Returns True if the parameter is a pathlib.Path object or a string, False
    otherwise

    Parameters
    ----------
    p: Any
        Object which may be a path or not

    Returns
    -------
    bool

    """
    return isinstance(p, str) or isinstance(p, pathlib.Path)


def get_caller_directory(parent_frames=0):
    """
    Find the directory where the code calling this
    function lives, or any number of jumps back up the stack

    Parameters
    ----------
    parent_frames: int
        Number of additional frames to go up in the call stack

    Returns
    -------
    directory: str
    """
    previous_frame = inspect.currentframe().f_back
    # go back more frames if desired
    for i in range(parent_frames):
        previous_frame = previous_frame.f_back

    filename = inspect.getframeinfo(previous_frame).filename
    p = pathlib.Path(filename)
    if not p.exists():
        # dynamically generated or interactive mode
        return None
    return str(p.parent)


def find_module_versions():
    """
    Generate a dictionary of versions of all imported modules
    by looking for __version__ or version attributes on them.

    Parameters
    ----------
    None

    Returns
    -------
    dict:
        A dictioary of the versions of all loaded modules
    """
    versions = {}
    for name, module in sys.modules.items():
        if hasattr(module, "version"):
            v = module.version
        elif hasattr(module, "__version__"):
            v = module.__version__
        else:
            continue
        if isinstance(v, str) or isinstance(v, distutils.version.Version):
            versions[name] = str(v)
    return versions


@contextlib.contextmanager
def open_hdf(hdf_file, mode):
    """Open an HDF file, or if a file is provided, simply return it"""
    import h5py

    if is_path(hdf_file):
        f = h5py.File(hdf_file, mode)
        try:
            yield f
        finally:
            f.close()
    else:
        yield hdf_file


@contextlib.contextmanager
def open_fits(fits_file, mode):
    """Open a FITS file, or if a file is already provided simply return it"""
    import fitsio

    if is_path(fits_file):
        exists = os.path.exists(fits_file)

        # By default the "w" mode in FITSIO is r/w.  We have to explicitly remove
        # first if we want to do a proper write and the file already exists.
        if mode == "w":
            mode = "rw"
            if exists:
                shutil.remove(fits_file)
        f = fitsio.FITS(fits_file, mode=mode)

        try:
            yield f
        finally:
            f.close()
    else:
        yield fits_file


@contextlib.contextmanager
def open_file(file, mode):
    """Open a regular file, or if a file is already provided simply return it"""

    if is_path(file):
        if mode == "r+" and not os.path.exists(file):
            f = open(file, "w+")
        else:
            f = open(file, mode=mode)

        try:
            yield f
        finally:
            f.close()
    else:
        yield file
