"""Utility functions need to run ceci"""

from contextlib import contextmanager
import sys
import os


def add_python_path(path, start):
    """add a path to the env var PYTHONPATH"""
    old = os.environ.get("PYTHONPATH", "")
    if start:
        new = path + ":" + old
    else:
        new = old + ":" + path
    os.environ["PYTHONPATH"] = new


def remove_python_path(path, start):
    """remove a path from PYTHONPATH"""
    p = os.environ.get("PYTHONPATH", "").split(":")
    if start:
        p.remove(path)
    else:
        remove_last(p, path)
    os.environ["PYTHONPATH"] = ":".join(p)


@contextmanager
def extra_paths(paths, start=True):
    """Add extra paths to PYTHONPATH while in a context"""

    # allow passing a single path or
    # a list of them
    if isinstance(paths, str):
        paths = paths.split()

    # On enter, add paths to both sys.path,
    # and the PYTHONPATH env var, so that subprocesses
    # can see it,
    # either the start or the end depending
    # on the start argument
    for path in paths:
        if start:
            sys.path.insert(0, path)
        else:
            sys.path.append(path)

        add_python_path(path, start)

    # Return control to caller
    try:
        yield
    # On exit, remove the paths
    finally:
        for path in paths:
            try:
                if start:
                    sys.path.remove(path)
                else:
                    remove_last(sys.path, path)
                # also remove env var entry
                remove_python_path(path, start)
            # If e.g. user has already done this
            # manually for some reason then just
            # skip
            except ValueError:
                pass


def remove_last(lst, item):
    """
    Removes (in-place) the last instance of item from the list lst.
    Raises ValueError if item is not in list

    Parameters
    ----------
    lst: List
        A list of anything
    item: object
        Item to be removed

    Returns
    -------
    None
    """
    tmp = lst[::-1]
    tmp.remove(item)
    lst[:] = tmp[::-1]


def embolden(text):
    """
    Make a string print in bold in a terminal using control codes.

    Note that if this text is read in other applications (not in a shell)
    then it probably won't work.


    Parameters
    ----------
    text: str
        Text to be emboldened

    Returns
    -------
    str
        Emboldened text
    """
    return "\033[1m" + text + "\033[0m"
