from contextlib import contextmanager
import sys

@contextmanager
def extra_paths(paths, start=True):
    # allow passing a single path or
    # a list of them
    if isinstance(paths, str):
        paths = paths.split()

    # On enter, add paths to sys.path,
    # either the start or the end depending
    # on the start argument
    for path in paths:
        if start:
            sys.path.insert(0, path)
        else:
            sys.path.append(path)

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
