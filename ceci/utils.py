from contextlib import contextmanager
import sys

@contextmanager
def extra_paths(paths, start=True):
    if isinstance(paths, str):
        paths = paths.split()

    for path in paths:
        if start:
            sys.path.insert(0, path)
        else:
            sys.path.append(path)

    try:
        yield
    finally:
        for path in paths:
            if start:
                sys.path.remove(path)
            else:
                remove_last(sys.path, path)

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
