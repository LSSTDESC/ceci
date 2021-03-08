import sys
from ceci.main import run
from ceci.utils import remove_last, extra_paths
import pytest
import os


def test_remove_item():
    l = list("abcdea")
    remove_last(l, "a")
    assert l == list("abcde")

    l = list("abcde")
    remove_last(l, "b")
    assert l == list("acde")

    with pytest.raises(ValueError):
        remove_last([1, 2, 3], 4)


class MyError(Exception):
    pass


def test_extra_paths():
    p = "xxx111yyy222"
    orig_path = sys.path[:]
    orig_env = os.environ.get("PYTHONPATH", "")

    # check path is put in
    with extra_paths(p):
        assert sys.path[0] == p
        assert p in os.environ["PYTHONPATH"]

    # check everything back to normal
    # after with statement
    assert p not in sys.path
    assert sys.path == orig_path
    assert p not in os.environ["PYTHONPATH"]
    assert os.environ["PYTHONPATH"] == orig_env

    # check that an exception does not interfere
    # with this
    try:
        with extra_paths(p):
            assert sys.path[0] == p
            raise MyError("x")
    except MyError:
        pass

    assert p not in sys.path
    assert sys.path == orig_path

    # now putting the item at the end not the start
    with extra_paths(p, start=False):
        assert sys.path[-1] == p

    assert p not in sys.path
    assert sys.path == orig_path

    try:
        with extra_paths(p, start=False):
            assert sys.path[-1] == p
            raise MyError("x")
    except MyError:
        pass

    assert p not in sys.path
    assert sys.path == orig_path

    # now agan with a list of paths
    p = ["xxx111yyy222", "aaa222333"]
    with extra_paths(p):
        assert sys.path[0] == p[1]
        assert sys.path[1] == p[0]

    for p1 in p:
        assert p1 not in sys.path
    assert sys.path == orig_path

    try:
        with extra_paths(p):
            assert sys.path[0] == p[1]
            assert sys.path[1] == p[0]
            raise MyError("x")
    except MyError:
        pass

    for p1 in p:
        assert p1 not in sys.path
    assert sys.path == orig_path

    # now agan with a list of paths, at the end
    p = ["xxx111yyy222", "aaa222333"]
    with extra_paths(p, start=False):
        assert sys.path[-1] == p[1]
        assert sys.path[-2] == p[0]

    for p1 in p:
        assert p1 not in sys.path
    assert sys.path == orig_path

    try:
        with extra_paths(p, start=False):
            assert sys.path[-1] == p[1]
            assert sys.path[-2] == p[0]
            raise MyError("x")
    except MyError:
        pass

    assert p not in sys.path
    assert sys.path == orig_path

    # check that if the user removes the path
    # themselves then it is okay
    p = ["xxx111yyy222", "aaa222333"]
    with extra_paths(p, start=True):
        sys.path.remove("xxx111yyy222")

    assert sys.path == orig_path

    # check only one copy is removed
    sys.path.append("aaa")
    tmp_paths = sys.path[:]
    p = "aaa"
    with extra_paths(p, start=True):
        pass

    assert sys.path == tmp_paths

    with extra_paths(p, start=False):
        pass
