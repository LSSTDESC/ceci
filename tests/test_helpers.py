import tempfile
import os
from functools import wraps


def in_temp_dir(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        with tempfile.TemporaryDirectory() as dirname:
            orig_dir = os.getcwd()
            os.chdir(dirname)
            try:
                res = f(*args, **kwargs)
            finally:
                os.chdir(orig_dir)
        return res

    return wrapper


@in_temp_dir
def get_temp_cwd():
    cwd = os.getcwd()
    print(cwd)
    return cwd


def test_in_temp_dir():
    d1 = os.getcwd()
    d2 = get_temp_cwd()
    d3 = os.getcwd()
    assert d2 != d1
    assert d3 == d1
    assert not os.path.exists(d2)
