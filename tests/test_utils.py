
from collections import OrderedDict

from ceci.sites.cori import parse_int_set
from ceci.pipeline import override_config
from ceci.utils import embolden
from ceci.config import cast_value, cast_to_streamable


def test_parse_ints():
    assert parse_int_set("1,2,3") == set([1, 2, 3])
    assert parse_int_set("10-12") == set([10, 11, 12])
    assert parse_int_set("10-12,15,19-21") == set([10, 11, 12, 15, 19, 20, 21])


def test_override_config():
    config = {
        "a": "b",
        "c": {"d": "e"},
        "h": True,
        "i": 8,
        "j": 17.5,
    }
    override_config(config, ["a=a", "c.d=e", "f.x.y.z=g", "h=False", "i=9", "j=19.5"])

    assert config["a"] == "a"
    assert config["c"] == {"d": "e"}
    assert config["f"] == {"x": {"y": {"z": "g"}}}
    assert config["h"] is False
    assert config["i"] == 9
    assert config["j"] == 19.5

def test_embolden():
    x = 'hj6_9xx0'
    y = embolden(x)
    assert x in embolden(x)
    assert y[4:-4] == x


def test_cast_value():
    # dtype is None should allow any value
    assert cast_value(None, 5) == 5
    assert cast_value(None, "dog") == "dog"

    # value is None is always allowed
    assert cast_value(float, None) is None
    assert cast_value(str, None) is None

    # if isinstance(value, dtype) return value
    assert cast_value(float, 5.) == 5.
    assert cast_value(str, "dog") == "dog"

    # if isinstance(value, Mapping) return dtype(**value)
    odict = cast_value(dict, dict(key1='dog', key2=5))
    assert odict['key1'] == 'dog'
    assert odict['key2'] == 5

    # if dtype(value) works return that
    assert cast_value(float, 5) == 5.

    # catch errors
    try:
        cast_value(float, "dog")
    except TypeError:
        pass
    else:
        raise TypeError("Failed to catch type error")
    
    try:
        cast_value(int, [])
    except TypeError:
        pass
    else:
        raise TypeError("Failed to catch type error")
    

def test_cast_streamable():
    assert cast_to_streamable(dict(key='dog'))['key'] == 'dog'
    assert cast_to_streamable(OrderedDict([('key', 'dog')]))['key'] == 'dog'
