from ceci.sites.cori import parse_int_set
from ceci.main import override_config


def test_parse_ints():
    assert parse_int_set("1,2,3") == set([1,2,3])
    assert parse_int_set("10-12") == set([10,11,12])
    assert parse_int_set("10-12,15,19-21") == set([10,11,12,15,19,20,21])


def test_override_config():
    config = {
        'a': 'b',
        'c': {'d':'e'},
        'h': True,
    }
    override_config(config, ['a=a', 'c.d=e', 'f.x.y.z=g', 'h=False'])

    assert config['a'] == 'a'
    assert config['c'] == {'d':'e'}
    assert config['f'] == {
        'x':{
            'y': {
                'z': 'g'
            }
        }
    }
    assert config['h'] is False
