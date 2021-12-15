import os

from collections.abc import Mapping


def cast_value(dtype, value): #pylint: disable=too-many-return-statements
    """Casts an input value to a particular type

    Parameters
    ----------
    dtype : type
        The type we are casting to
    value : ...
        The value being cast

    Returns
    -------
    out : ...
        The object cast to dtype

    Raises
    ------
    TypeError if neither value nor dtype are None and the casting fails

    Notes
    -----
    This will proceed in the following order
        1.  If dtype is None it will simply return value
        2.  If value is None it will return None
        3.  If value is an instance of dtype it will return value
        4.  If value is a Mapping it will use it as a keyword dictionary to the constructor of dtype, i.e., return dtype(**value)
        5.  It will try to pass value to the constructor of dtype, i.e., return dtype(value)
        6.  If all of these fail, it will raise a TypeError
    """
    # dtype is None means all values are legal 
    if dtype is None:
        return value
    # value is None is always allowed
    if value is None:
        return None
    # if value is an instance of self.dtype, then return it
    if isinstance(value, dtype):
        return value
    if isinstance(value, Mapping):
        return dtype(**value)
    try:
        return dtype(value)
    except (TypeError, ValueError):
        pass

    msg = f"Value of type {type(value)}, when {str(dtype)} was expected."    
    raise TypeError(msg)



class StageParameter:
    """
    """
    def __init__(self, **kwargs):
        kwcopy = kwargs.copy()
        self._help = kwcopy.pop('help', 'A Parameter')
        self._format = kwcopy.pop('format', '%s')
        self._dtype = kwcopy.pop('dtype', None)
        self._default = kwcopy.pop('default', None)        
        if kwcopy:
            raise ValueError(f"Unknown arguments to StageParameter {str(list(kwcopy.keys()))}")
        self._value = cast_value(self._dtype, self._default)
                
    @property
    def value(self):
        return self._value

    def set(self, value):
        self._value = cast_value(self._dtype, value)
        return self._value
        
    def set_default(self, value):
        self._value = cast_value(self._dtype, self._default)
        return self._value
        
    def __set__(self, obj, value):
        self._value = cast_value(self._dtype, value)
        return self._value
        
    def __get__(self, obj, obj_class):
        return self._value
        

class StageConfig:
    """
    """
    def __init__(self, **kwargs):
        self._param_dict = {}
        for key, val in kwargs.items():
            if val is None:
                dtype = None
                default = None
            elif isinstance(val, type):
                dtype = val
                default = None
            else:
                dtype = type(val)
                default = val                
            param = StageParameter(dtype=dtype, default=default)
            self._param_dict[key] = param

    def keys(self):
        return self._param_dict.keys()

    def values(self):
        return self._param_dict.values()

    def items(self):
        return self._param_dict.items()

    def __len__(self):
        return len(self._param_dict)

    def get(self, key, def_value=None):
        if key in self._param_dict:
            return self.__getattr__(key)
        return def_value        

    def __str__(self):
        s = "{"
        for key, attr in self._param_dict.items():
            if isinstance(attr, StageParameter):
                val = attr.value
            else:
                val = attr
            s += f"{key}:{val},"
        s += "}"
        return s
        
    def __repr__(self):
        s = "StageConfig"
        s += self.__str__()
        return s
        
    def __getitem__(self, key):
        return self.__getattr__(key)
    
    def __setitem__(self, key, value):
        return self.__setattr__(key, value)

    def __delitem__(self, key):
        return self.__delattr__(key)
    
    def __getattr__(self, key):
        attr = self._param_dict[key]
        if isinstance(attr, StageParameter):
            return attr.value
        return attr

    def __setattr__(self, key, value):
        if key == '_param_dict':
            self.__dict__[key] = value
            return
        if key in self._param_dict:
            attr = self._param_dict[key]
            if isinstance(attr, StageParameter):
                return attr.set(value)
        self._param_dict[key] = value            
        return value
                
    def __delattr__(self, key, value):
        if key in self._param_dict:
            attr = self._param_dict[key]
            if isinstance(attr, StageParameter):
                return attr.set_default()
        return self._param_dict.pop(key)

    def set_config(self, input_config, args):
        for key in self._param_dict.keys():
            val = None
            if key in input_config:
                val = input_config[key]
            if args.get(key) is not None:
                val = args[key]
            if val is None:
                raise ValueError(f"Missing configuration option {key}")
            self.__setattr__(key, val)

        for key, val in input_config.items():
            if key in self._param_dict:
                continue
            self._param_dict[key] = val
        

            
