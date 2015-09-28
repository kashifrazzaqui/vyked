import warnings
from functools import wraps


def deprecated(func):
    """
    Generates a deprecation warning
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        msg = "'{}' is deprecated".format(func.__name__)
        warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
        return func(*args, **kwargs)

    return wrapper
