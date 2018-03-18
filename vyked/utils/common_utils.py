import json
import collections

def json_file_to_dict(_file: str) -> dict:
    """
    convert json file data to dict

    :param str _file: file location including name

    :rtype: dict
    :return: converted json to dict
    """
    config = None
    try:
        with open(_file) as config_file:
            config = json.load(config_file)
    except:
        pass
    return config

def object_to_dict(result):
    if isinstance(result, collections.OrderedDict):
        res = {}
        for key,val in result.items():
            res[key] = object_to_dict(val)
        result = res
    if hasattr(result, '__dict__'):
        result = result.__dict__
    if isinstance(result, dict):
        for key, value in result.items():
            if hasattr(value, '__dict__'):
                value = value.__dict__
            result[key] = value
            if isinstance(value, dict):
                result[key] = {k: object_to_dict(v) for k, v in value.items()}
            if isinstance(value, list):
                result[key] = [object_to_dict(r) for r in value]
    if isinstance(result, list):
        result = [object_to_dict(r) for r in result]

    return result

def tcp_to_http_path_for_function(func):
    if isinstance(func, str):
        return "/tcp_to_http/{}".format(func)
    return "/tcp_to_http/{}".format(func.__name__)

def valid_timeout(timeout):
    return True if isinstance(timeout, int) and timeout > 0 and timeout <= 600 else False