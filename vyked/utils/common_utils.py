import json

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

def valid_timeout(timeout):
    return True if isinstance(timeout, int) and timeout > 0 and timeout <= 600 else False