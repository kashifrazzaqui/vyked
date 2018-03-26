
from .utils.common_utils import json_file_to_dict
config = json_file_to_dict('config.json')

class CONFIG:
    Convert_Tcp_To_Http =  config['TCP_TO_HTTP'] if (isinstance(config, dict) and 'TCP_TO_HTTP' in config  ) else True