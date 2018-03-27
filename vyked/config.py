
from .utils.common_utils import json_file_to_dict
config = json_file_to_dict('config.json')

class CONFIG:
    Convert_Tcp_To_Http =  config['TCP_TO_HTTP'] if (isinstance(config, dict) and 'TCP_TO_HTTP' in config  ) else True
    Http_keep_alive_Timeout = config['HTTP_KEEP_ALIVE_TIMEOUT'] if (isinstance(config, dict) and 'HTTP_KEEP_ALIVE_TIMEOUT' in config  ) else 15
    Http_Connection_timeout = config['HTTP_CONNECTION_TIMEOUT'] if (isinstance(config, dict) and 'HTTP_KEEP_ALIVE_TIMEOUT' in config  ) else 5

