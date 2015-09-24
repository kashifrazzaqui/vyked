import json
import datetime
from time import mktime


class VykedEncoder(json.JSONEncoder):

    """
    json dump encoder class
    """

    def default(self, obj):
        """
        convert datetime instance to str datetime
        """
        if isinstance(obj, datetime.datetime):
            return int(mktime(obj.timetuple()))
        return json.JSONEncoder.default(self, obj)
