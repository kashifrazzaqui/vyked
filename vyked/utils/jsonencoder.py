import json
import uuid
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

        if isinstance(obj, uuid.UUID):
            return str(obj)

        return json.JSONEncoder.default(self, obj)
