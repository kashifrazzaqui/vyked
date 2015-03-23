class Entity():
    def __init__(self, id):
        self._id = id

    @property
    def id(self):
        return self._id


class Value():

    @staticmethod
    def create(*args, **kwargs):
        pass