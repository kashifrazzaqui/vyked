class Entity:
    """
    objects which are defined by their identity
    """
    def __init__(self, id):
        self._id = id

    @property
    def id(self):
        return self._id


class Value:
    """
    describe a property of a thing, and they don’t have their own identity or lifecycle
    """
    @staticmethod
    def create(*args, **kwargs):
        pass


class Aggregate:
    """
    groups of objects which are treated as a single unit
    """
    pass


class Factory:
    """
    used to encapsulate the creation logic of an object or an aggregate
    """
    pass


class Repository():
    """
    used to fetch entities from the used data storage and save the information of entities to it.
    """
    pass
