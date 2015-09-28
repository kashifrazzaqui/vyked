import collections


class OrderedClassMembers(type):
    @classmethod
    def __prepare__(self, name, bases):
        return collections.OrderedDict()

    def __new__(self, name, bases, classdict):
        classdict['__ordered__'] = [key for key in classdict.keys() if key not in ('__module__', '__qualname__')]
        for each in bases: #add keys from base classes also
            classdict['__ordered__'] += [key for key in each.__dict__ if key not in ('__module__', '__qualname__')]

        return type.__new__(self, name, bases, classdict)
