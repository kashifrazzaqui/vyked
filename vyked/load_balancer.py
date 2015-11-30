import random
from collections import defaultdict


class LoadBalancer:
    def __init__(self):
        self._available_services = defaultdict(list)
        self.rrc = -1

    def add_instance(self, vendor_name, address):
        # Avoid duplication
        instance = (address['host'], address['port'], address['node_id'], address['type'], address['weight'],
                    address['strategy'])
        if instance not in self._available_services[vendor_name]:
            self._available_services[vendor_name].append(instance)

    def remove_instance(self, vendor_name, node_id):
        for instance in self._available_services[vendor_name]:
            if instance[2] == node_id:
                self._available_services[vendor_name].remove(instance)
                break

    def get_instance(self, service_name, service_type):
        instances = self._available_services[service_name]
        instances = [i for i in instances if i[3] == service_type]
        if not len(instances):
            return None
        strategy = None
        for i in instances:
            if strategy:
                assert(strategy == i[5])
            else:
                strategy = i[5]
        return getattr(self, strategy)(instances)

    def random_strategy(self, instances):
        return random.choice(instances)

    def weighted_strategy(self, instances):
        cumulative_weights = []
        current_weight = 0
        for i in instances:
            current_weight += i[4]
            cumulative_weights.append(current_weight)
        lottery = random.randint(0, current_weight-1)
        for i in range(len(cumulative_weights)):
            if lottery < cumulative_weights[i]:
                return instances[i]

    def round_robin_strategy(self, instances):
        self.rrc += 1
        self.rrc %= len(instances)
        return instances[self.rrc]
