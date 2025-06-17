from tables.subscription import Subscription
def check_integer(s):
    try:
        s = int(s)
        result = True
    except ValueError as e:
        result = False
    return result

class SubscriptionTable(object):
    """
    Class representing a table with all the subscriptions to context information
    """

    def __init__(self):
        self._table = {}

    def add_entry(self, sub: Subscription):
        if sub is not None:
            self._table[sub.get_object_id()] = sub

    def remove_entry(self, entry_id):
        if entry_id is not None and entry_id in self._table:
            del self._table[entry_id]
        return None

    def search_entry(self, entity_id, entity_type, scope, subscriber_address, subscriber_port):
        if entity_id is not None and entity_type is not None and scope is not None and subscriber_address is not None and subscriber_port is not None and check_integer(subscriber_port):
            key_identifier = hash(entity_id + entity_type + scope + subscriber_address + str(subscriber_port))
            if key_identifier in self._table:
                return self._table[key_identifier]
        return None

    def print(self):
        return str(self._table)