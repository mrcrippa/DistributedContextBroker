def check_if_string_is_valid_integer_bigger_than_zero(value: str) -> bool:
    """check if a string is a valid integer bigger than zero"""
    result = False
    try:
        s = int(value)
        if s > 0:
            result = True
    except ValueError:
        result = False
    return result


class ContextInfo(object):
    """Class representing an entry in the context info table"""

    def __init__(self):
        self._provider_id = ""
        self._entity_id = ""
        self._entity_type = ""
        self._scope = ""
        self._data = ""
        "timestamps for the beginning and the end of the time period when the context info is valid"
        "time expressed as milliseconds UNIX time"
        "use the result from round(time.time() * 1000)"
        self._validity_begin = 0
        self._validity_end = 0

    def get_object_id(self) -> str:
        """unique identifier for this object"""
        return str(hash(self._entity_id + self._entity_type + self._scope))

    def get_provider_id(self) -> str:
        return self._provider_id

    def set_provider_id(self, provider_id: str):
        if provider_id is not None:
            self._provider_id = provider_id

    def get_entity_id(self) -> str:
        return self._entity_id

    def set_entity_id(self, entity_id: str):
        if entity_id is not None:
            self._entity_id = entity_id

    def get_entity_type(self) -> str:
        return self._entity_type

    def set_entity_type(self, entity_type: str):
        if entity_type is not None:
            self._entity_type = entity_type

    def get_scope(self) -> str:
        return self._scope

    def set_scope(self, scope: str):
        if scope is not None:
            self._scope = scope

    def get_data(self) -> str:
        return self._data

    def set_data(self, data: str):
        if data is not None:
            self._data = data

    def get_validity_begin(self) -> int:
        """value for the beginning of the time period when the context info is valid, time expressed as milliseconds UNIX time (round(time.time() * 1000))"""
        return self._validity_begin

    def set_validity_begin(self, validity_begin: str):
        """timestamp for the beginning of the time period when the context info is valid, time expressed as milliseconds UNIX time (round(time.time() * 1000))"""
        if validity_begin is not None and check_if_string_is_valid_integer_bigger_than_zero(validity_begin):
            self._validity_begin = int(validity_begin)

    def get_validity_end(self) -> int:
        """value for the end of the time period when the context info is valid, time expressed as milliseconds UNIX time (round(time.time() * 1000))"""
        return self._validity_end

    def set_validity_end(self, validity_end: str):
        """timestamp for the end of the time period when the context info is valid, time expressed as milliseconds UNIX time (round(time.time() * 1000))"""
        if validity_end is not None and check_if_string_is_valid_integer_bigger_than_zero(validity_end):
            self._validity_end = int(validity_end)

    def print(self) -> str:
        contents = {"CONTEXT_INFO_ID": self.get_object_id(), "PROVIDER_ID": self.get_provider_id(),
                    "ENTITY_ID": self.get_entity_id(), "ENTITY_TYPE": self.get_entity_type(), "SCOPE": self.get_scope(),
                    "DATA": self.get_data(), "VALIDITY_BEGIN": self.get_validity_begin,
                    "VALIDITY_END": self.get_validity_end()}

        return str(contents)
