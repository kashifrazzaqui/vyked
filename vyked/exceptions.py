class RequestException(Exception):
    pass


class ClientException(Exception):
    pass


class ClientNotFoundError(ClientException):
    pass


class ClientDisconnected(ClientException):
    pass
