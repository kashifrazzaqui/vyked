from .jsonprotocol import VykedProtocol


def get_vyked_protocol(handler):
    return VykedProtocol(handler)
