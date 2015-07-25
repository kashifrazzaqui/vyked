from aiohttp.web import Request as Req, Response as Res


class Request(Req):
    """
    Wraps the aiohttp request object to hide it from user
    """
    pass


class Response(Res):
    """
    Wraps the aiohttp response object to hide it from user
    """
    pass
