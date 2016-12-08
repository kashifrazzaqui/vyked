from vyked.exceptions import VykedServiceException


class ServiceException(VykedServiceException):
    pass


class NotFoundException(VykedServiceException):
    pass


class NotFoundExceptionWithSuccess(VykedServiceException):
    pass


class ForbiddenException(VykedServiceException):
    pass


class UnAuthorizeException(VykedServiceException):
    pass


class UserNotVerified(VykedServiceException):
    pass


class ResourceConflictException(VykedServiceException):
    pass
