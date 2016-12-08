import json
from functools import wraps
from .Service_exceptions import *
from vyked.exceptions import RequestException
from aiohttp.web import Response

HTTP_STATUS_CODES = {
    'SUCCESS': 200,
    'CREATED': 201,
    'MULTI_STATUS': 207,
    'BAD_REQUEST': 400,
    'NOT_FOUND': 404,
    'FORBIDDEN': 403,
    'UNAUTHORIZED': 401,
    'INTERNAL_SERVER_ERROR': 500,
    'CONFLICT': 409
}

ERROR_MESSAGE = {
    'UNKNOWN_ERROR': 'unknown error occurred',
    'ERROR_CODE': 'error_code',
    'ERROR_MESSAGE': 'error_message'
}


def get_error_as_json(val):
    re = {'error': val}
    return json.dumps(re).encode()


def json_file_to_dict(_file: str) -> dict:
    """
    convert json file data to dict

    :param str _file: file location including name

    :rtype: dict
    :return: converted json to dict
    """
    config = None
    with open(_file) as config_file:
        config = json.load(config_file)

    return config


def get_result_from_tcp_response(response):
    if not response:
        raise ServiceException(ERROR_MESSAGE['UNKNOWN_ERROR'])
    if ERROR_MESSAGE['ERROR_CODE'] in response:
        error_code = response[ERROR_MESSAGE['ERROR_CODE']]
        if error_code == 204:
            raise NotFoundException(response[ERROR_MESSAGE['ERROR_MESSAGE']])
        elif error_code == 201:
            raise UnAuthorizeException(response[ERROR_MESSAGE['ERROR_MESSAGE']])
        elif error_code == 205:
            raise ResourceConflictException(response[ERROR_MESSAGE['ERROR_MESSAGE']])
        elif error_code == 203:
            raise ForbiddenException(response[ERROR_MESSAGE['ERROR_MESSAGE']])
        else:
            raise ServiceException(response[ERROR_MESSAGE['ERROR_MESSAGE']])
    else:
        return response['result']


def exception_handler_http(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = yield from func(*args, **kwargs)
            return result
        except NotFoundException as ne:
            return Response(status=HTTP_STATUS_CODES['NOT_FOUND'], body=get_error_as_json(str(ne)),
                            content_type='application/*json')
        except ForbiddenException as fe:
            return Response(status=HTTP_STATUS_CODES['FORBIDDEN'], body=get_error_as_json(str(fe)),
                            content_type='application/*json')
        except UnAuthorizeException as fe:
            return Response(status=HTTP_STATUS_CODES['UNAUTHORIZED'], body=get_error_as_json(str(fe)),
                            content_type='application/*json')
        except ResourceConflictException as ae:
            return Response(status=HTTP_STATUS_CODES['CONFLICT'], body=get_error_as_json(str(ae)),
                            content_type='application/*json')
        except ServiceException as ee:
            return Response(status=HTTP_STATUS_CODES['BAD_REQUEST'], body=get_error_as_json(str(ee)),
                            content_type='application/*json')
        except Exception as e:
            return Response(status=HTTP_STATUS_CODES['INTERNAL_SERVER_ERROR'], body=get_error_as_json(str(e)),
                            content_type='application/*json')

    return wrapper


def get_response_from_tcp(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            response = yield from func(*args, **kwargs)
            result = {
                'result': response
            }
            return result
        except RequestException as e:
            error = e.error.split('_')
            error_code = error[0]
            if len(error) > 1:
                error_message = error[1]
            else:
                error_message = ERROR_MESSAGE['UNKNOWN_ERROR']
            result = {
                ERROR_MESSAGE['ERROR_CODE']: int(error_code),
                ERROR_MESSAGE['ERROR_MESSAGE']: error_message
            }
            return result

    return wrapper


