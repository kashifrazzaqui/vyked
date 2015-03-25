import logging
from vyked.utils.log import patch_async_emit

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = patch_async_emit(logging.StreamHandler())
logger.addHandler(handler)

