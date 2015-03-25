import logging
from vyked.utils.log import patch_async_emit

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = patch_async_emit(logging.StreamHandler())
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

