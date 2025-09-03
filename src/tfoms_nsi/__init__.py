import logging
from logging import NullHandler

from .main import load_refbook_to_db

logging.getLogger(__name__).addHandler(NullHandler())
