import logging

from tfoms_nsi import load_refbook_to_db

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

load_refbook_to_db("F003", 50_000)
