from .config import settings

API_BASE_URL = "https://nsi.ffoms.ru/nsi-int/api"
BULK_LOAD_PAGE_SIZE = 50_000
DEFAULT_PAGE_SIZE = 200
IGNORED_COLUMNS = {"SYS_RECORDID", "version", "SYS_HASH"}
SCHEMA = settings.DATABASE_SCHEMA
