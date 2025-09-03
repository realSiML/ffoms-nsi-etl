from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()


class Config(BaseSettings):
    DATABASE_URL: str
    DATABASE_SCHEMA: str
    DEBUG: bool = False


settings: Config = Config.model_validate({})
