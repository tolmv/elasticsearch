from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    DATABASE_URL: str = Field(..., env="DATABASE_URL")
    ELASTICSEARCH_URL: str = Field(..., env="ELASTICSEARCH_URL")
    XML_FILE_PATH: str = Field("elektronika_products_20240924_074730.xml", env="XML_FILE_PATH")
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    CHUNK_SIZE: int = Field(1000, env="CHUNK_SIZE")
    MAX_WORKERS: int = Field(5, env="MAX_WORKERS")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()