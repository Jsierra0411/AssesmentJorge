import os
from pydantic_settings import BaseSettings
import pytest

class Settings(BaseSettings):
    database_hostname: str
    database_port: str
    database_password: str
    database_name: str
    database_username: str

    class Config:
        env_file = ".env.test"

def test_settings_loading():
    settings = Settings()
    assert settings.database_hostname == "test_hostname"
    assert settings.database_port == "1234"
    assert settings.database_password == "test_password"
    assert settings.database_name == "test_dbname"
    assert settings.database_username == "test_username"
