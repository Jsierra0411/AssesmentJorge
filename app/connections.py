from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from .config import settings

SQLALCHEMY_DATABASE_URL = f'postgresql://{settings.database_username}:{settings.database_password}@{settings.database_hostname}:{settings.database_port}/{settings.database_name}'

#---Logica para crear conexiones con la db

engine = create_engine(SQLALCHEMY_DATABASE_URL)
postgreSQLConnection = engine.connect()
SessionLocal = sessionmaker(autocommit=False,autoflush=False,bind=engine)

Base = declarative_base()

#--Funcion para crear una sesion 

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

