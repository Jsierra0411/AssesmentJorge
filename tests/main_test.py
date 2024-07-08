import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.main import app, get_db
from app.models import Base

#__configuración de la base de datos de pruebas

SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

#--creación de la base de datos de pruebas

Base.metadata.create_all(bind=engine)

#--dependency override para usar la base de datos de pruebas
def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db

client = TestClient(app)

def test_get_data_invalid_report():
    response = client.get("/get_data_report?report=3")
    assert response.status_code == 400
    assert response.json() == {"detail": "Incorrect value, must be 1 or 2"}

def test_insert_data_invalid_file_type():
    response = client.post(
        "/insert_data_tables",
        files={"file": ("test.txt", b"some content")},
        data={"table_insert": "jobs"}
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "The file must be a CSV"}

def test_insert_data_invalid_table_name():
    response = client.post(
        "/insert_data_tables",
        files={"file": ("test.csv", b"id,job\n1,Engineer")},
        data={"table_insert": "invalid_table"}
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Incorrect type, must be jobs,departments or hired_employees"}

def test_insert_data_success():
    csv_content = "id,department\n1,Engineering\n2,HR"
    response = client.post(
        "/insert_data_tables",
        files={"file": ("test.csv", csv_content)},
        data={"table_insert": "departments"}
    )
    assert response.status_code == 201
    assert response.json() == {"status": "INSERT_SUCCESS: table:departments file: test.csv"}
