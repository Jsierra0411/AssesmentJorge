import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import Base, departments, jobs, Hired_Employee

@pytest.fixture(scope='module')
def engine():
    return create_engine('sqlite:///:memory:')

@pytest.fixture(scope='module')
def tables(engine):
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)

@pytest.fixture(scope='module')
def session(engine, tables):
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()

def test_departments_table(session):
    department = departments(id=1, department="HR")
    session.add(department)
    session.commit()
    result = session.query(departments).filter_by(id=1).first()
    assert result is not None
    assert result.department == "HR"

def test_jobs_table(session):
    job = jobs(id=1, job="Engineer")
    session.add(job)
    session.commit()
    result = session.query(jobs).filter_by(id=1).first()
    assert result is not None
    assert result.job == "Engineer"

def test_hired_employee_table(session):
    employee = Hired_Employee(id=1, name="John Doe", datetime="2024-07-03 10:00:00", deparment_id=1, job_id=1)
    session.add(employee)
    session.commit()
    result = session.query(Hired_Employee).filter_by(id=1).first()
    assert result is not None
    assert result.name == "John Doe"
    assert result.datetime == "2024-07-03 10:00:00"
    assert result.deparment_id == 1
    assert result.job_id == 1
