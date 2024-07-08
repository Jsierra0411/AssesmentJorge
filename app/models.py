from sqlalchemy import Column, Integer, String
from .connections import Base


#--tablas db

class departments(Base):
    __tablename__ = "departments"

    id = Column(Integer,primary_key= True, nullable= False)
    department = Column(String, nullable= True)

class jobs(Base):
    __tablename__ = "jobs"

    id = Column(Integer,primary_key= True, nullable= False) 
    job = Column(String, nullable= True)
    
class Hired_Employee(Base):
    __tablename__ = "hired_employees"

    id = Column(Integer,primary_key= True, nullable= False)
    name = Column(String, nullable= True)
    datetime = Column(String, nullable= True)
    deparment_id = Column(Integer, nullable= True)
    job_id = Column(Integer, nullable=  True)  
    
    
    
