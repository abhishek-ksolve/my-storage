from sqlalchemy import create_engine 
from sqlalchemy import Column, String, SmallInteger ,DateTime, BigInteger, Integer
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import JSON
import constant

user = constant.user
password = constant.password
host =constant.host
database = constant.database
dbSchema = constant.schema
db_string = "postgresql://"+user+":"+password+"@"+host+"/"+database

db = create_engine(db_string)  
Base = declarative_base()


class Job(Base):  
    __tablename__ = 'job'
    __table_args__ = {'schema': dbSchema}
    
    job_uuid = Column(String(50), primary_key=True)
    job_definition_id = Column(BigInteger)
    end_time = Column(DateTime)
    start_time = Column(DateTime)
    create_date = Column(DateTime)
    update_date = Column(DateTime)
    status = Column(String(25))
    aws_glue_job_id = Column(String(100))
    parent_job_uuid = Column(String(50))
    job_submission_details = Column(JSON)
    job_result =  Column(JSON)
    

class JobDefinition(Base):  
    __tablename__ = 'job_definition'
    __table_args__ = {'schema': dbSchema}

    id = Column(BigInteger, primary_key=True)
    datasource_id = Column(BigInteger)
    infra_provider_id = Column(SmallInteger)
    create_date = Column(DateTime)
    update_date = Column(DateTime)
    algo_config = Column(JSON)



Session = sessionmaker(db)  
session = Session()
# Base.metadata.create_all(db)
