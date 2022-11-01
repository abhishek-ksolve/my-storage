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
db_string = "postgresql://"+user+":"+password+"@"+host+"/"+database

db = create_engine(db_string)  
Base = declarative_base()


class AlgoConfigs(Base):  
    __tablename__ = 'algo_config'
    __table_args__ = {'schema': 'er'}
    
    id = Column(SmallInteger, primary_key=True)
    create_date = Column(DateTime)
    update_date = Column(DateTime)
    algo_id = Column(BigInteger)
    config_name = Column(String)
    config_parameter_value = Column(String)
    description = Column(String)
    type = Column(String)
    status = Column(SmallInteger)
    required = Column(Integer,nullable=False, default=1)
    


class Algorithms(Base):  
    __tablename__ = 'algorithms'
    __table_args__ = {'schema': 'er'}

    id = Column(SmallInteger, primary_key=True)
    name = Column(String(30))
    create_date = Column(DateTime)
    update_date = Column(DateTime)
    workflow_id = Column(SmallInteger)
    logical_name = Column(String(120))
    description = Column(String(500))
    status = Column(String)
    

Session = sessionmaker(db)  
session = Session()
# Base.metadata.create_all(db)