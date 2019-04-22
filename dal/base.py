from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.interfaces import PoolListener

import configparser
import logging,os,sys

class MyListener(PoolListener):
    def connect(self, dbapi_con, con_record):
        dbapi_con.execute('PRAGMA journal_mode=WAL')

DLlogger = logging.getLogger('DL')
DLlogger.info('TweetDal * Init *')
DLlogger.info('Reading DB ini file')
config = configparser.RawConfigParser()
config.read('config.ini')
dbname = config.get('database', 'sqlite_file')
#dbname='septoct'

engine = create_engine('sqlite:///'+dbname, echo=True, connect_args={'check_same_thread': False},listeners= [MyListener()])
#engine = create_engine('sqlite:///'+dbname, echo=True)
#engine = create_engine('mysql://username:pass@localhost/')
#engine.execute("CREATE DATABASE "+dbname)
#engine.execute("USE "+dbname)
session_factory  = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

Base = declarative_base()


