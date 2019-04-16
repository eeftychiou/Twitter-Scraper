from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

import configparser
import logging,os,sys

DLlogger = logging.getLogger('DL')
DLlogger.info('TweetDal * Init *')
DLlogger.info('Reading DB ini file')
config = configparser.RawConfigParser()
config.read('config.ini')
dbname = config.get('database', 'sqlite_file')

engine = create_engine('sqlite:///'+dbname, echo=True)
session_factory  = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

Base = declarative_base()