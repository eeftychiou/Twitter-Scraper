from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import ConfigParser
import logging,os,sys

DLlogger = logging.getLogger('DL')
DLlogger.info('TweetDal * Init *')
DLlogger.info('Reading DB ini file')
config = ConfigParser.RawConfigParser()
config.read('config.ini')
dbname = config.get('database', 'sqlite_file')

engine = create_engine('sqlite:///'+dbname, echo=True)
Session = sessionmaker(bind=engine)

Base = declarative_base()