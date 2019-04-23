from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.interfaces import PoolListener
import sqlalchemy as sa


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
MYSQL_DB = config.get('database', 'MYSQL_DB')
MYSQL_PORT = config.get('database', 'MYSQL_PORT')
MYSQL_HOST = config.get('database', 'MYSQL_HOST')
MYSQL_USER = config.get('database', 'MYSQL_USER')
MYSQL_PASSWD = config.get('database', 'MYSQL_PASSWD')
MYSQL_DROPANDRECREATE = config.getboolean('database', 'MYSQL_DROPANDRECREATE')


#engine = create_engine('sqlite:///'+dbname, echo=True, connect_args={'check_same_thread': False},listeners= [MyListener()])




def create_mysql_pool():
    mysql_host = MYSQL_HOST
    mysql_port = MYSQL_PORT

    mysql_db = MYSQL_DB
    mysql_user = MYSQL_USER
    mysql_passwd = MYSQL_PASSWD

    # max_connections default for mysql = 100
    # set mysql connections to 90 and 5 for sqlalchemy buffer
    mysql_pool = create_engine(
        'mysql://{user}:{passwd}@{host}/?charset=utf8mb4'.format(
            user=mysql_user,
            passwd=mysql_passwd,
            host=mysql_host
        ),
        pool_size=10,
        max_overflow=5,
        pool_recycle=3600,
    )

    #mysql_pool.execute("DROP DATABASE IF EXISTS {db}".format(db=mysql_db))

    try:
        mysql_pool.execute("USE {db}".format(
            db=mysql_db)
        )
    except sa.exc.OperationalError:
        DLlogger.info('DATABASE {db} DOES NOT EXIST. CREATING...'.format(
            db=mysql_db)
        )
        mysql_pool.execute("CREATE DATABASE {db}".format(
            db=mysql_db)
        )
        mysql_pool.execute("USE {db}".format(
            db=mysql_db)
        )
    mysql_pool = create_engine(
        'mysql://{user}:{passwd}@{host}/{db}?charset=utf8mb4'.format(
            user=mysql_user,
            passwd=mysql_passwd,
            host=mysql_host,
            db=mysql_db
        ),
        pool_size=10,
        pool_recycle=3600,
    )
    return mysql_pool

Base = declarative_base()
# init_mysql_pool = create_mysql_pool()
# Base.metadata.create_all(init_mysql_pool, checkfirst=True)
# init_mysql_pool.dispose()




