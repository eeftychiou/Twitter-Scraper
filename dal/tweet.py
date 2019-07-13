from sqlalchemy import Column, String, Integer, Date , UniqueConstraint, Boolean, JSON, Text

from .base import Base


class Tweet(Base):
    __tablename__ = 'tweets'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset' : 'utf8mb4'}

    created_at = Column(String(64))
    id = Column(String(64), primary_key=True)
    text = Column(Text)
    source = Column(String(128))
    in_reply_to_status_id = Column(String(64), index=True)
    in_reply_to_user_id = Column(String(64) , index= True)
    in_reply_to_screen_name = Column(String(64))
    isReply = Column(Boolean)
    user_id = Column(String(64),index= True)
    screen_name = Column(String(64),index= True)
    isVerified = Column(Boolean)
    coordinates = Column(String(128))
    place = Column(Integer)   # 1 if place details present otherwise 0
    place_country = Column(String(512))
    place_country_code = Column(String(512))
    place_full_name = Column(String(512))
    place_id = Column(String(512))
    place_name = Column(String(512))
    place_type = Column(String(512))
    place_coord0 = Column(String(512))
    place_coord1 = Column(String(512))
    place_coord2 = Column(String(512))
    place_coord3 = Column(String(512))

    quoted_status_id = Column(String(64),index= True)
    is_quote_status = Column(Boolean)
    quoted_status = Column(Text)
    retweeted_status = Column(Text)
    isRetweet = Column(Boolean)
    quote_count = Column(Integer, default=0)
    reply_count = Column(Integer, default=0)
    retweet_count = Column(Integer,default=0)
    favorite_count = Column(Integer, default=0)
    contributors = Column(Boolean)
    lang = Column(String(512))
    truncated = Column(Boolean)
    conversationid = Column(String(64),index= True)
    isConversation = Column(Boolean)
    #Other fields relevant
    hasMedia = Column(Boolean, default=False)
    hasURL = Column(Boolean, default=False)        #fully expanded URL, exclude quoted tweets
    hasHashtag = Column(Boolean, default=False)
    hasSymbols = Column(Boolean, default=False)
    hasMentions = Column(Boolean, default=False)
    hasPoll = Column(Boolean, default=False)
    isSensitive = Column(Boolean, default=False)


    ts_source = Column(String(512))
    projectID = Column(Integer)
    sourceTweetStatusID = Column(String(64))




    # def __init__(self, id ,created_at, text, source,in_reply_to_status_id):
    #     self.id = id
    #     self.created_at = created_at
    #     self.text = text
    #     self.source = source


class Job(Base):
    __tablename__ = 'jobs'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'}

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String(10),index= True)
    worker = Column(Integer,index= True)
    payload = Column(String(64),index= True)
    json = Column(JSON)
    status = Column(Integer,index=True, default=0)
    retries = Column(Integer, default=0)
    begin_date = Column(String(512))
    end_date = Column(String(512))
    UniqueConstraint(job_type, payload)

    # def __init__(self ,job_type, payload):
    #
    #     self.job_type = job_type
    #     self.payload = payload

class User(Base):
    __tablename__ = 'users'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'}

    user_id = Column(String(512), primary_key=True)
    name = Column(String(512))
    screen_name = Column(String(64),index= True)
    location = Column(String(512))
    url = Column(Text)
    description = Column(Text)
    derived = Column(String(512))
    protected = Column(Boolean)
    verified = Column(Boolean)
    followers_count = Column(Integer)
    friends_count = Column(Integer)
    listed_count = Column(Integer)
    favourites_count = Column(Integer)
    statuses_count = Column(Integer)
    created_at = Column(String(512))
    geo_enabled = Column(Boolean)
    lang = Column(String(12))
    default_profile = Column(Boolean)
    default_profile_image = Column(Boolean)
    withheld_in_countries = Column(String(512))
    withheld_scope = Column(String(512))
    contributors_enabled = Column(Boolean)
    has_extended_profile = Column(Boolean)
    is_translation_enabled = Column(Boolean)
    is_translator = Column(Boolean)
    notifications = Column(Boolean)
    category = Column(String(32), index=True)

class Mention(Base):
    __tablename__ = 'mentions'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'}

    id = Column(Integer, primary_key=True)
    tweet_id = Column(String(64),index= True)
    user_id = Column(String(64))
    name = Column(String(64))
    screen_name = Column(String(64))

class Url(Base):
    __tablename__ = 'urls'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'}

    id = Column(Integer, primary_key=True)
    tweet_id = Column(String(64),index= True)
    url = Column(String(512))
    expanded_url = Column(Text)
    display_url = Column(Text)
    fully_expanded = Column(Text)
    domain = Column(String(100))
    subdomain = Column(String(45))
    suffix = Column(String(45))
    expanded = Column(Boolean)


class Hashtag(Base):
    __tablename__ = 'hashtags'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'}

    id = Column(Integer, primary_key=True)
    tweet_id = Column(String(64),index= True)
    text = Column(String(512))

class Symbol(Base):
    __tablename__ = 'symbols'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'}

    id = Column(Integer, primary_key=True)
    tweet_id = Column(String(64),index= True)
    text = Column(String(512))

class Media(Base):
    __tablename__ = 'media'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'}

    id = Column(Integer, primary_key=True)
    tweet_id = Column(String(64),index= True)
    display_url = Column(String(512))
    expanded_url = Column(Text)
    id_str = Column(String(64))
    media_url = Column(Text)
    media_url_https = Column(Text)
    source_status_id_str = Column(String(64))
    type = Column(String(512))

class Project(Base):
    __tablename__ = 'project'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'}

    proj_id = Column(Integer, primary_key=True)
    query_string = Column(String(512))
    username = Column(String(512))  #can be multiple
    since = Column(String(512))
    until = Column(String(512))
    max_tweets = Column(Integer)
    top_tweets = Column(Boolean)
    saveComments = Column(Boolean)
    maxComments = Column(Integer)
    saveCommentsofComments = Column(Boolean)




