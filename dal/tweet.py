from sqlalchemy import Column, String, Integer, Date , UniqueConstraint, Boolean

from base import Base


class Tweet(Base):
    __tablename__ = 'tweets'

    created_at = Column(String)
    id = Column(String, primary_key=True)
    text = Column(String)
    source = Column(String)
    in_reply_to_status_id = Column(String)
    in_reply_to_user_id = Column(String)
    in_reply_to_screen_name = Column(String)
    user_id = Column(String)
    coordinates = Column(String)

    place = Column(Integer)   # 1 if place details present otherwise 0
    place_country = Column(String)
    place_country_code = Column(String)
    place_full_name = Column(String)
    place_id = Column(String)
    place_name = Column(String)
    place_type = Column(String)
    place_coord0 = Column(String)
    place_coord1 = Column(String)
    place_coord2 = Column(String)
    place_coord3 = Column(String)

    quoted_status_id = Column(String)
    is_quote_status = Column(Boolean)
    quoted_status = Column(String)
    retweeted_status = Column(String)
    quote_count = Column(Integer, default=0)
    reply_count = Column(Integer, default=0)
    retweet_count = Column(Integer,default=0)
    favorite_count = Column(Integer, default=0)
    contributors = Column(Boolean)
    lang = Column(String)
    truncated = Column(String)


    # def __init__(self, id ,created_at, text, source,in_reply_to_status_id):
    #     self.id = id
    #     self.created_at = created_at
    #     self.text = text
    #     self.source = source


class Job(Base):
    __tablename__ = 'jobs'

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String)
    payload = Column(String)
    status = Column(Integer, default=0)
    retries = Column(Integer, default=0)
    begin_date = Column(String)
    end_date = Column(String)
    UniqueConstraint(job_type, payload, sqlite_on_conflict='IGNORE')

    # def __init__(self ,job_type, payload):
    #
    #     self.job_type = job_type
    #     self.payload = payload

class User(Base):
    __tablename__ = 'users'

    user_id = Column(String, primary_key=True)
    name = Column(String)
    screen_name = Column(String)
    location = Column(String)
    url = Column(String)
    description = Column(String)
    derived = Column(String)
    protected = Column(Boolean)
    verified = Column(Boolean)
    followers_count = Column(Integer)
    friends_count = Column(Integer)
    listed_count = Column(Integer)
    favourites_count = Column(Integer)
    statuses_count = Column(Integer)
    created_at = Column(String)
    geo_enabled = Column(Boolean)
    lang = Column(String)
    default_profile = Column(Boolean)
    default_profile_image = Column(Boolean)
    withheld_in_countries = Column(String)
    withheld_scope = Column(String)
    contributors_enabled = Column(Boolean)
    has_extended_profile = Column(Boolean)
    is_translation_enabled = Column(Boolean)
    is_translator = Column(Boolean)
    notifications = Column(Boolean)

class Mention(Base):
    __tablename__ = 'mentions'

    id = Column(Integer, primary_key=True)
    tweet_id = Column(String)
    user_id = Column(String)
    name = Column(String)
    screen_name = Column(String)

class Hashtag(Base):
    __tablename__ = 'hashtags'

    id = Column(Integer, primary_key=True)
    tweet_id = Column(String)
    text = Column(String)



