import sqlite3
import ConfigParser
import logging, os, sys
from datetime import datetime, date
from tweet import Tweet, Job, User
from base import Session, engine, Base

if sys.version_info[0] < 3:
    import got
else:
    import got3 as got

DLlogger = logging.getLogger('DL')


class TweetDal:

    def __init__(self):
        DLlogger.info('TweetDal * Init *')
        DLlogger.info('Reading DB ini file')
        config = ConfigParser.RawConfigParser()
        config.read('config.ini')

        Base.metadata.create_all(engine)
        self.session = Session()

    def add_tweet(self, tweet):
        """
        :param tweet:  tweepy object making up a tweet
        :return:
        """
        DLlogger.info('insert_tweet ')

        #create object
        tweet_obj = Tweet(id=tweet.id,
                          created_at=tweet.created_at)

        if hasattr(tweet, "text"): tweet_obj.text = tweet.text
        if hasattr(tweet, "source"): tweet_obj.source = tweet.source
        if hasattr(tweet, "in_reply_to_status_id"):
            tweet_obj.in_reply_to_status_id = tweet.in_reply_to_status_id
            if not self.tweetExists(tweet_obj.in_reply_to_status_id) and tweet_obj.in_reply_to_status_id !=None:
                self.add_job('tweet',tweet_obj.in_reply_to_status_id)

        if hasattr(tweet, "in_reply_to_user_id"):
            tweet_obj.in_reply_to_user_id = tweet.in_reply_to_user_id
            if not self.userExists(tweet_obj.in_reply_to_user_id) and tweet_obj.in_reply_to_user_id != None:
                self.add_job('user', tweet_obj.in_reply_to_user_id)

        if hasattr(tweet, "in_reply_to_screen_name"): tweet_obj.in_reply_to_screen_name = tweet.in_reply_to_screen_name
        if hasattr(tweet, "author"):
            tweet_obj.user_id = tweet.author.id_str
            if not self.userExists(tweet_obj.user_id):
                self.add_user(tweet.author)

        if tweet.place:
            tweet_obj.place=1
            if hasattr(tweet.place,"country"): tweet_obj.place_country=tweet.place.country
            if hasattr(tweet.place, "country_code"): tweet_obj.place_country_code = tweet.place.country_code
            if hasattr(tweet.place, "full_name"): tweet_obj.place_full_name = tweet.place.full_name
            if hasattr(tweet.place, "id"): tweet_obj.place_id = tweet.place.id
            if hasattr(tweet.place, "name"): tweet_obj.place_name = tweet.place.name
            if hasattr(tweet.place, "place_type"): tweet_obj.place_type = tweet.place.place_type

            if hasattr(tweet.place.bounding_box,"coordinates"):
                tweet_obj.place_coord0 = str(tweet.place.bounding_box.coordinates[0][0])
                tweet_obj.place_coord1 = str(tweet.place.bounding_box.coordinates[0][1])
                tweet_obj.place_coord2 = str(tweet.place.bounding_box.coordinates[0][2])
                tweet_obj.place_coord3 = str(tweet.place.bounding_box.coordinates[0][3])
        else:
            tweet_obj.place = 0

        if hasattr(tweet,"quoted_status_id_str"):
            tweet_obj.quoted_status_id = tweet.quoted_status_id_str
            if not self.tweetExists(tweet_obj.quoted_status_id) and tweet_obj.quoted_status_id != None:
                self.add_job('tweet', tweet_obj.quoted_status_id)

        if hasattr(tweet,"quoted_status"): tweet_obj.quoted_status = tweet.quoted_status.text

        if hasattr(tweet, "is_quote_status"): tweet_obj.is_quote_status = tweet.is_quote_status

        if  hasattr(tweet, "retweeted_status"):
            tweet_obj.retweeted_status = tweet.quoted_status.retweeted_status.id
            if not self.tweetExists(tweet_obj.retweeted_status) and tweet_obj.retweeted_status !=None:
                self.add_job('tweet',tweet_obj.retweeted_status)

        if hasattr(tweet, "quote_count"): tweet_obj.quote_count = tweet.quote_count
        if hasattr(tweet, "reply_count"): tweet_obj.reply_count = tweet.reply_count
        if hasattr(tweet, "retweet_count"): tweet_obj.retweet_count = tweet.retweet_count
        if hasattr(tweet, "favorite_count"): tweet_obj.favorite_count = tweet.favorite_count
        if tweet.coordinates:  tweet_obj.coordinates = tweet.coordinates

        self.session.add(tweet_obj)

        DLlogger.info('Tweet ID  %i added', tweet.id)
        self.complete_job("tweet", tweet.id_str)
        self.session.commit()

    def add_job(self, jobtype, payload):
        """
        Adds a single job to the task queue
        :param job:
        :return:
        """

        DLlogger.info('add_job - %s 0 %s', jobtype, payload)
        job_obj = Job(job_type=jobtype, payload=payload)
        self.session.add(job_obj)
        self.session.commit()

    def add_jobs(self, jobtype, jobs):
        """
        Adds a batch of jobs into the queue
        :param jobs:
        :return:
        """
        DLlogger.info('add_jobs[%s] -  Number: %i', jobtype, len(jobs))
        jobList = []
        for jobid in jobs:
            jobList.append(Job(job_type=jobtype, payload=jobid))

        self.session.bulk_save_objects(jobList)
        self.session.commit()

    def complete_job(self, jobtype, payload):
        """
        Completes the queue row corresponding to the jobtype and payload
        :param jobtype:
        :param payload:
        :return:
        """
        end_date = str(datetime.now())
        job_obj = self.session.query(Job).filter(Job.job_type == jobtype).filter(Job.payload == payload).first()

        if job_obj:
            job_obj.end_date = end_date
            job_obj.status = 2
            self.session.add(job_obj)
            self.session.commit()

    def increament_job(self, jobtype, payload):
        """
        Completes the queue row corresponding to the jobtype and payload
        :param jobtype:
        :param payload:
        :return:
        """
        end_date = str(datetime.now())
        job_obj = self.session.query(Job).filter(Job.job_type == jobtype).filter(Job.payload == payload).first()

        if job_obj:
            job_obj.end_date = end_date
            job_obj.status = 2
            self.session.add(job_obj)
            self.session.commit()

    def tweetExists(self, tweetID):
        DLlogger.info('tweetExists - %s', tweetID)

        id = self.session.query(Tweet).filter_by(id=tweetID).first()

        if id == None:
            DLlogger.info('There is no tweet named %s' % tweetID)
            return False
        else:
            DLlogger.info('Tweet %s found in %s row(s)' % (tweetID, id))
            return True

    def userExists(self, userID):
        DLlogger.info('userExists - %s', userID)

        id = self.session.query(User).filter_by(user_id=userID).first()

        if id == None:
            DLlogger.info('There is no tweet named %s' % userID)
            return False
        else:
            DLlogger.info('Tweet %s found' % userID)
            return True


    def get_jobs(self, jobtype, n):
        """
        gets a number of specified jobs pending jobs from the queue and update them accordingly
        :param jobtype:
        :return: a list of ids, this can be tweet_ids, user_ids but not mixed within the same list
        """
        DLlogger.info('get_jobs[%i] - %s ', n, jobtype)

        jobs = self.session.query(Job).filter(Job.status == 0) \
            .filter(Job.job_type == jobtype).filter(Job.retries <= 3). \
            filter(Job.begin_date == None).limit(n)

        idlist = []
        for obj in jobs:
            obj.retries = obj.retries + 1
            obj.status = 1
            obj.begin_date = str(datetime.now())
            idlist.append(obj.payload)
        self.session.bulk_save_objects(jobs)
        self.session.commit()

        return idlist

    def add_user(self,user):
        """
        Adds the user to the Users Table
        :param user: user tweepy object
        :return:
        """
        DLlogger.info('add_user ')
        #create object
        user_obj = User(user_id= user.id_str)

        if hasattr(user, "contributors_enabled"): user_obj.contributors_enabled = user.contributors_enabled
        if hasattr(user, "created_at"): user_obj.created_at = user.created_at
        if hasattr(user, "default_profile"): user_obj.default_profile = user.default_profile
        if hasattr(user, "default_profile_image"): user_obj.default_profile_image = user.default_profile_image
        if hasattr(user, "description"): user_obj.description = user.description
        if hasattr(user, "favourites_count"): user_obj.favourites_count = user.favourites_count
        if hasattr(user, "followers_count"): user_obj.followers_count = user.followers_count
        if hasattr(user, "following"): user_obj.following = user.following
        if hasattr(user, "friends_count"): user_obj.friends_count = user.friends_count
        if hasattr(user, "geo_enabled"): user_obj.geo_enabled = user.geo_enabled
        if hasattr(user, "has_extended_profile"): user_obj.has_extended_profile = user.has_extended_profile
        if hasattr(user, "is_translation_enabled"): user_obj.is_translation_enabled = user.is_translation_enabled
        if hasattr(user, "is_translator"): user_obj.is_translator = user.is_translator
        if hasattr(user, "lang"): user_obj.lang = user.lang
        if hasattr(user, "listed_count"): user_obj.listed_count = user.listed_count
        if hasattr(user, "location"): user_obj.location = user.location
        if hasattr(user, "name"): user_obj.name = user.name
        if hasattr(user, "notifications"): user_obj.notifications = user.notifications
        if hasattr(user, "protected"): user_obj.protected = user.protected
        if hasattr(user, "screen_name"): user_obj.screen_name = user.screen_name
        if hasattr(user, "statuses_count"): user_obj.statuses_count = user.statuses_count
        if hasattr(user, "url"): user_obj.url = user.url
        if hasattr(user, "verified"): user_obj.verified = user.verified


        self.session.add(user_obj)

        DLlogger.info('User ID  %s added', user_obj.user_id)
        self.complete_job("user", user_obj.user_id)
        self.session.commit()