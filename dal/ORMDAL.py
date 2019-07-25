import configparser
import logging, sys, json, random
from datetime import datetime
from .tweet import Tweet, Job, User, Mention, Hashtag, Url, Symbol,Media, Project
from .base import create_mysql_pool, Base
from sqlalchemy.orm import scoped_session, sessionmaker, load_only
from sqlalchemy.sql import text
import warnings, MySQLdb
import requests
import urllib
import tldextract
import re
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from urllib.parse import urlparse, parse_qs

warnings.filterwarnings('ignore', category=MySQLdb.Warning)

import tools

if sys.version_info[0] < 3:
    pass
else:
    pass

DLlogger = logging.getLogger('DL')



class TweetDal:

    def __init__(self):

        mysql_pool = create_mysql_pool()
        session_factory = sessionmaker(mysql_pool)

        Base.metadata.create_all(mysql_pool)
        self.session  = scoped_session(session_factory)


    def get_tweetDict(self, tweet, jsonstr):

        # create dict
        twDict = dict()
        twDict['id'] = tweet.id
        twDict['created_at'] = tweet.created_at

        twScrap = json.loads(jsonstr)

        if tweet.truncated:
            twDict['truncated'] =True
            twDict['text'] =twScrap['text']
        else:
            twDict['truncated'] =False
            if hasattr(tweet, "text"):
                twDict['text'] =tweet.text
            elif hasattr(tweet, "full_text"):
                twDict['text'] =tweet.full_text
            else:
                DLlogger.error('insert_tweet * missing text')
                sys.exit(1)

        if hasattr(tweet, "source"): twDict['source'] =tweet.source

        if hasattr(tweet, "in_reply_to_status_id"):  twDict['in_reply_to_status_id'] =tweet.in_reply_to_status_id

        if hasattr(tweet, "in_reply_to_user_id"): twDict['in_reply_to_user_id'] =tweet.in_reply_to_user_id

        if hasattr(tweet, "in_reply_to_screen_name"): twDict['in_reply_to_screen_name'] =tweet.in_reply_to_screen_name

        if twDict['in_reply_to_status_id']:
            twDict['isReply'] =True
        else:
            twDict['isReply'] =False

        if hasattr(tweet, "author"):
            twDict['user_id'] =tweet.author.id_str
            twDict['screen_name'] =tweet.author.screen_name
            twDict['isVerified'] =tweet.author.verified

        if hasattr(tweet, "lang"):
            twDict['lang'] =tweet.lang

        if tweet.place:
            twDict['place'] =1
            if hasattr(tweet.place, "country"): twDict['place_country'] =tweet.place.country
            if hasattr(tweet.place, "country_code"): twDict['place_country_code'] =tweet.place.country_code
            if hasattr(tweet.place, "full_name"): twDict['place_full_name'] =tweet.place.full_name
            if hasattr(tweet.place, "id"): twDict['place_id'] =tweet.place.id
            if hasattr(tweet.place, "name"): twDict['place_name'] =tweet.place.name
            if hasattr(tweet.place, "place_type"): twDict['place_type'] =tweet.place.place_type

            if hasattr(tweet.place.bounding_box, "coordinates"):
                twDict['place_coord0'] =str(tweet.place.bounding_box.coordinates[0][0])
                twDict['place_coord1'] =str(tweet.place.bounding_box.coordinates[0][1])
                twDict['place_coord2'] =str(tweet.place.bounding_box.coordinates[0][2])
                twDict['place_coord3'] =str(tweet.place.bounding_box.coordinates[0][3])
        else:
            twDict['place'] =0

        if hasattr(tweet, "quoted_status_id_str"): twDict['quoted_status_id'] =tweet.quoted_status_id_str

        if hasattr(tweet, "quoted_status"): twDict['quoted_status'] =tweet.quoted_status.full_text

        if hasattr(tweet, "is_quote_status"): twDict['is_quote_status'] =tweet.is_quote_status

        if hasattr(tweet, "retweeted_status"):
            twDict['retweeted_status'] =tweet.quoted_status.retweeted_status.id
            twDict['isRetweet'] =True
        else:
            twDict['isRetweet'] =False

        if hasattr(tweet, "quote_count"): twDict['quote_count'] = tweet.quote_count

        twDict['reply_count'] = twScrap['replies'] or 0

        if hasattr(tweet, "retweet_count"): twDict['retweet_count'] = tweet.retweet_count
        if hasattr(tweet, "favorite_count"): twDict['favorite_count'] = tweet.favorite_count
        if tweet.coordinates:
            if tweet.coordinates['type'] == 'Point':
                twDict['coordinates'] = ','.join(
                    (str(tweet.coordinates['coordinates'][0]), str(tweet.coordinates['coordinates'][1])))
            else:
                print("Non suppported coordinates type found")

        twDict['conversationid'] = twScrap['conversationid']
        if twDict['conversationid']:
            twDict['isConversation'] = True
        else:
            twDict['isConversation'] = False

        # entities bools
        if hasattr(tweet, "entities"):
            if 'urls' in tweet.entities:
                twDict['hasURL'] = bool(len(tweet.entities['urls']))
            else:
                twDict['hasURL'] = False

            if 'hashtags' in tweet.entities:
                twDict['hasHashtag'] = bool(len(tweet.entities['hashtags']))
            else:
                twDict['hasHashtag'] = False

            if 'user_mentions' in tweet.entities:
                twDict['hasMentions'] = bool(len(tweet.entities['user_mentions']))
            else:
                twDict['hasMentions'] = False

            if 'symbols' in tweet.entities:
                twDict['hasSymbols'] = bool(len(tweet.entities['symbols']))
            else:
                twDict['hasSymbols'] = False

            if 'media' in tweet.entities:
                twDict['hasMedia'] = bool(len(tweet.entities['media']))
            else:
                twDict['hasMedia'] = False

        if hasattr(tweet, "possibly_sensitive"):
            twDict['isSensitive'] = tweet.possibly_sensitive

        twDict['ts_source'] = twScrap.get('ts_source')
        twDict['projectID'] = twScrap.get('projectID')
        twDict['sourceTweetStatusID'] = twScrap.get('sourceTweetStatusID')


        ins = Tweet.__table__.insert().prefix_with('IGNORE').values( twDict )

        try:
            self.session.execute(ins)
            self.session.commit()
            self.complete_job("tweetApi", tweet.id_str)
            DLlogger.info('Tweet ID  %s added', twDict['id'])
        except Exception as e:
            DLlogger.error('* Exception[%s] in tweet ID  %s ', tweet.id_str, str(e))
            print('add_tweet * Exception[%s] in User ID  %s ' % (tweet.id_str, str(e)))


        ######################################################
        # Adding found tweets and users
        ######################################################
        if hasattr(tweet, "in_reply_to_status_id"):
            twDict['in_reply_to_status_id'] = tweet.in_reply_to_status_id
            if not self.tweetExists(twDict['in_reply_to_status_id']) and twDict['in_reply_to_status_id'] !=None:
                pdict={}
                pdict['username'] = tweet.in_reply_to_screen_name
                pdict['sourceTweetStatusID'] = twDict['id']
                pdict['projectID'] = twDict['projectID']
                self.add_job('tweet',random.randint(0,9),(twDict['in_reply_to_status_id'],json.dumps(pdict)))
                self.session.commit()

        if hasattr(tweet, "in_reply_to_user_id"):
            twDict['in_reply_to_user_id'] = tweet.in_reply_to_user_id
            if not self.userExists(twDict['in_reply_to_user_id']) and twDict['in_reply_to_user_id'] != None:
                self.add_job('userApi',0, (twDict['in_reply_to_user_id'],None))
                self.session.commit()

        if hasattr(tweet, "author"):
            twDict['user_id'] = tweet.author.id_str
            if not self.userExists(twDict['user_id']):
                self.add_user(tweet.author)
                self.session.commit()

        if hasattr(tweet,"quoted_status_id_str"):
            twDict['quoted_status_id'] = tweet.quoted_status_id_str
            if not self.tweetExists(twDict['quoted_status_id']) and twDict['quoted_status_id'] != None:
                if hasattr(tweet, "quoted_status"):
                    pdict={}
                    pdict['username'] = tweet.quoted_status.author.screen_name
                    pdict['sourceTweetStatusID'] = twDict['id']
                    pdict['projectID'] = twDict['projectID']
                    self.add_job('tweet',random.randint(0,9), (twDict['quoted_status_id'],json.dumps(pdict)))
                    self.session.commit()
                    if not self.userExists(tweet.quoted_status.author.id_str):
                        self.add_user(tweet.quoted_status.author)
                        self.session.commit()

        if  hasattr(tweet, "retweeted_status"):
            twDict['retweeted_status'] = tweet.quoted_status.retweeted_status.id
            if not self.tweetExists(twDict['retweeted_status']) and twDict['retweeted_status'] !=None:
                pdict={}
                pdict['username'] = tweet.quoted_status.author.screen_name
                pdict['sourceTweetStatusID'] = twDict['id']
                pdict['projectID'] = twDict['projectID']
                self.add_job('tweet',random.randint(0,9),(twDict['retweeted_status'],json.dumps(pdict)))
                self.session.commit()

        #####################
        # Processing Entities
        #####################

        #Mentions
        if twDict['hasMentions']:
            for mention in tweet.entities['user_mentions']:
                #add to jobs for processing if not present
                if not self.userExists(mention['id']):
                    self.add_job('userApi',0, (mention['id'],None))
                self.add_mention(mention,tweet.id_str)

        if twDict['hasURL']:
            for url in tweet.entities['urls']:
                self.add_url(url, tweet.id_str)

        if twDict['hasHashtag']:
            for hashtag in tweet.entities['hashtags']:
                self.add_hashtag(hashtag, tweet.id_str)

        if twDict['hasMedia']:
            for media in tweet.extended_entities['media']:
                self.add_media(media,tweet.id_str)

        DLlogger.info('Tweet ID  %i added', tweet.id)



    def add_tweet(self, tweet, jsonstr):
        """
        :param tweet:  tweepy object making up a tweet
               json : json object with additional information gathered from the scrapper if available
        :return:
        """
        DLlogger.info('add_tweet %s',tweet.id)

        self.get_tweetDict(tweet, jsonstr)
        return

        #create object
        tweet_obj = Tweet(id=tweet.id,
                          created_at=tweet.created_at)


        twScrap = json.loads(jsonstr)

        if tweet.truncated:
            tweet_obj.truncated=True
            tweet_obj.text = twScrap['text']
        else:
            tweet_obj.truncated = False
            if hasattr(tweet, "text"): tweet_obj.text = tweet.text
            elif hasattr(tweet, "full_text"): tweet_obj.text = tweet.full_text
            else:
                DLlogger.error('insert_tweet * missing text')
                sys.exit(1)

        if hasattr(tweet, "source"): tweet_obj.source = tweet.source

        if hasattr(tweet, "in_reply_to_status_id"):  tweet_obj.in_reply_to_status_id = tweet.in_reply_to_status_id

        if hasattr(tweet, "in_reply_to_user_id"): tweet_obj.in_reply_to_user_id = tweet.in_reply_to_user_id

        if hasattr(tweet, "in_reply_to_screen_name"): tweet_obj.in_reply_to_screen_name = tweet.in_reply_to_screen_name

        if tweet_obj.in_reply_to_status_id:
            tweet_obj.isReply = True
        else:
            tweet_obj.isReply = False

        if hasattr(tweet, "author"):
            tweet_obj.user_id = tweet.author.id_str
            tweet_obj.screen_name = tweet.author.screen_name
            tweet_obj.isVerified = tweet.author.verified

        if hasattr(tweet, "lang"):
            tweet_obj.lang = tweet.lang


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

        if hasattr(tweet,"quoted_status_id_str"): tweet_obj.quoted_status_id = tweet.quoted_status_id_str

        if hasattr(tweet,"quoted_status"): tweet_obj.quoted_status = tweet.quoted_status.full_text

        if hasattr(tweet, "is_quote_status"): tweet_obj.is_quote_status = tweet.is_quote_status

        if  hasattr(tweet, "retweeted_status"):
            tweet_obj.retweeted_status = tweet.quoted_status.retweeted_status.id
            tweet_obj.isRetweet = True
        else:
            tweet_obj.isRetweet = False

        if hasattr(tweet, "quote_count"): tweet_obj.quote_count = tweet.quote_count

        tweet_obj.reply_count = twScrap['replies'] or 0

        if hasattr(tweet, "retweet_count"): tweet_obj.retweet_count = tweet.retweet_count
        if hasattr(tweet, "favorite_count"): tweet_obj.favorite_count = tweet.favorite_count
        if tweet.coordinates:
            if tweet.coordinates['type'] == 'Point':
                tweet_obj.coordinates = ','.join((str(tweet.coordinates['coordinates'][0]),str(tweet.coordinates['coordinates'][1])))
            else:
                print("Non suppported coordinates type found")

        tweet_obj.conversationid = twScrap['conversationid']
        if tweet_obj.conversationid:
            tweet_obj.isConversation = True
        else:
            tweet_obj.isConversation = False



        #entities bools
        if hasattr(tweet,"entities"):
            if 'urls' in tweet.entities:
                tweet_obj.hasURL = bool(len(tweet.entities['urls']))
            else:
                tweet_obj.hasURL = False

            if 'hashtags' in tweet.entities:
                tweet_obj.hasHashtag = bool(len(tweet.entities['hashtags']))
            else:
                tweet_obj.hasHashtag = False

            if 'user_mentions' in tweet.entities:
                tweet_obj.hasMentions = bool(len(tweet.entities['user_mentions']))
            else:
                tweet_obj.hasMentions = False

            if 'symbols' in tweet.entities:
                tweet_obj.hasSymbols = bool(len(tweet.entities['symbols']))
            else:
                tweet_obj.hasSymbols = False

            if 'media' in tweet.entities:
                tweet_obj.hasMedia = bool(len(tweet.entities['media']))
            else:
                tweet_obj.hasMedia = False

        if hasattr(tweet,"possibly_sensitive"):
            tweet_obj.isSensitive = tweet.possibly_sensitive

        tweet_obj.ts_source = twScrap.get('ts_source')
        tweet_obj.projectID = twScrap.get('projectID')
        tweet_obj.sourceTweetStatusID = twScrap.get('sourceTweetStatusID')



        self.session.add(tweet_obj)
        self.complete_job("tweetApi", tweet.id_str)
        self.session.commit()

        ######################################################
        # Adding found tweets and users
        ######################################################
        if hasattr(tweet, "in_reply_to_status_id"):
            tweet_obj.in_reply_to_status_id = tweet.in_reply_to_status_id
            if not self.tweetExists(tweet_obj.in_reply_to_status_id) and tweet_obj.in_reply_to_status_id !=None:
                pdict={}
                pdict['username'] = tweet.in_reply_to_screen_name
                pdict['sourceTweetStatusID'] = tweet_obj.id
                pdict['projectID'] = tweet_obj.projectID
                self.add_job('tweet',random.randint(0,9),(tweet_obj.in_reply_to_status_id,json.dumps(pdict)))
                self.session.commit()

        if hasattr(tweet, "in_reply_to_user_id"):
            tweet_obj.in_reply_to_user_id = tweet.in_reply_to_user_id
            if not self.userExists(tweet_obj.in_reply_to_user_id) and tweet_obj.in_reply_to_user_id != None:
                self.add_job('userApi',0, (tweet_obj.in_reply_to_user_id,None))
                self.session.commit()

        if hasattr(tweet, "author"):
            tweet_obj.user_id = tweet.author.id_str
            if not self.userExists(tweet_obj.user_id):
                self.add_user(tweet.author)
                self.session.commit()

        if hasattr(tweet,"quoted_status_id_str"):
            tweet_obj.quoted_status_id = tweet.quoted_status_id_str
            if not self.tweetExists(tweet_obj.quoted_status_id) and tweet_obj.quoted_status_id != None:
                if hasattr(tweet, "quoted_status"):
                    pdict={}
                    pdict['username'] = tweet.quoted_status.author.screen_name
                    pdict['sourceTweetStatusID'] = tweet_obj.id
                    pdict['projectID'] = tweet_obj.projectID
                    self.add_job('tweet',random.randint(0,9), (tweet_obj.quoted_status_id,json.dumps(pdict)))
                    self.session.commit()
                    if not self.userExists(tweet.quoted_status.author.id_str):
                        self.add_user(tweet.quoted_status.author)
                        self.session.commit()

        if  hasattr(tweet, "retweeted_status"):
            tweet_obj.retweeted_status = tweet.quoted_status.retweeted_status.id
            if not self.tweetExists(tweet_obj.retweeted_status) and tweet_obj.retweeted_status !=None:
                pdict={}
                pdict['username'] = tweet.quoted_status.author.screen_name
                pdict['sourceTweetStatusID'] = tweet_obj.id
                pdict['projectID'] = tweet_obj.projectID
                self.add_job('tweet',random.randint(0,9),(tweet_obj.retweeted_status,json.dumps(pdict)))
                self.session.commit()

        #####################
        # Processing Entities
        #####################

        #Mentions
        if tweet_obj.hasMentions:
            for mention in tweet.entities['user_mentions']:
                #add to jobs for processing if not present
                if not self.userExists(mention['id']):
                    self.add_job('userApi',0, (mention['id'],None))
                self.add_mention(mention,tweet.id_str)

        if tweet_obj.hasURL:
            for url in tweet.entities['urls']:
                self.add_url(url, tweet.id_str)

        if tweet_obj.hasHashtag:
            for hashtag in tweet.entities['hashtags']:
                self.add_hashtag(hashtag, tweet.id_str)

        if tweet_obj.hasMedia:
            for media in tweet.extended_entities['media']:
                self.add_media(media,tweet.id_str)

        DLlogger.info('Tweet ID  %i added', tweet.id)

    def clearStops(self):
        """
        Completes any pending STOP commands
        :return:
        """
        DLlogger.info('clearStops - Entered')
        res = Job.__table__.update().where(Job.job_type=='STOP' and Job.status==0).values(status=2)
        self.session.execute(res)
        self.session.commit()

    def getStops(self):
        """
        Check is stop command is in job queue
        :return:
        """
        DLlogger.info('getStops - Entered')

        stop = self.session.query(Job.job_type).filter(Job.job_type=='STOP').filter(Job.status==0).filter(Job.begin_date == None).scalar() is not None
        DLlogger.info('getStops - %s',stop)
        return stop



    def add_job(self, jobtype, worker, payload):
        """
        Adds a single job to the task queue
        :param job:
        :return:
        """

        DLlogger.info('add_job - %s - %s - %i', jobtype, payload[0],worker)
        job_obj = Job(job_type=jobtype,worker=worker, payload=payload[0], json=payload[1])
        tab = Job.__table__
        res = tab.insert().prefix_with('IGNORE').values(job_type=jobtype, worker=worker, payload=payload[0],
                                                        json=payload[1])
        try:
            self.session.execute(res)
            self.session.commit()
        except Exception as e:
            print(str(e))

    def add_jobs(self, jobtype, jobs):
        """
        Adds a batch of jobs into the queue
        :param jobs:
        :return:
        """
        DLlogger.info('add_jobs[%s] -  Number: %i', jobtype, len(jobs))
        jobList = []
        for jobid in jobs:
            if jobtype =='tweet':
                rand = random.randint(0,9)
            elif jobtype =='tweetApi':
                rand = random.randint(0, 2)
            elif jobtype == 'userApi':
                rand = 0
            else:
                rand=0

            tab = Job.__table__
            res = tab.insert().prefix_with('IGNORE').values(job_type=jobtype, worker=rand, payload=jobid[0],
                                                            json=jobid[1])

            try:
                self.session.execute(res)
                self.session.commit()
            except Exception as e:
                print (str(e))


    def add_mention(self, mention, tweet_id):
        """
        Adds a single mention to the mentions table
        :param job:
        :return:
        """

        DLlogger.info('add_mention - %s  %s',tweet_id,  mention['screen_name'])
        men_obj = Mention(tweet_id=tweet_id, user_id=mention['id_str'], name = mention['name'], screen_name = mention['screen_name'])
        self.session.add(men_obj)
        self.session.commit()

    def add_url(self, url, tweet_id):
        """
        Adds a single mention to the mentions table
        :param job:
        :return:
        """

        DLlogger.info('add_url - %s - %s',tweet_id, url['expanded_url'])
        url_obj = Url(tweet_id=tweet_id, url=url['url'], expanded_url = url['expanded_url'], display_url = url['display_url'])
        self.session.add(url_obj)
        self.session.commit()

    def add_hashtag(self, hashtag, tweet_id):
        """
        Adds a single mention to the mentions table
        :param job:
        :return:
        """

        DLlogger.info('add_hashtag - %s - %s',tweet_id, hashtag['text'])
        has_obj = Hashtag(tweet_id=tweet_id, text=hashtag['text'])
        self.session.add(has_obj)
        self.session.commit()

    def add_symbol(self, symbol, tweet_id):
        """
        Adds a single mention to the mentions table
        :param job:
        :return:
        """

        DLlogger.info('add_symbol - %s - %s',tweet_id, symbol['text'])
        sym_obj = Symbol(tweet_id=tweet_id, text=symbol['text'])
        self.session.add(sym_obj)
        self.session.commit()

    def add_media(self, media, tweet_id):
        """
        Adds a single mention to the mentions table
        :param job:
        :return:
        """

        DLlogger.info('add_media - %s - %s',tweet_id, media['expanded_url'])
        med_obj = Media(tweet_id=tweet_id,
                        display_url=media['display_url'],
                        expanded_url = media['expanded_url'],
                        id_str = media['id_str'],
                        media_url= media['media_url'],
                        media_url_https = media ['media_url_https'],
                        source_status_id_str = media.get('source_status_id_str'),
                        type = media['type']
                        )
        self.session.add(med_obj)
        self.session.commit()


    def complete_job(self, jobtype, payload):
        """
        Completes the queue row corresponding to the jobtype and payload
        :param jobtype:
        :param payload:
        :return:
        """
        end_date = str(datetime.now())

        res = Job.__table__.update().where(Job.job_type==jobtype).where(Job.payload == payload).values(status=2, end_date = end_date)

        try:
            self.session.execute(res)
            self.session.commit()
        except Exception as e:
            print (str(e))




    def complete_jobs(self, jobtype, payload):
        """
        Completes the queue row corresponding to the jobtype and payload
        :param jobtype:
        :param payload:
        :return:
        """
        end_date = str(datetime.now())

        for item in payload:
            job_obj = self.session.query(Job).filter(Job.job_type == jobtype).filter(Job.payload == item).first()

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
        newretries = job_obj.retries + 1
        res = Job.__table__.update().where(Job.job_type==jobtype).where(Job.payload == payload).values(status=0, begin_date = None, retries = Job.retries +1)

        try:
            self.session.execute(res)
            self.session.commit()
        except Exception as e:
            print (str(e))

    def tweetExists(self, tweetID):
        DLlogger.info('tweetExists %s', tweetID)

        if tweetID == None:
            return False

        sqltxt = "select count(*) from tweets where tweets.id = {}".format(tweetID)
        res = self.session.execute(sqltxt)

        for row in res:
            if row[0]==1:
                DLlogger.info('Tweet %s found ' % tweetID)
                return True
            else:
                DLlogger.info('There is no tweet named %s' % tweetID)
                return False


    def jobExists(self, jobtype, tweetID):
        DLlogger.info('tweetExists ')
        query = self.session.query(Job)
        query = query.options(load_only('job_type','payload'))
        id = query.filter(Job.job_type == jobtype).filter(Job.payload==tweetID).first()

        if id == None:
            DLlogger.info('There is no job[%s] named %s' , jobtype, tweetID)
            return False
        else:
            DLlogger.info('Job[%s] and id [%s] found ' ,jobtype, tweetID)
            return True

    def userExists(self, userID):
        DLlogger.info('userExists')
        query = self.session.query(User)
        query = query.options(load_only('user_id'))
        id = query.filter(User.user_id==userID).first()

        if id == None:
            DLlogger.info('There is no user named %s' % userID)
            return False
        else:
            DLlogger.info('Tweet %s found' % userID)
            return True


    def get_jobs(self, jobtype, n , worker):
        """
        gets a number of specified jobs pending jobs from the queue and update them accordingly
        :param jobtype:
        :return: a list of ids, this can be tweet_ids, user_ids but not mixed within the same list
        """
        DLlogger.info('get_jobs[%i] - %s - %s', n, jobtype, worker)


        jobs = self.session.query(Job).filter(Job.status == 0).\
            filter(Job.job_type == jobtype).filter(Job.retries <= 3).\
            filter(Job.end_date == None).filter(Job.worker==worker).limit(n)

        idlist = {}
        for obj in jobs:
            obj.status = 1
            obj.begin_date = str(datetime.now())
            idlist[obj.payload]=obj.json
        self.session.bulk_save_objects(jobs)
        self.session.commit()
        self.session.flush()
        self.session.close()

        DLlogger.info('Found [%i] jobs ', len(idlist))

        return idlist

    def add_user(self,user):
        """
        Adds the user to the Users Table
        :param user: user tweepy object
        :return:
        """
        DLlogger.info('add_user ')
        #create object
        #user_obj = User(user_id= user.id_str)

        user_obj={}
        user_obj['user_id'] = user.id_str

        if hasattr(user, "contributors_enabled"): user_obj['contributors_enabled'] = user.contributors_enabled
        if hasattr(user, "created_at"): user_obj['created_at'] = user.created_at
        if hasattr(user, "default_profile"): user_obj['default_profile'] = user.default_profile
        if hasattr(user, "default_profile_image"): user_obj['default_profile_image'] = user.default_profile_image
        if hasattr(user, "description"): user_obj['description'] = user.description
        if hasattr(user, "favourites_count"): user_obj['favourites_count'] = user.favourites_count
        if hasattr(user, "followers_count"): user_obj['followers_count'] = user.followers_count
        if hasattr(user, "friends_count"): user_obj['friends_count'] = user.friends_count
        if hasattr(user, "geo_enabled"): user_obj['geo_enabled']= user.geo_enabled
        if hasattr(user, "has_extended_profile"): user_obj['has_extended_profile'] = user.has_extended_profile
        if hasattr(user, "is_translation_enabled"): user_obj['is_translation_enabled'] = user.is_translation_enabled
        if hasattr(user, "is_translator"): user_obj['is_translator'] = user.is_translator
        if hasattr(user, "lang"): user_obj['lang'] = user.lang
        if hasattr(user, "listed_count"): user_obj['listed_count'] = user.listed_count
        if hasattr(user, "location"): user_obj['location'] = user.location
        if hasattr(user, "name"): user_obj['name'] = user.name
        if hasattr(user, "notifications"): user_obj['notifications'] = user.notifications
        if hasattr(user, "protected"): user_obj['protected'] = user.protected
        if hasattr(user, "screen_name"): user_obj['screen_name'] = user.screen_name
        if hasattr(user, "statuses_count"): user_obj['statuses_count'] = user.statuses_count
        if hasattr(user, "url"): user_obj['url'] = user.url
        if hasattr(user, "verified"): user_obj['verified'] = user.verified

        ins = User.__table__.insert().prefix_with('IGNORE').values( user_obj )

        try:
            self.session.execute(ins)
            self.session.commit()
            self.complete_job("userApi", user.id_str)
            DLlogger.info('User ID  %s added', user_obj['user_id'])

        except Exception as e:
            DLlogger.error('add_user * Exception[%s] in User ID  %s ', user.id_str, str(e))
            print('add_user * Exception[%s] in User ID  %s ' % (user_obj.user_id, str(e)))




    def add_project(self, Criteria):
        """
        add a new project to the project table and get identifier
        :return:
        """
        DLlogger.info('add_project ')
        #create object
        prj_obj = Project(query_string= Criteria['querysearch'],
                           username = Criteria['username'],
                           since = Criteria['since'],
                           until = Criteria['until'],
                           max_tweets= Criteria['maxTweets'],
                           top_tweets= Criteria['topTweets'],
                           saveComments=Criteria['saveComments'],
                           maxComments=Criteria['maxComments'],
                           saveCommentsofComments=Criteria['saveCommentsofComments'])

        ret = self.session.add(prj_obj)
        self.session.commit()
        return prj_obj.proj_id


    def update_urls(self, n ):
        """
        gets a number of specified urls pending jobs from the queue and update them accordingly
        :param jobtype:
        :return: a list of ids, this can be tweet_ids, user_ids but not mixed within the same list
        """
        DLlogger.info('get_url[%i] - %s ', n, "URL")

        session = requests.Session()  # so connections are recycled

        urls = self.session.query(Url).filter(Url.expanded == False).limit(n)

        thread_pool = ThreadPoolExecutor(max_workers=n)

        rowret=[]

        with concurrent.futures.ThreadPoolExecutor(max_workers=n) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {executor.submit(self.__check_url, url): url for url in urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    rowret.append(data)
                except Exception as exc:
                    print('%r generated an exception: %s' % (data.id, exc))
                else:
                    # print('ID[%i] url[%r]  is domain %s' % (data.id,data.expanded_url, data.domain))
                    pass

        self.session.bulk_save_objects(rowret)
        self.session.commit()
        self.session.flush()
        self.session.close()

        DLlogger.info('Updated [%i] URLS ', n)

        return

    def __check_url(self, urlrow):
        #print(urlrow.expanded_url)
        url = urlrow.expanded_url

        try:
            resp = requests.head(url, allow_redirects=True, timeout=30)

            parsed = urlparse(resp.url)
            qs_parsed = parse_qs(parsed.query)

            if 'url' in qs_parsed:
                if qs_parsed['url'][0][0:4] == 'http':
                    resp.url = qs_parsed['url'][0]

            urlrow.fully_expanded = resp.url
            urlrow.expanded = 1

            extr = tldextract.extract(urlrow.fully_expanded)
            urlrow.domain = extr.domain
            urlrow.subdomain = extr.subdomain
            urlrow.suffix = extr.suffix


        except Exception as e:

            if hasattr(e, "request") and hasattr(e.request, "url"):

                parsed = urlparse(e.request.url)
                qs_parsed = parse_qs(parsed.query)

                if 'url' in qs_parsed:
                    if qs_parsed['url'][0][0:4] == 'http':
                        e.request.url= qs_parsed['url'][0]

                urlrow.fully_expanded = e.request.url
                urlrow.expanded = 2


                extr = tldextract.extract(urlrow.fully_expanded)
                urlrow.domain = extr.domain
                urlrow.subdomain = extr.subdomain
                urlrow.suffix = extr.suffix
            else:
                url = urlrow.url
                try:
                    resp = requests.head(url, allow_redirects=True, timeout=5)

                    urlrow.fully_expanded = resp.url
                    urlrow.expanded = 3

                    extr = tldextract.extract(urlrow.fully_expanded)
                    urlrow.domain = extr.domain
                    urlrow.subdomain = extr.subdomain
                    urlrow.suffix = extr.suffix


                except Exception as d:
                    DLlogger.info('get_url[%s] - %s ', url, str(e))
                    if 'host=' in str(d):
                        hoststr = e.args[0].args[0]
                        url = re.findall(r"'(.*?)'", hoststr)[0]

                        extr = tldextract.extract(url)
                        urlrow.domain = extr.domain
                        urlrow.subdomain = extr.subdomain
                        urlrow.suffix = extr.suffix
                        urlrow.expanded = 4
                    elif hasattr(d, "request") and hasattr(d.request, "url"):
                        urlrow.fully_expanded = d.request.url
                        urlrow.expanded = 5


                        extr = tldextract.extract(urlrow.fully_expanded)
                        urlrow.domain = extr.domain
                        urlrow.subdomain = extr.subdomain
                        urlrow.suffix = extr.suffix
                    else:  # try to use expanded_url
                        extr = tldextract.extract(urlrow.expanded_url)
                        urlrow.domain = extr.domain
                        urlrow.subdomain = extr.subdomain
                        urlrow.suffix = extr.suffix
                        urlrow.expanded = 10

        if len(urlrow.domain)>100:
            print(urlrow.id," URL Domain too big. Expanded[",urlrow.expanded_url,"] Domain[", urlrow.domain, "] Fully expanded[", urlrow.fully_expanded, "]")
            urlrow.domain=urlrow.domain[0:100]
            urlrow.expanded = 11

        return urlrow
