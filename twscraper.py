# coding=utf-8
import sys
import unicodecsv as csv
import networkx as nx
import tweepy
import ConfigParser, os
import dal
import threading

from datetime import datetime, timedelta
import mytools as tt


if sys.version_info[0] < 3:
    import got
else:
    import got3 as got


def main():
    logger = logging.getLogger('main')

    logger.info('Starting Process')

    logger.info('Reading ini file')
    config = ConfigParser.RawConfigParser()
    config.read('config.ini')

    consumer_key = config.get('twitter credentials','consumer_key')
    consumer_secret = config.get('twitter credentials','consumer_secret')

    access_token = config.get('twitter credentials','access_token')

    access_token_secret = config.get('twitter credentials','access_token_secret')

    savetocsv = config.getboolean('CSV','enabled')
    logger.info('Authenticating')
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)

    user = api.get_user('eeftychiou')

    logger.info("Connected to Twitter Api: %s",user._api.last_response.status_code)

    logger.info("Connecting to Database")
    dbacc = dal.TweetDal()

    # start consumer and continue scrapping
    logger.info("Starting Worker")
    TW_thread = threading.Thread(target=TWconsumer)
    TW_thread.start()



    #TODO load criteria from ini file
    searchTerms = 'refugee OR réfugié OR rifugiato OR flüchtling OR flykting OR ' \
                  'mülteci OR menekült OR refugees OR refugeeswelcome OR refugeecrisis OR ' \
                  'refugeesGR OR refugeeconvoy'
    searchTerms = 'refugee'

    dateFrom = "2015-08-29"
    dateToo = "2015-09-01"
    interval = 5   #days to sample each search
    maxTweetPerInterval = 250

    dtFrom = datetime.strptime(dateFrom,'%Y-%m-%d')
    dtToo = datetime.strptime(dateToo,'%Y-%m-%d')

    #setup csv writter
    if savetocsv:
        csv.register_dialect('myDialect', delimiter=';', quoting=csv.QUOTE_ALL)
        fname = dateFrom + dateToo + "_dump.csv"
        outputFile =open(fname, "w+")
        myFields = ['username' ,'date','retweets' , 'favorites','replies','text','geo','mentions','hashtags','id','permalink','conversationId','userid']
        writer = csv.DictWriter(outputFile, fieldnames=myFields, dialect='myDialect')
        writer.writeheader()

    logger.info('*** Criteria *** ')
    logger.info('searchTerms[%s]',searchTerms)
    logger.info('dateFrom[%s] to:[%s] interval[%i] maxTweetPerInterval[%i]', dateFrom,dateToo, interval,maxTweetPerInterval)

    for dtItfr in tt.daterange(dtFrom,dtToo, interval):
        dtItfrStr = dtItfr.strftime("%Y-%m-%d")
        dtItToo = dtItfr + timedelta(interval)
        dtIttooStr = dtItToo.strftime("%Y-%m-%d")
        logger.info ('Starting export for from: %s to: %s  ', dtItfrStr, dtIttooStr )

        tweetCriteria = got.manager.TweetCriteria().setQuerySearch(searchTerms).setSince(dtItfrStr).setUntil(
            dtIttooStr).setMaxTweets(maxTweetPerInterval)
        tweets = got.manager.TweetManager.getTweets(tweetCriteria)


        if savetocsv:
            for t in tweets:
                writer.writerow(t.data)
            logger.info(' Rows %d saved to file...\n' % len(tweets))

        tweetIDs = [x.data['id'] for x in tweets if not dbacc.tweetExists(x.data['id'])]

        dbacc.add_jobs('tweet',tweetIDs)

    logger.info('Finished Processing')



# TODO move into own package
def TWconsumer():
    import logging
    import logging.config
    import itertools
    import time

    logging.config.fileConfig('logConfig.cfg')
    TWlogger = logging.getLogger('TW')
    TWlogger.info('Reading ini file')
    config = ConfigParser.RawConfigParser()
    config.read('config.ini')

    consumer_key = config.get('twitter credentials','consumer_key')
    consumer_secret = config.get('twitter credentials','consumer_secret')

    access_token = config.get('twitter credentials','access_token')

    access_token_secret = config.get('twitter credentials','access_token_secret')

    TWlogger.info('Authenticating')
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)

    user = api.get_user('eeftychiou')

    TWlogger.info("Connected to Twitter Api: %s",user._api.last_response.status_code)

    TWlogger.info("Connecting to Database")
    TWdbacc = dal.TweetDal()

    Done = False

    opList = ['tweet', 'user', 'wait' ,'done']
    g = itertools.cycle(opList)
    while not Done:
        toggle = next(g)

        ids = TWdbacc.get_jobs(toggle, 100)
        if len(ids) == 0:
            time.sleep(5)
            continue
        if toggle=='tweet':
            ids = api.statuses_lookup(ids, include_entities=True)

            for id in ids:
                TWdbacc.add_tweet(id)


        elif toggle=='user':

            for userid in ids:
                TWlogger.info("User %s" ,userid)
                try:
                    if not TWdbacc.userExists(userid):
                        user = api.get_user(user_id =userid)
                        TWdbacc.add_user(user)
                except tweepy.TweepError, e:
                    print e






if __name__ == '__main__':
    import logging
    import logging.config

    logging.config.fileConfig('logConfig.cfg')

    main()