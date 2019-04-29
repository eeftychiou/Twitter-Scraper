# coding=utf-8
import csv
import tweepy
import configparser, random
import dal
import threading
import json
import time
from multiprocessing import Pool,Process
import logging
import logging.config


from datetime import datetime, timedelta

import tools

import got3 as got


def main():
    #testing()
    logger = logging.getLogger('main')

    logger.info('Starting Process')

    logger.info('Reading ini file')
    config = configparser.RawConfigParser()
    config.read('config.ini')

    consumer_key = config.get('twitter credentials','consumer_key')
    consumer_secret = config.get('twitter credentials','consumer_secret')

    access_token = config.get('twitter credentials','access_token')

    access_token_secret = config.get('twitter credentials','access_token_secret')

    savetocsv = config.getboolean('CSV','enabled')

    saveComments = config.getboolean('webscraper','saveComments')
    saveCommentsofComments = config.getboolean('webscraper','saveCommentsofComments')
    maxComments  = config.getint('webscraper','maxComments')
    webscraper = config.getboolean('webscraper','enabled')

    #get proxy settings

    proxies, useProxy, proxyRetries = tools.get_proxy_cfg(config, logger)

    logger.info('Authenticating')
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)

    user = api.get_user('eeftychiou')

    logger.info("Connected to Twitter Api: %s",user._api.last_response.status_code)

    logger.info("Connecting to Database")
    dbacc = dal.TweetDal()


    #New instance of Tweet Manager
    TM = got.manager.TweetManager(DBSession=dbacc)
    #Set proxy settings
    TM.useProxy=useProxy
    if useProxy:
        TM.proxies = proxies
        TM.retries = proxyRetries
        TM.proxiesWeights = [TM.retries] * len(TM.proxies)


    # start consumer and continue scrapping
    consumersEnabled = config.getboolean("consumers", 'enabled')
    if consumersEnabled:
        logger.info("Removing any stop commands")
        dbacc.clearStops()

        logger.info("Starting Workers")
        opList = config.get('consumers','consumers_cfg').strip().split(',')

        TW_process =[]
        for op in opList:
            processID = Process(target=TWconsumer, args=(op,))
            TW_process.append(processID)
            processID.start()
            time.sleep(5)



    if not webscraper:
        logger.info("Websraper is disabled * exiting main thread *")
        return



    terms = config.get('webscraper','terms').strip().split(',')
    searchTerms = ' OR '.join(terms)
    dateFrom = config.get('webscraper','dateFrom')
    dateToo = config.get('webscraper','dateToo')


    interval = config.getint('webscraper', 'interval')
    maxTweetPerInterval = config.getint('webscraper', 'maxTweetPerInterval')

    dtFrom = datetime.strptime(dateFrom,'%Y-%m-%d')
    dtToo = datetime.strptime(dateToo,'%Y-%m-%d')

    #setup csv writter
    if savetocsv:
        csv.register_dialect('myDialect', delimiter=';', quoting=csv.QUOTE_ALL)
        fname = dateFrom + dateToo + "_dump.csv"
        outputFile =open(fname, "w+", encoding='utf-8')
        myFields = ['date', 'username', 'to', 'replies', 'retweets' , 'favorites', 'text','geo','mentions','hashtags','id','permalink','conversationid']
        writer = csv.DictWriter(outputFile, fieldnames=myFields, dialect='myDialect')
        writer.writeheader()

    logger.info('*** Criteria *** ')
    logger.info('searchTerms[%s]',searchTerms)
    logger.info('dateFrom[%s] to:[%s] interval[%i] maxTweetPerInterval[%i]', dateFrom,dateToo, interval,maxTweetPerInterval)

    Criteria={}
    Criteria['querysearch'] = searchTerms
    Criteria['username'] = None
    Criteria['since'] = dateFrom
    Criteria['until'] = dateToo
    Criteria['maxTweets'] =  maxTweetPerInterval
    Criteria['topTweets'] = False
    Criteria['saveComments'] = saveComments
    Criteria['maxComments'] = maxComments
    Criteria['saveCommentsofComments'] = saveCommentsofComments

    proj_id = dbacc.add_project(Criteria)

    for dtItfr in tools.daterange(dtFrom,dtToo, interval):
        dtItfrStr = dtItfr.strftime("%Y-%m-%d")
        dtItToo = dtItfr + timedelta(interval)
        dtIttooStr = dtItToo.strftime("%Y-%m-%d")
        logger.info ('Starting export for from: %s to: %s  ', dtItfrStr, dtIttooStr )

        tweetCriteria = got.manager.TweetCriteria().setQuerySearch(searchTerms).\
            setSince(dtItfrStr).\
            setUntil(dtIttooStr).\
            setMaxTweets(maxTweetPerInterval).\
            setSaveComments(saveComments).\
            setMaxComments(maxComments).\
            setSaveCommentsofComments(saveCommentsofComments).\
            setProjId(proj_id)

        tweets = TM.getTweets(tweetCriteria)

        if useProxy:
            now = datetime.now()
            if (now.minute % 10) == 0:
                logger.info("Proxies in list are: {}".format(' '.join(map(str, TM.proxies))))
                logger.info("Proxies Weights in list are: {}".format(' '.join(map(str, TM.proxiesWeights))))
                logger.info("Proxies Weights len [%i] sum [%i]", len(TM.proxiesWeights), sum(TM.proxiesWeights))




        data={}
        if savetocsv:
            logger.info(' Rows %d saved to file...\n' % len(tweets))
            #TODO export jobs payload



    logger.info('Finished Processing')




# TODO move into own package
def TWconsumer(toggle):



    logging.config.fileConfig('logConfig.cfg')
    TWlogger = logging.getLogger('TW')
    TWlogger.info('%s - Reading ini file', toggle)
    config = configparser.RawConfigParser()
    config.read('config.ini')

    TWlogger.info("%s - Connecting to Database", toggle)
    TWdbacc = dal.TweetDal()

    TMW = got.manager.TweetManager(DBSession=TWdbacc)


    toggleParse = toggle.strip().split('-')

    if toggleParse[0] in ['userApi','tweetApi']:
        consumer_key = config.get('twitter credentials','consumer_key')
        consumer_secret = config.get('twitter credentials','consumer_secret')

        access_token = config.get('twitter credentials','access_token')

        access_token_secret = config.get('twitter credentials','access_token_secret')

        TWlogger.info('%s - Authenticating', toggle)
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        api = tweepy.API(auth, wait_on_rate_limit=True)

        user = api.get_user('eeftychiou')

        TWlogger.info("%s - Connected to Twitter Api: %s",toggle, user._api.last_response.status_code)

    elif toggleParse[0] =='tweet':

        saveComments = config.getboolean('webscraper','saveComments')
        saveCommentsofComments = config.getboolean('webscraper','saveCommentsofComments')
        maxComments  = config.getint('webscraper','maxComments')
        webscraper = config.getboolean('consumers','webscraper')
        # get proxy settings
        proxies, useProxy, proxyRetries = tools.get_proxy_cfg(config, TWlogger)

        TMW.useProxy = useProxy
        if useProxy:
            TMW.proxies = proxies
            TMW.retries = proxyRetries
            TMW.proxiesWeights = [TMW.retries] * len(TMW.proxies)


    time.sleep(10)

    Done = False

    while not Done:

        if toggleParse[0] == 'userApi':
            processUser(TWdbacc, TWlogger, api, toggle)

        elif toggleParse[0] == 'tweetApi':
            processTweetApi(TWdbacc, TWlogger, api, toggle)

        elif toggleParse[0] == 'tweet' and webscraper:

            processTweet(TMW, TWdbacc, TWlogger, maxComments, saveComments, saveCommentsofComments,toggle,useProxy)
        elif toggleParse[0] ==' tweet' and webscraper == False:
            TWlogger.info("%s - TS * Tweet Scrapper Disabled", toggle)
            return

        StopCmd = TWdbacc.getStops()

        if StopCmd:
            TWlogger.info("%s Received Stop Command exiting",toggle)
            print("{} Received Stop Command exiting".format(toggle))
            exit(0)
        time.sleep(1)









def processUser(TWdbacc, TWlogger, api, toggle):
    limit = 5
    type = toggle.split('-')
    ids = TWdbacc.get_jobs(type[0], limit, 0)
    if len(ids) == 0:
        time.sleep(60)
        return
    TWlogger.info("%s - Got [%i] of %s",toggle,  len(ids), toggle)
    TWlogger.info("%s - user * Started", toggle)
    TWlogger.info("%s - Tweepy Processing %i %s",toggle,  len(ids), toggle)
    for userid in ids:

        try:
            if not TWdbacc.userExists(userid):
                user = api.get_user(user_id=userid)
                try:
                    if not TWdbacc.userExists(userid):
                        TWdbacc.add_user(user)

                except Exception as e:
                    print("Exception userApi worker ", str(e))
                    TWlogger.error("%s - userApi worker exception %s",toggle, str(e))
                    #TWdbacc.session.rollback()
                    TWdbacc.increament_job('userApi', userid)
                TWdbacc.session.commit()
        except tweepy.TweepError as e:
            if e.api_code in [63, 50]:
                TWdbacc.complete_job("userApi", userid)
                TWlogger.info('%s - Tweepy Exception *UserApi [%s] Error Code: , %s',toggle, userid, e.reason)
                TWdbacc.session.commit()
    TWlogger.info("user * Finished")



def processTweet(TMW, TWdbacc, TWlogger, maxComments, saveComments, saveCommentsofComments, toggle, useProxy):


    if useProxy:
        now = datetime.now()
        if (now.minute%10)==0:
            TWlogger.info("Proxies in list are: {}".format(' '.join(map(str, TMW.proxies))))
            TWlogger.info("Proxies Weights in list are: {}".format(' '.join(map(str, TMW.proxiesWeights))))
            TWlogger.info("Proxies Weights len [%i] sum [%i]", len(TMW.proxiesWeights), sum(TMW.proxiesWeights))
    limit = 5
    type = toggle.split('-')
    ids = TWdbacc.get_jobs(type[0], limit,type[1])
    if len(ids) == 0:
        time.sleep(60)
        return
    TWlogger.info("TS * Started")
    TWlogger.info("Statusids {}".format(' '.join(map(str, ids))))
    TWlogger.info("TS Processing %i %ss", len(ids), toggle)
    for scr_tweet in ids:
        tWscrap = json.loads(ids[scr_tweet])
        TWlogger.info("TS Processing %s", scr_tweet)
        tweetCriteria = got.manager.TweetCriteria().setMaxTweets(-1). \
            setSaveComments(saveComments). \
            setMaxComments(maxComments). \
            setUsername(json.loads(ids[scr_tweet])['username']). \
            setStatusID(scr_tweet).setSaveCommentsofComments(saveCommentsofComments). \
            setProjId(tWscrap['projectID'])

        stTweet = TMW.getStatusPage(tweetCriteria, tWscrap)

        if stTweet == None:
            TWlogger.info("TS * Could not scrape tweet [%s]", scr_tweet)

            TWdbacc.increament_job("tweet", scr_tweet)
            continue

        TWlogger.info("TS Done Processing Status Page of  %s with %i replies", scr_tweet, stTweet.replies)
        if stTweet.replies and saveComments:
            TWlogger.info("TS Processing %i replies for %s", stTweet.replies, scr_tweet)
            tweets = TMW.getComments(tweetCriteria)
        else:
            tweets = []

        tweets.append(stTweet)

        tweetIDs = [[x.id, json.dumps(tools.getDict(x))] for x in tweets if not TWdbacc.tweetExists(x.id)]

        TWlogger.info("Adding Processed [%i] tweets to Jobs for tweepy processing", len(tweetIDs))
        if len(tweetIDs):
            TWdbacc.add_jobs('tweetApi', tweetIDs)

        TWdbacc.complete_job("tweet", scr_tweet)

        TWlogger.info("Done Adding Processed tweets to Database")
    TWlogger.info("tweet * Finished")
    return


def processTweetApi(TWdbacc, TWlogger, api, toggle):
    limit = 100
    type = toggle.split('-')
    ids = TWdbacc.get_jobs(type[0], limit, type[1])
    if len(ids) == 0:
        time.sleep(60)
        return
    TWlogger.info("%s - tweetApi * Started",toggle)
    TWlogger.info("%s - Tweepy Processing %i %s",toggle, len(ids), toggle)
    TWlogger.info("Statusids {}".format(' '.join(map(str, ids))))
    api_tweets = api.statuses_lookup(list(ids.keys()), include_entities=True, tweet_mode='extended')
    TWlogger.info("%s - Adding Processed tweets to Database",toggle)
    for tweet in api_tweets:
        try:
            if not TWdbacc.tweetExists(tweet.id_str):
                TWdbacc.add_tweet(tweet, ids[tweet.id_str])

        except Exception as e:
            print("Exception Worker unable to process tweet ", tweet.id_str, str(e))
            TWlogger.error("%s - tweetApi worker exception %s",toggle, str(e))
            TWdbacc.session.rollback()
            TWdbacc.increament_job("tweetApi", tweet.id_str)
        TWdbacc.session.commit()
    TWlogger.info("%s - Done Adding Processed tweets to Database",toggle)
    TWlogger.info("%s - tweetApi * Finished",toggle)



def testing():
    tweetCriteria = got.manager.TweetCriteria().setMaxTweets(-1). \
        setSaveComments(True). \
        setMaxComments(-1). \
        setUsername('IlhanMN').setStatusID('1108026064364859393')
    tweets = got.manager.TweetManager.getStatusJsonResponseSC(tweetCriteria)


if __name__ == '__main__':


    logging.config.fileConfig('logConfig.cfg')

    main()