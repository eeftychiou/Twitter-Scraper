# coding=utf-8
import csv
import tweepy
import configparser, random
import dal
import threading
import json


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
    useProxy = config.getboolean('webscraper', 'useProxy')
    getProxyList = config.getboolean("webscraper", 'getProxyList')
    proxyRetries = config.getint('webscraper','retries')
    if getProxyList and useProxy:
        pManager = tools.ProxyManager('https://twitter.com/i/MeettheBlues/conversation/1108057971194556416?include_available_features=1&include_entities=1&max_position=&reset_error_state=false',
                                          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36')
        # Refresh the status of the proxies we pulled on initialization
        pManager.refresh_proxy_status()
        got.manager.TweetManager.proxies = pManager.get_proxies_key_value('alive',True)
        got.manager.TweetManager.retries=proxyRetries
        got.manager.TweetManager.proxiesWeights = [got.manager.TweetManager.retries] * len(got.manager.TweetManager.proxies)
    elif useProxy and not getProxyList:
        proxyList = config.items('proxyList')
        if len(proxyList)==0:
            logger.info('Incorrect Proxy configuration, not using proxy')
            useProxy=False
        else:
            proxies = []
            [proxies.append(p[1]) for p in proxyList]
            got.manager.TweetManager.proxies = proxies
            got.manager.TweetManager.proxiesWeights = [got.manager.TweetManager.retries] * len(
                got.manager.TweetManager.proxies)

    elif not useProxy:
        logger.info('Not using proxy')
    else:
        logger.info('Incorrect Proxy configuration,not using proxy')
        useProxy = False


    logger.info('Authenticating')
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)

    user = api.get_user('eeftychiou')

    logger.info("Connected to Twitter Api: %s",user._api.last_response.status_code)

    logger.info("Connecting to Database")
    dbacc = dal.TweetDal()

    TM = got.manager.TweetManager(DBSession=dbacc)
    TM.useProxy=useProxy

    # start consumer and continue scrapping
    consumersEnabled = config.getboolean("consumers", 'enabled')
    if consumersEnabled:
        logger.info("Starting Workers")
        opList = ['tweetApi', 'userApi', 'tweet','tweet','tweet', 'tweet' ]
        TW_thread={}
        for op in opList:
            TW_thread[op] = threading.Thread(target=TWconsumer, args=(op,))
            TW_thread[op].start()

    if not webscraper:
        logger.info("Websraper is disabled * exiting main thread *")
        return


    #TODO load criteria from ini file
    searchTerms = 'refugee OR réfugié OR rifugiato OR flüchtling OR flykting OR ' \
                  'mülteci OR menekült OR refugees OR refugeeswelcome OR refugeecrisis OR ' \
                  'refugeesGR OR refugeeconvoy'

    # 1700 Replies https://twitter.com/PeteButtigieg/status/1112349547471273984
    # searchTerms = 'potential refugee crisis caused by great suffering in Central America'
    # dateFrom = "2019-03-31"
    # dateToo = "2019-04-01"


    # 15 replies or so
    # searchTerms = 'refugee crisis Venezuelan Usurper Maduro'
    # dateFrom = "2019-03-19"
    # dateToo = "2019-03-20"

    # project tweets
    searchTerms = 'displaced OR immigrant OR migrant OR migration OR refugee OR asylum seeker OR trafficking OR border'
    dateFrom = "2015-08-02"
    dateToo = "2015-08-04"

    # mixed tweets 25 root tweets
    # searchTerms = 'black hole heart'
    # dateFrom = "2019-04-01"
    # dateToo = "2019-04-03"

    interval = 1   #days to sample each search
    maxTweetPerInterval = -1

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
            logger.info("Proxies remaining in num_list are: {}".format(' '.join(map(str, TM.proxies))))

        data={}
        if savetocsv:
            logger.info(' Rows %d saved to file...\n' % len(tweets))
            #TODO export jobs payload



    logger.info('Finished Processing')





# TODO move into own package
def TWconsumer(toggle):
    import logging.config

    import time

    logging.config.fileConfig('logConfig.cfg')
    TWlogger = logging.getLogger('TW')
    TWlogger.info('Reading ini file')
    config = configparser.RawConfigParser()
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

    TMW = got.manager.TweetManager(DBSession=TWdbacc)

    #get proxy settings
    useProxy = config.getboolean('webscraper', 'useProxy')
    getProxyList = config.getboolean("webscraper", 'getProxyList')
    proxyRetries = config.getint('webscraper', 'retries')
    if getProxyList and useProxy:
        pManager = tools.ProxyManager('https://twitter.com/i/MeettheBlues/conversation/1108057971194556416?include_available_features=1&include_entities=1&max_position=&reset_error_state=false',
                                          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36')
        # Refresh the status of the proxies we pulled on initialization
        pManager.refresh_proxy_status()
        TMW.proxies = pManager.get_proxies_key_value('alive',True)
        TMW.retries = proxyRetries
        TMW.proxiesWeights = [TMW.retries] * len(TMW.proxies)

    elif useProxy and not getProxyList:
        proxyList = config.items('proxyList')
        if len(proxyList)==0:
            TWlogger.info('Incorrect Proxy configuration, not using proxy')
            useProxy=False
        else:
            proxies = []
            [proxies.append(p[1]) for p in proxyList]
            TMW.proxies=proxies
            TMW.proxiesWeights = [TMW.retries] * len(TMW.proxies)
    elif not useProxy:
        TWlogger.info('Not using proxy')
    else:
        TWlogger.info('Incorrect Proxy configuration,not using proxy')
        useProxy = False

    TMW.useProxy=useProxy

    time.sleep(60)

    Done = False

    while not Done:



        if toggle=='tweet':
            limit = 10
        else:
            limit = 100
        ids = TWdbacc.get_jobs(toggle, limit)
        TWlogger.info("Got [%i] of %s",len(ids), toggle)

        if len(ids) == 0 or toggle=='wait':
            time.sleep(5)
            continue

        if toggle=='tweetApi':   #get tweet data from API with json and add to database
            TWlogger.info("tweetApi * Started")
            TWlogger.info("Tweepy Processing %i %ss",len(ids),  toggle)
            api_tweets = api.statuses_lookup(list(ids.keys()), include_entities=True, tweet_mode='extended')
            TWlogger.info("Adding Processed tweets to Database")

            for tweet in api_tweets:
                try:
                    if not TWdbacc.tweetExists(tweet.id_str):
                        TWdbacc.add_tweet(tweet, ids[tweet.id_str])

                except Exception as e:
                    print ("Worker unable to process tweet ",tweet.id_str )
                    TWlogger.error("tweetApi worker exception %s", str(e))
                    TWdbacc.session.rollback()
                    TWdbacc.increament_job("tweetApi", tweet.id_str)
                TWdbacc.session.commit()
            TWlogger.info("Done Adding Processed tweets to Database")
            TWlogger.info("tweetApi * Finished")

        elif toggle == 'tweet':  #web scrap and add
            TWlogger.info("TS * Started")
            TWlogger.info("TS Processing %i %ss",len(ids),  toggle)
            for scr_tweet in ids:
                TWlogger.info("TS Processing %s", scr_tweet)
                tweetCriteria = got.manager.TweetCriteria().setMaxTweets(-1). \
                    setSaveComments(True). \
                    setMaxComments(-1). \
                    setUsername(json.loads(ids[scr_tweet])['username']).\
                    setStatusID(scr_tweet).setSaveCommentsofComments(False)

                stTweet = TMW.getStatusPage(tweetCriteria)

                if stTweet==None:
                    TWlogger.info("TS * Could not scrape tweet [%s]", scr_tweet)

                    TWdbacc.increament_job("tweet", scr_tweet)
                    continue
                TWlogger.info("TS Done Processing Status Page of  %s with %i replies", scr_tweet, stTweet.replies)
                if stTweet.replies:
                    TWlogger.info("TS Processing %i replies for %s", stTweet.replies, scr_tweet)
                    tweets = TMW.getComments(tweetCriteria)
                else:
                    tweets=[]

                tweets.append(stTweet)

                tweetIDs = [[x.id,json.dumps(tools.getDict(x))] for x in tweets if not TWdbacc.tweetExists(x.id)]

                TWlogger.info("Adding Processed [%i] tweets to Jobs for tweepy processing",len(tweetIDs))
                if len(tweetIDs):
                    TWdbacc.add_jobs('tweetApi', tweetIDs)

                TWdbacc.complete_job("tweet", scr_tweet)

                TWlogger.info("Done Adding Processed tweets to Database")
            TWlogger.info("tweet * Finished")


        elif toggle=='userApi':
            TWlogger.info("user * Started")
            TWlogger.info("Tweepy Processing %i %ss", len(ids), toggle)
            for userid in ids:

                try:
                    if not TWdbacc.userExists(userid):
                        user = api.get_user(user_id =userid)
                        try:
                            TWdbacc.add_user(user)

                        except Exception as e:
                            print("userApi worker exception ",str(e))
                            TWlogger.error("userApi worker exception %s",str(e))
                            TWdbacc.session.rollback()
                            TWdbacc.increament_job('userApi',userid)
                        TWdbacc.session.commit()
                except tweepy.TweepError as e:
                    if e.api_code in [63,50]:
                        TWdbacc.increament_job("userApi", userid)
                        TWlogger.info('Tweepy Exception *UserApi [%s] Error Code: , %s',userid, e.reason)
                        TWdbacc.session.commit()
                    print (e)
            TWlogger.info("user * Finished")



def testing():
    tweetCriteria = got.manager.TweetCriteria().setMaxTweets(-1). \
        setSaveComments(True). \
        setMaxComments(-1). \
        setUsername('IlhanMN').setStatusID('1108026064364859393')
    tweets = got.manager.TweetManager.getStatusJsonResponseSC(tweetCriteria)


if __name__ == '__main__':
    import logging
    import logging.config

    logging.config.fileConfig('logConfig.cfg')

    main()