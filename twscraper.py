# coding=utf-8
import sys
import unicodecsv as csv
import networkx as nx

import tweepy
import ConfigParser, os

from datetime import datetime, timedelta
import mytools as tt


if sys.version_info[0] < 3:
    import got
else:
    import got3 as got#


def main():
    logger = logging.getLogger('main')

    logger.info('Starting Process')

    logger.info('Reading ini file')
    config = ConfigParser.RawConfigParser()
    config.read('config.ini')

    consumer_key = config.get('credentials','consumer_key')
    consumer_secret = config.get('credentials','consumer_secret')

    access_token = config.get('credentials','access_token')

    access_token_secret = config.get('credentials','access_token_secret')

    logger.info('Authenticating')
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)

    user = api.get_user('eeftychiou')

    logger.info("Connected to Twitter Api: %s",user.status.text)


    searchTerms = 'refugee OR réfugié OR rifugiato OR flüchtling OR flykting OR ' \
                  'mülteci OR menekült OR refugees OR refugeeswelcome OR refugeecrisis OR ' \
                  'refugeesGR OR refugeeconvoy'

    #searchTerms = 'refugee'
    dateFrom = "2015-08-29"
    dateToo = "2015-09-30"
    interval = 1   #days to sample each search
    maxTweetPerInterval = -1

    dtFrom = datetime.strptime(dateFrom,'%Y-%m-%d')
    dtToo = datetime.strptime(dateToo,'%Y-%m-%d')

    #setup csv writter
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
        print (datetime.now()), 'Starting export for ', dtItfrStr, dtIttooStr

        tweetCriteria = got.manager.TweetCriteria().setQuerySearch(searchTerms).setSince(dtItfrStr).setUntil(
            dtIttooStr).setMaxTweets(maxTweetPerInterval)
        tweets = got.manager.TweetManager.getTweets(tweetCriteria)

        for t in tweets:
            writer.writerow(t.data)
            # outputFile.write(('\n%s;%s;%d;%d;%d;"%s";%s;%s;%s;"%s";%s;%s;%s' % (
            # t.username, t.date.strftime("%Y-%m-%d %H:%M"), t.retweets, t.favorites,t.replies, t.text, t.geo, t.mentions, t.hashtags,
            # t.id, t.permalink, t.conversationId, t.userID)))

        print(datetime.now()),' Rows %d saved on file...\n' % len(tweets)



if __name__ == '__main__':
    import logging
    import logging.config

    logging.config.fileConfig('logConfig.cfg')

    main()