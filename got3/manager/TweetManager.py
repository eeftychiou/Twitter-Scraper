# -*- coding: utf-8 -*-

import json, re, datetime, sys, random, http.cookiejar, time
import urllib.request, urllib.parse, urllib.error
from pyquery import PyQuery
import ssl, uuid, configparser
from urllib.error import HTTPError, URLError
import socket, logging


from .. import models
from . import TweetCriteria
import dal
import tools


class TweetManager:
    """A class for accessing the Twitter's search engine"""

    def __init__(self, DBSession, useragents=None, settings=None):

        self.TMlogger = logging.getLogger('TM')

        if DBSession == None:
            self.TMdal = dal.TweetDal()
        else:
            self.TMdal = DBSession



        if useragents == None:
            self.user_agents = [
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:63.0) Gecko/20100101 Firefox/63.0',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:62.0) Gecko/20100101 Firefox/62.0',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:61.0) Gecko/20100101 Firefox/61.0',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:63.0) Gecko/20100101 Firefox/63.0',
            'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15',
            ]
        else:
            self.user_agents = useragents

        if settings == None:
            self.useProxy = False
            self.retries = 30
            self.randomAgents = True
        else:
            self.useProxy = settings.useProxy
            self.proxies = settings.proxies
            self.randomAgents = True

        self.tweets = dict()
        self.users = dict()





    def getTweets(self, tweetCriteria, receiveBuffer=None, bufferLength=100, url = None):
        """Get tweets that match the tweetCriteria parameter
        A static method.

        Parameters
        ----------
        tweetCriteria : tweetCriteria, an object that specifies a match criteria
        receiveBuffer : callable, a function that will be called upon a getting next `bufferLength' tweets
        bufferLength: int, the number of tweets to pass to `receiveBuffer' function
        """
        self.TMlogger.info(" * scraping : %s ", tweetCriteria.getSettingsStr())
        if url:
            self.TMlogger.info("* URL Mode : %s ", url)
        results = []
        resultsAux = []
        cookieJar = http.cookiejar.CookieJar()
        user_agent = random.choice(self.user_agents)

        all_usernames = []
        usernames_per_batch = 20

        if hasattr(tweetCriteria, 'username'):
            if type(tweetCriteria.username) == str or not hasattr(tweetCriteria.username, '__iter__'):
                tweetCriteria.username = [tweetCriteria.username]

            usernames_ = [u.lstrip('@') for u in tweetCriteria.username if u]
            all_usernames = sorted({u.lower() for u in usernames_ if u})
            n_usernames = len(all_usernames)
            n_batches = n_usernames // usernames_per_batch + (n_usernames % usernames_per_batch > 0)
        else:
            n_batches = 1

        for batch in range(n_batches):  # process all_usernames by batches
            refreshCursor = ''
            batch_cnt_results = 0

            if all_usernames:  # a username in the criteria?
                tweetCriteria.username = all_usernames[
                                         batch * usernames_per_batch:batch * usernames_per_batch + usernames_per_batch]

            active = True
            while active:
                jsonstr = self.getTimelineJsonResponse(tweetCriteria, refreshCursor, cookieJar, urlstr=url)
                if jsonstr==None:
                    break
                if len(jsonstr['items_html'].strip()) == 0 :
                    break

                refreshCursor = jsonstr['min_position']
                scrapedTweets = PyQuery(jsonstr['items_html'])
                # Remove incomplete tweets withheld by Twitter Guidelines
                scrapedTweets.remove('div.withheld-tweet')
                tweets = scrapedTweets('div.js-stream-tweet')

                self.TMlogger.info(" * Processing : %i  tweets", len(tweets))
                if len(tweets) == 0:
                    break


                for tweetHTML in tweets:
                    tweetPQ = PyQuery(tweetHTML)
                    tweet = models.Tweet()

                    usernames = tweetPQ("span.username.u-dir b").text().split()
                    if not len(usernames):  # fix for issue #13
                        continue

                    tweet.username = usernames[0]
                    tweet.to = usernames[1] if len(usernames) >= 2 else None  # take the first recipient if many
                    tweet.text = re.sub(r"\s+", " ", tweetPQ("p.js-tweet-text").text()) \
                        .replace('# ', '#').replace('@ ', '@').replace('$ ', '$')
                    tweet.retweets = int(
                        tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr(
                            "data-tweet-stat-count").replace(",", ""))
                    tweet.favorites = int(
                        tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr(
                            "data-tweet-stat-count").replace(",", ""))
                    tweet.replies = int(tweetPQ("span.ProfileTweet-action--reply span.ProfileTweet-actionCount").attr(
                        "data-tweet-stat-count").replace(",", ""))
                    tweet.id = tweetPQ.attr("data-tweet-id")
                    tweet.permalink = 'https://twitter.com' + tweetPQ.attr("data-permalink-path")
                    tweet.author_id = int(tweetPQ("a.js-user-profile-link").attr("data-user-id"))

                    dateSec = int(tweetPQ("small.time span.js-short-timestamp").attr("data-time"))
                    tweet.date = datetime.datetime.fromtimestamp(dateSec, tz=datetime.timezone.utc)
                    tweet.formatted_date = datetime.datetime.fromtimestamp(dateSec, tz=datetime.timezone.utc) \
                        .strftime("%a %b %d %X +0000 %Y")
                    tweet.mentions = " ".join(re.compile('(@\\w*)').findall(tweet.text))
                    tweet.hashtags = " ".join(re.compile('(#\\w*)').findall(tweet.text))

                    tweet.conversationId = tweetPQ.attr("data-conversation-id")
                    tweet.ts_source = 'getTweets'
                    tweet.projectID = tweetCriteria.projectID
                    tweet.sourceTweetStatusID = None

                    geoSpan = tweetPQ('span.Tweet-geo')
                    if len(geoSpan) > 0:
                        tweet.geo = geoSpan.attr('title')
                    else:
                        tweet.geo = ''

                    urls = []
                    for link in tweetPQ("a"):
                        try:
                            urls.append((link.attrib["data-expanded-url"]))
                        except KeyError:
                            pass

                    tweet.urls = ",".join(urls)

                    if not self.TMdal.jobExists('tweetApi',tweet.id) :
                        self.TMdal.add_job('tweetApi',random.randint(0,2),(tweet.id,json.dumps(tools.getDict(tweet))))

                    self.tweets[tweet.id] = tweet
                    results.append(tweet)
                    resultsAux.append(tweet)


                    if receiveBuffer and len(resultsAux) >= bufferLength:
                        receiveBuffer(resultsAux)
                        resultsAux = []

                    batch_cnt_results += 1
                    if tweetCriteria.maxTweets > 0 and batch_cnt_results >= tweetCriteria.maxTweets:
                        active = False
                        break

                if jsonstr['has_more_items'] == False:
                    break

            if receiveBuffer and len(resultsAux) > 0:
                receiveBuffer(resultsAux)
                resultsAux = []


        self.TMlogger.info("*Finished * Added[%i] tweets, Total [%i] ", len(results), len(self.tweets))

        comments = []
        if tweetCriteria.saveComments:
            for tweet in results:
                if tweet.replies > 0:
                    commentsCriteria = TweetCriteria().setMaxTweets(tweetCriteria.maxTweets). \
                        setSaveComments(tweetCriteria.saveComments). \
                        setMaxComments(tweetCriteria.maxComments). \
                        setUsername(tweet.username).setStatusID(tweet.id).\
                        setSaveCommentsofComments(tweetCriteria.saveCommentsofComments).\
                        setProjId(tweetCriteria.projectID)

                    tcomments = self.getComments(commentsCriteria)

                    comments.extend(tcomments)
            results.extend(comments)

        return results


    def getTimelineJsonResponse(self, tweetCriteria, refreshCursor, cookieJar, urlstr=None):
        """Invoke an HTTP query to Twitter.
        Should not be used as an API function. A static method.
        """
        self.TMlogger.info("Entered * criteria[%s]", tweetCriteria.getSettingsStr())
        if urlstr:
            self.TMlogger.info("Url Mode: %s",urlstr)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        url = "https://twitter.com/i/search/timeline?"

        if not tweetCriteria.topTweets:
            url += "f=tweets&"

        url += ("vertical=news&q=%s&src=typd&%s"
                "&include_available_features=1&include_entities=1&max_position=%s"
                "&reset_error_state=false")

        urlGetData = ''

        if hasattr(tweetCriteria, 'querySearch'):
            urlGetData += tweetCriteria.querySearch

        if hasattr(tweetCriteria, 'username'):
            if not hasattr(tweetCriteria.username, '__iter__'):
                tweetCriteria.username = [tweetCriteria.username]

            usernames_ = [u.lstrip('@') for u in tweetCriteria.username if u]
            tweetCriteria.username = {u.lower() for u in usernames_ if u}

            usernames = [' from:' + u for u in sorted(tweetCriteria.username)]
            if usernames:
                urlGetData += ' OR'.join(usernames)

        if hasattr(tweetCriteria, 'near') and hasattr(tweetCriteria, 'within'):
            urlGetData += ' near:%s within:%s' % (tweetCriteria.near, tweetCriteria.within)

        if hasattr(tweetCriteria, 'since'):
            urlGetData += ' since:' + tweetCriteria.since

        if hasattr(tweetCriteria, 'until'):
            urlGetData += ' until:' + tweetCriteria.until

        if hasattr(tweetCriteria, 'lang'):
            urlLang = 'l=' + tweetCriteria.lang + '&'
        else:
            urlLang = ''
        url = url % (urllib.parse.quote(urlGetData.strip()), urlLang, urllib.parse.quote(refreshCursor))

        # resume where we left of
        if urlstr:
            url = urlstr

        self.TMlogger.info("Opening [%s]", url)
        tries=0
        while tries < self.retries:

            useragent = random.choice(self.user_agents)

            headers = [
                ('Host', "twitter.com"),
                ('User-Agent', useragent),
                ('Accept', "application/json, text/javascript, */*; q=0.01"),
                ('Accept-Language', "en-US,en;q=0.5"),
                ('X-Requested-With', "XMLHttpRequest"),
                ('Referer', url),
                ('Connection', "keep-alive")
            ]

            if self.useProxy:
                curproxy = random.choices(self.proxies, weights = self.proxiesWeights)[0]
                self.TMlogger.info("Using proxy:[%s]",curproxy)
                opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx),urllib.request.ProxyHandler({'http': curproxy, 'https': curproxy}),
                                                     urllib.request.HTTPCookieProcessor(cookieJar))
            else:
                opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookieJar))
            opener.addheaders = headers


            self.TMlogger.debug("Headers [%s]".join(h[0] + ': ' + h[1] for h in headers))


            try:
                time.sleep(0.3)
                response = opener.open(url, timeout=1)
                jsonResponse = response.read()
            except Exception as e:
                self.TMlogger.info("An error ocured during an HTTP request: %s" ,str(e))
                #self.TMlogger.info("Try to open in browser: https://twitter.com/search?q=%s&src=typd" % urllib.parse.quote(urlGetData))

                if self.useProxy:
                    if self.proxiesWeights[self.proxies.index(curproxy)]>0:
                        self.proxiesWeights[self.proxies.index(curproxy)] -= 1

                    if sum(self.proxiesWeights)< len(self.proxiesWeights):
                        self.TMlogger.info("Proxies in list are: {}".format(' '.join(map(str, self.proxies))))
                        self.TMlogger.info("Proxies Weights in list are: {}".format(' '.join(map(str, self.proxiesWeights))))
                        self.TMlogger.info("Proxies Weights len [%i] sum [%i]", len(self.proxiesWeights), sum(self.proxiesWeights))
                        self.TMlogger.info("Refreshing Proxies")
                        config = configparser.RawConfigParser()
                        config.read('config.ini')
                        proxies, useProxy, proxyRetries = tools.get_proxy_cfg(config, self.TMlogger)
                        self.proxies = proxies
                        self.retries = proxyRetries
                        self.proxiesWeights = [self.retries] * len(self.proxies)


                tries = tries + 1
                if tries >= self.retries:
                    self.TMlogger.error("Exceeded retries")
                    self.TMlogger.error("Skipping [%s]" %url)
                    encoded = uuid.uuid3(uuid.NAMESPACE_URL, url)
                    if not self.TMdal.jobExists('tweetURL', encoded):
                        self.TMlogger.error("Adding URL to Job queue")
                        urldict = {}
                        urldict['url'] = url
                        urldict['projectID'] = tweetCriteria.projectID

                        self.TMdal.add_job('tweetURL', 0,(encoded, json.dumps(urldict)))
                    else:
                        self.TMlogger.error("URL exists in Job queue")
                    return None
                self.TMlogger.info("Retrying - Tries: %i",tries)
                continue








            try:
                s_json = jsonResponse.decode()
            except:
                self.TMlogger.error("Invalid response from Twitter: ", url)
                #some proxies return html with other info which cannot be decoded
                if self.useProxy:
                    if self.proxiesWeights[self.proxies.index(curproxy)]>0:
                        self.proxiesWeights[self.proxies.index(curproxy)] -= 1
                if tries>=self.retries:
                    self.TMlogger.error("decode Exceeded retries")
                    self.TMlogger.error("Skipping [%s]" %url)
                    encoded = uuid.uuid3(uuid.NAMESPACE_URL, url)
                    if not self.TMdal.jobExists('tweetURL', encoded):
                        self.TMlogger.error("Adding URL to Job queue")
                        urldict = {}
                        urldict['url'] = url
                        urldict['projectID'] = tweetCriteria.projectID
                        self.TMdal.add_job('tweetURL', 0,(encoded, json.dumps(urldict)))
                    else:
                        self.TMlogger.error("URL exists in Job queue")
                    return None
                tries = tries + 1
                continue

            try:
                dataJson = json.loads(s_json)
                self.TMlogger.info("items_html [%i]",len(dataJson['items_html'].strip()))
                if len(dataJson['items_html'].strip()) == 0:
                    tries = tries + 5
                    continue
                break
            except Exception as e:
                self.TMlogger.debug("Error parsing JSON: %s \n %s \n Exception[%s]" , s_json, url, str(e))
                if self.useProxy:
                    if self.proxiesWeights[self.proxies.index(curproxy)]>0:
                        self.proxiesWeights[self.proxies.index(curproxy)] -= 1
                tries = tries + 5
                if tries >= self.retries:
                    self.TMlogger.error("loads Exceeded retries")
                    self.TMlogger.error("Skipping [%s]" % url)
                    encoded = uuid.uuid3(uuid.NAMESPACE_URL, url)
                    if not self.TMdal.jobExists('tweetURL', encoded):
                        self.TMlogger.error("Adding URL to Job queue")
                        urldict = {}
                        urldict['url'] = url
                        urldict['projectID'] = tweetCriteria.projectID
                        self.TMdal.add_job('tweetURL', 0, (encoded, json.dumps(urldict)))
                    else:
                        self.TMlogger.error("URL exists in Job queue")
                    return None

                continue

        self.TMlogger.info("Finished ")
        return dataJson


    def getComments(self,tweetCriteria, receiveBuffer=None, bufferLength=100):
        """Get tweets that match the tweetCriteria parameter
        A static method.

        Parameters
        ----------
        tweetCriteria : tweetCriteria, an object that specifies a match criteria
        receiveBuffer : callable, a function that will be called upon a getting next `bufferLength' tweets
        bufferLength: int, the number of tweets to pass to `receiveBuffer' function
        proxy: str, a proxy server to use
        debug: bool, output debug information
        """
        self.TMlogger.info("getComments tweetCriteria: %s ", tweetCriteria.getSettingsStr())
        results = []
        resultsAux = []
        cookieJar = http.cookiejar.CookieJar()
        user_agent = random.choice(self.user_agents)

        refreshCursor = ''
        active = True
        while active:
            jsonstr = self.getStatusJsonResponse(tweetCriteria, refreshCursor, cookieJar)
            if jsonstr==None:
                break
            if len(jsonstr['items_html'].strip()) == 0:
                break

            refreshCursor = jsonstr['min_position']
            scrapedTweets = PyQuery(jsonstr['items_html'])
            # Remove incomplete tweets withheld by Twitter Guidelines
            scrapedTweets.remove('div.withheld-tweet')
            tweets = scrapedTweets('div.js-stream-tweet')

            if len(tweets) == 0:
                break

            for tweetHTML in tweets:
                tweetPQ = PyQuery(tweetHTML)
                tweet = models.Tweet()

                usernames = tweetPQ("span.username.u-dir b").text().split()
                if not len(usernames):  # fix for issue #13
                    continue

                tweet.username = usernames[0]
                tweet.to = usernames[1] if len(usernames) >= 2 else None  # take the first recipient if many
                tweet.text = re.sub(r"\s+", " ", tweetPQ("p.js-tweet-text").text()) \
                    .replace('# ', '#').replace('@ ', '@').replace('$ ', '$')
                tweet.retweets = int(
                    tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr(
                        "data-tweet-stat-count").replace(",", ""))
                tweet.favorites = int(
                    tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr(
                        "data-tweet-stat-count").replace(",", ""))
                tweet.replies = int(tweetPQ("span.ProfileTweet-action--reply span.ProfileTweet-actionCount").attr(
                    "data-tweet-stat-count").replace(",", ""))
                tweet.id = tweetPQ.attr("data-tweet-id")
                tweet.permalink = 'https://twitter.com' + tweetPQ.attr("data-permalink-path")
                tweet.author_id = int(tweetPQ("a.js-user-profile-link").attr("data-user-id"))

                dateSec = int(tweetPQ("small.time span.js-short-timestamp").attr("data-time"))
                tweet.date = datetime.datetime.fromtimestamp(dateSec, tz=datetime.timezone.utc)
                tweet.formatted_date = datetime.datetime.fromtimestamp(dateSec, tz=datetime.timezone.utc) \
                    .strftime("%a %b %d %X +0000 %Y")
                tweet.mentions = " ".join(re.compile('(@\\w*)').findall(tweet.text))
                tweet.hashtags = " ".join(re.compile('(#\\w*)').findall(tweet.text))

                tweet.conversationId = tweetPQ.attr("data-conversation-id")
                tweet.ts_source = 'getComments'
                tweet.projectID = tweetCriteria.projectID
                tweet.sourceTweetStatusID = None

                geoSpan = tweetPQ('span.Tweet-geo')
                if len(geoSpan) > 0:
                    tweet.geo = geoSpan.attr('title')
                else:
                    tweet.geo = ''

                urls = []
                for link in tweetPQ("a"):
                    try:
                        urls.append((link.attrib["data-expanded-url"]))
                    except KeyError:
                        pass

                tweet.urls = ",".join(urls)

                self.tweets[tweet.id] = tweet
                results.append(tweet)
                resultsAux.append(tweet)

                if receiveBuffer and len(resultsAux) >= bufferLength:
                    receiveBuffer(resultsAux)
                    resultsAux = []

                if tweetCriteria.maxComments > 0 and len(results) >= tweetCriteria.maxComments:
                    active = False
                    break

            if jsonstr['has_more_items'] == False:
                break



        comments = []
        for tweet in results:

            jobExists = self.TMdal.jobExists('tweetApi', tweet.id)
            if tweet.replies > 0 and tweetCriteria.saveComments \
                    and tweetCriteria.saveCommentsofComments and not jobExists:
                commentsCriteria = TweetCriteria().setMaxTweets(tweetCriteria.maxTweets). \
                    setSaveComments(tweetCriteria.saveComments). \
                    setMaxComments(tweetCriteria.maxComments). \
                    setUsername(tweet.username).setStatusID(tweet.id).\
                    setSaveCommentsofComments(tweetCriteria.saveCommentsofComments).\
                    setProjId(tweetCriteria.projectID)
                comments = self.getComments(commentsCriteria)
                comments.extend(comments)
            if not jobExists:
                self.TMdal.add_job('tweetApi',random.randint(0,2), (tweet.id, json.dumps(tools.getDict(tweet))))

        results.extend(comments)
        resultsAux.extend(comments)



        if receiveBuffer and len(resultsAux) > 0:
            receiveBuffer(resultsAux)
            resultsAux = []

        self.TMlogger.info("*Finished * Added[%i] tweets, Total [%i] ",len(results), len(self.tweets))

        return results

    def getStatusJsonResponse(self, tweetCriteria, refreshCursor, cookieJar):
        """Invoke an HTTP query to Twitter.
        Should not be used as an API function. A static method.
        """
        self.TMlogger.info('*Entered * criteria [%s]', tweetCriteria.getSettingsStr())
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        url = "https://twitter.com/i/%s/conversation/%s?include_available_features=1&include_entities=1&max_position=%s&reset_error_state=false"

        urlGetData = ''

        if hasattr(tweetCriteria, 'username'):
            user_name = tweetCriteria.username

        if hasattr(tweetCriteria, 'statusID'):
            statusID = tweetCriteria.statusID

        url = url % (user_name, statusID, refreshCursor)


        tries = 0
        dataJson = None

        while tries < self.retries:

            useragent = random.choice(self.user_agents)

            headers = [
                ('Host', "twitter.com"),
                ('User-Agent', useragent),
                ('Accept', "application/json, text/javascript, */*; q=0.01"),
                ('Accept-Language', "en-US,en;q=0.5"),
                ('X-Requested-With', "XMLHttpRequest"),
                ('Referer', url),
                ('Connection', "keep-alive")
            ]

            if self.useProxy:
                curproxy = random.choices(self.proxies, weights = self.proxiesWeights)[0]
                self.TMlogger.info("Using proxy:%s", curproxy)
                opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx),urllib.request.ProxyHandler({'http': curproxy, 'https': curproxy}),
                                                     urllib.request.HTTPCookieProcessor(cookieJar))
            else:
                opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookieJar))
            opener.addheaders = headers


            self.TMlogger.info('*Opening URL [%s]',url)
            self.TMlogger.debug('headers[%s]'.join(h[0] + ': ' + h[1] for h in headers))


            try:
                time.sleep(0.3)
                response = opener.open(url, timeout=1)
                jsonResponse = response.read()
            except HTTPError as error:
                self.TMlogger.error("* An error occurred during an HTTP request:%s", str(error))
                tries = tries + 5
                continue
            except URLError or TimeoutError as error:
                self.TMlogger.info('UrlOpen Error [%s] ' % str(error))
                if self.useProxy:
                    self.proxiesWeights[self.proxies.index(curproxy)] -= 1
                tries = tries + 1
                if tries >= self.retries:
                    self.TMlogger.error("Exceeded retries")
                    self.TMlogger.error("Skipping [%s]" %url)
                    return None
            except Exception as e:
                self.TMlogger.error("getStatusJsonResponse * Unhandled Exception * %s", str(e))
                self.TMlogger.error("Try to open in browser: %s", url)
                continue


            try:
                s_json = jsonResponse.decode()
            except:
                self.TMlogger.error("Invalid response from Twitter check URL: %s", url)
                if self.useProxy:
                    self.proxiesWeights[self.proxies.index(curproxy)] -= 1
                tries = tries + 5
                if tries >= self.retries:
                    self.TMlogger.error("Exceeded retries")
                    self.TMlogger.error("Skipping [%s]" %url)
                    return None

                continue

            try:
                dataJson = json.loads(s_json)
                self.TMlogger.error("items_html [%i]",len(dataJson['items_html'].strip()))
                if len(dataJson['items_html'].strip()) == 0:

                    tries = tries + 5
                    continue
                break
            except:
                self.TMlogger.error("Error parsing JSON check URL: %s", url)
                self.TMlogger.error("Error parsing JSON: %s" % s_json)
                if self.useProxy:
                    self.proxiesWeights[self.proxies.index(curproxy)] -= 1
                tries = tries + 5
                if tries >= self.retries:
                    self.TMlogger.error("Exceeded retries")
                    self.TMlogger.error("Skipping [%s]" %url)
                    return None

                continue

        self.TMlogger.info("Finished ")
        return dataJson


    def getStatusPage(self, tweetCriteria, tWSrap):
        """Invoke an HTTP query to Twitter.
        Should not be used as an API function. A static method.
        """
        self.TMlogger.info("* Entered * criteria [%s]" , tweetCriteria.getSettingsStr())
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        url = 'https://twitter.com/%s/status/%s'

        cookieJar = http.cookiejar.CookieJar()

        if hasattr(tweetCriteria, 'username'):
            user_name = tweetCriteria.username

        if hasattr(tweetCriteria, 'statusID'):
            statusID = tweetCriteria.statusID

        url = url % (user_name, statusID)



        tries = 0
        while tries < self.retries:
            useragent = random.choice(self.user_agents)

            headers = [
                ('Host', "twitter.com"),
                ('User-Agent', useragent),
                ('Accept', "application/json, text/javascript, */*; q=0.01"),
                ('Accept-Language', "en-US,en;q=0.5"),
                ('X-Requested-With', "XMLHttpRequest"),
                ('Referer', url),
                ('Connection', "keep-alive")
            ]

            if self.useProxy:
                curproxy = random.choices(self.proxies, weights = self.proxiesWeights)[0]
                self.TMlogger.info("Using proxy:%s", curproxy)
                opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx),urllib.request.ProxyHandler({'http': curproxy, 'https': curproxy}),
                                                     urllib.request.HTTPCookieProcessor(cookieJar))
            else:
                opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookieJar))
            opener.addheaders = headers

            self.TMlogger.info('URL * [%s]',url)
            self.TMlogger.debug('headers[%s]'.join(h[0] + ': ' + h[1] for h in headers))

            try:
                time.sleep(0.3)
                response = opener.open(url, timeout=1)
                Response = response.read()
                break
            except HTTPError as error:
                self.TMlogger.info("* An error occurred during an HTTP request: %s", str(error))
                tries = tries + 5
                if tries >= self.retries:
                    self.TMlogger.error("Exceeded retries")
                    self.TMlogger.error("Skipping [%s]" %url)
                    return None
                continue
            except URLError or TimeoutError as error:
                self.TMlogger.info('UrlOpen Error [%s] '% str(error))
                if self.useProxy:
                    self.proxiesWeights[self.proxies.index(curproxy)] -= 1
                tries = tries + 1
                if tries >= self.retries:
                    self.TMlogger.error("Exceeded retries")
                    self.TMlogger.error("Skipping [%s]" %url)
                    return None
                continue
            except Exception as e:
                self.TMlogger.info("* Unhandled Exception * %s", str(e))
                self.TMlogger.info("Try to open in browser: %s" % url)
                continue


        twdata = PyQuery(Response)
        tweets = twdata('div.tweet')

        for tweetHTML in tweets:
            tweetPQ = PyQuery(tweetHTML)
            tweet = models.Tweet()

            # todo refactor into function given html tweet return tweet object
            usernames = tweetPQ("span.username.u-dir b").text().split()
            if not len(usernames):  # fix for issue #13
                self.TMlogger.Error("* Error while getting username [ %s ] ", url)
                return None

            if 'Learn' in tweetPQ("a.Tombstone-inlineAction").text().split():   #account suspended
                self.TMlogger.info("* User Suspended [ %s ] ", url)
                return None


            tweet.username = usernames[0]
            tweet.to = usernames[1] if len(usernames) >= 2 else None  # take the first recipient if many
            tweet.text = re.sub(r"\s+", " ", tweetPQ("p.js-tweet-text").text()) \
                .replace('# ', '#').replace('@ ', '@').replace('$ ', '$')
            tweet.retweets = int(
                tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr(
                    "data-tweet-stat-count").replace(",", ""))
            tweet.favorites = int(
                tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr(
                    "data-tweet-stat-count").replace(",", ""))
            tweet.replies = int(tweetPQ("span.ProfileTweet-action--reply span.ProfileTweet-actionCount").attr(
                "data-tweet-stat-count").replace(",", ""))
            tweet.id = tweetPQ.attr("data-tweet-id")
            tweet.permalink = 'https://twitter.com' + tweetPQ.attr("data-permalink-path")
            tweet.author_id = int(tweetPQ("a.js-user-profile-link").attr("data-user-id"))

            dateSec = int(tweetPQ("small.time span.js-short-timestamp").attr("data-time"))
            tweet.date = datetime.datetime.fromtimestamp(dateSec, tz=datetime.timezone.utc)
            tweet.formatted_date = datetime.datetime.fromtimestamp(dateSec, tz=datetime.timezone.utc) \
                .strftime("%a %b %d %X +0000 %Y")
            tweet.mentions = " ".join(re.compile('(@\\w*)').findall(tweet.text))
            tweet.hashtags = " ".join(re.compile('(#\\w*)').findall(tweet.text))

            tweet.conversationId = tweetPQ.attr("data-conversation-id")
            tweet.ts_source = 'getStatus'
            tweet.projectID = tWSrap['projectID']
            tweet.sourceTweetStatusID = tWSrap['sourceTweetStatusID']

            geoSpan = tweetPQ('span.Tweet-geo')
            if len(geoSpan) > 0:
                tweet.geo = geoSpan.attr('title')
            else:
                tweet.geo = ''

            urls = []
            for link in tweetPQ("a"):
                try:
                    urls.append((link.attrib["data-expanded-url"]))
                except KeyError:
                    pass

            tweet.urls = ",".join(urls)

            if tweet.id == tweetCriteria.statusID:
                break

        if 'tweet' in locals():
            self.tweets[tweet.id] = tweet
            self.TMlogger.info("Finished ")
            return tweet
        else:
            return None

