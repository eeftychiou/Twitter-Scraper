# coding=utf-8


from pyquery import PyQuery
from lxml.html import fromstring
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from datetime import datetime, timedelta

import got3 as got

def unshorten_url(url):
    return requests.head(url, allow_redirects=True).url

def daterange(start_date, end_date, interval):

    for n in range(int ((end_date - start_date).days/interval)+1):
        yield start_date + timedelta(n*interval)

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i + n]

def scrape_tweet(tweet):
    """
    Tries to scrape from the web more information and complete the tweepy object
    :param tweet: tweepy tweet object
    :return: completed tweepy object
    """


    dateUntil = tweet.created_at + timedelta(1)
    tweetCriteria = got.manager.TweetCriteria().setUsername(tweet.author.screen_name).setSince(
        tweet.created_at.strftime("%Y-%m-%d")).setUntil(dateUntil.strftime("%Y-%m-%d")).setMaxTweets(-1)
    found = False
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tw in tweets:
        if tw.id == tweet.id_str:
            tweet.reply_count = tw.replies
            break;
    return tweet

def getDict(t):
    data={}
    data['date'] = t.date.strftime("%Y-%m-%d %H:%M:%S")
    data['username'] = t.username
    data['to'] = t.to
    data['replies'] = t.replies
    data['retweets'] = t.retweets
    data['favorites'] = t.favorites
    data['text'] = t.text
    data['geo'] = t.geo
    data['mentions'] = t.mentions
    data['hashtags'] = t.hashtags
    data['id'] = t.id
    data['permalink'] = t.permalink
    data['conversationid'] = t.conversationId
    data['ts_source'] = t.ts_source
    data['projectID'] = t.projectID
    data['sourceTweetStatusID'] = t.sourceTweetStatusID

    return data

def get_proxies(user_agent):

    response = requests.get('https://www.free-proxy-list.net/', headers={'User-Agent': user_agent}, timeout=(9, 27))
    parser = PyQuery(response.text)
    proxies = set()

    rows = parser('tbody tr')
    for row in rows:
        res=PyQuery(row)
        columns = res('td')
        if columns[6].text == 'yes':
            # Grabbing IP and corresponding PORT
            proxy = ":".join((columns[0].text,columns[1].text))
            proxies.add(proxy)

            proxies.update({proxy: {'country_code': columns[2].text, 'country': columns[3].text, 'privacy': columns[4].text,
                                    'google': columns[5].text, 'https': columns[6].text, 'last_checked': None,
                                    'alive': True}})
    return proxies




def freeproxylist(user_agent):
    proxies = {}
    response = requests.get('https://www.free-proxy-list.net/', headers={'User-Agent': user_agent}, timeout=(9, 27))
    soup = BeautifulSoup(response.text, 'html.parser')
    proxy_list = soup.select('table#proxylisttable tr')
    for p in proxy_list:
        info = p.find_all('td')
        if len(info):
            if info[6].text == 'yes':
                proxy = ':'.join([info[0].text, info[1].text])
                proxies.update({proxy: {'country_code': info[2].text, 'country': info[3].text, 'privacy': info[4].text,
                                    'google': info[5].text, 'https': info[6].text, 'last_checked': None,
                                    'alive': True}})
    return proxies


class ProxyManager:

    def __init__(self, test_url, user_agent):
        self.test_url = test_url
        self.user_agent = user_agent
        self.thread_pool = ThreadPoolExecutor(max_workers=50)
        self.proxies = {}
        self.update_proxy_list()

    def update_proxy_list(self):
        try:
            self.proxies = freeproxylist(self.user_agent)

        except Exception as e:
            logging.error('Unable to update proxy list, exception : {}'.format(e))

    def __check_proxy_status(self, proxy, info):
        info['last_checked'] = datetime.now()
        try:
            res = requests.get(self.test_url, proxies={'http': proxy}, timeout=(3, 6))
            res.raise_for_status()
        except Exception as e:
            info['alive'] = False
        else:
            info['alive'] = True
        return {proxy: info}

    def refresh_proxy_status(self):
        results = [self.thread_pool.submit(self.__check_proxy_status, k, v) for k, v in self.proxies.items()]
        for res in results:
            result = res.result()
            self.proxies.update(result)

    def get_proxies_key_value(self, key, value):
        proxies = []
        for k, v in self.proxies.items():
            match = v.get(key)
            if match == value:
                proxies.append(k)
        return proxies

    def get_proxy(self):
        proxy = None
        for k, v in self.proxies.items():
            alive = v.get('alive')
            if alive:
                return k
        return proxy