import dal
import requests
from referer_parser import Referer
from urllib.parse import urlparse, parse_qs
import tldextract

referer_url = 'https://www.google.com/url?rct=j&sa=t&url=http://www.communitynews.com.au/news/Detention-centre-draws-protests/7662312&ct=ga&cd=CAIyHDczMGE2ODgxMmM3NGM3Zjk6Y28udWs6ZW46R0I&usg=AFQjCNFFMe0JQVOLA5XNslcRP1S-4DDmiQ&utm_source=twitterfeed&utm_medium=twitter'



dbacc = dal.TweetDal()

url = 'http://bit.ly/1Sx6kNf'

try:
    resp = requests.head(url,allow_redirects=True)
except Exception as d:

    if hasattr(d,"request") and hasattr(d.request,"url"):
        print(d.request.url)
    else:
        pass


parsed = urlparse(resp.url)
qs_parsed = parse_qs(parsed.query)



if 'url' in qs_parsed:
    resp.url = qs_parsed['url'][0]

fully_expanded = resp.url
expanded = 1

extr = tldextract.extract(fully_expanded)
domain = extr.domain
subdomain = extr.subdomain
suffix = extr.suffix

while True:
    dbacc.update_urls(500)
    print("Done 500 ")
