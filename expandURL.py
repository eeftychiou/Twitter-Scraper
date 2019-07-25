import dal
import requests

from urllib.parse import urlparse, parse_qs
import tldextract

#
#
# url = 'http://bloom.bg/1fOohVA'
#
# try:
#     resp = requests.head(url, allow_redirects=True, timeout=30)
#
#     parsed = urlparse(resp.url)
#     qs_parsed = parse_qs(parsed.query)
#
#
#     if 'url' in qs_parsed:
#         if qs_parsed['url'][0][0:4]=='http':
#             resp.url = qs_parsed['url'][0]
#
#     fully_expanded = resp.url
#     expanded = 1
#
#     extr = tldextract.extract(fully_expanded)
#     domain = extr.domain
#     subdomain = extr.subdomain
#     suffix = extr.suffix
#
#
# except Exception as e:
#
#     if hasattr(e, "request") and hasattr(e.request, "url"):
#
#         parsed = urlparse(e.request.url)
#         qs_parsed = parse_qs(parsed.query)
#
#         if 'url' in qs_parsed:
#             e.request.url = qs_parsed['url'][0]
#
#         fully_expanded = e.request.url
#         expanded = 2
#
#         extr = tldextract.extract(fully_expanded)
#         domain = extr.domain
#         subdomain = extr.subdomain
#         suffix = extr.suffix
#     else:
#         url = url
#         try:
#             resp = requests.head(url, allow_redirects=True, timeout=5)
#
#             fully_expanded = resp.url
#             expanded = 3
#
#             extr = tldextract.extract(fully_expanded)
#             domain = extr.domain
#             subdomain = extr.subdomain
#             suffix = extr.suffix
#
#         except Exception as e:
#             print("Exception:",str(e))
#
#





dbacc = dal.TweetDal()
while True:
    dbacc.update_urls(5000)
    print("Done 5000 ")
