import dal
import requests

dbacc = dal.TweetDal()

# url = 'http://goo.gl/P6bTaz'
#
# try:
#     resp = requests.head(url,allow_redirects=True)
# except Exception as d:
#
#     if hasattr(d,"request") and hasattr(d.request,"url"):
#         print(d.request.url)
#     else:
#         pass


while True:
    dbacc.update_urls(500)
    print("Done 500 ")
