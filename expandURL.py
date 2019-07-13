import dal
import requests

dbacc = dal.TweetDal()

# url = 'http://dlvr.it/BkXFrL'
#
# resp = requests.head(url,allow_redirects=True)


while True:
    dbacc.update_urls(400)
    print("Done 400 ")
