import dal

dbacc = dal.TweetDal()
while True:
    dbacc.update_urls(100)
    print("Done 100 ")
