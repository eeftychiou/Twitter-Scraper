class TweetCriteria:
    """Search parameters class"""

    def __init__(self):
        self.maxTweets = 0
        self.topTweets = False
        self.within = ""

    def setUsername(self, username):
        """Set username(s) of tweets author(s)
        Parameters
        ----------
        username : str or iterable

        If `username' is specified by str it should be a single username or
        usernames separeated by spaces or commas.
        `username` can contain a leading @

        Examples:
            setUsername('barackobama')
            setUsername('barackobama,whitehouse')
            setUsername('barackobama whitehouse')
            setUsername(['barackobama','whitehouse'])
        """
        self.username = username
        return self

    def setSince(self, since):
        """Set a lower bound date in UTC
        Parameters
        ----------
        since : str,
                format: "yyyy-mm-dd"
        """
        self.since = since
        return self

    def setUntil(self, until):
        """Set an upper bound date in UTC (not included in results)
        Parameters
        ----------
        until : str,
                format: "yyyy-mm-dd"
        """
        self.until = until
        return self

    def setNear(self, near):
        """Set location to search nearby
        Parameters
        ----------
        near : str,
               for example "Berlin, Germany"
        """
        self.near = near
        return self

    def setWithin(self, within):
        """Set the radius for search by location
        Parameters
        ----------
        within : str,
                 for example "15mi"
        """
        self.within = within
        return self

    def setQuerySearch(self, querySearch):
        """Set a text to be searched for
        Parameters
        ----------
        querySearch : str
        """
        self.querySearch = querySearch
        return self

    def setMaxTweets(self, maxTweets):
        """Set the maximum number of tweets to search
        Parameters
        ----------
        maxTweets : int
        """
        self.maxTweets = maxTweets
        return self

    def setLang(self, Lang):
        """Set language
        Parameters
        ----------
        Lang : str
        """
        self.lang = Lang
        return self

    def setTopTweets(self, topTweets):
        """Set the flag to search only for top tweets
        Parameters
        ----------
        topTweets : bool
        """
        self.topTweets = topTweets
        return self

    def setSaveComments(self, saveComments):
        """ Sets the flag that will cause the scraper to save the comments if any

        :param saveComments: bool
        :return:
        """
        self.saveComments = saveComments
        return self

    def setMaxComments (self, maxComments):
        """
        Sets the maxumum number of comments to retrieve per each tweet
        :param maxComments:  int -1 indicates all comments
        :return:
        """

        self.maxComments = maxComments
        return self
    def setStatusID (self, statusID):
        """
        Sets the status ID of the tweet which the comments will be retrieved
        :param statusID:
        :return:
        """
        self.statusID = statusID
        return self

    def setSaveCommentsofComments(self, saveCommentsofComments):
        """ Sets the flag that will cause the scraper to save the comments if any

        :param saveComments: bool
        :return:
        """
        self.saveCommentsofComments = saveCommentsofComments
        return self

    def getSettingsStr(self):

        attrs = vars(self)

        str = ', '.join("%s: %s" % item for item in attrs.items())
        return str