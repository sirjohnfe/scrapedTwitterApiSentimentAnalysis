#need to install tweepy if you don't have it
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import AzureDBConnection as db
from textblob import TextBlob
import re

access_token = '108465966-SMo4jOOq4XGlVjphh3bU9SO3YGzZ34060Knc62mD'
access_token_secret = '3qdH8CwaCXdBwMXwxsAC2fHuCd87q7pcgw9WkUH0SQSvl'
consumer_key = 'HEAYIchWBSa7PAK3wm2lFvl6d'
consumer_secret = 'BTZdxf6x070cgu6FRM95vfYwsnt1oEFzDfQzSYNAAzixeTWJ7u'
db_connection, db_cursor = db.get_conn()


MSDAUSERNAME = "obk701"
keywordsToFilterTwitter = ["Apple"]
Keywords = ",".join(keywordsToFilterTwitter)
#ckey = 'Yb91la6CgtVkJnO61MtQc63eL'
#csecret = '9TSOa1mm2LI9ndDa7rTzIls5MhLi2yezH99r31F1Rs0rwG5Ipa'
#atoken = '948246738388815872-EBkPCSHBwSbuOboL6TAhcv8tZyXxYik'
#asecret = '2NodcQCSP3fyuniQWOHFjPqPjJjCjUPer1B28LVI7pS3n'

def get_tweet_sentiment(tweet):
    '''
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    '''
    # create TextBlob object of passed tweet text
    analysis = TextBlob(clean_tweet(tweet))
    # set sentiment
    if analysis.sentiment.polarity > 0:
        return '+'
    elif analysis.sentiment.polarity == 0:
        return '.'
    else:
        return '-'


def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    cleanedTweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w+:\ / \ / \S+)", " ", tweet).split())
    print("cleaned tweet", cleanedTweet)
    return cleanedTweet

def get_query():
    #return "insert into twitterData_draft values ('"+ id_str + "','"+ text +"','')"
    #return "insert into twitterData_draft (TWEETID, TWEETMESSAGE, POLARITY) values (?,?,?)"
    return "insert into twitterData_modified (RecordID,MSDAUSERNAME,Keywords,CreatedAt,TweetID,TweetMessage,Source,UserID, " \
           "UserName,UserScreenName,location,UserFollowersCount,UserFriendsCount,UserListedCount,UserFavouritesCount," \
           "UserTweetsCount,AccountCreatedAt,Coordinates,Place,isRetweetedMessage,reTweetQuoteCount,reTweetReplyCount," \
           "reTweetcount,reTweetFavoriteCount,isReTweeted,isTweetFavorited,Polarity) values " \
           "(NEXT VALUE FOR Record_ID,?,?,?,?,?," \
           "?,?,?,?,?,?,?,?,?,?,?,?,?," \
           "?,?,?,?,?,?,?,?)"

class listener(StreamListener):

   def on_data(self, raw_data):
       def on_data(self, raw_data):
           print(raw_data)

       #saveFile = open('TwitterDB5.csv', 'w')
       #print(raw_data)
       rawdataJSON = json.loads(raw_data)
       tweetText = rawdataJSON['text']
       created_at = rawdataJSON['created_at']
       id_str = rawdataJSON['id_str']
       text = rawdataJSON['text']
       source = rawdataJSON['source']
       userID = str(rawdataJSON['user']['id'])
       UserName = rawdataJSON['user']['name']
       UserScreenName = rawdataJSON['user']['screen_name']
       location = rawdataJSON['user']['location']
       UserFollowersCount= rawdataJSON['user']['followers_count']
       UserFriendsCount = rawdataJSON['user']['friends_count']
       UserListedCount = rawdataJSON['user']['listed_count']
       UserFavouritesCount = rawdataJSON['user']['favourites_count']
       UserTweetsCount = rawdataJSON['user']['statuses_count']
       AccountCreatedAt = rawdataJSON['user']['created_at']
       Geo = str(rawdataJSON['geo'])
       Coordinates = str(rawdataJSON['coordinates'])
       if Geo is None:
           Geo = 'NA'
       if Coordinates is None:
           Coordinates = 'NA'
       Place = str(rawdataJSON['place'])
       if Place is None:
           Place = 'NA'
       isRetweetedMessage = ''
       reTweetQuoteCount = 0
       reTweetReplyCount = 0
       reTweetcount = 0
       reTweetFavoriteCount = 0
       if rawdataJSON.get('retweeted_status') is None :
        isRetweetedMessage ='N'
       else:
        isRetweetedMessage = 'Y'
        reTweetQuoteCount = rawdataJSON['retweeted_status']['quote_count']
        reTweetReplyCount = rawdataJSON['retweeted_status']['reply_count']
        reTweetcount = rawdataJSON['retweeted_status']['retweet_count']
        reTweetFavoriteCount = rawdataJSON['retweeted_status']['favorite_count']

       isReTweeted = rawdataJSON['retweeted']
       isTweetFavorited = rawdataJSON['favorited']
       #print("------------ The Details are ------------")
       # print(created_at, id_str, text, source, userID, UserName, UserScreenName, UserFollowersCount, UserFriendsCount, UserListedCount,
       #       UserFavouritesCount, UserTweetsCount, AccountCreatedAt, Geo, Coordinates, Place, isRetweetedMessage, reTweetQuoteCount,
       #       reTweetReplyCount, reTweetcount, reTweetFavoriteCount, isReTweeted, isTweetFavorited)

       # twitterDetails = [created_at, id_str, text, source, userID, UserName, UserScreenName, UserFollowersCount, UserFriendsCount, UserListedCount,
       #       UserFavouritesCount, UserTweetsCount, AccountCreatedAt, Geo, Coordinates, Place, isRetweetedMessage, reTweetQuoteCount,
       #       reTweetReplyCount, reTweetcount, reTweetFavoriteCount, isReTweeted, isTweetFavorited]

       query = get_query()
       polarity = get_tweet_sentiment(str(text))
       # RecordID, MSDAUSERNAME, Keywords, CreatedAt, TweetID, TweetMessage, Source, UserID, " \
       #            "
       # UserName, UserScreenName, location, UserFollowersCount, UserFriendsCount, UserListedCount, UserFavouritesCount, " \
       #            "
       # UserTweetsCount, AccountCreatedAt, Coordinates, Place, isRetweetedMessage, reTweetQuoteCount, reTweetReplyCount, " \
       #            "
       # reTweetcount, reTweetFavoriteCount, isReTweeted, isTweetFavorited, Polarity
       args = (MSDAUSERNAME, Keywords, created_at, id_str, text, source, userID, UserName, UserScreenName,
               location, UserFollowersCount, UserFriendsCount,UserListedCount, UserFavouritesCount, UserTweetsCount, AccountCreatedAt, Coordinates, Place,
               isRetweetedMessage, reTweetQuoteCount, reTweetReplyCount,reTweetcount, reTweetFavoriteCount, str(isReTweeted), str(isTweetFavorited), polarity)
       print(args)
       db.insert_data(query,db_connection, db_cursor, args)

       print("\n\n")
       saveFile = open('TwitterDB5.csv', 'a')
       saveFile.write(raw_data)
       saveFile.close()
       return True

   def on_error(self, status_code):
       print(status_code)


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

twitterStream = Stream(auth, listener())
#type in keywords you want tweets to have
twitterStream.filter(track=keywordsToFilterTwitter)

