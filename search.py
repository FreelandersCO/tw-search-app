import requests, sys, time, json
from pymongo import MongoClient
from TwitterSearch import *

MONGO_URL = 'mongo-app'
MONGO_PORT = 27017
DB_NAME = 'tw-listening'
USER = 'userd4t4'
PASS = '1nt3r4ct1v3'

class TwitterSearchApp():
    def __init__(self):
        connection = MongoClient('mongodb://%s:%s@%s:%s' % (USER, PASS, MONGO_URL, MONGO_PORT))
        self.db = connection[DB_NAME]
        print('Running Twitter Cron')
        response = requests.get('http://cms-app:1337/twsearches/')
        if response.status_code == 200:
            terms = response.json()
            for term in terms:
                self.searchTerm(term)
                time.sleep(60)

    def searchTerm(self, term):
        access_token = term['config']['access_token']
        access_token_secret = term['config']['access_secret_token']
        consumer_key = term['config']['api_key']
        consumer_secret = term['config']['api_secret_key']
        ts = TwitterSearch(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret
        )

        tso = TwitterSearchOrder()  # create a TwitterSearchOrder object
        # let's define all words we would like to have a look for
        tso.set_keywords([term['term']])
        tso.set_language('es')

        for tweet in ts.search_tweets_iterable(tso):
            self.insertData(tweet)

    def insertData(self, data):
        exist = self.db.tweets.find_one({"id": data['id']})
        if exist is None:
            self.db.tweets.insert_one(data)
       
def main():
  print('Class Executor')
  TwitterSearchApp()

if __name__ == '__main__':
    main()
