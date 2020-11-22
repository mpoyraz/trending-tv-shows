import os
import sys
import logging
import configparser
import time
import json
import sqlite3
import tweepy
import pandas as pd
from datetime import date, datetime, timedelta
from settings import dbPath, tbName, dirDB, dirLogs, streamCount, streamPeriod, tv_shows_fpath
from utils import createDir, setupLogger, connectToDB

# SQL queries for table creation and data insertion
CREATE_TABLE_SQL =  '''CREATE TABLE IF NOT EXISTS {} (
                        id_str text,
                        tweet_datetime text,
                        tweet_raw text
                    )'''.format(tbName)
INSERT_TABLE_SQL =  '''INSERT INTO {} values (?, ?, ?)'''.format(tbName)

# Streamer class
class TweetStreamListener(tweepy.StreamListener):

    def __init__(self, api, cursor, limit_count, limit_time):
        """ Stream listener class constructor

        Args:
        api: tweepy api object
        cursor : database connection cursor
        limit_count (int): maximum number of tweets to stream per session
        limit_time (float): maximum duration (seconds) to stream per session
        trackPhrases (string list): list of phrases to track on the stream
        """
        self.api = api
        self.me = api.me()
        self.cursor = cursor
        self.limit_count = limit_count
        self.limit_time = limit_time
        self.count = 0
        self.time_start = datetime.now()
        self.numReport = 1000
        self.onError = False
        self.waitOnError = 60 # seconds
        self.timeout = self.waitOnError*16 # seconds

    def on_status(self, status):
        """ Saves streamed tweets into the database

        Args:
        status : tweepy status object

        Returns:
        boolean : True continues to stream and False disconnects 
        """
        # Check stream duration limit
        if (datetime.now()-self.time_start).total_seconds() >= self.limit_time:
            logging.info('Reached {} seconds time limit, disconnecting from the stream'.format(self.limit_time))
            logging.info('Streamed {} tweets in this session'.format(self.count))
            return False
        # Log the number of tweets streamed
        if self.count % self.numReport == 0:
            logging.info('Number of tweets streamed since the start: {}'.format(self.count))
        self.onError = False
        self.count += 1
        # Tweet properties
        tweet_id = status.id_str
        tweet_datetime = datetime.strftime(status.created_at, '%Y-%m-%d %H:%M:%S')
        tweet_raw = json.dumps(status._json)
        # Insert into the table
        try:
            self.cursor.execute(INSERT_TABLE_SQL, (tweet_id, tweet_datetime, tweet_raw))
        except sqlite3.Error as e:
            logging.error(str(e))
            logging.error('Tweet {} could not be inserted into the table'.format(tweet_id))
        # Check stream count limit
        if self.count >= self.limit_count:
            logging.info('Reached {} tweets limit, disconnecting from the stream'.format(self.count))
            return False
        return True

    def on_error(self, status_code):
        """ Reports the error and tries to re-connect

        Args:
        status_code (int) : status code from the twitter api

        Returns:
        boolean : True tries to reconnect and False disconnects 
        """
        logging.error('Stream listener encountered error, status code: {}'.format(status_code))
        # Check stream duration limit
        if (datetime.now()-self.time_start).total_seconds() >= self.limit_time:
            logging.info('Reached {} seconds time limit, disconnecting from the stream'.format(self.limit_time))
            logging.info('Streamed {} tweets in this session'.format(self.count))
            return False
        # Try to reconnect within the timeout
        if status_code == 420:
            # Increase wait time on successive errors
            if self.onError:
                self.waitOnError *= 2
                logging.warning('Increased wait time to {} seconds'.format(self.waitOnError))
            if self.waitOnError > self.timeout:
                logging.error('Disconnecting from the stream')
                return False
            # Wait before trying to reconnect the stream
            self.onError = True
            time.sleep(self.waitOnError)
        return True

def createTwitterApi(consumer_key, comsumer_secret,
                     access_token, access_token_secret):
    """ Creates tweepy api object using twitter credentials

    Args:
    consumer_key (str): twitter api key
    comsumer_secret (str): twitter api secret
    access_token (str): twitter access token
    access_token_secret (str): twitter access token secret

    Returns:
    api: tweepy api object
    """
    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(consumer_key, comsumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # Create API object
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api

def startTwitterStream(api, cursor, limit_count, limit_time, trackPhrases):
    """ Starts twitter stream using tweepy Stream

    Args:
    api: tweepy api object
    cursor : database connection cursor
    limit_count (int): maximum number of tweets to stream per session
    limit_time (float): maximum duration (seconds) to stream per session
    trackPhrases (string list): list of phrases to track on the stream
    """
    # Setup the stream listener
    tweetStreamListener = TweetStreamListener(api, cursor, limit_count, limit_time)
    logging.info('Tweet stream listener is initialized')
    # Start the stream
    logging.info('Starting the tweet stream')
    tweetStream = tweepy.Stream(api.auth, tweetStreamListener)
    tweetStream.filter(track=trackPhrases, languages=["en"])
    logging.info('The tweet stream disconnected')

def createTable(cursor, create_table_sql):
    """ Executes the given sql query for table creation

    Args:
    cursor : database connection cursor
    create_table_sql (str): sql query for table creation

    Returns:
    boolean : whether the query execution was successful or not
    """
    # Create the table if not exists
    try:
        cursor.execute(create_table_sql)
    except sqlite3.Error as e:
        logging.error(str(e))
        return False
    return True

def countRowsTable(cursor, tableName):
    """ Counts and logs number of rows in the given table

    Args:
    cursor : database connection cursor
    tableName (str): table name
    """
    # Report current number of rows
    try:
        cursor.execute('SELECT COUNT(*) FROM {}'.format(tableName))
        numRow = cursor.fetchone()[0]
    except sqlite3.Error as e:
        logging.error(str(e))
        logging.info('Could not count the number of rows for {}'.format(tableName))
    else:
        logging.info('Table {} has {} rows'.format(tableName, numRow))

def getTrackPhrases(path):
    """ Read TV shows data (id, name, hashtag) and creates phrases for tracking

    Args:
    path (str): path for TV shows CSV file

    Returns:
    phrases (str list): TV shows hashtag and name
    """
    # Return TV show hashtag and name as track phrases
    df = pd.read_csv(path)
    phrases = df['hashtag'].tolist() + df['name'].tolist()
    logging.info('{} phrases are defined for tracking tweets'.format(len(phrases)))
    logging.info('Track phrases for tweets:')
    logging.info(phrases)
    return phrases

if __name__ == "__main__":
    # Create the required directories if not exits
    if not createDir(dirLogs):
        sys.exit('The directory "{}" could not be created'.format(dirLogs))
    if not createDir(dirDB):
        sys.exit('The directory "{}" could not be created'.format(dirDB))

    # Setup the logger
    logName = date.today().strftime("%Y-%m-%d") + '-tweet-stream.log'
    setupLogger(dirLogs, logName)

    # Connect to the database
    conn = connectToDB(dbPath)
    if conn is None:
        logging.error('Error while connecting to the database')
        sys.exit(1)
    cursor = conn.cursor()

    # Create table to store streamed tweets
    if not createTable(cursor, CREATE_TABLE_SQL):
        sys.exit(1)

    # Report number of rows in the tweets table
    countRowsTable(cursor, tbName)

    # Read the API configuration file
    config = configparser.ConfigParser()
    config.read('api.cfg')

    # Create tweepy api
    api = createTwitterApi(
        config.get('TWITTER','CONSUMER_KEY'),
        config.get('TWITTER','CONSUMER_SECRET'),
        config.get('TWITTER','ACCESS_TOKEN'),
        config.get('TWITTER','ACCESS_TOKEN_SECRET')
    )

    # Prepare phrases for tracking
    trackPhrases = getTrackPhrases(tv_shows_fpath)

    # Start streaming tweets
    startTwitterStream(api, cursor, streamCount, streamPeriod, trackPhrases)

    # Close the database connection
    try:
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(str(e))
        logging.info('Error while closing the database connection')
    else:
        logging.info('Closed the database connection')

    sys.exit(0)