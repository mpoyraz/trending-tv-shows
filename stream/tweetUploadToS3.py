import os
import sys
import logging
import configparser
import sqlite3
import time
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from datetime import date, datetime, timedelta
from settings import dbPath, tbName, dirTweet, dirLogs, s3_bucket, s3_key_tweet, s3_upload_try
from utils import createDir, setupLogger, connectToDB, uploadFileToS3

def getPastHourInterval():
    """ Calculates the time interval for the past hour

    Returns:
    dt_start (datetime) : datetime object for the past hour start
    datetime_start (str) : formatted datetime for the past hour start
    datetime_end (str) : formatted datetime for the current hour start
    """
    # Get the search period in terms of UTC time
    dt_cur = datetime.utcnow()
    logging.info('Current UTC time is {}'.format(dt_cur.strftime("%Y-%m-%d %H:%M:%S")))
    # Start and end of the time interval
    dt_end = dt_cur.replace(minute=0, second=0, microsecond=0)
    dt_start = dt_end - timedelta(hours=1)
    # Formatted datetime strings
    datetime_end = dt_end.strftime("%Y-%m-%d %H:%M:%S")
    datetime_start = dt_start.strftime("%Y-%m-%d %H:%M:%S")
    return dt_start, datetime_start, datetime_end

def retrieveDataFromDB(conn, datetime_start, datetime_end):
    """ Retrieves the tweets from the database for given time interval

    Args:
    conn : database connection object
    datetime_start (str) : datetime interval start
    datetime_end (str) : datetime interval end

    Returns:
    df (pandas dataframe) : raw tweet data
    """
    logging.info('Search interval for tweets: {} - {}'.format(datetime_start, datetime_end))
    # Construct the select query
    select_query = '''SELECT tweet_raw FROM {} WHERE
                        tweet_datetime > '{}' AND tweet_datetime < '{}'
                   '''.format(tbName, datetime_start, datetime_end)
    # Retrieve the data
    df = pd.read_sql_query(select_query, conn)
    return df

def getLocalPartitionedPath(dir_save, dt_save):
    """ Returns a partitioned directory and path based on the given datetime

    Args:
    dir_save (str) : the local directory for saving
    dt_save (datetime) : datetime used for directory setup

    Returns:
    local_dir (str) : dir_save/<year>/<month>/<day>/<hour>
    local_path (str) : dir_save/<year>/<month>/<day>/<hour>/<year-month-day-hour>.json
    """
    year, month, day, hour = dt_save.strftime("%Y-%m-%d-%H").split('-')
    local_dir = os.path.join(dir_save, year, month, day, hour)
    local_path = os.path.join(local_dir, '{}-{}-{}-{}.json'.format(year, month, day, hour))
    logging.info('Local path to save raw data: {}'.format(local_path))
    return local_dir, local_path

def getS3PartitionedPath(key_prefix, dt_save):
    """ Returns a partitioned S3 path based on the given datetime

    Args:
    key_prefix (str) : key prefix on S3
    dt_save (datetime) : datetime used for directory setup

    Returns:
    s3_path (str) : key_prefix/<year>/<month>/<day>/<hour>/<year-month-day-hour>.json
    """
    year, month, day, hour = dt_save.strftime("%Y-%m-%d-%H").split('-')
    s3_path = '{}/{}/{}/{}/{}/{}-{}-{}-{}.json'.format(
        key_prefix, year, month, day, hour, year, month, day, hour
    )
    logging.info('S3 key for upload: {}'.format(s3_path))
    return s3_path

def saveTweetsLocal(df, local_path):
    """ Saves raw tweets locally using the given path

    Args:
    df (pandas dataframe) : raw tweet data
    local_path (str) : path to save locally

    Returns:
    boolean : whether the file saving is successful or not
    """
    logging.info('Writing tweet data into {}'.format(local_path))
    isSuccess = False
    try:
        fp = open(local_path, 'w')
        for row in df.iloc[:,0].tolist():
            fp.write('{}\n'.format(row))
    except Exception as e:
        logging.error(str(e))
        logging.error('Error while writing tweets to the file')
    else:
        isSuccess = True
        logging.info('Tweets are successfully saved to the file')
    finally:
        if fp:
            fp.close()
    return isSuccess

if __name__ == "__main__":
    # Create the required directories if not exits
    if not createDir(dirTweet):
        sys.exit('The directory "{}" could not be created'.format(dirTweet))

    # Setup the logger
    logName = date.today().strftime("%Y-%m-%d") + '-tweet-upload.log'
    setupLogger(dirLogs, logName)

    # Get datetime interval for the past hour
    dt_save, datetime_start, datetime_end = getPastHourInterval()

    # Connect to the database
    conn = connectToDB(dbPath)
    if conn is None:
        logging.error('Error while connecting to the database')
        sys.exit(1)

    # Get the tweet data streamed in the past hour
    df = retrieveDataFromDB(conn, datetime_start, datetime_end)

    # Close the database connection
    try:
        conn.close()
    except Exception as e:
        logging.error(str(e))
        logging.info('Error while closing the database connection')
    else:
        logging.info('Closed the database connection')

    # Check the number of records
    num_tweet = len(df)
    if num_tweet == 0:
        logging.error('No tweet was retrieved from the database')
        sys.exit(1)
    else:
        logging.info('{} tweets were retrieved from the database'.format(num_tweet))

    # Local directory and path to save
    local_dir, local_path = getLocalPartitionedPath(dirTweet, dt_save)

    # Create the local directory for saving
    if not createDir(local_dir):
        logging.error('The directory "{}" could not be created')
        sys.exit(1)

    # Save the file locally
    if not saveTweetsLocal(df, local_path):
        logging.error('The tweet could not be saved to local, will not upload to S3')
        sys.exit(1)

    # Get S3 path to transfer the file
    s3_path = getS3PartitionedPath(s3_key_tweet, dt_save)

    # Read the API configuration file
    config = configparser.ConfigParser()
    config.read('api.cfg')

    # Upload the file to S3
    for i in range(s3_upload_try):
        isUploaded = uploadFileToS3(local_path, s3_bucket, s3_path,
                                    config.get('AWS','ACCESS_KEY_ID'),
                                    config.get('AWS','SECRET_ACCESS_KEY'),
                                    config.get('AWS','REGION')
        )
        # If not uploaded successfully, wait and try again
        if isUploaded:
            break
        else:
            logging.warning('Waiting 2 seconds and will try to upload to S3 again')
            time.sleep(2)

    sys.exit(0)

