import os
import logging
import sqlite3
import boto3
from botocore.exceptions import ClientError

def createDir(dir):
    """ Creates the given directory if not exits
        also created parent direcories recursively if needed

    Args:
    dir (str) : directory to create

    Returns:
    boolean : True if it already exists or created, False otherwise
    """
    if not os.path.isdir(dir):
        try:
            os.makedirs(dir, exist_ok=True)
        except Exception:
            return False
    return True

def setupLogger(dir, filename):
    """ Setups logger with given directory and filename

    Args:
    dir (str) : log directory
    filename (str) : log filename
    """
    logpath = os.path.join(dir, filename)
    logging.basicConfig(filename=logpath, level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s')
    logging.info('---------------- Logger started ----------------')

def connectToDB(dbPath):
    """ Connects to sqlite database and returns a connection object

    Args:
    dbPath (str) : path for the sqlite database

    Returns:
    conn : database connection object
    """
    conn = None
    try:
        logging.info('Try to connect to database: {}'.format(dbPath))
        conn = sqlite3.connect(dbPath)
        conn.isolation_level = None
    except sqlite3.Error as e:
        logging.error(str(e))
        logging.error('Could not connect to the database')
    else:
        logging.info('Connected to the database successfully')
    return conn

def uploadFileToS3(local_path, bucket_name, s3_path,
                    aws_access_key_id, aws_secret_access_key, region_name):
    """ Uploads a local file to S3 with given bucket and path

    Args:
    local_path (str) : local file path
    bucket_name (str) : S3 bucket name
    s3_path (str) : S3 key
    aws_access_key_id (str) : AWS access key id
    aws_secret_access_key (str) : AWS secret access key
    region_name (str) : AWS region name

    Returns:
    boolean : True if the file is uploaded succesfully, False otherwise
    """
    logging.info('Copy "{}" to S3 bucket "{}" and path "{}"'.format(local_path, bucket_name, s3_path))
    # Upload the file
    s3_client = boto3.client('s3',
                            aws_access_key_id=aws_access_key_id, 
                            aws_secret_access_key=aws_secret_access_key, 
                            region_name=region_name
    )
    try:
        s3_client.upload_file(local_path, bucket_name, s3_path)
    except ClientError as e:
        logging.error(e)
        logging.error('The file could not be copied to S3')
        return False
    logging.info('The file is succesfully copied to S3')
    return True