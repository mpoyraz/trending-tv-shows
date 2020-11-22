import os

# Directory names
DB_DIR = 'databases'
LOG_DIR = 'logs'
TWEET_DIR = 'tweet_raw'
TMDB_DIR = 'tbdm_raw'

# Directories
dirDB = os.path.join(os.getcwd(), DB_DIR)
dirLogs = os.path.join(os.getcwd(), LOG_DIR)
dirTweet = os.path.join(os.getcwd(), TWEET_DIR)
dirTmdb = os.path.join(os.getcwd(), TMDB_DIR)

# Database parameters
dbName = 'stream.db'
dbPath = os.path.join(dirDB, dbName)
tbName = 'tweets'

# Tweet streaming limits per hour
streamCount = 5000
streamPeriod = 30 # seconds

# S3 parameters
s3_bucket = 'trending-tv-shows'
s3_key_tweet = 'tweet_raw'
s3_key_tmdb = 'tmdb_raw'
s3_upload_try = 5