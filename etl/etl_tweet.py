import argparse
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp, to_date, year, month, dayofmonth,\
                                  udf, col, array_contains, expr, broadcast, when, lit
from pyspark.sql.types import StringType, ArrayType

# Default number of partitions
num_partitions_def = 4

def create_spark_session(shuffle_partition_size):
    """ Creates Spark Session object with appropriate configurations.

    Args:
    shuffle_partition_size (int) : Spark shuffle partition size

    Returns:
    spark: Spark Session object
    """

    spark = SparkSession \
        .builder \
        .appName("ETL Tweet") \
        .getOrCreate()

    # Set the log level
    spark.sparkContext.setLogLevel('WARN')
    # Set dataframe shuffle partition size
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partition_size)
    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark

def process_tv_shows(spark, input_path):
    """ Reads TV show list into a Spark dataframe and processes hashtags

    Args:
    spark : Spark Session object
    input_path (str): input path for the CSV file

    Returns:
    tv_shows (Dataframe) : TV show data with names and corresponding hashtags
    """
    # Read TV show data
    tv_shows = spark.read.csv(input_path, header=True)

    # Unify TV shows hashtags (lowercase and without #)
    def unify_hashtag(hashtag):
        return hashtag[1:].lower()
    unify_hashtag_udf=udf(lambda x: unify_hashtag(x), StringType())

    # Alternate hashtags (with or without 'the' prefix)
    def alternate_hastag(hashtag):
        if hashtag[0:3] == 'the':
            return hashtag[3:]
        else:
            return 'the' + hashtag
    alternate_hastag_udf=udf(lambda x: alternate_hastag(x), StringType())

    # Add unified and alternate hashtag columns
    tv_shows = tv_shows.withColumn("unified_hashtag", unify_hashtag_udf(tv_shows.hashtag))
    tv_shows = tv_shows.withColumn("alternate_hashtag", alternate_hastag_udf(tv_shows.unified_hashtag))

    return tv_shows

def process_tweets(spark, input_path, date, tv_show_names, num_partitions):
    """ Reads raw tweets and applies UDFs for hashtag and text processing

    Args:
    spark : Spark Session object
    input_path (str) : input path for raw tweets
    date (str) : date in 'YYYY-MM-DD' format to filter tweets
    tv_show_names (str) : list of TV show names to search in tweets text
    num_partitions (int) : number of partition to use

    Returns:
    tweets (Dataframe) : processed tweets data
    """
    # Read raw tweet data
    tweets_raw = spark.read.json(input_path)

    # Repartition
    tweets_raw = tweets_raw.repartition(num_partitions)

    # Get the final text of the tweets based on retweeted and extended status
    def get_text(retweeted_full_text, retweeted_text, extended_full_text, text):
        if retweeted_full_text:
            return retweeted_full_text
        elif retweeted_text:
            return retweeted_text
        elif extended_full_text:
            return extended_full_text
        else:
            return text
    get_text_udf=udf(lambda retweeted_full_text, retweeted_text, extended_full_text,text:
                     get_text(retweeted_full_text, retweeted_text, extended_full_text, text), StringType())

    # Lowercase the array of hashtags in tweets
    def lowercase_array(arrStr):
        res = []
        for s in arrStr:
            res.append(s.lower())
        return res
    lowercase_array_udf = udf(lowercase_array, ArrayType(StringType()))

    # Select the relevant columns from the raw data
    tweets = tweets_raw.select(
        'id_str',
        lowercase_array_udf('entities.hashtags.text').alias('hashtags'),
        get_text_udf('retweeted_status.extended_tweet.full_text',
                     'retweeted_status.text',
                     'extended_tweet.full_text',
                     'text').alias('final_text'),
        to_date(from_unixtime(unix_timestamp("created_at","EEE MMM dd HH:mm:ss Z yyyy"))).alias('date'),
    )

    # Filter based on the date
    tweets = tweets.filter(col("date") == date)

    # Get TV show mentions in the text of tweets
    def tv_show_mention(text):
        mentions = []
        for tv_show in tv_show_names:
            if tv_show.lower() in text.lower():
                mentions.append(tv_show)
        return mentions
    tv_show_mention_udf = udf(tv_show_mention, ArrayType(StringType()))

    tweets = tweets.withColumn('text_mentions', tv_show_mention_udf(tweets.final_text))

    return tweets

def compute_tweet_tv_show_stats(tv_shows, tweets):
    """ Calculates tweet hashtag and text count of TV shows

    Args:
    tv_shows (Dataframe) : TV shows with names and corresponding hashtags
    tweets (Dataframe) : tweets with hashtags and text mentions of TV shows

    Returns:
    tweet_stats (Dataframe) : tweet count statistics based on hashtag, text or either
    """
    # TV shows in the hashtags
    mention_hashtag = broadcast(tv_shows).join(
            tweets,
            expr("array_contains(hashtags, unified_hashtag)") | expr("array_contains(hashtags, alternate_hashtag)"),
            'left'
    )
    mention_hashtag = mention_hashtag.withColumn("match", when(col("id_str").isNull(), 0).otherwise(1))

    # TV shows in the texts
    mention_text = broadcast(tv_shows).join(
            tweets,
            expr("array_contains(text_mentions, name)"),
            'left'
    )
    mention_text = mention_text.withColumn("match", when(col("id_str").isNull(), 0).otherwise(1))

    # TV shows in the tweets (either hashtag or text)
    mention_tweet = broadcast(tv_shows).join(
            tweets,
            expr("array_contains(hashtags, unified_hashtag)") |
            expr("array_contains(hashtags, alternate_hashtag)") |
            expr("array_contains(text_mentions, name)"),
            'left'
    )
    mention_tweet = mention_tweet.withColumn("match", when(col("id_str").isNull(), 0).otherwise(1))

    # Mention counts based on hashtag, tweet text or either
    count_hashtag = mention_hashtag.groupBy('id').sum('match').withColumnRenamed('sum(match)','hashtag_count')
    count_text = mention_text.groupBy('id').sum('match').withColumnRenamed('sum(match)','text_count')
    count_tweet = mention_tweet.groupBy('id').sum('match').withColumnRenamed('sum(match)','tweet_count')

    tweet_stats = count_hashtag \
                .join(count_text, on=['id'], how='inner') \
                .join(count_tweet, on=['id'], how='inner')

    return tweet_stats

def save_tweet_stat(spark, tweet_stats, output_path, date):
    """ Saves tweet count statistics in Parquet format partitioned by year, month and day

    Args:
    spark : Spark Session object
    tweet_stats (Dataframe) : tweet count statistics based on hashtag, text or either
    output_path (str) : output path to save the given dataframe
    date (str) : date (YYYY-MM-DD format) on which tweet count statistics are calculated
    """
    # Add year, month, day columns for partitioning
    tweet_stats_save = tweet_stats.withColumn('date', to_date(lit(date)))
    tweet_stats_save = tweet_stats_save.withColumn('year', year('date')) \
                                       .withColumn('month', month('date')) \
                                       .withColumn('day', dayofmonth('date')) \

    # Merge partitions before saving
    tweet_stats_save = tweet_stats_save.repartition(1)

    # Save with year, month, day partitions
    tweet_stats_save.write.partitionBy('year','month','day').mode("overwrite").parquet(output_path)
    return

if __name__ == "__main__":

    # Parse arguments
    parser = argparse.ArgumentParser(description="ETL Tweet")
    parser.add_argument("path_tweet", type=str, help="Path for raw tweet data")
    parser.add_argument("path_tv_show", type=str, help="Path for TV show data")
    parser.add_argument("output", type=str, help="Output path to save tweet stats")
    parser.add_argument("date", type=str, help="Date in 'YYYY-MM-DD' format to filter tweets")
    parser.add_argument("--num_partitions", type=str, default=num_partitions_def, help="Number of partitions to use")
    args = parser.parse_args()

    # Create the spark session
    spark = create_spark_session(args.num_partitions)

    # Process TV show data and persist it
    tv_shows = process_tv_shows(spark, args.path_tv_show)
    tv_shows.persist(StorageLevel.MEMORY_AND_DISK)

    # TV show mentions in the text of tweets
    tv_show_names = [str(row.name) for row in tv_shows.select('name').collect()]

    # Process raw tweet data and persist it
    tweets = process_tweets(spark, args.path_tweet, args.date, tv_show_names, args.num_partitions)
    tweets.persist(StorageLevel.MEMORY_AND_DISK)

    # Compute tweet stats for TV shows
    tweet_stats = compute_tweet_tv_show_stats(tv_shows, tweets)

    # Save the tweet stats
    save_tweet_stat(spark, tweet_stats, args.output, args.date)
