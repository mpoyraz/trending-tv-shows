import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth

def create_spark_session():
    """ Creates Spark Session object with appropriate configurations.

    Returns:
    spark: Spark Session object
    """

    spark = SparkSession \
        .builder \
        .appName("ETL TMDB") \
        .getOrCreate()

    # Set the log level
    spark.sparkContext.setLogLevel('WARN')
    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark

def process_tmdb(spark, input_path, date):
    """ Reads raw TMDB TV show data and
        selects relevant quantitative fields for given date

    Args:
    spark : Spark Session object
    input_path (str) : input path for TMDB TV show data
    date (str) : date in 'YYYY-MM-DD' format to filter TMDB data

    Returns:
    tmdb_stats (Dataframe) : TV show stats from TMDB data
    """
    # Read the TMDB raw data
    tmdb_raw = spark.read.json(input_path)

    # Select relevant quantitative fields
    tmdb_stats = tmdb_raw.select('id', 'popularity', 'vote_average', 'vote_count',
                                 'number_of_episodes', 'number_of_seasons',
                                 to_date('query_datetime').alias('date'))

    # Filter based on the date
    tmdb_stats = tmdb_stats.filter(col("date") == date)

    return tmdb_stats

def save_tmdb_stat(spark, tmdb_stats, output_path):
    """ Saves TMDB TV show stats in Parquet format partitioned by year, month and day

    Args:
    spark : Spark Session object
    tmdb_stats (Dataframe) : TV show stats from TMDB api
    output_path (str) : output path to save the given dataframe
    """
    # Add year, month, day columns for partitioning
    tmdb_stats_save = tmdb_stats.withColumn('year', year('date')) \
                                .withColumn('month', month('date')) \
                                .withColumn('day', dayofmonth('date')) \

    # Save with year, month, day partitions
    tmdb_stats_save.write.partitionBy('year','month','day').mode("overwrite").parquet(output_path)
    return

if __name__ == "__main__":

    # Parse arguments
    parser = argparse.ArgumentParser(description="ETL TMDB")
    parser.add_argument("path_tmdb", type=str, help="Path for raw TMDB data")
    parser.add_argument("path_output", type=str, help="Output path to save TMDB stats")
    parser.add_argument("date", type=str, help="Date in 'YYYY-MM-DD' format to filter TMDB data")
    args = parser.parse_args()

    # Create the spark session
    spark = create_spark_session()

    # Process TMDB TV show data
    tmdb_stats = process_tmdb(spark, args.path_tmdb, args.date)

    # Save the TMDB TV show stats
    save_tmdb_stat(spark, tmdb_stats, args.path_output)
