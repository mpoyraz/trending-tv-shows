# Popular and Trending TV Shows
This repo walks you through steps to create an automated data pipeline to identify popular & trending TV shows based on Twitter and The Movie Database (TMDB) website.

# Introduction and Scope
In this project, the general goal is to find out popular and trending TV shows based on tweets and TV show data on TMDB. To achieve this, we want to create analytics tables with the following specifications:
 - Updated and aggreated on daily basis
 - Automated through a robust data pipeline

There are 2 main data sources:
 1. Hashtags and text of the tweets about TV shows
 2. Vote and popularity data about TV shows on TMDB website (https://www.themoviedb.org).

# Data Sources 
Data collection is a crucial part of this project as project goals require data collection and aggreation on daily basis.
 - Tweets are streamed through standard Twitter API with rate limits.
 - TMDB TV show data is collected through "tmdbv3api" python library.

## Twitter Streaming
Tweepy (http://docs.tweepy.org) Python library is used to stream tweets in real time into a SQLite database.
 - Twitter developer account is needed to have consumer and API keys. These keys are used to connect to a streaming endpoint.
 - Twitter standard API has rate limits and therefore 5000 tweets are collected per hour.
 - Standard API and Tweepy library allows to define a set of phrases to filter tweets. This mechanism explained more in detail at https://developer.twitter.com/en/docs/twitter-api/v1/tweets/filter-realtime/guides/basic-stream-parameters under track section.
 - 160 TV show names and hashtags are used for tracking and to control what is delivered at the stream.

## The Movie Database (TMDB)
tmdbv3api (https://pypi.org/project/tmdbv3api) is lightweight Python library for The Movie Database (TMDB) API. TMDB is community edited database and its API is free to use for non-commercial projects but requires a registration.
 - 160 TV shows are selected based on popularity on TMDB.
 - TMDB API allows user to search TV shows details such as genres, networks and current vote count, average vote, popularity etc ...
 - The API provides data for only current day and its database is updated daily so it should be queried daily to keep track of popular and trending TV shows. 
 
# Data Exploration
## Tweet Object
Tweet object of Twitter API has a complex nested JSON structure (https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/overview/tweet-object) and status object streamed through Tweepy allows access to a very similar raw JSON data.

A simplified view with most important fieds:

`{
 "created_at": "Wed Oct 10 20:19:24 +0000 2018",
 "id": 1050118621198921728,
 "id_str": "1050118621198921728",
 "text": "To make room for more expression, we will now count all emojis as equal—including those with gender‍‍‍ ‍‍and skin t… https://t.co/MkGjXf9aXm",
 "user": {},  
 "entities": {}
}`

We are interested in hashtags and text of the tweets.
 - Hastags are part of entities field and straightforward to extract.
 - Text of tweets are not that trivial as tweet could be extended and/or retweeted. Therefore, retweet and extended status of the tweets should be considered before extracting text of the tweets.
 
 ## TMDB TV Details Object
 TV details is provided as JSON from TMDB API and tmdbv3api serves this data as TV details object. This Python object is reverted back to its raw JSON format and then saved to file. 
 
 The most important fields for our purpose:
 
 `{
 "id": 1551830,
 "name": "Game of Thrones",
 "number_of_episodes": 73,
 "number_of_seasons": 8,
 "vote_average": 8.3,
 "vote_count": 11504,
 "popularity": 369.594
}`
 
More details can be found at https://developers.themoviedb.org/3/tv/get-tv-details.
 
# Data Model
Snowflake schema is selected as the data model for this project.
 - There are 2 fact tables, `tweet_stat` and `tmdb_stat`. One for tweet statistics and the other is for TMDB statistics for TV shows. This is a very reasonable choice as we have 2 main data sources: Twitter API and TMDB API.
 - There 2 main dimention tables, `tvshows` and `date`. Furthermore, `tvshows` table also has 2 child tables named `genres` and `networks`.

## Entity Relationship Diagram (ERD)
ERD of the data model is displayed in the figure below, bold table attributes are primary key columns of each table.

![image](https://github.com/mpoyraz/trending-tv-shows/blob/master/images/db_erd.png)

## Data dictionary:
Data Lake architecture on AWS Simple Storage Service (S3) is used to store both raw tweet and TMDB data and also analytics tables described below. Apache Spark is used for ETL jobs and therefore the data types is intended for Apache Spark framework.

`tweet_stat` fact table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | `long` | TV show id | 
| `date` | `date` | UTC date of tweets |
| `hashtag_count` | `long` | Number of tweets whose hastags include the TV show hashtag |
| `text_count` | `long` | Number of tweets whose text include the TV show name |
| `tweet_count` | `long` | Number of tweets which mentions the TV show either in hashtag or text |
 
 `tmdb_stat` fact table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | `long` | TV show id | 
| `date` | `date` | UTC date |
| `number_of_seasons` | `long` | Number of seasons |
| `number_of_episodes` | `long` | Number of episodes |
| `vote_count` | `long` | Number of votes for the TV show on TMDB website |
| `vote_average` | `double` | Average vote for the TV show on TMDB website |
| `popularity` | `double` | Popularity of the TV show on TMDB website |

 `date` dimention table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `date` | `date` | UTC date | 
| `weekday` | `int` | Day of the week starting from 1 (Monday) |
| `day` | `int` | Day of the month |
| `month` | `int` | Month |
| `year` | `int` | Year |

 `tvshows` dimention table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | `long` | TV show id | 
| `name` | `string` | Name of the TV show |
| `original_name` | `string` | Original name of the TV show |
| `original_language` | `string` | Original language of the TV show |
| `genres_id` | `array<int>` | Array of genre id which the TV show corresponds to |
| `networks_id` | `array<int>` | Array of network id which the TV show corresponds to |

 `genres` table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | `int` | Genre id | 
| `name` | `string` | Genre name |

 `networks` table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | `int` | Network id | 
| `name` | `string` | Network name |
| `origin_country` | `string` | Network's country of origin |

# Data Pipeline
The data pipeline from APIs to final analytics tables on Data Lake is illustrated as follows:

![image](https://github.com/mpoyraz/trending-tv-shows/blob/master/images/data_pipeline.PNG)

As we can see from the image above, there are 2 main parts in this data pipeline: Streaming and ETL.

## Streaming Pipeline
A single AWS Lightsail instance is created for streaming and batch upload of Twitter and TMDB data into the Data Lake on S3.

Twitter Standard API has rate limits, therefore a very modest Lightsail Ubuntu instance with 1vCPU, 512 MB RAM and 20 GB SSD configuration is used.

Python 3.7 and required libraries are installed on the Ubuntu instance:
 - `bash lightsail/install_python.sh`
 - `pip3 install -r lightsail/requirements.txt`

Linux Cron jobs are setup to perform the following:
 - 5000 tweets are streamed per hour and tweets are streamed directly into a local SQLite database as raw JSON.
 - The tweets streamed in the past hour are uploaded to S3 as JSON file and the raw tweets data partitioned by year/month/day/hour. Example S3 path:  `s3://trending-tv-shows/tweet_raw/2020/11/23/00/2020-11-23-00.json`.
 - The TMDB TV show data is queried from TMDB API daily and TV show data uploaded S3 as JSON file and the raw TMDB data partitioned by year/month/day. Example S3 path: `s3://trending-tv-shows/tmdb_raw/2020/11/23/2020-11-23.json`.
 
 The exact setup for the cron jobs are available at [crontab.txt](https://github.com/mpoyraz/trending-tv-shows/blob/master/lightsail/crontab.txt).
  - Run `crontab -e` and then copy & paste the content of [crontab.txt](https://github.com/mpoyraz/trending-tv-shows/blob/master/lightsail/crontab.txt) to setup the cron jobs.
  - Run `crontab -l` to see the list of cron jobs.
 
 ## ETL Pipeline
 ETL jobs are fully owned and orchestrated by Apache Airflow data pipelines. Before going into details of Airflow DAGs, I would like to talk about the general strategy for storage and processing.
 
 Data Lake architecture is build on top of AWS EMR (Spark + HDFS) and S3.
  - Cluster are spun on-demand daily for processing and automatically terminated upon completion of ETL steps.
  - Permanent clusters are not needed and SPOT instances are choosed and therefore ETL is performed in a very cost-effective way.
  - Both raw and processed data is stored on S3 and HDFS is only used as mere temporary storage.
  - `s3-dist-cp` is used to copy the data efficiently between S3 and HDFS of EMR cluster.

Fact tables `tweet_stat` and `tmdb_stat` are stored in Parquet file format and partitioned by year, month and day. This allows very efficient reading and minimize IO. Dimention tables are queried from TMDB API and are relatively small in size, they are stored as CSV files.

There are 2 DAGs defined:
 1. ETL Tweet DAG to process raw tweets on on-demand EMR clusters using Apache Spark
    ![image](https://github.com/mpoyraz/trending-tv-shows/blob/master/images/tweet_dag.PNG)
 2. ETL TMDB DAG to process raw TMDB data on on-demand EMR clusters using Apache Spark
    ![image](https://github.com/mpoyraz/trending-tv-shows/blob/master/images/tmdb_dag.PNG)

As we can see from the DAG's graph view, both dags have the same structure and consists of 4 tasks. The details of each task are described below in the order they are run:

### Task #1: Raw_<tweet/tmdb>_data_quality_check

A custom data quality operator `S3DataQualityOperator` is implemented to check the presense of raw and processed data on Data Lake hosted on S3. The custom operator uses S3 Hook to retrieve keys on a given S3 bucket and key prefix. There should be at least 1 key to pass and if there is no key found, the operator raises a value error.

Both tweet and TMDB raw data are partitioned by date and there should be at least 1 key that corresponds to the execution date. Otherwise, a value error is raised and DAG fails. This step prevents unnecessary EMR cluster launch when no raw data is available.
 
### Task #2: Create_emr_job_flow
 
 EmrCreateJobFlowOperator is used for this task. EMR cluster is launched based on the configuration specified and there are 3 main ETL steps defined for the cluster:
   1. Copy raw tweet/TMDB data from S3 to HDFS using `s3-dist-cp` command for the given execution date. Raw tweets in JSON format are about ~ 0.9 GB per day and TMDB data in JSON format is about ~ 7 MB per day.
   2. Process raw data using `spark-submit`. PySpark ETL scripts `etl/etl_tweet.py` and `etl/etl_tmdb.py` are uploaded to S3 and provided as arguments to `spark-submit`. Spark application is launched through YARN for tweets processing and locally for TMDB data processing. This is a reasoanble choice based on the data sizes.
   3. Processed data is saved to HDFS. The final step copies the processed data from HDFS to S3 using `s3-dist-cp`. The Parquet partitions are preserved.

If there is any failure happens at a step, next steps are canceled. After all the steps are executed or canceled, the cluster auto-terminates.

### Task #3: Check_emr_job_flow

EmrJobFlowSensor is used to monitor the execution of the EMR job flow. It reports the return value of the EMR job flow.

### Task #4: Processed_<tweet/tmdb>_data_quality_check

The same `S3DataQualityOperator` is also used as the last task. Fact table Parquet partition correspond to the execution date is checked. If no key is found under the partition, it means that ETL failed for that day and value error is raised.



