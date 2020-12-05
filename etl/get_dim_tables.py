import os
import logging
import argparse
import configparser
import pandas as pd
from datetime import date, timedelta
from tmdbv3api import TMDb, TV

def initTmdb(api_key):
    """ Returns a TMDB object initialized with the given api key

    Args:
    api_key (str) : TMDB api key

    Returns:
    tmdb : TMDB object
    """
    tmdb = TMDb()
    tmdb.api_key = api_key
    tmdb.language = 'en'
    return tmdb

def getTVShowsId(path):
    """ Read TV shows data (id, name, hashtag) and returns their id as list

    Args:
    path (str): path for TV shows CSV file

    Returns:
    tv_shows_id (int list): list of tv shows id
    """
    # Read the TV shows CSV file
    df_tv = pd.read_csv(path)
    # Get the id of tb shows
    tv_shows_id = df_tv['id'].tolist()
    logging.info('{} TV shows are defined for searching'.format(len(tv_shows_id)))
    logging.info('TV shows id:')
    logging.info(tv_shows_id)
    return tv_shows_id

def get_dim_tmdb(tv_shows_id):
    """ Queries TV show details from TMDB and
        extracts genres, networks and tv show data

    Args:
    tv_shows_id (int list): list of tv shows id

    Returns:
    df_genre (dataframe) : genre id and name
    df_network (dataframe) : network id, name and origin country
    df_tvshow (dataframe) : TV show id, name, original name, genre ids and network ids
    """
    # Create the TV object
    tv = TV()

    # Retrieve raw TV show data
    tv_shows_raw = []
    for tv_show_id in tv_shows_id:
        tv_show_detail = tv.details(tv_show_id)
        tv_shows_raw.append(tv_show_detail.__dict__)

    # Define the columns
    genre_columns = ['id', 'name']
    network_columns = ['id', 'name', 'origin_country']
    tv_show_columns = ['id', 'name', 'original_name', 'original_language', 'genres_id', 'networks_id']

    # Initialize the dataframes
    df_genre = pd.DataFrame(columns=genre_columns)
    df_network = pd.DataFrame(columns=network_columns)
    df_tvshow = pd.DataFrame(columns=tv_show_columns)

    # Extract from the raw data
    for tv_show_data in tv_shows_raw:
        # Genres
        for genre in tv_show_data.get('genres', []):
            genre_id, genre_name = int(genre['id']), genre['name']
            if not ((df_genre['id'] == genre_id) & (df_genre['name'] == genre_name)).any():
                df_genre.loc[len(df_genre),:] = genre_id, genre_name
        # Networks
        for network in tv_show_data.get('networks', []):
            network_id, network_name, network_country = int(network['id']), network['name'], network['origin_country']
            if not ((df_network['id'] == network_id) & (df_network['name'] == network_name)).any():
                df_network.loc[len(df_network),:] = network_id, network_name, network_country
        # TV shows
        genres_id = []
        for genre in tv_show_data.get('genres', []):
            genres_id.append(int(genre['id']))
        networks_id = []
        for network in tv_show_data.get('networks', []):
            networks_id.append(int(network['id']))
        df_tvshow.loc[len(df_tvshow),:] = [int(tv_show_data['id']), tv_show_data['name'], 
            tv_show_data['original_name'], tv_show_data['original_language'], genres_id,networks_id]

    return df_genre, df_network, df_tvshow

def get_dim_date(date = date.today(), num_years = 1):
    """ Creates the date dataframe

    Args:
    date (datetime.date): input date on which starting year is determined
    num_years (int) : number of years from the start year

    Returns:
    df_date (dataframe) : with columns [ date (YYYY-MM-DD format),
                            weekday, day, month, year ]
    """
    # Define columns and initialize the dataframe
    date_columns = ['date', 'weekday', 'day', 'month', 'year']
    df_date = pd.DataFrame(columns=date_columns)

    # Iterate given number of years from the start date
    date_start = date.replace(month=1, day=1)
    for i in range(num_years*365):
        date_cur = date_start + timedelta(days=i)
        df_date.loc[i,:] = [date_cur.strftime("%Y-%m-%d"), date_cur.isoweekday(),
                                date_cur.day, date_cur.month, date_cur.year]

    return df_date

def save_dim_tables(df_list, filenames, path):
    """ Saves the dataframes as CSV files

    Args:
    df_list : list of dataframes
    filenames : list of filenames
    path (str) : directory path in which dataframes are saved

    Returns:
    boolean to denote success or failure
    """
    # Check if the given path is a directory
    if not os.path.isdir(path):
        logging.error('The output path {} is not a directory'.format(path))
        return False

    # Check the number of dataframes and filenames
    if len(df_list) != len(filenames):
        logging.error('The number of dataframes and filenames do not match')
        return False

    # Save the dataframes as CSV files
    try:
        for i, filename in enumerate(filenames):
            path_save = os.path.join(path, filename)
            df_list[i].to_csv(path_save, header=True, index=False)
    except Exception as e:
        logging.error(str(e))
        return False

    return True

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Upload TMDB TV show data to S3",
        add_help=True
    )
    parser.add_argument("path_config", type=str,
                        help="Path to configuration file with API credentials")
    parser.add_argument("path_tv_show", type=str,
                        help="Path to the CSV file with TV show information")
    parser.add_argument("--path_output", type=str, default=os.getcwd(),
                        help="Path to save the dimention tables")
    args = parser.parse_args()

    # Setup the logger
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s')
    logging.info('---------------- Logger started ----------------')

    # Read the API configuration file
    config = configparser.ConfigParser()
    config.read(args.path_config)

    # Initialize TMDB object
    tmdb = initTmdb(config.get('TMDB','API_KEY'))

    # Get the TV shows id
    tv_shows_id = getTVShowsId(args.path_tv_show)

    # Dimention tables from TMDB raw TV show data
    df_genre, df_network, df_tvshow = get_dim_tmdb(tv_shows_id)

    # Date dimention
    df_date = get_dim_date()

    # Save the dimention tables
    if save_dim_tables([df_genre, df_network, df_tvshow, df_date],
                        ['genres.csv', 'networks.csv', 'tvshows.csv', 'date.csv'],
                        args.path_output):
        logging.info('The dimention tables are saved successfully')
    else:
        logging.error('Problem occured while saving the dimention tables')
