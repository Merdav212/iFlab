import requests
import pandas as pd
import pymongo
from io import BytesIO, StringIO
from minio import Minio
from minio.error import S3Error
import json
import logging
import argparse
import os
import datetime
import numpy as np

#CONFIG_FILE_PATH = "/app/confg.json"

is_running_in_docker = "DOCKER_CONTAINER" in os.environ
CACHE_DIR = "cache"
CACHE_EXPIRATION_TIME = 86400  # Cache expires after 24 hours

CONFIG_FILE_PATH = "/app/confg.json"
# if is_running_in_docker:
#     # Running inside a Docker container
#     CONFIG_FILE_PATH = "/app/confg.json"
# else:
#     # Running locally on your host
#     CONFIG_FILE_PATH = "/home/lunet/wsmj3/fraganal/Flaskapp/confg.json"

# Load the config file
with open(CONFIG_FILE_PATH, "r") as config_file:
    config = json.load(config_file)


# Function to load configuration from a file
def load_config(file_path):
    try:
        with open(file_path, 'r') as config_file:
            config = json.load(config_file)
        return config
    except FileNotFoundError:
        raise Exception(f"Config file not found at {CONFIG_FILE_PATH}")
    except json.JSONDecodeError:
        raise Exception(f"Error parsing JSON in config file")

# Function to configure the logger
def configure_logger():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

def load_cache(cache_key):
    cache_path = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_path):
        with open(cache_path, "r") as cache_file:
            cache_data = json.load(cache_file)
        # # Check if the cache is still valid (not expired)
        # if datetime.datetime.now() - cache_data["timestamp"] < datetime.timedelta(seconds=CACHE_EXPIRATION_TIME):
        return cache_data['data']
        
    # Cache is not valid or does not exist
    return None

def save_cache(cache_key, data):
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)

    cache_path = os.path.join(CACHE_DIR, f"{cache_key}.json")
    with open(cache_path, "w") as cache_file:
        # json.dump({"data":data, "timestamp": datetime.datetime.now().isoformat()}, cache_file)
        json.dump({"data":data}, cache_file)


# Function to validate date format (YYYY-MM-DD)
def is_valid_date(date):
    try:
        pd.to_datetime(date)
        return True
    except (ValueError, TypeError):
        return False

# Function to validate and parse command-line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description="Download data from OONI and Radar APIs and store it in MongoDB.")
    parser.add_argument("--start_date", type=str, required=True, help="Start date for data retrieval (YYYY-MM-DD).")
    parser.add_argument("--end_date", type=str, required=True, help="End date for data retrieval (YYYY-MM-DD).")
    parser.add_argument("--database", type=str, required=True, help="Database name to store the data.")
    args = parser.parse_args()

    if not is_valid_date(args.start_date) or not is_valid_date(args.end_date):
        raise ValueError("Invalid date format. Please use YYYY-MM-DD.")

    return args

# Load configuration from the config file
config = load_config(CONFIG_FILE_PATH)

# Configure the logger
logger = configure_logger()

# Function to connect to the local MongoDB instance
def connect_to_mongodb():
    try:
        client = pymongo.MongoClient(config["mongodb_uri"])
        database = client[config["mongodb_database"]]
        return client, database
    except Exception as e:
        logger.error(f"MongoDB connection error: {e}")
        raise

# Function to insert a Pandas DataFrame into MongoDB
def insert_dataframe_into_mongodb(df, collection_name):
    try:
        if not df.empty:
            data = df.to_dict(orient='records')
            collection = db[collection_name]
            collection.insert_many(data)
            logger.info(f"Inserted data into MongoDB for {collection_name}")
        else:
            logger.info(f"No data to insert into {collection_name}")
    except Exception as e:
        logger.error(f"MongoDB insertion error for {collection_name}: {e}")

# List of excluded countries
excluded_countries = config.get("excluded_countries", [])

# Function to filter countries based on configuration
def filter_countries(standard_cc_list, excluded_list):
    filtered_cc = list(set(standard_cc_list).difference(excluded_list))
    if np.nan in filtered_cc:
        filtered_cc.remove(np.nan)
    return filtered_cc

# Function to download datasets from Cloudflare Radar
def download_radar_dataset(proto, cc, start_date, end_date):

    protocol_name = 'IP Version' if proto == 'ip_version' else 'TLS Version'
    collection_name = f'radar_{proto}_collection'

    cache_key = f"radar_{proto}_{cc}_{start_date}_{end_date}"
    cached_data = load_cache(cache_key)
    if cached_data is not None:
        logger.info(f"Using cached data for {proto} ({cc}, {start_date} to {end_date})")
        return cached_data



    
    # Check if data for the specified date range already exists in MongoDB
    collection = db[collection_name]
    existing_data = collection.count_documents({
        'measurement_start_day': {
            '$gte': start_date,
            '$lte': end_date
        },
        'CountryCode': cc
    })

    if existing_data > 0:
        logger.info(f"Data for {protocol_name} already exists in MongoDB for {cc} and the specified date range. Skipping download.")
        return


    url = f"{config['radar_base_url']}/{proto}?dateStart={start_date}T00:00:00Z&dateEnd={end_date}T00:00:00Z&location={cc}&format=csv"
    
    headers = {
        'X-Auth-Email': config['radar_auth_email'],
        'X-Auth-Key': config['radar_auth_key'],
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        protocol_name = 'IP Version' if proto == 'ip_version' else 'TLS Version'
        df['protocol'] = protocol_name
        df['CountryCode'] = cc  # Add the country code field
        insert_dataframe_into_mongodb(df, f'radar_{proto}_collection')
        logger.info(f"Inserted radar data ({protocol_name}) into MongoDB for {cc}")
        
        upload_to_minio(df.to_csv(), f'radar_{proto}_collection.csv')
        # Save data to cache
        save_cache(cache_key, df.to_dict(orient='records'))
    except requests.exceptions.HTTPError as errh:
        logger.error(f"HTTP Error while downloading radar data for {cc}: {errh}")

def download_ooni_web_connectivity(cc, start_date, end_date):
    cache_key = f"ooni_web_{cc}_{start_date}_{end_date}"
    cached_data = load_cache(cache_key)
    if cached_data is not None:
        logger.info(f"Using cached data for ooni web ({cc}, {start_date} to {end_date})")
        return cached_data

    # Check for existing data in MongoDB
    collection = db['ooni_web_collection']
    existing_data = collection.count_documents({
        'measurement_start_day': {
            '$gte': start_date,
            '$lte': end_date
        },
        'CountryCode': cc
    })

    if existing_data > 0:
        logger.info(f"Data for ooni web already exists in MongoDB for {cc} and the specified date range. Skipping download.")
        return

    url = f"{config['ooni_web_base_url']}?probe_cc={cc}&test_name=web_connectivity&since={start_date}&until={end_date}&time_grain=day&axis_x=category_code&axis_y=measurement_start_day&format=CSV&download=true"
    
    headers = {
        'X-Auth-Email': config['ooni_auth_email'],
        'X-Auth-Key': config['ooni_auth_key'],
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        df['CountryCode'] = cc  # Add the country code field
        insert_dataframe_into_mongodb(df, 'ooni_web_collection')
        logger.info(f"Inserted ooni web data into MongoDB for {cc}")

        upload_to_minio(df.to_csv(), 'ooni_web_collection.csv')
        
        # Save data to cache
        save_cache(cache_key, df.to_dict(orient='records'))
    except requests.exceptions.HTTPError as errh:
        logger.error(f"HTTP Error while downloading ooni web data for {cc}: {errh}")

def download_ooni_circumvention_tools(cc, start_date, end_date):
    cache_key = f"ooni_circumvention_{cc}_{start_date}_{end_date}"
    cached_data = load_cache(cache_key)
    if cached_data is not None:
        logger.info(f"Using cached data for ooni circumvention tools ({cc}, {start_date} to {end_date})")
        return cached_data

    # Check for existing data in MongoDB
    collection = db['ooni_circumvention_collection']
    existing_data = collection.count_documents({
        'measurement_start_day': {
            '$gte': start_date,
            '$lte': end_date
        },
        'CountryCode': cc
    })

    if existing_data > 0:
        logger.info(f"Data for ooni circumvention tools already exists in MongoDB for {cc} and the specified date range. Skipping download.")
        return

    url = f"{config['ooni_circumvention_base_url']}?probe_cc={cc}&test_name=torsf,tor,stunreachability,psiphon,riseupvpn&since={start_date}&until={end_date}&time_grain=day&axis_x=test_name&axis_y=measurement_start_day&format=CSV&download=true"
    
    headers = {
        'X-Auth-Email': config['ooni_auth_email'],
        'X-Auth-Key': config['ooni_auth_key'],
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        df['CountryCode'] = cc  # Add the country code field
        insert_dataframe_into_mongodb(df, 'ooni_circumvention_collection')
        logger.info(f"Inserted ooni circumvention data into MongoDB for {cc}")

        upload_to_minio(df.to_csv(), 'ooni_circumvention_collection.csv')
        
        # Save data to cache
        save_cache(cache_key, df.to_dict(orient='records'))
    except requests.exceptions.HTTPError as errh:
        logger.error(f"HTTP Error while downloading ooni circumvention data for {cc}: {errh}")

# Function to upload data to Minio
def upload_to_minio(data, filename):
    try:
        minio_client = Minio(
            config['minio_endpoint'],
            access_key=config['minio_access_key'],
            secret_key=config['minio_secret_key'],
            secure=config['minio_secure']
        )
        minio_bucket = config['minio_bucket']
        if not minio_client.bucket_exists(minio_bucket):
            minio_client.make_bucket(minio_bucket)
        data_bytes = data.encode("utf-8")
        minio_client.put_object(
            minio_bucket,
            filename,
            BytesIO(data_bytes),
            len(data),
            content_type="application/csv"
        )
        logger.info(f"Uploaded {filename} to Minio in {minio_bucket} bucket")
        
    except S3Error as exc:
        logger.error(f"Minio error: {exc}")

# Function to load the standard list of country codes
def load_standard_country_codes(cc_filepath):
    cc_df = pd.read_csv(cc_filepath)
    return cc_df['alpha-2'].to_list()

# Function to get the latest date in MongoDB collection
def get_latest_date_from_mongodb(cc, database):
    
    if database == 'radar_ip' or database == 'radar_tls':
        collection_name = f'{database}_version_collection'
    else:
        collection_name = f'{database}_collection'
    latest_date = db[collection_name].find({'probe_cc': cc}).sort('measurement_start_day', pymongo.DESCENDING).limit(1)
    # Convert the cursor to a list and use len to get the count
    latest_date_list = list(latest_date)
    return latest_date_list[0]['measurement_start_day'] if len(latest_date_list) > 0 else None


# Function to get existing dates in MongoDB collection after a specified date
def get_existing_dates_from_mongodb(cc, database, start_date):
    if database == 'radar_ip' or database == 'radar_tls':
        collection_name = f'{database}_version_collection'
    else:
        collection_name = f'{database}_collection'

    existing_dates = db[collection_name].distinct('measurement_start_day', {'probe_cc': cc, 'measurement_start_day': {'$gte': start_date}})
    return existing_dates

# Function to find missing dates between a date range and a list of existing dates
def find_missing_dates(start_date, end_date, existing_dates):
    date_range = pd.date_range(start=start_date, end=end_date)
    missing_dates = [date.strftime('%Y-%m-%d') for date in date_range if date.strftime('%Y-%m-%d') not in existing_dates]
    return missing_dates

def adjust_dates(start_date, end_date, existing_dates, latest_date):

    if latest_date is None:
        adjusted_start_date = start_date
        adjusted_end_date = end_date

    else:

        # Scenario 1: Start date within range, end date out of range
        if start_date >= latest_date and start_date <= end_date:
            adjusted_start_date = latest_date
        else:
            adjusted_start_date = start_date

        # Scenario 2: End date within range, start date out of range
        if end_date >= latest_date and end_date <= latest_date:
            adjusted_end_date = latest_date
        else:
            adjusted_end_date = end_date

        # Scenario 3: Both start and end date out of range with overlapping dates
        if start_date < latest_date and end_date > latest_date:
            adjusted_start_date = latest_date + datetime.timedelta(days=1)

    return adjusted_start_date, adjusted_end_date

def main(start_date, end_date, database):
    try:
        # Connect to MongoDB
        global db
        client, db = connect_to_mongodb()

        # Load standard country codes and filter excluded countries
        standard_cc_list = load_standard_country_codes(config['cc_filepath'])
        filtered_cc = filter_countries(standard_cc_list, excluded_countries)

        # Download datasets based on the specified database
        if database == 'radar_ip':
            for cc in filtered_cc:
                # Check for existing data
                latest_date = get_latest_date_from_mongodb(cc, database)
                existing_dates = get_existing_dates_from_mongodb(cc, database, start_date)

                # Adjust dates based on existing data
                adjusted_start_date, adjusted_end_date = adjust_dates(start_date, end_date, existing_dates, latest_date)

                # Check if adjusted dates are valid
                if adjusted_start_date >= adjusted_end_date:
                    logger.info(f"No data to download for {cc}. All data exists.")
                    continue

                download_radar_dataset('ip_version', cc, adjusted_start_date, adjusted_end_date)
                logger.info(f"Downloading radar dataset (IP version) for {cc} done")

        elif database == 'radar_tls':
            for cc in filtered_cc:
                # Check for existing data
                latest_date = get_latest_date_from_mongodb(cc, database)
                existing_dates = get_existing_dates_from_mongodb(cc, database, start_date)

                # Adjust dates based on existing data
                adjusted_start_date, adjusted_end_date = adjust_dates(start_date, end_date, existing_dates, latest_date)

                # Check if adjusted dates are valid
                if adjusted_start_date >= adjusted_end_date:
                    logger.info(f"No data to download for {cc}. All data exists.")
                    continue

                download_radar_dataset('tls_version', cc, adjusted_start_date, adjusted_end_date)
                logger.info(f"Downloading radar datasets (TLS version) for {cc} done")

        elif database == 'ooni_web':
            for cc in filtered_cc:
                # Check for existing data
                latest_date = get_latest_date_from_mongodb(cc, database)
                existing_dates = get_existing_dates_from_mongodb(cc, database, start_date)

                # Adjust dates based on existing data
                adjusted_start_date, adjusted_end_date = adjust_dates(start_date, end_date, existing_dates, latest_date)

                # Check if adjusted dates are valid
                if adjusted_start_date >= adjusted_end_date:
                    logger.info(f"No data to download for {cc}. All data exists.")
                    continue

                download_ooni_web_connectivity(cc, adjusted_start_date, adjusted_end_date)
                logger.info(f"Downloading ooni web data for {cc} done")

        elif database == 'ooni_circumvention':
            for cc in filtered_cc:
                # Check for existing data
                latest_date = get_latest_date_from_mongodb(cc, database)
                existing_dates = get_existing_dates_from_mongodb(cc, database, start_date)

                # Adjust dates based on existing data
                adjusted_start_date, adjusted_end_date = adjust_dates(start_date, end_date, existing_dates, latest_date)

                # Check if adjusted dates are valid
                if adjusted_start_date >= adjusted_end_date:
                    logger.info(f"No data to download for {cc}. All data exists.")
                    continue

                download_ooni_circumvention_tools(cc, adjusted_start_date, adjusted_end_date)
                logger.info(f"Downloading ooni circumvention data for {cc} done")

        else:
            logger.info(f"Skipping download. Database {database} not recognized.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    args = parse_arguments()
    main(args.start_date, args.end_date, args.database)
