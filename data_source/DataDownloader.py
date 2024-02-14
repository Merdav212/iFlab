import requests
import pandas as pd
import pymongo
from io import StringIO 

# MongoDB Atlas connection string with the database name
#mongo_uri = "mongodb+srv://Mercedeh:UBY3JHxZAkbEQUAa@iflab.ouvlq0u.mongodb.net/mydb?retryWrites=true&w=majority"

# Function to connect to MongoDB Atlas
# Function to connect to the local MongoDB instance in the Docker container
def connect_to_mongodb():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    database = client["mydb"]  # Replace "mydb" with the name of your database
    return client, database


# Function to insert a Pandas DataFrame into MongoDB
def insert_dataframe_into_mongodb(df, collection_name):
    if not df.empty:  # Check if the DataFrame is not empty
        # Convert the DataFrame to a list of dictionaries (records)
        data = df.to_dict(orient='records')
        # Insert the data into the MongoDB collection
        collection = db[collection_name]
        collection.insert_many(data)
        print(f"Inserted data into MongoDB for {collection_name}")
    else:
        print(f"No data to insert into {collection_name}")

# Define the list of countries to exclude
excluded_countries = [
    'VG', 'KN', 'ER', 'PW', 'TC', 'FM', 'SX', 'PN', 'DM', 'BL', 'GS', 'MS', 'AI', 'VU', 'TK', 'CX', 'GI', 'NF', 'WF', 'AQ', 'NR',
    'AX', 'SH', 'KP', 'EH', 'GW', 'UM', 'VA', 'PM', 'BV', 'GQ', 'MH', 'WS', 'ZA', 'SM', 'AS', 'FK', 'TF', 'NU', 'KI', 'VI', 'IO',
    'SJ', 'BM', 'HM', 'TO', 'TV', 'MF', 'CK', 'MP', 'NR', 'AX'
]

# Function to load the standard list of country codes
def load_standard_country_codes(cc_filepath):
    cc_df = pd.read_csv(cc_filepath)
    return cc_df['alpha-2'].to_list()

# Function to remove excluded countries and 'nan'
def filter_countries(standard_cc_list, excluded_list):
    filtered_cc = list(set(standard_cc_list).difference(excluded_list))
    # Check if 'nan' is in the list before attempting to remove it
    if 'nan' in filtered_cc:
        filtered_cc.remove('nan')
    return filtered_cc

# Function to download datasets from Cloudflare Radar
def download_radar_dataset(proto, cc):
    url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/{proto}?dateStart=2022-01-01T00:00:00Z&dateEnd=2023-01-01T00:00:00Z&location={cc}&format=csv"
    headers = {
        'X-Auth-Email': 'mercedeh.rezaei@yahoo.com',
        'X-Auth-Key': '3acceb864e9602ba3dfd472bc1e1136e7b1fd',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        # Parse the CSV data into a dataframe
        df = pd.read_csv(StringIO(response.text))
        # Determine the protocol name based on 'proto' parameter
        protocol_name = 'IP Version' if proto == 'ip_version' else 'TLS Version'
        # Insert the dataframe into MongoDB with an additional 'protocol' field
        df['protocol'] = protocol_name
        insert_dataframe_into_mongodb(df, f'radar_{proto}_collection')
        print(f"Inserted radar data ({protocol_name}) into MongoDB for {cc}")
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error while downloading radar data for {cc}: {errh}")


# Function to download OONI Web Connectivity datasets
def download_ooni_web_connectivity(cc):
    url = f"https://api.ooni.io/api/v1/aggregation?probe_cc={cc}&test_name=web_connectivity&since=2022-01-01&until=2022-12-31&time_grain=day&axis_x=category_code&axis_y=measurement_start_day&format=CSV&download=true"

    try:
        response = requests.get(url)
        response.raise_for_status()
        # Parse the CSV data into a dataframe
        df = pd.read_csv(StringIO(response.text))
        # Insert the dataframe into MongoDB
        insert_dataframe_into_mongodb(df, 'ooni_web_collection')
        print(f"Inserted ooni web data into MongoDB for {cc}")
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error while downloading ooni web data for {cc}: {errh}")

# Function to download OONI Circumvention Tool datasets
def download_ooni_circumvention_tools(cc):
    url = f"https://api.ooni.io/api/v1/aggregation?probe_cc={cc}&test_name=torsf,tor,stunreachability,psiphon,riseupvpn&since=2022-01-01&until=2022-12-31&time_grain=day&axis_x=test_name&axis_y=measurement_start_day&format=CSV&download=true"

    try:
        response = requests.get(url)
        response.raise_for_status()
        # Parse the CSV data into a dataframe
        df = pd.read_csv(StringIO(response.text))
        # Insert the dataframe into MongoDB
        insert_dataframe_into_mongodb(df, 'ooni_circumvention_collection')
        print(f"Inserted ooni circumvention data into MongoDB for {cc}")
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error while downloading ooni circumvention data for {cc}: {errh}")

# Main function for downloading datasets
def main():
    # Connect to MongoDB
    # Connect to the local MongoDB instance in the Docker container and select the "mydb" database
    global db
    client, db = connect_to_mongodb()

    
    # Define the paths to country code files
    cc_filepath = "/app/data_source/cc_alpha2_3.csv"

    # Load standard country codes and filter excluded countries
    standard_cc_list = load_standard_country_codes(cc_filepath)
    filtered_cc = filter_countries(standard_cc_list, excluded_countries)

    # Download IP version and TLS version dataset for all filtered countries
    for cc in filtered_cc:
        download_radar_dataset('ip_version', cc)
        download_radar_dataset('tls_version', cc)
        print("Downloading radar datasets (IP version and TLS version) for", cc, "done")

    # Download OONI Web Connectivity dataset for all filtered countries
    for cc in filtered_cc:
        download_ooni_web_connectivity(cc)
        print("Downloading ooni web data for", cc, "done")

    # Download OONI Circumvention Tool dataset for all filtered countries
    for cc in filtered_cc:
        download_ooni_circumvention_tools(cc)
        print("Downloading ooni circumvention data for", cc, "done")

if __name__ == "__main__":
    main()
