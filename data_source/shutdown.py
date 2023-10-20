import pandas as pd
from pathlib import Path
from pymongo import MongoClient

shutdown_path = "/app/data_source/shut"

def save_shutdown_data_to_mongodb(path, mongo_uri, collection_name):
    # MongoDB Connection
    client = MongoClient(mongo_uri)

    # Read data from CSV files in the specified path
    files = Path(path).glob('*.csv')
    data_frames = []

    for file in files:
        data = pd.read_csv(file)
        data_frames.append(data)

    # Concatenate data frames into a single data frame
    shutdown_data = pd.concat(data_frames, ignore_index=True)

    # Insert data into MongoDB collection
    db = client.get_database()
    collection = db[collection_name]
    collection.insert_many(shutdown_data.to_dict(orient='records'))

    print(f"{len(shutdown_data)} documents inserted into {collection_name} collection.")

# Define MongoDB connection details and collection name
mongo_uri = "mongodb+srv://Mercedeh:UBY3JHxZAkbEQUAa@iflab.ouvlq0u.mongodb.net/mydb?retryWrites=true&w=majority"
collection_name = 'shut_collection'

# Call the function to save shutdown data to MongoDB
save_shutdown_data_to_mongodb(shutdown_path, mongo_uri, collection_name)
