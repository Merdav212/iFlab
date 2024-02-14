from pymongo import MongoClient

# Connect to the MongoDB instance
client = MongoClient('mongodb://mongodb1:27017/')

# Specify the database names and collection names
database_names = ['your_database', 'testdb']
web_collection_name = 'web_collection'
circumvention_collection_name = 'circumvention_collection'

# Function to drop collections
def drop_collections(database_name, collection_names):
    for collection_name in collection_names:
        collection = client[database_name][collection_name]
        collection.drop()
        print(f"Dropped collection: {database_name}_{collection_name}")

# Drop collections for your_database
drop_collections('your_database', ['web_collection', 'circumvention_collection'])

# Drop collections for testdb
drop_collections('testdb', ['web_collection', 'circumvention_collection'])

# List all collections in the databases
for database_name in database_names:
    collections = client[database_name].list_collection_names()
    print(f"\nCollections in {database_name}: {collections}")

# Close the MongoDB connection
client.close()
