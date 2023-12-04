import pymongo

# Define the MongoDB connection string with the obtained IP address
mongodb_uri = "mongodb://172.28.0.1:27017/mydb"

try:
    # Create a MongoClient and check if it can connect to MongoDB
    client = pymongo.MongoClient(mongodb_uri)
    db_list = client.list_database_names()
    
    # If you reach this point, the connection is successful
    print("Successfully connected to MongoDB")
    
except pymongo.errors.ConnectionFailure as e:
    # Connection failed
    print(f"Could not connect to MongoDB: {e}")
