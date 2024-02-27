from pymongo import MongoClient
import datetime

# Connect to MongoDB
client = MongoClient('mongodb://mongodb1:27017/')
db = client['mydb']
collection = db['ooni_circumvention_collection']

# Query MongoDB for unique country codes
unique_countries = collection.distinct("CountryCode")

# Close the MongoDB connection
client.close()

columns = [
    {'name': 'country', 'type': 'string', 'friendly_name': 'Country Code'},
]

rows = [{'country': country} for country in unique_countries]

# Return the result dictionary
result = {'columns': columns, 'rows': rows}
result
