from pymongo import MongoClient
import datetime

# Connect to MongoDB
client = MongoClient('mongodb://mongodb1:27017/')
db = client['mydb']

# Retrieve start and end dates from temporary collection
temp_collection = db['temp_dates_ct']
temp_data = temp_collection.find_one()

# Check if start_date and end_date are specified in the temp collection
if temp_data and 'start_date' in temp_data and 'end_date' in temp_data:
    start_date = temp_data['start_date']
    end_date = temp_data['end_date']
else:
    # No start or end date specified in the temp collection
    result = {'message': 'No data available for the selected timeframe.'}
    print(result)
    client.close()
    exit()

# Define the collection containing your data
collection = db['ooni_circumvention_collection']

# Define a mapping from test names to categories
test_name_categories = {
    "tor": 1,
    "stunreachability": 2,
    "psiphon": 3,
    "riseupvpn": 4,
    "torsf": 5
}

# Define the pipeline
pipeline = [
    {"$match": {"measurement_start_day": {"$gte": start_date, "$lte": end_date}}},
    {"$group": {"_id": {"CountryCode": "$CountryCode", "test_name": "$test_name"}, "count": {"$sum": 1}}},
    {"$sort": {"_id.CountryCode": 1, "count": -1}},
    {"$group": {"_id": "$_id.CountryCode", "top_test_name": {"$first": "$_id.test_name"}, "count": {"$first": "$count"}}}
]

# Execute the pipeline
results = list(collection.aggregate(pipeline))

# Extract data for plotting
columns = [
    {'name': 'country', 'type': 'string', 'friendly_name': 'Country Code'},
    {'name': 'top_test_name', 'type': 'string', 'friendly_name': 'Top Test Name'},
    {'name': 'count', 'type': 'integer', 'friendly_name': 'Count'},
    {'name' :'top_test_code', 'type':'integer', 'friendly_name': 'Top Test Code'}
]

rows = []

for result in results:
    # top_test_code = test_name_categories.get(result['top_test_name'], None)
    # print(f"Top test name: {result['top_test_name']}, Top test code: {top_test_code}")
    rows.append({
        'country': result['_id'],
        'top_test_name': result['top_test_name'],
        'count': result['count'],
        'top_test_code': test_name_categories.get(result['top_test_name'], None)
    })
    

# Close the MongoDB connection
client.close()

# Return the result dictionary
result = {'columns': columns, 'rows': rows}
print(result)
