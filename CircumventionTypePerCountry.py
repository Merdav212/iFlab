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

# Define the pipeline with the date match stage
pipeline = [
    {"$match": {"measurement_start_day": {"$gte": start_date, "$lte": end_date}}},
    {
        "$group": {
            "_id": {"country": "$CountryCode", "test_name": "$test_name"},
            "total_count": {"$sum": 1},  # Count occurrences of each test_name for each country
        }
    },
    {
        "$sort": {"_id.country": 1, "total_count": -1}  # Sort by country and total_count descending
    },
    {
        "$group": {
            "_id": "$_id.country",  # Group by country_code
            "top_test_name": {"$first": "$_id.test_name"},  # Get the first (most used) test_name for each country
            "frequency": {"$first": "$total_count"},  # Get the frequency of the most used test_name
        }
    }
]

# Execute the pipeline
results = list(collection.aggregate(pipeline))

# Extract data for plotting
columns = [
    {'name': 'country', 'type': 'string', 'friendly_name': 'Country Code'},
    {'name': 'top_test_name', 'type': 'string', 'friendly_name': 'Most Used Test Name'},
    {'name': 'frequency', 'type': 'integer', 'friendly_name': 'Frequency'}
]

rows = []

for result in results:
    rows.append({
        'country': result['_id'],
        'top_test_name': result['top_test_name'],
        'frequency': result['frequency']
    })

# Close the MongoDB connection
client.close()

# Return the result dictionary
result = {'columns': columns, 'rows': rows}
print(result)
