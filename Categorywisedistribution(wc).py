from pymongo import MongoClient

CountryCode = "{{Country}}"

client = MongoClient('mongodb://mongodb1:27017/')
db = client['mydb']

# Retrieve start and end dates from temporary collection
temp_collection = db['temp_dates_wc']
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
collection = db['ooni_web_collection']

pipeline = [
    {
        "$match": {
            "CountryCode": CountryCode,
            "measurement_start_day": {"$gte": start_date, "$lte": end_date}
        }
    },
    {
        "$group": {
            "_id": "$category_code",
            "count": {"$sum": 1}
        }
    }
]

results = list(collection.aggregate(pipeline))
client.close()

# Convert results to the desired format
columns = [
    {'name': 'country', 'type': 'string', 'friendly_name': 'Country Code'},
    {'name': 'Anomaly_type', 'type': 'string', 'friendly_name': 'Category Code'},
    {'name': 'Anomaly_count', 'type': 'int', 'friendly_name': 'Count'},
]

rows = [{'country': "{{Country}}", 'Anomaly_type': result['_id'], 'Anomaly_count': result['count']} for result in results]

# Return the result dictionary
result = {'columns': columns, 'rows': rows}
result
