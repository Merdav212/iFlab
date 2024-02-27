import requests
from pymongo import MongoClient
import datetime

# Connect to MongoDB
client = MongoClient('mongodb://mongodb1:27017/')
db = client['mydb']

# Check if the collection exists
collection_name = 'temp_dates_ct'
if collection_name in db.list_collection_names():
    # Collection exists, so drop it
    db.drop_collection(collection_name)
# Recreate the temporary collection
temp_collection = db[collection_name]

# Check if start_date and end_date are specified by the user
start_date = "{{ start_date }}"
end_date = "{{ end_date }}"

# Save the user-specified start and end dates in the temporary collection
temp_collection.insert_one({'start_date': start_date, 'end_date': end_date})

# Define the collection containing your data
collection = db['ooni_circumvention_collection']

# Check if data exists for the specified timeframe
data_exists = False
if start_date and end_date:
    existing_data = collection.find_one({'measurement_start_day': {'$gte': start_date, '$lte': end_date}})
    data_exists = bool(existing_data)

message = ""

# If data doesn't exist, fetch it from external source
if not data_exists:
    message = "Data is being downloaded for the selected timeframe. Please wait for the next dashboard refresh to view the updated visualization."

    # Define query parameters
    database_name = "ooni_circumvention"

    # Define URL and headers for your Flask endpoint
    url = "http://flask-app:5002/your-endpoint"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # Modify payload to use parameters
    payload = {
        "start_date": str(start_date),
        "end_date": str(end_date),
        "database_name": str(database_name),
    }

    # Send POST request to Flask endpoint
    response = requests.post(url, headers=headers, data=payload)
    data = response.json()

else:
    # If data exists, no need to fetch from an external source
    message = "Data is already available for the selected timeframe."

# Query MongoDB for anomaly_count and measurement_start_day
pipeline = [
    {
        "$match": {
            "measurement_start_day": {"$gte": start_date, "$lte": end_date}
        }
    },
    {
        "$project": {
            "month": {"$substr": ["$measurement_start_day", 0, 7]},  # Extract YYYY-MM from the date
            "anomaly_count": 1,
        }
    },
    {
        "$group": {
            "_id": "$month",
            "total_anomaly_count": {"$sum": "$anomaly_count"},
        }
    },
    {
        "$sort": {"_id": -1}  # Sort by month descending
    },
    {
        "$limit": 12  # Limit to the last 12 months
    }
]

results = list(collection.aggregate(pipeline))

# Extract data for plotting
columns = [
    {'name': 'month', 'type': 'date', 'friendly_name': 'Month'},
    {'name': 'total_anomaly_count', 'type': 'integer', 'friendly_name': 'Total Anomaly Count'}
]

rows = []

for result in results:
    try: 
        month_value = datetime.datetime.strptime(result['_id'], '%Y-%m')
        rows.append({
            'month': month_value,
            'total_anomaly_count': result['total_anomaly_count']
        })
    except ValueError as ve:
        print(f"Error parsing month value for {result['_id']}: {ve}")
        # skip this data point and continue with the loop
        continue

# Close the MongoDB connection
client.close()

# Return the result dictionary along with the message and data availability flag
result = {'columns': columns, 'rows': rows, 'message': message}
print(result)
