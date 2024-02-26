from pymongo import MongoClient
import datetime

# Connect to MongoDB
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

# Define the pipeline with the date match stage
pipeline = [
    {"$match": {"measurement_start_day": {"$gte": start_date, "$lte": end_date}}},
    {
        "$project": {
            "month": {"$substr": ["$measurement_start_day", 0, 7]},  # Extract YYYY-MM from the date
            "confirmed_count": 1,
            "country_code": "$CountryCode",
        }
    },
    {
        "$group": {
            "_id": {"country": "$country_code", "month": "$month"},
            "total_anomaly_count": {"$sum": "$confirmed_count"},
        }
    },
    {
        "$sort": {"_id.month": -1, "_id.country": 1}  # Sort by month descending and then by country
    },
    {
        "$limit": 12 * len(collection.distinct("CountryCode"))  # Limit to the last 12 months for each country
    }
]

# Execute the pipeline
results = list(collection.aggregate(pipeline))

# Extract data for plotting
columns = [
    {'name': 'month', 'type': 'date', 'friendly_name': 'Month'},
    {'name': 'total_anomaly_count', 'type': 'integer', 'friendly_name': 'Total Anomaly Count'},
    {'name': 'country', 'type': 'string', 'friendly_name': 'Country Code'},
]

rows = []

for result in results:
    rows.append({
        'month': datetime.datetime.strptime(result['_id']['month'], '%Y-%m'),
        'total_anomaly_count': result['total_anomaly_count'],
        'country': result['_id']['country']
    })

# Close the MongoDB connection
client.close()

# Return the result dictionary
result = {'columns': columns, 'rows': rows}
print(result)
