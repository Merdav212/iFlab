import requests
import json

url = "http://flask-app:5003/your-endpoint"

data = {
    "database": "testdb",
    "start_date": "2022-01-01",
    "end_date": "2023-01-01"
}

headers = {
    "Content-Type": "application/json"
}

response = requests.post(url, data=json.dumps(data), headers=headers)
