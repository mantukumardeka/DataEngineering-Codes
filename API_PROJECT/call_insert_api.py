import requests

payload = {
    "emp_id": 7,
    "name": "dipali",
    "salary": 30000,
    "loc": "India"
}

response = requests.post(
    "http://localhost:8000/employee",
    json=payload
)

print(response.json())