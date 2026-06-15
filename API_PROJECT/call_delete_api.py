import requests

response = requests.delete(
    "http://localhost:8000/employee/7"
)

print(response.json())