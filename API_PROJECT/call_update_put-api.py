import requests

payload = {
    "name":"dipali",
    "salary":35000,
    "loc":"USA"
}

response = requests.put(
    "http://localhost:8000/employee/7",
    json=payload
)

print(response.json())