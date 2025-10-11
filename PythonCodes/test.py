import requests
import os

url = "https://raw.githubusercontent.com/databricks/LearningSparkV2/master/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
# choose a local path where you want to save it
local_path = "/Users/mantukumardeka/Desktop/delete/sf-fire-calls.csv"

# ensure the directory exists
os.makedirs(os.path.dirname(local_path), exist_ok=True)

# download
response = requests.get(url)
if response.status_code == 200:
    with open(local_path, "wb") as f:
        f.write(response.content)
    print("Download successful:", local_path)
else:
    print("Failed to download. Status code:", response.status_code)
