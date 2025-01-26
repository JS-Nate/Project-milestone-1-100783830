from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import os
import csv

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0]

# Configure Pub/Sub
project_id = "peak-seat-448616-v4"
topic_name = "car-data-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# Read and publish data from CSV
csv_file_path = "Labels.csv"
with open(csv_file_path, mode="r") as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # Convert row to JSON
        message = json.dumps(row).encode("utf-8")
        print(f"Publishing record: {message}")
        future = publisher.publish(topic_path, message)
        future.result()
print(f"All records published to topic {topic_name}.")
