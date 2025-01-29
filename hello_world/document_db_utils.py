# Function to Save Metrics to AtlasDB
# TODO update lambda to save db
import os
from pymongo import MongoClient


def save_metrics_to_documentdb(mongodb_APIgateway_uri, db_name, collection_name, event_json):
    try:

        # Connect to DocumentDB
        client = MongoClient(mongodb_APIgateway_uri)
        db = client[db_name]  # Database name: jw-mongodb-db
        collection = db[collection_name]  # Collection name: jw-inference-pipeline-metrics

        print(f"Client >>> {client}")

        print(f"db >>> {db}")

        print(f"collection >>> {collection}")

        # Insert the metrics
        result = collection.insert_one(event_json)  # Insert the document

        print(f"event_json inserted with ID: {result.inserted_id}")

        # Close the connection
        client.close()
    except Exception as e:
        print(f"Error saving metrics to DocumentDB: {e}")
