import os
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import pandas as pd
from pymongo import MongoClient
from prefect import flow, task

# Fetch MongoDB URI from environment variable or use a default value
MONGO_URI = "mongodb+srv://Yasshuu:pass123@cluster0.x8qfcnt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

@task
def run_notebook(notebook_path: str):
    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)
        ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
        ep.preprocess(nb, {'metadata': {'path': './'}})
    print(f"Executed notebook: {notebook_path}")

@task
def read_processed_data():
    # Read the processed DataFrames from the pickle files
    category_ratings = pd.read_pickle('category_ratings.pkl')  # insight 3
    # monthly_sales = pd.read_pickle('monthly_sales.pkl')  # insight 4
    top_products = pd.read_pickle('top_products.pkl')  # insight 2
    top_customers = pd.read_pickle('top_customers.pkl')  # insight 1
    aggregation = pd.read_pickle('aggregation.pkl')   #aggregation
    return category_ratings, top_products, top_customers, aggregation

@task
def connect_to_mongo():
    client = MongoClient(MONGO_URI)
    return client

@task
def delete_collections(client):
    db = client['Advarisk_aggregation_insights']
    collection_names = db.list_collection_names()
    for name in collection_names:
        db[name].drop()
    print(f"Dropped collections: {collection_names}")


@task
def insert_data(client, data_dict):
    db = client['Advarisk_aggregation_insights']  
    inserted_ids = {}

    for collection_name, data in data_dict.items():
        collection = db[collection_name]
        records = data.to_dict('records')
        result = collection.insert_many(records)
        inserted_ids[collection_name] = result.inserted_ids

    return inserted_ids

@flow(name="MongoDB Insert Data Flow", log_prints=True)
def main_flow():
    # Execute the notebook
    run_notebook('Data_pipeline.ipynb')

    # Read the processed data
    category_ratings, top_products, top_customers, aggregation = read_processed_data()

    # Connect to MongoDB
    client = connect_to_mongo()

    # Delete all collections
    delete_collections(client)

    # Prepare data dictionary
    data_dict = {
        'category_ratings': category_ratings,
        # 'monthly_sales': monthly_sales,
        'top_products': top_products,
        'top_customers': top_customers,
        'aggregation': aggregation
    }

    # Insert data into MongoDB
    insert_result = insert_data(client, data_dict)
    print(f"Inserted document IDs: {insert_result}")

if __name__ == "__main__":
    main_flow.serve(name="mongo-db-insert-data-deployment", cron="0 */3 * * *")
