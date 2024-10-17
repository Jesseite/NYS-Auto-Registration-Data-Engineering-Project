import pymongo
import requests
import os
from dotenv import load_dotenv

#Loading variables from .env file
load_dotenv()

#JSON URL
url = "https://data.ny.gov/resource/w4pv-hbkt.json"

#Fetch data from the url
response = requests.get(url)

#Ensure the URL connection is working
if response.status_code == 200:
    json_data = response.json()

    #Connect to MongoDB
    client = pymongo.MongoClient(os.getenv('MONGO_CLIENT'))
    db = client[os.getenv('MONGO_DATABASE')]
    collection = db[os.getenv('MONGO_COLLECTION')]

    #Load the JSON data into MongoDB
    if isinstance(json_data, list):
        collection.insert_many(json_data)

    else:
        collection.insert_one(json_data)
    
    print("The JSON data has been inserted")

    #Close MongoDB connection
    client.close

else:
    print(F"An error occurred:  {response.status_code}")