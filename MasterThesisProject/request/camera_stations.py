import json
import pandas
import requests

from pymongo import MongoClient

connection = MongoClient("mongodb://localhost:27017")
db=connection.dataapis

try:
  conn=MongoClient()
  print ("Connected successfully!!")
except:
  print ("Could not connect to MongoDB")

# database
db = conn.dataapis

# create collection
collection = db.camerastationsLPR

#retrieve data api

r=requests.get(url='https://tie.digitraffic.fi/api/v1/metadata/camera-stations?lastUpdated=false')
cameras = r.json()
counter = 0
camera_stations = {}

for i in cameras['features']:
    if i['properties']['municipalityCode'] == '405':
        collection.insert_one({
            "camera_stations": i['id'],
            "timestamp": cameras["dataUpdatedTime"],
            "roadStationId":i["properties"]
            })


print("Data inserted with record")

# Printing the data inserted
cursor = collection.find()
for record in cursor:
    print(record)
