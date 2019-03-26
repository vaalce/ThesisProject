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
collection = db.tms_stations

#retrieve data

r=requests.get(url='https://tie.digitraffic.fi/api/v1/metadata/tms-stations?lastUpdated=false&state=active')
stations = r.json()
counter = 0
tms_stations = {}

for i in stations['features']:
    if i['properties']['municipalityCode'] == '405':
        collection.insert_one({
            "roadId": i['id'],
            "Timestamp": stations["dataUpdatedTime"],
            "roadStationId": i["properties"]
        })
#insert data
#collection.insert_one(tms_stations)

print("Data inserted with record")

# Printing the data inserted
cursor = collection.find()
for record in cursor:
    print(record)
