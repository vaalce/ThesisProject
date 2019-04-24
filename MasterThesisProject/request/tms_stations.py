import json
import pandas
import requests

from pymongo import MongoClient

#Connect to db
connection = MongoClient("mongodb://localhost:27017")
db=connection.dataapis

try:
  conn=MongoClient()
  print ("Connected successfully!!")
except:
  print ("Could not connect to MongoDB")

# database
db = conn.dataapis

#Query data from API
r=requests.get(url='https://tie.digitraffic.fi/api/v1/metadata/tms-stations?lastUpdated=false&state=active')
stations = r.json()
print ("Current date API: " + stations['dataUpdatedTime'])

#If collection does not exist, creates it and fill it out for the first time from source API
if "tms_stations" not in db.list_collection_names():
    collection = db['tms_stations']
#Filter data just by LPR municipalityCode
    counter = 0
    tms_stations = {}
    print("New collection created")
    for i in stations['features']:
        if i['properties']['municipalityCode'] == '405':
            collection.insert_one({
                "roadId": i['id'],
                "Timestamp": stations["dataUpdatedTime"],
                "roadStationId": i["properties"]
            })
#If collection exists, get the most recent timestamp from mongodb
else:
     lastupdatedb=db.tms_stations.find_one(sort=[("Timestamp",-1)])["Timestamp"]
#Compare timestamp versus source api and agregate data
     if stations['dataUpdatedTime'] > lastupdatedb:
         counter = 0
         tms_stations = {}
         print (lastupdatedb, "Freshly data inserted")
         for i in stations['features']:
             if i['properties']['municipalityCode'] == '405':
                 db.tms_stations.insert_one({
                     "roadId": i['id'],
                     "Timestamp": stations["dataUpdatedTime"],
                     "roadStationId": i["properties"]
                 })
     else:
            print ("Data is up to date:" , lastupdatedb)

# Printing the data inserted
# cursor = collection.find()
# for record in cursor:
#     print(record)
