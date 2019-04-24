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
r=requests.get(url='https://tie.digitraffic.fi/api/v1/metadata/camera-stations?lastUpdated=false')
cameras = r.json()
print ("Current date API: " + cameras['dataUpdatedTime'])

#If collection does not exist, creates it and fill it out for the first time from source API
if "camerastationsLPR" not in db.list_collection_names():
    collection = db['camerastationsLPR']
#Filter data just by LPR municipalityCode
    counter = 0
    camera_stations = {}
    print("New collection created")
    for i in cameras['features']:
        if i['properties']['municipalityCode'] == '405':
            collection.insert_one({
                "camera_stations": i['id'],
                "timestamp": cameras["dataUpdatedTime"],
                "roadStationId":i["properties"]
                })
#If collection exists, get the most recent timestamp from mongodb
else:
     lastupdatedb=db.camerastationsLPR.find_one(sort=[("timestamp",-1)])["timestamp"]
#Compare timestamp versus source api and agregate data
     if cameras['dataUpdatedTime'] > lastupdatedb:
            counter = 0
            camera_stations = {}
            print ("lastupdatedb + Freshly data inserted")
            for i in cameras['features']:
                if i['properties']['municipalityCode'] == '405':
                    db.camerastationsLPR.insert_one({
                        "camera_stations": i['id'],
                        "timestamp": cameras["dataUpdatedTime"],
                        "roadStationId":i["properties"]
                        })
     else:
            print ("Data is up to date:" , lastupdatedb)


#db.camerastationsLPR.find().sort({timestamp:-1}).limit(1)
#print (f"{lastupdatedb} and {cameras['dataUpdatedTime']} ")


# Printing the data inserted
# cursor = collection.find()
# for record in cursor:
#     print(record)
