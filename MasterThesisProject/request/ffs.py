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
collection = db.freeflowspeeds

#retrieve data api

r=requests.get(url='https://tie.digitraffic.fi/api/v1/data/free-flow-speeds?lastUpdated=false')
freeflowspeeds = r.json()
counter = 0
freeflowspeedsdata = {}

for i in freeflowspeeds['tmsFreeFlowSpeeds']:
    if i['tmsNumber'] == 306 or i['tmsNumber'] == 533 or i['tmsNumber'] == 534 or i['tmsNumber'] == 547 or i['tmsNumber'] == 556 or i['tmsNumber'] == 557 or i['tmsNumber'] == 558 or i['tmsNumber'] == 559 or i['tmsNumber'] == 560 or i['tmsNumber'] == 561 or i['tmsNumber'] == 562 or i['tmsNumber'] == 563 or i['tmsNumber'] == 564 or i['tmsNumber'] == 582 or i['tmsNumber'] == 593 or i['tmsNumber'] == 597:
        collection.insert_one({
            "tmsstationId": i['id'],
            "timestamp": freeflowspeeds["dataUpdatedTime"],
            "freeFlowSpeed1": i["freeFlowSpeed1"],
            "freeFlowSpeed2": i["freeFlowSpeed2"],
            "tmsNumber": i["tmsNumber"]
        })
         

print("Data inserted with record")

# Printing the data inserted
cursor = collection.find()
for record in cursor:
    print(record)
