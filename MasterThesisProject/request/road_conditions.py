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
r=requests.get(url='https://tie.digitraffic.fi/api/v1/data/road-conditions?lastUpdated=false')
roads = r.json()
print ("Current date API: " + roads['dataUpdatedTime'])

#Store only roads and sections from LPR
relevant_roads = {'00013', '00006', '00387'}
relevant_sections = {"241" , "304", "303", "240" , "301", "239" ,"215" ,"216" ,"302" , "305","308" , "007"  }

#If collection does not exist, creates it and fill it out for the first time from source API
if "roadconditions" not in db.list_collection_names():
    collection = db['roadconditions']
#Insert data just from LPR
    counter = 0
    road_conditions = {}
    print("New collection created")
    for i in roads['weatherData']:
        road_number = i['id'].split('_')[0]
        road_section = i['id'].split('_')[1]
        if road_number in relevant_roads and road_section in relevant_sections:
            collection.insert_one({
                "strid": i["id"],
                "timestamp": roads["dataUpdatedTime"],
                "roadConditions": i["roadConditions"]
            })
    print("**********************************")
#If collection exists, get the most recent timestamp from mongodb
else:
     lastupdatedb=db.roadconditions.find_one(sort=[("timestamp",-1)])
    # print(lastupdatedb)
     lastupdatedb=lastupdatedb["timestamp"]
#Compare timestamp versus source api and agregate data
     if roads['dataUpdatedTime'] > lastupdatedb:
         counter = 0
         road_conditions = {}
         print (lastupdatedb , "Freshly data inserted")
         for i in roads['weatherData']:
             road_number = i['id'].split('_')[0]
             road_section = i['id'].split('_')[1]
             if road_number in relevant_roads and road_section in relevant_sections:
                 db.roadconditions.insert_one({
                     "strid": i["id"],
                     "roadConditions": i["roadConditions"],
                     "timestamp": roads["dataUpdatedTime"]
                 })
     else:
             print ("Data is up to date:" , lastupdatedb)




#print("Data inserted with record")

# Printing the data inserted
# cursor = collection.find()
# for record in cursor:
#     print(record)
