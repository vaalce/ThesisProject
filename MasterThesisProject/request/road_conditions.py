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
collection = db.roadconditions

#retrieve data api

r=requests.get(url='https://tie.digitraffic.fi/api/v1/data/road-conditions?lastUpdated=false')
roads = r.json()

relevant_roads = {'00013', '00006', '00387'}
relevant_sections = {"241" , "304", "303", "240" , "301", "239" ,"215" ,"216" ,"302" , "305","308" , "007"  }


counter = 0
road_conditions = {}

for i in roads['weatherData']:
    road_number = i['id'].split('_')[0]
    road_section = i['id'].split('_')[1]
    if road_number in relevant_roads and road_section in relevant_sections:
        collection.insert_one({
            "strid": i["id"],
            "roadConditions": i["roadConditions"]
        })


print("Data inserted with record")

# Printing the data inserted
cursor = collection.find()
for record in cursor:
    print(record)
