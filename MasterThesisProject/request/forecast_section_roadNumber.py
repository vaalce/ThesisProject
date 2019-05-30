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
r=requests.get(url='https://tie.digitraffic.fi/api/v2/metadata/forecast-sections?lastUpdated=false')
roads = r.json()
print ("Current date API: " + roads['dataUpdatedTime'])


#If collection does not exist, creates it and fill it out for the first time from source API
if "forecastsections" not in db.list_collection_names():
    collection = db['forecastsections']
#Insert data just from LPR

    roads_lpr = {"roads_lpr":[]}
    print("New collection created")
    for i in range(len(roads['features'])):
        if roads['features'][i]['properties']['roadNumber'] == 387 or roads['features'][i]['properties']['roadNumber'] == 13 or roads['features'][i]['properties']['roadNumber'] == 6:
            print (roads['features'][i]['properties']['roadNumber'])
            collection.insert_one(roads['features'][i]['properties'])
    # collection.insert_one(roads_lpr)
    # print(roads_lpr)

    print("**********************************")




# Printing the data inserted
# cursor = collection.find()
# for record in cursor:
#     print(record)

# for i in range(len(roads['features'])):
#     if roads['features'][i]['properties']['roadNumber'] == 387 or roads['features'][i]['properties']['roadNumber'] == 13 or roads['features'][i]['properties']['roadNumber'] == 6:
#         print (roads['features'][i]['properties']['roadNumber'])
#         roads_lpr["roads_lpr"].append(roads['features'][i]['properties'])
# collection.insert_one(roads_lpr)
# print(roads_lpr)
