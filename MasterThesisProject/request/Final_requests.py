#!/usr/bin/env python
# coding: utf-8

# In[3]:


import json


# In[4]:


import pandas


# In[5]:


import requests


# In[29]:


r=requests.get(url='https://tie.digitraffic.fi/api/v1/metadata/tms-stations?lastUpdated=false&state=active')


# In[30]:


print(r.json())


# In[31]:


stations = r.json()
counter = 0
tms_stations = {}

for i in stations['features']:
    if i['properties']['municipalityCode'] == '405':
        tms_stations[i['id']] = {"Timestamp": stations["dataUpdatedTime"],
                          "roadStationId": i["properties"]
                         }


# In[166]:


tms_stations


# In[167]:


with open('static_tms_stations.json', 'w') as fp:
    json.dump(vault, fp)


# In[170]:


r=requests.get(url='https://tie.digitraffic.fi/api/v1/data/free-flow-speeds?lastUpdated=false')


# In[171]:


print(r.json())


# In[151]:


freeflowspeeds = r.json()
counter = 0
freeflowspeedsdata = {}

for i in freeflowspeeds['tmsFreeFlowSpeeds']:
    if i['tmsNumber'] == 306 or i['tmsNumber'] == 533 or i['tmsNumber'] == 534 or i['tmsNumber'] == 547 or i['tmsNumber'] == 556 or i['tmsNumber'] == 557 or i['tmsNumber'] == 558 or i['tmsNumber'] == 559 or i['tmsNumber'] == 560 or i['tmsNumber'] == 561 or i['tmsNumber'] == 562 or i['tmsNumber'] == 563 or i['tmsNumber'] == 564 or i['tmsNumber'] == 582 or i['tmsNumber'] == 593 or i['tmsNumber'] == 597:
        freeflowspeedsdata[i['id']] = {'timestamp':freeflowspeeds['dataUpdatedTime'],
                                    'freeFlowSpeed1': i['freeFlowSpeed1'],
                                    'freeFlowSpeed2': i['freeFlowSpeed2'],
                                    'tmsNumber':i['tmsNumber']
                                      }




# In[152]:


freeflowspeedsdata


# In[153]:


with open('free_flow_speeds.json', 'w') as fp:
    json.dump(freeflowspeedsdata, fp)


# In[180]:


r=requests.get(url='https://tie.digitraffic.fi/api/v1/metadata/camera-stations?lastUpdated=false')


# In[188]:


print(r.json())


# In[182]:


cameras = r.json()
counter = 0
camera_stations = {}

for i in cameras['features']:
    if i['properties']['municipalityCode'] == '405':
        camera_stations[i['id']] = {'timestamp':cameras['dataUpdatedTime'],
                                    'roadStationId':i["properties"]
                                      }


# In[183]:


camera_stations


# In[184]:


with open('camera_station_LPR.json', 'w') as fp:
    json.dump(camera_stations, fp)


# In[6]:


r=requests.get(url='https://tie.digitraffic.fi/api/v1/data/road-conditions?lastUpdated=false')


# In[7]:


print(r.json())


# In[222]:


roads = r.json()


# In[227]:


roads['dataUpdatedTime']


# In[17]:


relevant_roads = {'00013', '00006'}


# In[26]:


roads = r.json()
counter = 0
road_conditions = {}

for i in roads['weatherData']:
    road_number = i['id'].split('_')[0]
    road_section = i['id'].split('_')[1]
    if road_number == "00006" or road_number == "00013":
        road_conditions[road_number + '_' +  road_section] = {"roadConditions": i["roadConditions"]}


# In[28]:


len(road_conditions)


# In[ ]:
