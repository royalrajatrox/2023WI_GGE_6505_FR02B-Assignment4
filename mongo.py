import json
import pymongo
import sodapy
import requests
from sodapy import Socrata
import pandas as pd
from pymongo import MongoClient
import requests

myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017/")

mydb = myclient["citydata"]
mycol = mydb["citydataall"]
mycol1 = mydb["newcollection"]

dataApi = "https://www.giantbomb.com/api/reviews/?api_key=32d7804fc03abd5e65e393be7f8f4dd2ee283f01&format=json"

headers = {
    "user_agent": "royalrajatrox API Access"
}

review_fields = "id,dlc_name,score,deck,description"

pages = list(range(0, 20))
pages_list = pages[0:20:10]


def get_reviwer(url_base, num_pages, fields, collection):
    field_list = "&field_list=" + fields + "&sort=score:desc" + "&offset="

    for page in num_pages:
        url = url_base + field_list + str(page)
        response = requests.get(url, headers=headers).json()
        video_games = response['results']
        for i in video_games:
            collection.insert_one(i)
            print("Got the data")


get_reviwer(dataApi, pages_list, review_fields, mycol1)
print(get_reviwer)

scores = []
from pprint import pprint

for score in list(mycol.find({}, {"_id": 0, "score": 10})):
    scores.append(score)

pprint(scores[:5])

urlnew = "https://www.giantbomb.com/api/search/"

params = {
    "api_key": "32d7804fc03abd5e65e393be7f8f4dd2ee283f01",  #we have hidden our API key
    "format": "json",
    "query": "game",
    "resources": "game",
    "limit": 10
}

response = requests.get(urlnew, params=params)
# datanew1 = response.json()




# for item in datanew["results"]:
#     mycol1.insert_one(item)
