#!/usr/bin/env python3

import requests
import json
import csv
import MySQLdb
import pprint
from sql import *
from sql.aggregate import *
from sql.conditionals import *

db = MySQLdb.Connect(host='173.255.208.109',
                     user='root',
                     passwd='toot',
                     db = 'gentry')
db.autocommit(True)

def getNeighborhood(jsonObj):
    for component in jsonObj['results'][0]['address_components']:
        if('neighborhood' in  component['types']):
            neighborhood = component['long_name']
            return neighborhood
    return None;

cur = db.cursor();
cur.execute('SELECT * FROM building_permit WHERE neighborhood IS NULL LIMIT 1000')
results = cur.fetchall()

for row in results:
    rowID = row[0]
    lat = row[-2]
    long = row[-3]
    if(lat != 0 and long != 0):
            #https://maps.googleapis.com/maps/api/geocode/json?latlng=40.714224,-73.961452&
        url = "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + str(lat) + "," + str(long)
        jsonObj = requests.get(url).json()
        pprint.pprint(jsonObj)
        neighborhood = getNeighborhood(jsonObj)
        if(neighborhood != None):
            cur.execute ("""
                UPDATE building_permit
                SET neighborhood=%s
                WHERE id=%s
             """, (neighborhood, rowID))
            pprint.pprint(neighborhood + str(rowID))
