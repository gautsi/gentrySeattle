#!/usr/bin/env python3

import requests
import json
import csv
import MySQLdb
import pprint
import urllib
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

def getLatLong(jsonObj):
    if 'location' in jsonObj['results'][0]['geometry']:
        location = jsonObj['results'][0]['geometry']['location']
        return location
    return None;   

def fillNeighborhoodByLatLong(results):
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

def fillLatLongByAddress():
    cur = db.cursor();
    cur.execute('SELECT * FROM building_permit WHERE latitude = 0 AND Address <> "" order by id asc')
    results = cur.fetchall()
    for row in results:
        rowID = row[0]
        combined = row[2] + " Seattle WA"
        address = urllib.urlencode({"address" : combined})
            #https://maps.googleapis.com/maps/api/geocode/json?latlng=40.714224,-73.961452&
        url = "https://maps.googleapis.com/maps/api/geocode/json?" + address
        jsonObj = requests.get(url).json()
        #pprint.pprint(jsonObj)
        location = getLatLong(jsonObj)
        pprint.pprint(location)
        if(location != None):
            cur.execute ("""
                UPDATE building_permit
                SET latitude=%s, longitude=%s
                WHERE id=%s
             """, (location['lat'],location['lng'], rowID))
            pprint.pprint( str(address) + " " + str(location['lat']) + str(location['lng'])  + " " + str(rowID))
