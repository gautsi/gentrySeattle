import csv
import MySQLdb
import pprint
from sql import *
from sql.aggregate import *
from sql.conditionals import *

class importer():
    csvfilename = ''
    row = 0
    parsedDict = []
    db = MySQLdb.Connect(host='localhost',
                     user='root',
                     db = 'gentry')
    
    def __init__(self, filename):
        self.setFilename(filename)
        pass
    
    def setFilename(self,filename):
        self.csvfilename = filename
        pass
    #Will read from specified line using an internal counter by default
    def readFile(self):
        with open(self.csvfilename, 'rb') as csvfile:
            spamreader = csv.DictReader(csvfile)
            for row in spamreader:
               self.parsedDict.append(row)
        pass
    
    def getHeader(self):
        with open(self.csvfilename, 'rb') as csvfile:
            spamreader = csv.DictReader(csvfile)
            header = spamreader[0]
            header_keys = [normalize_text(x) in header.keys()]
        pass
    
    def normalize_text(self, text):
        final_text = text.lower().replace(" ", "_").strip()
        return final_text
    
    
    pass

#
#from peewee import *
#import MySQLdb
#
#database = MySQLDatabase('gentry')
#
#
#cur = db.cursor()
#
#cur.execute("select * from building_permit limit 10")

parsedDict = []
with open('culture.csv', 'rb') as csvfile:
            spamreader = csv.DictReader(csvfile)
            for row in spamreader:
               parsedDict.append(row)
               
               
pprint.pprint(parsedDict[0].keys())
str = "Seats total"
