import csv

class importer(filename):
    csvfilename = ''
    row = 0
    parsedDict = []
    
    def __init__(self, filename):
        setFilename(filename)
        pass
    
    def setFilename(filename):
        self.csvfilename = filename
        pass
    #Will read from specified line using an internal counter by default
    def readLine(row = self.row, delimiter = ','):
        with open('eggs.csv', 'rb') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for row in spamreader:
               self.parsedDict.add(row) 
        pass
#    Import the csv
#   Read the 1st line into dictionary header class var
#   Ea. read line will take the next line and map
    pass

with open('culture.csv', 'rb') as csvfile:
     spamreader = csv.DictReader(csvfile, delimiter=',', quotechar='|')
     for row in spamreader:
        print ', '.join(row)


from peewee import *
import MySQLdb

database = MySQLDatabase('gentry')

db = MySQLdb.Connect(host='localhost',
                     user='root',
                     db = 'gentry')

cur = db.cursor()

cur.execute("select * from building_permit limit 10")


                                        