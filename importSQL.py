#!/usr/bin/env python

import mysqldb
from optparse import OptionParser
import json

# Check if we have a config file: If we do, USE IT
configData=open('config.json')

data = json.load(configData)
print json.dumps(data)
json_data.close()

parser = OptionParser()

parser.add_option("-s", "--sourceUUID", dest="sourceUUID", help="The data source you wish to grab data from and put it in a table")
parser.add_option("-t", "--table", dest="table", help="The name of the table you wish to import the data into")
parser.add_option("-d", "--database", dest="database", help="the name fo the mysql database that you wish to import your data into")
parser.add_option("-U", "--username", dest="username")
parser.add_option("-P", "--password", dest="password")