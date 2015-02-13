#!/usr/bin/env python

import mysqldb
from optparse import OptionParser
import json

print "I'm still working on this so wait a few days until i'm done <3"
sys.exit(1)

def getRequiredConfigData(options,configData, configName):
	if options[configName] is None:
		print "Missing required option: " + configName
		sys.exit(1)

def getConfigOptions(options, configData): 
	getRequiredConfigData(options, configData, "sourceUUID")
	getRequiredConfigData(options, configData, "table")
	getRequiredConfigData(options, configData, "database")
	return configData

def importRESTQuery():
	# Need to do this still LOL

def pushToSQL():
	# Need to do this still LOL

def doImport(configData):
	# Need to do this also LOL

parser = OptionParser()

# Get the config options from the commandline
parser.add_option("-U", "--username", dest="username")
parser.add_option("-P", "--password", dest="password")
parser.add_option("-s", "--sourceUUID", dest="sourceUUID", help="The data source you wish to grab data from and put it in a table")
parser.add_option("-t", "--table", dest="table", help="The name of the table you wish to import the data into")
parser.add_option("-d", "--database", dest="database", help="the name fo the mysql database that you wish to import your data into")

(options, args) = parser.parse_args()

configData = {}
configData["username"] = options.username;
configData["password"] = options.password;

# Check if we have a config file: If we do, USE IT
try:
    configDataString=open('config.json')
    configData = json.load(configDataJson)
	json_data.close()
    print "CONFIG FOUND, YAY!"
except IOError:
    print 'NO CONFIG FILE FOUND, going to use defaults', 'config.json'
    configData = getRequiredConfigData()

#Do the call to import and push it out to the sql database in the configuration file
doImport(configData)



