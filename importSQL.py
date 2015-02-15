#!/usr/bin/env python

import MySQLdb as mdb
from optparse import OptionParser
import json
import urllib2

print "I'm still working on this so wait a few days until i'm done <3"
sys.exit(1)

def getRequiredConfigData(options,configData, configName):
	if options[configName] is None:
		print "Missing required option: " + configName
		sys.exit(1)
	else:
		configName[configData] = options[configName]

def getOptionalConfigData(options, configData, configName, default):
	if options[configName] is None:
		configName[configData] = default
	else:
		configName[configData] = options[configName]


def getConfigOptions(options, configData): 
	getRequiredConfigData(options, configData, "sourceUUID")
	getRequiredConfigData(options, configData, "table")
	getRequiredConfigData(options, configData, "database")

	getRequiredConfigData(options, configData, "ioUserID")
	getRequiredConfigData(options, configData, "ioAPIKey")

	getOptionalConfigData(options, configData, "host", "localhost")
	getOptionalConfigData(options, configData, "port", 3306)
	getOptionalConfigData(options, configData, "username", "root")
	getOptionalConfigData(options, configData, "password", "root")
	return configData

def importRESTQuery(sourceUUID, ioUserID, ioAPIKey):
	url = 'https://api.import.io/store/data/' + sourceUUID + '/_query?input/webpage/url=http%3A%2F%2Fowlkingdom.com%2F&_user=' + ioUserID + '&_apikey=' + ioAPIKey
	response = urllib2.urlopen(url).read()
	return JSON.loads(response)["results"];

def pushToSQL(configData, results):

	fieldMappings = configData.mapping

	sqlFieldMappingString = ""

	for mapping in fieldMappings:
		sqlFieldMappingString = sqlFieldMappingString + ", " + fieldMappings[mapping]

	print sqlFieldMappingString

	try:
	    con = mdb.connect(configData.host, configData.username, configData.password, configData.database);

	    cur = con.cursor()

	    for result in results:
	    	values = []
	    	for mapping in fieldMapping:
	    		if result[mapping] is None:
	    			print "Missing data for " + field
		    		values.append("")
		    	else
		    		values.append(result[mapping])

		   	queryString = "INSERT INTO " + configData.table + "(" + sqlFieldMappingString + ") VALUES("+sqlFieldValuesString+")"
		    print queryString

	    	cur.execute(queryString)

	    ver = cur.fetchone()
	    
	    print "Database version : %s " % ver
	    
	except mdb.Error, e:
	  
	    print "Error %d: %s" % (e.args[0],e.args[1])
	    sys.exit(1)

def doImport(configData):
	results = importRESTQuery(configData.sourceUUID, configData.ioUserID, configData.ioAPIKey);

parser = OptionParser()

# Get the config options from the commandline
parser.add_option("-U", "--username", dest="username", help="Your mysql username")
parser.add_option("-P", "--password", dest="password", help="Your mysql password")
parser.add_option("-u", "--iouserid", dest="ioUserID", help="Your import.io user ID")
parser.add_option("-p", "--ioapikey", dest="ioAPIKey", help="Your import.io API key")
parser.add_option("-s", "--sourceUUID", dest="sourceUUID", help="The data source you wish to grab data from and put it in a table")
parser.add_option("-t", "--table", dest="table", help="The name of the table you wish to import the data into")
parser.add_option("-d", "--database", dest="database", help="The name fo the mysql database that you wish to import your data into")
parser.add_option("-h", "--host", dest="host", help="The mysql host")
parser.add_option("-p", "--port", dest="port", help="The mysql port")

(options, args) = parser.parse_args()

configData = {}
configData["username"] = options.username;
configData["password"] = options.password;
configData["ioUserID"] = options.ioUserID;
configData["ioAPIKey"] = options.ioAPIKey;

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



