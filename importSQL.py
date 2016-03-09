#!/usr/bin/env python

import MySQLdb as mdb
from optparse import OptionParser
import json
import urllib2
import urllib
import sys
import zlib


def get_required_config_data(options, config_data, config_name):
    if config_name not in options.__dict__ or options.__dict__[config_name] is None:
        if config_name not in config_data or config_data[config_name] is None:
            print "Missing required option: " + config_name
            print "This option needs to be in a config file, or supplied on the commandline"
            sys.exit(1)
    else:
        config_data[config_name] = options.__dict__[config_name]

def get_optional_config_data(options, config_data, config_name, default):
    if options.__dict__[config_name] is None:
        if default is not None and config_name not in config_data:
            config_data[config_name] = default
    else:
        config_data[config_name] = options.__dict__[config_name]


def getConfigOptions(options, config_data):

    # Get required config data
    get_required_config_data(options, config_data, "sourceUUID")
    get_required_config_data(options, config_data, "table")
    get_required_config_data(options, config_data, "database")

    get_required_config_data(options, config_data, "ioAPIKey")
    get_required_config_data(options, config_data, "inputUrl")

    # Grab optional configuration parameters, use defaults if they don't exist
    get_optional_config_data(options, config_data, "host", "localhost")
    get_optional_config_data(options, config_data, "port", 3306)
    get_optional_config_data(options, config_data, "username", None)
    get_optional_config_data(options, config_data, "password", None)
    get_optional_config_data(options, config_data, "crawl", False);

    return config_data


def grab_from_crawl_snapshot(sourceUUID, ioAPIKey):
    """
    Grab the data from a crawler snapshot
    :param sourceUUID:
    :param ioAPIKey:
    :return:
    """
    url_auth_params = urllib.urlencode({"_apikey": ioAPIKey})

    connector_url = 'https://api.import.io/store/data/' + sourceUUID + "?" + url_auth_params
    connector_response = json.loads(urllib2.urlopen(connector_url).read())
    snapshot_guid = connector_response["snapshot"]

    # Have to use gzip encoding for this
    request = urllib2.Request(
        'https://api.import.io/store/data/' + sourceUUID + "/_attachment/snapshot/" + snapshot_guid + "?" + url_auth_params)
    request.add_header('Accept-encoding', 'gzip')
    response = urllib2.urlopen(request)
    snapshot_response = json.loads(zlib.decompress(response.read(), 16 + zlib.MAX_WBITS))

    crawledPages = snapshot_response["tiles"][0]["results"][0]["pages"]

    results = []

    for page in crawledPages:
        results.extend(page["results"])

    return results

def import_rest_query(sourceUUID, inputUrl, ioAPIKey):
    """
    Grab the data from import.io
    :param sourceUUID:
    :param inputUrl:
    :param ioAPIKey:
    :return:
    """
    urlParams = urllib.urlencode({"input/webpage/url": inputUrl, "_apikey": ioAPIKey})
    url = 'https://api.import.io/store/data/' + sourceUUID + '/_query?' + urlParams

    response = urllib2.urlopen(url).read()
    return json.loads(response)["results"]


def push_to_sql(config_data, results):
    """
    Convert the data to a reasonable format and stick it in SQL
    :param config_data:
    :param results:
    :return:
    """
    field_mappings = None

    if "mapping" not in config_data:
        print "Using default mapping from import.io data, assuming the field names are identical to the SQL field names"
    else:
        field_mappings = config_data["mapping"]
        sql_field_mapping = []

        for mapping in field_mappings:
            sql_field_mapping.append(field_mappings[mapping])

        sql_field_mapping_string = ", ".join(sql_field_mapping)
        print sql_field_mapping_string

        print "Mappings: %s" % field_mappings

    con = None
    try:

        print "Connecting to SQL server..."

        if "password" in config_data:
            if "username" not in config_data:
                print "Missing username! You need to place a username in your config.json or supply it on the commandline"
            con = mdb.connect(host=config_data["host"], port=config_data["port"], user=config_data["username"],
                              passwd=config_data["password"], db=config_data["database"], charset='utf8')
        else:
            con = mdb.connect(host=config_data["host"], port=config_data["port"],
                              db=config_data["database"], charset='utf8')
        cur = con.cursor()

        print "Connected!"

        for result in results:

            values = []

            if (field_mappings is not None):
                # Get the values for each row based on the mapping that we supplied in config.json
                for mapping in field_mappings:
                    if result[mapping] is not None:
                        values.append("'" + unicode(result[mapping]) + "'")
            else:
                # Get the values from the import.io source (assume the field names are identical)
                sql_field_mapping = [];
                for key in result:
                    sql_field_mapping.append(key.replace("/_", "_"))
                    values.append("'" + unicode(result[key]) + "'")
                sql_field_mapping_string = ", ".join(sql_field_mapping)

            print "Row data: %s" % ', '.join(values)

            query_string = "INSERT INTO " + config_data[
                "table"] + " (" + sql_field_mapping_string + ") VALUES(" + ', '.join(["%s" for i in values]) + ");"

            cur.execute(query_string, values)
            con.commit()

        cur.close()

        print "%s:%s" % (config_data["host"], config_data["port"])

    except (RuntimeError, TypeError, NameError, mdb.Error) as e:
        print "Error %s: " % e
    except BaseException as e:
        print "Error when connecting to sql server: %s" % e
    finally:
        if con:
            con.rollback()
            con.close()
        sys.exit(1)


def do_import(configData):
    if configData["crawl"] == True:
        results = grab_from_crawl_snapshot(configData["sourceUUID"], configData["ioAPIKey"])
    else:
        results = import_rest_query(configData["sourceUUID"], configData["inputUrl"], configData["ioAPIKey"]);
    print "Recieved %d rows of data" % (len(results))
    push_to_sql(configData, results)


if __name__ == "__main__":
    parser = OptionParser()

    # Get the config options from the commandline

    # Import.io setup info
    parser.add_option("-p", "--ioapikey", dest="ioAPIKey", help="Your import.io API key")
    parser.add_option("-i", "--inputURL", dest="inputUrl", help="The input url for the extractor")
    parser.add_option("-s", "--sourceUUID", dest="sourceUUID",
                      help="The data source you wish to grab data from and put it in a table")
    parser.add_option("-c", "--crawl", action="store_true", dest="crawl",
                      help="flag for if this information is stored in a crawler")

    # MySQL setup info
    parser.add_option("-U", "--username", dest="username", help="Your mysql username")
    parser.add_option("-P", "--password", dest="password", help="Your mysql password")
    parser.add_option("-t", "--table", dest="table", help="The name of the table you wish to import the data into")
    parser.add_option("-d", "--database", dest="database",
                      help="The name fo the mysql database that you wish to import your data into")
    parser.add_option("-H", "--host", dest="host", help="The mysql host")
    parser.add_option("-E", "--port", dest="port", help="The mysql port")

    (options, args) = parser.parse_args()

    config_data = {}
    config_data["username"] = options.username;
    config_data["password"] = options.password;
    config_data["ioAPIKey"] = options.ioAPIKey;

    # Check if we have a config file: If we do, USE IT
    try:
        config_data_file = open('config.json')
        config_data = json.load(config_data_file)
        config_data_file.close()
        print "CONFIG FOUND, YAY!"
    except IOError:
        print 'NO CONFIG FILE FOUND, going to use defaults', 'config.json'

    # Do the call to import and push it out to the sql database in the configuration file
    config_data = getConfigOptions(options, config_data)
    do_import(config_data)
