#!/usr/bin/env python2

import MySQLdb as mdb
import json
import urllib2
import urllib
import sys
import zlib

from config import getConfig




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

    try:
        response = urllib2.urlopen(url)
    except URLError as e:
        if hasattr(e, 'reason'):
            print 'We failed to reach a server.'
            print 'Reason: ', e.reason
        elif hasattr(e, 'code'):
            print 'The server couldn\'t fulfill the request.'
            print 'Error code: ', e.code
        sys.exit
    else:
        response_json = json.loads(response.read())
        print response_json
        return response_json["results"];


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
        sql_field_mapping = [];

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
                              passwd=config_data["password"], db=config_data["database"])
        else:
            con = mdb.connect(host=config_data["host"], port=config_data["port"],
                              db=config_data["database"])
        cur = con.cursor()

        print "Connected!"

        for result in results:

            values = []

            if (field_mappings is not None):
                # Get the values for each row based on the mapping that we supplied in config.json
                for mapping in field_mappings:
                    if result[mapping] is not None:
                        values.append("'" + str(result[mapping]) + "'")
            else:
                # Get the values from the import.io source (assume the field names are identical)
                sql_field_mapping = [];
                for key in result:
                    sql_field_mapping.append(key.replace("/_", "_"))
                    values.append("'" + str(result[key]) + "'")
                sql_field_mapping_string = ", ".join(sql_field_mapping)

            sql_field_values_string = ", ".join(values)
            print "row data: %s" % sql_field_values_string

            query_string = "INSERT INTO " + config_data[
                "table"] + " (" + sql_field_mapping_string + ") VALUES(" + sql_field_values_string + ");"

            cur.execute(query_string)
            con.commit()

        cur.close()

        print "%s:%s" % (config_data["host"], config_data["port"])

    except (RuntimeError, TypeError, NameError, mdb.Error) as e:
        print "Error %d: %s" % (e.args[0], e.args[1])
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
        results = []
        for inputUrl in configData["inputUrls"]:
            results = results + import_rest_query(configData["sourceUUID"], inputUrl, configData["ioAPIKey"]);
    print "Recieved %d rows of data" % (len(results))
    push_to_sql(configData, results)


if __name__ == "__main__":

    config_data = getConfig()

    print "Using this config" + config_data

    do_import(config_data)
