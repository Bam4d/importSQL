#!/usr/bin/env python2

import json
import urllib2
import urllib
import sys
import zlib

from config_handler import getConfig
from sql_handler import insert_into_db




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


def do_import(configData):
    if configData["crawl"] == True:
        results = grab_from_crawl_snapshot(configData["sourceUUID"], configData["ioAPIKey"])
    else:
        results = []
        for inputUrl in configData["inputUrls"]:
            results = results + import_rest_query(configData["sourceUUID"], inputUrl, configData["ioAPIKey"]);
    print "Recieved %d rows of data" % (len(results))
    insert_into_db(configData, results)


if __name__ == "__main__":

    config_data = getConfig()

    print "Using this config", config_data

    do_import(config_data)
