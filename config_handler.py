import argparse
import json
import sys

def get_required_config_data(args, config_data, config_name):
    if config_name not in args.__dict__ or args.__dict__[config_name] is None:
        if config_name not in config_data or config_data[config_name] is None:
            print "Missing required option: " + config_name
            print "This option needs to be in a config file, or supplied on the commandline"
            sys.exit(1)
    else:
        config_data[config_name] = args.__dict__[config_name]

def get_optional_config_data(args, config_data, config_name, default):
    if args.__dict__[config_name] is None:
        if default is not None and config_name not in config_data:
            config_data[config_name] = default
    else:
        config_data[config_name] = args.__dict__[config_name]


def getConfigOptions(args, config_data):

    # Get required config data
    get_required_config_data(args, config_data, "sourceUUID")
    get_required_config_data(args, config_data, "table")
    get_required_config_data(args, config_data, "database")

    get_required_config_data(args, config_data, "ioAPIKey")
    get_required_config_data(args, config_data, "inputUrls")

    # Grab optional configuration parameters, use defaults if they don't exist
    get_optional_config_data(args, config_data, "host", "localhost")
    get_optional_config_data(args, config_data, "port", 3306)
    get_optional_config_data(args, config_data, "username", None)
    get_optional_config_data(args, config_data, "password", None)
    get_optional_config_data(args, config_data, "crawl", False)

    get_optional_config_data(args, config_data, "addTimestamp", False)

    return config_data

def getConfig():
    parser = argparse.ArgumentParser(description='Calculate metrics from benchmarked datasets')

    # Get the config options from the commandline

    # Import.io setup info
    parser.add_argument("-p", "--ioapikey", dest="ioAPIKey", help="Your import.io API key")
    parser.add_argument("-i", "--inputURL", dest="inputUrls", help="The input url for the extractor", action="append")
    parser.add_argument("-s", "--sourceUUID", dest="sourceUUID",
                      help="The data source you wish to grab data from and put it in a table")
    parser.add_argument("-c", "--crawl", action="store_true", dest="crawl",
                      help="flag for if this information is stored in a crawler")

    # MySQL setup info
    parser.add_argument("-U", "--username", dest="username", help="Your mysql username")
    parser.add_argument("-P", "--password", dest="password", help="Your mysql password")
    parser.add_argument("-T", "--timestamp", dest="addTimestamp", help="Add a timestamp to the fields", action='store_true')
    parser.add_argument("-t", "--table", dest="table", help="The name of the table you wish to import the data into")
    parser.add_argument("-d", "--database", dest="database",
                      help="The name fo the mysql database that you wish to import your data into")
    parser.add_argument("-H", "--host", dest="host", help="The mysql host")
    parser.add_argument("-E", "--port", dest="port", help="The mysql port")

    args = parser.parse_args()

    config_data = {}

    # Check if we have a config file: If we do, USE IT
    try:
        config_data_file = open('config.json')
        config_data = json.load(config_data_file)
        config_data_file.close()
        print "CONFIG FOUND, YAY!"
    except IOError:
        print 'NO CONFIG FILE FOUND, going to use defaults', 'config.json'

    # Do the call to import and push it out to the sql database in the configuration file
    config_data = getConfigOptions(args, config_data)

    return config_data
