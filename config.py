from optparse import OptionParser
import json

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
    get_required_config_data(options, config_data, "inputUrls")

    # Grab optional configuration parameters, use defaults if they don't exist
    get_optional_config_data(options, config_data, "host", "localhost")
    get_optional_config_data(options, config_data, "port", 3306)
    get_optional_config_data(options, config_data, "username", None)
    get_optional_config_data(options, config_data, "password", None)
    get_optional_config_data(options, config_data, "crawl", False);

    return config_data

def getConfig():
    parser = OptionParser()

    # Get the config options from the commandline

    # Import.io setup info
    parser.add_option("-p", "--ioapikey", dest="ioAPIKey", help="Your import.io API key")
    parser.add_option("-i", "--input", dest="inputUrl", help="The input url for the extractor")
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

    # transform inputUrl to an Array
    options.inputUrls=[options.inputUrl]

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

    return config_data
