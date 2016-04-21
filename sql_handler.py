import datetime
import MySQLdb as mdb

def insert_into_db(config_data, results):
    """
    Convert the data to a reasonable format and stick it in SQL
    :param config_data:
    :param results:
    :return:
    """
    
    mappings = config_data["mapping"]

    con, cur = connect_to_db(config_data["host"], config_data["port"], config_data["username"], config_data["password"], config_data["database"])

    for result in results:
        values = []
        if (mappings is not None):
            sql_field_mapping = []
            # Get the values for each row based on the mapping that we supplied in config.json
            for mapping in mappings:
                if mapping in result:
                    sql_field_mapping.append(mappings[mapping])
                    values.append(result[mapping])
        else:
            # Get the values from the import.io source (assume the field names are identical)
            sql_field_mapping = []
            for key in result:
                sql_field_mapping.append(key.replace("/_", "_"))
                values.append(result[mapping])

        if "addDate" in config_data and config_data["addDate"]:
            sql_field_mapping.append("date")
            values.append(datetime.datetime.now().isoformat().split('T')[0])

        
        # creates ( %s, %s, %s ) with the right amount of %s
        sql_sequence = "( %s"
        for sql_field in sql_field_mapping[1:]:
            sql_sequence += ", %s "
        sql_sequence += ")"


        sql_field_mapping_string = ", ".join(sql_field_mapping)
        
        print sql_field_mapping_string, values

        query_string = "INSERT INTO " + config_data["table"] + " (" + sql_field_mapping_string + ") VALUES" + sql_sequence + ";"

        config_data["table"]
        print query_string

        cur.execute(query_string, values)
        con.commit()

    cur.close()

    print "%s:%s" % (config_data["host"], config_data["port"])

    # finally:
    #     if con:
    #         con.rollback()
    #         con.close()
    #     sys.exit(1)

def connect_to_db(host, port, username, password, database):
    try:
        con = mdb.connect(host=host, port=port, user=username, passwd=password, db=database)
        # con = mdb.connect(host=host, port=port, db=database)
    except (RuntimeError, TypeError, NameError, mdb.Error) as e:
        # print "Error %d: %s" % (e.args[0], e.args[1])
        print "Error %s"  % e
    except BaseException as e:
        print "Error when connecting to sql server: %s" % e
    else:
        print "Connected!"
        cur = con.cursor()
        return (con, cur)


