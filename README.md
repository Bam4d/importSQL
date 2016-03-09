# importSQL
A configurable and re-usable python script to import data from an import.io extractor into an SQL database

# Dependencies
You will need to install the following:

1. python
1. python-mysqldb (used as an sql client only)
1. A running SQL server (SQLlite, MySQL etc...)


## Running it

To run it you have a few options, but firstly you need to have your table and database ready

Run the following commands to set up the demo table:

mysql>

```
CREATE DATABASE ILOVEOWLS 
    DEFAULT CHARACTER SET utf8 
    DEFAULT COLLATE utf8_general_ci;
    
USE ILOVEOWLS;

CREATE TABLE OMFGowls
(
field_image varchar(255),
field_name varchar(255),
field_price varchar(255),
field_size varchar(255),
field_colour varchar(255)
) DEFAULT CHARACTER SET utf8
  DEFAULT COLLATE utf8_general_ci;

```

### Using a config Script

* Using this you can set up a configuration that you can set up once and re-use to get the latest data into yours database table

* Make sure you know your [import.io user credentials](https://import.io/data/account/)

* Create a config.json file that follows this pattern:

```
{
	// Config for import.io
	"sourceUUID": "94cdc938-c24e-42db-b94f-3fb852c450a9",
	"inputUrl": "http://owlkingdom.com",
	"ioAPIKey": "[your API key]",

	// Config for mysql
	"table": "OMFGowls",
	"database": "ILOVEOWLS",
	"host": "localhost",
	"port": 3306,
	"username": "root",
	"password": "root",
 
	"mapping": {
		"image": "field_image",
		"name": "field_name",
		"price": "field_price",
		"size": "field_size",
		"colour": "field_colour"
	}
}
```

* Put it in the same directory as your importSQL script.
* RUN IT! `importSQL [optional:-U [sql username] -P [sql password] -p [io API key]]`

*This json file above will grab the owls from [Owl Kingdom](http://owlkingdom.com) and put them into your SQL table*

### mapping

This mapping field defines the mapping between the column names in import.io and the column names in your MySQL database

```
"mapping": {
   	// import.io	// MySQL
	"image": 	"field_image",
	"name": 	"field_name",
	"price": 	"field_price",
	"size": 	"field_size",
	"colour": 	"field_colour"
}
```

### Getting Data from Crawlers

To get all the data from your crawl from import.io into your SQL database, you can use the **-c** option to turn on crawler mode.
This will get the data from the last crawl snapshot and not directly query the crawler using an input url.

(if you have settings in a config file, they will be loaded, but overwritten by anything you supply on the commandline)

```
importSQL -c -s "your crawler guid" [optional:-U [sql username] -P [sql password] -p [io API key]]
```


### Using commandline options

* When using just commandline options, be aware the the script will assume that the column names from import.io match the columns names in mysql

Here are the list of commandline options you can use:

* **-U** mysql username _(default: root)_
* **-P** mysql password _(default: root)_
* **-H** mysql host name _(default: localhost)_
* **-E** mysql port number _(default: 3306)_
* **-t** mysql table name
* **-d** mysql database name

* **-p** import.io APIKey
* **-s** source UUID
* **-i** input url for data source
* **-c** flag to tell if the source you want data from is an uploaded crawl
