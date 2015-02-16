# importSQL *** work in progress, don't hate me ***
A simple python script to import data from an import.io source to an sql database

# Dependencies
You will need to install the following:

1. python
1. python-mysqldb
1. python-urllib2

## Running it

To run it you have two option, but firstly you need to have your table and database ready

Run the following commands to set up the demo table:

mysql>

```
CREATE DATABASE ILOVEOWLS;
USE ILOVEOWLS;

CREATE TABLE OMFGowls
(
field_image varchar(255),
field_name varchar(255),
field_price varchar(255),
field_size varchar(255),
field_colour varchar(255)
);

```

### 1. Using a config script 

* Using this option you can set up a configuration that you can set up once and re-use to get the latest data into yours database table

* Make sure you know your [import.io user credentials](https://import.io/data/account/)

* Create a config.json file that follows this pattern:

```
{
	// Config for import.io
	"sourceUUID": "94cdc938-c24e-42db-b94f-3fb852c450a9",
	"inputUrl": "http://owlkingdom.com",
	"ioUserID": "[your User ID]",
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
* RUN IT! `importSQL [optional:-U [sql username] -P [sql password] -u [io user ID] -p [io API key]]`

*This json file above will grab the owls from [Owl Kingdom](http://owlkingdom.com) and put them into your SQL table*

#### What is this "mapping"?

This mapping script simply converts the data in the import.io columns to the field names in your sql database

```
"mapping": {
	"image": "field_image",
	"name": "field_name",
	"price": "field_price",
	"size": "field_size",
	"colour": "field_colour"
}
```


### 2. Using commandline options

* When using just commandline options, be aware the the script will assume that the column names from import.io match the columns names in mysql

Here are the list of commandline options you can use:

* *-U* mysql username -(default: root)-
* *-P* mysql password -(default: root)-
* *-H* mysql host name -(default: localhost)-
* *-E* mysql port number -(default: 3306)-
* *-t* mysql table name
* *-d* mysql database name

* *-u* import.io userID
* *-p* import.io APIKey
* *-s* import.io source UUID
* *-t* mysql password