# importSQL *** work in progress, don't hate me ***
A simple python script to import data from an import.io source to an sql database

# Dependencies
You will need to install the following:

1. python
1. python-mysqldb

## Running it

To run it you have two option, but firstly you need to have your table and database ready

Run the following commands to set up the demo table:

mysql>

```
CREATE DATABASE ILOVEOWLS;
USE DATABASE ILOVEOWLS;

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

* Create a config.json file that follows this pattern:

```
{
	"sourceGuid": "94cdc938-c24e-42db-b94f-3fb852c450a9",
	"table": "OMFGowls",
	"database": "ILOVEOWLS",
	"host": "localhost",
	"port": 3306,
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
* RUN IT! `importSQL [optional:-U sqlusername -P sqlpassword]`


*This json file above will grab the owls from [Owl Kingdom](http://owlkingdom.com) and put them into your SQL table*

#### What is this "mapping"?

This mapping script simply converts the data in the import.io columns to the field names in your sql database

```
"mapping": {
	"image": "field_image",
	"name": "field_name",
	"price": "field_price",
	"size": "field_size",
	"image": "field_colour"
}
```


### 2. Using commandline properties
