# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In Febuary 2024 I set about [experimenting with data processing](https://paulr70.substack.com/p/data-processing-experiment-part-0) - I created a small framework to allow:
# MAGIC
# MAGIC - easy configuration of tables and tasks to load, clean, transform and output data, 
# MAGIC - getting experience with 
# MAGIC   - Kotlin, 
# MAGIC   - Spark, 
# MAGIC   - Data processing basics, 
# MAGIC   - Code design and 
# MAGIC   - Writing
# MAGIC
# MAGIC Now I plan to get some experience with DataBricks notebooks. Doing a similar thing in a different way...

# COMMAND ----------

pip install pyjson5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utility functions
# MAGIC
# MAGIC These are convenience methods to cleanly load files (closing after) and for getting values.

# COMMAND ----------

import pyjson5
import sys

print(sys.version)

def loadJson(path):
    with open(path, "r") as file:
        return pyjson5.load(file)

def valueOrDefault(dictionary, key, default):
    if key not in dictionary:
        return default
    return dictionary[key]

def readFile(path):
    with open(path, "r") as file:
        return file.read()

print(valueOrDefault({'a':'1', 'b':'2'}, 'c', "default")) # should return default
print(valueOrDefault({'a':'1', 'b':'2'}, 'b', "default")) # should return 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare state

# COMMAND ----------

user_id = spark.sql('select current_user() as user').collect()[0]['user']
basePath = "dbfs:/FileStore/shared_uploads/" + user_id
localPath = "/tmp/config/sample1"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Prepare table configuration
# MAGIC
# MAGIC Copy the configuration from dbfs to local file so it can be read from local files.

# COMMAND ----------

# copy the file from dbfs so it can be read as a local file
# these files were uploaded to dbfs using the UI
# https://stackoverflow.com/a/67601809

dbutils.fs.cp(basePath + "/config/sample1/", "file:" + localPath + "/", recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load table configuration
# MAGIC Load the JSON5 table configuration so we can work with it.

# COMMAND ----------

import json
import pyjson5

table_config = loadJson(localPath + '/sample1_tables.json5')
print(json.dumps(table_config, indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load raw tables
# MAGIC Now we want to load the tables as specified in the configuration.

# COMMAND ----------

raw_context = {}

for source in table_config['sources']:
    table = source['table']
    path = basePath + "/" + source['path']
    delimiter = valueOrDefault(table, 'delimiter', ',')
    print("Loading table " + source['id'] + " from " + source['path'] )
    # load dataframe
    df = spark.read.format(source['type']) \
        .option("header", "true") \
        .option("sep", delimiter) \
        .load(path)
    # display
    df.printSchema()
    display(df)
    # add to context
    raw_context[source['id']]=df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selected tables
# MAGIC Now that we have access to the raw data, in the "Selected" stage we select only the columns that we want and rename them to a standard column name. In our table configuration we can specify multiple names for the columns (in case data is delivered inconsistently) and need to select the first one that exists.
# MAGIC
# MAGIC Configuration for the table and columns looks like this:
# MAGIC ```json
# MAGIC           ...
# MAGIC           "table": {
# MAGIC                 "name": "transactions",
# MAGIC                 "description": "account transactions",
# MAGIC                 "deduplicate": true,
# MAGIC                 "trim": true,
# MAGIC                 "columns": [
# MAGIC                     {
# MAGIC                         "names": [
# MAGIC                             "date"
# MAGIC                         ],
# MAGIC                         "alias": "date",
# MAGIC                         "description": "date of transaction",
# MAGIC                         "type": "date",
# MAGIC                         "formats": [
# MAGIC                             "yyyy-MM-dd",
# MAGIC                             "dd-MM-yyyy"
# MAGIC                         ],
# MAGIC                         "required": true
# MAGIC                     },
# MAGIC                     {
# MAGIC                         "names": [
# MAGIC                             "account"
# MAGIC                         ],
# MAGIC                         "alias": "account",
# MAGIC                         "description": "account",
# MAGIC                         "type": "string",
# MAGIC                         "required": true
# MAGIC                     },
# MAGIC                     ...
# MAGIC ```
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

def first_column(df, column_names):
    for c in column_names:
        if (c in df.columns):
            return c
        
    raise Exception("Could not find any of {0} in {1}".format(column_names, df.columns()))

standard_context = {}

for source in table_config['sources']:
    df = raw_context[source['id']]
    table = source['table']
    cols_to_select = []

    for column in table['columns']:
        cols_to_select.append(col(first_column(df, column['names'])).alias(column['alias']))

    df = df.select(cols_to_select)

    # display
    df.printSchema()
    display(df)
    # update context
    standard_context[source['id']]=df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Typed tables
# MAGIC Now we need to transform the columns to the configured types. 
# MAGIC
# MAGIC Column configuration looks like this:
# MAGIC ```json
# MAGIC                 ...
# MAGIC                 "columns": [
# MAGIC                     {
# MAGIC                         "names": [
# MAGIC                             "date"
# MAGIC                         ],
# MAGIC                         "alias": "date",
# MAGIC                         "description": "date of transaction",
# MAGIC                         "type": "date",
# MAGIC                         "formats": [
# MAGIC                             "yyyy-MM-dd",
# MAGIC                             "dd-MM-yyyy"
# MAGIC                         ],
# MAGIC                         "required": true
# MAGIC                     },
# MAGIC                     ...
# MAGIC ```
# MAGIC Each type is handled differently, and in this example we see how dates can have multiple formats.

# COMMAND ----------

from pyspark.sql.functions import coalesce, to_date, trim

typed_context = {}

def typed_column(df, column):
    col_type = column['type']
    c = trim(col(column['alias'])).alias(column['alias'])
    # Date formatter, supporting multiple formats
    if (col_type=='date'):
        cols = []
        for f in column['formats']:
            cols.append(to_date(c, f))
        # pick the first non-null value
        return coalesce(*cols).alias(column['alias'])
    elif (col_type == 'string'):
        return c
    elif (col_type == 'integer'):
        return c.cast('integer').alias(column['alias'])
    elif (col_type == 'decimal'):
        return c.cast('decimal(10,2)').alias(column['alias'])
    else:
        # return the column as is if there is no mapping
        print ("WARNING Type {0} not supported, using default.".format(col_type))
        return c
        # raise Exception("Type {0} not supported".format(type))

for source in table_config['sources']:
    df = standard_context[source['id']]
    table = source['table']
    cols_to_select = []

    for column in table['columns']:
        col_type = column['type']
        cols_to_select.append(typed_column(df, column))

    df = df.select(cols_to_select)

    # display
    df.printSchema()
    display(df)
    # update context
    typed_context[source['id']]=df

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary
# MAGIC
# MAGIC Here, I've gone from loading raw data from configuration to a typed dataset. There is still much to do to get it to parity with the functionality of the Kotlin codebase, but using python without any Object Oriented design wouldn't be satifying so I'm going to leave it here, and try a different approach later.
