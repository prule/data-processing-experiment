# Module Data Processing Experiment - Spark

This module uses [SparkSQL](https://spark.apache.org/sql/) to process the configuration and provide dataframes for processing. 

# Package com.example.dataprocessingexperiment.spark.types

This package implements type conversions as specified in the configuration. 

For each type conversion supported, there is an implementation of Typer.
In the most simple case these can just be a CAST to the required type.

A formats parameter can be used to supply formatting information - each type will have it's own way of defining and interpreting these formats so see the documentation for the specific type.

Refer to the tests for examples of behaviour.