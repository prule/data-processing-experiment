Externalising the table configuration
====

When data is coming from external sources, I think it would be useful to be able to externalise the configuration so it can be easily changed. It will also let many datasets be read without having to write lots of boiler plate code. 

I want to be able to use a configuration that represents a CSV like the following and have code interpret it and build a spark dataframe containing these columns, typed appropriately:

```
{
    name: "transactions",
    description: "account transactions",
    columns: [
      {
        name: "date",
        description: "date of transaction",
        type: "date",
        formats: [
          "yyyy-MM-dd",
          "dd-MM-yyyy"
        ]
      },
      {
        name: "account",
        description: "account",
        type: "string",
      },
      {
        name: "description",
        description: "description",
        type: "string",
      },
      {
        name: "amount",
        description: "amount can be a positive (credit) or negative (debit) number representing dollars and cents",
        type: "decimal",
        formats: [
          "10",
          "2"
        ]
      }
    ]
  }
```

With this configuration, the code simply [becomes](https://github.com/prule/data-processing-experiment/blob/part-3/app/src/main/kotlin/com/example/dataprocessingexperiment/app/App.kt):

```kotlin
val fileSource = Json5.decodeFromStream<FileSource>(
    this::class.java.getResourceAsStream("/sample1.statements.json5")!!
)

// set up the dataframe
val dataFrameBuilder = DataFrameBuilder(
    sparkSession,
    fileSource,
    Types.all(),
    "../data/"
)

// get the typed version of the dataset, with columns and types specified in config
val typedDataset = dataFrameBuilder.typed()
```
DataFrameBuilder reads the RAW dataset and uses the FileSource configuration to produce a TYPED dataset. The typed dataset will only contain the columns specified in the configuration and uses the Types to do the conversion.

Previously everything was [hardcoded](https://github.com/prule/data-processing-experiment/blob/part-2/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/Spike1.kt):

```kotlin
// load raw data frame
val statementsDataFrame = sparkSession.read()
    .format("csv")
    .option("header", true)
    .load("../data/sample1/statements")
    .alias("statements")

// only select the columns needed so we can exclude data we don't need here
val selectedDataFrame = statementsDataFrame.select(
    functions.col("date"),
    functions.col("account"),
    functions.col("description"),
    functions.col("amount"),
)

// convert to typed columns
val typedDataFrame = selectedDataFrame.select(
    functions.to_date(functions.col("date"), "yyyy-MM-dd").alias("amount"),
    functions.col("account"),
    functions.col("description"),
    functions.col("amount").cast("double").alias("amount")
)
```

The key points here are:

- FileSource contains information about where the data is along with metadata to help make the configuration self documenting. [DataFrameBuilder.raw](https://github.com/prule/data-processing-experiment/blob/part-3/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/DataFrameBuilder.kt) will use this to load the table using Spark.
```json5
{
  name: "sample 1",
  description: "sample 1 description",
  path: "sample1/statements/",
  type: "csv",
  table: {
    name: "transactions",
    description: "account transactions",
    columns: [
      ...
    ]
  }
```

- Column definitions identify the name of the column - which must match the column header in the CSV - and the data type with formatting options.
```json5
{
  name: "date",
  description: "date of transaction",
  type: "date",
  formats: [
    "yyyy-MM-dd",
    "dd-MM-yyyy"
  ]
}
```
- Type classes are implemented to perform the conversion - for some situations this can be a simple CAST, but for richer functionality much more can be implemented here. See [DateType](https://github.com/prule/data-processing-experiment/blob/part-3/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/types/DateType.kt) for an example. This is extensible - all we have to do is implement the `Typer` interface to add new capabilities and register the types with their names in a  `Types` instance which we pass to `DataFrameBuilder`.
```kotlin
class IntegerType : Typer {
    override fun key(): String {
        return "integer"
    }

    override fun process(name: String, formats: List<String>?): Column {
        return functions.col(name).cast("integer").alias(name)
    }
}
```

- DataFrameBuilder.typed() builds a collection of columns appropriately typed by using the type definition to return a spark column appropriately converted. It then selects these columns from the raw dataframe:
```kotlin
fun typed(): Dataset<Row> {
    val typedColumns: List<Column> = fileSource.table.columns.map { column -> types.get(column.type).process(column.name, column.formats) }
    return raw.select(*typedColumns.map { it }.toTypedArray())
}
```


----

Now with the configuration externalised we can make changes without having to update and deploy code - while this may not seem like much at the moment, notice how the code to achieve this is very small, simple, and easy to work with. 

Also note that this configuration could be sourced from anywhere - like from a database (a web application could be used to store, edit and version configurations). The configuration also acts as easy to read documentation about what data you are using and how the values are typed. You might be able to imagine a system where this information could be easily accessible/published, so it can be easily referenced (similar in concept to JavaDoc).

It's important to acknowledge that using the external configuration (json5) isn't mandatory. You can still get some benefits from a hardcoded version - See [DataFrameBuilderTest](https://github.com/prule/data-processing-experiment/blob/c9df1629f8bdac3153c4993adbe0efbdedb140ad/spark/src/test/kotlin/com/example/dataprocessingexperiment/spark/DataFrameBuilderTest.kt#L22) (where the configuration would be created by code instead of deserializing json) - reuse of functionality, consistent patterns, and future functionality I'll be introducing soon. External configuration is in addition to these benefits.

From now on, any data that I need to read can be accessed in this way - reusing this functionality and pattern. You may have noticed how the DateType allows multiple formats - useful for when your data is not consistent - and now I get this functionality (and the rest) for free in the future.

Leveraging patterns and functionality like this is one way to get faster and more efficient value out of your projects. By creating a capability (easily create dataframes) we can leverage this again and again - and process data in a consistent manner.

From here the code can be progressed to provide more functionality which I'll address in the next couple of parts...

- Now that we have table definitions, I can imagine creating more capabilities to:
    - generate data in this format
    - produce documentation explaining how data is used
    - produce reports detailing insights about the data
        - in both raw and typed states
    - filter out invalid data

So let's do a sanity check at this point:

- Complexity = VERY LOW
- Value = SMALL, LIMITED
- Potential = MEDIUM

Granted this is a subjective assessment, but lets see how it shapes up after further progressions.


----
Running the application via gradle (configured with the required --add-exports values) we get:

```text
% ./gradlew app:run

> Task :app:run
Starting...

Raw data frame

root
 |-- date: string (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- amount: string (nullable = true)
 |-- comment: string (nullable = true)

+------------+-------+------------+-------+--------------------+
|        date|account| description| amount|             comment|
+------------+-------+------------+-------+--------------------+
|  2020-13-01|      x|      burger|   0.01|        invalid date|
|invalid date|      x|      petrol|   0.02|        invalid date|
|        NULL|      x|      tennis|   0.03|             no date|
|  2020-12-01|       |      tennis|   0.04|          no account|
|  2020-12-01|      x|      petrol|      x| invalid number f...|
|  01-03-2020|      1|      burger|  15.47|alternative date ...|
|  03-03-2020|      1|      tennis|  35.03|alternative date ...|
|  04-03-2020|      2|      petrol| 150.47|alternative date ...|
|  2020-02-01|      1|      burger|  15.46|                NULL|
|  2020-02-02|      1|       movie|  20.01|                NULL|
|  2020-02-03|      1|      tennis|  35.01|                NULL|
|  2020-02-04|      2|      petrol| 150.46|                NULL|
|  2020-02-04|      2| electricity| 300.47|                NULL|
|  2020-01-01|      1|      burger|  15.45|                NULL|
|  2020-01-02|      1|       movie|  20.00|                NULL|
|  2020-01-03|      1|      tennis|  35.00|                NULL|
|  2020-01-04|      2|      petrol| 150.45|                NULL|
+------------+-------+------------+-------+--------------------+


Typed data frame

root
 |-- date: date (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- amount: decimal(10,2) (nullable = true)

+----------+-------+------------+------+
|      date|account| description|amount|
+----------+-------+------------+------+
|      NULL|      x|      burger|  0.01|
|      NULL|      x|      petrol|  0.02|
|      NULL|      x|      tennis|  0.03|
|2020-12-01|       |      tennis|  0.04|
|2020-12-01|      x|      petrol|  NULL|
|2020-03-01|      1|      burger| 15.47|
|2020-03-03|      1|      tennis| 35.03|
|2020-03-04|      2|      petrol|150.47|
|2020-02-01|      1|      burger| 15.46|
|2020-02-02|      1|       movie| 20.01|
|2020-02-03|      1|      tennis| 35.01|
|2020-02-04|      2|      petrol|150.46|
|2020-02-04|      2| electricity|300.47|
|2020-01-01|      1|      burger| 15.45|
|2020-01-02|      1|       movie| 20.00|
|2020-01-03|      1|      tennis| 35.00|
|2020-01-04|      2|      petrol|150.45|
+----------+-------+------------+------+

Finished...

BUILD SUCCESSFUL in 3s
10 actionable tasks: 5 executed, 5 up-to-date
```