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
          "10,2"
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

Now with the configuration externalised we can make changes without having to update and deploy code - while this may not seem like much at the moment, notice how the code to achieve this is very small, simple, and easy to work with. Also note that this configuration could be sourced from anywhere - like from a database (a web application could be used to store, edit, version configurations). The configuration also acts as easy to read documentation about what data you are using and how the values are typed.

It's important to acknowledge that using the external configuration (json5) isn't mandatory. You can still get some benefits from the hardcoded version - reuse of functionality, consistent code patterns etc. External configuration is in addition to these benefits.

From now on, any data that I need to read in can be accessed in this way - reusing this functionality and pattern. You may have noticed how the DateType allows multiple formats - useful for when your data is not consistent - and now I get this functionality (and the rest) for free in the future.

Leveraging patterns and functionality like this is one way to get faster and more efficient value out of your projects. 

Running the application via gradle (configured with the required --add-exports values) we get:

```bash
% ./gradlew app:run

> Task :app:run
Starting...
SLF4J: Class path contains multiple SLF4J providers.
SLF4J: Found provider [ch.qos.logback.classic.spi.LogbackServiceProvider@6236eb5f]
SLF4J: Found provider [org.apache.logging.slf4j.SLF4JServiceProvider@7c1e2a9e]
SLF4J: See https://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual provider is of type [ch.qos.logback.classic.spi.LogbackServiceProvider@6236eb5f]
14:49:17.487 [main] WARN  o.a.hadoop.util.NativeCodeLoader MDC= - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable

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

From here the code can be progressed to provide more functionality which I'll address in the next couple of parts...