Enhancing table metadata
-

Now that we have an easy table configuration, the next step is to make it more useful. Here, I'm adding a `required` field to `Column`. 

With this, I can easily remove rows that don't have required fields. 

Super simple, I've added a `valid()` method to `DataFrameBuilder` which returns a dataframe with only valid rows:

```kotlin
fun valid(): Dataset<Row> {
    // required columns != null conditions
    val requiredColumns: List<Column> = fileSource.table.columns
        .filter { column -> column.required }
        .map { column ->
            // for strings, check for null and empty strings
            if (string.key() == column.type)
                col(column.name).isNotNull.and(
                    trim(col(column.name)).notEqual(functions.lit(""))
                )
            else
            // otherwise just check for null
                col(column.name).isNotNull
        }
    // and all columns together so none of the required columns can be null
    var combined: Column? = null
    requiredColumns.forEach { col ->
        combined = if (combined == null) col else combined!!.and(col)
    }
    // select all columns where the required columns are not null
    return typed().select("*").where(combined)
}
```

For a string column I'm checking for null or blank (trimming the column and comparing to and empty string). For everything else it's just a null check - remember, if dates don't match the right format, or if a value can't be converted by casting then the result will be NULL.

An interesting thought comes to mind now - should the type converters contain more logic to enforce validity? Or should this be a separate concern?

For example, IntegerType could take configuration to supply a min and/or max value
- with this it could return null for any values that fall outside the defined range. 
- and then, these invalid rows would be filtered from our valid dataframe

I'm not sure about this yet, so I think I'll defer this decision. With more information and experience options will shake out. Part of this experiment is about knowing when to go broad, and when to expand detail... the beauty of it is, at the moment, making changes is easy - the codebase is simple, flexible, unobtrusive so many things are possible.

The downside is that without specific use-cases the validity of the experiment is hard to judge. I'm not too concerned here either. It's unrealistic to build a silver bullet - there will be some cases where this will add value and be fit for purpose and there will be some that won't. 

In the future I intend to apply this framework to different datasets (different in shape and in size) and see how it pans out... 

So lets recap where we are:

- we can create a simple table definition in json5 
- we can easily build a dataframe from this, with columns properly typed
- we can easily filter out invalid rows where required fields are null
- all with a very small amount of flexible code

The next most interesting thing (to me, at the moment) is to derive some insights from this data. As an example, knowing the row count for the raw dataframe - and for the valid dataframe - lets us know the quantity of invalid rows. This type of insight should be easily configurable again, with some simple metadata... but that's for the next installment...

See the code:

- [App](https://github.com/prule/data-processing-experiment/blob/part-4/app/src/main/kotlin/com/example/dataprocessingexperiment/app/App.kt)
- [DataFrameBuilder](https://github.com/prule/data-processing-experiment/blob/part-4/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/DataFrameBuilder.kt)
- [sample1.statements.json5](https://github.com/prule/data-processing-experiment/blob/part-4/app/src/main/resources/sample1.statements.json5)

----

Running the application via gradle (configured with the required --add-exports values) we get:

```text
 % ./run run

Running application


> Task :app:run
Starting...

Raw dataset

root
 |-- date: string (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- amount: string (nullable = true)
 |-- comment: string (nullable = true)

+------------+-------+------------+-------+--------------------+
|        date|account| description| amount|             comment|
+------------+-------+------------+-------+--------------------+
|        NULL|      x|      tennis|   0.03|             no date|
|  01-03-2020|      1|      burger|  15.47|alternative date ...|
|  03-03-2020|      1|      tennis|  35.03|alternative date ...|
|  04-03-2020|      2|      petrol| 150.47|alternative date ...|
|  2020-01-01|      1|      burger|  15.45|                NULL|
|  2020-01-02|      1|       movie|  20.00|                NULL|
|  2020-01-03|      1|      tennis|  35.00|                NULL|
|  2020-01-04|      2|      petrol| 150.45|                NULL|
|  2020-02-01|      1|      burger|  15.46|                NULL|
|  2020-02-02|      1|       movie|  20.01|                NULL|
|  2020-02-03|      1|      tennis|  35.01|                NULL|
|  2020-02-04|      2|      petrol| 150.46|                NULL|
|  2020-02-04|      2| electricity| 300.47|                NULL|
|  2020-12-01|       |      tennis|   0.04| blank (many spac...|
|  2020-12-01|      x|      petrol|      x| invalid number f...|
|  2020-13-01|      x|      burger|   0.01|        invalid date|
|invalid date|      x|      petrol|   0.02|        invalid date|
|           x|      x|           x|      x| row with multipl...|
+------------+-------+------------+-------+--------------------+

row count = 18
23:19:34.086 [main] INFO  c.e.d.spark.types.DecimalType MDC= - Using decimal(10,2) for column amount

Typed dataset

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
|      NULL|      x|           x|  NULL|
|2020-01-01|      1|      burger| 15.45|
|2020-01-02|      1|       movie| 20.00|
|2020-01-03|      1|      tennis| 35.00|
|2020-01-04|      2|      petrol|150.45|
|2020-02-01|      1|      burger| 15.46|
|2020-02-02|      1|       movie| 20.01|
|2020-02-03|      1|      tennis| 35.01|
|2020-02-04|      2|      petrol|150.46|
|2020-02-04|      2| electricity|300.47|
|2020-03-01|      1|      burger| 15.47|
|2020-03-03|      1|      tennis| 35.03|
|2020-03-04|      2|      petrol|150.47|
|2020-12-01|       |      tennis|  0.04|
|2020-12-01|      x|      petrol|  NULL|
+----------+-------+------------+------+

row count = 18
23:19:34.238 [main] INFO  c.e.d.spark.types.DecimalType MDC= - Using decimal(10,2) for column amount

Valid dataset

root
 |-- date: date (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- amount: decimal(10,2) (nullable = true)

+----------+-------+------------+------+
|      date|account| description|amount|
+----------+-------+------------+------+
|2020-01-01|      1|      burger| 15.45|
|2020-01-02|      1|       movie| 20.00|
|2020-01-03|      1|      tennis| 35.00|
|2020-01-04|      2|      petrol|150.45|
|2020-02-01|      1|      burger| 15.46|
|2020-02-02|      1|       movie| 20.01|
|2020-02-03|      1|      tennis| 35.01|
|2020-02-04|      2|      petrol|150.46|
|2020-02-04|      2| electricity|300.47|
|2020-03-01|      1|      burger| 15.47|
|2020-03-03|      1|      tennis| 35.03|
|2020-03-04|      2|      petrol|150.47|
+----------+-------+------------+------+

row count = 12
Finished...

BUILD SUCCESSFUL in 4s
10 actionable tasks: 5 executed, 5 up-to-date
```

---

**Note**

One thing I like doing on my pet projects is creating a bash script for common things I need to do. Not only is it convenient - less to type - it also helps me remember what functionality is available and important. 

For example, at some point I’ll forget what the command is to generate the Dokka HTML documentation - fear not, I can just use `./run doc` and it’ll generate it and then open it in the browser...

See [run](https://github.com/prule/data-processing-experiment/blob/part-4/run) for an example