Data Processing Experiment - Part 9
---
- The one where I try applying the framework to an external use-case.

---

> The code for this project is available in GitHub - I’m using a branch for each part and merging each part into the **[latest](https://github.com/prule/data-processing-experiment/tree/latest)** branch. See the ReadMe.md in each branch for the story.
>
> - [Github repository for this project](https://github.com/prule/data-processing-experiment/)
> - [Pull requests for each part](https://github.com/prule/data-processing-experiment/pulls?q=is%3Apr+is%3Aclosed) 
> - [Branch for part-9](https://github.com/prule/data-processing-experiment/tree/part-9)

---

Now the basics are in place, I want to try exercising it against an external requirement and see how it holds up.

Kaggle have some inspiration in [data cleaning course](https://www.kaggle.com/learn/data-cleaning) and [Ultimate Cheatsheets: Data Cleaning](https://www.kaggle.com/code/vivovinco/ultimate-cheatsheets-data-cleaning). Here they cover things such as:
- Handling missing values
  - count the nulls per column
  - calculate the percentage of values that are null
  - remove columns that are missing values
  - fill in missing values with appropriate defaults (zero for numbers)
- Scaling and Normalization
  - scale to fit within a specific range
- Parsing Dates
- Character Encodings
- Inconsistent Data Entry
  - fixing case
  - trimming strings
  - fuzzy matching

Tableau also have a good explanation of data cleaning in their article [Guide To Data Cleaning: Definition, Benefits, Components, And How To Clean Your Data](https://www.tableau.com/learn/articles/what-is-data-cleaning)

So with this in mind, I've done the following:

*Refactoring*

- Some refactoring to remove the pipeline configuration classes and just directly instantiate the pipeline processors when loading the configuration (taking advantage of polymorphic serialization and removing a lot of unnecessary code!). This simplifies the codebase and keeps it clean.

*Parsing Dates*
- This was already handled from the beginning since table definitions allowed multiple date formats to be specified:
```json5
      ...
      table: {
        name: "transactions",
        description: "account transactions",
        deduplicate: true,
        trim: true,
        columns: [
          {
            names: ["date"],
            alias: "date",
            description: "date of transaction",
            type: "date",
            formats: [
              "yyyy-MM-dd",
              "dd-MM-yyyy"
            ],
            required: true
          },
          ...
```

*Empty count statistic*

- Added [EmptyCount](https://github.com/prule/data-processing-experiment/blob/part-9/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/statistics/EmptyCount.kt) statistic - This counts the empty values for each column (or just the columns you specify). "Empty" means different things depending on the data type of the column - For numbers it can be NULL or NaN. For strings it could be NULL, or a blank string, or whitespace.  
  - This also adds an "EmptyPercentage" statistic, calculated from the number of empty values as a percentage of the total number of values.
```text
|         EmptyCount|       date|         NULL|                1|
|         EmptyCount|    account|         NULL|                1|
|         EmptyCount|description|         NULL|                0|
|         EmptyCount|     amount|         NULL|                0|
|         EmptyCount|   location|         NULL|                7|
|         EmptyCount|    comment|         NULL|               10|
|    EmptyPercentage|       NULL|         NULL|               15|
```

*Summary statistic*

- Added [summary](https://github.com/prule/data-processing-experiment/blob/part-9/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/statistics/Summary.kt) statistic - this is based off the spark dataset.summary() feature which return statistics like mean, min, max, stddev, count, percentiles etc.

```text
|            Summary|     amount|        count|               20|
|            Summary|     amount|         mean|64.34294117647059|
|            Summary|     amount|       stddev|85.01729903119026|
|            Summary|     amount|          min|             0.01|
|            Summary|     amount|          max|                x|
|            Summary|     amount|          25%|            15.45|
|            Summary|     amount|          50%|            20.01|
|            Summary|     amount|          75%|           150.45|
```

*Trimming whitespace*

- Added the capability to specify that a [column should be trimmed](https://github.com/prule/data-processing-experiment/blob/c9dca17f23a489c90ee0ce626f8f224c791fc4ae/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/data/DataFrameBuilder.kt#L48) when loading the "selected" dataset - both at the column level and at the table level.
- Adding some spaces to the sample data shows how whitespace interferes 
  - the 2 rows for "burger" have different values so wouldn't considered the same
  - the join on location isn't working for some values now and level_1_name is null for some rows
```
|      date|account| description|            location|amount|       type|level_1_name|
|2020-01-01|      1|    burger  |            Gympie  | 15.45|TRANSACTION|        NULL|
|2020-01-02|      1|      movie |      Southern Downs| 20.00|TRANSACTION|        NULL|
|2020-01-03|      1|      tennis|            Banana  | 35.00|TRANSACTION|        NULL|
|2020-01-04|      2|      petrol|   Central Highlands|150.45|TRANSACTION|        NULL|
|2020-02-01|      1|      burger|            Yarrabah| 15.46|TRANSACTION|  Queensland|
```
- When adding `trim=true` to the description column we see whitespace removed from the description column:
```
|      date|account|description|            location|amount|       type|level_1_name|
|2020-01-01|      1|     burger|            Gympie  | 15.45|TRANSACTION|        NULL|
|2020-01-02|      1|      movie|      Southern Downs| 20.00|TRANSACTION|        NULL|
|2020-01-03|      1|     tennis|            Banana  | 35.00|TRANSACTION|        NULL|
|2020-01-04|      2|     petrol|   Central Highlands|150.45|TRANSACTION|  Queensland|
|2020-02-01|      1|     burger|            Yarrabah| 15.46|TRANSACTION|  Queensland|
```
- When adding `trim=true` to the whole table we see whitespace removed from both the description and location columns:
```
|      date|account|description|         location|amount|       type|level_1_name|
|2020-01-01|      1|     burger|           Gympie| 15.45|TRANSACTION|  Queensland|
|2020-01-02|      1|      movie|   Southern Downs| 20.00|TRANSACTION|  Queensland|
|2020-01-03|      1|     tennis|           Banana| 35.00|TRANSACTION|  Queensland|
|2020-01-04|      2|     petrol|Central Highlands|150.45|TRANSACTION|  Queensland|
|2020-02-01|      1|     burger|         Yarrabah| 15.46|TRANSACTION|  Queensland|
```
- Removing the whitespaces by trimming description and location now fixes the issues and the join to level_1_name is working again making the data cleaner and more consistent.

*Fixing typos*

- We want to standardise values - so that "burgers" would become "burger". If the values are consistently wrong we can create a mapping to map the incorrect value to the correct value and have this logic run in the pipeline. We could use statistics to alert us to new values that we might need to check. 
  - For this exercise I'll implement this as the simplest thing that would work by using a mapping of values to replace, but note there's a very interesting article [here](https://medium.com/analytics-vidhya/fuzzy-string-matching-with-spark-in-python-7fcd0c422f71) about fuzzy matching with spark in python.
- For [ValueMappingJoinProcessor](https://github.com/prule/data-processing-experiment/blob/part-9/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/pipeline/ValueMappingJoinProcessor.kt) implementation:
  - Using this configuration:
```json
  {
      type: "com.example.dataprocessingexperiment.spark.pipeline.ValueMappingJoinProcessor",
      id: "t0.1",
      name: "Value mappings",
      description: "Update values for columns based on mappings",
      // ids of mapping tables to process
      tables: [
        "mappings"
      ]
  }
```
- Given a mapping table:
```
 +------------+-----------+-------+------+
 |       table|     column|   from|    to|
 +------------+-----------+-------+------+
 |transactions|description|burgers|burger|
 +------------+-----------+-------+------+
```
- And a "transactions" table:
```
 +----+-----------+----+
 |val1|description|val2|
 +----+-----------+----+
 |   a|    burgers|   1|
 |   b|     burger|   2|
 |   c|      apple|   3|
 |   d|       NULL|   4|
 +----+-----------+----+
```
- Applying the mapping produces:
```
 +----+-----------+----+
 |val1|description|val2|
 +----+-----------+----+
 |   a|     burger|   1|
 |   b|     burger|   2|
 |   c|      apple|   3|
 |   d|       NULL|   4|
 +----+-----------+----+
```
- This implementation may be a bit naive - it could be a lot of joining if the data is large and perhaps it wouldn't perform. 
  - Pros
    - External configuration of the mappings allows it to be data driven.
  - Cons
    - Implementation using joins may not perform well - it seems a bit heavy.
- So lets try another implementation where instead of joining we use the `when col=X then Y otherwise Z` approach.
- For [ValueMappingWhenProcessor](https://github.com/prule/data-processing-experiment/blob/part-9/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/pipeline/ValueMappingWhenProcessor.kt) implementation:
  - Using a configuration like the following we get the same outcome:
```json
{
  type: "com.example.dataprocessingexperiment.spark.pipeline.ValueMappingWhenProcessor",
  id: "t0.2",
  name: "Value mappings",
  description: "Update values for columns based on mappings",
  // ids of mapping tables to process
  mappings: [
    {
      tableId: "lga-1",
      deduplicate: true,
      columns: [
        {
          columnName: "level_1_name",
          mapping: {
            // this is the value we want to see in the data
            value: "New South Wales",
            // these are the values we want to replace
            alternatives: [
              "NSW",
              "N.S.W."
            ]
          }
        },
        {
          columnName: "level_1_code",
          mapping: {
            // this is the value we want to see in the data
            value: "1",
            // these are the values we want to replace
            alternatives: [
              "01",
              "001"
            ]
          }
        }
      ]
    }
  ]
}
```
- Note that this configuration could still be externalised rather than being part of the pipeline configuration. 
- One difference with this implementation is by modelling the mapping classes the code becomes much cleaner.
  - See the mapping classes (TableMapping, ColumnMapping, ValueMapping) in [ValueMappingWhenProcessor](https://github.com/prule/data-processing-experiment/blob/e91dbe09d17931194561212e86841254f0882411/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/pipeline/ValueMappingWhenProcessor.kt#L87)
- Hopefully the performance of this method is better, and it would be good to measure it at some point so I can detect potential problem implementations.

The configuration for the reference application be seen here:
- [tables](https://github.com/prule/data-processing-experiment/blob/part-9/app/src/main/resources/sample1.tables.json5)
- [statistics](https://github.com/prule/data-processing-experiment/blob/part-9/app/src/main/resources/sample1.statistics.json5)
- [pipeline](https://github.com/prule/data-processing-experiment/blob/part-9/app/src/main/resources/sample1.pipeline.json5)

Building this via components is working well, since it's easy to provide competing implementations and switch them out.
That's all I've got time for this week!

Here's the output from the [reference application](https://github.com/prule/data-processing-experiment/blob/part-9/app/src/main/kotlin/com/example/dataprocessingexperiment/app/App.kt) at this stage:
- source data has been changed to exercise new functionality
- sorting has also been introduced to make the output more deterministic and therefore easier to compare across parts

```text
> Task :app:run
Starting...
15:20:04.342 [main] WARN  o.a.hadoop.util.NativeCodeLoader MDC= - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
SLF4J: Class path contains multiple SLF4J providers.
SLF4J: Found provider [ch.qos.logback.classic.spi.LogbackServiceProvider@6236eb5f]
SLF4J: Found provider [org.apache.logging.slf4j.SLF4JServiceProvider@7c1e2a9e]
SLF4J: See https://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual provider is of type [ch.qos.logback.classic.spi.LogbackServiceProvider@6236eb5f]

Raw dataset

root
 |-- date: string (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- amount: string (nullable = true)
 |-- location: string (nullable = true)
 |-- comment: string (nullable = true)

+------------+-------+------------+-------+--------------------+--------------------+
|        date|account| description| amount|            location|             comment|
+------------+-------+------------+-------+--------------------+--------------------+
|        NULL|      x|      tennis|   0.03|                NULL|             no date|
|  2020-01-03|      1|      tennis|  35.00|            Banana  |                NULL|
| 2020-01-04 |      2|      petrol| 150.45|   Central Highlands|                NULL|
|  01-03-2020|      1|      burger|  15.47|   Greater Dandenong|alternative date ...|
|  03-03-2020|      1|      tennis|  35.03|           Maroondah|alternative date ...|
|  04-03-2020|      2|      petrol| 150.47|                NULL|alternative date ...|
|  05-03-2020|      2|      petrol|  50.47|             Burwood|                NULL|
|  05-03-2020|      2|      petrol|  50.48|            Berrigan|                NULL|
| 2020-01-01 |      1|   burgers  |  15.45|            Gympie  |                NULL|
|  2020-01-02|      1|      movie |  20.00|      Southern Downs|                NULL|
|  2020-01-04|      2|      petrol| 150.45|   Central Highlands|                NULL|
|  2020-02-01|      1|      burger|  15.46|            Yarrabah|                NULL|
|  2020-02-02|      1|      movies|  20.01|          Barcaldine|                NULL|
|  2020-02-03|      1|      tennis|  35.01|           Maroondah|                NULL|
|  2020-02-04|      2|      petrol| 150.46|          Gannawarra|                NULL|
|  2020-02-04|      2| electricity| 300.47|             Hepburn|                NULL|
|  2020-12-01|       |      tennis|   0.04|                NULL| blank (many spac...|
|  2020-12-01|      x|      petrol|      x|                NULL| invalid number f...|
|  2020-13-01|      x|      burger|   0.01|    unknown location|        invalid date|
|invalid date|      x|      petrol|   0.02|                    |        invalid date|
|           x|      x|           x|      x|                NULL| row with multipl...|
|           x|      x|           x|      x|                NULL| row with multipl...|
+------------+-------+------------+-------+--------------------+--------------------+

row count = 22
+-------+-----------------+-----------+
|summary|           amount|description|
+-------+-----------------+-----------+
|  count|               22|         22|
|   mean|62.88315789473684|       NULL|
| stddev|80.27425538964889|       NULL|
|    min|             0.01|     petrol|
|    max|                x|          x|
|    25%|            15.45|       NULL|
|    50%|             35.0|       NULL|
|    75%|           150.45|       NULL|
+-------+-----------------+-----------+


RAW Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+-----------+-------------+-----------------+
|                key|     column|discriminator|            value|
+-------------------+-----------+-------------+-----------------+
|       CountByMonth|       date|         NULL|               10|
|       CountByMonth|       date|      2020-01|                5|
|       CountByMonth|       date|      2020-02|                5|
|       CountByMonth|       date|      2020-12|                2|
|       CountByValue|    account|         NULL|                1|
|       CountByValue|    account|            1|                8|
|       CountByValue|    account|            2|                7|
|       CountByValue|    account|            x|                6|
|         EmptyCount|       date|         NULL|                1|
|         EmptyCount|    account|         NULL|                1|
|         EmptyCount|description|         NULL|                0|
|         EmptyCount|     amount|         NULL|                0|
|         EmptyCount|   location|         NULL|                7|
|         EmptyCount|    comment|         NULL|               12|
|    EmptyPercentage|       NULL|         NULL|               15|
|            Summary|     amount|        count|               22|
|            Summary|     amount|         mean|62.88315789473684|
|            Summary|     amount|       stddev|80.27425538964889|
|            Summary|     amount|          min|             0.01|
|            Summary|     amount|          max|                x|
|            Summary|     amount|          25%|            15.45|
|            Summary|     amount|          50%|             35.0|
|            Summary|     amount|          75%|           150.45|
|            Summary|description|        count|               22|
|            Summary|description|         mean|             NULL|
|            Summary|description|       stddev|             NULL|
|            Summary|description|          min|           petrol|
|            Summary|description|          max|                x|
|            Summary|description|          25%|             NULL|
|            Summary|description|          50%|             NULL|
|            Summary|description|          75%|             NULL|
|       column count|       NULL|         NULL|                6|
|duplicate row count|       NULL|         NULL|                2|
|                max|     amount|         NULL|                x|
|                min|     amount|         NULL|             0.01|
|          row count|       NULL|         NULL|               22|
+-------------------+-----------+-------------+-----------------+

row count = 36

SELECTED dataset

root
 |-- date: string (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- location: string (nullable = true)
 |-- amount: string (nullable = true)

+------------+-------+-----------+-----------------+------+
|        date|account|description|         location|amount|
+------------+-------+-----------+-----------------+------+
|        NULL|      x|     tennis|             NULL|  0.03|
|  01-03-2020|      1|     burger|Greater Dandenong| 15.47|
|  03-03-2020|      1|     tennis|        Maroondah| 35.03|
|  04-03-2020|      2|     petrol|             NULL|150.47|
|  05-03-2020|      2|     petrol|          Burwood| 50.47|
|  05-03-2020|      2|     petrol|         Berrigan| 50.48|
|  2020-01-01|      1|    burgers|           Gympie| 15.45|
|  2020-01-02|      1|      movie|   Southern Downs| 20.00|
|  2020-01-03|      1|     tennis|           Banana| 35.00|
|  2020-01-04|      2|     petrol|Central Highlands|150.45|
|  2020-01-04|      2|     petrol|Central Highlands|150.45|
|  2020-02-01|      1|     burger|         Yarrabah| 15.46|
|  2020-02-02|      1|     movies|       Barcaldine| 20.01|
|  2020-02-03|      1|     tennis|        Maroondah| 35.01|
|  2020-02-04|      2|     petrol|       Gannawarra|150.46|
|  2020-02-04|      2|electricity|          Hepburn|300.47|
|  2020-12-01|       |     tennis|             NULL|  0.04|
|  2020-12-01|      x|     petrol|             NULL|     x|
|  2020-13-01|      x|     burger| unknown location|  0.01|
|invalid date|      x|     petrol|                 |  0.02|
|           x|      x|          x|             NULL|     x|
|           x|      x|          x|             NULL|     x|
+------------+-------+-----------+-----------------+------+

row count = 22

Typed dataset

root
 |-- date: date (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- location: string (nullable = true)
 |-- amount: decimal(10,2) (nullable = true)

+----------+-------+-----------+-----------------+------+
|      date|account|description|         location|amount|
+----------+-------+-----------+-----------------+------+
|      NULL|      x|     burger| unknown location|  0.01|
|      NULL|      x|     petrol|                 |  0.02|
|      NULL|      x|     tennis|             NULL|  0.03|
|      NULL|      x|          x|             NULL|  NULL|
|      NULL|      x|          x|             NULL|  NULL|
|2020-01-01|      1|    burgers|           Gympie| 15.45|
|2020-01-02|      1|      movie|   Southern Downs| 20.00|
|2020-01-03|      1|     tennis|           Banana| 35.00|
|2020-01-04|      2|     petrol|Central Highlands|150.45|
|2020-01-04|      2|     petrol|Central Highlands|150.45|
|2020-02-01|      1|     burger|         Yarrabah| 15.46|
|2020-02-02|      1|     movies|       Barcaldine| 20.01|
|2020-02-03|      1|     tennis|        Maroondah| 35.01|
|2020-02-04|      2|     petrol|       Gannawarra|150.46|
|2020-02-04|      2|electricity|          Hepburn|300.47|
|2020-03-01|      1|     burger|Greater Dandenong| 15.47|
|2020-03-03|      1|     tennis|        Maroondah| 35.03|
|2020-03-04|      2|     petrol|             NULL|150.47|
|2020-03-05|      2|     petrol|          Burwood| 50.47|
|2020-03-05|      2|     petrol|         Berrigan| 50.48|
|2020-12-01|       |     tennis|             NULL|  0.04|
|2020-12-01|      x|     petrol|             NULL|  NULL|
+----------+-------+-----------+-----------------+------+

row count = 22

Valid dataset

root
 |-- date: date (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- location: string (nullable = true)
 |-- amount: decimal(10,2) (nullable = true)

+----------+-------+-----------+-----------------+------+
|      date|account|description|         location|amount|
+----------+-------+-----------+-----------------+------+
|2020-01-01|      1|    burgers|           Gympie| 15.45|
|2020-01-02|      1|      movie|   Southern Downs| 20.00|
|2020-01-03|      1|     tennis|           Banana| 35.00|
|2020-01-04|      2|     petrol|Central Highlands|150.45|
|2020-02-01|      1|     burger|         Yarrabah| 15.46|
|2020-02-02|      1|     movies|       Barcaldine| 20.01|
|2020-02-03|      1|     tennis|        Maroondah| 35.01|
|2020-02-04|      2|electricity|          Hepburn|300.47|
|2020-02-04|      2|     petrol|       Gannawarra|150.46|
|2020-03-01|      1|     burger|Greater Dandenong| 15.47|
|2020-03-03|      1|     tennis|        Maroondah| 35.03|
|2020-03-04|      2|     petrol|             NULL|150.47|
|2020-03-05|      2|     petrol|         Berrigan| 50.48|
|2020-03-05|      2|     petrol|          Burwood| 50.47|
+----------+-------+-----------+-----------------+------+

row count = 14
+-------+-----------------+-----------+
|summary|           amount|description|
+-------+-----------------+-----------+
|  count|               14|         14|
|   mean|        74.587857|       NULL|
| stddev|83.48222530101118|       NULL|
|    min|            15.45|     burger|
|    max|           300.47|     tennis|
|    25%|             20.0|       NULL|
|    50%|            35.01|       NULL|
|    75%|           150.45|       NULL|
+-------+-----------------+-----------+


VALID Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+-----------+-------------+-----------------+
|                key|     column|discriminator|            value|
+-------------------+-----------+-------------+-----------------+
|       CountByMonth|       date|      2020-01|                4|
|       CountByMonth|       date|      2020-02|                5|
|       CountByMonth|       date|      2020-03|                5|
|       CountByValue|    account|            1|                8|
|       CountByValue|    account|            2|                6|
|         EmptyCount|       date|         NULL|                0|
|         EmptyCount|    account|         NULL|                0|
|         EmptyCount|description|         NULL|                0|
|         EmptyCount|   location|         NULL|                1|
|         EmptyCount|     amount|         NULL|                0|
|    EmptyPercentage|       NULL|         NULL|                1|
|            Summary|     amount|        count|               14|
|            Summary|     amount|         mean|        74.587857|
|            Summary|     amount|       stddev|83.48222530101118|
|            Summary|     amount|          min|            15.45|
|            Summary|     amount|          max|           300.47|
|            Summary|     amount|          25%|             20.0|
|            Summary|     amount|          50%|            35.01|
|            Summary|     amount|          75%|           150.45|
|            Summary|description|        count|               14|
|            Summary|description|         mean|             NULL|
|            Summary|description|       stddev|             NULL|
|            Summary|description|          min|           burger|
|            Summary|description|          max|           tennis|
|            Summary|description|          25%|             NULL|
|            Summary|description|          50%|             NULL|
|            Summary|description|          75%|             NULL|
|       column count|       NULL|         NULL|                5|
|duplicate row count|       NULL|         NULL|                0|
|                max|     amount|         NULL|           300.47|
|                min|     amount|         NULL|            15.45|
|          row count|       NULL|         NULL|               14|
+-------------------+-----------+-------------+-----------------+

row count = 32

Raw dataset

root
 |-- Code State: string (nullable = true)
 |-- Name State: string (nullable = true)
 |-- Year: string (nullable = true)
 |-- Code Local Government Area: string (nullable = true)
 |-- Iso 3166-3 Area Code: string (nullable = true)
 |-- Name Local Government Area: string (nullable = true)
 |-- Type: string (nullable = true)
 |-- Long Name Local Government Area: string (nullable = true)

+----------+---------------+----+--------------------------+--------------------+--------------------------+--------------------+-------------------------------+
|Code State|     Name State|Year|Code Local Government Area|Iso 3166-3 Area Code|Name Local Government Area|                Type|Long Name Local Government Area|
+----------+---------------+----+--------------------------+--------------------+--------------------------+--------------------+-------------------------------+
|         1|            NSW|2021|                     10650|                 AUS|                  Berrigan|local government ...|                       Berrigan|
|        01|            NSW|2021|                     10650|                 AUS|                  Berrigan|local government ...|                       Berrigan|
|       001|         N.S.W.|2021|                     10850|                  AU|                   Blayney|local government ...|                        Blayney|
|       001|New South Wales|2021|                     11500|                 AUS|              Campbelltown|local government ...|             Campbelltown (NSW)|
|         1|New South Wales|2021|                     11700|                 AUS|           Central Darling|local government ...|                Central Darling|
|         1|New South Wales|2021|                     15990|                 AUS|          Northern Beaches|local government ...|               Northern Beaches|
|         1|            NSW|2021|                     11300|                 AUS|                   Burwood|local government ...|                        Burwood|
|         1|New South Wales|2021|                     11750|                 AUS|                     Cobar|local government ...|                          Cobar|
|         1|New South Wales|2021|                     14000|                 AUS|                   Hornsby|local government ...|                        Hornsby|
|         1|New South Wales|2021|                     14750|                 AUS|                    Leeton|local government ...|                         Leeton|
|         1|New South Wales|2021|                     17200|                 AUS|                    Sydney|local government ...|                         Sydney|
+----------+---------------+----+--------------------------+--------------------+--------------------------+--------------------+-------------------------------+

row count = 11

RAW Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|       column count|  NULL|         NULL|    8|
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   11|
+-------------------+------+-------------+-----+

row count = 3

SELECTED dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+---------------+------------+----------------+
|level_1_code|   level_1_name|level_2_code|    level_2_name|
+------------+---------------+------------+----------------+
|           1|            NSW|       10650|        Berrigan|
|          01|            NSW|       10650|        Berrigan|
|         001|         N.S.W.|       10850|         Blayney|
|         001|New South Wales|       11500|    Campbelltown|
|           1|New South Wales|       11700| Central Darling|
|           1|New South Wales|       15990|Northern Beaches|
|           1|            NSW|       11300|         Burwood|
|           1|New South Wales|       11750|           Cobar|
|           1|New South Wales|       14000|         Hornsby|
|           1|New South Wales|       14750|          Leeton|
|           1|New South Wales|       17200|          Sydney|
+------------+---------------+------------+----------------+

row count = 11

Typed dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+---------------+------------+----------------+
|level_1_code|   level_1_name|level_2_code|    level_2_name|
+------------+---------------+------------+----------------+
|           1|            NSW|       10650|        Berrigan|
|          01|            NSW|       10650|        Berrigan|
|         001|         N.S.W.|       10850|         Blayney|
|         001|New South Wales|       11500|    Campbelltown|
|           1|New South Wales|       11700| Central Darling|
|           1|New South Wales|       15990|Northern Beaches|
|           1|            NSW|       11300|         Burwood|
|           1|New South Wales|       11750|           Cobar|
|           1|New South Wales|       14000|         Hornsby|
|           1|New South Wales|       14750|          Leeton|
|           1|New South Wales|       17200|          Sydney|
+------------+---------------+------------+----------------+

row count = 11

Valid dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+---------------+------------+----------------+
|level_1_code|   level_1_name|level_2_code|    level_2_name|
+------------+---------------+------------+----------------+
|         001|         N.S.W.|       10850|         Blayney|
|           1|New South Wales|       11700| Central Darling|
|         001|New South Wales|       11500|    Campbelltown|
|          01|            NSW|       10650|        Berrigan|
|           1|New South Wales|       17200|          Sydney|
|           1|            NSW|       11300|         Burwood|
|           1|New South Wales|       11750|           Cobar|
|           1|New South Wales|       15990|Northern Beaches|
|           1|            NSW|       10650|        Berrigan|
|           1|New South Wales|       14750|          Leeton|
|           1|New South Wales|       14000|         Hornsby|
+------------+---------------+------------+----------------+

row count = 11

VALID Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|       column count|  NULL|         NULL|    4|
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   11|
+-------------------+------+-------------+-----+

row count = 3

Raw dataset

root
 |-- Official Name State: string (nullable = true)
 |-- Year: string (nullable = true)
 |-- Official Code Local Government Area: string (nullable = true)
 |-- Official Code State: string (nullable = true)
 |-- Iso 3166-3 Area Code: string (nullable = true)
 |-- Official Name Local Government Area: string (nullable = true)

+-------------------+----+-----------------------------------+-------------------+--------------------+-----------------------------------+
|Official Name State|Year|Official Code Local Government Area|Official Code State|Iso 3166-3 Area Code|Official Name Local Government Area|
+-------------------+----+-----------------------------------+-------------------+--------------------+-----------------------------------+
|           Victoria|2021|                              21750|                  2|                 AUS|                        Colac Otway|
|           Victoria|2021|                              22670|                  2|                 AUS|                  Greater Dandenong|
|           Victoria|2021|                              22910|                  2|                 AUS|                            Hepburn|
|           Victoria|2021|                              24410|                  2|                 AUS|                          Maroondah|
|           Victoria|2021|                              24850|                  2|                 AUS|                           Mitchell|
|           Victoria|2021|                              23350|                  2|                 AUS|                             Indigo|
|           Victoria|2021|                              26610|                  2|                 AUS|                          Swan Hill|
|           Victoria|2021|                              22250|                  2|                 AUS|                         Gannawarra|
|           Victoria|2021|                              20830|                  2|                 AUS|                            Baw Baw|
|           Victoria|2021|                              21110|                  2|                 AUS|                         Boroondara|
+-------------------+----+-----------------------------------+-------------------+--------------------+-----------------------------------+

row count = 10

RAW Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|       column count|  NULL|         NULL|    6|
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 3

SELECTED dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           2|    Victoria|       21750|      Colac Otway|
|           2|    Victoria|       22670|Greater Dandenong|
|           2|    Victoria|       22910|          Hepburn|
|           2|    Victoria|       24410|        Maroondah|
|           2|    Victoria|       24850|         Mitchell|
|           2|    Victoria|       23350|           Indigo|
|           2|    Victoria|       26610|        Swan Hill|
|           2|    Victoria|       22250|       Gannawarra|
|           2|    Victoria|       20830|          Baw Baw|
|           2|    Victoria|       21110|       Boroondara|
+------------+------------+------------+-----------------+

row count = 10

Typed dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           2|    Victoria|       21750|      Colac Otway|
|           2|    Victoria|       22670|Greater Dandenong|
|           2|    Victoria|       22910|          Hepburn|
|           2|    Victoria|       24410|        Maroondah|
|           2|    Victoria|       24850|         Mitchell|
|           2|    Victoria|       23350|           Indigo|
|           2|    Victoria|       26610|        Swan Hill|
|           2|    Victoria|       22250|       Gannawarra|
|           2|    Victoria|       20830|          Baw Baw|
|           2|    Victoria|       21110|       Boroondara|
+------------+------------+------------+-----------------+

row count = 10

Valid dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           2|    Victoria|       24410|        Maroondah|
|           2|    Victoria|       22910|          Hepburn|
|           2|    Victoria|       21110|       Boroondara|
|           2|    Victoria|       22250|       Gannawarra|
|           2|    Victoria|       21750|      Colac Otway|
|           2|    Victoria|       26610|        Swan Hill|
|           2|    Victoria|       24850|         Mitchell|
|           2|    Victoria|       22670|Greater Dandenong|
|           2|    Victoria|       23350|           Indigo|
|           2|    Victoria|       20830|          Baw Baw|
+------------+------------+------------+-----------------+

row count = 10

VALID Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|       column count|  NULL|         NULL|    4|
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 3

Raw dataset

root
 |-- Official Name State: string (nullable = true)
 |-- Official Code Local Government Area: string (nullable = true)
 |-- Official Code State: string (nullable = true)
 |-- Iso 3166-3 Area Code: string (nullable = true)
 |-- Official Name Local Government Area: string (nullable = true)

+-------------------+-----------------------------------+-------------------+--------------------+-----------------------------------+
|Official Name State|Official Code Local Government Area|Official Code State|Iso 3166-3 Area Code|Official Name Local Government Area|
+-------------------+-----------------------------------+-------------------+--------------------+-----------------------------------+
|         Queensland|                              30410|                  3|                 AUS|                         Barcaldine|
|         Queensland|                              33620|                  3|                 AUS|                             Gympie|
|         Queensland|                              35670|                  3|                 AUS|                           Napranum|
|         Queensland|                              36660|                  3|                 AUS|                     Southern Downs|
|         Queensland|                              30370|                  3|                 AUS|                             Banana|
|         Queensland|                              33100|                  3|                 AUS|                          Etheridge|
|         Queensland|                              34830|                  3|                 AUS|                             Mapoon|
|         Queensland|                              35300|                  3|                 AUS|                          Mount Isa|
|         Queensland|                              37600|                  3|                 AUS|                           Yarrabah|
|         Queensland|                              32270|                  3|                 AUS|                  Central Highlands|
+-------------------+-----------------------------------+-------------------+--------------------+-----------------------------------+

row count = 10

RAW Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|       column count|  NULL|         NULL|    5|
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 3

SELECTED dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           3|  Queensland|       30410|       Barcaldine|
|           3|  Queensland|       33620|           Gympie|
|           3|  Queensland|       35670|         Napranum|
|           3|  Queensland|       36660|   Southern Downs|
|           3|  Queensland|       30370|           Banana|
|           3|  Queensland|       33100|        Etheridge|
|           3|  Queensland|       34830|           Mapoon|
|           3|  Queensland|       35300|        Mount Isa|
|           3|  Queensland|       37600|         Yarrabah|
|           3|  Queensland|       32270|Central Highlands|
+------------+------------+------------+-----------------+

row count = 10

Typed dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           3|  Queensland|       30410|       Barcaldine|
|           3|  Queensland|       33620|           Gympie|
|           3|  Queensland|       35670|         Napranum|
|           3|  Queensland|       36660|   Southern Downs|
|           3|  Queensland|       30370|           Banana|
|           3|  Queensland|       33100|        Etheridge|
|           3|  Queensland|       34830|           Mapoon|
|           3|  Queensland|       35300|        Mount Isa|
|           3|  Queensland|       37600|         Yarrabah|
|           3|  Queensland|       32270|Central Highlands|
+------------+------------+------------+-----------------+

row count = 10

Valid dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           3|  Queensland|       30370|           Banana|
|           3|  Queensland|       35670|         Napranum|
|           3|  Queensland|       32270|Central Highlands|
|           3|  Queensland|       37600|         Yarrabah|
|           3|  Queensland|       30410|       Barcaldine|
|           3|  Queensland|       34830|           Mapoon|
|           3|  Queensland|       33100|        Etheridge|
|           3|  Queensland|       36660|   Southern Downs|
|           3|  Queensland|       33620|           Gympie|
|           3|  Queensland|       35300|        Mount Isa|
+------------+------------+------------+-----------------+

row count = 10

VALID Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|       column count|  NULL|         NULL|    4|
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 3

Raw dataset

root
 |-- table: string (nullable = true)
 |-- column: string (nullable = true)
 |-- from: string (nullable = true)
 |-- to: string (nullable = true)

+------------+-----------+-------+------+
|       table|     column|   from|    to|
+------------+-----------+-------+------+
|transactions|description|burgers|burger|
+------------+-----------+-------+------+

row count = 1

SELECTED dataset

root
 |-- table: string (nullable = true)
 |-- column: string (nullable = true)
 |-- from: string (nullable = true)
 |-- to: string (nullable = true)

+------------+-----------+-------+------+
|       table|     column|   from|    to|
+------------+-----------+-------+------+
|transactions|description|burgers|burger|
+------------+-----------+-------+------+

row count = 1

Typed dataset

root
 |-- table: string (nullable = true)
 |-- column: string (nullable = true)
 |-- from: string (nullable = true)
 |-- to: string (nullable = true)

+------------+-----------+-------+------+
|       table|     column|   from|    to|
+------------+-----------+-------+------+
|transactions|description|burgers|burger|
+------------+-----------+-------+------+

row count = 1

Valid dataset

root
 |-- table: string (nullable = true)
 |-- column: string (nullable = true)
 |-- from: string (nullable = true)
 |-- to: string (nullable = true)

+------------+-----------+-------+------+
|       table|     column|   from|    to|
+------------+-----------+-------+------+
|transactions|description|burgers|burger|
+------------+-----------+-------+------+

row count = 1
15:20:13.525 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting pipeline id=sample-1 name=Sample Pipeline 1
15:20:13.525 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting processor ValueMappingJoinProcessor(id='t0.1', name='Value mappings', description='Update values for columns based on mappings', tables=[mappings])
15:20:13.639 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Finished processor ValueMappingJoinProcessor(id='t0.1', name='Value mappings', description='Update values for columns based on mappings', tables=[mappings])
15:20:13.640 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting processor ValueMappingWhenProcessor(id='t0.2', name='Value mappings', description='Update values for columns based on mappings', mappings='[TableMapping(tableId=transactions, deduplicate=false, columns=[ColumnMapping(columnName=description, mapping=ValueMapping(value=movie, alternatives=[movies]))]), TableMapping(tableId=lga-1, deduplicate=true, columns=[ColumnMapping(columnName=level_1_name, mapping=ValueMapping(value=New South Wales, alternatives=[NSW, N.S.W.])), ColumnMapping(columnName=level_1_code, mapping=ValueMapping(value=1, alternatives=[01, 001]))])]')
15:20:13.643 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Finished processor ValueMappingWhenProcessor(id='t0.2', name='Value mappings', description='Update values for columns based on mappings', mappings='[TableMapping(tableId=transactions, deduplicate=false, columns=[ColumnMapping(columnName=description, mapping=ValueMapping(value=movie, alternatives=[movies]))]), TableMapping(tableId=lga-1, deduplicate=true, columns=[ColumnMapping(columnName=level_1_name, mapping=ValueMapping(value=New South Wales, alternatives=[NSW, N.S.W.])), ColumnMapping(columnName=level_1_code, mapping=ValueMapping(value=1, alternatives=[01, 001]))])]')
15:20:13.644 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting processor UnionProcessor(id='t1', name='Union LGAs', description='Unions LGA tables together so we have one dataframe with all LGAs', destination='lgas', tables=[lga-1, lga-2, lga-3])
15:20:13.651 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Finished processor UnionProcessor(id='t1', name='Union LGAs', description='Unions LGA tables together so we have one dataframe with all LGAs', destination='lgas', tables=[lga-1, lga-2, lga-3])
15:20:13.651 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting processor LiteralProcessor(id='t2', name='Transaction type', description='Adds a literal column to the transactions table', table='transactions', columns={type=TRANSACTION})
15:20:13.652 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Finished processor LiteralProcessor(id='t2', name='Transaction type', description='Adds a literal column to the transactions table', table='transactions', columns={type=TRANSACTION})
15:20:13.654 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting processor JoinProcessor(id='t3', name='Denormalize transactions', description='Joins LGAs to transactions in order to add the level 1 LGA', table1='transactions', table2='lgas', destination='transactionsWithLGAs', joinType='left', on={location=level_2_name}, columns=[level_1_name])
15:20:13.658 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Finished processor JoinProcessor(id='t3', name='Denormalize transactions', description='Joins LGAs to transactions in order to add the level 1 LGA', table1='transactions', table2='lgas', destination='transactionsWithLGAs', joinType='left', on={location=level_2_name}, columns=[level_1_name])
15:20:13.659 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting processor OutputProcessor(id='t4', name='Output transactions', description='Writes out the denormalized transactions', table='transactionsWithLGAs', path='./build/output/sample1/transactions', format='csv', mode='overwrite', options={header=true})
15:20:14.059 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Finished processor OutputProcessor(id='t4', name='Output transactions', description='Writes out the denormalized transactions', table='transactionsWithLGAs', path='./build/output/sample1/transactions', format='csv', mode='overwrite', options={header=true})
==============================================
Context
transactions ordered by date
+----------+-------+-----------+-----------------+------+-----------+
|      date|account|description|         location|amount|       type|
+----------+-------+-----------+-----------------+------+-----------+
|2020-01-01|      1|     burger|           Gympie| 15.45|TRANSACTION|
|2020-01-02|      1|      movie|   Southern Downs| 20.00|TRANSACTION|
|2020-01-03|      1|     tennis|           Banana| 35.00|TRANSACTION|
|2020-01-04|      2|     petrol|Central Highlands|150.45|TRANSACTION|
|2020-02-01|      1|     burger|         Yarrabah| 15.46|TRANSACTION|
|2020-02-02|      1|      movie|       Barcaldine| 20.01|TRANSACTION|
|2020-02-03|      1|     tennis|        Maroondah| 35.01|TRANSACTION|
|2020-02-04|      2|electricity|          Hepburn|300.47|TRANSACTION|
|2020-02-04|      2|     petrol|       Gannawarra|150.46|TRANSACTION|
|2020-03-01|      1|     burger|Greater Dandenong| 15.47|TRANSACTION|
|2020-03-03|      1|     tennis|        Maroondah| 35.03|TRANSACTION|
|2020-03-04|      2|     petrol|             NULL|150.47|TRANSACTION|
|2020-03-05|      2|     petrol|         Berrigan| 50.48|TRANSACTION|
|2020-03-05|      2|     petrol|          Burwood| 50.47|TRANSACTION|
+----------+-------+-----------+-----------------+------+-----------+

lga-1 ordered by level_1_code
+------------+---------------+------------+----------------+
|level_1_code|   level_1_name|level_2_code|    level_2_name|
+------------+---------------+------------+----------------+
|           1|New South Wales|       11700| Central Darling|
|           1|New South Wales|       11500|    Campbelltown|
|           1|New South Wales|       17200|          Sydney|
|           1|New South Wales|       11750|           Cobar|
|           1|New South Wales|       15990|Northern Beaches|
|           1|New South Wales|       10650|        Berrigan|
|           1|New South Wales|       11300|         Burwood|
|           1|New South Wales|       14750|          Leeton|
|           1|New South Wales|       10850|         Blayney|
|           1|New South Wales|       14000|         Hornsby|
+------------+---------------+------------+----------------+

lga-2 ordered by level_1_code
+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           2|    Victoria|       24410|        Maroondah|
|           2|    Victoria|       22910|          Hepburn|
|           2|    Victoria|       21110|       Boroondara|
|           2|    Victoria|       22250|       Gannawarra|
|           2|    Victoria|       21750|      Colac Otway|
|           2|    Victoria|       26610|        Swan Hill|
|           2|    Victoria|       24850|         Mitchell|
|           2|    Victoria|       22670|Greater Dandenong|
|           2|    Victoria|       23350|           Indigo|
|           2|    Victoria|       20830|          Baw Baw|
+------------+------------+------------+-----------------+

lga-3 ordered by level_1_code
+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           3|  Queensland|       30370|           Banana|
|           3|  Queensland|       35670|         Napranum|
|           3|  Queensland|       32270|Central Highlands|
|           3|  Queensland|       37600|         Yarrabah|
|           3|  Queensland|       30410|       Barcaldine|
|           3|  Queensland|       34830|           Mapoon|
|           3|  Queensland|       33100|        Etheridge|
|           3|  Queensland|       36660|   Southern Downs|
|           3|  Queensland|       33620|           Gympie|
|           3|  Queensland|       35300|        Mount Isa|
+------------+------------+------------+-----------------+

mappings ordered by table
+------------+-----------+-------+------+
|       table|     column|   from|    to|
+------------+-----------+-------+------+
|transactions|description|burgers|burger|
+------------+-----------+-------+------+

lgas ordered by level_1_code
+------------+---------------+------------+-----------------+
|level_1_code|   level_1_name|level_2_code|     level_2_name|
+------------+---------------+------------+-----------------+
|           1|New South Wales|       11700|  Central Darling|
|           1|New South Wales|       11500|     Campbelltown|
|           1|New South Wales|       17200|           Sydney|
|           1|New South Wales|       11750|            Cobar|
|           1|New South Wales|       15990| Northern Beaches|
|           1|New South Wales|       10650|         Berrigan|
|           1|New South Wales|       11300|          Burwood|
|           1|New South Wales|       14750|           Leeton|
|           1|New South Wales|       10850|          Blayney|
|           1|New South Wales|       14000|          Hornsby|
|           2|       Victoria|       24410|        Maroondah|
|           2|       Victoria|       22910|          Hepburn|
|           2|       Victoria|       21110|       Boroondara|
|           2|       Victoria|       22250|       Gannawarra|
|           2|       Victoria|       21750|      Colac Otway|
|           2|       Victoria|       26610|        Swan Hill|
|           2|       Victoria|       24850|         Mitchell|
|           2|       Victoria|       22670|Greater Dandenong|
|           2|       Victoria|       23350|           Indigo|
|           2|       Victoria|       20830|          Baw Baw|
|           3|     Queensland|       30370|           Banana|
|           3|     Queensland|       35670|         Napranum|
|           3|     Queensland|       32270|Central Highlands|
|           3|     Queensland|       37600|         Yarrabah|
|           3|     Queensland|       30410|       Barcaldine|
|           3|     Queensland|       34830|           Mapoon|
|           3|     Queensland|       33100|        Etheridge|
|           3|     Queensland|       36660|   Southern Downs|
|           3|     Queensland|       33620|           Gympie|
|           3|     Queensland|       35300|        Mount Isa|
+------------+---------------+------------+-----------------+

transactionsWithLGAs ordered by date
+----------+-------+-----------+-----------------+------+-----------+---------------+
|      date|account|description|         location|amount|       type|   level_1_name|
+----------+-------+-----------+-----------------+------+-----------+---------------+
|2020-01-01|      1|     burger|           Gympie| 15.45|TRANSACTION|     Queensland|
|2020-01-02|      1|      movie|   Southern Downs| 20.00|TRANSACTION|     Queensland|
|2020-01-03|      1|     tennis|           Banana| 35.00|TRANSACTION|     Queensland|
|2020-01-04|      2|     petrol|Central Highlands|150.45|TRANSACTION|     Queensland|
|2020-02-01|      1|     burger|         Yarrabah| 15.46|TRANSACTION|     Queensland|
|2020-02-02|      1|      movie|       Barcaldine| 20.01|TRANSACTION|     Queensland|
|2020-02-03|      1|     tennis|        Maroondah| 35.01|TRANSACTION|       Victoria|
|2020-02-04|      2|electricity|          Hepburn|300.47|TRANSACTION|       Victoria|
|2020-02-04|      2|     petrol|       Gannawarra|150.46|TRANSACTION|       Victoria|
|2020-03-01|      1|     burger|Greater Dandenong| 15.47|TRANSACTION|       Victoria|
|2020-03-03|      1|     tennis|        Maroondah| 35.03|TRANSACTION|       Victoria|
|2020-03-04|      2|     petrol|             NULL|150.47|TRANSACTION|           NULL|
|2020-03-05|      2|     petrol|         Berrigan| 50.48|TRANSACTION|New South Wales|
|2020-03-05|      2|     petrol|          Burwood| 50.47|TRANSACTION|New South Wales|
+----------+-------+-----------+-----------------+------+-----------+---------------+

==============================================
Finished...

BUILD SUCCESSFUL in 11s
10 actionable tasks: 7 executed, 3 up-to-date
3:20:14 pm: Execution finished 'run --stacktrace'.
```