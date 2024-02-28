Data Processing Experiment - Part 5
-
The one where I derive statistics from the data.

---

With the table definitions set up, there's now an opportunity to easily create some statistics about the data. 

The most simple scenario would be a row count for the dataframe so we know how much data is arriving, and by comparing the raw row count with the valid row count we know the amount of invalid data. Ways this could be useful are to generate reports whenever the data is processed - so patterns can be established and anomalies spotted. 

A couple of patterns I've seen in software frequently impress me:

- Spring Framework - does so many things, and makes everything easy yet is not intrusive - you only need to use what you want
- AWS services - many individual services that do one simple thing, but together these can be combined to provide value greater than the sum of the individual services
- Linux - the simplicity of the [everything is a file](https://en.wikipedia.org/wiki/Everything_is_a_file) approach.

With this in mind, I don't want this aspect to impact the existing code - it should sit alongside it - not with it. In a bigger project I'd use a separate module - but for brevity here I'll just put everything in a separate package - separate and optional.

So the model I want looks something like:

```kotlin
StatisticsRunner().process(dataset, statistics, collector)
```

Statistics should be configurable and be able to be applied to any dataframe. 

Configuration for the statements table could look like this:

```json5
{
  name: "Statements Statistics",
  description: "Statistics configuration for the statements table",
  values: [
    {
      id: "count"
    },
    {
      id: "countByMonth",
      column: "date"
    },
    {
      id: "countByValue",
      column: "account"
    },
    {
      id: "bounds",
      column: "amount"
    }
  ],
}
```
This is a list of statistics we want to run, with optional configuration such as the column name needed for that particular statistic.

In this example we have
- count : count the number of rows
- countByMonth: row count grouped by the Year-Month value of the given column 
- countByValue: row count grouped by the value of the given column
- bounds: the min and max values of the given column

With our sample dataset the result would look something like:

For the RAW dataset
```text
RAW Statistics

+------------+-------+-------------+-----+
|         key| column|discriminator|value|
+------------+-------+-------------+-----+
|CountByMonth|   date|         NULL|    7|
|CountByMonth|   date|      2020-01|    4|
|CountByMonth|   date|      2020-02|    5|
|CountByMonth|   date|      2020-12|    2|
|CountByValue|account|         NULL|    1|
|CountByValue|account|            1|    8|
|CountByValue|account|            2|    4|
|CountByValue|account|            x|    5|
|         max| amount|         NULL|    x|
|         min| amount|         NULL| 0.01|
|   row count|   NULL|         NULL|   18|
+------------+-------+-------------+-----+
```
For the VALID dataset
```text
VALID Statistics

+------------+-------+-------------+------+
|         key| column|discriminator| value|
+------------+-------+-------------+------+
|CountByMonth|   date|      2020-01|     4|
|CountByMonth|   date|      2020-02|     5|
|CountByMonth|   date|      2020-03|     3|
|CountByValue|account|            1|     8|
|CountByValue|account|            2|     4|
|         max| amount|         NULL|300.47|
|         min| amount|         NULL| 15.45|
|   row count|   NULL|         NULL|    12|
+------------+-------+-------------+------+
``` 
Looking at [App.kt](https://github.com/prule/data-processing-experiment/blob/part-5/app/src/main/kotlin/com/example/dataprocessingexperiment/app/App.kt) shows how to use this:

```kotlin
    private fun generateStatistics(dataset: Dataset<Row>, path: String, sparkSession: SparkSession) {
    // load configuration
    val statisticConfiguration = Json5.decodeFromStream<Statistics>(
        this::class.java.getResourceAsStream("/sample1.statements.statistics.json5")!!
    )
    // transform from configuration to implementation
    val statistics = StatisticRepository().buildStatistics(statisticConfiguration)
    // instantiate a collector for gathering results
    val collector = SparkCollector(sparkSession, path)
    // process the statistics for the given dataset, and close the collector on completion
    // this will result in the statistics being written to CSV 
    collector.use {
        StatisticsRunner().process(dataset, statistics, collector)
    }
}
```
Implementing a [statistic](https://github.com/prule/data-processing-experiment/tree/part-5/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/statistics) is simple:
```kotlin
class Count(): Statistic {

    override fun run(data: Dataset<Row>, collector: Collector) {
        val value = data.count()
        collector.add("row count", "", "", value)
    }

}
```
If a new statistic is implemented it needs to be registered via `StatisticRepository` so that when we use `statisticRepository.buildStatistics()` it will find the implementation.

There's much that could be improved and extended here - this is just a start. But it's extendable. New statistics can be coded, registered, and applied easily. 

Generating reports from these statistics is a separate concern - this would be a separate module again, that is only concerned with displaying and comparing data. Not in anyway involved with how that data is generated.

Let's do another (subjective) sanity check at this point:

- Complexity = VERY LOW
- Value = SMALL, LIMITED (growing?)
- Potential = MEDIUM

Now lets do a reality check:

- [AWS SageMaker Data Wrangler](https://aws.amazon.com/sagemaker/data-wrangler/) lets you do more than I have time for 
  - data selection, cleansing, exploration, visualization, and processing at scale
  - 300 built-in data transformations, so you can quickly transform data without writing any code.

But at the same time I see this discussion on Reddit:

- [Now that Talend is no longer free, what other ETL tool would you recommend...](https://www.reddit.com/r/dataengineering/comments/1axyooe/talend_is_no_longer_free/) where people suggest everything from custom code to all manner of tools. 

Should I take comfort in knowing that many people need these products and many tools exist to serve those needs? It's enough motivation to know that the problem space is real, even if my solution is hypothetical and serves only for experience... give me a thumbs up if you want to encourage me and see where this goes!

Let's run the application again to see what it produces:

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

RAW Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+------------+-------+-------------+-----+
|         key| column|discriminator|value|
+------------+-------+-------------+-----+
|CountByMonth|   date|         NULL|    7|
|CountByMonth|   date|      2020-01|    4|
|CountByMonth|   date|      2020-02|    5|
|CountByMonth|   date|      2020-12|    2|
|CountByValue|account|         NULL|    1|
|CountByValue|account|            1|    8|
|CountByValue|account|            2|    4|
|CountByValue|account|            x|    5|
|         max| amount|         NULL|    x|
|         min| amount|         NULL| 0.01|
|   row count|   NULL|         NULL|   18|
+------------+-------+-------------+-----+

row count = 11
21:57:16.830 [main] INFO  c.e.d.spark.data.types.DecimalType MDC= - Using decimal(10,2) for column amount

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
21:57:16.974 [main] INFO  c.e.d.spark.data.types.DecimalType MDC= - Using decimal(10,2) for column amount

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

VALID Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+------------+-------+-------------+------+
|         key| column|discriminator| value|
+------------+-------+-------------+------+
|CountByMonth|   date|      2020-01|     4|
|CountByMonth|   date|      2020-02|     5|
|CountByMonth|   date|      2020-03|     3|
|CountByValue|account|            1|     8|
|CountByValue|account|            2|     4|
|         max| amount|         NULL|300.47|
|         min| amount|         NULL| 15.45|
|   row count|   NULL|         NULL|    12|
+------------+-------+-------------+------+

row count = 8
Finished...

BUILD SUCCESSFUL in 5s
10 actionable tasks: 4 executed, 6 up-to-date
```