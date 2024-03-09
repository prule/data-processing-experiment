Data Processing Experiment - Part 7
- The one where I look at the current state of things and ponder what the future may hold

---

> The code for this project is available in GitHub - I’m using a branch for each part and merging each part into the **[latest](https://github.com/prule/data-processing-experiment/tree/latest)** branch. See the ReadMe.md in each branch for the story.
>
> - [Github repository for this project](https://github.com/prule/data-processing-experiment/)
> - [Pull requests for each part](https://github.com/prule/data-processing-experiment/pulls?q=is%3Apr+is%3Aclosed) 
> - [Branch for part-7](https://github.com/prule/data-processing-experiment/tree/part-7)
---

Review
=

At the moment this framework supports the following:
- Configuration of raw tables via JSON
  - since this is "relatively" easy to read, it can be part of documentation - we can see what raw data is used, and how it is formatted or interpreted
- These tables can be loaded into a **raw** dataframe with only the columns we have configured
  - everything is a string at this point
- The raw dataframe (everything is a string) can be converted to a **typed** dataframe using information in the table configuration
  - custom type converters can be easily implemented
  - values that can't be converted will be null
  - columns are renamed if an alias is provided in the table configuration providing standards we can use in further processing (abstracted from changes that may happen with the raw data)
- The typed dataframe can be filtered to remove rows that have missing required columns and duplicate rows can be removed - all based off configuration - producing a **valid** dataframe
- Statistics can be generated for each stage 
  - for the raw dataframe so we capture the state of the data as we received it 
  - by comparing statistics from the valid dataframe to the raw statistics we can get a sense of data quality - how much was deemed invalid
  - we also have a benchmark of the shape of the data which we can compare over time
    - how fast is the data growing
    - how the data is distributed (eg number of transactions per month)
      - when things don't look right, these statistics might be useful in finding data problems

Futures
=
Possible future capabilities/considerations:
- support multiple column names in the raw data (for when data is delivered inconsistently)
- unions
- joins
- configure a list of transformers for a table
  - could these be implemented as transformers?
    - add literal columns
    - add derived columns (like col A * col B)
    - map column values (eg from 1 to Male, from 2 to Female)
    - de-normalise hierarchy in fact table
    - write custom/bespoke transforms
- task based pipelines with automatic dependency management
- statistics
  - cardinality of columns?
- performance
  - partitioning?
  - https://spark.apache.org/docs/latest/sql-performance-tuning.html
    - join hints
- sample data generation
  - using the table definitions and a little extra configuration we could generate data in the format matching the configuration - good for when you need data but don't need real (which might be sensitive) data
    - provide a way to inject invalid values to exercise statistics and validation
- defensive measures
  - statistics could generate massive data if - for example - countBy is used on a high cardinality column - should it check cardinality first before running?
- configuration validation
  - detect problems with configurations and report them nicely so they can be fixed
- Applications
  - web based configuration? don't need to handcraft json then.
  - web based reporting of statistics and comparisons across statistics?
    - easy viewing of statistics across phases of a pipeline
    - easy viewing of statistics across different runs of a pipeline
  - web based management of pipelines - running, reporting
- Is it worth considering other implementations
  - Instead of spark can we use SQL?
    - eg if the data resides in a database can we do the same things - same config, just different implementation?

There is much to be distracted by here, but the primary concerns are keeping the system simple and delivering value.

---
Moving forward
=
Multiple column names
-
Starting from the top, supporting multiple column names is trivial. The first step is to modify the JSON configuration so we can provide an array of strings for `name` and rename the column field from `name` to `names` so now we can do:

```json5
columns: [
  {
    names: [
      "Code State",
      "Official Code State"
    ],
    alias: "level_1_code",
    description: "",
    type: "string",
    required: true
  },
  ...
```
With this configuration, we will select the first column with any of the following names
```
"Code State",
"Official Code State"
```
and alias it to `level_1_code`.

As a result of this, it's necessary to make `alias` mandatory now, so that there is a definitive name for the column - this is also a good thing since any processes that follow will know what the column is called and can reference it - protected from any changes to the raw data column names.

To support this I've added a new step [selected()](https://github.com/prule/data-processing-experiment/blob/part-7/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/data/DataFrameBuilder.kt) so now we have the following stages:
- raw
- selected 
- typed
- valid

```kotlin
/**
 * Selects only the columns specified in configuration and maps them to the alias.
 */
fun selected(): Dataset<Row> {
    val columns: List<Column> =
        fileSource.table.columns.map { column ->
            // find the first column that exists with the given names
            var c: Column? = null
            for (name in column.names) {
                if (raw.columns().contains(name)) {
                    c = col(name)
                    break
                }
            }
            // if we can't find a column then throw exception
            if (c == null) {
                throw RuntimeException("Could not find any of the columns ${fileSource.table.columns} on table ${fileSource.id} at ${fileSource.path}")
            }
            // rename column to alias
            c.`as`(column.alias)
        }

    return raw.select(*columns.map { it }.toTypedArray())
}
```

---

Giving tables context
-

So far all thats happening is loading some CSV files into dataframes and performing some simple transforms. In order to do more than that these tables need to be stored in a context after which they can be accessed by a series of transformers that get the real work done.

In the reference implementation [App.kt](https://github.com/prule/data-processing-experiment/blob/part-7/app/src/main/kotlin/com/example/dataprocessingexperiment/app/App.kt#L111), as each table is loaded, the valid dataframe is added to the context
```kotlin
context.add(fileSource.id, validDataset)
```

Once the context is fully populated, we have the opportunity to provide functionality across tables. Lets start with being able to UNION tables together. In this case we'll union the 3 lga tables (lga-1, lga-2, lga-3) together into one (lgas) by using a `union` attribute on the FileSource configuration:

```json5
{
  id: "lga-1",
  type: "csv",
  //...
  union: "lgas", // union this to a dataframe called "lgas"
  table: {
    //...
  }
},
{
  id: "lga-2",
  type: "csv",
  //...
  union: "lgas", // union this to a dataframe called "lgas"
  table: {
    //...
  }
},
{
  id: "lga-3",
  type: "csv",
  //...
  union: "lgas", // union this to a dataframe called "lgas"
  table: {
    //...
  }
},
```
The union operation is handled by [UnionProcessor](https://github.com/prule/data-processing-experiment/blob/part-7/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/UnionProcessor.kt) after the context is built using: 
```kotlin
// process union directives
UnionProcessor(context).process()

// display result
context.show()
```

---
Wrapping up
=
Note that all this is very fluid at the moment - because we don't KNOW what final form we want it to take, it's good to work fast and refactor ruthlessly - so everything is subject to change. What I'm keen to avoid is analysis paralysis and getting tied into a particular implementation or pattern prematurely. I'll implement something, see what it looks like, and then change accordingly.

Next up, we'll see what it looks like to define and execute a series of tasks given the populated context.

Here's the output from the reference application at this stage:

```text
> Task :app:run
Starting...

Raw dataset

root
 |-- date: string (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- amount: string (nullable = true)
 |-- location: string (nullable = true)
 |-- comment: string (nullable = true)

+------------+-------+------------+-------+-----------------+--------------------+
|        date|account| description| amount|         location|             comment|
+------------+-------+------------+-------+-----------------+--------------------+
|        NULL|      x|      tennis|   0.03|             NULL|             no date|
|  01-03-2020|      1|      burger|  15.47|Greater Dandenong|alternative date ...|
|  03-03-2020|      1|      tennis|  35.03|        Maroondah|alternative date ...|
|  04-03-2020|      2|      petrol| 150.47|             NULL|alternative date ...|
|  2020-01-01|      1|      burger|  15.45|           Gympie|                NULL|
|  2020-01-02|      1|       movie|  20.00|   Southern Downs|                NULL|
|  2020-01-03|      1|      tennis|  35.00|           Banana|                NULL|
|  2020-01-04|      2|      petrol| 150.45|Central Highlands|                NULL|
|  2020-01-04|      2|      petrol| 150.45|Central Highlands|                NULL|
|  2020-02-01|      1|      burger|  15.46|         Yarrabah|                NULL|
|  2020-02-02|      1|       movie|  20.01|       Barcaldine|                NULL|
|  2020-02-03|      1|      tennis|  35.01|        Maroondah|                NULL|
|  2020-02-04|      2|      petrol| 150.46|       Gannawarra|                NULL|
|  2020-02-04|      2| electricity| 300.47|          Hepburn|                NULL|
|  2020-12-01|       |      tennis|   0.04|             NULL| blank (many spac...|
|  2020-12-01|      x|      petrol|      x|             NULL| invalid number f...|
|  2020-13-01|      x|      burger|   0.01| unknown location|        invalid date|
|invalid date|      x|      petrol|   0.02|                 |        invalid date|
|           x|      x|           x|      x|             NULL| row with multipl...|
|           x|      x|           x|      x|             NULL| row with multipl...|
+------------+-------+------------+-------+-----------------+--------------------+

row count = 20

RAW Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+-------+-------------+-----+
|                key| column|discriminator|value|
+-------------------+-------+-------------+-----+
|       CountByMonth|   date|         NULL|    8|
|       CountByMonth|   date|      2020-01|    5|
|       CountByMonth|   date|      2020-02|    5|
|       CountByMonth|   date|      2020-12|    2|
|       CountByValue|account|         NULL|    1|
|       CountByValue|account|            1|    8|
|       CountByValue|account|            2|    5|
|       CountByValue|account|            x|    6|
|duplicate row count|   NULL|         NULL|    4|
|                max| amount|         NULL|    x|
|                min| amount|         NULL| 0.01|
|          row count|   NULL|         NULL|   20|
+-------------------+-------+-------------+-----+

row count = 12

SELECTED dataset

root
 |-- date: string (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- location: string (nullable = true)
 |-- amount: string (nullable = true)

+------------+-------+------------+-----------------+-------+
|        date|account| description|         location| amount|
+------------+-------+------------+-----------------+-------+
|        NULL|      x|      tennis|             NULL|   0.03|
|  01-03-2020|      1|      burger|Greater Dandenong|  15.47|
|  03-03-2020|      1|      tennis|        Maroondah|  35.03|
|  04-03-2020|      2|      petrol|             NULL| 150.47|
|  2020-01-01|      1|      burger|           Gympie|  15.45|
|  2020-01-02|      1|       movie|   Southern Downs|  20.00|
|  2020-01-03|      1|      tennis|           Banana|  35.00|
|  2020-01-04|      2|      petrol|Central Highlands| 150.45|
|  2020-01-04|      2|      petrol|Central Highlands| 150.45|
|  2020-02-01|      1|      burger|         Yarrabah|  15.46|
|  2020-02-02|      1|       movie|       Barcaldine|  20.01|
|  2020-02-03|      1|      tennis|        Maroondah|  35.01|
|  2020-02-04|      2|      petrol|       Gannawarra| 150.46|
|  2020-02-04|      2| electricity|          Hepburn| 300.47|
|  2020-12-01|       |      tennis|             NULL|   0.04|
|  2020-12-01|      x|      petrol|             NULL|      x|
|  2020-13-01|      x|      burger| unknown location|   0.01|
|invalid date|      x|      petrol|                 |   0.02|
|           x|      x|           x|             NULL|      x|
|           x|      x|           x|             NULL|      x|
+------------+-------+------------+-----------------+-------+

row count = 20

Typed dataset

root
 |-- date: date (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- location: string (nullable = true)
 |-- amount: decimal(10,2) (nullable = true)

+----------+-------+------------+-----------------+------+
|      date|account| description|         location|amount|
+----------+-------+------------+-----------------+------+
|      NULL|      x|      burger| unknown location|  0.01|
|      NULL|      x|      petrol|                 |  0.02|
|      NULL|      x|      tennis|             NULL|  0.03|
|      NULL|      x|           x|             NULL|  NULL|
|      NULL|      x|           x|             NULL|  NULL|
|2020-01-01|      1|      burger|           Gympie| 15.45|
|2020-01-02|      1|       movie|   Southern Downs| 20.00|
|2020-01-03|      1|      tennis|           Banana| 35.00|
|2020-01-04|      2|      petrol|Central Highlands|150.45|
|2020-01-04|      2|      petrol|Central Highlands|150.45|
|2020-02-01|      1|      burger|         Yarrabah| 15.46|
|2020-02-02|      1|       movie|       Barcaldine| 20.01|
|2020-02-03|      1|      tennis|        Maroondah| 35.01|
|2020-02-04|      2|      petrol|       Gannawarra|150.46|
|2020-02-04|      2| electricity|          Hepburn|300.47|
|2020-03-01|      1|      burger|Greater Dandenong| 15.47|
|2020-03-03|      1|      tennis|        Maroondah| 35.03|
|2020-03-04|      2|      petrol|             NULL|150.47|
|2020-12-01|       |      tennis|             NULL|  0.04|
|2020-12-01|      x|      petrol|             NULL|  NULL|
+----------+-------+------------+-----------------+------+

row count = 20

Valid dataset

root
 |-- date: date (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- location: string (nullable = true)
 |-- amount: decimal(10,2) (nullable = true)

+----------+-------+------------+-----------------+------+
|      date|account| description|         location|amount|
+----------+-------+------------+-----------------+------+
|2020-01-01|      1|      burger|           Gympie| 15.45|
|2020-01-02|      1|       movie|   Southern Downs| 20.00|
|2020-01-03|      1|      tennis|           Banana| 35.00|
|2020-01-04|      2|      petrol|Central Highlands|150.45|
|2020-02-01|      1|      burger|         Yarrabah| 15.46|
|2020-02-02|      1|       movie|       Barcaldine| 20.01|
|2020-02-03|      1|      tennis|        Maroondah| 35.01|
|2020-02-04|      2|      petrol|       Gannawarra|150.46|
|2020-02-04|      2| electricity|          Hepburn|300.47|
|2020-03-01|      1|      burger|Greater Dandenong| 15.47|
|2020-03-03|      1|      tennis|        Maroondah| 35.03|
|2020-03-04|      2|      petrol|             NULL|150.47|
+----------+-------+------------+-----------------+------+

row count = 12

VALID Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+-------+-------------+------+
|                key| column|discriminator| value|
+-------------------+-------+-------------+------+
|       CountByMonth|   date|      2020-01|     4|
|       CountByMonth|   date|      2020-02|     5|
|       CountByMonth|   date|      2020-03|     3|
|       CountByValue|account|            1|     8|
|       CountByValue|account|            2|     4|
|duplicate row count|   NULL|         NULL|     0|
|                max| amount|         NULL|300.47|
|                min| amount|         NULL| 15.45|
|          row count|   NULL|         NULL|    12|
+-------------------+-------+-------------+------+

row count = 9

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
|         1|New South Wales|2021|                     10650|                 AUS|                  Berrigan|local government ...|                       Berrigan|
|         1|New South Wales|2021|                     10650|                 AUS|                  Berrigan|local government ...|                       Berrigan|
|         1|New South Wales|2021|                     10850|                 AUS|                   Blayney|local government ...|                        Blayney|
|         1|New South Wales|2021|                     11500|                 AUS|              Campbelltown|local government ...|             Campbelltown (NSW)|
|         1|New South Wales|2021|                     11700|                 AUS|           Central Darling|local government ...|                Central Darling|
|         1|New South Wales|2021|                     15990|                 AUS|          Northern Beaches|local government ...|               Northern Beaches|
|         1|New South Wales|2021|                     11300|                 AUS|                   Burwood|local government ...|                        Burwood|
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
|duplicate row count|  NULL|         NULL|    2|
|          row count|  NULL|         NULL|   11|
+-------------------+------+-------------+-----+

row count = 2

SELECTED dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+---------------+------------+----------------+
|level_1_code|   level_1_name|level_2_code|    level_2_name|
+------------+---------------+------------+----------------+
|           1|New South Wales|       10650|        Berrigan|
|           1|New South Wales|       10650|        Berrigan|
|           1|New South Wales|       10850|         Blayney|
|           1|New South Wales|       11500|    Campbelltown|
|           1|New South Wales|       11700| Central Darling|
|           1|New South Wales|       15990|Northern Beaches|
|           1|New South Wales|       11300|         Burwood|
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
|           1|New South Wales|       10650|        Berrigan|
|           1|New South Wales|       10650|        Berrigan|
|           1|New South Wales|       10850|         Blayney|
|           1|New South Wales|       11500|    Campbelltown|
|           1|New South Wales|       11700| Central Darling|
|           1|New South Wales|       15990|Northern Beaches|
|           1|New South Wales|       11300|         Burwood|
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
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 2

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
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 2

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
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 2

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
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 2

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
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 2
==============================================
Context
transactions
+----------+-------+------------+-----------------+------+
|      date|account| description|         location|amount|
+----------+-------+------------+-----------------+------+
|2020-03-01|      1|      burger|Greater Dandenong| 15.47|
|2020-02-04|      2|      petrol|       Gannawarra|150.46|
|2020-01-01|      1|      burger|           Gympie| 15.45|
|2020-02-03|      1|      tennis|        Maroondah| 35.01|
|2020-01-02|      1|       movie|   Southern Downs| 20.00|
|2020-02-02|      1|       movie|       Barcaldine| 20.01|
|2020-01-03|      1|      tennis|           Banana| 35.00|
|2020-02-01|      1|      burger|         Yarrabah| 15.46|
|2020-01-04|      2|      petrol|Central Highlands|150.45|
|2020-02-04|      2| electricity|          Hepburn|300.47|
|2020-03-03|      1|      tennis|        Maroondah| 35.03|
|2020-03-04|      2|      petrol|             NULL|150.47|
+----------+-------+------------+-----------------+------+

lga-1
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

lga-2
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

lga-3
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

lgas
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

==============================================
Finished...
```

