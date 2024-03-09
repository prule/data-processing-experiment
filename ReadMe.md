Data Processing Experiment - Part 6
-
The one where I start loading multiple tables

---

> The code for this project is available in GitHub - I’m using a branch for each part and merging each part into the **[latest](https://github.com/prule/data-processing-experiment/tree/latest)** branch. See the ReadMe.md in each branch for the story.
>
> - [Github repository for this project](https://github.com/prule/data-processing-experiment/)
> - [Pull requests for each part](https://github.com/prule/data-processing-experiment/pulls?q=is%3Apr+is%3Aclosed) 
> - [Branch for part-6](https://github.com/prule/data-processing-experiment/tree/part-6)

Now that I have a simple system that lets me load a table, type it, validate it, and generate statistics, it's time to start pulling it together and making it more useful.

To start with I need to be able to define multiple tables and then I'll need a bit more functionality for tables and columns. I'll also need some more sample data to make the current use case more interesting. 

I've added `deduplicate` and `delimiter` to the table definition configuration:
```json5
table: {
    name: "lga-1",
    description: "",
    deduplicate: true,
    delimiter: ";",
    columns: [
    //...
    ]
}
```

I'm not 100% sure at the moment if these should live on Table or FileSource - but I won't dwell on this now since it's a [type 2](https://www.businessinsider.com/jeff-bezos-on-type-1-and-type-2-decisions-2016-4) decision at this point. It looks okay and this decision is easily reversible, so let's run with it for now. I'm speaking from the perspective of an early stage experiment here - if this project was to mature into a usable project and be released then the external facing parts - such as the json configuration - would need extra scrutiny since when you have a wide user base these things then become more difficult to change. But we are nowhere near this now.

I've also added `alias` to column definitions so I can change a column name when getting the typed dataset.

```json5
{
    name: "Code State", // csv header name
    alias: "level_1_code", // column name in typed dataset
    description: "",
    type: "string",
    required: true
}
```
What I'm trying to do here is support receiving data from different parties - whereby it might be arranged slightly differently even if the meaning of the data is the same.

So now 
- the configuration lets us handle different delimiters in the CSV files
- duplicates can be removed easily
- column names can be standardised through aliasing

I've also implemented a [DuplicateCount](https://github.com/prule/data-processing-experiment/blob/part-6/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/statistics/DuplicateCount.kt) statistic since it can be useful to know how many rows are duplicated in the data. At the moment it's the simplest implementation that would work (uses ALL columns) but it could be extended to take a list of columns to use in duplicate detection.

I've added some more [data](https://github.com/prule/data-processing-experiment/tree/part-6/data/sample1/lgas) to sample 1 - Hierarchy data mapping Local Government Areas (LGA) to State. To keep the size small I've reduced it to only 3 states, provided separately and changed it so there are differences in the column names and number of columns - but they all provide the essential information. Instead of being comma separated, this data uses semicolon - hence the need to add delimiter configuration.

```text
data
└── sample1
    ├── lgas
    │   ├── 1
    │   │   └── lga-1.csv
    │   ├── 2
    │   │   └── lga-2.csv
    │   └── 3
    │       └── lga-3.csv
    ├── transactions
    │   ├── 2020-01.csv
    │   ├── 2020-02.csv
    │   ├── 2020-03.csv
    │   └── invalid-rows.csv
```

An example LGA to state mapping hierarchy CSV looks like this:

| Code State |Name State|Code Local Government Area|Name Local Government Area|
|------------|----------|--------------------------|--------------------------|
| 1	         | New South Wales|	10850	   |                 Blayney|
| 1	         | New South Wales|11700|Central Darling|
| ...        | | | |
| 2|Victoria|21750|Colac Otway|
| 2|Victoria|22670|Greater Dandenong|
| ...        | | | |

which can be represented as a tree:
```csv
Australia
├── NSW
│   ├── Blayney
│   ├── Central Darling
│   ├── ...
├── VIC
│   ├── Colac Otway
│   ├── Greater Dandenong
│   ├── ...
...
```
Now if we see a transaction with a location of `Berrigan` we know it's in the state of NSW and now we can roll up transactions by state.

For details about hierarchies and all things star schema, see [Star Schema The Complete Reference](https://www.amazon.com.au/Schema-Complete-Reference-Christopher-Adamson/dp/0071744320/ref=sr_1_1) - a great book, I highly recommend.

Because I've simulated the 3 LGA datasets coming from different sources they get defined as separate tables with slightly different details in the table configuration. To link the transactions to the hierarchy, a location column has been added to transactions. 

```json5
{
  id: "sample1",
  name: "Sample 1",
  description: "Sample 1 is a basic dataset configuration to demonstrate capability",
  sources: [
    {
      id: "transactions",
      name: "Transactions",
      description: "Transactions contains transactions from multiple bank accounts",
      path: "sample1/transactions/",
      type: "csv",
      table: {
        name: "transactions",
        description: "account transactions",
        deduplicate: true,
        columns: [
          {
            name: "date",
            description: "date of transaction",
            type: "date",
            formats: [
              "yyyy-MM-dd",
              "dd-MM-yyyy"
            ],
            required: true
          },
          
          // other columns excluded for brevity...
          
          // add location column so we can link to the LGA hierarchy
          {
            name: "location",
            description: "location",
            type: "string"
          },
        ]
      }
    },
    {
      id: "lga-1",
      name: "lga-1",
      description: "lga-1",
      path: "sample1/lgas/1",
      type: "csv",
      table: {
        name: "lga-1",
        description: "",
        deduplicate: true,
        delimiter: ";",
        // just the columns we want, using alias to standardise column names
        columns: [
          {
            name: "Code State",
            alias: "level_1_code",
            description: "",
            type: "string",
            required: true
          },
          {
            name: "Name State",
            alias: "level_1_name",
            description: "",
            type: "string",
            required: true
          },
          {
            name: "Code Local Government Area",
            alias: "level_2_code",
            description: "",
            type: "string",
            required: true
          },
          {
            name: "Name Local Government Area",
            alias: "level_2_name",
            description: "",
            type: "string",
            required: true
          }
        ]
      }
    },

    {
      id: "lga-2",
      name: "lga-2",
      description: "lga-2",
      path: "sample1/lgas/2",
      type: "csv",
      table: {
        name: "lga-2",
        description: "",
        deduplicate: true,
        delimiter: ";",
        // just the columns we want, using alias to standardise column names
        columns: [
          {
            name: "Official Code State",
            alias: "level_1_code",
            description: "",
            type: "string",
            required: true
          },
          {
            name: "Official Name State",
            alias: "level_1_name",
            description: "",
            type: "string",
            required: true
          },
          {
            name: "Official Code Local Government Area",
            alias: "level_2_code",
            description: "",
            type: "string",
            required: true
          },
          {
            name: "Official Name Local Government Area",
            alias: "level_2_name",
            description: "",
            type: "string",
            required: true
          }
        ]
      }
    },
    {
      id: "lga-3",
      name: "lga-3",
      description: "lga-3",
      path: "sample1/lgas/3",
      type: "csv",
      table: {
        name: "lga-3",
        description: "",
        deduplicate: true,
        delimiter: ";",
        // just the columns we want, using alias to standardise column names
        columns: [
          {
            name: "Official Code State",
            alias: "level_1_code",
            description: "",
            type: "string",
            required: true
          },
          {
            name: "Official Name State",
            alias: "level_1_name",
            description: "",
            type: "string",
            required: true
          },
          {
            name: "Official Code Local Government Area",
            alias: "level_2_code",
            description: "",
            type: "string",
            required: true
          },
          {
            name: "Official Name Local Government Area",
            alias: "level_2_name",
            description: "",
            type: "string",
            required: true
          }
        ]
      }
    }

  ]
}
```

Once we load these LGA dataframes they'll all look the same, and it would be possible to union them together. In fact, as this experiment progresses I hope to add more functionality - such as unioning, joining, derived columns etc via configuration - in the hope that this achieves 80% of the basic needs, leaving only the remaining 20% for bespoke implementation. If this pans out then it may result in a framework that allows for quick, easy, self documenting, consistent, low code data pipelines - which would be much better than 100% bespoke code every time.

In the [reference application](https://github.com/prule/data-processing-experiment/blob/part-6/app/src/main/kotlin/com/example/dataprocessingexperiment/app/App.kt), all thats needed now is to load the table configurations and iterate over the table definitions: 

```kotlin
// load table configuration
val tables = Json5.decodeFromStream<Tables>(
    this::class.java.getResourceAsStream("/sample1.tables.json5")!!
)
// load statistics configuration
val statisticConfiguration = Json5.decodeFromStream<StatisticsConfiguration>(
    this::class.java.getResourceAsStream("/sample1.statistics.json5")!!
)
// process each table
tables.sources.forEach { fileSource ->
    
    // use DataFrameBuilder to load the table

    // get the RAW version of the dataset, everything is a string, and all columns are included
    //   output some rows for demonstration
    //   generate RAW statistics 

    // get the TYPED dataset
    //   output some rows for demonstration

    // get the VALID dataset
    //   output some rows for demonstration
    //   generate VALID statistics 
    
}
```

If the [JSON configuration](https://github.com/prule/data-processing-experiment/blob/part-6/app/src/main/resources/sample1.tables.json5) is looking unwieldy then don't worry. In a complete solution the JSON wouldn't have to be hand crafted - tools could take care of this...

Imagine an application that lets you define data providers:
- For each data provider you define data sources (tables)
  - Then you can set up data pipelines and reference any of these sources
    - Then data pipelines can be run
      - Where each step in the pipeline defines it's inputs and outputs so the dependency graph can be derived and executed appropriately

I'm aware there are existing tools for doing this - each with their associated costs and constraints. But the purpose of this exercise is to gain experience through DOING ([John Crickett](https://www.linkedin.com/in/johncrickett/) style!). And I'm writing this up do gain experience in communication - which is actually taking more time than doing the coding. 

> This is an interesting point - **building the system is actually fast, inexpensive, and simple** (this is a **credit to the tools, languages and libraries** we have these days). Writing about it is slower, harder, and more complicated and is where the time is going!

Now, let's see the output from our reference implementation:

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
+----------+
|sum(count)|
+----------+
|         4|
+----------+


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
+----------+
|sum(count)|
+----------+
|      NULL|
+----------+


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
+----------+
|sum(count)|
+----------+
|         2|
+----------+


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
+----------+
|sum(count)|
+----------+
|      NULL|
+----------+


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
+----------+
|sum(count)|
+----------+
|      NULL|
+----------+


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
+----------+
|sum(count)|
+----------+
|      NULL|
+----------+


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
+----------+
|sum(count)|
+----------+
|      NULL|
+----------+


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
+----------+
|sum(count)|
+----------+
|      NULL|
+----------+


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
Finished...
BUILD SUCCESSFUL in 8s
```

