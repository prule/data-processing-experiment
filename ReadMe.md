Data Processing Experiment - Part 6
-
The one where I start loading multiple tables

---

Now that I have a simple system that lets me load a table, type it, validate it, and generate statistics, it's time to start pulling it together and making it more useful.

To start with I need to be able to define multiple tables and then I'll need a bit more functionality for tables and columns. I'll also need some sample data to make the current use case more interesting. 

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

I'm not 100% sure at the moment if these should live on Table or FileSource - but I won't dwell on this now since it's a [type 2](https://www.businessinsider.com/jeff-bezos-on-type-1-and-type-2-decisions-2016-4) decision. It looks okay and this decision is easily reversible, so let's run with it for now. (I'm speaking from the perspective of an early stage experiment here - if this project was to mature into a usable project and be released then the external facing parts - such as the json configuration - would need extra scrutiny since when you have a wide user base these things then become more difficult to change. But we are nowhere near this now.)

I've also added `alias` to column definitions so I can change a column name.

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

I've added some more data to sample 1 - Hierarchy data mapping Local Government Areas (LGA) to State. To keep the size small I've reduced it to only 3 states, provided separately and changed it so there are differences in the column names and number of columns - but they all provide the essential information. Instead of being comma separated, this data uses semicolon - hence the need to add delimiter configuration.

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

For details about hierarchies and all things star schema, see [Star Schema The Complete Reference](https://www.amazon.com.au/Schema-Complete-Reference-Christopher-Adamson/dp/0071744320/ref=sr_1_1) - a great book, I highly recommend.

Because I've simulated the 3 LGA datasets coming from different sources they get defined as separate tables in the table configuration. To link the transactions to the hierarchy, a location column has been added to transactions. 

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

If the JSON configuration is looking unwieldy then don't worry. In a complete solution the JSON wouldn't have to be hand crafted - tools could do this...

Imagine an application that lets you define data providers:
- For each data provider you define data sources (tables)
  - Then you can set up data pipelines and reference any of these sources
    - Then data pipelines can be run
      - Where each step in the pipeline defines it's inputs and outputs so the dependency graph can be derived and executed appropriately

I'm aware there are existing tools for doing this - each with their associated costs and constraints. But the purpose of this exercise is to gain experience through DOING ([John Crickett](https://www.linkedin.com/in/johncrickett/) style!). And I'm writing this up do gain experience in communication - which is actually taking more time than doing the coding. 

This is an interesting point - building the system is actually fast, inexpensive, and simple. Writing about it is slower, harder, and more complicated!

Now, let's see the output from our reference implementation:

```text
```

