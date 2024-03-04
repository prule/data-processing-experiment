Data Processing Experiment - Part 7
- The one where I look at the current state of things and ponder what the future may hold


---

> The code for this project is available in GitHub - I’m using a branch for each part and merging each part into the **[latest](https://github.com/prule/data-processing-experiment/tree/latest)** branch. See the ReadMe.md in each branch for the story.
>
> - [Github repository for this project](https://github.com/prule/data-processing-experiment/)
> - [Pull requests for each part](https://github.com/prule/data-processing-experiment/pulls?q=is%3Apr+is%3Aclosed) 
> - [Branch for part-7](https://github.com/prule/data-processing-experiment/tree/part-7)

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
As a result of this, we need to make `alias` mandatory now, so that we have a definitive name for the column - this is also a good thing since any processes that follow will know what the column is called and can reference it - protected from any changes to the raw data column names.

To support this I've added a new step "Selected" so now we go from raw -> selected -> typed -> valid

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
So now we'll select the first column with any of the following names
```
"Code State",
"Official Code State"
```
and alias it to `level_1_code`.

---

Giving tables context
-

So far all thats happening is loading some CSV files into dataframes and performing some simple transforms. In order to do more than that these tables need to be stored in a context after which they can be accessed by a series of transformers that get the real work done.

In the reference implementation App.kt, as each table is loaded, the valid dataframe is added to the context
```kotlin
context.add(fileSource.id, validDataset)
```
Once the context is fully populated, UnionProcessor can then process tables with `union` directives and further build the context. 
```kotlin
// process union directives
UnionProcessor(context).process()

// display result
context.show()
```

Note that all this is very fluid at the moment - because we don't KNOW what final form we want it to take, it's good to work fast and refactor ruthlessly - so everything is subject to change. What I'm keen to avoid is analysis paralysis and getting tied into a particular implementation or pattern prematurely. I'll implement something, see what it looks like, and then change accordingly.

Next up, we'll see what it looks like to add transformers to table definitions.