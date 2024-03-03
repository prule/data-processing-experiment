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
- joins
- unions
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