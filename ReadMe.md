Data Processing Experiment - Part 10
---
- The one where I refactor to clean up and use more polymorphic serialization to simplify and reduce code 

---

> The code for this project is available in GitHub - I’m using a branch for each part and merging each part into the **[latest](https://github.com/prule/data-processing-experiment/tree/latest)** branch. See the ReadMe.md in each branch for the story.
>
> - [Github repository for this project](https://github.com/prule/data-processing-experiment/)
> - [Pull requests for each part](https://github.com/prule/data-processing-experiment/pulls?q=is%3Apr+is%3Aclosed) 
> - [Branch for part-10](https://github.com/prule/data-processing-experiment/tree/part-10)

---

In its current state the framework can:

- load tables from configuration
  - trim, 
  - handle multiple formats
  - handle multiple column names
- select only configured columns
- convert to types
- remove invalid rows
- deduplicate
- generate statistics
- apply a pipeline of tasks
  - join
  - union
  - map values
  - add literal columns
  - write output

The framework is extensible so more types, statistics and tasks can easily be added as future requirements evolve.

The configuration for the reference application be seen here:
- [tables](https://github.com/prule/data-processing-experiment/tree/latest/app/src/main/resources/sample1.tables.json5)
- [statistics](https://github.com/prule/data-processing-experiment/tree/latest/app/src/main/resources/sample1.statistics.json5)
- [pipeline](https://github.com/prule/data-processing-experiment/tree/latest/app/src/main/resources/sample1.pipeline.json5)

Here's the [output](https://github.com/prule/data-processing-experiment/tree/latest/app) from the [reference application](https://github.com/prule/data-processing-experiment/tree/latest/app/src/main/kotlin/com/example/dataprocessingexperiment/app/App.kt) for each stage for comparison.

This week I haven't been able to add any new features, but I have done some clean up and refactoring. Since discovering how to use kotlin polymorphic serialization for the pipeline work, I now have an appreciation of how much it can simplify the codebase. Accordingly I've modified the table configuration to use this so it directly instantiates types instead of creating a generic type definition which then has to be transformed into the type - reducing code and complexity...

Next week I'm going to experiment with Notebooks to implement similar functionality...

Some options I hope to look into over the next couple of weeks are:

- [Databricks community edition](https://community.cloud.databricks.com)
- [Google Colab](https://colab.research.google.com)
- [Intellij Jupyter Notebook](https://plugins.jetbrains.com/plugin/22814-jupyter)
- [JetBrains DataLore](https://www.jetbrains.com/datalore/)
