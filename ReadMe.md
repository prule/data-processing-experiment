Data Processing Experiment - Part 8
---
- The one where I experiment with task pipelines


---

> The code for this project is available in GitHub - I’m using a branch for each part and merging each part into the **[latest](https://github.com/prule/data-processing-experiment/tree/latest)** branch. See the ReadMe.md in each branch for the story.
>
> - [Github repository for this project](https://github.com/prule/data-processing-experiment/)
> - [Pull requests for each part](https://github.com/prule/data-processing-experiment/pulls?q=is%3Apr+is%3Aclosed) 
> - [Branch for part-8](https://github.com/prule/data-processing-experiment/tree/part-7)

I'm not confident about what I did in the last part by adding the union field to the source. This won't work for other functionality - what we really need is a list of transforms to perform. These could go on the table source - if it only needs to operate on itself - or it could be separated out into it's own space and run across all tables accessible via the context.

So this has been refactored now. Instead what I have is a PipelineConfiguration which is a list of tasks to perform. In this case I want to:

1. Union the 3 LGA dataframes into one called `lgas`
2. Add a column to the `transactions` dataframe called `type` with the literal value `TRANSACTION`
3. Join `transactions` to `lgas` on `transactions.location=lgas.level_2_name` resulting in a dataframe called `transactionsWithLGAs` and has all the columns from `transactions` plus `lgas.level_1_name`.

```json5
{
  id: "p1",
  name: "name",
  description: "description",
  tasks: [
    {
      type: "com.example.dataprocessingexperiment.tables.pipeline.UnionTaskDefinition",
      id: "t2",
      name: "name",
      description: "description",
      // Union tables "lga-1" and "lga-2" and "lga-3" together into table "lgas"
      destination: "lgas",
      tables: [
        "lga-1",
        "lga-2",
        "lga-3",
      ]
    },
    {
      type: "com.example.dataprocessingexperiment.tables.pipeline.LiteralTaskDefinition",
      id: "id",
      name: "name",
      description: "description",
      table: "transactions",
      // add literal columns with name -> value mapping
      columns: {
        // add a column "type" with literal value "TRANSACTION"
        "type": "TRANSACTION"
      },
    },
    {
      type: "com.example.dataprocessingexperiment.tables.pipeline.JoinTaskDefinition",
      id: "t1",
      name: "name",
      description: "description",
      // join table "transactions" to "lgas" using transactions.location=lgas.level_2_name
      // include columns from t1, add lgas.level_1_name and store this in "transactionsWithLGAs" (we could store as transactions if we want to override the original transactions dataframe with the joined result)
      table1: "transactions",
      table2: "lgas",
      destination: "transactionsWithLGAs",
      // join "on" definition
      on: {
        "location": "level_2_name"
      },
      // include these columns from lgas
      columns: [
        "level_1_name"
      ],
      joinType: "left"
    },
  ]
}
```