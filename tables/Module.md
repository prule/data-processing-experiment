# Module Data Processing Experiment - Tables

This module is simply a collection of data classes that let us define tables. It is useful both in code and for serializing/de-serialising (eg JSON5).

# Package com.example.dataprocessingexperiment.tables

Serializable classes to define tables.

Using these classes we can define a table as follows:
```json5
{
  name: "sample 1",
  description: "sample 1 description",
  path: "sample1/statements/",
  type: "csv",
  table: {
    name: "transactions",
    description: "account transactions",
    columns: [
      {
        name: "date",
        description: "date of transaction",
        type: "date",
        formats: [
          "yyyy-MM-dd",
          "dd-MM-yyyy"
        ]
      },
      {
        name: "account",
        description: "account",
        type: "string",
      },
      {
        name: "description",
        description: "description",
        type: "string",
      },
      {
        name: "amount",
        description: "amount can be a positive (credit) or negative (debit) number representing dollars and cents",
        type: "decimal",
        formats: [
          "10",
          "2"
        ]
      }
    ]
  }
}
```

All classes have a name (which could be treated as a unique id) and description (which can be used for documentation).