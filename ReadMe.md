Externalising the table configuration
====

When data is coming from external sources, I think it would be useful to be able to externalise the configuration so it can be easily changed. It will also let many datasets be read without having to write lots of boiler plate code. 

I want to be able to use a configuration that represents a CSV like the following and have code interpret it and build a spark dataframe containing these columns, typed appropriately:

```
{
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
          "10,2"
        ]
      }
    ]
  }
```

