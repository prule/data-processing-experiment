Enhancing table metadata
-

Now that we have an easy table configuration, the next step is to make it more useful. Here, I'm adding a `required` field to `Column`. 

With this, I can easily remove rows that don't have required fields. 

Super simple, I've added a `valid()` method to `DataFrameBuilder` which returns a dataframe with only valid rows:

```kotlin
    fun valid(): Dataset<Row> {
        // required columns != null conditions
        val requiredColumns: List<Column> = fileSource.table.columns
            .filter { column -> column.required }
            .map { column ->
                // for strings, check for null and empty strings
                if (string.key() == column.type)
                    col(column.name).isNotNull.and(
                        trim(col(column.name)).notEqual(functions.lit(""))
                    )
                else
                // otherwise just check for null
                    col(column.name).isNotNull
            }
        // and all columns together so none of the required columns can be null
        var combined: Column? = null
        requiredColumns.forEach { col ->
            combined = if (combined == null) col else combined!!.and(col)
        }
        // select all columns where the required columns are not null
        return typed().select("*").where(combined)
    }
```

For a string column I'm checking for null or blank (trimming the column and comparing to and empty string). For everything else it's just a null check - remember, if dates don't match the right format, or if a value can't be converted by casting then the result will be NULL.

