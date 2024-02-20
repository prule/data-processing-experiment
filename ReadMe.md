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

An interesting thought comes to mind now - should the type converters contain more logic to enforce validity? Or should this be a separate concern?

For example, IntegerType could take configuration to supply a min and/or max value
- with this it could return null for any values that fall outside the defined range. 
- and then, these invalid rows would be filtered from our valid dataframe

I'm not sure about this yet, so I think I'll defer this decision. With more information and experience options will shake out. Part of this experiment is about knowing when to go broad, and when to expand detail... the beauty of it is, at the moment, making changes is easy - the codebase is simple, flexible, unobtrusive so many things are possible.

The downside is that without specific use-cases the validity of the experiment is hard to judge. I'm not too concerned here either. It's unrealistic to build a silver bullet - there will be some cases where this will add value and be fit for purpose and there will be some that won't. 

In the future I intend to apply this framework to different datasets (different in shape and in size) and see how it pans out... 