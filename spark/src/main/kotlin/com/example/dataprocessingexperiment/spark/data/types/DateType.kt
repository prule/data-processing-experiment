package com.example.dataprocessingexperiment.spark.data.types

import com.example.dataprocessingexperiment.spark.data.functions.Date
import kotlinx.serialization.Serializable
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

/**
 * Converts to a Date, choosing the first non-null value resulting from the parsing of each provided format.
 *
 * The following column definition specifies 2 date formats to use when trying to parse the date.
 * ```
 *       {
 *         name: "date",
 *         description: "date of transaction",
 *         type: "date",
 *         formats: [
 *           "yyyy-MM-dd",
 *           "dd-MM-yyyy"
 *         ]
 *       }
 * ```
 * In this case, values "01-01-2020" and "2020-01-02" will be correctly parsed, whereas "Jan-2000" will result in NULL.
 *
 * @see DateTypeTest
 */
@Serializable
class DateType(val formats: List<String>) : Typer {
    override fun key(): String {
        return "date"
    }

    override fun process(name: String): Column {
        return Date(formats).parse(functions.col(name)).alias(name)
    }
}