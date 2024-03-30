package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import kotlinx.serialization.Serializable
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

/**
 *
 */
@Serializable
class JoinProcessor(
    override val id: String,
    override val name: String,
    override val description: String,
    val table1: String,
    val table2: String,
    val destination: String,
    val joinType: String,
    val on: Map<String, String>,
    val columns: List<String>
) : Processor {

    override fun process(context: SparkContext) {

        // alias the tables so we avoid problems where columns are named the same in both datasets
        val table1 = context.get(this.table1).alias(this.table1)
        val table2 = context.get(this.table2).alias(this.table2)

        // build the "on" condition for the join
        var expr = functions.lit(true)
        on.forEach { (key, value) ->
            expr = expr.and(
                functions.col("${this.table1}.$key").equalTo(functions.col("${this.table2}.$value"))
            )
        }

        val columns: MutableList<Column> = mutableListOf()

        // add all columns from table 1
        table1.columns().forEach {
            columns.add(functions.col("${this.table1}.$it").alias(it))
        }

        // add the desired columns from table 2
        table2.columns().forEach {
            // only bring across the columns specified
            if (this.columns.contains(it)) {
                // if the column name exists in both tables then prefix it with the table id
                if (table1.columns().contains(it)) {
                    columns.add(functions.col("${this.table2}.$it").alias("${this.table2}_$it"))
                } else {
                    // otherwise no need to prefix the column since there is no clash
                    columns.add(functions.col("${this.table2}.$it").alias(it))
                }
            }
        }

        // join and select the columns
        val result = table1.join(table2, expr, joinType)
            .select(*columns.map { it }.toTypedArray())

        // add to context
        context.set(this.destination, result)

    }

    override fun toString(): String {
        return "JoinProcessor(id='$id', name='$name', description='$description', table1='$table1', table2='$table2', destination='$destination', joinType='$joinType', on=$on, columns=$columns)"
    }

}