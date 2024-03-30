package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.tables.Tables
import io.kotest.matchers.shouldBe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class ValueMappingJoinProcessorTest {
    private val dataHelper = SparkDataHelper(sparkSession, true)

    @Test
    fun `should substitute values`() {
        // prepare
        val tableName = "transactions"
        val columnName = "description"
        
        // mappings
        // converts a description of "burgers" to "burger" in the transactions table

        //        +------------+-----------+-------+------+
        //        |       table|     column|   from|    to|
        //        +------------+-----------+-------+------+
        //        |transactions|description|burgers|burger|
        //        +------------+-----------+-------+------+

        val mappings = dataHelper.asDataFrame(
            listOf(
                GenericRow(arrayOf(tableName, columnName, "burgers", "burger")),
            ),
            listOf(
                Pair("table", DataTypes.StringType),
                Pair("column", DataTypes.StringType),
                Pair("from", DataTypes.StringType),
                Pair("to", DataTypes.StringType),
            )
        )

        // transactions

        //        +----+-----------+----+
        //        |val1|description|val2|
        //        +----+-----------+----+
        //        |   a|    burgers|   1|
        //        |   b|     burger|   2|
        //        |   c|      apple|   3|
        //        |   d|       NULL|   4|
        //        +----+-----------+----+

        val transactions = dataHelper.asDataFrame(
            listOf(
                GenericRow(arrayOf("a", "burgers", "1")),
                GenericRow(arrayOf("a", "burgers", "1")),
                GenericRow(arrayOf("b", "burger", "2")),
                GenericRow(arrayOf("c", "apple", "3")),
                GenericRow(arrayOf("d", null, "4")),
            ),
            listOf(
                Pair("val1", DataTypes.StringType),
                Pair(columnName, DataTypes.StringType),
                Pair("val2", DataTypes.StringType),
            )
        )

        // build context
        val context = SparkContext(Tables("test", "test", "test", listOf()))
        context.set("mappings", mappings)
        context.set(tableName, transactions)

        // perform
        ValueMappingJoinProcessor(
            "id",
            "name",
            "description goes here",
            listOf("mappings"),
            true
        ).process(context)

        // expect result
        //        +----+-----------+----+
        //        |val1|description|val2|
        //        +----+-----------+----+
        //        |   a|     burger|   1|
        //        |   b|     burger|   2|
        //        |   c|      apple|   3|
        //        |   d|       NULL|   4|
        //        +----+-----------+----+

        val expected = dataHelper.asDataFrame(
            listOf(
                GenericRow(arrayOf("a", "burger", "1")),
                GenericRow(arrayOf("b", "burger", "2")),
                GenericRow(arrayOf("c", "apple", "3")),
                GenericRow(arrayOf("d", null, "4")),
            ), listOf(
                Pair("val1", DataTypes.StringType),
                Pair(columnName, DataTypes.StringType),
                Pair("val2", DataTypes.StringType),
            )
        )

        // verify
        context.get(tableName).count() shouldBe 4
        context.get(tableName).except(expected).count() shouldBe 0

        /*
updatedTable.explain(true)

== Parsed Logical Plan ==
'Project ['val1, coalesce('to, 'transactions.description) AS description#35, 'val2]
+- Join LeftOuter, (description#9 = from#2)
   :- SubqueryAlias transactions
   :  +- LocalRelation [val1#8, description#9, val2#10]
   +- SubqueryAlias mapping
      +- Project [from#2, to#3]
         +- Filter (column#1 = description)
            +- Project [from#2, to#3, column#1]
               +- Project [column#1, from#2, to#3]
                  +- Filter (table#0 = transactions)
                     +- Project [column#1, from#2, to#3, table#0]
                        +- LocalRelation [table#0, column#1, from#2, to#3]

== Analyzed Logical Plan ==
val1: string, description: string, val2: string
Project [val1#8, coalesce(to#3, description#9) AS description#35, val2#10]
+- Join LeftOuter, (description#9 = from#2)
   :- SubqueryAlias transactions
   :  +- LocalRelation [val1#8, description#9, val2#10]
   +- SubqueryAlias mapping
      +- Project [from#2, to#3]
         +- Filter (column#1 = description)
            +- Project [from#2, to#3, column#1]
               +- Project [column#1, from#2, to#3]
                  +- Filter (table#0 = transactions)
                     +- Project [column#1, from#2, to#3, table#0]
                        +- LocalRelation [table#0, column#1, from#2, to#3]

== Optimized Logical Plan ==
Project [val1#8, coalesce(to#3, description#9) AS description#35, val2#10]
+- Join LeftOuter, (description#9 = from#2)
   :- LocalRelation [val1#8, description#9, val2#10]
   +- LocalRelation [from#2, to#3]

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [val1#8, coalesce(to#3, description#9) AS description#35, val2#10]
   +- BroadcastHashJoin [description#9], [from#2], LeftOuter, BuildRight, false
      :- LocalTableScan [val1#8, description#9, val2#10]
      +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=22]
         +- LocalTableScan [from#2, to#3]

         */
    }

    companion object {
        private val sparkSessionHelper = SparkSessionHelper()
        private val sparkSession: SparkSession by lazy { sparkSessionHelper.open() }

        @JvmStatic
        @AfterAll
        fun after() {
            sparkSessionHelper.close()
        }
    }

}