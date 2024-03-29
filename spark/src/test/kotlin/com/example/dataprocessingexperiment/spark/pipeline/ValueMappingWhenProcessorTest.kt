package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.tables.Tables
import io.github.xn32.json5k.Json5
import io.kotest.matchers.shouldBe
import kotlinx.serialization.encodeToString
import org.apache.hadoop.shaded.org.apache.avro.data.Json
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class ValueMappingWhenProcessorTest {
    private val dataHelper = SparkDataHelper(sparkSession, true)

    @Test
    fun `should substitute values`() {
        // prepare
        val tableName = "transactions"
        val columnName = "description"

        val tableMapping = ValueMappingWhenProcessor.TableMapping(
            "transactions",
            true,
            listOf(
                ValueMappingWhenProcessor.ColumnMapping(
                    "description",
                    ValueMappingWhenProcessor.ValueMapping(
                        "burger",
                        listOf("burgers")
                    )
                )
            )
        )

        println(Json5.encodeToString(ValueMappingWhenProcessor.TableMapping.serializer(), tableMapping))

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
        context.set(tableName, transactions)

        println("original")
        context.get(tableName).show()

        // perform
        ValueMappingWhenProcessor(
            "id",
            "name",
            "description",
            listOf(tableMapping),
        ).process(context)

        println("result")
        context.get(tableName).show()

        // expected result
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
        context.get(tableName).except(expected).count() shouldBe 0
        context.get(tableName).schema() shouldBe expected.schema()


/*

result.explain(true)

== Parsed Logical Plan ==
Deduplicate [val1#0, description#19, val2#2]
+- Project [val1#0, CASE WHEN (description#1 = burgers) THEN burger ELSE description#1 END AS description#19, val2#2]
   +- SubqueryAlias transactions
      +- LocalRelation [val1#0, description#1, val2#2]

== Analyzed Logical Plan ==
val1: string, description: string, val2: string
Deduplicate [val1#0, description#19, val2#2]
+- Project [val1#0, CASE WHEN (description#1 = burgers) THEN burger ELSE description#1 END AS description#19, val2#2]
   +- SubqueryAlias transactions
      +- LocalRelation [val1#0, description#1, val2#2]

== Optimized Logical Plan ==
Aggregate [val1#0, description#19, val2#2], [val1#0, description#19, val2#2]
+- LocalRelation [val1#0, description#19, val2#2]

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[val1#0, description#19, val2#2], functions=[], output=[val1#0, description#19, val2#2])
   +- Exchange hashpartitioning(val1#0, description#19, val2#2, 200), ENSURE_REQUIREMENTS, [plan_id=14]
      +- HashAggregate(keys=[val1#0, description#19, val2#2], functions=[], output=[val1#0, description#19, val2#2])
         +- LocalTableScan [val1#0, description#19, val2#2]

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