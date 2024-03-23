package com.example.dataprocessingexperiment.spark.data

import org.apache.arrow.flatbuf.Bool
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*

/**
 * From https://spark.apache.org/docs/latest/sql-ref-datatypes.html
 */
class SparkDataTypes {

    fun type(dataType: DataType): Types? {
        for (value in Types.entries) {
            if (value.matches(dataType)) {
                return value
            }
        }
        return null
    }
}

enum class Types(private val types: List<DataType>, val nullPredicate: (Column, Boolean) -> Column) {

    NUMERIC(
        listOf(
            DataTypes.ByteType,
            DataTypes.ShortType,
            DataTypes.IntegerType,
            DataTypes.LongType,
            DataTypes.FloatType,
            DataTypes.DoubleType
        ),
        { col: Column, nullable: Boolean -> col.isNull.or(col.isNaN) }
    ),
    STRING(
        listOf(DataTypes.StringType),
        { col: Column, nullable: Boolean ->
            if (nullable) {
                col.isNull
                    .or(
                        col.isNotNull.and(
                            length(trim(col)).equalTo(0).or(
                                lower(trim(col)).equalTo("null")
                            )
                        )
                    )
            } else {
                length(trim(col)).equalTo(0).or(
                    lower(trim(col)).equalTo("null")
                )
            }
        }
    ), ;

    fun matches(dataType: DataType): Boolean {
        return types.contains(dataType)
    }
//    BINARY,
//    BOOLEAN,
//    DATETIME,
//    INTERVAL,
//    COMPLEX
}