package com.example.dataprocessingexperiment.app

import com.example.dataprocessingexperiment.spark.data.types.*
import com.example.dataprocessingexperiment.spark.statistics.*
import com.example.dataprocessingexperiment.tables.ColumnType
import com.example.dataprocessingexperiment.tables.statistics.StatisticDefinition
import io.github.xn32.json5k.Json5
import kotlinx.serialization.modules.SerializersModule

class DefaultJsonSerializer {

    fun tableModule(): Json5 {
        val tablesModule = SerializersModule {
            polymorphic(ColumnType::class, BooleanType::class, BooleanType.serializer())
            polymorphic(ColumnType::class, StringType::class, StringType.serializer())
            polymorphic(ColumnType::class, DateType::class, DateType.serializer())
            polymorphic(ColumnType::class, IntegerType::class, IntegerType.serializer())
            polymorphic(ColumnType::class, DecimalType::class, DecimalType.serializer())
            polymorphic(ColumnType::class, NoOpType::class, NoOpType.serializer())
        }
        return Json5 { serializersModule = tablesModule }
    }

    fun statisticsModule(): Json5 {
        val statisticsModule = SerializersModule {
            polymorphic(StatisticDefinition::class, Bounds::class, Bounds.serializer())
            polymorphic(StatisticDefinition::class, ColCount::class, ColCount.serializer())
            polymorphic(StatisticDefinition::class, CountByMonth::class, CountByMonth.serializer())
            polymorphic(StatisticDefinition::class, CountByValue::class, CountByValue.serializer())
            polymorphic(StatisticDefinition::class, DuplicateCount::class, DuplicateCount.serializer())
            polymorphic(StatisticDefinition::class, Maximum::class, Maximum.serializer())
            polymorphic(StatisticDefinition::class, Minimum::class, Minimum.serializer())
            polymorphic(StatisticDefinition::class, RowCount::class, RowCount.serializer())
            polymorphic(StatisticDefinition::class, EmptyCount::class, EmptyCount.serializer())
            polymorphic(StatisticDefinition::class, Summary::class, Summary.serializer())
        }
        return Json5 { serializersModule = statisticsModule }
    }
}