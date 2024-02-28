package com.example.dataprocessingexperiment.spark.data.types

import mu.KotlinLogging

/**
 * A convenience class to define what types are available.
 *
 * Use `Types.all()` if you only need to use the provided types, or if you are
 * using custom types create an instance of `Types` add use `add()` to populate.
 */
class Types {
    private val logger = KotlinLogging.logger {}
    private val map: MutableMap<String, Typer> = mutableMapOf()

    fun add(typer: Typer) {
        map[typer.key()] = typer
    }

    fun get(type: String): Typer {
        if (!map.containsKey(type)) {
            logger.warn { "Type $type is not defined and will be defaulted to String" }
        }
        return map.getOrDefault(type, noop)
    }

    companion object {
        private val noop = NoOpType()
        fun all(): Types {
            val types = Types()
            types.add(StringType())
            types.add(DateType()) // this supports multiple configurable date formats
            types.add(DecimalType())
            types.add(BooleanType())
            types.add(IntegerType())
            types.add(noop)
            return types
        }

    }
}