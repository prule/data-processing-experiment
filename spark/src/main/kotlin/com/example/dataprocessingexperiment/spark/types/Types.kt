package com.example.dataprocessingexperiment.spark.types

class Types {
    private val map: MutableMap<String, Typer> = mutableMapOf()
    private val noop = NoOpType()

    fun add(typer: Typer) {
        map[typer.key()] = typer
    }

    fun get(type: String): Typer {
        return map.getOrDefault(type, noop)
    }

    companion object {
        fun all(): Types {
            val types = Types()
            types.add(DateType()) // this supports multiple configurable date formats
            types.add(DecimalType())
            types.add(BooleanType())
            types.add(IntegerType())
            types.add(NoOpType())
            return types
        }

    }
}