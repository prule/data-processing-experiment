package com.example.dataprocessingexperiment.tables

/**
 *           quotedstring: "\"",
 *           escape: "\"",
 *           multiline: true,
 *           header: false,
 *           delimiter: ";"
 */
class DefaultCsvSourceType(
    val header: Boolean = true,
    val quotedstring: String = "\"",
    val escape: String = "\"",
    val multiline: Boolean = true,
    val delimiter: String = ",",
) {

    fun get(): SourceType {
        return SourceType(
            format = "csv",
            options = mapOf(
                "header" to header.toString(),
                "quotedstring" to quotedstring,
                "delimiter" to delimiter,
                "escape" to escape,
                "multiline" to multiline.toString(),
            )
        )
    }
}