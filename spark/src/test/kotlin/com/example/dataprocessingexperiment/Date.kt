package com.example.dataprocessingexperiment

import java.sql.Date
import java.time.LocalDate
import java.time.ZoneId

class Date(
    val year: Int,
    val month: Int,
    val dayOfMonth: Int
) {
    fun toSqlDate(): Date {
        return Date(LocalDate.of(year, month, dayOfMonth).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli())
    }
}