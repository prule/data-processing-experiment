package com.example.dataprocessingexperiment.spark

import java.io.ByteArrayOutputStream
import java.io.PrintStream

// repurposed from here https://www.tomaszezula.com/streamlining-console-output-verification-with-kotest/

class ConsoleCapture {
    private val originalOut = System.out
    private val originalErr = System.err
    private val outBuffer = ByteArrayOutputStream()
    private val errBuffer = ByteArrayOutputStream()
    val capturedOutput = CapturedOutput(outBuffer, errBuffer)

    fun reset() {
        outBuffer.reset()
        errBuffer.reset()
    }

    fun setup() {
        System.setOut(PrintStream(outBuffer))
        System.setErr(PrintStream(outBuffer))
    }

    fun teardown() {
        System.setOut(originalOut)
        System.setOut(originalErr)
    }

}

class CapturedOutput(
    private val outBuffer: ByteArrayOutputStream,
    private val errBuffer: ByteArrayOutputStream
) {
    fun out(): String = outBuffer.toString()
    fun err(): String = errBuffer.toString()
}