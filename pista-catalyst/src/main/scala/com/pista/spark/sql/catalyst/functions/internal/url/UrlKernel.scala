package com.pista.spark.sql.catalyst.functions.internal.url

import org.apache.spark.unsafe.types.UTF8String

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}

object UrlKernel {
  private val MaxInputLength = 1024 * 1024

  def tryDecode(input: UTF8String): UTF8String = {
    val value = input.toString
    if (value.length > MaxInputLength) return null
    val output = new ByteArrayOutputStream(value.length)
    var index = 0
    while (index < value.length) {
      value.charAt(index) match {
        case '+' =>
          output.write(' ')
          index += 1
        case '%' =>
          if (index + 2 >= value.length) return null
          val high = Character.digit(value.charAt(index + 1), 16)
          val low = Character.digit(value.charAt(index + 2), 16)
          if (high < 0 || low < 0) return null
          output.write((high << 4) | low)
          index += 3
        case _ =>
          val start = index
          while (index < value.length && value.charAt(index) != '%' && value.charAt(index) != '+') {
            index += 1
          }
          val encoder = StandardCharsets.UTF_8.newEncoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
          try {
            val encoded = encoder.encode(CharBuffer.wrap(value, start, index))
            val bytes = new Array[Byte](encoded.remaining())
            encoded.get(bytes)
            output.write(bytes)
          } catch { case _: Exception => return null }
      }
    }
    val decoder = StandardCharsets.UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    try UTF8String.fromString(decoder.decode(ByteBuffer.wrap(output.toByteArray)).toString)
    catch { case _: Exception => null }
  }
}
