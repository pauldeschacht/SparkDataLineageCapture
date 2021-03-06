package io.nomad48.datective.lineage

import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.spi.LoggingEvent

import scala.collection.mutable.ArrayBuffer

class InMemoryAppender extends AppenderSkeleton {
  private val eventBuffer = new ArrayBuffer[LoggingEvent]()

  @Override
  def close(): Unit = {}

  @Override
  def requiresLayout(): Boolean = false

  @Override
  protected def append(event: LoggingEvent): Unit = synchronized {
    eventBuffer += event
  }

  def getLogResult: List[String] = synchronized {
    eventBuffer.toList.map(_.getRenderedMessage)
  }

}
