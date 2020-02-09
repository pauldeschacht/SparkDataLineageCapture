package io.nomad47

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class DataLineageQueryExecutionListener[T](handler: DataLineageHandler[T]) extends QueryExecutionListener {
  override def onSuccess(functionName: String, qe: QueryExecution, duration: Long) : Unit = {
    handler.onSuccess(functionName, qe, duration)
  }
  override def onFailure(functionName: String, qe: QueryExecution, ex : Exception) : Unit  = {
    handler.onFailure(functionName, qe, ex)
  }
}