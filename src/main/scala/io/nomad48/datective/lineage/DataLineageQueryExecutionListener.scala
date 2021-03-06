package io.nomad48.datective.lineage

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.json4s.JsonAST.{JField, JInt, JObject, JString, JValue}
import com.typesafe.scalalogging.LazyLogging

class DataLineageQueryExecutionListener()
    extends QueryExecutionListener
    with LazyLogging {
  private lazy val visitor = new DataLineageJson
  // actions: https://spark.apache.org/docs/latest/rdd-programming-guide.html
  val lineageFunctions: List[String] = List(
    "collect",
    "command",
    "count",
    "first",
    "take",
    "takeSample",
    "takeOrdered",
    "save",
    "saveAsTextFile",
    "saveAsSequenceFile",
    "saveAsObjectFile",
    "countByKey",
    "foreach"
  )

  def onSuccess(
      functionName: String,
      qe: QueryExecution,
      duration: Long
  ): Unit = {
    val t0 = System.currentTimeMillis()
    if (lineageFunctions.contains(functionName)) {
      qe.executedPlan.collectLeaves()
      lazy val lineage: JValue = visitor.visit(qe.analyzed)
      lazy val result: JValue = JObject(
        JField("user", JString(qe.sparkSession.sparkContext.sparkUser)) ::
          JField("appName", JString(qe.sparkSession.sparkContext.appName)) ::
          JField(
            "appId",
            JString(qe.sparkSession.sparkContext.applicationId)
          ) ::
          JField(
            "appAttemptId",
            JString(qe.sparkSession.sparkContext.applicationAttemptId match {
              case Some(name) => name
              case _          => ""
            })
          ) ::
          JField("duration", JInt(duration)) ::
          JField("lineage", lineage) ::
          Nil
      )
      logger info org.json4s.jackson.compactJson(result)
    } else {
      logger warn s" function $functionName ignored"
    }
    val t1 = System.currentTimeMillis()
    logger trace s"onSuccess duration: ${t1 - t0} ms"
  }
  def onFailure(
      functionName: String,
      qe: QueryExecution,
      ex: Exception
  ): Unit = {
    logger error s"Exception during function $functionName, ${ex.toString}"
    ex.printStackTrace()
  }
}
