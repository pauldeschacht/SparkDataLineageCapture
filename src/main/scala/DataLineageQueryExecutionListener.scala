package io.nomad47

import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.util.QueryExecutionListener
import org.json4s.JsonAST._

class DataLineageQueryExecutionListener extends QueryExecutionListener {
  val process : DataLineageJson = new DataLineageJson()

  override def onSuccess(functionName: String, qe: QueryExecution, duration: Long) : Unit = {
    println(s"onSuccess function ${functionName}")
    if (functionName != "head") {
      /*
    qe.debug.codegen()
    println("==============")
    println(s"Schema executed plan : ${qe.executedPlan.schemaString}")
    println(s"Input set")
    qe.executedPlan.inputSet.map( a => println(a.toJSON))
    println(s"Output set")
    qe.executedPlan.outputSet.map( a => println(a.toJSON))
    println(s"Expressions")
    qe.executedPlan.expressions.map( e => println(e.toString))
    println("==============")
    println(qe.executedPlan.verboseString)
    process.visit(qe.analyzed)
    println("==============")
    println(qe.analyzed.toJSON)
    println("==============")
    println(qe.executedPlan.toJSON)
    println("==============")
    */
      // TODO: put in Future for async processing
      val lineage = process.visit(qe.analyzed)
      val result = JObject(
        JField("user", JString(qe.sparkSession.sparkContext.sparkUser)) ::
          JField("appName", JString(qe.sparkSession.sparkContext.appName)) ::
          JField("appId", JString(qe.sparkSession.sparkContext.applicationId)) ::
          JField("appAttemptId", JString(qe.sparkSession.sparkContext.applicationAttemptId match {
            case Some(name) => name
            case _ => ""
          })) ::
          JField("lineage", lineage) ::
          Nil
      )
      // TODO: save to HDFS
      println(org.json4s.jackson.compactJson(result))
    }
  }
  override def onFailure(functionName: String, qe: QueryExecution, ex : Exception) : Unit  = {
    println(s"onFailure function ${functionName}")
  }
}