package io.nomad47

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.util.Success
class DataLineageJsonTest extends FunSuite {

  lazy implicit val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("ThothSnapshotTest")
      .getOrCreate()
  }

  val writer = new DataLineageStringWriter
  val dataLineageJsonHandler = new DataLineageJsonHandler(writer)
  spark.listenerManager.register(new DataLineageQueryExecutionListener(dataLineageJsonHandler))

  val filename = s"FL_insurance_sample.csv"
  val resourcesPath = getClass.getResource("/insurance_sample.csv.gz")
  val data = spark.read.option("header", true).csv(resourcesPath.getPath)
  val wood = data.filter("construction = 'Wood'").select("statecode", "policyId")
  // action "count" will generate the logical plan, which is captured and transformed to JSON
  val c = wood.count
  assert(c == 1212)

  import scala.concurrent.ExecutionContext.Implicits.global
  val futureWriter = dataLineageJsonHandler.getWriter()
  futureWriter.onComplete{
    case Success(_) => assert(writer.json.isDefined); println(writer.json.get)
    case _ => assert(false)
  }
}
