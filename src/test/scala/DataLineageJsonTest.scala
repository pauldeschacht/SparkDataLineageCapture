import org.apache.spark.sql.SparkSession
import org.json4s.JsonAST.{JField, JValue}
import org.scalatest.FunSuite

import scala.io.Source
import scala.util.Success
class DataLineageJsonTest extends FunSuite {

  lazy implicit val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("DataLineage Test")
      .getOrCreate()
  }

  val writer = new DataLineageJValueWriter
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
    case Success(_) => {
      assert(writer.json.isDefined)
      val constantLineage = writer.json.get.removeField { case(n,v) => (n == "appId" || n == "duration" || n == "user" || n == "location")}

      import org.json4s.jackson.JsonMethods._
      val expected: JValue = parse(Source.fromURL(this.getClass.getResource("/expectedJson.json")).getLines.mkString)
      val constantExpected =expected.removeField { case(n,v) => (n == "appId" || n == "duration" || n == "user" || n == "location")}
      assert(constantLineage === constantExpected)
    };
    case _ => assert(false)
  }
}
