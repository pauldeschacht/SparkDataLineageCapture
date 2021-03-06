package io.nomad48.datective.lineage

import io.nomad48.datective.embedded.EmbeddedHdfsSpark
import io.nomad48.datective.flow.{DataFlow, FlowToDot, GraphElement}
import org.apache.commons.lang3.SystemUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j.Logger
import org.json4s.JValue
import org.json4s.jackson.JsonMethods._

import java.io.File
import scala.util.Random

class DataLineageTest
    extends AnyFunSuite
    with BeforeAndAfterAll
    with EmbeddedHdfsSpark {

  val r = new Random(1000)
  def pseudoRandomString: String = {
    r.alphanumeric.take(6).mkString
  }

  override def beforeAll(): Unit = {
    if (SystemUtils.IS_OS_WINDOWS)
      // on Windows, use a tmp folder without spaces
      startHdfs(new File("c:\\tmp"))
    else
      startHdfs()
    copyFromLocal(
      "src/it/resources/insurance_sample.csv.gz",
      "/insurance_sample.csv.gz"
    )
    copyFromLocal("src/it/resources/characters.json", "/characters.json")
    copyFromLocal("src/it/resources/locations.json", "/locations.json")
  }

  override def afterAll(): Unit = {
    stopHdfs()
  }

  test("run Spark on insurance file and get the logged lineage") {
    val inMemoryAppender: InMemoryAppender = new InMemoryAppender

    val listener: DataLineageQueryExecutionListener =
      new DataLineageQueryExecutionListener()
    val log: Logger = Logger.getLogger(listener.getClass)
    log.addAppender(inMemoryAppender)

    getSpark().listenerManager.register(
      listener
    )

    val allLines: DataFrame = getSpark().read
      .format("csv")
      .options(
        Map(
          "header" -> "true",
          "compression" -> "gzip",
          "inferSchema" -> "true"
        )
      )
      .load("/insurance_sample.csv.gz")
    assert(allLines.count == 1730)

    val wood =
      allLines.filter("construction = 'Wood'").select("statecode", "policyId")

    wood.createOrReplaceTempView("woodview")
    val countWood = getSpark().sql("SELECT COUNT(*) FROM woodview")

    // executing this collect will materialize "countWood"
    val countResult = countWood.collect()
    assert(countResult.length == 1)
    assert(countResult.head.getLong(0) == 1212)

    wood.write.format("parquet").mode("Overwrite").save("/wood.parquet")

    // assumes the level is INFO and not TRACE
    val lastLog: String = inMemoryAppender.getLogResult.last
    val expectedLog: String =
      """{"user":"Paul De Schacht","appName":"PrepareForUploadTest","appId":"local-1615055943219","appAttemptId":"","duration":1445103100,"lineage":{"op":"hadoopWrite","output":"hdfs://localhost:9000/wood.parquet","format":"Parquet","mode":"Overwrite","child":{"op":"project","fields":[{"name":"statecode","type":"StringType"},{"name":"policyId","type":"IntegerType"}],"child":{"op":"filter","condition":{"op":"=","type":"BooleanType","sql":"(`construction` = 'Wood')","left":{"op":"namedExpression","alias":"construction","type":"StringType","sql":"`construction`"},"right":{"op":"literal","type":"StringType","value":"Wood","sql":"'Wood'"}},"child":{"op":"relation","class":"class org.apache.spark.sql.execution.datasources.LogicalRelation","streaming":false,"catalog":null,"relation":{"op":"hadoopRead","location":["hdfs://localhost:9000/insurance_sample.csv.gz"],"format":"csv","schema":{"type":"struct","fields":[{"name":"policyID","type":"integer","nullable":true,"metadata":{}},{"name":"statecode","type":"string","nullable":true,"metadata":{}},{"name":"county","type":"string","nullable":true,"metadata":{}},{"name":"eq_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"hu_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"fl_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"fr_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"tiv_2011","type":"double","nullable":true,"metadata":{}},{"name":"tiv_2012","type":"double","nullable":true,"metadata":{}},{"name":"eq_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"hu_site_deductible","type":"double","nullable":true,"metadata":{}},{"name":"fl_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"fr_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"point_latitude","type":"double","nullable":true,"metadata":{}},{"name":"point_longitude","type":"double","nullable":true,"metadata":{}},{"name":"line","type":"string","nullable":true,"metadata":{}},{"name":"construction","type":"string","nullable":true,"metadata":{}},{"name":"point_granularity","type":"integer","nullable":true,"metadata":{}}]},"dataSchema":{"type":"struct","fields":[{"name":"policyID","type":"integer","nullable":true,"metadata":{}},{"name":"statecode","type":"string","nullable":true,"metadata":{}},{"name":"county","type":"string","nullable":true,"metadata":{}},{"name":"eq_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"hu_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"fl_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"fr_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"tiv_2011","type":"double","nullable":true,"metadata":{}},{"name":"tiv_2012","type":"double","nullable":true,"metadata":{}},{"name":"eq_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"hu_site_deductible","type":"double","nullable":true,"metadata":{}},{"name":"fl_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"fr_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"point_latitude","type":"double","nullable":true,"metadata":{}},{"name":"point_longitude","type":"double","nullable":true,"metadata":{}},{"name":"line","type":"string","nullable":true,"metadata":{}},{"name":"construction","type":"string","nullable":true,"metadata":{}},{"name":"point_granularity","type":"integer","nullable":true,"metadata":{}}]},"estimatedSize":39182}}}}}}"""

    val j1: JValue = parse(lastLog) \ "lineage"
    val j2: JValue = parse(expectedLog) \ "lineage"

    assert(j1 === j2)

  }

  test("RickAndMorty") {

    val inMemoryAppender: InMemoryAppender = new InMemoryAppender
    val listener: DataLineageQueryExecutionListener =
      new DataLineageQueryExecutionListener()

    val log: Logger = Logger.getLogger(listener.getClass)
    log.addAppender(inMemoryAppender)

    getSpark().listenerManager.register(
      listener
    )

    val characters: DataFrame = getSpark().read
      .format("json")
      .options(Map("multiline" -> "true"))
      .load("/characters.json")

    val locations: DataFrame = getSpark().read
      .format("json")
      .options(Map("multiline" -> "true"))
      .load("/locations.json")

    val df = characters
      .join(locations, characters("location.url") === locations("url"))
      .select(
        characters("id").alias("charId"),
        characters("name").alias("charName"),
        characters("location.url").alias("charLocation"),
        locations("id").as("locationId"),
        locations("name").alias("locationName"),
        locations("url").alias("locationUrl")
      )

    df.write.format("parquet").save("/charactersWithLocation.parquet")

    val logs: List[String] = inMemoryAppender.getLogResult

    val graphs: List[GraphElement] =
      DataFlow.transform(
        org.json4s.jackson.JsonMethods.parse(logs.head),
        pseudoRandomString
      )

    val dots: String =
      FlowToDot.graphToDot(graphs)

    val expected: String = """digraph g {
                             |graph [ rankdir = "LR" ];
                             |node [ fontsize = "16"; shape = "ellipse" ];
                             |edge [];
                             |"P54WYD" [ label = "<f0> HadoopWrite | hdfs://localhost:9000/charactersWithLocation.parquet "; shape = "record"; ];
                             |"bZyKV3":<f0> -> "P54WYD":<f0> [ id=jo ];
                             |"bZyKV3" [ label = "<f0> Project | charId:LongType| charName:StringType| charLocation:StringType| locationId:LongType| locationName:StringType| locationUrl:StringType "; shape = "record"; ];
                             |"ULq3yy":<f0> -> "bZyKV3":<f0> [ id=bZyKV3 ];
                             |"ULq3yy" [ label = "<f0> Join | condition "; shape = "record"; ];
                             |"7xsJLQ":<f0> -> "ULq3yy":<f0> [ id=ULq3yy ];
                             |"7xsJLQ" [ label = "<f0> HadoopRead | hdfs://localhost:9000/characters.json| created:string| episode:[string]| gender:string| id:long| image:string| location:\{\nname:string\nurl:string\n\}| name:string| origin:\{\nname:string\nurl:string\n\}| species:string| status:string| type:string| url:string "; shape = "record"; ];
                             |"JwNezk":<f0> -> "ULq3yy":<f0> [ id=ULq3yy ];
                             |"JwNezk" [ label = "<f0> HadoopRead | hdfs://localhost:9000/locations.json| created:string| dimension:string| id:long| name:string| residents:[string]| type:string| url:string "; shape = "record"; ];
                             |}
                             |""".stripMargin

    assert(
      expected
        .replaceAll("[\\n\\r]", "")
        .compare(dots.replaceAll("[\\n\\r]", "")) == 0
    )
  }
}
