package io.nomad48.datective.lineage

import io.nomad48.datective.flow.{DataFlow, FlowToDot}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class DataFlowTest extends AnyFunSuite {

  val r: Random = new Random(1000)
  def pseudoRandomString: String = {
    r.alphanumeric.take(6).mkString
  }

  val strLineage: String =
    """
      | {"user":"Paul De Schacht","appName":"PrepareForUploadTest","appId":"local-1611734000341","appAttemptId":"","duration":1036329199,"lineage":{"op":"hadoopWrite","output":"hdfs://localhost:9000/wood.parquet","format":"Parquet","mode":"Overwrite","child":{"op":"project","fields":[{"name":"statecode","type":"StringType"},{"name":"policyId","type":"IntegerType"}],"child":{"op":"filter","condition":{"op":"=","type":"BooleanType","sql":"(`construction` = 'Wood')","left":{"op":"namedExpression","alias":"construction","type":"StringType","sql":"`construction`"},"right":{"op":"literal","type":"StringType","value":"Wood","sql":"'Wood'"}},"child":{"op":"relation","class":"class org.apache.spark.sql.execution.datasources.LogicalRelation","streaming":false,"catalog":null,"relation":{"op":"hadoopRead","location":["hdfs://localhost:9000/insurance_sample.csv.gz"],"format":"csv","schema":{"type":"struct","fields":[{"name":"policyID","type":"integer","nullable":true,"metadata":{}},{"name":"statecode","type":"string","nullable":true,"metadata":{}},{"name":"county","type":"string","nullable":true,"metadata":{}},{"name":"eq_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"hu_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"fl_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"fr_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"tiv_2011","type":"double","nullable":true,"metadata":{}},{"name":"tiv_2012","type":"double","nullable":true,"metadata":{}},{"name":"eq_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"hu_site_deductible","type":"double","nullable":true,"metadata":{}},{"name":"fl_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"fr_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"point_latitude","type":"double","nullable":true,"metadata":{}},{"name":"point_longitude","type":"double","nullable":true,"metadata":{}},{"name":"line","type":"string","nullable":true,"metadata":{}},{"name":"construction","type":"string","nullable":true,"metadata":{}},{"name":"point_granularity","type":"integer","nullable":true,"metadata":{}}]},"dataSchema":{"type":"struct","fields":[{"name":"policyID","type":"integer","nullable":true,"metadata":{}},{"name":"statecode","type":"string","nullable":true,"metadata":{}},{"name":"county","type":"string","nullable":true,"metadata":{}},{"name":"eq_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"hu_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"fl_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"fr_site_limit","type":"double","nullable":true,"metadata":{}},{"name":"tiv_2011","type":"double","nullable":true,"metadata":{}},{"name":"tiv_2012","type":"double","nullable":true,"metadata":{}},{"name":"eq_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"hu_site_deductible","type":"double","nullable":true,"metadata":{}},{"name":"fl_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"fr_site_deductible","type":"integer","nullable":true,"metadata":{}},{"name":"point_latitude","type":"double","nullable":true,"metadata":{}},{"name":"point_longitude","type":"double","nullable":true,"metadata":{}},{"name":"line","type":"string","nullable":true,"metadata":{}},{"name":"construction","type":"string","nullable":true,"metadata":{}},{"name":"point_granularity","type":"integer","nullable":true,"metadata":{}}]},"estimatedSize":39182}}}}}}
      | """.stripMargin

  test("transform lineage to graph") {
    val lineage = parse(strLineage)
    val graph = DataFlow.transform(lineage, pseudoRandomString)
    val dot: String = FlowToDot.graphToDot(graph)
    val expected: String = """digraph g {
                             |graph [ rankdir = "LR" ];
                             |node [ fontsize = "16"; shape = "ellipse" ];
                             |edge [];
                             |"P54WYD" [ label = "<f0> HadoopWrite | hdfs://localhost:9000/wood.parquet "; shape = "record"; ];
                             |"bZyKV3":<f0> -> "P54WYD":<f0> [ id=jo ];
                             |"bZyKV3" [ label = "<f0> Project | statecode:StringType| policyId:IntegerType "; shape = "record"; ];
                             |"ULq3yy":<f0> -> "bZyKV3":<f0> [ id=bZyKV3 ];
                             |"ULq3yy" [ label = "<f0> Filter | (`construction` = 'Wood') "; shape = "record"; ];
                             |"7xsJLQ":<f0> -> "ULq3yy":<f0> [ id=ULq3yy ];
                             |"7xsJLQ" [ label = "<f0> HadoopRead | hdfs://localhost:9000/insurance_sample.csv.gz| policyID:integer| statecode:string| county:string| eq_site_limit:double| hu_site_limit:double| fl_site_limit:double| fr_site_limit:double| tiv_2011:double| tiv_2012:double| eq_site_deductible:integer| hu_site_deductible:double| fl_site_deductible:integer| fr_site_deductible:integer| point_latitude:double| point_longitude:double| line:string| construction:string| point_granularity:integer "; shape = "record"; ];
                             |}
                             |""".stripMargin

    assert(
      expected
        .replaceAll("[\\n\\r]", "")
        .compareTo(dot.replaceAll("[\\n\\r]", "")) == 0
    )
  }
}
