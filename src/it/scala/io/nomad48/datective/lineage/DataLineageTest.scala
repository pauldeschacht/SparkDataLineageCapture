package io.nomad48.datective.lineage

import io.nomad48.datective.embedded.EmbeddedHdfsSpark
import org.apache.commons.lang3.SystemUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class DataLineageTest
    extends AnyFunSuite
    with BeforeAndAfterAll
    with EmbeddedHdfsSpark {

  override def beforeAll(): Unit = {
    if (SystemUtils.IS_OS_WINDOWS) {
      // on Windows, use a tmp folder without spaces
      startHdfs(new File("c:\\tmp"))
    }
    copyFromLocal(
      "src/it/resources/insurance_sample.csv.gz",
      "/insurance_sample.csv.gz"
    )
  }

  override def afterAll(): Unit = {
    stopHdfs()
  }

  test("run Spark on insurance file and get the logged lineage") {
    getSpark().listenerManager.register(
      new DataLineageQueryExecutionListener()
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

    // executing this count will materialize "allLines"
    assert(allLines.count == 1730)

    val wood =
      allLines.filter("construction = 'Wood'").select("statecode", "policyId")

    wood.createOrReplaceTempView("woodview")
    val countWood = getSpark().sql("SELECT COUNT(*) FROM woodview")

    // executing this collect will materialize "countWood"
    val countResult = countWood.collect()
    assert(countResult.length == 1)
    assert(countResult.head.getLong(0) == 1212)
  }
}
