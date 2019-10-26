package io.nomad47

import org.apache.spark.sql.{SaveMode, SparkSession}
/*
// spark-shell
import org.apache.spark.sql.{SaveMode, SparkSession}
:load "D:\\dev\\SparkDataLineageCapture\\src\\main\\scala\\DataLineageLogicalPlanVisitor.scala"
:load "D:\\dev\\SparkDataLineageCapture\\src\\main\\scala\\DataLineageJson.scala"
:load "D:\\dev\\SparkDataLineageCapture\\src\\main\\scala\\DataLineageQueryExecutionListener.scala"
spark.listenerManager.register(new DataLineageQueryExecutionListener)
val filename = s"D:\\hadoop\\FL_insurance_sample.csv"
val data = spark.read.option("header", true).csv(filename)
val wood = data.filter("construction = 'Wood'").select("statecode", "policyId").withColumnRenamed("statecode", "STATE")
data.createOrReplaceTempView("FL_insurance")
val selection = spark.sql("SELECT * FROM FL_insurance WHERE fr_site_limit > 100000 ORDER BY tiv_2012")
selection.write.mode(SaveMode.Overwrite).csv(s"D:\\hadoop\\test2.csv")
wood.write.mode(SaveMode.Overwrite).csv(s"D:\\hadoop\\test1.csv")
 */
object DataLineageCapture {
  val spark = SparkSession.builder
    .master("local")
    .appName("spark session example")
    .getOrCreate()

  spark.listenerManager.register(new DataLineageQueryExecutionListener)
  val filename = s"D:\\hadoop\\FL_insurance_sample.csv"
  val data = spark.read.option("header", true).csv(filename)
  val wood = data.filter("construction = 'Wood'").select("statecode", "policyId")
  wood.write.mode(SaveMode.Overwrite).csv(s"D:\\hadoop\\test.csv")

  data.printSchema

  data.show(20,false)
}