This is prototyping, far from complete

# SparkDataLineageCapture

The complete data lineage consists of 3 different parts 

1. Capture the logical plan in a structured format and write it to a file
2. Offline process will analyze the logical plans and will construct a sequence of inputs, transformations and outputs
3. Visualization parts draws the sequence of data and transformations

## Capture 

The library registers a QueryExectionEventHandler which converts Spark's query exection event in a JSON format. This data is saved in a HDFS file

The following snippet how the big lines 

```scala
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
```