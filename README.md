Prototype, work in progress

# SparkDataLineageCapture

Capture the logical plan in a structured format and write it.

## Abstractions

The class ```DataLineageQueryExecutionListener``` extends the [QueryExecutionListener](https://spark.apache.org/docs/2.3.2/api/scala/index.html#org.apache.spark.sql.util.QueryExecutionListener). The listeners are called when a query execution was executed.

The trait ```DataLineageHandler``` is the handler that implements the Spark QueryExecutionListener callbacks (onSuccess and onFailure).  
One of the arguments the listeners receive is an instance of [QueryExecution](https://github.com/apache/spark/blob/branch-2.3/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala#L42) which contains the [logical plan](https://github.com/apache/spark/blob/branch-2.3/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LogicalPlan.scala).

The trait ```DataLineageLogicalPlanVisitor[T]``` is the abstract visitor to transform a logical plan into a type T. This is derived from the [LogicalPlanVisitor](https://github.com/apache/spark/blob/branch-2.3/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LogicalPlanVisitor.scala) and allows to capture some DDL and HDFS actions as well.

The trait ```DataLineageWriter[T]``` is the contract to write the instance of type T. It allows to persist the transformed logical plan or to hold that data (the QueryExecutionListener callback functions cannot return the transformed data).

## JSON Implementations

The traits have a implementation that transforms the logical plan into JSON. The final result can be returned as a JValue (DataLineageJValueWriter) or can be written to HDFS (DataLineageJsonHDFSWriter). 

## Example 

```scala
/**
* initialize Spark with the DataLineageQueryExecutionListener
* The listener will capture the logical plan and pass it to the DataLineageJsonHandler to transform the logical plan in JSON. 
* Depending on the DataLineageWriter, the result is written to HDFS, or retrieved as JValue.
**/
 val writer = new DataLineageJValueWriter
  val dataLineageJsonHandler = new DataLineageJsonHandler(writer)
  spark.listenerManager.register(new DataLineageQueryExecutionListener(dataLineageJsonHandler))

val data = spark.read.option("header", true).csv(filename)
val result = data.filter("construction = 'Wood'").select("statecode", "policyId")

val futureWriter = dataLineageJsonHandler.getWriter()
  futureWriter.onComplete{
    case Success(_) => {
      match writer.json => {
        Some(lineage) => println(org.json4s.jackson.JsonMethods.compact(lineage))
     }
}

```


