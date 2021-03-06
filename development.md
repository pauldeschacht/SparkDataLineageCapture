
## 2021-01-27

DataFlow converts the logical plan in a concise INPUTS -> TX -> OUTPUTS for easy visualization of the flow. In simple cases, the TX is an SQL string. It is also possible that the TX is a flow. This recursive definition is then flattened into a pipeline.


The generation of this dataflow involves either 
1. ~~getting details from the QueryExecution (see below)~~
2. building up that information during the visitor phase
3. transforming the JSON text - this can be done in a post processing step by extracting that JSON from the log files


1. QueryExecution

The additional information is rich (in case of toJSON) but is merely a dump of the case classes. It does not give a concise or easy to consume SQL/transformation part.

```
      println("-toString------------------------------")
      println(qe.analyzed.toString())
      println("-toJSON--------------------------------")
      println(qe.analyzed.prettyJson)
      println("-treeString----------------------------")
      println(qe.analyzed.treeString(true, true))
      println("-numberedTreeString--------------------")
      println(qe.analyzed.numberedTreeString)
      println("---------------------------------------")
```

2. Visitor Phase

The visitor generates an object with the same structure as the query. Similar to the internal Spark implementation of sql(): String on Expression, the visitor could generate a "sql string" at each node and each parent assembles the sql part of the children.

3. JSON Text

In contrast to the Visitor phase, this transformation starts from an unstructured JSON representation. This comes either from a log file (read text into JSON), or it comes as a postprocessing in the DataLineage library and dumps the dataflow in a log.

! From an operational standpoint, it is an advantage if both the "raw" lineage and dataflow information already available in the logs.


The problem is that the transformation of a JSON object is cumbersome: For any object, detect the type of node by finding the "op" key and then extracting the operation from that field. If the type of node is determined, then specific actions are executed (for example, extraction of "locations" field").

```json
{
  "op" : "hadoopWrite",
  "location": [ ... ],
  "child" : {
    ...
    "child": {
      "op": "hadoodRead",
      "location": [
        ...
      ]
    }
  }
}
```

It is possible to convert the JSON into case classes, but the idea of using JSON was to simplify the development in case more or less fields are needed (handle different versions Spark..) -although that structure is now encoded in the "getFlow".

```scala

  def getFlow(json): List[JValue] match {

    case "hadoopWrite" => { ... }
    case "hadoopRead" => { ... }
    case "project" => {
      val columnNames = json \\ list
      s"""SELECT ${columnNames}"""
    }
    case " " => {
      s"""WHERE """
    }
  }

```
