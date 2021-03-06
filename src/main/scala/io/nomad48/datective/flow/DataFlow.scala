package io.nomad48.datective.flow

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.Random
/*
Under development !
The goal of the DataFlow object is to transform the lineage tree into a sequential structure (pipeline).
The tree is a direct product of the Spark engine, the pipeline is a more functional view on the operations.

At the moment, the DataFlow takes some shortcuts to prove the point and product a DOT program.
 */
abstract class GraphElement(val id: String)
case class Node(override val id: String, name: String, lines: List[String])
    extends GraphElement(id)
case class Relation(
    override val id: String,
    name: String,
    source: String,
    destination: String
) extends GraphElement(id)

object DataFlow {
  implicit val formats: DefaultFormats.type = DefaultFormats

  protected def removePartitionedFilenames(filenames: List[String]): String = {
    filenames
      .map { filename => filename.replaceAll("/part-.*\\.parquet$", "") }
      .distinct
      .mkString(",")
  }
  def doesContainOperation(o: JValue, operation: String): Boolean = {
    o match {
      case JObject(l) =>
        l.exists { field =>
          (field._1.compareTo("op") == 0) && (field._2 match {
            case JString(content) => content.compareTo(operation) == 0
            case _                => false
          })
        }
      case _ => false
    }
  }
  def getOperation(json: JValue): String = {
    try {
      (json \ "op").extract[String]
    } catch {
      case e: Throwable =>
        println(s"Unable to extract op from ${render(json)}")
        throw e
    }
  }
  def transformType(json: JValue, key: String = "type"): String = {
    json \ key match {
      case s: JString => s.values
      case o: JObject =>
        val strInnerType: String = (o \ "type").extract[String]
        strInnerType match {
          case "array" =>
            val str = transformType(o, "elementType")
            List("[", str, "]").mkString
          case "struct" =>
            val innerFields: List[String] = transformSchema(o)
            (List("\\{") ++ innerFields ++ List("\\}")).mkString("\\n")
          case _ => "unknown inner type"
        }
      case _ => "unknown type"
    }
  }
  def transformSchema(json: JValue): List[String] = {
    json \ "fields" match {
      case fields: JArray =>
        fields.arr.map { el =>
          {
            val fieldName: String = (el \ "name").extract[String]
            val typeName: String = transformType(el)
            s"$fieldName:$typeName"
          }
        }
      case _ =>
        throw new Exception(s"fields is not defined in ${compact(json)}")
    }
  }
  def transform(lineage: JValue): List[GraphElement] = {
    val r = new Random()
    def randomString: String = {
      r.alphanumeric.take(6).mkString
    }
    transform(lineage, randomString)
  }
  def transform(
      lineage: JValue,
      randomString: => String
  ): List[GraphElement] = {
    def transformRecur(json: JValue): List[GraphElement] = {
      getOperation(json) match {
        case "hadoopWrite" =>
          val id: String = randomString
          val node =
            Node(id, "HadoopWrite", List((json \ "output").extract[String]))
          val childGraph: List[GraphElement] =
            transformRecur(json \ "child")
          val childId = childGraph.head.id
          val relation = Relation("jo", "write", childId, id)
          node :: relation :: childGraph

        case "hadoopRead" =>
          val id: String = randomString
          val schemaString: List[String] = transformSchema(json \ "schema")
          val files: List[String] = json \ "location" match {
            case a: JArray =>
              a.arr.map { el => el.extract[String] }
            case _ =>
              throw new Exception("Expect an array for the HadoopRead location")
          }
          val singleEntry = removePartitionedFilenames(files)
          val node =
            Node(
              id,
              "HadoopRead",
              List(singleEntry) ++ schemaString
            )
          node :: Nil
        case "project" =>
          val id: String = randomString
          val columns: List[String] = transformSchema(json)
          val node = Node(id, "Project", columns)
          val childGraph = transformRecur(json \ "child")
          val childId = childGraph.head.id
          val relation = Relation(id, "to", childId, id)
          node :: relation :: childGraph

        case "filter" =>
          val id: String = randomString
          val node = Node(
            id,
            "Filter",
            List((json \ "condition" \ "sql").extract[String])
          )
          val subGraph = transformRecur(json \ "child")
          val childId = subGraph.head.id
          val relation = Relation(id, "filter", childId, id)
          node :: relation :: subGraph

        case "relation" =>
          transformRecur(json \ "relation")

        case "view" =>
          val id: String = randomString
          val node = Node(id, "View", List((json \ "table").extract[String]))
          //val childGraph: List[GraphElement] =
          transformRecur(json \ "child") match {
            case Nil => Nil
            case childGraph =>
              val childId = childGraph.head.id
              val relation = Relation("output", "write", childId, id)
              node :: relation :: childGraph
          }

        case "join" =>
          val id: String = randomString
          val node = Node(id, "Join", List("condition"))
          val left: List[GraphElement] = transformRecur(json \ "left") match {
            case Nil => Nil
            case childGraph =>
              val childId = childGraph.head.id
              val relation = Relation(id, "join", childId, id)
              relation :: childGraph
          }
          val right: List[GraphElement] = transformRecur(json \ "right") match {
            case Nil => Nil
            case childGraph =>
              val childId = childGraph.head.id
              val relation = Relation(id, "join", childId, id)
              relation :: childGraph
          }
          node :: (left ++ right)

        case _ =>
          println(s"Unhandled operation in ${pretty(json)}")
          val id: String = randomString
          Node(id, "Unhandled operation", List(getOperation(json))) :: Nil
      }
    }
    transformRecur(lineage \ "lineage")
  }
}
