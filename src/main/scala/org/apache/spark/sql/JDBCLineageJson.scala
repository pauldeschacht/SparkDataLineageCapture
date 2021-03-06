package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.jdbc.{
  JDBCOptions,
  JDBCRelation
}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.JsonAST.{JField, _}

import java.util.Properties
import scala.collection.JavaConverters.asScalaSetConverter

class JDBCLineageJson extends JDBCVisitor[JValue] {

  def propertiesToFields(props: Properties): List[JField] =
    props
      .keySet()
      .asScala
      .flatMap {
        _ match {
          case key: String =>
            if (key.compareTo("password") == 0)
              Some(JField(key, JString("****")))
            else
              Some(JField(key, JString(props.getProperty(key))))
          case _ => None
        }
      }
      .toList

  def jdbcOptionsToJson(jdbcOptions: JDBCOptions): JObject =
    JObject(propertiesToFields(jdbcOptions.asProperties))

  override def visitJDBCRelation(r: JDBCRelation): JValue = {
    JObject(
      JField("op", JString("JDBC")) ::
        JField("schema", parse(r.schema.json)) ::
        JField("estimatedSize", JInt(r.sizeInBytes)) ::
        JField("jdbcOptions", jdbcOptionsToJson(r.jdbcOptions)) ::
        JField("estimatedSize", JInt(r.sizeInBytes)) ::
        JField("partitions", JInt(r.parts.length)) ::
        Nil
    )
  }
}
