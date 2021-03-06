package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation
import org.apache.spark.sql.sources.BaseRelation

object JDBCHelper {
  def isJDBCRelation(p: BaseRelation): Boolean = p match {
    case _: JDBCRelation => true
    case _               => false
  }
}

trait JDBCVisitor[T] {
  def visit(r: BaseRelation): T = r match {
    case r: JDBCRelation => visitJDBCRelation(r)
    case _ =>
      throw new Exception(
        s"Unexpected BaseRelation type $r in the JDBC handler"
      )
  }
  def visitJDBCRelation(relation: JDBCRelation): T
}
