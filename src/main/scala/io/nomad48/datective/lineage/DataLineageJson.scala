package io.nomad48.datective.lineage

import org.apache.spark.sql.JDBCLineageJson
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  Count
}
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeSet,
  BinaryOperator,
  Expression,
  Literal,
  NamedExpression,
  SortOrder
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.{
  CreateViewCommand,
  GlobalTempView,
  LocalTempView,
  PersistedView,
  ViewType
}
import org.apache.spark.sql.execution.datasources.{
  CreateTempViewUsing,
  InsertIntoHadoopFsRelationCommand,
  _
}
import org.apache.spark.sql.execution.streaming.ConsoleRelation
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.json4s.JsonAST.{JField, _}
import org.json4s.jackson.JsonMethods.parse

/**
  * Implementation of RelationVisitor to transform the relations into JSON
  */
class RelationJson extends RelationVisitor[JValue] {
  override def visitHadoopFsRelation(r: HadoopFsRelation): JValue = {
    JObject(
      JField("op", JString("HadoopRead")) ::
        JField(
          "location",
          JArray(r.location.inputFiles.map(i => JString(i.toString)).toList)
        ) ::
        JField(
          "format",
          r.fileFormat match {
            case source: DataSourceRegister => JString(source.shortName())
            case _                          => JString("HadoopFiles")
          }
        ) ::
        JField("schema", parse(r.schema.json)) ::
        JField("dataSchema", parse(r.dataSchema.json)) ::
        JField("estimatedSize", JInt(r.sizeInBytes)) ::
        Nil
    )
  }
  override def visitConsoleRelation(r: ConsoleRelation): JValue = {
    JObject(
      JField("op", JString("console")) ::
        JField("schema", parse(r.schema.json)) ::
        JField("estimatedSize", JInt(r.sizeInBytes)) ::
        Nil
    )
  }

  private lazy val jdbcLineageJson = new JDBCLineageJson()

  override def visitJDBCRelation(relation: BaseRelation): JValue = {
    jdbcLineageJson.visit(relation)
  }
  override def default(r: BaseRelation): JValue = {
    JObject(
      JField("op", JString("relation")) ::
        JField("schema", parse(r.schema.json)) ::
        JField("estimatedSize", JInt(r.sizeInBytes)) ::
        Nil
    )
  }
}

/**
  * Implementation of ExpressionVisitor to transform the expressions into JSON
  */
class ExpressionJson extends ExpressionVisitor[JValue] {
  override def visitLiteral(literal: Literal): JValue = {
    JObject(
      JField("op", JString("literal")) ::
        JField("type", JString(literal.dataType.toString())) ::
        JField("value", JString(literal.value.toString)) ::
        Nil
    )
  }
  override def visitNamedExpression(expression: NamedExpression): JValue = {
    JObject(
      JField("alias", JString(expression.qualifiedName)) ::
        JField("type", JString(expression.dataType.toString())) ::
        JField("children", JArray(expression.children.map(visit).toList)) ::
        Nil
    )
  }
  override def visitCount(count: Count): JValue = {
    JObject(
      JField("op", JString("count")) ::
        JField("children", JArray(count.children.map(visit).toList)) ::
        Nil
    )
  }
  override def visitAggregateExpression(
      expression: AggregateExpression
  ): JValue = {
    JObject(
      JField("op", JString("aggregation")) ::
        JField("mode", JString(expression.mode.toString)) ::
        JField("distinct", JBool(expression.isDistinct)) ::
        JField("children", JArray(expression.children.map(visit).toList)) ::
        Nil
    )
  }
  override def visitBinaryOperator(operator: BinaryOperator): JValue = {
    JObject(
      JField("op", JString(operator.sqlOperator)) ::
        JField("left", visit(operator.left)) ::
        JField("right", visit(operator.right)) ::
        Nil
    )
  }
  override def default(e: Expression): JValue = {
    JObject(
      JField("type", JString(e.dataType.toString())) ::
        JField("children", JArray(e.children.map(visit).toList)) ::
        Nil
    )
  }
}

/**
  * Implementation of DataLineageLogicalPlanVisitor to transform the logical plan into JSON
  */
class DataLineageJson extends DataLineageLogicalPlanVisitor[JValue] {
  def relationJson = new RelationJson
  def expressionJson = new ExpressionJson

  private[this] def removeEmpty(value: JValue): JValue = value match {
    case JString("") => JNothing
    case JArray(items) => {
      val items2 = items map removeEmpty
      if (items2.size > 0) JArray(items2) else JNothing
    }
    case JObject(fields) => {
      val lst = fields map { case JField(name, value) =>
        JField(name, removeEmpty(value))
      }
      JObject(
        lst.filter(field =>
          field match {
            case JField(name, JNothing) => false
            case _                      => true
          }
        )
      )
    }
    case oth => oth
  }
  override def visit(p: LogicalPlan): JValue = {
    removeEmpty(super.visit(p))
  }
  def processStructType(s: StructType): JValue = {
    JArray(s.fields.map { field =>
      JObject(
        JField("name", JString(field.name)) ::
          JField("type", JString(field.dataType.catalogString)) ::
          Nil
      )
    }.toList)
  }
  def processAttributes(attributes: AttributeSet): JArray = {
    JArray(
      attributes
        .map(attribute =>
          JObject(
            JField("name", JString(attribute.name)) ::
              JField("type", JString(attribute.dataType.toString())) ::
              Nil
          )
        )
        .toList
    )
  }
  def processAttributes(attributes: Seq[Attribute]): JArray = {
    JArray(
      attributes
        .map(attribute =>
          JObject(
            JField("name", JString(attribute.name)) ::
              JField("type", JString(attribute.dataType.toString())) ::
              Nil
          )
        )
        .toList
    )
  }
  def processCatalogTable(catalogTable: CatalogTable): JValue = {
    JObject(
      JField("identifier", JString(catalogTable.identifier.identifier)) ::
        //TODO: use structure of CatalogStorage instead of single string
        JField("storage", JString(catalogTable.storage.toString)) ::
        JField("owner", JString(catalogTable.owner)) ::
        Nil
    )
  }
  def processCatalogTable(catalogTable: Option[CatalogTable]): JValue =
    catalogTable match {
      case Some(table) => processCatalogTable(table)
      case _           => JNull
    }
  override def visitAggregate(aggregate: Aggregate): JValue = {
    JObject(
      JField("child", visit(aggregate.child)) ::
        JField(
          "groupedBy",
          JArray(aggregate.groupingExpressions.map(expressionJson.visit).toList)
        ) ::
        JField(
          "aggregate",
          JArray(
            aggregate.aggregateExpressions.map(expressionJson.visit).toList
          )
        ) ::
        Nil
    )
  }
  override def visitDistinct(distinct: Distinct): JValue = {
    JObject(JField("op", JString("distinct")))
  }
  override def visitExcept(except: Except): JValue = {
    JObject(
      JField("op", JString("except")) ::
        JField("left", visit(except.left)) ::
        JField("right", visit(except.right)) ::
        Nil
    )
  }
  override def visitExpand(expand: Expand): JValue = {
    JObject(
      JField("op", JString("expand")) ::
        JField("child", visit(expand.child)) ::
        Nil
    )
  }
  override def visitFilter(filter: Filter): JValue = {
    JObject(
      JField("op", JString("filter")) ::
        JField("condition", expressionJson.visit(filter.condition)) ::
        JField("child", visit(filter.child)) ::
        Nil
    )
  }
  override def visitGenerate(generate: Generate): JValue = {
    JObject(
      JField("op", JString("generate")) ::
        JField("child", visit(generate.child)) ::
        Nil
    )
  }
  override def visitGlobalLimit(globalLimit: GlobalLimit): JValue = {
    JObject(
      JField("op", JString("limit")) ::
        JField(
          "limit",
          globalLimit.maxRows match {
            case Some(maxRows) => JInt(maxRows)
            case None          => JNothing
          }
        ) ::
        JField("child", visit(globalLimit.child)) ::
        Nil
    )
  }
  override def visitIntersect(intersect: Intersect): JValue = {
    JObject(
      JField("op", JString("intersect")) ::
        JField("left", visit(intersect.left)) ::
        JField("right", visit(intersect.right)) ::
        Nil
    )
  }
  override def visitJoin(join: Join): JValue = {
    JObject(
      JField("op", JString("join")) ::
        JField("type", JString(join.joinType.toString)) ::
        JField("left", visit(join.left)) ::
        JField("right", visit(join.right)) ::
        JField(
          "condition",
          join.condition match {
            case Some(e) => expressionJson.visit(e)
            case None    => JNothing
          }
        ) ::
        JField(
          "constraints",
          JArray(join.constraints.map(expressionJson.visit).toList)
        ) ::
        Nil
    )
  }
  override def visitLocalLimit(localLimit: LocalLimit): JValue = {
    JObject(
      JField("op", JString("localLimit")) ::
        JField(
          "limitPerPartitions",
          localLimit.maxRowsPerPartition match {
            case Some(rows) => JInt(rows)
            case None       => JNothing
          }
        ) ::
        JField("child", visit(localLimit.child)) ::
        Nil
    )
  }
  override def visitLogicalRelation(
      logicalRelation: LogicalRelation
  ): JValue = {
    JObject(
      JField("class", JString(logicalRelation.getClass.toString)) ::
        JField("streaming", JBool(logicalRelation.isStreaming)) ::
        JField("catalog", processCatalogTable(logicalRelation.catalogTable)) ::
        JField("relation", relationJson.visit(logicalRelation.relation)) ::
        Nil
    )
  }

  override def visitPivot(pivot: Pivot): JValue = {
    JObject(
      JField("op", JString("pivot")) ::
        JField(
          "groupBy",
          JArray(pivot.groupByExprs.map(expressionJson.visit).toList)
        ) ::
        JField("pivotColumn", expressionJson.visit(pivot.pivotColumn)) ::
        JField(
          "pivotValues",
          JArray(
            pivot.pivotValues.map(literal => JString(literal.toString)).toList
          )
        ) ::
        JField(
          "aggregates",
          JArray(pivot.aggregates.map(expressionJson.visit).toList)
        ) ::
        JField("child", visit(pivot.child)) ::
        Nil
    )
  }
  override def visitProject(project: Project): JValue = {
    JObject(
      JField("op", JString("project")) ::
        JField("list", processAttributes(project.output)) ::
        JField("child", visit(project.child)) ::
        Nil
    )
  }
  override def visitRepartition(
      repartitionOperation: RepartitionOperation
  ): JValue = {
    JObject(
      JField("op", JString("repartition")) ::
        JField("numPartitions", JInt(repartitionOperation.numPartitions)) ::
        JField("shuffle", JBool(repartitionOperation.shuffle)) ::
        JField("child", visit(repartitionOperation.child)) ::
        Nil
    )
  }
  override def visitRepartitionByExpr(
      repartitionByExpression: RepartitionByExpression
  ): JValue = {
    JObject(
      JField("op", JString("repartition")) ::
        JField("numPartitions", JInt(repartitionByExpression.numPartitions)) ::
        JField("shuffle", JBool(repartitionByExpression.shuffle)) ::
        JField(
          "partitionBy",
          JArray(
            repartitionByExpression.partitionExpressions
              .map(expressionJson.visit)
              .toList
          )
        ) ::
        JField("child", visit(repartitionByExpression.child)) ::
        Nil
    )
  }
  override def visitSample(sample: Sample): JValue = {
    JObject(
      JField("op", JString("sample")) ::
        Nil
    )
  }
  override def visitScriptTransform(
      scriptTransformation: ScriptTransformation
  ): JValue = {
    JObject(
      JField("class", JString(scriptTransformation.getClass.toString)) ::
        Nil
    )
  }
  def visitSortOrder(order: SortOrder): JValue = {
    parse(order.toJSON)
  }
  override def visitSort(sort: Sort): JValue = {

    JObject(
      JField("op", JString("orderBy")) ::
        JField("global", JBool(sort.global)) ::
        JField("order", JArray(sort.order.map(visitSortOrder).toList)) ::
        JField("child", visit(sort.child)) ::
        Nil
    )
  }
  override def visitSubqueryAlias(alias: SubqueryAlias): JValue = {
    JObject(
      JField("class", JString(alias.getClass.toString)) ::
        JField("alias", JString(alias.alias)) ::
        JField("child", visit(alias.child)) ::
        Nil
    )
  }
  override def visitUnion(union: Union): JValue = {
    JObject(
      JField("op", JString("union")) ::
        JField("children", JArray(union.children.map(visit).toList)) ::
        Nil
    )
  }
  override def visitWindow(window: Window): JValue = {
    JObject(
      JField("op", JString("windows")) ::
        JField(
          "partitionBy",
          JArray(window.partitionSpec.map(expressionJson.visit).toList)
        ) ::
        JField(
          "orderBy",
          JArray(window.orderSpec.map(visitSortOrder).toList)
        ) ::
        JField(
          "over",
          JArray(window.windowExpressions.map(expressionJson.visit).toList)
        ) ::
        JField("child", visit(window.child)) ::
        Nil
    )
  }
  protected def getViewType(viewType: ViewType): String = viewType match {
    case PersistedView  => "PersistedView"
    case LocalTempView  => "LocalTempView"
    case GlobalTempView => "GlobalTempView"
  }
  override def visitCreateViewCommand(
      createViewCommand: CreateViewCommand
  ): JValue = {
    JObject(
      JField("op", JString("view")) ::
        JField("table", JString(createViewCommand.name.identifier)) ::
        JField("replace", JBool(createViewCommand.replace)) ::
        JField("allowExisting", JBool(createViewCommand.allowExisting)) ::
        JField("type", JString(getViewType(createViewCommand.viewType))) ::
        JField("child", visit(createViewCommand.child)) ::
        Nil
    )
  }
  override def visitCreateTempViewUsing(
      createTempViewUsing: CreateTempViewUsing
  ): JValue = {
    JObject(
      JField("op", JString("tempView")) ::
        JField("table", JString(createTempViewUsing.tableIdent.table)) ::
        JField("replace", JBool(createTempViewUsing.replace)) ::
        JField("global", JBool(createTempViewUsing.global)) ::
        JField("schema", processStructType(createTempViewUsing.schema)) ::
        Nil
    )
  }
  def visitInsertIntoHadoopFsRelationCommand(
      insertIntoHadoopFsRelationCommand: InsertIntoHadoopFsRelationCommand
  ): JValue = {
    JObject(
      JField("op", JString("HadoopWrite")) ::
        JField(
          "output",
          JString(insertIntoHadoopFsRelationCommand.outputPath.toString)
        ) ::
        JField(
          "format",
          JString(insertIntoHadoopFsRelationCommand.fileFormat.toString)
        ) ::
        JField(
          "mode",
          JString(insertIntoHadoopFsRelationCommand.mode.toString)
        ) ::
        JField("child", visit(insertIntoHadoopFsRelationCommand.query)) ::
        Nil
    )
  }
  override def visitAnalyisBarrier(analysisBarrier: AnalysisBarrier): JValue = {
    //this node contains only 1 child, which is an analyzed logical plan
    visit(analysisBarrier.child)
  }
  override def visitCommand(command: Command): JValue = {
    JObject(
      JField("op", JString("dll")) ::
        JField("children", JArray(command.children.map(visit).toList)) ::
        Nil
    )
  }
  override def default(p: LogicalPlan): JValue = p match {
    case _: LogicalPlan =>
      JObject(JField("class", JString(p.getClass.toString)) :: Nil)
  }
}
