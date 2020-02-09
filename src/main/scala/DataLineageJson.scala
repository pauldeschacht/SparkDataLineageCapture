package io.nomad47

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, InsertIntoHadoopFsRelationCommand, _}
import org.apache.spark.sql.execution.streaming.ConsoleRelation
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.json4s.JsonAST.{JField, _}
import org.json4s.jackson.JsonMethods.parse

import scala.concurrent.{ExecutionContext, Future}

class DataLineageJsonWriter(filename: String) extends DataLineageWriter[JValue] {
  def write(j: JValue)(implicit ec: ExecutionContext) : Future[Unit] = Future {
    val s = org.json4s.jackson.compactJson(j)
    //TODO: write to HDFS: spark.write.format("json").save(filename)
  }
}
class DataLineageStringWriter extends DataLineageWriter[JValue] {
  private var s: Option[String] = None
  def write(j: JValue)(implicit ec: ExecutionContext) : Future[Unit] = Future {
    s = Some(org.json4s.jackson.compactJson(j))
  }
  def json : Option[String] = s
}

class DataLineageJsonHandler(writer: DataLineageWriter[JValue]) extends DataLineageHandler[JValue] {
  private lazy val visitor = new DataLineageJson
  private var futureWriter : Future[Unit] = null

  def onSuccess(functionName: String, qe: QueryExecution, duration: Long) = {
    if (functionName == "count") {
      val lineage = visitor.visit(qe.analyzed)
      val result = JObject(
        JField("user", JString(qe.sparkSession.sparkContext.sparkUser)) ::
          JField("appName", JString(qe.sparkSession.sparkContext.appName)) ::
          JField("appId", JString(qe.sparkSession.sparkContext.applicationId)) ::
          JField("appAttemptId", JString(qe.sparkSession.sparkContext.applicationAttemptId match {
            case Some(name) => name
            case _ => ""
          })) ::
          JField("duration", JInt(duration)) ::
          JField("lineage", lineage) ::
          Nil)
      import scala.concurrent.ExecutionContext.Implicits.global
      futureWriter = writer.write(result)
    }
  }
  def onFailure(functionName: String, qe: QueryExecution, ex : Exception): Unit = {
    //log error s"${functionName}, ex.toString"
  }
  def getWriter(): Future[Unit] = futureWriter
}

class RelationJson extends RelationVisitor[JValue] {
  override def visitHadoopFsRelation(r: HadoopFsRelation): JValue = {
    JObject(
      JField("class" , JString(r.getClass.toString)) ::
      JField("location", JArray(r.location.inputFiles.map( i => JString(i.toString)).toList)) ::
      JField("format", r.fileFormat match {
        case source: DataSourceRegister => JString(source.shortName())
        case _ => JString("HadoopFiles") }) ::
      JField("schema", parse(r.schema.json)) ::
      JField("dataSchema", parse(r.dataSchema.json)) ::
      JField("estimatedSize", JInt(r.sizeInBytes)) ::
      Nil
    )
  }
  override def visitConsoleRelation(r: ConsoleRelation): JValue = {
    JObject(
      JField("class" , JString(r.getClass.toString)) ::
      JField("schema", parse(r.schema.json)) ::
      JField("estimatedSize", JInt(r.sizeInBytes)) ::
      Nil
    )
  }
  override def default(r: BaseRelation): JValue = {
    JObject(
      JField("class" , JString(r.getClass.toString)) ::
      JField("schema", parse(r.schema.json)) ::
      JField("estimatedSize", JInt(r.sizeInBytes)) ::
      Nil
    )
  }
}

class DataLineageJson extends DataLineageLogicalPlanVisitor[JValue] {

  def relationJson = new RelationJson;

  override def visit(p: LogicalPlan): JValue = {
    println(p.getClass.toString)
    super.visit(p)
  }

  def processExpression(e : Expression) : JValue = parse(e.toJSON)
  def processExpression(e : Option[Expression]): JValue = e match {
    case Some(expression) => processExpression(expression)
    case _ => JNull
  }
  def processAttributes(attributes : AttributeSet) : JArray = {
    JArray(attributes.map(attribute => {
      processExpression(attribute) ++ JObject(JField("name", JString(attribute.name)))} ).toList)
  }
  def processAttributes(attributes : Seq[Attribute]) : JArray = {
    JArray(attributes.map(attribute => {
      processExpression(attribute) ++ JObject(JField("name", JString(attribute.name)))} ).toList)
  }
  def processCatalogTable(catalogTable: CatalogTable) : JValue = {
    JObject(
      JField("identifier", JString(catalogTable.identifier.identifier)) ::
        JField("storage", JString(catalogTable.storage.toString)) ::   // FIXME
        JField("owner", JString(catalogTable.owner)) ::   // FIXME
        Nil
    )
  }
  def processCatalogTable(catalogTable: Option[CatalogTable]) : JValue = catalogTable match {
    case Some(table) => processCatalogTable(table)
    case _ => JNull
  }
  override def visitAggregate(aggregate: Aggregate) : JValue = {
    JObject(JField("class", JString(aggregate.getClass.toString)))
  }
  override def visitDistinct(distinct: Distinct) : JValue = {
    JObject(JField("class", JString(distinct.getClass.toString)))
  }
  override def visitExcept(except: Except) : JValue = {
    JObject(JField("class", JString(except.getClass.toString)))
  }
  override def visitExpand(expand: Expand) : JValue = {
    JObject(JField("class", JString(expand.getClass.toString)))
  }
  override def visitFilter(filter: Filter) : JValue = {
    JObject(
      JField("class", JString(filter.getClass.toString)) ::
      JField("condition", processExpression(filter.condition) ) ::
      JField("child" , visit(filter.child)) ::
      Nil)
  }
  override def visitGenerate(generate: Generate) : JValue = {
    JObject(
      JField("class", JString(generate.getClass.toString)) ::
      JField("child", visit(generate.child)) ::
      Nil)
  }
  override def visitGlobalLimit(globalLimit: GlobalLimit) : JValue = {
    JObject(
      JField("class", JString(globalLimit.getClass.toString)) ::
      JField("limit", processExpression(Some(globalLimit.limitExpr))) ::
      JField("child", visit(globalLimit.child)) ::
      Nil
    )
  }
  override def visitIntersect(intersect: Intersect) : JValue = {
    JObject(
      JField("class", JString(intersect.getClass.toString)) ::
      JField("left", visit(intersect.left)) ::
      JField("right", visit(intersect.right)) ::
      Nil
    )
  }
  override def visitJoin(join: Join) : JValue = {
    JObject(
      JField("class", JString(join.getClass.toString)) ::
      JField("left", visit(join.left)) ::
      JField("right", visit(join.right)) ::
      JField("condition", processExpression(join.condition)) ::
      JField("constraints", JArray( join.constraints.map( e => processExpression(e)).toList)) ::
      JField("type" , JString( join.joinType.toString)) ::
        Nil
    )
  }
  override def visitLocalLimit(localLimit: LocalLimit)  : JValue = {
    JObject(
      JField("class", JString(localLimit.getClass.toString)) ::
        JField("limit", processExpression(Some(localLimit.limitExpr))) ::
        JField("child", visit(localLimit.child)) ::
        Nil
    )
  }
  override def visitLogicalRelation(logicalRelation: LogicalRelation) : JValue = {
    JObject(
      JField("class", JString(logicalRelation.getClass.toString)) ::
      JField("streaming", JBool(logicalRelation.isStreaming)) ::
      JField("catalog", processCatalogTable(logicalRelation.catalogTable)) ::
      JField("relation", relationJson.visit(logicalRelation.relation)) ::
    Nil
    )
  }

  override def visitPivot(pivot: Pivot) : JValue = {
    JObject(
      JField("class", JString(pivot.getClass.toString)) ::
      Nil
    )
  }
  override def visitProject(project: Project) : JValue = {
    JObject(
      JField("class", JString(project.getClass.toString)) ::
      JField("child", visit(project.child)) ::
      JField("output", processAttributes(project.output)) ::
      Nil
    )
  }
  override def visitRepartition(repartitionOperation: RepartitionOperation) : JValue = {
    JObject(
      JField("class", JString(repartitionOperation.getClass.toString)) ::
      Nil
    )
  }
  override def visitRepartitionByExpr(repartitionByExpression: RepartitionByExpression) : JValue = {
    JObject(
      JField("class", JString(repartitionByExpression.getClass.toString)) ::
      Nil
    )
  }
  override def visitSample(sample: Sample) : JValue = {
    JObject(
      JField("class", JString(sample.getClass.toString)) ::
      Nil
    )
  }
  override def visitScriptTransform(scriptTransformation: ScriptTransformation) : JValue = {
    JObject(
      JField("class", JString(scriptTransformation.getClass.toString)) ::
      Nil
    )
  }
  override def visitSort(sort: Sort) : JValue = {
    def visitSortOrder(order: SortOrder) : JValue = {
      parse(order.toJSON)
    }
    JObject(
      JField("class", JString(sort.getClass.toString)) ::
      JField("global", JBool(sort.global)) ::
      JField("order", JArray(sort.order.map(visitSortOrder(_)).toList)) ::
      JField("child", visit(sort.child)) ::
      Nil
    )
  }
  override def visitSubqueryAlias(alias: SubqueryAlias) : JValue = {
    JObject(
      JField("class", JString(alias.getClass.toString)) ::
      JField("alias", JString(alias.alias)) ::
      JField("child", visit(alias.child)) ::
      Nil
    )
  }
  override def visitUnion(union: Union) : JValue = {
    JObject(
      JField("class", JString(union.getClass.toString)) ::
      JField("children", JArray(union.children.map(visit).toList)) ::
      Nil
    )
  }
  override def visitWindow(window: Window) : JValue = {
    JObject(
      JField("class", JString(window.getClass.toString)) ::
      Nil
    )
  }
  override def visitCreateViewCommand(createViewCommand: CreateViewCommand) : JValue = {
    JObject(
      JField("class", JString(createViewCommand.getClass.toString)) ::
      JField("table", JString(createViewCommand.name.identifier)) ::
      JField("replace", JBool(createViewCommand.replace)) ::
      JField("child", visit(createViewCommand.child)) ::
      Nil
    )

  }
  override def visitCreateTempViewUsing(createTempViewUsing: CreateTempViewUsing) : JValue = {
    JObject(
      JField("class", JString(createTempViewUsing.getClass.toString)) ::
      Nil
    )
  }
  def visitInsertIntoHadoopFsRelationCommand(insertIntoHadoopFsRelationCommand: InsertIntoHadoopFsRelationCommand) : JValue = {
    JObject(
      JField("class", JString(insertIntoHadoopFsRelationCommand.getClass.toString)) ::
      JField("output", JString(insertIntoHadoopFsRelationCommand.outputPath.toString)) ::
      JField("format", JString(insertIntoHadoopFsRelationCommand.fileFormat.toString)) ::
      JField("mode", JString(insertIntoHadoopFsRelationCommand.mode.toString)) ::
      JField("child", visit(insertIntoHadoopFsRelationCommand.query)) ::
      Nil
    )
  }
  override def visitCommand(command: Command) : JValue = {
    JObject(
      JField("class", JString(command.getClass.toString)) ::
        Nil
    )
  }
  override def default(p : LogicalPlan) : JValue = p match {
    case _ : LogicalPlan => JObject( JField("class", JString(p.getClass.toString)) :: Nil)
  }
}
