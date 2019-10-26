package io.nomad47

import org.json4s.JsonAST.{JField, _}
import org.json4s.jackson.JsonMethods.parse
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming.ConsoleRelation
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}


// FIXME: avoid the struct --> json --> string --> json  (currently using the parse function)

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
    relationJson.visit(logicalRelation.relation)
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
