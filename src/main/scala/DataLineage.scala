import org.apache.spark.sql.catalyst.expressions.{BinaryOperator, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.streaming.ConsoleRelation
import org.apache.spark.sql.sources.BaseRelation

import scala.concurrent.{ExecutionContext, Future}

/**
 * The trait DataLineageHandler defines the QueryExecutionListener functions
 */
trait DataLineageHandler {
  /**
   * A callback function that is called when the query is executed successfully
   * @param functionName: name of the function that triggered the execution of the query
   * @param qe: instance of the QueryExecution that contains the logical plan
   * @param duration: execution time for this query
   */
  def onSuccess(functionName: String, qe: QueryExecution, duration: Long): Unit
  def onFailure(functionName: String, qe: QueryExecution, ex : Exception): Unit
}
/**
 * The trait DataLineageWriter defines the function that implements the final processing of the transformed logical plan.
 * The function is defined as a Future to handle the common case of writing to HDFS
 * @tparam T: transformed logical plan
 */
trait DataLineageWriter[T] {
  def write(lineage: T)(implicit ec: ExecutionContext) : Future[Unit]
}
/**
 * The trait RelationVisitor defines the visitor functions for the different relations
 * Relations represent data with schema's: https://spark.apache.org/docs/2.3.0/api/scala/index.html#org.apache.spark.sql.sources.BaseRelation
 * @tparam T
 */
trait RelationVisitor[T] {
  def visit(r: BaseRelation) : T = r match {
    case r : ConsoleRelation => visitConsoleRelation(r)
    case r : HadoopFsRelation => visitHadoopFsRelation(r)
    //case r : JDBCRelation => visitJDBCRelation(r)  // defined as private
    case _ => default(r)
  }
  def visitConsoleRelation(consoleRelation : ConsoleRelation) : T
  def visitHadoopFsRelation(relation: HadoopFsRelation) : T
  def default(relation : BaseRelation) : T
}
/**
 * The trait ExpressionVisitor defines the visitor functions for the different types of Expressions
 * Relations represent data with schema's: https://spark.apache.org/docs/2.3.0/api/scala/index.html#org.apache.spark.sql.sources.BaseRelation
 * @tparam T
 */
trait ExpressionVisitor[T] {
  def visit(e: Expression) : T = e match {
    case e: Literal => visitLiteral(e)
    case e: NamedExpression => visitNamedExpression(e)
    case e: Count => visitCount(e)
    case e: AggregateExpression => visitAggregateExpression(e)
    case e: BinaryOperator => visitBinaryOperator(e)
    case e : Expression => default(e)
  }
  def visitLiteral(literal: Literal) : T
  def visitNamedExpression(expression: NamedExpression) : T
  def visitCount(count: Count) : T
  def visitAggregateExpression(expression: AggregateExpression) : T
  def visitBinaryOperator(operator: BinaryOperator) : T
  def default(e: Expression) : T
}
/**
 * The trait DataLineageLogicalPlanVisitor defines the visitor functions for the logical plan
 * @tparam T
 */
trait DataLineageLogicalPlanVisitor[T]  {
  def visit(p: LogicalPlan) : T = p match {
    case p: Aggregate => visitAggregate(p)
    case p: Distinct => visitDistinct(p)
    case p: Except => visitExcept(p)
    case p: Expand => visitExpand(p)
    case p: Filter => visitFilter(p)
    case p: Generate => visitGenerate(p)
    case p: GlobalLimit => visitGlobalLimit(p)
    case p: Intersect => visitIntersect(p)
    case p: Join => visitJoin(p)
    case p: LocalLimit => visitLocalLimit(p)
    case p: LogicalRelation => visitLogicalRelation(p)
    case p: Pivot => visitPivot(p)
    case p: Project => visitProject(p)
    case p: Repartition => visitRepartition(p)
    case p: RepartitionByExpression => visitRepartitionByExpr(p)
    case p: Sample => visitSample(p)
    case p: ScriptTransformation => visitScriptTransform(p)
    case p: Sort => visitSort(p)
    case p: SubqueryAlias => visitSubqueryAlias(p)
    case p: Union => visitUnion(p)
    case p: Window => visitWindow(p)
    case p: CreateTempViewUsing => visitCreateTempViewUsing(p)
    case p: InsertIntoHadoopFsRelationCommand => visitInsertIntoHadoopFsRelationCommand(p)
    case p: Command => visitCommand(p)
    case p: LogicalPlan => default(p)
  }
  def visitAggregate(aggregate: Aggregate) : T
  def visitDistinct(distinct: Distinct) : T
  def visitExcept(except: Except) : T
  def visitExpand(expand: Expand) : T
  def visitFilter(filter: Filter) : T
  def visitGenerate(generate: Generate) : T
  def visitGlobalLimit(globalLimit: GlobalLimit) : T
  def visitIntersect(intersect: Intersect) : T
  def visitJoin(join: Join) : T
  def visitLocalLimit(localLimit: LocalLimit) : T
  def visitLogicalRelation(relation: LogicalRelation) : T
  def visitPivot(pivot: Pivot) : T
  def visitProject(project: Project) : T
  def visitRepartition(repartitionOperation: RepartitionOperation) : T
  def visitRepartitionByExpr(repartitionByExpression: RepartitionByExpression) : T
  def visitSample(sample: Sample) : T
  def visitScriptTransform(scriptTransformation: ScriptTransformation) : T
  def visitSort(sort : Sort) : T
  def visitSubqueryAlias(alias: SubqueryAlias) : T
  def visitUnion(union: Union) : T
  def visitWindow(window: Window) : T
  def visitCreateViewCommand(createViewCommand: CreateViewCommand) : T
  def visitCreateTempViewUsing(createTempViewUsing: CreateTempViewUsing) : T
  def visitInsertIntoHadoopFsRelationCommand(insertIntoHadoopFsRelationCommand: InsertIntoHadoopFsRelationCommand) : T
  def visitCommand(command: Command) : T
  def default(p : LogicalPlan) : T
}