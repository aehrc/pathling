package au.csiro.pathling.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSession, Strategy}

/**
 * Custom spark SQL strategy with additional rules for custom Pathling operations.
 */
object SqlStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {

      case MapWithPartitionPreview(serializer, decoder, deserializer, preview, mapper, child) =>
        MapWithPartitionPreviewExec(deserializer, decoder, serializer.value, preview, mapper,
          planLater(child)) :: Nil

      case _ => Nil
    }
  }


  /**
   * Injects SqlStrategy into a given Spark session.
   *
   * @param session Spark session to add SqlStrategy to
   */
  def setup(session: SparkSession): Unit = {
    if (!session.experimental.extraStrategies.contains(SqlStrategy)) {
      session.experimental.extraStrategies = Seq(SqlStrategy) ++ session.experimental
        .extraStrategies
    }
  }
}
