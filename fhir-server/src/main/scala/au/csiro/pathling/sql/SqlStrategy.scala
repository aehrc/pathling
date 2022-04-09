/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._

/**
 * Custom spark SQL strategy with additional rules for custom Pathling operations.
 */
object SqlStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {

      case MapWithPartitionPreview(serializer, decoder, deserializer, preview, mapper, child) =>
        MapWithPartitionPreviewExec(deserializer, decoder, serializer.value, preview, mapper, planLater(child)) :: Nil

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
      session.experimental.extraStrategies = Seq(SqlStrategy) ++ session.experimental.extraStrategies
    }
  }
}
