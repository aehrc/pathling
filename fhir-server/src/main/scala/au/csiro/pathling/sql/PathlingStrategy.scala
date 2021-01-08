/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._

/**
 * Custom spark sql strategy with additional rules for custom pathling operations.
 */
object PathlingStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {

      case MapWithPartitionPreview(serializer, decoder, deserializer, preview, mapper, child) =>
        MapWithPartitionPreviewExec(deserializer, decoder, serializer.value, preview, mapper, planLater(child)) :: Nil

      case _ => Nil
    }
  }


  /**
   * Inject PathlingStrategy into given spark session.
   *
   * @param session spark session to add PathlingStrategy to
   */
  def setup(session: SparkSession): Unit = {
    if (!session.experimental.extraStrategies.contains(PathlingStrategy)) {
      session.experimental.extraStrategies = Seq(PathlingStrategy) ++ session.experimental.extraStrategies
    }
  }
}
