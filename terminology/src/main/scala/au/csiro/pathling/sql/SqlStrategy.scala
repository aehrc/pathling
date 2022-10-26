/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
