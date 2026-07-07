/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology.store;

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ANCESTOR_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DESCENDANT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DIRECT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_DENSE_ID;
import static org.apache.spark.sql.functions.col;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

/**
 * Computes the transitive closure of a concept hierarchy at import time using iterative Spark
 * self-joins (the semi-naive algorithm). The input is the set of active direct is-a edges (child to
 * parent); the output is every ancestor/descendant pair, excluding self-pairs, with a flag marking
 * the pairs that correspond to a direct parent-child edge.
 *
 * <p>Each edge {@code child is-a parent} means {@code parent} is an ancestor of {@code child}. The
 * closure is grown one hop at a time until no new pairs appear, so the number of Spark jobs is
 * proportional to the depth of the hierarchy rather than the number of concepts. All joins carry
 * the system-version key, so versions never mix.
 *
 * @author John Grimes
 */
public class TransitiveClosureBuilder {

  /**
   * Builds the transitive closure of the supplied is-a edges.
   *
   * @param isaEdges active is-a edges with columns {@code system_version_id}, {@code
   *     source_dense_id} (the child), and {@code target_dense_id} (the parent)
   * @return closure rows with columns {@code system_version_id}, {@code ancestor_dense_id}, {@code
   *     descendant_dense_id}, and {@code direct}
   */
  @Nonnull
  public Dataset<Row> build(@Nonnull final Dataset<Row> isaEdges) {
    // A direct edge (child is-a parent) is the ancestor/descendant pair (parent, child).
    final Dataset<Row> edges =
        isaEdges
            .select(
                col(COLUMN_SYSTEM_VERSION_ID),
                col(COLUMN_TARGET_DENSE_ID).alias(COLUMN_ANCESTOR_DENSE_ID),
                col(COLUMN_SOURCE_DENSE_ID).alias(COLUMN_DESCENDANT_DENSE_ID))
            .distinct()
            .transform(TransitiveClosureBuilder::materialiseWithTruncatedLineage);

    Dataset<Row> result = edges;
    Dataset<Row> frontier = edges;
    while (true) {
      // Extend each frontier pair (a -> b) by one edge (b -> c) to reach (a -> c).
      final Dataset<Row> extended =
          frontier
              .as("f")
              .join(
                  edges.as("e"),
                  col("f." + COLUMN_SYSTEM_VERSION_ID)
                      .equalTo(col("e." + COLUMN_SYSTEM_VERSION_ID))
                      .and(
                          col("f." + COLUMN_DESCENDANT_DENSE_ID)
                              .equalTo(col("e." + COLUMN_ANCESTOR_DENSE_ID))))
              .select(
                  col("f." + COLUMN_SYSTEM_VERSION_ID),
                  col("f." + COLUMN_ANCESTOR_DENSE_ID),
                  col("e." + COLUMN_DESCENDANT_DENSE_ID))
              .distinct();

      // Materialising each generation behind a leaf plan keeps the logical plan size constant
      // across iterations; without this, analysis cost grows exponentially with hierarchy depth.
      final Dataset<Row> newPairs = materialiseWithTruncatedLineage(extended.except(result));
      if (newPairs.isEmpty()) {
        break;
      }
      result = result.union(newPairs);
      frontier = newPairs;
    }

    // Flag the pairs that are direct parent-child edges by re-joining against the edge set.
    final Column joinCondition =
        col("c." + COLUMN_SYSTEM_VERSION_ID)
            .equalTo(col("d." + COLUMN_SYSTEM_VERSION_ID))
            .and(col("c." + COLUMN_ANCESTOR_DENSE_ID).equalTo(col("d." + COLUMN_ANCESTOR_DENSE_ID)))
            .and(
                col("c." + COLUMN_DESCENDANT_DENSE_ID)
                    .equalTo(col("d." + COLUMN_DESCENDANT_DENSE_ID)));
    return result
        .as("c")
        .join(edges.as("d"), joinCondition, "left_outer")
        .select(
            col("c." + COLUMN_SYSTEM_VERSION_ID),
            col("c." + COLUMN_ANCESTOR_DENSE_ID),
            col("c." + COLUMN_DESCENDANT_DENSE_ID),
            col("d." + COLUMN_ANCESTOR_DENSE_ID).isNotNull().alias(COLUMN_DIRECT));
  }

  /**
   * Eagerly computes a dataset, caches the rows, and returns a dataset whose logical plan is a leaf
   * over the computed rows. This truncates the Catalyst lineage so that iterative self-referential
   * plans do not grow with each generation.
   *
   * @param dataset the dataset to materialise
   * @return an equivalent cached dataset with a leaf logical plan
   */
  @Nonnull
  private static Dataset<Row> materialiseWithTruncatedLineage(@Nonnull final Dataset<Row> dataset) {
    final Dataset<Row> truncated =
        dataset
            .sparkSession()
            .createDataFrame(dataset.javaRDD(), dataset.schema())
            .persist(StorageLevel.MEMORY_AND_DISK());
    truncated.count();
    return truncated;
  }
}
