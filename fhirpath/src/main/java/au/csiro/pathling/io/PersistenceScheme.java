/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.io;

import io.delta.tables.DeltaMergeBuilder;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.util.Set;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.slf4j.Logger;

/**
 * A scheme for the persistence of FHIR resource data to Delta tables.
 *
 * @author John Grimes
 */
public interface PersistenceScheme {

  Logger log = org.slf4j.LoggerFactory.getLogger(PersistenceScheme.class);

  /**
   * Read the Delta table corresponding to the given resource type.
   *
   * @param resourceType the resource type to be read
   * @return the Delta table
   */
  @Nonnull
  DeltaTable read(@Nonnull ResourceType resourceType);

  /**
   * Write the given dataset that contains the given resource type.
   *
   * @param resourceType the resource type to be written
   * @param writer the dataset to be written
   */
  void write(@Nonnull ResourceType resourceType, @Nonnull DataFrameWriter<Row> writer);

  /**
   * Merge the given dataset that contains the given resource type.
   *
   * @param resourceType the resource type to be merged
   * @param merge the merge builder to be executed
   */
  void merge(@Nonnull ResourceType resourceType, @Nonnull DeltaMergeBuilder merge);

  /**
   * Check that the given resource type exists.
   *
   * @param resourceType the resource type to be checked
   * @return true if the resource type exists
   */
  boolean exists(@Nonnull ResourceType resourceType);

  /**
   * Signals to the persistence scheme that the data for the given resource type has changed in a
   * substantive way.
   *
   * @param resourceType the resource type that has changed
   */
  void invalidate(@Nonnull ResourceType resourceType);

  /**
   * @return a set of all the resource types that are currently persisted
   */
  @Nonnull
  Set<ResourceType> list();

  /**
   * Delete the data for the given resource type.
   *
   * @param resourceType the resource type to be deleted
   */
  void delete(@Nonnull ResourceType resourceType);

}
