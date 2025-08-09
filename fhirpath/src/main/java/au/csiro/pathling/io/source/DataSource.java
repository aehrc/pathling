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
package au.csiro.pathling.io.source;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A FHIR data source that can read data for a particular resource type, and also list the resource
 * types that are available.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public interface DataSource {

  /**
   * Gets the dataset for the specified FHIR resource type.
   *
   * @param resourceCode the code for the FHIR resource type
   * @return the dataset with the resource data
   */
  @Nonnull
  Dataset<Row> read(@Nullable final String resourceCode);

  /**
   * @return the set of resources that are available through this data source
   */
  @Nonnull
  Set<String> getResourceTypes();

}
