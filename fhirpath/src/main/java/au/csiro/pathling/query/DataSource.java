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
package au.csiro.pathling.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

import javax.annotation.Nonnull;

/**
 * The abstract representation of a data source that can provide datasets for FHIR resources.
 *
 * @author Piotr Szul
 */
public interface DataSource {

  /**
   * Gets the dataset for the specified FHIR resource type.
   *
   * @param resourceType the type of the FHIR resource.
   * @return the dataset with the resource data.
   */
  @Nonnull
  Dataset<Row> read(@Nonnull final ResourceType resourceType);
}