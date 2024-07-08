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
package au.csiro.pathling.library.io.source;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.query.AggregateQuery;
import au.csiro.pathling.library.query.ExtractQuery;
import au.csiro.pathling.library.query.FhirViewQuery;
import jakarta.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A FHIR data source that can be queried using the aggregate and extract operations, and can also
 * be written out to a data sink.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public interface QueryableDataSource extends DataSource {

  /**
   * @return a builder capable of writing this data source using various methods
   */
  @Nonnull
  DataSinkBuilder write();

  /**
   * @param subjectResource the subject resource type
   * @return a query builder for the aggregate operation
   */
  @Nonnull
  AggregateQuery aggregate(@Nullable ResourceType subjectResource);

  /**
   * @param subjectResource the subject resource type
   * @return a query builder for the extract operation
   */
  @Nonnull
  ExtractQuery extract(@Nullable ResourceType subjectResource);

  /**
   * @param subjectResource the subject resource type
   * @return a query builder for the view operation
   */
  @Nonnull
  FhirViewQuery view(@Nullable ResourceType subjectResource);

  /**
   * @param subjectResource the subject resource code
   * @return a query builder for the aggregate operation
   */
  @Nonnull
  AggregateQuery aggregate(@Nullable String subjectResource);

  /**
   * @param subjectResource the subject resource code
   * @return a query builder for the extract operation
   */
  @Nonnull
  ExtractQuery extract(@Nullable String subjectResource);

  /**
   * @param subjectResource the subject resource code
   * @return a query builder for the view operation
   */
  @Nonnull
  FhirViewQuery view(@Nullable String subjectResource);

}
