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

package au.csiro.pathling.library.query;

import au.csiro.pathling.extract.ExtractRequest;
import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.utilities.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents an extract query.
 *
 * @author Piotr Szul
 */
public class ExtractQuery extends AbstractQueryWithFilters<ExtractQuery> {

  @Nonnull
  private final List<ExpressionWithLabel> columnsWithLabels = new ArrayList<>();

  private ExtractQuery(@Nonnull final ResourceType subjectResource) {
    super(subjectResource);
  }

  @Nonnull
  @Override
  protected Dataset<Row> doExecute(@Nonnull final QueryExecutor queryExecutor) {
    return queryExecutor.execute(buildRequest());
  }

  /**
   * Adds a FHIRPath expression that represents a column to be extract in the result.
   *
   * @param columnFhirpath the column expressions
   * @return this query
   */
  @Nonnull
  public ExtractQuery withColumn(@Nonnull final String columnFhirpath) {
    columnsWithLabels.add(ExpressionWithLabel.withExpressionAsLabel(columnFhirpath));
    return this;
  }

  /**
   * Adds a FHIRPath expression that represents a column to be extract in the result with the
   * explict label.
   *
   * @param columnFhirpath the column expressions
   * @param label the label of the column
   * @return this query
   */
  @Nonnull
  public ExtractQuery withColumn(@Nonnull final String columnFhirpath,
      @Nonnull final String label) {
    columnsWithLabels.add(ExpressionWithLabel.of(columnFhirpath, label));
    return this;
  }

  /**
   * Construct a new extract query instance for the given subject resource type.
   *
   * @param subjectResourceType the type of the subject resource
   * @return the new instance of (unbound) extract query
   */
  @Nonnull
  public static ExtractQuery of(@Nonnull final ResourceType subjectResourceType) {
    return new ExtractQuery(subjectResourceType);
  }

  @Nonnull
  private ExtractRequest buildRequest() {
    return new ExtractRequest(subjectResource,
        Lists.normalizeEmpty(columnsWithLabels),
        Lists.normalizeEmpty(filters),
        Optional.empty());
  }
}
