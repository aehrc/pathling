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

package au.csiro.pathling.extract;

import au.csiro.pathling.query.ExpressionWithLabel;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
public class ExtractRequestBuilder {

  @Nonnull
  private final ResourceType subjectResource;

  @Nonnull
  private final List<ExpressionWithLabel> columns;

  @Nonnull
  private final List<String> filters;

  @Nullable
  private Integer limit;

  public ExtractRequestBuilder(@Nonnull final ResourceType subjectResource) {
    this.subjectResource = subjectResource;
    columns = new ArrayList<>();
    filters = new ArrayList<>();
    limit = null;
  }

  public ExtractRequestBuilder withColumn(@Nonnull final String expression) {
    columns.add(ExpressionWithLabel.withNoLabel(expression));
    return this;
  }

  public ExtractRequestBuilder withColumn(@Nonnull final String expression,
      @Nonnull final String label) {
    columns.add(ExpressionWithLabel.of(expression, label));
    return this;
  }

  public ExtractRequestBuilder withFilter(@Nonnull final String expression) {
    filters.add(expression);
    return this;
  }

  public ExtractRequestBuilder withLimit(@Nonnull final Integer limit) {
    this.limit = limit;
    return this;
  }

  public ExtractRequest build() {
    return new ExtractRequest(subjectResource, columns, filters, Optional.ofNullable(limit));
  }
}
