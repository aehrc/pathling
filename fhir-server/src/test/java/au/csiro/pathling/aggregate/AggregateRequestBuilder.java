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

package au.csiro.pathling.aggregate;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
public class AggregateRequestBuilder {

  @Nonnull
  private final ResourceType subjectResource;

  @Nonnull
  private final List<String> aggregations;

  @Nonnull
  private final List<String> groupings;

  @Nonnull
  private final List<String> filters;

  public AggregateRequestBuilder(@Nonnull final ResourceType subjectResource) {
    this.subjectResource = subjectResource;
    aggregations = new ArrayList<>();
    groupings = new ArrayList<>();
    filters = new ArrayList<>();
  }

  public AggregateRequestBuilder withAggregation(@Nonnull final String expression) {
    aggregations.add(expression);
    return this;
  }

  public AggregateRequestBuilder withGrouping(@Nonnull final String expression) {
    groupings.add(expression);
    return this;
  }

  public AggregateRequestBuilder withFilter(@Nonnull final String expression) {
    filters.add(expression);
    return this;
  }

  public AggregateRequest build() {
    return AggregateRequest.fromUserInput(subjectResource, Optional.of(aggregations),
        Optional.of(groupings),
        Optional.of(filters));
  }

}
