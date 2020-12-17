/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
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
    return new AggregateRequest(subjectResource, aggregations, groupings, filters);
  }

}
