/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.extract;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
public class ExtractRequestBuilder {

  @Nonnull
  private final ResourceType subjectResource;

  @Nonnull
  private final List<String> columns;

  @Nonnull
  private final List<String> filters;

  @Nonnull
  private String requestId;

  public ExtractRequestBuilder(@Nonnull final ResourceType subjectResource) {
    this.subjectResource = subjectResource;
    columns = new ArrayList<>();
    filters = new ArrayList<>();
    requestId = UUID.randomUUID().toString();
  }

  public ExtractRequestBuilder withColumn(@Nonnull final String expression) {
    columns.add(expression);
    return this;
  }

  public ExtractRequestBuilder withFilter(@Nonnull final String expression) {
    filters.add(expression);
    return this;
  }

  public ExtractRequestBuilder withRequestId(@Nonnull final String requestId) {
    this.requestId = requestId;
    return this;
  }

  public ExtractRequest build() {
    return new ExtractRequest(subjectResource, Optional.of(columns), Optional.of(filters),
        Optional.of(requestId));
  }

}
