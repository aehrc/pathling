/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.errors.ErrorHandling.handleError;

import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * HAPI plain provider that provides an entry point for the "aggregate" system-wide operation.
 *
 * @author John Grimes
 */
@Component
public class AggregateProvider {

  @Nonnull
  private final AggregateExecutor aggregateExecutor;

  @Autowired
  private AggregateProvider(@Nonnull final AggregateExecutor aggregateExecutor) {
    this.aggregateExecutor = aggregateExecutor;
  }

  /**
   * Extended FHIR operation: "aggregate".
   *
   * @param parameters Input {@link Parameters} for the operation
   * @return {@link Parameters} object representing the result
   */
  @Operation(name = "$aggregate", idempotent = true)
  public Parameters aggregate(@Nullable @ResourceParam final Parameters parameters) {
    if (parameters == null) {
      throw new InvalidRequestException("Missing Parameters resource");
    }

    try {
      final AggregateRequest query = AggregateRequest.from(parameters);
      final AggregateResponse result = aggregateExecutor.execute(query);
      return result.toParameters();

    } catch (final Throwable e) {
      throw handleError(e);
    }
  }

}
