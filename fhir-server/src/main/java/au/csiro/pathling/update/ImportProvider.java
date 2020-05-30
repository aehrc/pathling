/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import au.csiro.pathling.errors.InvalidUserInputError;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.stereotype.Component;

/**
 * Enables the bulk import of data into the server.
 *
 * @author John Grimes
 */
@Component
@Slf4j
public class ImportProvider {

  @Nonnull
  private final ImportExecutor executor;

  /**
   * @param executor An {@link ImportExecutor} to use in executing import requests
   */
  public ImportProvider(@Nonnull final ImportExecutor executor) {
    this.executor = executor;
  }

  /**
   * Accepts a request of type `application/fhir+ndjson` and overwrites the warehouse tables with
   * the contents. Does not currently support any sort of incremental update or appending to the
   * warehouse tables. Also does not currently support asynchronous processing.
   * <p>
   * Each input will be treated as a file containing only one type of resource type. Bundles are not
   * currently given any special treatment. Each resource type is assumed to appear in the list only
   * once - multiple occurrences will result in the last input overwriting the previous ones.
   *
   * @param parameters A FHIR {@link Parameters} object describing the import request
   * @return A FHIR {@link OperationOutcome} resource describing the result
   */
  @Operation(name = "$import")
  public OperationOutcome importOperation(@ResourceParam final Parameters parameters) {
    try {
      return executor.execute(parameters);

    } catch (final InvalidUserInputError e) {
      throw new InvalidRequestException(e);

    } catch (final Exception e) {
      throw new InternalErrorException("Unexpected error occurred during import", e);
    }
  }

}
