/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import java.io.IOException;
import java.net.URISyntaxException;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enables the bulk import of data into the server.
 *
 * @author John Grimes
 */
public class ImportProvider {

  private static final Logger logger = LoggerFactory.getLogger(ImportProvider.class);
  private final ImportExecutor executor;

  public ImportProvider(ImportExecutor executor) {
    this.executor = executor;
  }

  /**
   * Accepts a request of type `application/fhir+ndjson` and overwrites the warehouse tables with
   * the contents. Does not currently support any sort of incremental update or appending to the
   * warehouse tables. Also does not currently support asynchronous processing.
   *
   * Each input will be treated as a file containing only one type of resource type. Bundles are not
   * currently given any special treatment. Each resource type is assumed to appear in the list only
   * once - multiple occurrences will result in the last input overwriting the previous ones.
   */
  @Operation(name = "$import")
  public OperationOutcome importOperation(@ResourceParam Parameters inParams)
      throws IOException, URISyntaxException {
    return executor.execute(inParams);
  }

}
