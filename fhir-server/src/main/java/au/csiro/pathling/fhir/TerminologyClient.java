/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.fhir;

import ca.uhn.fhir.rest.annotation.*;
import ca.uhn.fhir.rest.client.api.IBasicClient;
import java.util.List;
import org.hl7.fhir.r4.model.*;

/**
 * @author John Grimes
 */
public interface TerminologyClient extends IBasicClient {

  @Metadata
  CapabilityStatement getServerMetadata();

  @Transaction
  Bundle batch(@TransactionParam Bundle input);

  @Operation(name = "$closure")
  ConceptMap closure(@OperationParam(name = "name") StringType name,
      @OperationParam(name = "concept") List<Coding> concept,
      @OperationParam(name = "version") StringType version);

}
