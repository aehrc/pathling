/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import ca.uhn.fhir.rest.annotation.*;
import ca.uhn.fhir.rest.client.api.IBasicClient;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.*;

/**
 * @author John Grimes
 */
public interface TerminologyClient extends IBasicClient {

  @Metadata
  CapabilityStatement getServerMetadata();

  @Search
  List<StructureDefinition> getAllStructureDefinitions(@Elements Set<String> theElements);

  @Read
  StructureDefinition getStructureDefinitionById(@IdParam IdType theId);

  @Search
  List<CodeSystem> getAllCodeSystems(@Elements Set<String> theElements);

  @Operation(name = "$validate-code", type = ValueSet.class)
  Parameters validateCode(@OperationParam(name = "url") UriType url,
      @OperationParam(name = "coding") Coding coding);

  @Operation(name = "$validate-code", type = ValueSet.class)
  Parameters validateCode(@OperationParam(name = "url") UriType url,
      @OperationParam(name = "codeableConcept") CodeableConcept codeableConcept);

  @Operation(name = "$closure")
  ConceptMap closure(@OperationParam(name = "name") StringType name,
      @OperationParam(name = "concept") List<Coding> concept,
      @OperationParam(name = "version") StringType version);

}
