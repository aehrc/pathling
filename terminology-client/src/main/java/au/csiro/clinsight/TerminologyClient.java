/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import ca.uhn.fhir.rest.annotation.*;
import ca.uhn.fhir.rest.client.api.IBasicClient;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.dstu3.model.*;

/**
 * @author John Grimes
 */
public interface TerminologyClient extends IBasicClient {

  @Search
  List<StructureDefinition> getAllStructureDefinitions(@Elements Set<String> theElements);

  @Read
  StructureDefinition getStructureDefinitionById(@IdParam IdType theId);

  @Operation(name = "$expand", type = ValueSet.class)
  ValueSet expandValueSet(@OperationParam(name = "url") UriType url);

  @Operation(name = "$closure", type = ConceptMap.class)
  ConceptMap closure(@OperationParam(name = "name") StringType name,
      @OperationParam(name = "concept") List<Coding> concept,
      @OperationParam(name = "version") StringType version);

}
